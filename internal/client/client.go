package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"kamaRPC/internal/breaker"
	"kamaRPC/internal/codec"
	"kamaRPC/internal/limiter"
	"kamaRPC/internal/loadbalance"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/registry"
	"kamaRPC/internal/transport"
)

// 客户端侧治理错误
var (
	ErrRateLimited  = errors.New("client: rate limit exceeded")
	ErrBreakerOpen  = errors.New("client: circuit breaker open")
	ErrNoInstance   = errors.New("client: no available instance")
	ErrClientClosed = errors.New("client: closed")
)

// defaultPoolSize 单个节点的默认连接数。
//
// 连接的作用是把流量分散到不同节点, 而不是提升单节点的并发 —— 单节点并发靠
// 请求复用(一条连接上并发多个请求)就够了。连接开多了反而让每条连接上的在途
// 请求变少, 读写都攒不成批, 系统调用次数上升。
//
// 实测(同步 50 并发 10s)显示真正决定吞吐的是"总连接数", 最优在 2 条附近:
//
//	两个实例 x pool=1 = 2 条   220k QPS  P99 0.43ms
//	一个实例 x pool=2 = 2 条   220k QPS  P99 0.44ms
//	一个实例 x pool=1 = 1 条   214k QPS  P99 0.57ms
//	两个实例 x pool=2 = 4 条   153k QPS  P99 0.54ms
//	一个实例 x pool=4 = 4 条   158k QPS  P99 0.53ms
//	两个实例 x pool=8 = 8 条   106k QPS  P99 0.84ms
//
// 每节点 1 条时总连接数自然跟着实例数走, 单实例与双实例下都在最优值 3% 以内;
// 而固定 2 条在双实例下会掉 31%。异步批量负载(100 并发 batch 100)结论一致:
// pool=1 中位数约 213k, pool=2 约 149k。
//
// 注意这个取值依赖于响应攒批与写合并 —— 在它们落地之前最优值是 2。
// 参数最优点会随实现变化, 每轮优化后都要重新验证
const defaultPoolSize = 1

// Client 一次调用的控制中心: 限流 → 服务发现 → 选址 → 熔断 → 取连接 → 组包 → 发包
type Client struct {
	reg     *registry.Registry       // 服务发现来源
	lb      loadbalance.LoadBalancer // 负载均衡策略
	limiter *limiter.TokenBucket     // 客户端限流, 保护自己
	timeout time.Duration            // 默认调用超时
	codec   codec.Codec              // 默认序列化协议

	breaker sync.Map // service+addr -> *breaker.CircuitBreaker, 实例级隔离
	pools   sync.Map // addr -> *transport.ConnectionPool, 连接复用

	// 熔断与连接池参数
	breakerWindow      int
	breakerThreshold   float64
	breakerOpenTimeout time.Duration
	poolMaxActive      int

	closed bool
	mu     sync.Mutex
}

// NewClient 选项模式构造客户端, 未指定的选项走默认值
func NewClient(reg *registry.Registry, opts ...ClientOption) (*Client, error) {
	if reg == nil {
		return nil, errors.New("client: registry is nil")
	}

	defaultCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}

	c := &Client{
		reg:     reg,
		lb:      loadbalance.NewRR(),
		limiter: limiter.NewTokenBucket(10000),
		timeout: 5 * time.Second,
		codec:   defaultCodec,

		breakerWindow:      20,
		breakerThreshold:   0.6,
		breakerOpenTimeout: 5 * time.Second,
		poolMaxActive:      defaultPoolSize,
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// InvokeAsync 异步调用, 立即返回 Future
func (c *Client) InvokeAsync(ctx context.Context, service string, method string, args interface{}) (*transport.Future, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClientClosed
	}

	// 1. 客户端限流
	if !c.limiter.Allow() {
		return nil, ErrRateLimited
	}

	// 2. 服务发现 + 3. 负载均衡选址
	addr, err := c.getAddr(service)
	if err != nil {
		return nil, err
	}

	// 4. 熔断判断
	br := c.getBreaker(service, addr)
	if !br.Allow() {
		return nil, ErrBreakerOpen
	}

	// 5. 连接池取连接
	pool := c.getPool(addr)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		br.RecordFailure()
		return nil, err
	}

	// 6. 序列化参数
	body, err := c.codec.Marshal(args)
	if err != nil {
		return nil, err
	}

	// 7. 组装协议 Message
	req := &protocol.Message{
		Header: &protocol.Header{
			ServiceName: service,
			MethodName:  method,
			CodecType:   c.codec.Type(),
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}

	// 8. 发送并返回 Future
	future, err := conn.SendAsync(req)
	if err != nil {
		br.RecordFailure()
		return nil, err
	}
	future.SetCodec(c.codec)

	// 9. Future 完成时更新熔断统计
	future.OnComplete(func(err error) {
		if err != nil {
			br.RecordFailure()
		} else {
			br.RecordSuccess()
		}
	})

	return future, nil
}

// BatchCall 批量调用里的一次调用
type BatchCall struct {
	Method string
	Args   interface{}
}

// InvokeAsyncBatch 把同一个服务的多次调用打成一批发出, 只写一次 socket。
//
// 与循环调用 InvokeAsync 的区别: 服务发现、选址、熔断判断、取连接都只做一次,
// 而且 N 个请求编码进同一个缓冲、一次写出。适合客户端本来就要连发一批请求的
// 场景(比如批量查询), 单条请求的延迟敏感场景仍然用 InvokeAsync。
//
// 全有或全无: 任何一步失败都返回错误, 不会有部分请求已发出的中间状态
func (c *Client) InvokeAsyncBatch(ctx context.Context, service string, calls []BatchCall) ([]*transport.Future, error) {
	if len(calls) == 0 {
		return nil, nil
	}

	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClientClosed
	}

	// 限流按请求条数扣减, 不能整批只算一个
	for i := 0; i < len(calls); i++ {
		if !c.limiter.Allow() {
			return nil, ErrRateLimited
		}
	}

	addr, err := c.getAddr(service)
	if err != nil {
		return nil, err
	}

	br := c.getBreaker(service, addr)
	if !br.Allow() {
		return nil, ErrBreakerOpen
	}

	pool := c.getPool(addr)

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		br.RecordFailure()
		return nil, err
	}

	msgs := make([]*protocol.Message, len(calls))
	headers := make([]protocol.Header, len(calls))
	for i, call := range calls {
		body, err := c.codec.Marshal(call.Args)
		if err != nil {
			return nil, err
		}
		headers[i] = protocol.Header{
			ServiceName: service,
			MethodName:  call.Method,
			CodecType:   c.codec.Type(),
			Compression: codec.CompressionGzip,
		}
		msgs[i] = &protocol.Message{Header: &headers[i], Body: body}
	}

	futures, err := conn.SendAsyncBatch(msgs)
	if err != nil {
		br.RecordFailure()
		return nil, err
	}

	for _, future := range futures {
		future.SetCodec(c.codec)
		future.OnComplete(func(err error) {
			if err != nil {
				br.RecordFailure()
			} else {
				br.RecordSuccess()
			}
		})
	}

	return futures, nil
}

// Invoke 同步调用, 本质是 InvokeAsync + 等待 Future
func (c *Client) Invoke(ctx context.Context, service string, method string, args interface{}, reply interface{}) error {
	future, err := c.InvokeAsync(ctx, service, method, args)
	if err != nil {
		return err
	}

	waitCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	select {
	case <-future.DoneChan():
		return future.GetResult(reply)
	case <-waitCtx.Done():
		return fmt.Errorf("client: invoke %s.%s: %w", service, method, waitCtx.Err())
	}
}

// getAddr 服务发现 + 负载均衡选址
func (c *Client) getAddr(service string) (string, error) {
	instances, err := c.reg.Discover(service)
	if err != nil {
		return "", err
	}
	if len(instances) == 0 {
		return "", fmt.Errorf("%w for service %s", ErrNoInstance, service)
	}

	ins := c.lb.Select(instances)
	if ins.Addr == "" {
		return "", fmt.Errorf("%w for service %s", ErrNoInstance, service)
	}
	return ins.Addr, nil
}

// getBreaker 按 service+addr 维度拿熔断器, 让每个下游实例独立熔断
func (c *Client) getBreaker(service, addr string) *breaker.CircuitBreaker {
	key := service + "@" + addr
	if v, ok := c.breaker.Load(key); ok {
		return v.(*breaker.CircuitBreaker)
	}
	br := breaker.NewCircuitBreaker(c.breakerWindow, c.breakerThreshold, c.breakerOpenTimeout)
	actual, _ := c.breaker.LoadOrStore(key, br)
	return actual.(*breaker.CircuitBreaker)
}

// getPool 按 addr 维度拿连接池, 做到实例隔离 + 连接复用
func (c *Client) getPool(addr string) *transport.ConnectionPool {
	if v, ok := c.pools.Load(addr); ok {
		return v.(*transport.ConnectionPool)
	}
	pool := transport.NewConnectionPool(addr, c.poolMaxActive, c.poolMaxActive)
	actual, loaded := c.pools.LoadOrStore(addr, pool)
	if loaded {
		pool.Close()
	}
	return actual.(*transport.ConnectionPool)
}

// Close 关闭所有连接池
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.pools.Range(func(key, value interface{}) bool {
		value.(*transport.ConnectionPool).Close()
		c.pools.Delete(key)
		return true
	})
	return nil
}
