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
// 直觉会想开大一点, 但连接本身支持请求复用, 一条连接就能承载高并发;
// 连接开多了反而让每条连接上的在途请求变少, 读写都摊不到批量, 系统调用次数上升。
// 实测(50 并发同步, 10s, 数据稳定可复现):
//
//	pool=1   154k QPS   P99 0.48ms
//	pool=2   126k QPS   P99 0.59ms
//	pool=4   111k QPS   P99 0.70ms
//	pool=8   102k QPS   P99 0.80ms
//	pool=16   93k QPS   P99 0.89ms
//
// 异步批量负载(100 并发 / batch 100)则偏好多连接(服务端按连接并行处理),
// 但多次交错实测 pool=2 与 pool=8 的中位数是 121k 与 130k, 波动 ±15%,
// 差异落在噪声里, 不足以支撑结论。
//
// 因此取 2: 同步路径拿到明显且可复现的收益, 异步路径不明显变差。
// 单连接虽然同步最快, 但一条连接是单点, 且写入串行化, 留 2 条更稳妥
const defaultPoolSize = 2

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
