package server

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/limiter"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
)

// Server RPC 服务端: 监听 + 连接管理 + 限流 + 交给 Handler 反射调用
//
// 网络模型是 One Connection One Goroutine, 底层依赖 Go runtime 的
// netpoll(epoll/kqueue) 与 M:N 调度, 相当于 Go 风格的 Reactor 封装
type Server struct {
	addr     string
	services map[string]*serviceEntry // serviceName -> 方法表, 注册时解析好反射信息
	limiter  *limiter.TokenBucket     // 服务端限流, 保护自己
	listener net.Listener
	handler  *Handler
	codec    codec.Codec

	mu      sync.Mutex
	conns   map[*transport.TCPConnection]struct{} // 已建立连接, 用于优雅关闭
	closing chan struct{}
	closed  bool
}

// NewServer 选项模式构造服务端
func NewServer(addr string, opts ...ServerOption) (*Server, error) {
	defaultCodec, err := codec.New(codec.JSON)
	if err != nil {
		return nil, err
	}

	s := &Server{
		addr:     addr,
		services: make(map[string]*serviceEntry),
		limiter:  limiter.NewTokenBucket(100000),
		codec:    defaultCodec,
		conns:    make(map[*transport.TCPConnection]struct{}),
		closing:  make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	s.handler = NewHandler(s.codec)
	return s, nil
}

// Register 注册服务实例。方法查找与签名校验在这里一次性完成并缓存成方法表,
// 调用时不必再 MethodByName; 签名写错也能在启动阶段就发现
func (s *Server) Register(name string, service interface{}) error {
	if name == "" || service == nil {
		return errors.New("server: invalid service registration")
	}

	entry, err := newServiceEntry(name, service)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[name]; ok {
		return errors.New("server: service already registered: " + name)
	}
	s.services[name] = entry
	return nil
}

func (s *Server) lookup(name string) (*serviceEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc, ok := s.services[name]
	return svc, ok
}

// Addr 实际监听地址(便于监听 :0 时取到真实端口)
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Start 监听端口并为每个连接启动一个协程
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.closing:
				return nil
			default:
				continue
			}
		}

		tcpConn := transport.NewTCPConnection(conn)

		s.mu.Lock()
		s.conns[tcpConn] = struct{}{}
		s.mu.Unlock()

		go func() {
			s.Handle(tcpConn)

			s.mu.Lock()
			delete(s.conns, tcpConn)
			s.mu.Unlock()
		}()
	}
}

// MaxConcurrentRequestsPerConn 单连接最大并发处理请求数。
// 客户端支持请求复用(一条连接上并发多个请求), 服务端串行处理会让吞吐被
// 最慢的方法卡住; 但也不能无限开协程, 所以用信号量封顶
const MaxConcurrentRequestsPerConn = 256

// dispatchThreshold 业务方法平均耗时超过这个值才值得切协程并发处理。
// 切协程本身要付调度与缓存局部性的代价(实测约几微秒), 对于只花几微秒的
// 方法, 并发处理是净亏损; 对于毫秒级方法, 串行处理会把单连接吞吐压到 1/耗时
const dispatchThreshold = 50 * time.Microsecond

// Handle 单连接的读-限流-分发循环。
//
// 读包与限流在本协程串行完成(保证按到达顺序处理)。业务调用是否切协程, 取决于
// 这条连接上业务方法的平均耗时: 微秒级方法内联执行, 不付调度开销; 毫秒级方法
// 切协程, 避免一个慢调用堵住整条连接的读取。
// 响应写回由 TCPConnection 的写锁保证不会交错
func (s *Server) Handle(conn *transport.TCPConnection) {
	defer conn.Close()

	var wg sync.WaitGroup
	// 连接退出前等在途请求写完响应, 避免响应丢失
	defer wg.Wait()

	var (
		sem       = make(chan struct{}, MaxConcurrentRequestsPerConn)
		avgCost   int64 // 本连接业务方法耗时的滑动平均(纳秒)
		threshold = int64(dispatchThreshold)
		respBuf   []byte // 攒批待写的响应
	)

	for {
		// 只在"下一个请求还没到"时才把攒下的响应写出去。
		// 缓冲区里已经躺着完整包, 说明下一次 Read 不会阻塞, 可以继续攒,
		// 把流水线请求的多次 write 压成一次
		if len(respBuf) > 0 && !conn.HasBufferedPacket() {
			if err := conn.WriteRaw(respBuf); err != nil {
				return
			}
			respBuf = respBuf[:0]
		}

		msg, err := conn.Read()
		if err != nil {
			// 连接关闭或出错, 退出
			return
		}

		// 服务端限流
		if !s.limiter.Allow() {
			respBuf = AppendErrorResponse(respBuf, msg.Header.RequestID, "rate limit exceeded")
			continue
		}

		service, ok := s.lookup(msg.Header.ServiceName)
		if !ok {
			respBuf = AppendErrorResponse(respBuf, msg.Header.RequestID, "service not found: "+msg.Header.ServiceName)
			continue
		}

		// 只有慢方法才值得切协程
		if atomic.LoadInt64(&avgCost) > threshold {
			// 慢方法交给协程后何时完成不确定, 不能让已攒的响应干等, 先写出去
			if len(respBuf) > 0 {
				if err := conn.WriteRaw(respBuf); err != nil {
					return
				}
				respBuf = respBuf[:0]
			}

			// 并发上限满了就在这里阻塞, 相当于对读取端反压
			sem <- struct{}{}
			wg.Add(1)
			go func(msg *protocol.Message, service *serviceEntry) {
				defer func() {
					<-sem
					wg.Done()
				}()
				s.processAndRecord(conn, msg, service, &avgCost)
			}(msg, service)
			continue
		}

		respBuf = s.appendAndRecord(respBuf, msg, service, &avgCost)

		// 攒得太多先落盘, 避免占用过多内存, 也避免响应迟迟不回
		if len(respBuf) >= maxBatchedResponseBytes {
			if err := conn.WriteRaw(respBuf); err != nil {
				return
			}
			respBuf = respBuf[:0]
		}
	}
}

// maxBatchedResponseBytes 攒批响应的上限, 超过就先写出去
const maxBatchedResponseBytes = 64 << 10

// appendAndRecord 执行调用并把响应追加到 buf, 同时把耗时并入滑动平均
func (s *Server) appendAndRecord(dst []byte, msg *protocol.Message, service *serviceEntry, avgCost *int64) []byte {
	start := time.Now()
	out := s.handler.AppendResponse(dst, msg, service)
	cost := int64(time.Since(start))

	old := atomic.LoadInt64(avgCost)
	atomic.StoreInt64(avgCost, old-old/8+cost/8)
	return out
}

// processAndRecord 执行调用并把耗时并入滑动平均, 供分发策略参考
func (s *Server) processAndRecord(conn *transport.TCPConnection, msg *protocol.Message, service *serviceEntry, avgCost *int64) {
	start := time.Now()
	s.handler.Process(conn, msg, service)
	cost := int64(time.Since(start))

	// EWMA, α = 1/8; 并发更新只要求近似, 不需要严格原子读改写
	old := atomic.LoadInt64(avgCost)
	atomic.StoreInt64(avgCost, old-old/8+cost/8)
}

// Stop 优雅关闭: 停止 Accept 并关掉所有连接
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.closing)

	ln := s.listener
	conns := make([]*transport.TCPConnection, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	return nil
}
