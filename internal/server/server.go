package server

import (
	"errors"
	"net"
	"sync"

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
	services map[string]interface{} // serviceName -> 实例, 分发靠反射
	limiter  *limiter.TokenBucket   // 服务端限流, 保护自己
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
		services: make(map[string]interface{}),
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

// Register 注册服务实例, 真正的方法分发在调用时由反射完成
func (s *Server) Register(name string, service interface{}) error {
	if name == "" || service == nil {
		return errors.New("server: invalid service registration")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[name]; ok {
		return errors.New("server: service already registered: " + name)
	}
	s.services[name] = service
	return nil
}

func (s *Server) lookup(name string) (interface{}, bool) {
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

// Handle 单连接的读-限流-处理循环
func (s *Server) Handle(conn *transport.TCPConnection) {
	defer conn.Close()

	for {
		msg, err := conn.Read()
		if err != nil {
			// 连接关闭或出错, 退出
			return
		}

		// 服务端限流
		if !s.limiter.Allow() {
			s.writeError(conn, msg.Header.RequestID, "rate limit exceeded")
			continue
		}

		service, ok := s.lookup(msg.Header.ServiceName)
		if !ok {
			s.writeError(conn, msg.Header.RequestID, "service not found: "+msg.Header.ServiceName)
			continue
		}

		s.handler.Process(conn, msg, service)
	}
}

func (s *Server) writeError(conn *transport.TCPConnection, requestID uint64, msg string) {
	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   requestID,
			Error:       msg,
			Compression: codec.CompressionGzip,
		},
	}
	_ = conn.Write(resp)
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
