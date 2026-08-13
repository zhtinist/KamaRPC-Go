package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrPoolClosed 连接池已关闭
var ErrPoolClosed = errors.New("transport: connection pool closed")

// ConnectionPool 单个目标节点的连接池, 实现连接复用;
// 池内连接本身支持请求复用, 所以取出的连接不会从池中弹出
type ConnectionPool struct {
	addr      string       // 远端地址
	maxActive int          // 最大连接数
	conns     []*TCPClient // 当前连接列表
	mu        sync.Mutex   // 并发安全
	closed    bool         // 是否已关闭
	next      int          // 轮询索引
}

// NewConnectionPool 创建连接池, 连接按需创建(懒加载), 避免启动即建连
func NewConnectionPool(addr string, maxIdle, maxActive int) *ConnectionPool {
	if maxActive <= 0 {
		maxActive = 1
	}
	return &ConnectionPool{
		addr:      addr,
		maxActive: maxActive,
		conns:     make([]*TCPClient, 0, maxActive),
	}
}

// Acquire 取一条可用连接
func (p *ConnectionPool) Acquire(ctx context.Context) (*TCPClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	// 没到上限就新建, 提升并发度
	if len(p.conns) < p.maxActive {
		conn, err := newTCPClient(p.addr)
		if err != nil {
			return nil, err
		}
		p.conns = append(p.conns, conn)
		return conn, nil
	}

	// 到上限则轮询已有连接; 同一条连接可以被多个协程同时持有(请求复用)
	for i := 0; i < len(p.conns); i++ {
		idx := (p.next + i) % len(p.conns)
		conn := p.conns[idx]

		if atomic.LoadInt32(&conn.closed) == 0 {
			p.next = (idx + 1) % len(p.conns)
			return conn, nil
		}

		// 清理已死连接: 服务端重启后旧连接仍在池中, 直接用会发送失败
		p.conns = append(p.conns[:idx], p.conns[idx+1:]...)
		if len(p.conns) == 0 {
			break
		}
		i--
	}

	// 全死了就重新建一条
	conn, err := newTCPClient(p.addr)
	if err != nil {
		return nil, err
	}
	p.conns = append(p.conns, conn)
	return conn, nil
}

// Len 当前池内连接数
func (p *ConnectionPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// Close 关闭池内所有连接
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true

	for _, conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
}
