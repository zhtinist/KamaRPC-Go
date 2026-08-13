package transport

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kamaRPC/internal/protocol"
)

// ErrConnClosed 连接已关闭
var ErrConnClosed = errors.New("transport: connection closed")

// dialTimeout 建连超时
const dialTimeout = 5 * time.Second

// TCPClient 带请求复用能力的逻辑客户端:
// requestID 分配 / Future 管理 / 异步响应分发 / 连接失败广播
type TCPClient struct {
	conn *TCPConnection // 底层协议连接
	addr string         // 服务端地址

	writeMu sync.Mutex // 发包级别串行化
	seq     uint64     // requestID 生成器

	pending sync.Map // map[uint64]*Future, 多路复用的响应归属表

	closed int32 // 关闭标记
}

// newTCPClient 建立连接并启动响应分发协程
func newTCPClient(addr string) (*TCPClient, error) {
	rawConn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}

	c := &TCPClient{
		conn: NewTCPConnection(rawConn),
		addr: addr,
	}
	go c.readLoop()
	return c, nil
}

// NewTCPClient 对外暴露的构造函数
func NewTCPClient(addr string) (*TCPClient, error) {
	return newTCPClient(addr)
}

// Addr 返回对端地址
func (c *TCPClient) Addr() string { return c.addr }

func (c *TCPClient) nextSeq() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}

// readLoop 后台协程: 按 RequestID 把响应派发给对应 Future
func (c *TCPClient) readLoop() {
	for {
		msg, err := c.conn.Read()
		if err != nil {
			c.fail(err)
			return
		}

		seq := msg.Header.RequestID
		val, ok := c.pending.LoadAndDelete(seq)
		if !ok {
			continue
		}

		future := val.(*Future)
		if msg.Header.Error != "" {
			future.Done(nil, errors.New(msg.Header.Error))
		} else {
			future.Done(msg.Body, nil)
		}
	}
}

// SendAsync 发送请求并返回 Future
func (c *TCPClient) SendAsync(msg *protocol.Message) (*Future, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, ErrConnClosed
	}

	seq := c.nextSeq()
	msg.Header.RequestID = seq

	future := NewFuture()
	c.pending.Store(seq, future)

	c.writeMu.Lock()
	err := c.conn.Write(msg)
	c.writeMu.Unlock()

	// 写失败一般意味着连接已断, 必须彻底杀死连接,
	// 否则后续请求会继续复用这条无效连接
	if err != nil {
		c.pending.Delete(seq)
		c.fail(err)
		return nil, err
	}

	return future, nil
}

// IsClosed 连接是否已关闭
func (c *TCPClient) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// fail 关闭连接并让所有 pending 请求失败
func (c *TCPClient) fail(err error) {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	_ = c.conn.Close()

	c.pending.Range(func(key, value interface{}) bool {
		future := value.(*Future)
		future.Done(nil, err)
		c.pending.Delete(key)
		return true
	})
}

// Close 主动关闭连接
func (c *TCPClient) Close() error {
	c.fail(ErrConnClosed)
	return nil
}
