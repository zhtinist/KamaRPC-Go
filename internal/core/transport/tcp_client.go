package transport

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kamaRPC/internal/core/protocol"
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

	pending *pendingMap // requestID → Future, 多路复用的响应归属表

	closed int32 // 关闭标记
}

// newTCPClient 建立连接并启动响应分发协程
func newTCPClient(addr string) (*TCPClient, error) {
	rawConn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}

	c := &TCPClient{
		conn:    NewTCPConnection(rawConn),
		addr:    addr,
		pending: newPendingMap(),
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
		// 借用式读取省掉整包拷贝与每条消息的 Message/Header 分配;
		// 只有要交给 Future 异步持有的 Body 需要拷一份出来
		msg, err := c.conn.ReadBorrowed()
		if err != nil {
			c.fail(err)
			return
		}

		seq := msg.Header.RequestID
		future, ok := c.pending.LoadAndDelete(seq)
		if !ok {
			continue
		}

		if msg.Header.Error != "" {
			future.Done(nil, errors.New(msg.Header.Error))
		} else {
			body := make([]byte, len(msg.Body))
			copy(body, msg.Body)
			future.Done(body, nil)
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

// SendAsyncBatch 一次性发出多条请求, 编码进同一个缓冲、只写一次。
//
// 单协程连续发请求时, 写合并(group commit)是不生效的 —— 没有并发写就没有
// 可合并的对象。异步批量场景正是这种形态: 一个协程连发 N 个请求, 每个各来
// 一次 write。这个接口把它压成一次。
//
// 语义是全有或全无: 任何一步失败都会撤销本批已登记的 Future 并返回错误
func (c *TCPClient) SendAsyncBatch(msgs []*protocol.Message) ([]*Future, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, ErrConnClosed
	}

	futures := make([]*Future, len(msgs))
	seqs := make([]uint64, len(msgs))

	bufp := writeBufPool.Get().(*[]byte)
	buf := (*bufp)[:0]

	rollback := func(n int) {
		for i := 0; i < n; i++ {
			c.pending.Delete(seqs[i])
		}
		if cap(buf) <= maxPooledWriteBuf {
			*bufp = buf
			writeBufPool.Put(bufp)
		}
	}

	for i, msg := range msgs {
		seq := c.nextSeq()
		msg.Header.RequestID = seq

		future := NewFuture()
		c.pending.Store(seq, future)
		futures[i], seqs[i] = future, seq

		var err error
		if buf, err = protocol.AppendEncoded(buf, msg); err != nil {
			rollback(i + 1)
			return nil, err
		}
	}

	err := c.conn.WriteRaw(buf)

	*bufp = buf
	if cap(buf) <= maxPooledWriteBuf {
		writeBufPool.Put(bufp)
	}

	if err != nil {
		// 与单条发送一致: 写失败即判定连接已死, 让全部在途请求失败
		for _, seq := range seqs {
			c.pending.Delete(seq)
		}
		c.fail(err)
		return nil, err
	}

	return futures, nil
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

	for _, future := range c.pending.DrainAll() {
		future.Done(nil, err)
	}
}

// Close 主动关闭连接
func (c *TCPClient) Close() error {
	c.fail(ErrConnClosed)
	return nil
}
