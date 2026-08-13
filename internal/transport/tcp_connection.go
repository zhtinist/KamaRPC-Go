package transport

import (
	"bufio"
	"net"
	"sync"

	"kamaRPC/internal/protocol"
)

// BufferSize 单次 socket 读取的大小, 同时作为 bufio.Reader 的缓冲大小
const BufferSize = 4096

// PacketBuffer 累积 socket 读到的碎片数据, 用来解决粘包/半包。
// 有效数据是 buf[off:], 用读偏移代替切片前移, 底层数组可以一直复用
type PacketBuffer struct {
	buf  []byte
	off  int
	lock sync.Mutex
}

// Read 尝试从缓冲区里切出一个完整包(拷贝一份), 不够一个完整包就返回 nil。
// 返回的切片归调用方所有, 可以任意持有
func (pb *PacketBuffer) Read() []byte {
	packet := pb.readBorrowed()
	if packet == nil {
		return nil
	}
	out := make([]byte, len(packet))
	copy(out, packet)
	return out
}

// readBorrowed 返回指向内部缓冲的完整包, 不拷贝。
//
// 借来的切片只在"下一次 Write 之前"有效 —— Write 可能追加、压缩或搬移底层
// 数组。由于同一条连接的读取只由一个协程完成, 这等价于"下一次 Read 之前有效"
func (pb *PacketBuffer) readBorrowed() []byte {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	data := pb.buf[pb.off:]

	// 最小包头长度校验
	if len(data) < protocol.HeaderFixedLen {
		return nil
	}

	headerLen := int(protocol.DecodeHeaderLen(data[2:6]))
	bodyLen := int(protocol.DecodeBodyLen(data[6:10]))
	totalLen := protocol.HeaderFixedLen + headerLen + bodyLen

	if len(data) < totalLen {
		return nil
	}

	packet := data[:totalLen]

	// 移动读偏移; 全部消费完则直接复位, 复用底层数组
	pb.off += totalLen
	if pb.off == len(pb.buf) {
		pb.buf = pb.buf[:0]
		pb.off = 0
	}
	return packet
}

// HasPacket 只判断缓冲区里是否已经攒够一个完整包, 不消费数据
func (pb *PacketBuffer) HasPacket() bool {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	data := pb.buf[pb.off:]
	if len(data) < protocol.HeaderFixedLen {
		return false
	}

	headerLen := int(protocol.DecodeHeaderLen(data[2:6]))
	bodyLen := int(protocol.DecodeBodyLen(data[6:10]))
	return len(data) >= protocol.HeaderFixedLen+headerLen+bodyLen
}

// Write 把新从 socket 读到的字节追加进缓冲区
func (pb *PacketBuffer) Write(data []byte) {
	pb.lock.Lock()
	// 追加会超出容量时, 先把已消费的前缀挤掉, 尽量避免重新分配
	if pb.off > 0 && len(pb.buf)+len(data) > cap(pb.buf) {
		n := copy(pb.buf, pb.buf[pb.off:])
		pb.buf = pb.buf[:n]
		pb.off = 0
	}
	pb.buf = append(pb.buf, data...)
	pb.lock.Unlock()
}

// TCPConnection 在 TCP 字节流之上构建有边界的消息协议:
// 1. 解决粘包/半包 2. 提供完整 Message 读写 3. 屏蔽底层 net.Conn 细节
type TCPConnection struct {
	conn    net.Conn      // 操作系统 TCP
	reader  *bufio.Reader // 带缓冲读取, 减少系统调用
	buffer  *PacketBuffer // 解决粘包/半包
	readBuf []byte        // 复用的读暂存区, 见 Read 的单协程约定

	// 借用式读取复用的消息与头部, 避免每条消息都分配。
	// 只由读协程使用, 见 ReadBorrowed 的契约
	scratchMsg    protocol.Message
	scratchHeader protocol.Header

	writeMu sync.Mutex  // 保护 writing/batch, 并保证一条消息完整写入
	writing bool        // 是否已有协程在写(它负责代写排队的批次)
	batch   *writeBatch // 等待被合并写出的数据
}

// NewTCPConnection 把一个普通 TCP 连接包装成带协议能力的连接
func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, BufferSize),
		buffer: &PacketBuffer{
			buf: make([]byte, 0, BufferSize*2),
		},
		readBuf: make([]byte, BufferSize),
	}
}

// ReadBorrowed 读一条消息, 返回的 Message 与其 Body 都指向连接内部缓冲。
//
// 契约: 返回值只在**下一次对本连接调用 Read/ReadBorrowed 之前**有效。
// 需要跨越这个边界持有(比如把 Body 交给 Future, 或丢给协程异步处理)时,
// 必须先自己拷贝一份。
//
// 换来的是零拷贝与零分配: 省掉整包拷贝, 也省掉每条消息的 Message/Header 分配
func (tc *TCPConnection) ReadBorrowed() (*protocol.Message, error) {
	for {
		if packet := tc.buffer.readBorrowed(); packet != nil {
			tc.scratchMsg.Header = &tc.scratchHeader
			if err := protocol.DecodeInto(packet, &tc.scratchMsg); err != nil {
				return nil, err
			}
			return &tc.scratchMsg, nil
		}

		n, err := tc.reader.Read(tc.readBuf)
		if n > 0 {
			tc.buffer.Write(tc.readBuf[:n])
		}
		if err != nil {
			return nil, err
		}
	}
}

// Read 不断读取数据, 直到拼出一个完整协议包。
//
// 约定: 单个连接的 Read 只由一个协程调用(客户端是 readLoop, 服务端是 Handle),
// 因此 readBuf 可以在多次读取间复用 —— 读到的字节会立刻拷进 PacketBuffer,
// 不会有引用逃逸出去
func (tc *TCPConnection) Read() (*protocol.Message, error) {
	for {
		// 先看缓冲区里有没有完整的一条消息
		if packet := tc.buffer.Read(); packet != nil {
			return protocol.Decode(packet)
		}

		n, err := tc.reader.Read(tc.readBuf)
		if n > 0 {
			tc.buffer.Write(tc.readBuf[:n])
		}
		if err != nil {
			return nil, err
		}
	}
}

// maxPooledWriteBuf 超过这个容量的写缓冲不再放回池子,
// 避免个别大包把大块内存长期留在池里
const maxPooledWriteBuf = 64 << 10

// 写缓冲池: 编码在写锁之外完成, 所以用池而不是连接内的固定缓冲,
// 这样同一连接上的并发写(服务端并发处理时)仍可各自编码
var writeBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, BufferSize)
		return &buf
	},
}

// writeBatch 一批等待合并写出的数据。
// 一条连接上并发写时, 后来者把自己的字节追加进同一批, 由当前正在写的协程
// (leader)一次性写出去, 把 N 次 write 系统调用压成 1 次
type writeBatch struct {
	buf  []byte
	done chan struct{}
	err  error
}

var batchBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, BufferSize)
		return &buf
	},
}

// Write 编码并完整写入一条消息。
//
// 采用 group commit: 没有并发写时走快路径, 直接写自己的数据, 不产生任何
// 额外开销; 有并发写时, 后来者把数据并进批次并等待, 当前的 leader 写完自己
// 的数据后顺手把整批一次写出。
//
// 错误语义与串行版本一致: 每个调用方拿到的都是"自己这条消息所在那次写"的
// 错误, 因此 SendAsync 依赖写失败判定连接已死的逻辑不受影响。
func (tc *TCPConnection) Write(msg *protocol.Message) error {
	bufp := writeBufPool.Get().(*[]byte)
	data, err := protocol.AppendEncoded((*bufp)[:0], msg)
	if err != nil {
		writeBufPool.Put(bufp)
		return err
	}
	// 记住扩容后的切片, 下次复用更大的容量
	*bufp = data

	writeErr := tc.WriteRaw(data)

	if cap(*bufp) <= maxPooledWriteBuf {
		writeBufPool.Put(bufp)
	}
	return writeErr
}

// WriteRaw 写入已经编码好的字节(可以是多条消息拼在一起)。
// 返回后调用方即可复用自己的缓冲: 走批次时数据已拷贝, 走快路径时已写完
func (tc *TCPConnection) WriteRaw(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	tc.writeMu.Lock()

	// 已经有协程在写: 把自己并进批次, 等它代写
	if tc.writing {
		if tc.batch == nil {
			buf := batchBufPool.Get().(*[]byte)
			tc.batch = &writeBatch{buf: (*buf)[:0], done: make(chan struct{})}
		}
		b := tc.batch
		b.buf = append(b.buf, data...)
		tc.writeMu.Unlock()

		<-b.done
		return b.err
	}

	// 快路径: 当前没人在写, 自己写自己的
	tc.writing = true
	tc.writeMu.Unlock()

	writeErr := tc.flush(data)

	// 我写的期间可能有人排上了队, 由我把他们的批次一次性写出去。
	// 必须排空到没有批次为止 —— 等待者只能靠 leader 唤醒, 中途撒手会让他们永远挂住
	tc.writeMu.Lock()
	for tc.batch != nil {
		cur := tc.batch
		tc.batch = nil
		tc.writeMu.Unlock()

		curErr := tc.flush(cur.buf)

		buf := cur.buf
		if cap(buf) <= maxPooledWriteBuf {
			batchBufPool.Put(&buf)
		}
		cur.buf = nil
		cur.err = curErr
		close(cur.done)

		tc.writeMu.Lock()
	}
	tc.writing = false
	tc.writeMu.Unlock()

	return writeErr
}

// flush 把整块数据完整写入 socket。
// 一条消息(或一批消息)必须整体写完, 否则会出现字节交叉, 对端解不出包
func (tc *TCPConnection) flush(data []byte) error {
	total := 0
	for total < len(data) {
		n, err := tc.conn.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}
	return nil
}

// HasBufferedPacket 判断是否已经有下一个完整包躺在缓冲区里。
// 为真时下一次 Read 不会阻塞在系统调用上, 调用方可以据此决定是否继续攒批。
// 只看已拼好的完整包, 不看 bufio 里的半包 —— 半包仍可能需要等待网络。
// 只能由 Read 所在的协程调用
func (tc *TCPConnection) HasBufferedPacket() bool {
	return tc.buffer.HasPacket()
}

// RemoteAddr 对端地址, 用于日志与问题定位
func (tc *TCPConnection) RemoteAddr() string {
	if tc.conn == nil {
		return ""
	}
	return tc.conn.RemoteAddr().String()
}

// Close 关闭连接
func (tc *TCPConnection) Close() error {
	if tcp, ok := tc.conn.(*net.TCPConn); ok {
		// 关闭时不等待未发送数据, 强制断开
		tcp.SetLinger(0)
	}
	return tc.conn.Close()
}
