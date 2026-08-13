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

// Read 尝试从缓冲区里切出一个完整包, 不够一个完整包就返回 nil
func (pb *PacketBuffer) Read() []byte {
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

	// 必须拷贝: 返回的包会被 Decode 解出 Body 交给 Future 异步持有,
	// 直接引用共享缓冲区会被后续写入覆盖
	packet := make([]byte, totalLen)
	copy(packet, data[:totalLen])

	// 移动读偏移; 全部消费完则直接复位, 复用底层数组
	pb.off += totalLen
	if pb.off == len(pb.buf) {
		pb.buf = pb.buf[:0]
		pb.off = 0
	}
	return packet
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
	writeMu sync.Mutex    // 保证一条消息完整写入, 避免字节交叉
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

// Write 编码并完整写入一条消息
func (tc *TCPConnection) Write(msg *protocol.Message) error {
	data, err := protocol.Encode(msg)
	if err != nil {
		return err
	}

	// 请求复用 + TCP 流式的双重原因: 必须整条消息串行写入,
	// 否则会出现 ABAB 交错, 而不是预期的 AABB
	tc.writeMu.Lock()
	defer tc.writeMu.Unlock()

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
