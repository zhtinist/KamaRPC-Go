package protocol

import (
	"encoding/binary"
	"fmt"

	"kamaRPC/internal/codec"
)

// Header 的二进制编码(协议 v2)。
//
// 教程原始设计里 Header 走 JSON, 好处是可读、可扩展, 代价是体积与解析开销:
// 一个只有两个整数的请求, JSON Header 约 100 字节, 而 Body 只有 4~13 字节 ——
// 控制面反而成了包体积的大头。二进制编码把 Header 压到十几字节。
//
// 兼容性: 用另一个 Magic 区分两种 Header 编码, 收包时按 Magic 分派,
// 所以仍然认得老对端发来的 JSON Header 包。分帧字段(headerLen/bodyLen)
// 布局不变, PacketBuffer 的拆包逻辑对两种格式通用。
//
// 布局(全部小端 uvarint 或定长):
//
//	RequestID    uvarint
//	CodecType    1 字节
//	Compression  1 字节
//	ServiceName  uvarint 长度 + 字节
//	MethodName   uvarint 长度 + 字节
//	Error        uvarint 长度 + 字节
//
// 扩展方式: 因为 headerLen 已经给出了 Header 的总长度, 新版本可以在尾部追加
// 字段, 老版本读到已知字段后直接忽略剩余字节, 不需要再改 Magic

// maxHeaderStringLen Header 里单个字符串字段的长度上限。
// 防止坏包声明一个巨大的长度让我们去切片越界或分配大内存
const maxHeaderStringLen = 1 << 20

// appendHeaderBinary 把 Header 按二进制布局追加到 dst
func appendHeaderBinary(dst []byte, h *Header) []byte {
	dst = binary.AppendUvarint(dst, h.RequestID)
	dst = append(dst, byte(h.CodecType), byte(h.Compression))
	dst = appendLenPrefixed(dst, h.ServiceName)
	dst = appendLenPrefixed(dst, h.MethodName)
	dst = appendLenPrefixed(dst, h.Error)
	return dst
}

func appendLenPrefixed(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// maxHeaderBinaryLen 估算二进制 Header 的长度上界, 用于一次性预留容量
func maxHeaderBinaryLen(h *Header) int {
	// RequestID 最多 10 字节 varint, 两个枚举各 1 字节, 三个长度前缀各最多 5 字节
	return 10 + 2 + 15 + len(h.ServiceName) + len(h.MethodName) + len(h.Error)
}

// parseHeaderBinary 解析二进制 Header。
// 任何长度越界都当坏包处理, 不做部分容忍
func parseHeaderBinary(data []byte, h *Header) error {
	*h = Header{}

	requestID, n := binary.Uvarint(data)
	if n <= 0 {
		return fmt.Errorf("protocol: bad request id in binary header")
	}
	h.RequestID = requestID
	i := n

	if i+2 > len(data) {
		return fmt.Errorf("protocol: binary header truncated")
	}
	h.CodecType = codec.Type(data[i])
	h.Compression = codec.CompressionType(data[i+1])
	i += 2

	var err error
	// 服务名与方法名取值集合小, 驻留复用; Error 是任意文本, 不驻留免得撑爆缓存
	if h.ServiceName, i, err = readLenPrefixed(data, i, true); err != nil {
		return err
	}
	if h.MethodName, i, err = readLenPrefixed(data, i, true); err != nil {
		return err
	}
	if h.Error, i, err = readLenPrefixed(data, i, false); err != nil {
		return err
	}

	// 尾部可能是新版本追加的字段, 直接忽略
	return nil
}

func readLenPrefixed(data []byte, i int, intern bool) (string, int, error) {
	if i >= len(data) {
		return "", i, fmt.Errorf("protocol: binary header truncated")
	}

	n, read := binary.Uvarint(data[i:])
	if read <= 0 {
		return "", i, fmt.Errorf("protocol: bad string length in binary header")
	}
	i += read

	if n > maxHeaderStringLen {
		return "", i, fmt.Errorf("protocol: header string too long: %d", n)
	}
	end := i + int(n)
	if end > len(data) || end < i {
		return "", i, fmt.Errorf("protocol: binary header truncated")
	}

	if intern {
		return internBytes(data[i:end]), end, nil
	}
	return string(data[i:end]), end, nil
}
