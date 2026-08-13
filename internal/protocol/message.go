package protocol

import (
	"encoding/binary"
	"fmt"

	"kamaRPC/internal/codec"
)

// Message 一条完整的 RPC 消息 = 头部(控制面) + 消息体(数据面)
//
// 网络上的完整包结构:
//
//	┌────────┬───────────┬─────────┬────────────┬──────────┐
//	│ Magic  │ headerLen │ bodyLen │ Header数据  │ Body数据  │
//	│ 2字节  │ 4字节     │ 4字节   │ 变长        │ 变长     │
//	└────────┴───────────┴─────────┴────────────┴──────────┘
type Message struct {
	Header *Header
	Body   []byte
}

// MinCompressSize 低于这个长度的 Body 不压缩:
// gzip 自身有 20 余字节固定开销, 小包压完通常更大, 而 CPU 与内存开销远高于收益。
// 实际压缩方式由 Header.Compression 告知对端, 所以跳过压缩对协议是透明的
const MinCompressSize = 512

// DecodeHeaderLen 从字节切片解析 headerLen
func DecodeHeaderLen(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// DecodeBodyLen 从字节切片解析 bodyLen
func DecodeBodyLen(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

// Encode 把 Message 编成可以直接写到 TCP 上的字节数组
func Encode(msg *Message) ([]byte, error) {
	return AppendEncoded(nil, msg)
}

// AppendEncoded 把编码结果追加到 dst 并返回扩展后的切片,
// 便于调用方复用缓冲区(见 transport 的写缓冲池), 避免每条消息都分配
func AppendEncoded(dst []byte, msg *Message) ([]byte, error) {
	// 没有 Header 就不知道请求信息, 必须要求不为空
	if msg.Header == nil {
		return nil, fmt.Errorf("protocol: header is nil")
	}

	bodyBytes := msg.Body

	// 压缩 Body(Header 不压缩), 小包直接跳过
	compression := msg.Header.Compression
	if compression != codec.CompressionNone && len(bodyBytes) < MinCompressSize {
		compression = codec.CompressionNone
	}
	if compression != codec.CompressionNone {
		var err error
		bodyBytes, err = codec.Compress(bodyBytes, compression)
		if err != nil {
			return nil, err
		}
	}

	// 实际生效的压缩方式必须写进 Header, 否则对端会错误地尝试解压。
	// 这里编码副本, 不改调用方持有的 Header
	header := *msg.Header
	header.Compression = compression

	// 一次性把整包拼进同一个缓冲区: 先占住固定头的位置, 直接把 Header JSON
	// 与 Body 追加进去, 最后回填两个长度字段, 不需要中间缓冲
	base := len(dst)
	need := HeaderFixedLen + maxHeaderJSONLen(&header) + len(bodyBytes)
	if cap(dst)-base < need {
		grown := make([]byte, base, base+need)
		copy(grown, dst)
		dst = grown
	}

	buf := append(dst, fixedHeaderPlaceholder[:]...)

	buf, err := appendHeaderJSON(buf, &header)
	if err != nil {
		return nil, err
	}
	headerLen := len(buf) - base - HeaderFixedLen

	buf = append(buf, bodyBytes...)

	fixed := buf[base : base+HeaderFixedLen]
	binary.BigEndian.PutUint16(fixed[0:2], Magic)
	binary.BigEndian.PutUint32(fixed[2:6], uint32(headerLen))
	binary.BigEndian.PutUint32(fixed[6:10], uint32(len(bodyBytes)))

	return buf, nil
}

// fixedHeaderPlaceholder 用来先占住固定头的位置, 长度字段最后回填
var fixedHeaderPlaceholder [HeaderFixedLen]byte

// headerJSONOverhead 是 Header JSON 里除三个字符串内容之外的最大长度:
// 字段名与标点约 90 字节, 加上 RequestID(20) 与两个 uint8(各 3)
const headerJSONOverhead = 128

// maxHeaderJSONLen 估算 Header 编码后的上界, 用于一次性预留容量。
// 需要转义时实际会更长, 那种情况下 append 自行扩容, 不影响正确性
func maxHeaderJSONLen(h *Header) int {
	return headerJSONOverhead + len(h.ServiceName) + len(h.MethodName) + len(h.Error)
}

// Decode 把一个完整包的字节数组还原成 Message
func Decode(data []byte) (*Message, error) {
	// 连固定头都不够, 根本没法解析
	if len(data) < HeaderFixedLen {
		return nil, fmt.Errorf("protocol: data too short")
	}

	if binary.BigEndian.Uint16(data[0:2]) != Magic {
		return nil, fmt.Errorf("protocol: invalid magic number")
	}

	headerLen := DecodeHeaderLen(data[2:6])
	bodyLen := DecodeBodyLen(data[6:10])

	totalLen := HeaderFixedLen + int(headerLen) + int(bodyLen)
	// 必须是完整包才处理
	if len(data) < totalLen {
		return nil, fmt.Errorf("protocol: incomplete packet")
	}

	headerBytes := data[HeaderFixedLen : HeaderFixedLen+int(headerLen)]
	var header Header
	if err := parseHeaderJSON(headerBytes, &header); err != nil {
		return nil, err
	}

	bodyBytes := data[HeaderFixedLen+int(headerLen) : totalLen]
	if header.Compression != codec.CompressionNone {
		var err error
		bodyBytes, err = codec.Decompress(bodyBytes, header.Compression)
		if err != nil {
			return nil, err
		}
	}

	return &Message{
		Header: &header,
		Body:   bodyBytes,
	}, nil
}
