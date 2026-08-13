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

// Header 固定用 JSON 编码, 保证两端总能解出控制面信息。
// 编解码器无状态, 建包时复用同一个实例, 避免每条消息都走一次注册表查找
var headerCodec = func() codec.Codec {
	c, err := codec.New(codec.JSON)
	if err != nil {
		panic("protocol: json codec must be registered: " + err.Error())
	}
	return c
}()

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

	headerBytes, err := headerCodec.Marshal(&header)
	if err != nil {
		return nil, err
	}

	headerLen := uint32(len(headerBytes))
	bodyLen := uint32(len(bodyBytes))

	total := HeaderFixedLen + int(headerLen) + int(bodyLen)
	buf := make([]byte, total)

	binary.BigEndian.PutUint16(buf[0:2], Magic)
	binary.BigEndian.PutUint32(buf[2:6], headerLen)
	binary.BigEndian.PutUint32(buf[6:10], bodyLen)
	copy(buf[HeaderFixedLen:], headerBytes)
	copy(buf[HeaderFixedLen+headerLen:], bodyBytes)

	return buf, nil
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
	if err := headerCodec.Unmarshal(headerBytes, &header); err != nil {
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
