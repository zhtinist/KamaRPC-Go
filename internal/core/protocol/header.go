package protocol

import "kamaRPC/internal/core/codec"

// Magic 魔数, 用于快速识别非法包。
//
// 两个取值区分 Header 的编码方式: 收包时按 Magic 分派, 因此新旧两端可以互通。
// 分帧字段(headerLen/bodyLen)布局与 Magic 无关, 拆包逻辑对两者通用
const (
	// MagicJSONHeader 协议 v1: Header 用 JSON 编码(教程原始设计)
	MagicJSONHeader uint16 = 0x4B52 // "KR"
	// MagicBinaryHeader 协议 v2: Header 用二进制编码, 见 header_binary.go
	MagicBinaryHeader uint16 = 0x4B53 // "KS"
)

// Magic 保留旧名字, 指向 v1, 便于老代码与文档对照
const Magic = MagicJSONHeader

// HeaderFixedLen 固定头长度: Magic(2) + headerLen(4) + bodyLen(4)
const HeaderFixedLen = 10

// CodecType 复用 codec.Type, 保证协议层与编解码层只有一份类型定义
type CodecType = codec.Type

// Header 协议控制面: 路由信息 + 错误 + 编解码/压缩方式
type Header struct {
	RequestID   uint64                // 多路复用的核心键
	ServiceName string                // 服务名, 服务端据此找到注册的实例
	MethodName  string                // 方法名, 服务端据此反射调用
	Error       string                // 服务端错误, 不与业务结构体耦合
	CodecType   CodecType             // Body 的序列化方式
	Compression codec.CompressionType // Body 的压缩方式
}
