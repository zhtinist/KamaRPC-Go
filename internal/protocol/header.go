package protocol

import "kamaRPC/internal/codec"

// Magic 魔数, 用于快速识别非法包
const Magic uint16 = 0x4B52 // "KR"

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
