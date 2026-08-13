package codec

import (
	"fmt"
	"sync"
)

// Type 序列化类型, 属于协议控制面信息, 由 Header.CodecType 携带给对端
type Type uint8

const (
	// JSON 通用序列化, 无需 IDL, 目前框架默认使用
	JSON Type = iota + 1
	// Protobuf 需要配合 proto 工具生成的代码使用
	Protobuf
)

func (t Type) String() string {
	switch t {
	case JSON:
		return "json"
	case Protobuf:
		return "protobuf"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// Codec 编解码器接口, 框架内部只认 []byte, 业务结构体只在这一层出现
type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	Type() Type
}

// NewCodecFunc 编解码器构造函数
type NewCodecFunc func() Codec

var (
	mu     sync.RWMutex
	codecs = make(map[Type]NewCodecFunc)
)

// Register 注册一种编解码实现, 实现可插拔
func Register(t Type, f NewCodecFunc) {
	mu.Lock()
	defer mu.Unlock()
	codecs[t] = f
}

// New 按类型创建编解码器
func New(t Type) (Codec, error) {
	mu.RLock()
	f, ok := codecs[t]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("codec: unsupported codec type %s", t)
	}
	return f(), nil
}
