package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func init() {
	Register(Protobuf, func() Codec { return ProtobufCodec{} })
}

// ProtobufCodec 只能处理 proto 工具生成的 IDL 代码,
// 即实现了 proto.Message 的结构体, 其余类型直接报错
type ProtobufCodec struct{}

func (ProtobufCodec) Marshal(v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("codec: %T is not a proto.Message", v)
	}
	return proto.Marshal(msg)
}

func (ProtobufCodec) Unmarshal(data []byte, v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("codec: %T is not a proto.Message", v)
	}
	if len(data) == 0 {
		return nil
	}
	return proto.Unmarshal(data, msg)
}

func (ProtobufCodec) Type() Type { return Protobuf }
