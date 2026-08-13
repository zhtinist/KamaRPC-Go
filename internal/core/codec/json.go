package codec

import "encoding/json"

func init() {
	Register(JSON, func() Codec { return JSONCodec{} })
}

// JSONCodec 基于 encoding/json 的实现, 无状态, 可以随用随建
type JSONCodec struct{}

func (JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, v)
}

func (JSONCodec) Type() Type { return JSON }
