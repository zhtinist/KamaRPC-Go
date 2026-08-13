package codec_test

import (
	"testing"

	"kamaRPC/internal/codec"
	"kamaRPC/pkg/api"
	"kamaRPC/pkg/api/pb"
)

// 纯编解码开销对比: 不含网络, 因此没有机器漂移干扰
func benchCodec(b *testing.B, t codec.Type, args interface{}, newReply func() interface{}) {
	b.Helper()
	c, err := codec.New(t)
	if err != nil {
		b.Fatal(err)
	}

	data, err := c.Marshal(args)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(data)), "body-bytes")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := c.Marshal(args)
		if err != nil {
			b.Fatal(err)
		}
		if err := c.Unmarshal(out, newReply()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecJSON(b *testing.B) {
	benchCodec(b, codec.JSON, &api.Args{A: 1, B: 2}, func() interface{} { return &api.Args{} })
}

func BenchmarkCodecProtobuf(b *testing.B) {
	benchCodec(b, codec.Protobuf, &pb.Args{A: 1, B: 2}, func() interface{} { return &pb.Args{} })
}

// 大包场景: 200 个整数 + 1KB 文本
func bigPayload() ([]int64, string) {
	values := make([]int64, 200)
	for i := range values {
		values[i] = int64(i) * 7919
	}
	text := ""
	for len(text) < 1024 {
		text += "kamaRPC-payload-"
	}
	return values, text
}

func BenchmarkCodecJSONBig(b *testing.B) {
	values, text := bigPayload()
	benchCodec(b, codec.JSON, &api.Big{Values: values, Text: text},
		func() interface{} { return &api.Big{} })
}

func BenchmarkCodecProtobufBig(b *testing.B) {
	values, text := bigPayload()
	benchCodec(b, codec.Protobuf, &pb.Big{Values: values, Text: text},
		func() interface{} { return &pb.Big{} })
}
