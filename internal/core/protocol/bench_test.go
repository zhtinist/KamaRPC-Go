package protocol

import (
	"testing"

	"kamaRPC/internal/core/codec"
)

// 典型 RPC 小包: JSON 序列化后的 {"A":1,"B":2} 量级
var smallBody = []byte(`{"A":1,"B":2}`)

func benchRoundTrip(b *testing.B, compression codec.CompressionType, body []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))

	for i := 0; i < b.N; i++ {
		msg := &Message{
			Header: &Header{
				RequestID:   uint64(i),
				ServiceName: "Arith",
				MethodName:  "Add",
				CodecType:   codec.JSON,
				Compression: compression,
			},
			Body: body,
		}

		data, err := Encode(msg)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoundTripSmallGzip(b *testing.B) {
	benchRoundTrip(b, codec.CompressionGzip, smallBody)
}

func BenchmarkRoundTripSmallNone(b *testing.B) {
	benchRoundTrip(b, codec.CompressionNone, smallBody)
}

func BenchmarkRoundTripLargeGzip(b *testing.B) {
	body := make([]byte, 0, 8192)
	for len(body) < 8192 {
		body = append(body, smallBody...)
	}
	benchRoundTrip(b, codec.CompressionGzip, body)
}
