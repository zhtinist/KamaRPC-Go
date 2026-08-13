package server

import (
	"encoding/json"
	"testing"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
	"kamaRPC/pkg/api"
)

// 端到端(不含 etcd 与负载均衡)的调用链路压测:
// client TCPClient → TCP → server Handle → 反射调用 → 响应写回 → Future
func benchSetup(b *testing.B) (*Server, *transport.TCPClient) {
	b.Helper()

	// 压测吞吐会超过服务端默认限流(100000/s), 这里放开, 否则测的是限流器
	srv, addr := startTestServer(b, WithServerRateLimit(50000000))
	c, err := transport.NewTCPClient(addr)
	if err != nil {
		b.Fatalf("NewTCPClient: %v", err)
	}

	b.Cleanup(func() {
		c.Close()
		srv.Stop()
	})
	return srv, c
}

func benchRequest(b *testing.B, a, bb int) *protocol.Message {
	b.Helper()
	body, err := json.Marshal(&api.Args{A: a, B: bb})
	if err != nil {
		b.Fatal(err)
	}
	return &protocol.Message{
		Header: &protocol.Header{
			ServiceName: "Arith",
			MethodName:  "Add",
			CodecType:   codec.JSON,
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}
}

// 串行往返: 反映单次调用的纯延迟
func BenchmarkRPCRoundTrip(b *testing.B) {
	_, c := benchSetup(b)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		future, err := c.SendAsync(benchRequest(b, i, 1))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := future.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}

// 单连接并发往返: 反映多路复用下的吞吐
func BenchmarkRPCParallel(b *testing.B) {
	_, c := benchSetup(b)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future, err := c.SendAsync(benchRequest(b, 1, 2))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := future.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
