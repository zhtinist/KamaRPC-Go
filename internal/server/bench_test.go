package server

import (
	"encoding/json"
	"testing"
	"time"

	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/protocol"
	"kamaRPC/internal/core/transport"
	"kamaRPC/pkg/api"
)

// slowService 模拟真实业务方法(查库/调下游)的耗时,
// 用来暴露"服务端串行处理单连接请求"对吞吐的影响
type slowService struct {
	d time.Duration
}

func (s *slowService) Work(args *api.Args, reply *api.Reply) error {
	time.Sleep(s.d)
	reply.Result = args.A + args.B
	return nil
}

// 端到端(不含 etcd 与负载均衡)的调用链路压测:
// client TCPClient → TCP → server Handle → 反射调用 → 响应写回 → Future
func benchSetup(b *testing.B) (*Server, *transport.TCPClient) {
	b.Helper()

	// 压测吞吐会超过服务端默认限流(100000/s), 这里放开, 否则测的是限流器
	srv, addr := startTestServer(b, WithServerRateLimit(50000000))
	if err := srv.Register("Slow", &slowService{d: time.Millisecond}); err != nil {
		b.Fatalf("Register: %v", err)
	}

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
	return serviceRequest(b, "Arith", "Add", a, bb)
}

func serviceRequest(b *testing.B, service, method string, a, bb int) *protocol.Message {
	b.Helper()
	body, err := json.Marshal(&api.Args{A: a, B: bb})
	if err != nil {
		b.Fatal(err)
	}
	return &protocol.Message{
		Header: &protocol.Header{
			ServiceName: service,
			MethodName:  method,
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

// 单连接上并发调用一个耗时 1ms 的业务方法:
// 服务端若串行处理, 单连接吞吐会被死死压在 1000 QPS 上,
// 客户端的多路复用能力等于白费
func BenchmarkRPCParallelSlowHandler(b *testing.B) {
	_, c := benchSetup(b)
	b.SetParallelism(32)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			future, err := c.SendAsync(serviceRequest(b, "Slow", "Work", 1, 2))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := future.Wait(); err != nil {
				b.Fatal(err)
			}
		}
	})
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
