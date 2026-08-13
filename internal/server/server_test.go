package server

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
	"kamaRPC/pkg/api"
)

// startTestServer 起一个不依赖 etcd 的服务端, 直接用 transport 层驱动调用链路
func startTestServer(t testing.TB, opts ...ServerOption) (*Server, string) {
	t.Helper()

	srv, err := NewServer("127.0.0.1:0", append([]ServerOption{WithServerCodec(codec.JSON)}, opts...)...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := srv.Register("Arith", &api.Arith{}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 先占端口再 Start, 避免测试里拿不到真实端口
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv.addr = addr
	go func() {
		_ = srv.Start()
	}()

	// 等待监听就绪
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return srv, addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not start on %s", addr)
	return nil, ""
}

func request(t *testing.T, service, method string, args *api.Args) *protocol.Message {
	t.Helper()
	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal: %v", err)
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

func TestServerReflectiveCall(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	defer c.Close()

	future, err := c.SendAsync(request(t, "Arith", "Add", &api.Args{A: 3, B: 4}))
	if err != nil {
		t.Fatalf("SendAsync: %v", err)
	}

	reply := &api.Reply{}
	if err := future.GetResult(reply); err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if reply.Result != 7 {
		t.Fatalf("Arith.Add(3,4) = %d, want 7", reply.Result)
	}
}

// 单连接上并发多个请求, 响应靠 requestId 归位
func TestServerConcurrentMultiplexing(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	defer c.Close()

	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		future, err := c.SendAsync(request(t, "Arith", "Mul", &api.Args{A: i, B: 2}))
		if err != nil {
			t.Fatalf("SendAsync: %v", err)
		}

		wg.Add(1)
		go func(want int, f *transport.Future) {
			defer wg.Done()
			reply := &api.Reply{}
			if err := f.GetResult(reply); err != nil {
				errs <- err
				return
			}
			if reply.Result != want {
				t.Errorf("got %d want %d", reply.Result, want)
			}
		}(i*2, future)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("call failed: %v", err)
	}
}

func TestServerErrorsTravelInHeader(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	defer c.Close()

	cases := []struct {
		name    string
		service string
		method  string
		args    *api.Args
	}{
		{"business error", "Arith", "Div", &api.Args{A: 1, B: 0}},
		{"unknown method", "Arith", "Nope", &api.Args{A: 1, B: 1}},
		{"unknown service", "Nope", "Add", &api.Args{A: 1, B: 1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			future, err := c.SendAsync(request(t, tc.service, tc.method, tc.args))
			if err != nil {
				t.Fatalf("SendAsync: %v", err)
			}
			if _, err := future.Wait(); err == nil {
				t.Fatal("expected an error from the server")
			}
		})
	}
}
