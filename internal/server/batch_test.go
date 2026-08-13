package server

import (
	"encoding/json"
	"testing"

	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/protocol"
	"kamaRPC/internal/core/transport"
	"kamaRPC/pkg/api"
)

// 批量发送的每条请求都要能各自正确返回, 且响应按 requestId 归位
func TestSendAsyncBatch(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	defer c.Close()

	const n = 64
	msgs := make([]*protocol.Message, n)
	for i := range msgs {
		body, err := json.Marshal(&api.Args{A: i, B: 1})
		if err != nil {
			t.Fatal(err)
		}
		msgs[i] = &protocol.Message{
			Header: &protocol.Header{
				ServiceName: "Arith", MethodName: "Add",
				CodecType: codec.JSON, Compression: codec.CompressionGzip,
			},
			Body: body,
		}
	}

	futures, err := c.SendAsyncBatch(msgs)
	if err != nil {
		t.Fatalf("SendAsyncBatch: %v", err)
	}
	if len(futures) != n {
		t.Fatalf("got %d futures, want %d", len(futures), n)
	}

	for i, future := range futures {
		reply := &api.Reply{}
		if err := future.GetResult(reply); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if reply.Result != i+1 {
			t.Fatalf("call %d: got %d want %d", i, reply.Result, i+1)
		}
	}
}

// 连接已关闭时批量发送要报错, 而且不能留下永远不会完成的 Future
func TestSendAsyncBatchOnClosedConn(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	c.Close()

	body, _ := json.Marshal(&api.Args{A: 1, B: 2})
	msgs := []*protocol.Message{{
		Header: &protocol.Header{ServiceName: "Arith", MethodName: "Add", CodecType: codec.JSON},
		Body:   body,
	}}

	if _, err := c.SendAsyncBatch(msgs); err == nil {
		t.Fatal("expected an error on a closed connection")
	}
}

func TestSendAsyncBatchEmpty(t *testing.T) {
	srv, addr := startTestServer(t)
	defer srv.Stop()

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	defer c.Close()

	futures, err := c.SendAsyncBatch(nil)
	if err != nil || futures != nil {
		t.Fatalf("empty batch: got (%v, %v)", futures, err)
	}
}
