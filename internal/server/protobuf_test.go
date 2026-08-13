package server

import (
	"encoding/json"
	"testing"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
	"kamaRPC/pkg/api"
	"kamaRPC/pkg/api/pb"
)

// 同一个服务端同时服务 JSON 与 Protobuf 客户端:
// 编解码方式由请求 Header 里的 CodecType 决定, 两端不需要事先约定
func TestServerServesBothCodecs(t *testing.T) {
	srv, addr := startTestServer(t) // 默认 codec 是 JSON
	defer srv.Stop()

	if err := srv.Register("ArithPB", &api.ArithPB{}); err != nil {
		t.Fatalf("Register ArithPB: %v", err)
	}

	c, err := transport.NewTCPClient(addr)
	if err != nil {
		t.Fatalf("NewTCPClient: %v", err)
	}
	defer c.Close()

	// 1) JSON 客户端调 JSON 服务
	jsonBody, err := json.Marshal(&api.Args{A: 3, B: 4})
	if err != nil {
		t.Fatal(err)
	}
	jsonFuture, err := c.SendAsync(&protocol.Message{
		Header: &protocol.Header{
			ServiceName: "Arith", MethodName: "Add",
			CodecType: codec.JSON, Compression: codec.CompressionGzip,
		},
		Body: jsonBody,
	})
	if err != nil {
		t.Fatalf("SendAsync(json): %v", err)
	}

	// 2) Protobuf 客户端调 Protobuf 服务, 走同一条连接
	pbBody, err := proto.Marshal(&pb.Args{A: 10, B: 32})
	if err != nil {
		t.Fatal(err)
	}
	pbFuture, err := c.SendAsync(&protocol.Message{
		Header: &protocol.Header{
			ServiceName: "ArithPB", MethodName: "Add",
			CodecType: codec.Protobuf, Compression: codec.CompressionGzip,
		},
		Body: pbBody,
	})
	if err != nil {
		t.Fatalf("SendAsync(proto): %v", err)
	}

	jsonRes, err := jsonFuture.Wait()
	if err != nil {
		t.Fatalf("json call: %v", err)
	}
	var jsonReply api.Reply
	if err := json.Unmarshal(jsonRes, &jsonReply); err != nil {
		t.Fatalf("json reply: %v", err)
	}
	if jsonReply.Result != 7 {
		t.Fatalf("json Arith.Add(3,4) = %d, want 7", jsonReply.Result)
	}

	pbRes, err := pbFuture.Wait()
	if err != nil {
		t.Fatalf("proto call: %v", err)
	}
	var pbReply pb.Reply
	if err := proto.Unmarshal(pbRes, &pbReply); err != nil {
		t.Fatalf("proto reply: %v", err)
	}
	if pbReply.Result != 42 {
		t.Fatalf("proto ArithPB.Add(10,32) = %d, want 42", pbReply.Result)
	}
}

// 记录两种编码下的实际包体积: Body 与整包分别多大
func TestCodecPacketSizes(t *testing.T) {
	jsonBody, err := json.Marshal(&api.Args{A: 1, B: 2})
	if err != nil {
		t.Fatal(err)
	}
	pbBody, err := proto.Marshal(&pb.Args{A: 1, B: 2})
	if err != nil {
		t.Fatal(err)
	}

	pack := func(service string, body []byte, ct codec.Type) int {
		data, err := protocol.Encode(&protocol.Message{
			Header: &protocol.Header{
				RequestID: 1, ServiceName: service, MethodName: "Add",
				CodecType: ct, Compression: codec.CompressionGzip,
			},
			Body: body,
		})
		if err != nil {
			t.Fatal(err)
		}
		return len(data)
	}

	jsonTotal := pack("Arith", jsonBody, codec.JSON)
	pbTotal := pack("ArithPB", pbBody, codec.Protobuf)

	t.Logf("Body:  JSON %d 字节, Protobuf %d 字节", len(jsonBody), len(pbBody))
	t.Logf("整包:  JSON %d 字节, Protobuf %d 字节 (固定头 %d + 二进制 Header)",
		jsonTotal, pbTotal, protocol.HeaderFixedLen)

	if len(pbBody) >= len(jsonBody) {
		t.Fatalf("protobuf body 应该更小: %d vs %d", len(pbBody), len(jsonBody))
	}

	// 解释为什么小包场景 Protobuf 端到端反而不占优: 生成结构体自带
	// protoimpl 状态, 服务端每个请求都要 reflect.New 一个更大的对象
	t.Logf("结构体大小: api.Args %d 字节, pb.Args %d 字节",
		unsafe.Sizeof(api.Args{}), unsafe.Sizeof(pb.Args{}))
}
