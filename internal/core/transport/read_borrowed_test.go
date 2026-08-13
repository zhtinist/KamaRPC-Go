package transport

import (
	"net"
	"strconv"
	"testing"

	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/protocol"
)

// 多条消息一次写入(流水线), 借用式读取必须逐条给出正确内容 ——
// 读偏移或复位逻辑写错的话, 这里会读到串味或截断的数据
func TestReadBorrowedPipelined(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	sender := NewTCPConnection(client)
	receiver := NewTCPConnection(server)

	const n = 50
	go func() {
		var buf []byte
		for i := 0; i < n; i++ {
			msg := &protocol.Message{
				Header: &protocol.Header{
					RequestID:   uint64(i),
					ServiceName: "Arith",
					MethodName:  "Add",
					CodecType:   codec.JSON,
				},
				Body: []byte("body-" + strconv.Itoa(i)),
			}
			var err error
			if buf, err = protocol.AppendEncoded(buf, msg); err != nil {
				t.Errorf("encode: %v", err)
				return
			}
		}
		// 一次写出全部, 让接收端缓冲区里同时躺着多个完整包
		if err := sender.WriteRaw(buf); err != nil {
			t.Errorf("write: %v", err)
		}
	}()

	for i := 0; i < n; i++ {
		msg, err := receiver.ReadBorrowed()
		if err != nil {
			t.Fatalf("消息 %d: %v", i, err)
		}
		if msg.Header.RequestID != uint64(i) {
			t.Fatalf("消息 %d: RequestID = %d", i, msg.Header.RequestID)
		}
		if want := "body-" + strconv.Itoa(i); string(msg.Body) != want {
			t.Fatalf("消息 %d: body = %q want %q", i, msg.Body, want)
		}
		if msg.Header.ServiceName != "Arith" || msg.Header.MethodName != "Add" {
			t.Fatalf("消息 %d: header = %+v", i, msg.Header)
		}
	}
}

// 借用读与拷贝读必须给出一致的结果
func TestReadBorrowedMatchesRead(t *testing.T) {
	msg := &protocol.Message{
		Header: &protocol.Header{
			RequestID: 42, ServiceName: "Arith", MethodName: "Div",
			Error: "divide by zero", CodecType: codec.JSON,
		},
		Body: []byte(`{"A":1,"B":0}`),
	}

	for _, borrowed := range []bool{false, true} {
		client, server := net.Pipe()
		sender := NewTCPConnection(client)
		receiver := NewTCPConnection(server)

		go func() {
			if err := sender.Write(msg); err != nil {
				t.Errorf("write: %v", err)
			}
		}()

		var (
			got *protocol.Message
			err error
		)
		if borrowed {
			got, err = receiver.ReadBorrowed()
		} else {
			got, err = receiver.Read()
		}
		if err != nil {
			t.Fatalf("borrowed=%v: %v", borrowed, err)
		}
		if *got.Header != *msg.Header || string(got.Body) != string(msg.Body) {
			t.Fatalf("borrowed=%v: got %+v %q", borrowed, got.Header, got.Body)
		}

		client.Close()
		server.Close()
	}
}
