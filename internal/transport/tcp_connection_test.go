package transport

import (
	"testing"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
)

func encodeMsg(t *testing.T, id uint64, body string) []byte {
	t.Helper()
	data, err := protocol.Encode(&protocol.Message{
		Header: &protocol.Header{RequestID: id, CodecType: codec.JSON},
		Body:   []byte(body),
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return data
}

// 半包: 分片喂入, 只有攒满一个完整包才切出来
func TestPacketBufferHalfPacket(t *testing.T) {
	pb := &PacketBuffer{}
	data := encodeMsg(t, 1, "hello")

	pb.Write(data[:5])
	if got := pb.Read(); got != nil {
		t.Fatal("expected nil before a full packet arrives")
	}

	pb.Write(data[5 : len(data)-1])
	if got := pb.Read(); got != nil {
		t.Fatal("expected nil while one byte is still missing")
	}

	pb.Write(data[len(data)-1:])
	packet := pb.Read()
	if packet == nil {
		t.Fatal("expected a full packet")
	}

	msg, err := protocol.Decode(packet)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Header.RequestID != 1 || string(msg.Body) != "hello" {
		t.Fatalf("unexpected message: %+v body=%q", msg.Header, msg.Body)
	}
}

// 粘包: 两条消息一次写入, 必须按边界拆成两条
func TestPacketBufferStickyPackets(t *testing.T) {
	pb := &PacketBuffer{}
	pb.Write(append(encodeMsg(t, 1, "aaa"), encodeMsg(t, 2, "bbbb")...))

	for i, want := range []struct {
		id   uint64
		body string
	}{{1, "aaa"}, {2, "bbbb"}} {
		packet := pb.Read()
		if packet == nil {
			t.Fatalf("packet %d: expected a full packet", i)
		}
		msg, err := protocol.Decode(packet)
		if err != nil {
			t.Fatalf("packet %d: Decode: %v", i, err)
		}
		if msg.Header.RequestID != want.id || string(msg.Body) != want.body {
			t.Fatalf("packet %d: got id=%d body=%q", i, msg.Header.RequestID, msg.Body)
		}
	}

	if got := pb.Read(); got != nil {
		t.Fatal("expected buffer to be drained")
	}
}
