package protocol

import (
	"bytes"
	"testing"

	"kamaRPC/internal/codec"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, compression := range []codec.CompressionType{codec.CompressionNone, codec.CompressionGzip} {
		msg := &Message{
			Header: &Header{
				RequestID:   42,
				ServiceName: "Arith",
				MethodName:  "Add",
				CodecType:   codec.JSON,
				Compression: compression,
			},
			Body: []byte(`{"A":1,"B":2}`),
		}

		data, err := Encode(msg)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}

		got, err := Decode(data)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		if got.Header.RequestID != msg.Header.RequestID ||
			got.Header.ServiceName != msg.Header.ServiceName ||
			got.Header.MethodName != msg.Header.MethodName {
			t.Fatalf("header mismatch: got %+v want %+v", got.Header, msg.Header)
		}
		if !bytes.Equal(got.Body, msg.Body) {
			t.Fatalf("body mismatch: got %q want %q", got.Body, msg.Body)
		}
	}
}

func TestDecodeRejectsBadInput(t *testing.T) {
	if _, err := Decode([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for short data")
	}

	msg := &Message{Header: &Header{RequestID: 1}, Body: []byte("x")}
	data, err := Encode(msg)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// 破坏 magic
	bad := append([]byte(nil), data...)
	bad[0] ^= 0xFF
	if _, err := Decode(bad); err == nil {
		t.Fatal("expected error for invalid magic")
	}

	// 截断包
	if _, err := Decode(data[:len(data)-1]); err == nil {
		t.Fatal("expected error for incomplete packet")
	}
}
