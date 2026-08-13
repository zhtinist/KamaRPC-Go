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

// 小包跳过压缩时, Header 必须如实记录 CompressionNone, 否则对端会去解压明文
func TestEncodeSkipsCompressionForSmallBody(t *testing.T) {
	small := []byte(`{"A":1,"B":2}`)
	if len(small) >= MinCompressSize {
		t.Fatalf("test body should be below %d bytes", MinCompressSize)
	}

	data, err := Encode(&Message{
		Header: &Header{RequestID: 1, CodecType: codec.JSON, Compression: codec.CompressionGzip},
		Body:   small,
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Header.Compression != codec.CompressionNone {
		t.Fatalf("small body should not be compressed, got %d", got.Header.Compression)
	}
	if !bytes.Equal(got.Body, small) {
		t.Fatalf("body mismatch: got %q want %q", got.Body, small)
	}

	// 调用方持有的 Header 不应被 Encode 改写
	header := &Header{RequestID: 2, Compression: codec.CompressionGzip}
	if _, err := Encode(&Message{Header: header, Body: small}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if header.Compression != codec.CompressionGzip {
		t.Fatal("Encode must not mutate the caller's header")
	}
}

// 超过阈值的 Body 仍然走压缩, 且必须能原样解回
func TestEncodeCompressesLargeBody(t *testing.T) {
	large := bytes.Repeat([]byte("kamaRPC"), MinCompressSize)

	data, err := Encode(&Message{
		Header: &Header{RequestID: 1, CodecType: codec.JSON, Compression: codec.CompressionGzip},
		Body:   large,
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(data) >= len(large) {
		t.Fatalf("expected compression to shrink the packet: %d >= %d", len(data), len(large))
	}

	got, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Header.Compression != codec.CompressionGzip {
		t.Fatalf("large body should stay compressed, got %d", got.Header.Compression)
	}
	if !bytes.Equal(got.Body, large) {
		t.Fatal("body mismatch after decompression")
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
