package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"kamaRPC/internal/core/codec"
)

// 两种 Header 编码必须能互相认: 新端发的包老端认不出没关系(老端不认新 Magic),
// 但新端一定要认得老端发来的 JSON Header 包, 否则灰度升级时会全线中断
func TestDecodeAcceptsBothHeaderVersions(t *testing.T) {
	for _, tc := range headerCases {
		msg := &Message{Header: &tc, Body: []byte("payload")}

		v1, err := AppendEncodedJSONHeader(nil, msg)
		if err != nil {
			t.Fatalf("v1 encode %+v: %v", tc, err)
		}
		v2, err := AppendEncoded(nil, msg)
		if err != nil {
			t.Fatalf("v2 encode %+v: %v", tc, err)
		}

		if got := binary.BigEndian.Uint16(v1[0:2]); got != MagicJSONHeader {
			t.Fatalf("v1 magic = %#x", got)
		}
		if got := binary.BigEndian.Uint16(v2[0:2]); got != MagicBinaryHeader {
			t.Fatalf("v2 magic = %#x", got)
		}

		for name, data := range map[string][]byte{"v1": v1, "v2": v2} {
			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("%s decode %+v: %v", name, tc, err)
			}
			// Compression 会被小包免压逻辑改写, 单独比其余字段
			want := tc
			want.Compression = decoded.Header.Compression
			if *decoded.Header != want {
				t.Fatalf("%s: got %+v want %+v", name, *decoded.Header, want)
			}
			if !bytes.Equal(decoded.Body, []byte("payload")) {
				t.Fatalf("%s: body mismatch", name)
			}
		}
	}
}

// 二进制 Header 应该显著小于 JSON Header
func TestBinaryHeaderIsSmaller(t *testing.T) {
	msg := &Message{
		Header: &Header{
			RequestID: 12345, ServiceName: "Arith", MethodName: "Add",
			CodecType: codec.JSON, Compression: codec.CompressionGzip,
		},
		Body: []byte(`{"A":1,"B":2}`),
	}

	v1, err := AppendEncodedJSONHeader(nil, msg)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := AppendEncoded(nil, msg)
	if err != nil {
		t.Fatal(err)
	}

	v1Header := len(v1) - HeaderFixedLen - len(msg.Body)
	v2Header := len(v2) - HeaderFixedLen - len(msg.Body)
	t.Logf("Header: JSON %d 字节 → 二进制 %d 字节; 整包: %d → %d 字节",
		v1Header, v2Header, len(v1), len(v2))

	if v2Header >= v1Header {
		t.Fatalf("二进制 Header 应该更小: %d vs %d", v2Header, v1Header)
	}
}

// 尾部追加未知字段时, 老版本解析器应该忽略而不是报错(向前兼容的扩展方式)
func TestBinaryHeaderIgnoresTrailingBytes(t *testing.T) {
	h := Header{RequestID: 9, ServiceName: "S", MethodName: "M", CodecType: codec.JSON}

	data := appendHeaderBinary(nil, &h)
	data = append(data, 0xDE, 0xAD, 0xBE, 0xEF) // 假装是新版本追加的字段

	var got Header
	if err := parseHeaderBinary(data, &got); err != nil {
		t.Fatalf("parseHeaderBinary: %v", err)
	}
	if got != h {
		t.Fatalf("got %+v want %+v", got, h)
	}
}

func TestBinaryHeaderRejectsTruncated(t *testing.T) {
	h := Header{RequestID: 7, ServiceName: "Arith", MethodName: "Add", Error: "boom"}
	full := appendHeaderBinary(nil, &h)

	for i := 0; i < len(full); i++ {
		var got Header
		if err := parseHeaderBinary(full[:i], &got); err == nil {
			t.Fatalf("截断到 %d 字节仍被接受", i)
		}
	}
}

// 坏包不能让解析器 panic 或越界, 也不能凭空造出巨大的字符串
func FuzzParseHeaderBinary(f *testing.F) {
	for _, h := range headerCases {
		f.Add(appendHeaderBinary(nil, &h))
	}
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var h Header
		if err := parseHeaderBinary(data, &h); err != nil {
			return
		}
		// 解析成功时, 各字符串长度必须落在输入范围内
		if len(h.ServiceName)+len(h.MethodName)+len(h.Error) > len(data) {
			t.Fatalf("解析出的字符串总长 %d 超过输入 %d 字节",
				len(h.ServiceName)+len(h.MethodName)+len(h.Error), len(data))
		}
		// 再编回去必须能解回同样的值
		again := appendHeaderBinary(nil, &h)
		var back Header
		if err := parseHeaderBinary(again, &back); err != nil {
			t.Fatalf("重新解析失败: %v", err)
		}
		if back != h {
			t.Fatalf("往返不一致: %+v → %+v", h, back)
		}
	})
}
