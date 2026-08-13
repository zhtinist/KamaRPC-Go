package protocol

import (
	"encoding/json"
	"testing"

	"kamaRPC/internal/core/codec"
)

var headerCases = []Header{
	{},
	{RequestID: 1, ServiceName: "Arith", MethodName: "Add", CodecType: codec.JSON},
	{RequestID: 18446744073709551615, ServiceName: "S", MethodName: "M", Error: "boom", CodecType: 255, Compression: 255},
	// 需要转义: 引号、反斜杠、控制字符、HTML 字符、中文
	{RequestID: 7, Error: `he said "hi"`},
	{RequestID: 8, Error: "back\\slash"},
	{RequestID: 9, Error: "line\nbreak\ttab"},
	{RequestID: 10, ServiceName: "a<b>c&d"},
	{RequestID: 11, Error: "服务不存在"},
	{RequestID: 12, Error: "emoji 🚀"},
}

// 手写编码的产物必须与标准库语义等价: 用标准库解回来应得到同一个 Header
func TestAppendHeaderJSONMatchesStdlib(t *testing.T) {
	for _, want := range headerCases {
		got, err := appendHeaderJSON(nil, &want)
		if err != nil {
			t.Fatalf("appendHeaderJSON(%+v): %v", want, err)
		}

		if !json.Valid(got) {
			t.Fatalf("appendHeaderJSON(%+v) produced invalid JSON: %s", want, got)
		}

		var viaStdlib Header
		if err := json.Unmarshal(got, &viaStdlib); err != nil {
			t.Fatalf("stdlib cannot decode our output %s: %v", got, err)
		}
		if viaStdlib != want {
			t.Fatalf("round trip mismatch:\n got  %+v\n want %+v\n json %s", viaStdlib, want, got)
		}
	}
}

// 反过来: 标准库编出来的字节, 我们的解析器必须解出同样的结果
func TestParseHeaderJSONMatchesStdlib(t *testing.T) {
	for _, want := range headerCases {
		data, err := json.Marshal(&want)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}

		var got Header
		if err := parseHeaderJSON(data, &got); err != nil {
			t.Fatalf("parseHeaderJSON(%s): %v", data, err)
		}
		if got != want {
			t.Fatalf("mismatch:\n got  %+v\n want %+v\n json %s", got, want, data)
		}
	}
}

// 快路径不能吃下畸形输入而给出错误结果: 要么回退标准库解析成功, 要么报错
func TestParseHeaderJSONRejectsMalformed(t *testing.T) {
	for _, data := range []string{
		``, `{`, `}`, `null`, `[]`, `{"RequestID"}`, `{"RequestID":}`,
		`{"RequestID":-1}`, `{"RequestID":1.5}`, `{"RequestID":1}{`,
		`{"ServiceName":"unterminated`, `{"CodecType":99999}`,
	} {
		var h Header
		if err := parseHeaderJSON([]byte(data), &h); err == nil {
			// 没报错的话, 必须与标准库结论一致
			var viaStdlib Header
			if stdErr := json.Unmarshal([]byte(data), &viaStdlib); stdErr != nil {
				t.Fatalf("input %q: we accepted it but stdlib rejected: %v", data, stdErr)
			}
			if h != viaStdlib {
				t.Fatalf("input %q: got %+v, stdlib got %+v", data, h, viaStdlib)
			}
		}
	}
}

// 未知字段与乱序字段应通过回退路径正确处理
func TestParseHeaderJSONUnknownAndReorderedFields(t *testing.T) {
	data := []byte(`{"Compression":1,"Future":{"nested":[1,2]},"ServiceName":"Arith","RequestID":42}`)

	var got Header
	if err := parseHeaderJSON(data, &got); err != nil {
		t.Fatalf("parseHeaderJSON: %v", err)
	}

	want := Header{RequestID: 42, ServiceName: "Arith", Compression: codec.CompressionGzip}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

// FuzzHeaderJSON 用随机输入对比自研实现与标准库, 保证两者判断一致
func FuzzHeaderJSON(f *testing.F) {
	for _, h := range headerCases {
		data, err := json.Marshal(&h)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte(`{"ServiceName":"äö","RequestID":3}`))
	f.Add([]byte(`{ "RequestID" : 5 , "Error" : "x" }`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var got Header
		gotErr := parseHeaderJSON(data, &got)

		var want Header
		wantErr := json.Unmarshal(data, &want)

		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("error mismatch on %q: ours=%v stdlib=%v", data, gotErr, wantErr)
		}
		if gotErr == nil && got != want {
			t.Fatalf("value mismatch on %q: ours=%+v stdlib=%+v", data, got, want)
		}

		if gotErr != nil {
			return
		}

		// 解出来的 Header 再编回去, 必须还能解回同样的值
		encoded, err := appendHeaderJSON(nil, &got)
		if err != nil {
			t.Fatalf("appendHeaderJSON(%+v): %v", got, err)
		}
		var again Header
		if err := json.Unmarshal(encoded, &again); err != nil {
			t.Fatalf("stdlib cannot decode our output %s: %v", encoded, err)
		}
		if again != got {
			t.Fatalf("re-encode mismatch: %+v → %s → %+v", got, encoded, again)
		}
	})
}
