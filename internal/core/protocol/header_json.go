package protocol

import (
	"encoding/json"
	"strconv"

	"kamaRPC/internal/core/codec"
)

// Header 的手写 JSON 编解码。
//
// 线上格式仍然是 JSON, 字段名与 encoding/json 的默认行为完全一致, 因此与用
// 标准库实现的对端可以互通; 这里只是绕开反射与通用扫描器带来的开销 ——
// profile 显示 encoding/json 在小包场景占了协议层大部分 CPU 与分配。
//
// 设计原则: 只对可以确定处理正确的输入走快路径, 其余一律回退标准库,
// 把正确性风险限制在"要么快, 要么退回已验证实现"。

// safeJSONByte 标记可以原样放进 JSON 字符串的字节。
// 排除引号、反斜杠、控制字符, 以及 encoding/json 默认会转义的 < > &;
// 0x80 以上一律视为不安全(多字节 UTF-8、U+2028/2029 等交给标准库处理)
var safeJSONByte = func() [256]bool {
	var t [256]bool
	for c := 0x20; c < 0x80; c++ {
		switch c {
		case '"', '\\', '<', '>', '&':
		default:
			t[c] = true
		}
	}
	return t
}()

func stringNeedsEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		if !safeJSONByte[s[i]] {
			return true
		}
	}
	return false
}

// appendHeaderJSON 把 Header 序列化成 JSON 追加到 dst。
// 出现需要转义的字符串时回退标准库, 保证转义规则与 encoding/json 一致
func appendHeaderJSON(dst []byte, h *Header) ([]byte, error) {
	if stringNeedsEscape(h.ServiceName) || stringNeedsEscape(h.MethodName) || stringNeedsEscape(h.Error) {
		b, err := json.Marshal(h)
		if err != nil {
			return nil, err
		}
		return append(dst, b...), nil
	}

	dst = append(dst, `{"RequestID":`...)
	dst = strconv.AppendUint(dst, h.RequestID, 10)

	dst = append(dst, `,"ServiceName":"`...)
	dst = append(dst, h.ServiceName...)

	dst = append(dst, `","MethodName":"`...)
	dst = append(dst, h.MethodName...)

	dst = append(dst, `","Error":"`...)
	dst = append(dst, h.Error...)

	dst = append(dst, `","CodecType":`...)
	dst = strconv.AppendUint(dst, uint64(h.CodecType), 10)

	dst = append(dst, `,"Compression":`...)
	dst = strconv.AppendUint(dst, uint64(h.Compression), 10)

	return append(dst, '}'), nil
}

// parseHeaderJSON 解析 Header。快路径处理本框架自己产出的形状,
// 遇到任何非预期结构(转义、嵌套、未知类型)就回退标准库。
//
// 回退直接用 encoding/json 而不是 Body 用的那个 JSONCodec: 后者把空输入
// 当成合法的空对象(响应体可以为空), 但 Header 为空一定是坏包, 不能放过
func parseHeaderJSON(data []byte, h *Header) error {
	if parseHeaderFast(data, h) {
		return nil
	}
	*h = Header{}
	return json.Unmarshal(data, h)
}

func skipSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

// parseHeaderFast 成功返回 true; 返回 false 表示交给标准库处理,
// 此时不保证 h 的内容, 调用方会先清零再重新解析
func parseHeaderFast(data []byte, h *Header) bool {
	i := skipSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return false
	}
	i++

	*h = Header{}

	i = skipSpace(data, i)
	if i < len(data) && data[i] == '}' {
		return skipSpace(data, i+1) == len(data)
	}

	for {
		i = skipSpace(data, i)

		key, next, ok := parseRawBytes(data, i)
		if !ok {
			return false
		}
		i = skipSpace(data, next)
		if i >= len(data) || data[i] != ':' {
			return false
		}
		i = skipSpace(data, i+1)

		switch string(key) { // 编译器不会为这种 switch 分配字符串
		case "ServiceName":
			b, next, ok := parseRawBytes(data, i)
			if !ok {
				return false
			}
			// 服务名取值集合很小, 驻留复用避免每个请求都分配
			h.ServiceName, i = internBytes(b), next

		case "MethodName":
			b, next, ok := parseRawBytes(data, i)
			if !ok {
				return false
			}
			h.MethodName, i = internBytes(b), next

		case "Error":
			b, next, ok := parseRawBytes(data, i)
			if !ok {
				return false
			}
			// 错误信息是任意文本, 不驻留, 免得把缓存撑爆
			h.Error, i = string(b), next

		case "RequestID":
			v, next, ok := parseUint(data, i)
			if !ok {
				return false
			}
			h.RequestID, i = v, next

		case "CodecType":
			v, next, ok := parseUint(data, i)
			if !ok || v > 255 {
				return false
			}
			h.CodecType, i = codec.Type(v), next

		case "Compression":
			v, next, ok := parseUint(data, i)
			if !ok || v > 255 {
				return false
			}
			h.Compression, i = codec.CompressionType(v), next

		default:
			// 未知字段: 交给标准库, 避免自己实现通用跳过
			return false
		}

		i = skipSpace(data, i)
		if i >= len(data) {
			return false
		}
		switch data[i] {
		case ',':
			i++
		case '}':
			// 尾部只允许空白
			return skipSpace(data, i+1) == len(data)
		default:
			return false
		}
	}
}

// parseRawBytes 解析不含转义的 JSON 字符串, 返回指向原缓冲的字节切片;
// 含转义或非 ASCII 时返回 false, 交给标准库处理。
// 返回的切片只在本次解析期间有效, 调用方负责转成字符串
func parseRawBytes(data []byte, i int) ([]byte, int, bool) {
	if i >= len(data) || data[i] != '"' {
		return nil, i, false
	}
	i++

	start := i
	for i < len(data) {
		c := data[i]
		switch {
		case c == '"':
			return data[start:i], i + 1, true
		case c == '\\' || c < 0x20 || c >= 0x80:
			return nil, i, false
		}
		i++
	}
	return nil, i, false
}

// parseUint 解析非负整数, 其他数字形式(负号/小数/指数)返回 false
func parseUint(data []byte, i int) (uint64, int, bool) {
	start := i
	for i < len(data) && data[i] >= '0' && data[i] <= '9' {
		i++
	}
	if i == start {
		return 0, i, false
	}
	// JSON 不允许前导零("0" 本身合法, "00"/"01" 非法),
	// strconv.ParseUint 会接受, 所以这里必须自己挡掉, 否则与标准库判断不一致
	if i-start > 1 && data[start] == '0' {
		return 0, i, false
	}
	// 数字后面紧跟小数点或指数, 说明不是整数, 交给标准库
	if i < len(data) {
		switch data[i] {
		case '.', 'e', 'E':
			return 0, i, false
		}
	}

	v, err := strconv.ParseUint(string(data[start:i]), 10, 64)
	if err != nil {
		return 0, i, false
	}
	return v, i, true
}
