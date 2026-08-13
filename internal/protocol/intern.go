package protocol

import "sync"

// 服务名与方法名的字符串驻留。
//
// Header 解析时 ServiceName / MethodName 每次都要从字节切片转成 string,
// 每个请求两次分配, profile 里占了协议层分配对象数的两成。而这两个字段的
// 取值集合很小(就那么几个服务方法), 所以把已见过的字符串缓存复用。
//
// 关键点: 用内建 map 而不是 sync.Map —— 编译器会把 m[string(b)] 这种查找
// 优化成不分配字符串的形式, sync.Map 走接口参数拿不到这个优化。
//
// 上限用来防止对端构造大量随机服务名把内存吃掉: 超过上限就退化成直接分配。
const maxInternedStrings = 1024

var (
	internMu sync.RWMutex
	interned = make(map[string]string, 64)
)

// internBytes 返回与 b 内容相同的字符串, 尽量复用已缓存的那一份
func internBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	internMu.RLock()
	if s, ok := interned[string(b)]; ok { // 这里不会分配
		internMu.RUnlock()
		return s
	}
	internMu.RUnlock()

	s := string(b)

	internMu.Lock()
	if len(interned) < maxInternedStrings {
		interned[s] = s
	}
	internMu.Unlock()

	return s
}
