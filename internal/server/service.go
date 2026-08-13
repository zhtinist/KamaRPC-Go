package server

import (
	"fmt"
	"reflect"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// methodEntry 一个可被远程调用的方法, 反射信息在注册时就解析好,
// 调用时只需查表 + 构造参数, 不再每个请求都 MethodByName 与校验签名
type methodEntry struct {
	fn        reflect.Value // 已绑定接收者的方法
	reqType   reflect.Type  // 请求结构体类型(指针的元素类型)
	replyType reflect.Type  // 响应结构体类型(指针的元素类型)
}

// serviceEntry 一个注册的服务及其方法表
type serviceEntry struct {
	name    string
	methods map[string]*methodEntry
}

// newServiceEntry 扫描实例上所有导出方法, 收集符合 net/rpc 风格签名的方法:
//
//	func(req *Req, reply *Resp) error
//
// 签名不符的方法直接跳过; 一个方法都没有说明用法有误, 在注册时就报错,
// 而不是等到线上调用才返回 method not found
func newServiceEntry(name string, service interface{}) (*serviceEntry, error) {
	value := reflect.ValueOf(service)
	if !value.IsValid() {
		return nil, fmt.Errorf("server: service %s is nil", name)
	}

	entry := &serviceEntry{
		name:    name,
		methods: make(map[string]*methodEntry),
	}

	t := value.Type()
	for i := 0; i < t.NumMethod(); i++ {
		methodName := t.Method(i).Name
		fn := value.Method(i)

		req, reply, ok := parseSignature(fn.Type())
		if !ok {
			continue
		}

		entry.methods[methodName] = &methodEntry{
			fn:        fn,
			reqType:   req,
			replyType: reply,
		}
	}

	if len(entry.methods) == 0 {
		return nil, fmt.Errorf(
			"server: service %s (%T) has no method with signature func(req *Req, reply *Resp) error",
			name, service)
	}
	return entry, nil
}

// parseSignature 校验方法签名并返回请求/响应的元素类型
func parseSignature(fnType reflect.Type) (req, reply reflect.Type, ok bool) {
	if fnType.NumIn() != 2 || fnType.NumOut() != 1 {
		return nil, nil, false
	}
	if fnType.In(0).Kind() != reflect.Ptr || fnType.In(1).Kind() != reflect.Ptr {
		return nil, nil, false
	}
	if !fnType.Out(0).Implements(errorType) {
		return nil, nil, false
	}
	return fnType.In(0).Elem(), fnType.In(1).Elem(), true
}

// lookup 按方法名取方法
func (e *serviceEntry) lookup(method string) (*methodEntry, bool) {
	m, ok := e.methods[method]
	return m, ok
}

// MethodNames 返回已注册的方法名, 便于排查与展示
func (e *serviceEntry) MethodNames() []string {
	names := make([]string, 0, len(e.methods))
	for name := range e.methods {
		names = append(names, name)
	}
	return names
}
