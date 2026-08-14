package server

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/protocol"
	"kamaRPC/internal/core/transport"
)

// Handler 负责请求 Body 反序列化 + 反射调用 + 响应组装,
// 与 Server 的网络/连接管理职责分离
type Handler struct {
	codec codec.Codec
}

// NewHandler 创建请求处理器
func NewHandler(c codec.Codec) *Handler {
	return &Handler{codec: c}
}

// Process 调用本地方法并把结果写回对端, 返回这次调用是否失败
func (h *Handler) Process(conn *transport.TCPConnection, msg *protocol.Message, service *serviceEntry) bool {
	bufp := respBufPool.Get().(*[]byte)
	buf, failed := h.AppendResponse((*bufp)[:0], msg, service)
	*bufp = buf

	if err := conn.WriteRaw(buf); err != nil {
		log.Println("write response error:", err)
		failed = true
	}

	if cap(*bufp) <= maxPooledRespBuf {
		respBufPool.Put(bufp)
	}
	return failed
}

// maxPooledRespBuf 超过这个容量的响应缓冲不再回池
const maxPooledRespBuf = 64 << 10

var respBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// codecFor 按请求声明的 CodecType 选择编解码器, 未声明或不认识时用服务端默认值。
//
// 这样一个服务端可以同时服务 JSON 与 Protobuf 客户端, 不需要两端事先约定;
// 响应也用同一种编码回去, 客户端才解得开
func (h *Handler) codecFor(t codec.Type) codec.Codec {
	if t != 0 {
		if c, ok := codec.Get(t); ok {
			return c
		}
	}
	return h.codec
}

// AppendResponse 调用本地方法并把编码好的响应追加到 dst。
//
// 与直接写回相比, 这样可以把多个流水线请求的响应攒在一起、一次写出,
// 把 N 次 write 系统调用压成 1 次
// 返回值 failed 表示这次调用是否以错误告终, 供指标采集使用
func (h *Handler) AppendResponse(dst []byte, msg *protocol.Message, service *serviceEntry) ([]byte, bool) {
	c := h.codecFor(msg.Header.CodecType)

	result, err := h.invoke(c, service, msg.Header.MethodName, msg.Body)
	if err != nil {
		return AppendErrorResponse(dst, msg.Header.RequestID, err.Error()), true
	}

	var body []byte
	if result != nil {
		var marshalErr error
		body, marshalErr = c.Marshal(result)
		if marshalErr != nil {
			log.Println("marshal error:", marshalErr)
			return AppendErrorResponse(dst, msg.Header.RequestID, marshalErr.Error()), true
		}
	}

	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   msg.Header.RequestID,
			CodecType:   c.Type(),
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}

	out, encErr := protocol.AppendEncoded(dst, resp)
	if encErr != nil {
		log.Println("encode response error:", encErr)
		return AppendErrorResponse(dst, msg.Header.RequestID, encErr.Error()), true
	}
	return out, false
}

// AppendErrorResponse 把一条只带错误信息的响应追加到 dst
func AppendErrorResponse(dst []byte, requestID uint64, errMsg string) []byte {
	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   requestID,
			Error:       errMsg,
			Compression: codec.CompressionGzip,
		},
	}
	out, err := protocol.AppendEncoded(dst, resp)
	if err != nil {
		// 只带错误信息的响应编不出来说明协议层出了问题, 这里只能放弃这条响应
		log.Println("encode error response failed:", err)
		return dst
	}
	return out
}

// invoke 按方法名动态调用本地方法。
// 方法查找与签名校验在注册时已经完成, 这里只做: 查表 → 构造 req/reply →
// 反序列化 → 反射调用 → 返回 reply
func (h *Handler) invoke(c codec.Codec, service *serviceEntry, methodName string, body []byte) (interface{}, error) {
	method, ok := service.lookup(methodName)
	if !ok {
		return nil, fmt.Errorf("method not found: %s.%s", service.name, methodName)
	}

	// 动态构造 req 并反序列化请求数据
	req := reflect.New(method.reqType)
	if len(body) > 0 {
		if err := c.Unmarshal(body, req.Interface()); err != nil {
			return nil, err
		}
	}

	// 动态构造 reply
	reply := reflect.New(method.replyType)

	// 反射调用, 返回值固定为 error
	results := method.fn.Call([]reflect.Value{req, reply})
	if errVal := results[0].Interface(); errVal != nil {
		return nil, errVal.(error)
	}

	// 返回指针而不是 reply.Elem().Interface(): 后者要把结构体值装箱, 等于多一次
	// 拷贝加一次分配; 指针装进 interface 不分配, 而 JSON 序列化结果完全一样
	return reply.Interface(), nil
}
