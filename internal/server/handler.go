package server

import (
	"fmt"
	"log"
	"reflect"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
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

// Process 调用本地方法并把结果写回对端
func (h *Handler) Process(conn *transport.TCPConnection, msg *protocol.Message, service *serviceEntry) {
	result, err := h.invoke(service, msg.Header.MethodName, msg.Body)
	if err != nil {
		h.writeError(conn, msg.Header.RequestID, err.Error())
		return
	}

	var body []byte
	if result != nil {
		var marshalErr error
		body, marshalErr = h.codec.Marshal(result)
		if marshalErr != nil {
			log.Println("marshal error:", marshalErr)
			h.writeError(conn, msg.Header.RequestID, marshalErr.Error())
			return
		}
	}

	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   msg.Header.RequestID,
			CodecType:   h.codec.Type(),
			Compression: codec.CompressionGzip,
		},
		Body: body,
	}

	if err := conn.Write(resp); err != nil {
		log.Println("write response error:", err)
	}
}

// invoke 按方法名动态调用本地方法。
// 方法查找与签名校验在注册时已经完成, 这里只做: 查表 → 构造 req/reply →
// 反序列化 → 反射调用 → 返回 reply
func (h *Handler) invoke(service *serviceEntry, methodName string, body []byte) (interface{}, error) {
	method, ok := service.lookup(methodName)
	if !ok {
		return nil, fmt.Errorf("method not found: %s.%s", service.name, methodName)
	}

	// 动态构造 req 并反序列化请求数据
	req := reflect.New(method.reqType)
	if len(body) > 0 {
		if err := h.codec.Unmarshal(body, req.Interface()); err != nil {
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

	return reply.Elem().Interface(), nil
}

func (h *Handler) writeError(conn *transport.TCPConnection, requestID uint64, errMsg string) {
	resp := &protocol.Message{
		Header: &protocol.Header{
			RequestID:   requestID,
			Error:       errMsg,
			Compression: codec.CompressionGzip,
		},
	}
	if err := conn.Write(resp); err != nil {
		log.Println("write error response failed:", err)
	}
}
