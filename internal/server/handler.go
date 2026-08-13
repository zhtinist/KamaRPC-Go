package server

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
	"kamaRPC/internal/transport"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

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
func (h *Handler) Process(conn *transport.TCPConnection, msg *protocol.Message, service interface{}) {
	result, err := h.invoke(
		context.Background(),
		service,
		msg.Header.ServiceName,
		msg.Header.MethodName,
		msg.Body,
	)
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

// invoke 根据字符串方法名动态调用本地方法,
// 目前支持 net/rpc 风格签名: func(req *Req, reply *Resp) error
func (h *Handler) invoke(ctx context.Context, service interface{}, serviceName, methodName string, body []byte) (interface{}, error) {
	// 拿到服务实例的反射对象
	serviceValue := reflect.ValueOf(service)
	if !serviceValue.IsValid() {
		return nil, fmt.Errorf("service not found: %s", serviceName)
	}

	// 按方法名查找方法
	method := serviceValue.MethodByName(methodName)
	if !method.IsValid() {
		return nil, fmt.Errorf("method not found: %s.%s", serviceName, methodName)
	}

	// 校验方法签名
	methodType := method.Type()
	numIn := methodType.NumIn()
	numOut := methodType.NumOut()

	if numIn != 2 ||
		methodType.In(0).Kind() != reflect.Ptr ||
		methodType.In(1).Kind() != reflect.Ptr ||
		numOut != 1 ||
		!methodType.Out(0).Implements(errorType) {
		return nil, fmt.Errorf("unsupported method signature: %s.%s", serviceName, methodName)
	}

	// 动态构造 req 并反序列化请求数据
	reqType := methodType.In(0)
	req := reflect.New(reqType.Elem())
	if len(body) > 0 {
		if err := h.codec.Unmarshal(body, req.Interface()); err != nil {
			return nil, err
		}
	}

	// 动态构造 reply
	replyType := methodType.In(1)
	reply := reflect.New(replyType.Elem())

	// 反射调用
	results := method.Call([]reflect.Value{req, reply})

	// 返回值固定为 error
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
