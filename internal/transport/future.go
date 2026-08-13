package transport

import (
	"sync"

	"kamaRPC/internal/codec"
)

// Future 异步调用的结果占位符:
// SendAsync 先返回 Future, 响应回来后由 readLoop 填充结果
type Future struct {
	done chan struct{} // 一次性完成通知, close 即广播
	res  []byte        // 响应 Body 原始字节
	err  error         // 网络错误/超时/服务端错误

	mu    sync.Mutex
	codec codec.Codec // GetResult 时把 res 反序列化到业务结构体

	onComplete func(error) // 完成回调, 框架内部用来更新熔断统计
	completed  bool
}

// NewFuture 创建一个未完成的 Future, 默认使用 JSON 解码结果
func NewFuture() *Future {
	c, _ := codec.New(codec.JSON)
	return &Future{
		done:  make(chan struct{}),
		codec: c,
	}
}

// SetCodec 指定结果反序列化使用的编解码器
func (f *Future) SetCodec(c codec.Codec) {
	if c == nil {
		return
	}
	f.mu.Lock()
	f.codec = c
	f.mu.Unlock()
}

// OnComplete 注册完成回调; 如果 Future 已经完成则立即回调,
// 避免"响应比注册更快"时回调丢失
func (f *Future) OnComplete(fn func(error)) {
	f.mu.Lock()
	if f.completed {
		err := f.err
		f.mu.Unlock()
		if fn != nil {
			fn(err)
		}
		return
	}
	f.onComplete = fn
	f.mu.Unlock()
}

// Done 由 readLoop 调用: 填充结果, 触发回调, 并唤醒所有等待方
func (f *Future) Done(res []byte, err error) {
	f.mu.Lock()
	if f.completed {
		f.mu.Unlock()
		return
	}
	f.res = res
	f.err = err
	f.completed = true
	fn := f.onComplete
	f.mu.Unlock()

	if fn != nil {
		fn(err)
	}
	close(f.done)
}

// DoneChan 暴露完成信号, 便于调用方自己 select 超时
func (f *Future) DoneChan() <-chan struct{} {
	return f.done
}

// Wait 阻塞等待原始结果
func (f *Future) Wait() ([]byte, error) {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.res, f.err
}

// GetResult 阻塞等待并把结果反序列化到 reply
func (f *Future) GetResult(reply interface{}) error {
	<-f.done
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	return f.codec.Unmarshal(f.res, reply)
}
