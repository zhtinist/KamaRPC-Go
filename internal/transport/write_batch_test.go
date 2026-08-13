package transport

import (
	"net"
	"strconv"
	"sync"
	"testing"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/protocol"
)

// 并发写合并后, 对端必须仍能解出每一条完整且内容正确的消息 ——
// 批次拼接一旦出错, 表现就是字节交叉、对端解包失败或串味
func TestConcurrentWritesStayIntact(t *testing.T) {
	const (
		writers       = 32
		msgsPerWriter = 50
		expectedTotal = writers * msgsPerWriter
	)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	sender := NewTCPConnection(client)
	receiver := NewTCPConnection(server)

	// 读端: 收满预期条数
	type result struct {
		bodies map[string]int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		bodies := make(map[string]int)
		for i := 0; i < expectedTotal; i++ {
			msg, err := receiver.Read()
			if err != nil {
				done <- result{err: err}
				return
			}
			bodies[string(msg.Body)]++
		}
		done <- result{bodies: bodies}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < msgsPerWriter; i++ {
				body := "w" + strconv.Itoa(w) + "-m" + strconv.Itoa(i)
				msg := &protocol.Message{
					Header: &protocol.Header{
						ServiceName: "Arith",
						MethodName:  "Add",
						CodecType:   codec.JSON,
						Compression: codec.CompressionGzip,
					},
					Body: []byte(body),
				}
				if err := sender.Write(msg); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	got := <-done
	if got.err != nil {
		t.Fatalf("read: %v", got.err)
	}
	if len(got.bodies) != expectedTotal {
		t.Fatalf("收到 %d 种不同消息, 期望 %d", len(got.bodies), expectedTotal)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < msgsPerWriter; i++ {
			body := "w" + strconv.Itoa(w) + "-m" + strconv.Itoa(i)
			if got.bodies[body] != 1 {
				t.Fatalf("消息 %q 出现 %d 次, 期望 1 次", body, got.bodies[body])
			}
		}
	}
}

// 写入失败时, 参与同一批次的所有调用方都必须拿到错误,
// 否则 SendAsync 会以为发送成功, 请求永远等不到响应
func TestConcurrentWritesReportErrors(t *testing.T) {
	client, server := net.Pipe()
	sender := NewTCPConnection(client)

	// 读端直接关掉, 后续写入必然失败
	server.Close()

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		errs  int
		total = 16
	)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := &protocol.Message{
				Header: &protocol.Header{RequestID: uint64(i), CodecType: codec.JSON},
				Body:   []byte("x"),
			}
			if err := sender.Write(msg); err != nil {
				mu.Lock()
				errs++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if errs != total {
		t.Fatalf("%d/%d 个调用方拿到了写入错误, 期望全部拿到", errs, total)
	}
}
