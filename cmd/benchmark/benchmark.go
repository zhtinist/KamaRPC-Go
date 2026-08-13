package main

// 固定请求数量的压测: 请求总数固定, 结果可重复, 适合优化前后横向对比
// 走异步接口 InvokeAsync, 客户端自行维护发送与收包策略

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"kamaRPC/internal/client"
	"kamaRPC/internal/codec"
	"kamaRPC/internal/registry"
	"kamaRPC/internal/transport"
	"kamaRPC/pkg/api"
)

var (
	concurrency = flag.Int("c", 100, "并发客户端数量")
	total       = flag.Int("n", 10000, "请求总数")
	batchSize   = flag.Int("b", 100, "异步请求的批处理大小")
	etcdAddr    = flag.String("etcd", "localhost:2379", "etcd的地址")
	serviceName = flag.String("s", "Arith", "服务名")
	methodName  = flag.String("m", "Add", "方法名")
	// 客户端限流默认 10000 QPS, 压测时会成为瓶颈,
	// 所以这里默认放开, 想压限流本身就把它调小
	rate = flag.Int("rate", 1000000, "客户端限流速率(每秒令牌数)")
)

type callResult struct {
	future   *transport.Future
	reqStart time.Time
}

func main() {
	flag.Parse()

	if *concurrency < 1 {
		*concurrency = 1
	}
	if *batchSize < 1 {
		*batchSize = 1
	}

	log.Printf("Starting benchmark: concurrency=%d, total=%d, batch=%d\n", *concurrency, *total, *batchSize)

	reg, err := registry.NewRegistry([]string{*etcdAddr})
	if err != nil {
		log.Fatalf("Failed to create registry: %v", err)
	}
	defer reg.Close()

	c, err := client.NewClient(
		reg,
		client.WithClientCodec(codec.JSON),
		client.WithClientRateLimit(*rate),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	var (
		wg             sync.WaitGroup
		successCount   int64
		failCount      int64
		throttledCount int64
		totalLatency   int64
	)

	start := time.Now()

	base := *total / *concurrency
	rem := *total % *concurrency

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		reqs := base
		if i < rem {
			reqs++
		}

		go func(requestsPerClient int) {
			defer wg.Done()

			remaining := requestsPerClient
			for remaining > 0 {
				// 一批一批地发
				currentBatch := *batchSize
				if remaining < currentBatch {
					currentBatch = remaining
				}

				var calls []callResult

				for j := 0; j < currentBatch; j++ {
					args := &api.Args{A: j, B: j}
					reqStart := time.Now()

					f, err := c.InvokeAsync(context.Background(), *serviceName, *methodName, args)
					if err != nil {
						if errors.Is(err, client.ErrRateLimited) {
							atomic.AddInt64(&throttledCount, 1)
						} else {
							atomic.AddInt64(&failCount, 1)
						}
						continue
					}
					calls = append(calls, callResult{future: f, reqStart: reqStart})
				}

				doneCh := make(chan callResult, len(calls))
				for _, item := range calls {
					go func(it callResult) {
						<-it.future.DoneChan()
						doneCh <- it
					}(item)
				}

				for k := 0; k < len(calls); k++ {
					item := <-doneCh
					reply := &api.Reply{}
					if err := item.future.GetResult(reply); err != nil {
						atomic.AddInt64(&failCount, 1)
					} else {
						atomic.AddInt64(&successCount, 1)
						latency := time.Since(item.reqStart).Microseconds()
						atomic.AddInt64(&totalLatency, latency)
					}
				}

				remaining -= currentBatch
			}
		}(reqs)
	}

	wg.Wait()
	duration := time.Since(start)

	qps := float64(successCount) / duration.Seconds()
	var avgLatency float64
	if successCount > 0 {
		avgLatency = float64(totalLatency) / float64(successCount) / 1000.0
	}

	fmt.Println("\nBenchmark Result:")
	fmt.Printf("Total Requests: %d\n", *total)
	fmt.Printf("Concurrency:    %d\n", *concurrency)
	fmt.Printf("Batch Size:     %d\n", *batchSize)
	fmt.Printf("Duration:       %v\n", duration)
	fmt.Printf("Success:        %d\n", successCount)
	fmt.Printf("Failed:         %d\n", failCount)
	fmt.Printf("Throttled:      %d\n", throttledCount)
	fmt.Printf("QPS:            %.2f\n", qps)
	fmt.Printf("Avg Latency:    %.2f ms\n", avgLatency)
}
