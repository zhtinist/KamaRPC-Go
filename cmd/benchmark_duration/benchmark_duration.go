package main

// 固定时间窗口的压测: 持续稳定施压, 更接近线上真实流量,
// 走同步接口 Invoke, 统计 QPS 与 P50/P90/P99 尾延迟

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"kamaRPC/internal/client"
	"kamaRPC/internal/codec"
	"kamaRPC/internal/registry"
	"kamaRPC/pkg/api"
	"kamaRPC/pkg/api/pb"
)

var (
	concurrency = flag.Int("c", 50, "并发客户端数量")
	durationSec = flag.Int("d", 10, "基准测试持续时间（单位为秒）")
	etcdAddr    = flag.String("etcd", "localhost:2379", "etcd的地址")
	serviceName = flag.String("s", "", "服务名(默认 JSON 用 Arith, proto 用 ArithPB)")
	codecName   = flag.String("codec", "json", "编解码方式: json 或 proto")
	methodName  = flag.String("m", "Add", "方法名")
	// 客户端限流默认 10000 QPS, 压测时会成为瓶颈,
	// 所以这里默认放开, 想压限流本身就把它调小
	rate     = flag.Int("rate", 1000000, "客户端限流速率(每秒令牌数)")
	poolSize = flag.Int("pool", 1, "单个目标节点的连接池大小")
)

type metrics struct {
	success   int64
	fail      int64
	throttled int64 // 被客户端限流挡掉的请求, 与真实失败分开统计
	latency   []int64
	mu        sync.Mutex
}

func (m *metrics) recordLatency(us int64) {
	m.mu.Lock()
	m.latency = append(m.latency, us)
	m.mu.Unlock()
}

func main() {
	flag.Parse()

	codecType, service := resolveCodec(*codecName, *serviceName)

	log.Printf("Starting benchmark: concurrency=%d duration=%ds codec=%s service=%s\n",
		*concurrency, *durationSec, *codecName, service)

	reg, err := registry.NewRegistry([]string{*etcdAddr})
	if err != nil {
		log.Fatal(err)
	}
	defer reg.Close()

	c, err := client.NewClient(
		reg,
		client.WithClientCodec(codecType),
		client.WithClientRateLimit(*rate),
		client.WithClientPoolSize(*poolSize),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	var m metrics

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(*durationSec)*time.Second,
	)
	defer cancel()

	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					args, reply := newArgsReply(codecType)

					reqStart := time.Now()
					err := c.Invoke(
						context.Background(),
						service,
						*methodName,
						args,
						reply,
					)
					lat := time.Since(reqStart).Microseconds()

					if err != nil {
						if errors.Is(err, client.ErrRateLimited) {
							atomic.AddInt64(&m.throttled, 1)
						} else {
							atomic.AddInt64(&m.fail, 1)
						}
						continue
					}

					atomic.AddInt64(&m.success, 1)
					m.recordLatency(lat)
				}
			}
		}()
	}

	wg.Wait()

	printStats(&m, time.Since(start))
}

// resolveCodec 解析 -codec, 并给出对应的默认服务名
func resolveCodec(name, service string) (codec.Type, string) {
	switch name {
	case "proto", "protobuf":
		if service == "" {
			service = "ArithPB"
		}
		return codec.Protobuf, service
	case "json":
		if service == "" {
			service = "Arith"
		}
		return codec.JSON, service
	default:
		log.Fatalf("unknown codec %q, want json or proto", name)
		return 0, ""
	}
}

// newArgsReply 按编解码方式构造请求与响应结构体
func newArgsReply(t codec.Type) (interface{}, interface{}) {
	if t == codec.Protobuf {
		return &pb.Args{A: 1, B: 2}, &pb.Reply{}
	}
	return &api.Args{A: 1, B: 2}, &api.Reply{}
}

func printStats(m *metrics, duration time.Duration) {
	total := m.success + m.fail + m.throttled
	qps := float64(m.success) / duration.Seconds()

	sort.Slice(m.latency, func(i, j int) bool {
		return m.latency[i] < m.latency[j]
	})

	p50 := percentile(m.latency, 50)
	p90 := percentile(m.latency, 90)
	p99 := percentile(m.latency, 99)

	var avg float64
	if len(m.latency) > 0 {
		var sum int64
		for _, v := range m.latency {
			sum += v
		}
		avg = float64(sum) / float64(len(m.latency)) / 1000
	}

	fmt.Println()
	fmt.Println("========= Benchmark Result =========")
	fmt.Printf("Duration:        %v\n", duration)
	fmt.Printf("Total Requests:  %d\n", total)
	fmt.Printf("Success:         %d\n", m.success)
	fmt.Printf("Failed:          %d\n", m.fail)
	fmt.Printf("Throttled:       %d\n", m.throttled)
	fmt.Printf("QPS:             %.2f\n", qps)
	fmt.Printf("Avg Latency:     %.2f ms\n", avg)
	fmt.Printf("P50 Latency:     %.2f ms\n", float64(p50)/1000)
	fmt.Printf("P90 Latency:     %.2f ms\n", float64(p90)/1000)
	fmt.Printf("P99 Latency:     %.2f ms\n", float64(p99)/1000)
}

func percentile(data []int64, p int) int64 {
	if len(data) == 0 {
		return 0
	}

	k := int(float64(len(data)) * float64(p) / 100.0)
	if k >= len(data) {
		k = len(data) - 1
	}
	return data[k]
}
