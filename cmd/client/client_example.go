package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	"kamaRPC/internal/client"
	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/registry"
	"kamaRPC/internal/core/transport"
	"kamaRPC/pkg/api"
)

var (
	etcdAddr    = flag.String("etcd", "localhost:2379", "etcd 地址")
	serviceName = flag.String("s", "Arith", "服务名")
	methodName  = flag.String("m", "Add", "方法名")
	interval    = flag.Duration("interval", 5*time.Second, "每轮请求的间隔")
	perRound    = flag.Int("n", 3, "每轮并发请求数")
	statsAddr   = flag.String("stats", ":9290", "客户端指标接口地址, 供监控面板抓取; 置空则不启动")
)

func main() {
	flag.Parse()

	// 1. 连接注册中心
	reg, err := registry.NewRegistry([]string{*etcdAddr})
	if err != nil {
		log.Fatalf("connect etcd failed: %v", err)
	}
	defer reg.Close()

	// 2. 创建客户端
	c, err := client.NewClient(
		reg,
		client.WithClientCodec(codec.JSON),
		client.WithClientName("demo-client"),
	)
	if err != nil {
		log.Fatalf("create client failed: %v", err)
	}
	defer c.Close()

	// 客户端侧的治理状态(熔断器、连接池)只有这个进程看得到, 单独暴露给面板
	if *statsAddr != "" {
		go func() {
			log.Printf("client stats endpoint on %s/stats", *statsAddr)
			if err := c.ServeStats(*statsAddr); err != nil {
				log.Printf("client stats endpoint stopped: %v", err)
			}
		}()
	}

	// 3. 周期性并发调用: 演示异步 Future + requestId 多路复用 + 服务发现选址
	for round := 1; ; round++ {
		var wg sync.WaitGroup

		for i := 0; i < *perRound; i++ {
			args := &api.Args{A: round, B: i}

			future, err := c.InvokeAsync(context.Background(), *serviceName, *methodName, args)
			if err != nil {
				log.Printf("[round %d] invoke failed: %v", round, err)
				continue
			}

			wg.Add(1)
			go func(f *transport.Future, a *api.Args) {
				defer wg.Done()

				reply := &api.Reply{}
				if err := f.GetResult(reply); err != nil {
					log.Printf("[round %d] %s.%s(%d, %d) error: %v",
						round, *serviceName, *methodName, a.A, a.B, err)
					return
				}
				log.Printf("[round %d] %s.%s(%d, %d) = %d",
					round, *serviceName, *methodName, a.A, a.B, reply.Result)
			}(future, args)
		}

		wg.Wait()
		time.Sleep(*interval)
	}
}
