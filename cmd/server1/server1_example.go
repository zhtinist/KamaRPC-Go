package main

import (
	"flag"
	"log"

	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/registry"
	"kamaRPC/internal/server"
	"kamaRPC/pkg/api"
)

var (
	addr      = flag.String("addr", ":9090", "服务端监听地址")
	etcdAddr  = flag.String("etcd", "localhost:2379", "etcd 地址")
	advertise = flag.String("advertise", "localhost:9090", "注册到 etcd 的地址")
	ttl       = flag.Int64("ttl", 10, "租约时间(秒)")
	rate      = flag.Int("rate", 100000, "服务端限流速率(每秒令牌数)")
	statsAddr = flag.String("stats", ":9190", "指标接口监听地址, 供监控面板抓取; 置空则不启动")
)

func main() {
	flag.Parse()

	// 1. 连接注册中心
	reg, err := registry.NewRegistry([]string{*etcdAddr})
	if err != nil {
		log.Fatalf("connect etcd failed: %v", err)
	}
	defer reg.Close()

	// 2. 创建 RPC Server
	srv, err := server.NewServer(*addr,
		server.WithServerCodec(codec.JSON),
		server.WithServerRateLimit(*rate),
	)
	if err != nil {
		log.Fatalf("create server failed: %v", err)
	}

	// 3. 注册本地服务(供反射调用)
	if err := srv.Register("Arith", &api.Arith{}); err != nil {
		log.Fatalf("register Arith failed: %v", err)
	}
	// 同名服务的 Protobuf 版本, 编解码方式由请求 Header 决定
	if err := srv.Register("ArithPB", &api.ArithPB{}); err != nil {
		log.Fatalf("register ArithPB failed: %v", err)
	}
	if err := srv.Register("Arith2", &api.Arith2{}); err != nil {
		log.Fatalf("register Arith2 failed: %v", err)
	}

	// 4. 把自身地址写入 etcd, 供客户端发现
	ins := registry.Instance{Addr: *advertise}
	for _, service := range []string{"Arith", "Arith2", "ArithPB"} {
		if err := reg.Register(service, ins, *ttl); err != nil {
			log.Fatalf("register %s to etcd failed: %v", service, err)
		}
	}

	// 只读指标接口, 单独端口, 挂了不影响 RPC 服务
	if *statsAddr != "" {
		go func() {
			log.Printf("stats endpoint on %s/stats", *statsAddr)
			if err := srv.ServeStats(*statsAddr); err != nil {
				log.Printf("stats endpoint stopped: %v", err)
			}
		}()
	}

	log.Printf("server1 listening on %s, advertise %s", *addr, *advertise)

	// 5. 开始监听并 Accept
	if err := srv.Start(); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
