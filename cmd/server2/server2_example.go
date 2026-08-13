package main

import (
	"flag"
	"log"

	"kamaRPC/internal/codec"
	"kamaRPC/internal/registry"
	"kamaRPC/internal/server"
	"kamaRPC/pkg/api"
)

var (
	addr      = flag.String("addr", ":9091", "服务端监听地址")
	etcdAddr  = flag.String("etcd", "localhost:2379", "etcd 地址")
	advertise = flag.String("advertise", "localhost:9091", "注册到 etcd 的地址")
	ttl       = flag.Int64("ttl", 10, "租约时间(秒)")
	rate      = flag.Int("rate", 100000, "服务端限流速率(每秒令牌数)")
)

// server2 与 server1 的区别只有端口和注册的服务数量,
// 用于演示多实例下的服务发现与负载均衡
func main() {
	flag.Parse()

	reg, err := registry.NewRegistry([]string{*etcdAddr})
	if err != nil {
		log.Fatalf("connect etcd failed: %v", err)
	}
	defer reg.Close()

	srv, err := server.NewServer(*addr,
		server.WithServerCodec(codec.JSON),
		server.WithServerRateLimit(*rate),
	)
	if err != nil {
		log.Fatalf("create server failed: %v", err)
	}

	if err := srv.Register("Arith", &api.Arith{}); err != nil {
		log.Fatalf("register Arith failed: %v", err)
	}
	// 同名服务的 Protobuf 版本, 编解码方式由请求 Header 决定
	if err := srv.Register("ArithPB", &api.ArithPB{}); err != nil {
		log.Fatalf("register ArithPB failed: %v", err)
	}

	for _, service := range []string{"Arith", "ArithPB"} {
		if err := reg.Register(service, registry.Instance{Addr: *advertise}, *ttl); err != nil {
			log.Fatalf("register %s to etcd failed: %v", service, err)
		}
	}

	log.Printf("server2 listening on %s, advertise %s", *addr, *advertise)

	if err := srv.Start(); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
