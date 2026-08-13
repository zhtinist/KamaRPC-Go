# KamaRPC-Go

从零实现的 Go 语言 RPC 框架：自定义二进制协议 + TCP 长连接 + 请求多路复用，配套 etcd 服务注册发现、负载均衡、熔断与限流。

不依赖 gRPC / net/rpc，通信、编解码、连接管理、服务治理全部自己实现，用来打通分布式系统的核心链路。

## 特性

| 模块 | 能力 |
| --- | --- |
| protocol | 自定义二进制协议 `Magic(2) + headerLen(4) + bodyLen(4) + Header + Body`，长度字段分帧 |
| transport | TCP 长连接、粘包/半包处理、`requestId → Future` 多路复用、连接池（连接复用 + 请求复用） |
| codec | JSON / Protobuf 可插拔注册工厂，gzip 压缩（只压 Body） |
| client | 异步 `InvokeAsync`（Future）与同步 `Invoke`，超时控制，实例级熔断与连接池隔离 |
| server | TCP 监听、One Connection One Goroutine、服务注册表 + 反射分发 |
| registry | etcd 注册 + lease keepalive + watch 增量更新 + 本地缓存 |
| loadbalance | Round Robin / Random / 平滑加权轮询（Smooth WRR） |
| breaker | Closed / Open / HalfOpen 状态机，滑动计数窗口按失败率熔断，半开单探测恢复 |
| limiter | 令牌桶，客户端与服务端双端限流 |

## 一次调用的链路

```
客户端限流 → 服务发现(etcd + 本地缓存) → 负载均衡选址 → 熔断判断
   → 连接池取连接 → 序列化参数 → 组装协议 Message → SendAsync 返回 Future
服务端 Accept → 每连接一个 goroutine → 读完整包 → 服务端限流
   → 查服务注册表 → 反射调用 → 写回响应
客户端 readLoop → 按 requestId 找到 Future → Done 唤醒等待方 + 更新熔断统计
```

## 目录结构

```
├── cmd/                              # 可执行示例入口
│   ├── client/                       # 客户端示例：周期性并发 InvokeAsync
│   ├── server1/                      # 服务端示例1：:9090 注册 Arith/Arith2
│   ├── server2/                      # 服务端示例2：:9091 只注册 Arith
│   ├── benchmark/                    # 压测一：固定请求数（异步接口）
│   └── benchmark_duration/           # 压测二：固定时间窗口（同步接口，含 P50/P90/P99）
├── internal/                         # 框架核心实现
│   ├── client/                       # 发现、LB、连接池、熔断、限流、超时
│   ├── server/                       # 监听、限流、反射调用、响应写回
│   ├── transport/                    # TCP 连接、粘包拆包、多路复用、Future、连接池
│   ├── protocol/                     # Header / Message + 编解码
│   ├── codec/                        # JSON / Protobuf / gzip
│   ├── registry/                     # etcd 注册发现 + watch + 本地缓存
│   ├── loadbalance/                  # RR / Random / WeightedRR
│   ├── breaker/                      # 熔断器状态机
│   └── limiter/                      # 令牌桶
└── pkg/api/                          # 示例服务与请求/响应结构体
```

## 环境要求

- Go 1.24+
- etcd（仅运行示例需要；单元测试不依赖 etcd）

macOS 安装并启动 etcd：

```bash
brew install etcd && etcd
```

## 运行示例

先启动 etcd，然后开三个终端（先服务端后客户端）：

```bash
go run ./cmd/server1
```

```bash
go run ./cmd/server2
```

```bash
go run ./cmd/client
```

客户端会每 5 秒发一轮 3 个并发异步请求，打印 `Arith.Add` 的结果。

## 压测

两种方式：固定请求数（异步接口，结果可重复，适合优化前后对比）与固定时间窗口（同步接口，持续施压，看 QPS 与尾延迟）。

```bash
go run ./cmd/benchmark -c 100 -n 50000 -b 100
```

```bash
go run ./cmd/benchmark_duration -c 50 -d 10
```

客户端默认限流是 10000 QPS，压测时会先撞上限流而不是框架瓶颈，所以两个压测程序都带 `-rate` 参数（默认放开到 1000000），并把被限流挡掉的请求单独计入 `Throttled`，不混进 `Failed`。想压限流本身就把 `-rate` 调小。

本机实测（M 系列 Mac，两个服务端 + 一个 etcd 全部跑在本地回环）：

```
固定请求数   50000 请求 / 100 并发 / batch 100 → QPS 16641, 失败 0
固定时间窗口 50 并发 / 10s → QPS 17048, 失败 0
                              Avg 2.88ms  P50 2.52ms  P90 5.40ms  P99 9.10ms
```

## 测试

单元测试覆盖协议编解码、粘包/半包、平滑加权轮询、熔断状态机、令牌桶，以及不依赖 etcd 的服务端反射调用与单连接并发多路复用：

```bash
go test -race ./...
```

## 服务定义约定

服务方法遵循 net/rpc 风格签名，由服务端反射调用：

```go
func (s *YourService) Method(req *Req, reply *Resp) error
```

```go
srv, _ := server.NewServer(":9090", server.WithServerCodec(codec.JSON))
srv.Register("Arith", &api.Arith{})
reg.Register("Arith", registry.Instance{Addr: "localhost:9090"}, 10)
srv.Start()
```

```go
c, _ := client.NewClient(reg, client.WithClientCodec(codec.JSON))
reply := &api.Reply{}
err := c.Invoke(context.Background(), "Arith", "Add", &api.Args{A: 1, B: 2}, reply)
```

## 说明

本仓库是照 KamaRPC-Go 教程从零手写的实现，用于学习 RPC 框架与分布式基础设施的核心机制。原项目见 [youngyangyang04/KamaRPC-Go](https://github.com/youngyangyang04/KamaRPC-Go)。

## License

[MIT](LICENSE)
