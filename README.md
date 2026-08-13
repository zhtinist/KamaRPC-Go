# KamaRPC-Go

从零实现的 Go 语言 RPC 框架：自定义二进制协议 + TCP 长连接 + 请求多路复用，配套 etcd 服务注册发现、负载均衡、熔断与限流。

不依赖 gRPC / net/rpc，通信、编解码、连接管理、服务治理全部自己实现，用来打通分布式系统的核心链路。

## 特性

| 模块 | 能力 |
| --- | --- |
| protocol | 自定义二进制协议 `Magic(2) + headerLen(4) + bodyLen(4) + Header + Body`，长度字段分帧；Header 为二进制编码（v2），按 Magic 兼容 v1 的 JSON Header |
| transport | TCP 长连接、粘包/半包处理、`requestId → Future` 多路复用、连接池（连接复用 + 请求复用） |
| codec | JSON / Protobuf 全链路可用，服务端按请求 Header 的 `CodecType` 选择编解码方式（一个服务端可同时服务两种客户端）；gzip 压缩（只压 Body，小包免压） |
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
go run ./cmd/benchmark -c 100 -n 500000 -b 100            # 默认走批量接口
go run ./cmd/benchmark -c 100 -n 500000 -b 100 -batchapi=false   # 逐条 InvokeAsync 对照
```

```bash
go run ./cmd/benchmark_duration -c 50 -d 10
```

客户端默认限流是 10000 QPS，压测时会先撞上限流而不是框架瓶颈，所以两个压测程序都带 `-rate` 参数（默认放开到 1000000），并把被限流挡掉的请求单独计入 `Throttled`，不混进 `Failed`。想压限流本身就把 `-rate` 调小。

本机实测（M 系列 Mac，两个服务端 + 一个 etcd 全部跑在本地回环）。注意机器状态影响明显（连续压测会发热降频），所以给出连续 5 次的分布而不是最好那次：

```
固定时间窗口 50 并发 / 10s，连续 3 次：
  QPS 208874 / 200514 / 194802   约 20 万，失败 0
  P99 0.42 / 0.42 / 0.43 ms
固定请求数 500000 请求 / 100 并发 / batch 100（走批量接口），连续 3 次：
  QPS 1611470 / 1459898 / 1237556   约 145 万，失败 0
  平均延迟 5.75 / 6.35 / 7.60 ms
```

## 性能优化记录

框架的第一个可用版本只有 17k QPS、P99 9.1ms，经过十三轮基于 profile 的优化到现在的水平。每轮都有 benchmark 前后对比，`git log` 里记录了完整过程：

| 优化 | 手段 | 效果 |
| --- | --- | --- |
| 小包免压 + 压缩器池化 | 低于 512B 的 Body 跳过 gzip（实际压缩方式如实写回 Header，协议兼容）；`gzip.Writer/Reader` 用 `sync.Pool` 复用 | 协议层小包往返 45303ns/857KB → 806ns/688B；端到端 17k → 107k QPS，P99 9.10 → 1.27ms |
| 读路径缓冲区复用 | 连接持有可复用读缓冲；`PacketBuffer` 用读偏移代替切片前移，复用底层数组 | 单次调用 10863B/49 allocs → 2410B/45 allocs；P99 1.27 → 0.96ms |
| 服务端自适应并发分发 | 按连接统计方法耗时 EWMA，超过 50µs 才切协程；信号量封顶 256 形成反压 | 1ms 业务方法单连接吞吐 762 → 169000 QPS（222×），微秒级方法仅 -3% |
| Header 手写 JSON 编解码 | 绕开反射与通用扫描器，格式与字段名不变；含转义/非 ASCII/未知字段回退 `encoding/json`；与标准库的等价性用 differential fuzz 验证 | 协议层小包往返 798ns/688B → 209ns/464B（3.8×）；P99 0.96 → 0.83ms |
| 反射方法表前置 + 写缓冲池化 | `Register` 时解析并缓存方法表（签名错误提前到注册期报错）；写路径用 `sync.Pool` 复用编码缓冲 | 单次调用 2018B/47 allocs → 1534B/42 allocs；P99 0.83 → 0.79ms |
| 并发写合并 + 流水线响应攒批 | 一条连接上的并发写用 leader 代写合并成一次 syscall（保留同步错误语义）；服务端仅在「下一个完整包已就位」时继续攒响应 | 同步 127.5k → 152.2k QPS（+19%），异步批量 +17%，并发慢方法 +22% |
| 连接池默认值 8 → 2 → 1 | 连接的作用是分散到不同节点，单节点并发靠请求复用即可；实测决定吞吐的是「总连接数」，最优约 2 条，每节点 1 条时总数自然跟随实例数 | 同步 106k → 130k → 约 199k QPS（中位数），P99 0.79 → 0.45ms |
| Header 解析零分配化 | 字段名改为 `switch string(key)`（编译器不分配）；服务名/方法名字符串驻留；`invoke` 返回 reply 指针而非装箱结构体 | 单次调用 1534B/42 allocs → 1349B/27 allocs；吞吐在噪声内 |
| 客户端批量发送接口 | `InvokeAsyncBatch` 把 N 条请求编码进同一缓冲一次写出，发现/选址/熔断/取连接各做一次 | 异步批量 235k → 1202k QPS（5.1×），平均延迟 20.5 → 7.6ms |
| 读路径零拷贝 | `ReadBorrowed` 返回指向连接内部缓冲的消息（契约：下一次读之前有效），消息与头部复用；需跨越边界持有的显式拷贝 | 单次调用 1187B/27 allocs → 916B/22 allocs；批量 +18% |
| pending 表换分片 map | 访问模式是写一次读一次即删，不适合为读多写少优化的 `sync.Map` | 916B/22 allocs → 860B/20 allocs |
| Header 二进制编码（协议 v2） | uvarint + 长度前缀取代 JSON；用第二个 Magic 区分版本，仍认老对端的 JSON Header | Header 101 → 15 字节，整包 124 → 38 字节（-69%）；协议层 170.5 → 88.3 ns（1.93×）；异步批量 143.7k → 229.8k QPS（+60%） |

复跑方式：

```bash
go test -run XXX -bench . -benchtime 3s ./internal/...
```

### 与教程官方实现的同机对比

同一台机器、同一个 etcd、同样的压测参数（50 并发 / 10s 同步）。为避免比较停留在限流默认值上，给上游打了只改两行限流常量的补丁（令牌桶 10000 → 5000 万），其余代码未动：

| 配置 | QPS | P99 |
| --- | --- | --- |
| 上游官方，开箱默认（池=1，限流 10000） | 9936 | 9.5ms |
| 上游官方 + 仅放开限流（池=1） | 10576 | 9.8ms |
| 本实现，默认配置（池=1，与上游一致） | 198732（中位数，峰值 217563） | 0.45ms |

同配置下吞吐约 **18.8 倍**（按中位数）、P99 约 **22 倍**。上游摘掉限流后只从 9936 涨到 10576，说明限流并非其瓶颈 —— 真正的原因是服务端串行处理叠加每条消息都做 gzip，排队延迟累积到 4.7ms。

## Protobuf

编解码方式由请求 Header 的 `CodecType` 决定，**服务端按请求选择**、用同一种编码回响应，因此一个服务端可以同时服务 JSON 与 Protobuf 客户端，两端不需要事先约定。业务方法签名不变，框架里没有任何 Protobuf 特例：

```go
// 只是把请求/响应换成 protoc 生成的类型
func (a *ArithPB) Add(args *pb.Args, reply *pb.Reply) error {
	reply.Result = args.A + args.B
	return nil
}
```

```bash
go run ./cmd/benchmark_duration -c 50 -d 10 -codec proto
```

改 `.proto` 后重新生成（需要 `protoc` 与 `protoc-gen-go`）：

```bash
protoc --go_out=. --go_opt=module=kamaRPC -I pkg/api/pb pkg/api/pb/arith.proto
```

### 协议版本与兼容

Header 有两种编码，用 Magic 区分，收包时按 Magic 分派，因此升级不需要两端同时上线：

| Magic | 版本 | Header 编码 | 说明 |
| --- | --- | --- | --- |
| `0x4B52` "KR" | v1 | JSON | 教程原始设计，仍可解析 |
| `0x4B53` "KS" | v2 | 二进制 | 当前默认，Header 101 → 15 字节 |

分帧字段（`headerLen`/`bodyLen`）布局与版本无关，所以拆包逻辑对两种格式通用。v2 的扩展方式是**在 Header 尾部追加字段**：`headerLen` 已经给出总长度，老版本读完已知字段后忽略剩余字节，不需要再改 Magic。

### JSON 与 Protobuf 实测对比

| 口径 | JSON | Protobuf |
| --- | --- | --- |
| 纯编解码，小包（2 个整数） | 273 ns，248 B，6 allocs | **77 ns，68 B，2 allocs（3.5×）** |
| 纯编解码，大包（200 整数 + 1KB 文本） | 16824 ns，8094 B，17 allocs | **1400 ns，4688 B，4 allocs（12×）** |
| Body 体积（`Args{1,2}`） | 13 字节 | **4 字节** |
| 整包体积（二进制 Header 下） | 37 字节 | **30 字节（-19%）** |
| 端到端小包吞吐（50 并发 / 10s） | **约 195k QPS** | 约 188k QPS（慢 3~4%） |

两个反直觉但有数据支撑的结论：

1. **Header 编码方式决定了 Body 优化的上限**：Header 还是 JSON 时，整包只小 6%（120 → 113 字节），Body 省下的 9 字节被 100 字节的 Header 淹没；把 Header 换成二进制之后，同样的 Body 差异变成整包 **-19%**（37 → 30 字节）。先解决占大头的那部分，另一项优化才显得出来。
2. **小包场景端到端反而慢 3~4%**：整条链路 43% 的时间花在系统调用上，编解码本来就不是瓶颈；而 `pb.Args` 带 protoimpl 状态占 56 字节（`api.Args` 只有 16），服务端每个请求都要 `reflect.New` 一个更大的对象，这点开销盖过了编解码的收益。Protobuf 的价值要到**大包**（12 倍）和**跨网络省带宽**时才体现出来。

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
