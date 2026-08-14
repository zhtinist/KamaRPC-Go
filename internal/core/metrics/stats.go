package metrics

// 服务端 /stats 接口的数据契约。
//
// 放在 core 而不是 server: 服务端是生产方, 监控面板是消费方, 两边都只依赖
// 这里的类型定义, 面板不必为了一个结构体去 import 服务端运行时。

// ServerStats 一个 RPC 服务端自曝的运行状态
type ServerStats struct {
	Addr        string        `json:"addr"`
	PID         int           `json:"pid"`
	UptimeSec   float64       `json:"uptimeSec"`
	Goroutines  int           `json:"goroutines"`
	Connections int           `json:"connections"`
	Services    []ServiceInfo `json:"services"`
	Methods     []MethodStat  `json:"methods"`
	TimestampMS int64         `json:"timestampMs"`
}

// ServiceInfo 一个已注册服务及其可调用方法
type ServiceInfo struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
}

// ClientStats 一个 RPC 客户端自曝的治理状态。
//
// 与服务端指标的区别在于视角: 服务端只看得到"进来的请求", 客户端这边才能看到
// 熔断器状态、连接池水位, 以及被治理策略在本地挡掉(限流/熔断)的调用 ——
// 这些请求根本没上网络, 服务端不可能统计到
type ClientStats struct {
	Name        string        `json:"name"`
	PID         int           `json:"pid"`
	UptimeSec   float64       `json:"uptimeSec"`
	Goroutines  int           `json:"goroutines"`
	Breakers    []BreakerStat `json:"breakers"`
	Pools       []PoolStat    `json:"pools"`
	Methods     []MethodStat  `json:"methods"`
	TimestampMS int64         `json:"timestampMs"`
}

// BreakerStat 一个「服务@实例」维度熔断器的当前状态
type BreakerStat struct {
	Service   string `json:"service"`
	Addr      string `json:"addr"`
	State     string `json:"state"` // closed / open / half-open
	Failures  int    `json:"failures"`
	Successes int    `json:"successes"`
}

// PoolStat 一个目标实例的连接池水位
type PoolStat struct {
	Addr      string `json:"addr"`
	Active    int    `json:"active"`
	MaxActive int    `json:"maxActive"`
	Closed    bool   `json:"closed"`
}
