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
