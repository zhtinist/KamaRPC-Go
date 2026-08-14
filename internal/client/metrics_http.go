package client

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"time"

	"kamaRPC/internal/client/breaker"
	"kamaRPC/internal/core/metrics"
	"kamaRPC/internal/core/transport"
)

var processStart = time.Now()

// Metrics 返回客户端的指标采集器
func (c *Client) Metrics() *metrics.Collector { return c.metrics }

// Stats 汇总客户端当前的治理状态
func (c *Client) Stats() metrics.ClientStats {
	breakers := make([]metrics.BreakerStat, 0, 8)
	c.breaker.Range(func(key, value interface{}) bool {
		service, addr := splitBreakerKey(key.(string))
		state, failures, successes := value.(*breaker.CircuitBreaker).Snapshot()
		breakers = append(breakers, metrics.BreakerStat{
			Service:   service,
			Addr:      addr,
			State:     state.String(),
			Failures:  failures,
			Successes: successes,
		})
		return true
	})

	pools := make([]metrics.PoolStat, 0, 8)
	c.pools.Range(func(key, value interface{}) bool {
		active, maxActive, closed := value.(*transport.ConnectionPool).Snapshot()
		pools = append(pools, metrics.PoolStat{
			Addr:      key.(string),
			Active:    active,
			MaxActive: maxActive,
			Closed:    closed,
		})
		return true
	})

	return metrics.ClientStats{
		Name:        c.name,
		PID:         os.Getpid(),
		UptimeSec:   time.Since(processStart).Seconds(),
		Goroutines:  runtime.NumGoroutine(),
		Breakers:    breakers,
		Pools:       pools,
		Methods:     c.metrics.Snapshot(),
		TimestampMS: time.Now().UnixMilli(),
	}
}

// StatsHandler 把客户端治理状态以 JSON 暴露出去。
// 与服务端那个端点一样是只读的, 非 GET 一律拒绝
func (c *Client) StatsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "only GET is allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(c.Stats())
	})
	return mux
}

// ServeStats 在单独端口上启动只读的客户端指标接口
func (c *Client) ServeStats(addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           c.StatsHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return srv.ListenAndServe()
}
