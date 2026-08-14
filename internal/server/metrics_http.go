package server

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"time"

	"kamaRPC/internal/core/metrics"
)

var processStart = time.Now()

// Metrics 返回服务端的指标采集器
func (s *Server) Metrics() *metrics.Collector { return s.metrics }

// Stats 汇总当前运行状态
func (s *Server) Stats() metrics.ServerStats {
	s.mu.Lock()
	conns := len(s.conns)
	services := make([]metrics.ServiceInfo, 0, len(s.services))
	for name, entry := range s.services {
		services = append(services, metrics.ServiceInfo{Name: name, Methods: entry.MethodNames()})
	}
	s.mu.Unlock()

	return metrics.ServerStats{
		Addr:        s.Addr(),
		PID:         os.Getpid(),
		UptimeSec:   time.Since(processStart).Seconds(),
		Goroutines:  runtime.NumGoroutine(),
		Connections: conns,
		Services:    services,
		Methods:     s.metrics.Snapshot(),
		TimestampMS: time.Now().UnixMilli(),
	}
}

// StatsHandler 把 Stats 以 JSON 暴露出去。
//
// 只读接口, 面板通过它抓取; 跨域打开(面板与服务端通常不同端口),
// 但只允许 GET —— 这个端点不该被用来改任何状态
func (s *Server) StatsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "only GET is allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(s.Stats())
	})
	return mux
}

// ServeStats 在单独的端口上启动只读的指标接口。
// 失败只记录不影响 RPC 服务本身, 所以由调用方决定是否关心返回的错误
func (s *Server) ServeStats(addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.StatsHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return srv.ListenAndServe()
}
