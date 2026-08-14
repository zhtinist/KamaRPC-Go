// console 是 KamaRPC 的 Web 监控面板。
//
// 前后端分离: 这个进程是后端, 只出 JSON API 并托管前端静态文件;
// 前端是纯静态页面(无构建步骤、无外部依赖), 靠轮询这些 API 刷新。
//
//	GET /api/topology  从 etcd 读服务与实例(反映注册发现的实时状态)
//	GET /api/stats     抓取各 RPC 服务端的只读指标接口并汇总
//	GET /api/overview  上面两者合一, 面板默认用这个, 一次请求拿全
package main

import (
	"embed"
	"encoding/json"
	"flag"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"kamaRPC/internal/core/metrics"
	"kamaRPC/internal/core/registry"
)

//go:embed web
var webFS embed.FS

var (
	addr     = flag.String("addr", ":8080", "面板监听地址")
	etcdAddr = flag.String("etcd", "localhost:2379", "etcd 地址")
	targets  = flag.String("targets", "localhost:9190,localhost:9191", "各 RPC 服务端的指标接口地址, 逗号分隔")
	services = flag.String("services", "Arith,Arith2,ArithPB", "要在拓扑里展示的服务名, 逗号分隔")
	scrapeTO = flag.Duration("timeout", 2*time.Second, "抓取单个服务端指标的超时")
)

func main() {
	flag.Parse()

	reg, err := registry.NewRegistry([]string{*etcdAddr})
	if err != nil {
		log.Fatalf("连接 etcd 失败: %v", err)
	}
	defer reg.Close()

	c := &console{
		reg:      reg,
		targets:  splitAndTrim(*targets),
		services: splitAndTrim(*services),
		client:   &http.Client{Timeout: *scrapeTO},
	}

	// 前端资源编进二进制, 部署时只有一个可执行文件
	static, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("加载前端资源失败: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/topology", c.handleTopology)
	mux.HandleFunc("/api/stats", c.handleStats)
	mux.HandleFunc("/api/overview", c.handleOverview)
	mux.Handle("/", http.FileServer(http.FS(static)))

	log.Printf("控制台已启动: http://localhost%s", *addr)
	log.Printf("  etcd=%s  指标目标=%v", *etcdAddr, c.targets)

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("面板退出: %v", err)
	}
}

type console struct {
	reg      *registry.Registry
	targets  []string
	services []string
	client   *http.Client
}

// ServiceTopology 一个服务当前在注册中心里的实例
type ServiceTopology struct {
	Service   string   `json:"service"`
	Instances []string `json:"instances"`
	Error     string   `json:"error,omitempty"`
}

// TargetStats 单个 RPC 服务端的抓取结果
type TargetStats struct {
	Target string               `json:"target"`
	OK     bool                 `json:"ok"`
	Error  string               `json:"error,omitempty"`
	Stats  *metrics.ServerStats `json:"stats,omitempty"`
}

func (c *console) topology() []ServiceTopology {
	out := make([]ServiceTopology, 0, len(c.services))
	for _, name := range c.services {
		item := ServiceTopology{Service: name, Instances: []string{}}
		instances, err := c.reg.Discover(name)
		if err != nil {
			item.Error = err.Error()
		} else {
			for _, ins := range instances {
				item.Instances = append(item.Instances, ins.Addr)
			}
		}
		out = append(out, item)
	}
	return out
}

// scrape 并发抓取所有目标, 单个目标失败不影响其余
func (c *console) scrape() []TargetStats {
	out := make([]TargetStats, len(c.targets))

	var wg sync.WaitGroup
	for i, target := range c.targets {
		wg.Add(1)
		go func(i int, target string) {
			defer wg.Done()
			out[i] = TargetStats{Target: target}

			resp, err := c.client.Get("http://" + target + "/stats")
			if err != nil {
				out[i].Error = err.Error()
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				out[i].Error = "HTTP " + resp.Status
				return
			}

			body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if err != nil {
				out[i].Error = err.Error()
				return
			}

			var stats metrics.ServerStats
			if err := json.Unmarshal(body, &stats); err != nil {
				out[i].Error = err.Error()
				return
			}
			out[i].OK = true
			out[i].Stats = &stats
		}(i, target)
	}
	wg.Wait()
	return out
}

func (c *console) handleTopology(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, map[string]interface{}{"topology": c.topology()})
}

func (c *console) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, map[string]interface{}{"targets": c.scrape()})
}

func (c *console) handleOverview(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, map[string]interface{}{
		"topology":    c.topology(),
		"targets":     c.scrape(),
		"timestampMs": time.Now().UnixMilli(),
	})
}

func writeJSON(w http.ResponseWriter, r *http.Request, v interface{}) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET is allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
