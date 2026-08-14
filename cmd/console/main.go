// console 是 KamaRPC 的 Web 监控面板。
//
// 前后端分离: 这个进程是后端, 只出 JSON API 并托管前端静态文件;
// 前端是纯静态页面(无构建步骤、无外部依赖), 靠轮询这些 API 刷新。
//
//	GET /api/topology  从 etcd 读服务与实例(反映注册发现的实时状态)
//	GET /api/stats     抓取各服务端与客户端的只读指标接口并汇总
//	GET /api/overview  上面两者合一, 面板默认用这个, 一次请求拿全
package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
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
	clients  = flag.String("clients", "localhost:9290,localhost:9291", "各客户端的指标接口地址, 逗号分隔")
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
		clients:  splitAndTrim(*clients),
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
	targets  []string // 服务端指标接口
	clients  []string // 客户端指标接口
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

// ClientTargetStats 单个客户端的抓取结果
type ClientTargetStats struct {
	Target string               `json:"target"`
	OK     bool                 `json:"ok"`
	Error  string               `json:"error,omitempty"`
	Stats  *metrics.ClientStats `json:"stats,omitempty"`
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

// fetchStats 抓取一个 /stats 端点并解析成 T
func fetchStats[T any](hc *http.Client, target string) (*T, error) {
	resp, err := hc.Get("http://" + target + "/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}

	// 限制读取长度: 面板不该因为对端返回一个超大响应就把自己撑爆
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	var out T
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// scrapeServers 并发抓取所有服务端, 单个失败不影响其余
func (c *console) scrapeServers() []TargetStats {
	out := make([]TargetStats, len(c.targets))

	var wg sync.WaitGroup
	for i, target := range c.targets {
		wg.Add(1)
		go func(i int, target string) {
			defer wg.Done()
			out[i] = TargetStats{Target: target}
			stats, err := fetchStats[metrics.ServerStats](c.client, target)
			if err != nil {
				out[i].Error = err.Error()
				return
			}
			out[i].OK, out[i].Stats = true, stats
		}(i, target)
	}
	wg.Wait()
	return out
}

// scrapeClients 并发抓取所有客户端
func (c *console) scrapeClients() []ClientTargetStats {
	out := make([]ClientTargetStats, len(c.clients))

	var wg sync.WaitGroup
	for i, target := range c.clients {
		wg.Add(1)
		go func(i int, target string) {
			defer wg.Done()
			out[i] = ClientTargetStats{Target: target}
			stats, err := fetchStats[metrics.ClientStats](c.client, target)
			if err != nil {
				out[i].Error = err.Error()
				return
			}
			out[i].OK, out[i].Stats = true, stats
		}(i, target)
	}
	wg.Wait()
	return out
}

func (c *console) handleTopology(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, map[string]interface{}{"topology": c.topology()})
}

func (c *console) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, map[string]interface{}{
		"targets": c.scrapeServers(),
		"clients": c.scrapeClients(),
	})
}

func (c *console) handleOverview(w http.ResponseWriter, r *http.Request) {
	// 服务端与客户端并行抓, 拓扑读的是本地缓存, 顺序取即可
	var (
		wg      sync.WaitGroup
		servers []TargetStats
		clients []ClientTargetStats
	)
	wg.Add(2)
	go func() { defer wg.Done(); servers = c.scrapeServers() }()
	go func() { defer wg.Done(); clients = c.scrapeClients() }()
	wg.Wait()

	writeJSON(w, r, map[string]interface{}{
		"topology":    c.topology(),
		"targets":     servers,
		"clients":     clients,
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
