package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kamaRPC/internal/client/breaker"
	"kamaRPC/internal/core/metrics"
	"kamaRPC/internal/core/transport"
)

// 熔断器的键是 service@addr, 地址本身带冒号也不能拆错
func TestBreakerKeyRoundTrip(t *testing.T) {
	cases := [][2]string{
		{"Arith", "localhost:9090"},
		{"Arith2", "127.0.0.1:9091"},
		{"ArithPB", "[::1]:9090"},
	}
	for _, c := range cases {
		service, addr := splitBreakerKey(breakerKey(c[0], c[1]))
		if service != c[0] || addr != c[1] {
			t.Fatalf("拆分 %q@%q 得到 %q / %q", c[0], c[1], service, addr)
		}
	}
}

func TestClientStatsReportsBreakersAndPools(t *testing.T) {
	c := &Client{
		name:               "test-client",
		metrics:            metrics.NewCollector(),
		breakerWindow:      4,
		breakerThreshold:   0.5,
		breakerOpenTimeout: time.Second,
		poolMaxActive:      2,
	}

	// 造一个已经熔断的实例, 和一个正常的
	open := c.getBreaker("Arith", "localhost:9091")
	for i := 0; i < 4; i++ {
		open.RecordFailure()
	}
	c.getBreaker("Arith", "localhost:9090")

	c.pools.Store("localhost:9090", transport.NewConnectionPool("localhost:9090", 2, 2))
	c.metrics.Observe("Arith", "Add", 2*time.Millisecond, false)

	rec := httptest.NewRecorder()
	c.StatsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 %d", rec.Code)
	}

	var got metrics.ClientStats
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}

	if got.Name != "test-client" {
		t.Fatalf("name = %q", got.Name)
	}
	if len(got.Breakers) != 2 {
		t.Fatalf("应有两个熔断器, 实际 %+v", got.Breakers)
	}

	var opened int
	for _, b := range got.Breakers {
		if b.Service != "Arith" {
			t.Fatalf("service = %q", b.Service)
		}
		if b.State == breaker.Open.String() {
			opened++
			if b.Addr != "localhost:9091" {
				t.Fatalf("熔断的应该是 9091, 实际 %q", b.Addr)
			}
		}
	}
	if opened != 1 {
		t.Fatalf("应有一个处于熔断状态, 实际 %d", opened)
	}

	if len(got.Pools) != 1 || got.Pools[0].MaxActive != 2 {
		t.Fatalf("pools = %+v", got.Pools)
	}
	if len(got.Methods) != 1 || got.Methods[0].Count != 1 {
		t.Fatalf("methods = %+v", got.Methods)
	}
}

func TestClientStatsHandlerRejectsNonGET(t *testing.T) {
	c := &Client{name: "t", metrics: metrics.NewCollector()}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		c.StatsHandler().ServeHTTP(rec, httptest.NewRequest(method, "/stats", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s 应被拒绝, 实际 %d", method, rec.Code)
		}
	}
}

// 被限流挡掉的调用没上网络, 服务端统计不到 —— 客户端必须自己记下来
func TestClientRecordsLocallyRejectedCalls(t *testing.T) {
	c := &Client{
		name:    "t",
		metrics: metrics.NewCollector(),
	}
	c.metrics.Observe("Arith", "Add", 0, true) // 模拟限流拒绝

	stats := c.Stats()
	if len(stats.Methods) != 1 || stats.Methods[0].Errors != 1 {
		t.Fatalf("methods = %+v", stats.Methods)
	}
}
