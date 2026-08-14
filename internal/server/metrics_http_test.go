package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kamaRPC/pkg/api"
)

func TestStatsHandlerReportsServicesAndMethods(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := srv.Register("Arith", &api.Arith{}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// 模拟两次调用: 一次成功一次失败
	srv.Metrics().Observe("Arith", "Add", 2*time.Millisecond, false)
	srv.Metrics().Observe("Arith", "Div", 3*time.Millisecond, true)

	rec := httptest.NewRecorder()
	srv.StatsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}

	var got StatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}

	if len(got.Services) != 1 || got.Services[0].Name != "Arith" {
		t.Fatalf("services = %+v", got.Services)
	}
	if len(got.Services[0].Methods) != 4 {
		t.Fatalf("Arith 应有 4 个方法, 实际 %v", got.Services[0].Methods)
	}
	if len(got.Methods) != 2 {
		t.Fatalf("应有两条方法指标, 实际 %+v", got.Methods)
	}
	if got.PID == 0 || got.UptimeSec <= 0 {
		t.Fatalf("进程信息缺失: %+v", got)
	}

	var errCount uint64
	for _, m := range got.Methods {
		errCount += m.Errors
	}
	if errCount != 1 {
		t.Fatalf("错误数应为 1, 实际 %d", errCount)
	}
}

// 这是只读端点, 不该接受任何会改状态的请求
func TestStatsHandlerRejectsNonGET(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		rec := httptest.NewRecorder()
		srv.StatsHandler().ServeHTTP(rec, httptest.NewRequest(method, "/stats", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s 应被拒绝, 实际状态码 %d", method, rec.Code)
		}
	}
}
