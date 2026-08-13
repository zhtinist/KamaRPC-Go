package breaker

import (
	"testing"
	"time"
)

func TestBreakerOpensOnFailureRate(t *testing.T) {
	cb := NewCircuitBreaker(4, 0.5, 50*time.Millisecond)

	// 样本不足时不判定
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != Closed {
		t.Fatalf("expected closed before window is filled, got %s", cb.State())
	}

	cb.RecordSuccess()
	cb.RecordFailure() // 4 个样本, 失败率 0.75 >= 0.5
	if cb.State() != Open {
		t.Fatalf("expected open, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected requests to be rejected while open")
	}
}

func TestBreakerHalfOpenProbe(t *testing.T) {
	cb := NewCircuitBreaker(2, 0.5, 20*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != Open {
		t.Fatalf("expected open, got %s", cb.State())
	}

	time.Sleep(30 * time.Millisecond)

	// 熔断超时后放行一个探测请求
	if !cb.Allow() {
		t.Fatal("expected probe request to be allowed")
	}
	if cb.State() != HalfOpen {
		t.Fatalf("expected half-open, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected only one probe request in half-open")
	}

	// 探测成功则恢复
	cb.RecordSuccess()
	if cb.State() != Closed {
		t.Fatalf("expected closed after successful probe, got %s", cb.State())
	}

	// 探测失败则重新熔断
	cb2 := NewCircuitBreaker(2, 0.5, 20*time.Millisecond)
	cb2.RecordFailure()
	cb2.RecordFailure()
	time.Sleep(30 * time.Millisecond)
	cb2.Allow()
	cb2.RecordFailure()
	if cb2.State() != Open {
		t.Fatalf("expected open after failed probe, got %s", cb2.State())
	}
}
