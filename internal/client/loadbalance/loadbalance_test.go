package loadbalance

import (
	"strings"
	"testing"

	"kamaRPC/internal/core/registry"
)

var testList = []registry.Instance{{Addr: "A"}, {Addr: "B"}}

// 权重 5:1 时平滑加权轮询应产出 AAABAA, 而不是 AAAAAB
func TestWeightedRRIsSmooth(t *testing.T) {
	w := NewWeightedRR([]int{5, 1})

	var sb strings.Builder
	for i := 0; i < 6; i++ {
		sb.WriteString(w.Select(testList).Addr)
	}

	if got, want := sb.String(), "AAABAA"; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestWeightedRRRejectsMismatchedWeights(t *testing.T) {
	w := NewWeightedRR([]int{1, 1, 1})
	if ins := w.Select(testList); ins.Addr != "" {
		t.Fatalf("expected empty instance, got %q", ins.Addr)
	}
}

func TestRoundRobinCyclesAndHandlesEmpty(t *testing.T) {
	rr := NewRR()

	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		seen[rr.Select(testList).Addr]++
	}
	if seen["A"] != 2 || seen["B"] != 2 {
		t.Fatalf("expected even split, got %v", seen)
	}

	if ins := rr.Select(nil); ins.Addr != "" {
		t.Fatalf("expected empty instance for empty list, got %q", ins.Addr)
	}
}

func TestRandomHandlesEmpty(t *testing.T) {
	r := NewRandom()
	if ins := r.Select(nil); ins.Addr != "" {
		t.Fatalf("expected empty instance for empty list, got %q", ins.Addr)
	}
	if ins := r.Select(testList); ins.Addr != "A" && ins.Addr != "B" {
		t.Fatalf("unexpected instance %q", ins.Addr)
	}
}
