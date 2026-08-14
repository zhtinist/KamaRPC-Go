package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestCollectorCountsAndPercentiles(t *testing.T) {
	c := NewCollector()

	// 90 个 1ms + 10 个 100ms, 其中 5 个失败
	for i := 0; i < 90; i++ {
		c.Observe("Arith", "Add", time.Millisecond, i < 5)
	}
	for i := 0; i < 10; i++ {
		c.Observe("Arith", "Add", 100*time.Millisecond, false)
	}

	stats := c.Snapshot()
	if len(stats) != 1 {
		t.Fatalf("got %d 条统计, want 1", len(stats))
	}

	s := stats[0]
	if s.Service != "Arith" || s.Method != "Add" {
		t.Fatalf("unexpected key: %+v", s)
	}
	if s.Count != 100 || s.Errors != 5 {
		t.Fatalf("count=%d errors=%d, want 100/5", s.Count, s.Errors)
	}
	// P50 落在 1ms 那批里, P99 落在 100ms 那批里
	if s.P50MS > 2 {
		t.Fatalf("P50 = %.2fms, 应该在 1ms 量级", s.P50MS)
	}
	if s.P99MS < 50 {
		t.Fatalf("P99 = %.2fms, 应该在 100ms 量级", s.P99MS)
	}
	// 平均值: (90*1 + 10*100)/100 ≈ 10.9ms
	if s.AvgMS < 9 || s.AvgMS > 13 {
		t.Fatalf("平均延迟 %.2fms 不在预期范围", s.AvgMS)
	}
}

func TestCollectorSeparatesMethods(t *testing.T) {
	c := NewCollector()
	c.Observe("Arith", "Add", time.Millisecond, false)
	c.Observe("Arith", "Sub", time.Millisecond, false)
	c.Observe("Other", "Add", time.Millisecond, false)

	if got := len(c.Snapshot()); got != 3 {
		t.Fatalf("got %d 条统计, want 3", got)
	}
}

// nil 采集器必须是安全的空操作, 这样调用方不必到处判空
func TestNilCollectorIsNoop(t *testing.T) {
	var c *Collector
	c.Observe("Arith", "Add", time.Millisecond, false)
	if s := c.Snapshot(); s != nil {
		t.Fatalf("nil 采集器不应返回数据: %v", s)
	}
}

func TestCollectorConcurrent(t *testing.T) {
	c := NewCollector()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.Observe("Arith", "Add", time.Duration(j)*time.Microsecond, j%10 == 0)
			}
			if i%4 == 0 {
				c.Snapshot()
			}
		}(i)
	}
	wg.Wait()

	stats := c.Snapshot()
	if len(stats) != 1 || stats[0].Count != 32*200 {
		t.Fatalf("并发计数丢失: %+v", stats)
	}
	if stats[0].Errors != 32*20 {
		t.Fatalf("错误数不对: %d", stats[0].Errors)
	}
}
