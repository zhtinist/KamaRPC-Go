// Package metrics 采集按「服务.方法」维度的调用量、错误数与延迟分布。
//
// 设计取舍:
//   - 只记累计值, 不在这里算 QPS —— 速率由消费方(监控面板)对两次快照做差得出,
//     这样采集侧无需维护时间窗口, 也不会因为采集间隔不同而失真
//   - 延迟用固定分桶的直方图而不是保留原始样本: 内存恒定、并发只需原子加,
//     代价是分位数为近似值(取所在桶的上界), 对监控足够
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// bucketBoundsUS 延迟直方图的桶上界(微秒), 覆盖 10µs ~ 10s
var bucketBoundsUS = [...]int64{
	10, 20, 50, 100, 200, 500,
	1_000, 2_000, 5_000, 10_000, 20_000, 50_000,
	100_000, 200_000, 500_000, 1_000_000, 10_000_000,
}

const bucketCount = len(bucketBoundsUS) + 1 // 末尾一个溢出桶

type methodKey struct {
	service string
	method  string
}

type methodMetrics struct {
	count   atomic.Uint64
	errors  atomic.Uint64
	totalUS atomic.Uint64
	buckets [bucketCount]atomic.Uint64
}

// Collector 并发安全的指标采集器
type Collector struct {
	mu      sync.RWMutex
	methods map[methodKey]*methodMetrics
}

// NewCollector 创建采集器
func NewCollector() *Collector {
	return &Collector{methods: make(map[methodKey]*methodMetrics, 16)}
}

// Observe 记录一次调用
func (c *Collector) Observe(service, method string, d time.Duration, failed bool) {
	if c == nil {
		return
	}

	m := c.methodFor(service, method)
	m.count.Add(1)
	if failed {
		m.errors.Add(1)
	}

	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	m.totalUS.Add(uint64(us))
	m.buckets[bucketIndex(us)].Add(1)
}

func (c *Collector) methodFor(service, method string) *methodMetrics {
	k := methodKey{service, method}

	c.mu.RLock()
	m, ok := c.methods[k]
	c.mu.RUnlock()
	if ok {
		return m
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok = c.methods[k]; ok {
		return m
	}
	m = &methodMetrics{}
	c.methods[k] = m
	return m
}

func bucketIndex(us int64) int {
	for i, bound := range bucketBoundsUS {
		if us <= bound {
			return i
		}
	}
	return bucketCount - 1
}

// MethodStat 单个「服务.方法」的累计指标快照
type MethodStat struct {
	Service   string  `json:"service"`
	Method    string  `json:"method"`
	Count     uint64  `json:"count"`
	Errors    uint64  `json:"errors"`
	AvgMS     float64 `json:"avgMs"`
	P50MS     float64 `json:"p50Ms"`
	P90MS     float64 `json:"p90Ms"`
	P99MS     float64 `json:"p99Ms"`
	MaxSeenMS float64 `json:"maxSeenMs"`
}

// Snapshot 返回当前所有方法的累计指标
func (c *Collector) Snapshot() []MethodStat {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	keys := make([]methodKey, 0, len(c.methods))
	vals := make([]*methodMetrics, 0, len(c.methods))
	for k, v := range c.methods {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	c.mu.RUnlock()

	stats := make([]MethodStat, 0, len(keys))
	for i, k := range keys {
		m := vals[i]
		count := m.count.Load()

		var counts [bucketCount]uint64
		for j := range counts {
			counts[j] = m.buckets[j].Load()
		}

		stat := MethodStat{
			Service: k.service,
			Method:  k.method,
			Count:   count,
			Errors:  m.errors.Load(),
		}
		if count > 0 {
			stat.AvgMS = float64(m.totalUS.Load()) / float64(count) / 1000
			stat.P50MS = percentileMS(counts, count, 0.50)
			stat.P90MS = percentileMS(counts, count, 0.90)
			stat.P99MS = percentileMS(counts, count, 0.99)
			stat.MaxSeenMS = maxSeenMS(counts)
		}
		stats = append(stats, stat)
	}
	return stats
}

// percentileMS 从直方图求近似分位数, 返回所在桶的上界(毫秒)
func percentileMS(counts [bucketCount]uint64, total uint64, p float64) float64 {
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * p)
	if target == 0 {
		target = 1
	}

	var cum uint64
	for i, n := range counts {
		cum += n
		if cum >= target {
			return boundMS(i)
		}
	}
	return boundMS(bucketCount - 1)
}

func maxSeenMS(counts [bucketCount]uint64) float64 {
	for i := bucketCount - 1; i >= 0; i-- {
		if counts[i] > 0 {
			return boundMS(i)
		}
	}
	return 0
}

func boundMS(i int) float64 {
	if i >= len(bucketBoundsUS) {
		// 溢出桶: 用最后一个上界表示"至少这么大"
		return float64(bucketBoundsUS[len(bucketBoundsUS)-1]) / 1000
	}
	return float64(bucketBoundsUS[i]) / 1000
}
