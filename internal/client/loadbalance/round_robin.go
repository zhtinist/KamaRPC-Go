package loadbalance

import (
	"sync/atomic"

	"kamaRPC/internal/core/registry"
)

// RoundRobin 轮询: A → B → C → A, 均匀简单, 但不感知机器性能差异
type RoundRobin struct {
	idx uint64
}

// NewRR 创建轮询策略
func NewRR() *RoundRobin {
	return &RoundRobin{}
}

func (r *RoundRobin) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}
	i := atomic.AddUint64(&r.idx, 1)
	return list[i%uint64(len(list))]
}
