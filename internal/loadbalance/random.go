package loadbalance

import (
	"math/rand"
	"sync"
	"time"

	"kamaRPC/internal/registry"
)

// Random 随机: 实现最简单, 但短时间内可能连续打到同一台
type Random struct {
	r *rand.Rand
	m sync.Mutex
}

// NewRandom 创建随机策略
func NewRandom() *Random {
	return &Random{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (r *Random) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}

	r.m.Lock()
	defer r.m.Unlock()
	return list[r.r.Intn(len(list))]
}
