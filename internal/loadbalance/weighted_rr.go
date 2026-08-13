package loadbalance

import (
	"log"
	"sync"

	"kamaRPC/internal/registry"
)

// WeightedRR 平滑加权轮询: 用动态权重实现均匀且平滑的分配,
// 权重 A:5 B:1 时结果是 AAABAA 而不是 AAAAAB
type WeightedRR struct {
	mu            sync.Mutex
	weights       []int // 固定权重
	currentWeight []int // 当前权重(动态变化)
	totalWeight   int   // 权重总和
}

// NewWeightedRR 按实例顺序传入权重列表
func NewWeightedRR(weights []int) *WeightedRR {
	w := &WeightedRR{
		weights:       make([]int, len(weights)),
		currentWeight: make([]int, len(weights)),
	}

	copy(w.weights, weights)

	total := 0
	for i, wt := range w.weights {
		if wt < 0 {
			wt = 0
			w.weights[i] = 0
		}
		total += wt
	}
	w.totalWeight = total

	return w
}

func (w *WeightedRR) Select(list []registry.Instance) registry.Instance {
	if len(list) == 0 {
		return registry.Instance{}
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 演示版约束: 权重列表必须与实例数量一致
	if len(list) != len(w.weights) {
		log.Println("实例数量和权重列表数量不一致")
		return registry.Instance{}
	}

	maxIdx := -1
	// 第一步: 所有节点累加权重
	for i := 0; i < len(list); i++ {
		w.currentWeight[i] += w.weights[i]
		// 第二步: 选中当前权重最大的节点
		if maxIdx == -1 || w.currentWeight[i] > w.currentWeight[maxIdx] {
			maxIdx = i
		}
	}
	// 第三步: 被选中节点减去总权重, 消耗它的优势, 让其他节点有机会追上
	w.currentWeight[maxIdx] -= w.totalWeight
	return list[maxIdx]
}
