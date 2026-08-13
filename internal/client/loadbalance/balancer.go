package loadbalance

import "kamaRPC/internal/core/registry"

// LoadBalancer 负载均衡策略接口, 实现可插拔
type LoadBalancer interface {
	Select([]registry.Instance) registry.Instance
}
