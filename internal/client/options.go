package client

import (
	"errors"
	"time"

	"kamaRPC/internal/client/loadbalance"
	"kamaRPC/internal/core/codec"
	"kamaRPC/internal/core/limiter"
)

// ClientOption 函数式选项: 新增配置项不需要改 NewClient 的签名
type ClientOption func(*Client) error

// WithClientCodec 指定序列化协议
func WithClientCodec(t codec.Type) ClientOption {
	return func(c *Client) error {
		cc, err := codec.New(t)
		if err != nil {
			return err
		}
		c.codec = cc
		return nil
	}
}

// WithClientTimeout 指定默认调用超时
func WithClientTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		if d <= 0 {
			return errors.New("client: timeout must be positive")
		}
		c.timeout = d
		return nil
	}
}

// WithClientLoadBalancer 指定负载均衡策略
func WithClientLoadBalancer(lb loadbalance.LoadBalancer) ClientOption {
	return func(c *Client) error {
		if lb == nil {
			return errors.New("client: load balancer is nil")
		}
		c.lb = lb
		return nil
	}
}

// WithClientRateLimit 指定客户端限流速率(每秒令牌数)
func WithClientRateLimit(rate int) ClientOption {
	return func(c *Client) error {
		if rate <= 0 {
			return errors.New("client: rate must be positive")
		}
		c.limiter = limiter.NewTokenBucket(rate)
		return nil
	}
}

// WithClientBreaker 指定熔断参数: 统计窗口 / 失败率阈值 / 熔断持续时间
func WithClientBreaker(windowSize int, failureThreshold float64, openTimeout time.Duration) ClientOption {
	return func(c *Client) error {
		if windowSize <= 0 {
			return errors.New("client: breaker window size must be positive")
		}
		if failureThreshold <= 0 || failureThreshold > 1 {
			return errors.New("client: breaker failure threshold must be in (0, 1]")
		}
		if openTimeout <= 0 {
			return errors.New("client: breaker open timeout must be positive")
		}
		c.breakerWindow = windowSize
		c.breakerThreshold = failureThreshold
		c.breakerOpenTimeout = openTimeout
		return nil
	}
}

// WithClientPoolSize 指定单个节点的最大连接数
func WithClientPoolSize(maxActive int) ClientOption {
	return func(c *Client) error {
		if maxActive <= 0 {
			return errors.New("client: pool size must be positive")
		}
		c.poolMaxActive = maxActive
		return nil
	}
}
