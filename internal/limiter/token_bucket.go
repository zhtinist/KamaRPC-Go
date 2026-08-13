package limiter

import (
	"sync"
	"time"
)

// TokenBucket 令牌桶限流: 允许一定突发流量, 比漏桶更适合 RPC 的短时突发模型
type TokenBucket struct {
	tokens int        // 当前可用令牌数
	rate   int        // 每秒补充的令牌数
	mu     sync.Mutex // 保护 tokens
}

// NewTokenBucket 创建令牌桶, 并启动一个协程每秒把桶补满
func NewTokenBucket(rate int) *TokenBucket {
	tb := &TokenBucket{tokens: rate, rate: rate}
	go func() {
		for {
			time.Sleep(time.Second)
			tb.mu.Lock()
			tb.tokens = tb.rate
			tb.mu.Unlock()
		}
	}()
	return tb
}

// Allow 有令牌则扣减放行, 否则拒绝
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}
