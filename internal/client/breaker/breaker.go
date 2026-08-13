package breaker

import (
	"sync"
	"time"
)

// State 熔断器状态
type State int

const (
	// Closed 正常放行
	Closed State = iota
	// Open 熔断打开, 拒绝请求
	Open
	// HalfOpen 半开, 允许少量试探
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker 基于滑动计数窗口的熔断器状态机
type CircuitBreaker struct {
	mu sync.Mutex

	state State

	// 统计数据
	failureCount int
	successCount int

	// 配置参数
	windowSize       int           // 统计窗口大小(次数), 样本不足不判定
	failureThreshold float64       // 失败率阈值
	openTimeout      time.Duration // 熔断持续时间

	// 状态控制
	lastStateChange time.Time
	halfOpenProbe   bool // 半开状态下是否已放行探测请求
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(windowSize int, failureThreshold float64, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            Closed,
		windowSize:       windowSize,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
		lastStateChange:  time.Now(),
	}
}

// Allow 是否允许请求通过
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {

	case Closed:
		return true

	case Open:
		// 熔断时间到了, 进入半开
		if time.Since(cb.lastStateChange) > cb.openTimeout {
			cb.state = HalfOpen
			cb.lastStateChange = time.Now()
			cb.halfOpenProbe = true
			return true
		}
		return false

	case HalfOpen:
		// 只允许一个探测请求, 避免半开阶段被大量请求冲垮
		if cb.halfOpenProbe {
			return false
		}
		cb.halfOpenProbe = true
		return true
	}

	return true
}

// RecordSuccess 记录一次成功
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {

	case Closed:
		cb.successCount++

	case HalfOpen:
		// 探测成功 → 恢复
		cb.toClosed()

	case Open:
		// 理论上不会进入这里
	}
}

// RecordFailure 记录一次失败, 必要时切到 Open
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {

	case Closed:
		cb.failureCount++

		total := cb.failureCount + cb.successCount
		if total < cb.windowSize {
			return
		}

		rate := float64(cb.failureCount) / float64(total)
		if rate >= cb.failureThreshold {
			cb.toOpen()
			return
		}

		cb.resetCounts()

	case HalfOpen:
		// 探测失败 → 重新熔断
		cb.toOpen()

	case Open:
		// 已经熔断, 不处理
	}
}

func (cb *CircuitBreaker) toOpen() {
	cb.state = Open
	cb.lastStateChange = time.Now()
	cb.resetCounts()
	cb.halfOpenProbe = false
}

func (cb *CircuitBreaker) toClosed() {
	cb.state = Closed
	cb.lastStateChange = time.Now()
	cb.resetCounts()
	cb.halfOpenProbe = false
}

func (cb *CircuitBreaker) resetCounts() {
	cb.failureCount = 0
	cb.successCount = 0
}

// State 当前状态
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
