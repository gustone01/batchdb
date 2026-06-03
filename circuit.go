package batchdb

import "sync/atomic"

// 熔断器状态常量。
const (
	circuitClosed   int32 = 0 // 正常状态，允许写入
	circuitOpen     int32 = 1 // 熔断打开，所有写入直接降级到 WAL
	circuitHalfOpen int32 = 2 // 半开状态，尝试探测恢复
)

// circuitBreaker 是一个简单的连续失败计数熔断器。
// 当连续失败次数达到 threshold 时切换到 Open 状态，一次成功即重置。
type circuitBreaker struct {
	state     atomic.Int32 // 当前状态：circuitClosed / circuitOpen / circuitHalfOpen
	failures  atomic.Int64 // 连续失败计数
	threshold int          // 触发熔断的失败次数阈值
}

// State 返回当前熔断器状态。
func (cb *circuitBreaker) State() int32 {
	return cb.state.Load()
}

// RecordSuccess 记录一次写入成功，重置失败计数并关闭熔断器。
func (cb *circuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(circuitClosed)
}

// RecordFailure 记录一次写入失败。连续失败达到阈值时打开熔断器。
func (cb *circuitBreaker) RecordFailure() {
	n := cb.failures.Add(1)
	if int(n) >= cb.threshold {
		cb.state.Store(circuitOpen)
	}
}

// TryHalfOpen 尝试将熔断器从 Open 切换到 HalfOpen，用于探测恢复。
// 仅一个 goroutine 能成功 CAS，返回 true 表示获得探测权。
func (cb *circuitBreaker) TryHalfOpen() bool {
	return cb.state.CompareAndSwap(circuitOpen, circuitHalfOpen)
}

// Reset 强制重置熔断器到 Closed 状态。
func (cb *circuitBreaker) Reset() {
	cb.failures.Store(0)
	cb.state.Store(circuitClosed)
}
