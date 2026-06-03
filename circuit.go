package batchdb

import "sync/atomic"

const (
	circuitClosed   int32 = 0
	circuitOpen     int32 = 1
	circuitHalfOpen int32 = 2
)

type circuitBreaker struct {
	state     atomic.Int32
	failures  atomic.Int64
	threshold int
}

func (cb *circuitBreaker) State() int32 {
	return cb.state.Load()
}

func (cb *circuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(circuitClosed)
}

func (cb *circuitBreaker) RecordFailure() {
	n := cb.failures.Add(1)
	if int(n) >= cb.threshold {
		cb.state.Store(circuitOpen)
	}
}

func (cb *circuitBreaker) TryHalfOpen() bool {
	return cb.state.CompareAndSwap(circuitOpen, circuitHalfOpen)
}

func (cb *circuitBreaker) Reset() {
	cb.failures.Store(0)
	cb.state.Store(circuitClosed)
}
