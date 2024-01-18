package gopool

import (
	"runtime"
	"sync/atomic"
	"time"
)

type gcNotify struct {
	c       chan time.Time
	stopped atomic.Bool
}

func newGcNotify() *gcNotify {
	n := &gcNotify{
		c: make(chan time.Time, 1),
	}
	runtime.SetFinalizer(&gcSentinel{n: n}, sentinelFinalizer)
	return n
}

func (n *gcNotify) Stop() {
	n.stopped.Store(true)
}

func (n *gcNotify) C() <-chan time.Time {
	return n.c
}

type gcSentinel struct {
	n *gcNotify
}

func sentinelFinalizer(s *gcSentinel) {
	if s.n.stopped.Load() {
		return
	}
	select {
	case s.n.c <- time.Now():
	default:
	}
	runtime.SetFinalizer(s, sentinelFinalizer) // loop
}
