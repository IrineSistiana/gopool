package gopool

import (
	"sync"
	"sync/atomic"
)

var _globalGoPool = NewPool[func()]()

// Run fn in another goroutine.
func Go(fn func()) {
	_globalGoPool.GoJob(Job[func()]{Fn: func(fn func()) { fn() }, Args: fn})
}

func Size() int {
	return _globalGoPool.Size()
}

func GoCreated() uint64 {
	return _globalGoPool.GoCreated()
}

func GoReused() uint64 {
	return _globalGoPool.GoReused()
}

const goPoolSubPoolNum = 8

type Pool[T any] struct {
	sub [goPoolSubPoolNum]shard[T]

	c           atomic.Uint32
	closeOnce   sync.Once
	closeNotify chan struct{}
	gcNotify    *gcNotify
}

// NewPool creates a new goroutine pool.
// It is recommended to use the global funcs Go() in most cases.
// Creating your own pool for a specific func signature can eliminate the allocation of args.
// Idled goroutines will be removed automatically. The pace is depending on the runtime GC.
// After the GC finishing, all workers will be marked as idle. After the GC finishing again, workers
// that are still marked as idle will be removed. This logic is kind same as sync.Pool.
func NewPool[T any]() *Pool[T] {
	p := &Pool[T]{
		closeNotify: make(chan struct{}),
		gcNotify:    newGcNotify(),
	}

	go p.gcLoop()
	return p
}

func (p *Pool[T]) GoJob(j Job[T]) {
	p.sub[p.c.Add(1)%goPoolSubPoolNum].GoJob(j)
}

func (p *Pool[T]) gcLoop() {
	for {
		select {
		case <-p.closeNotify:
			return
		case <-p.gcNotify.C():
			p.gc()
		}
	}
}

func (p *Pool[T]) gc() {
	for i := range p.sub {
		p.sub[i].gc()
	}
}

// Current pool size (aka. the number of current idled workers).
func (p *Pool[T]) Size() (n int) {
	for i := range p.sub {
		n += p.sub[i].Size()
	}
	return n
}

// The number of started workers/goroutines.
func (p *Pool[T]) GoCreated() uint64 {
	var n uint64
	for i := range p.sub {
		n += p.sub[i].GoCreated()
	}
	return n
}

// The number of reused goroutines.
func (p *Pool[T]) GoReused() uint64 {
	var n uint64
	for i := range p.sub {
		n += p.sub[i].GoReused()
	}
	return n
}

// Close the pool gc goroutine and all idled workers. It won't stop any busy worker.
// After the pool being closed, GoArgs() can still be called but it will fallback to "go".
func (p *Pool[T]) Close() {
	p.closeOnce.Do(func() {
		for i := range p.sub {
			p.sub[i].Close()
		}
		close(p.closeNotify)
		p.gcNotify.Stop()
	})
}

type shard[T any] struct {
	m       sync.Mutex
	p       []*goWorker[T]
	minIdle int
	closed  bool

	goCreated atomic.Uint64
	goReused  atomic.Uint64
}

func (p *shard[T]) Size() int {
	p.m.Lock()
	defer p.m.Unlock()
	return len(p.p)
}

func (p *shard[T]) GoCreated() uint64 {
	return p.goCreated.Load()
}

func (p *shard[T]) GoReused() uint64 {
	return p.goReused.Load()
}

func (p *shard[T]) GoJob(j Job[T]) {
	var w *goWorker[T]

	// pop a worker
	p.m.Lock()
	if p.closed {
		p.m.Unlock()
		go j.Fn(j.Args) // closed pool, fallback to "go"
		return
	}
	if len(p.p) > 0 {
		w = p.p[len(p.p)-1]
		p.p[len(p.p)-1] = nil
		p.p = p.p[:len(p.p)-1]
		if len(p.p) < p.minIdle {
			p.minIdle = len(p.p)
		}
	}
	p.m.Unlock()

	if w == nil {
		p.goCreated.Add(1)
		w = p.newWorker()
	} else {
		p.goReused.Add(1)
	}
	w.sendJob(j)
}

func (p *shard[T]) Close() {
	p.m.Lock()
	defer p.m.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	for _, w := range p.p {
		close(w.jobC)
	}
	clear(p.p)
	p.p = nil
}

func (p *shard[T]) gc() {
	const maxBatchSize = 256
	var closeWait [maxBatchSize]*goWorker[T]
	var needEmit = -1

	for {
		p.m.Lock()
		if needEmit == -1 {
			// These workers have been sit idle since latest gc.
			needEmit = p.minIdle
		}
		if needEmit == 0 {
			p.minIdle = len(p.p)
			p.m.Unlock()
			return
		}
		batchSize := min(maxBatchSize, needEmit, len(p.p))
		emitSlice := p.p[len(p.p)-batchSize : len(p.p)]
		copy(closeWait[:], emitSlice)
		clear(emitSlice)
		p.p = p.p[:len(p.p)-batchSize]
		needEmit -= batchSize

		if needEmit == 0 { // gc is done
			if c := cap(p.p); c > 64 && c>>2 > len(p.p) { // len is < 25% of the cap
				p.p = append(make([]*goWorker[T], 0, c>>1), p.p...) // shrink the slice to half of the cap
			}
			p.minIdle = len(p.p)
		}
		p.m.Unlock()

		for i := 0; i < batchSize; i++ {
			close(closeWait[i].jobC)
		}

		if needEmit == 0 {
			return
		}
	}
}

func (p *shard[T]) putIdle(w *goWorker[T]) (ok bool) {
	p.m.Lock()
	if p.closed {
		p.m.Unlock()
		return false
	}
	p.p = append(p.p, w)
	p.m.Unlock()
	return true
}

type goWorker[T any] struct {
	p    *shard[T]
	jobC chan Job[T]
}

type Job[T any] struct {
	Fn   func(T)
	Args T
}

func (w *goWorker[T]) jobLoop() {
	for j := range w.jobC {
		j.Fn(j.Args)
		ok := w.p.putIdle(w)
		if !ok { // pool closed
			return
		}
	}
}

func (w *goWorker[T]) sendJob(j Job[T]) {
	select {
	case w.jobC <- j:
	default:
		panic("blocked job chan")
	}
}

func (p *shard[T]) newWorker() *goWorker[T] {
	w := &goWorker[T]{
		p:    p,
		jobC: make(chan Job[T], 1),
	}
	go w.jobLoop()
	return w
}
