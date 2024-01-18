package gopool

import (
	"io"
	"net"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_gcNotify(t *testing.T) {
	r := require.New(t)

	n := newGcNotify()
	runtime.GC()
	select {
	case <-time.After(time.Second):
		r.FailNow("gc notify timed out")
	case <-n.C():
	}
}

func Test_Pool(t *testing.T) {
	debug.SetGCPercent(-1)
	defer debug.SetGCPercent(100)

	r := require.New(t)
	p := NewPool[*atomic.Uint32]()

	wg := new(sync.WaitGroup)
	cc := 1024
	u := new(atomic.Uint32)
	wg.Add(cc)
	for i := 0; i < cc; i++ {
		p.GoJob(Job[*atomic.Uint32]{
			Fn: func(u *atomic.Uint32) {
				defer wg.Done()
				u.Add(1)
			},
			Args: u,
		})
	}
	wg.Wait()

	r.Equal(cc, int(p.GoCreated())+int(p.GoReused()))
	r.Equal(cc, int(u.Load()))
}

func Test_shard_gc(t *testing.T) {
	r := require.New(t)
	p := new(shard[struct{}])

	// put 1024 workers into pool
	const cc = 1024
	for i := 0; i < cc; i++ {
		w := p.newWorker()
		ok := p.putIdle(w)
		r.True(ok)
	}

	p.gc() // mark all workers

	p.m.Lock()
	idle := p.minIdle
	p.m.Unlock()
	r.Equal(cc, idle)
	r.Equal(cc, p.Size())

	p.gc() // delete all workers
	r.Equal(0, p.Size())
}

func Benchmark_GoPool(b *testing.B) {
	b.ReportAllocs()

	r := require.New(b)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	r.NoError(err)
	defer l.Close()

	type args struct {
		c net.Conn
		b []byte
	}
	p := NewPool[args]()

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}

			go func() {
				buf := make([]byte, 5)
				for {
					_, err := io.ReadFull(c, buf)
					if err != nil {
						return
					}
					if false {
						go func() {
							c.Write(buf)
						}()
					} else {
						p.GoJob(Job[args]{Fn: func(a args) { a.c.Write(a.b) }, Args: args{c: c, b: buf}})
					}
				}
			}()
		}
	}()

	c, err := net.Dial("tcp", l.Addr().String())
	r.NoError(err)

	buf := make([]byte, 5)
	for i := 0; i < b.N; i++ {
		_, err := c.Write(buf)
		if err != nil {
			b.Fatal(err)
		}
		_, err = io.ReadFull(c, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
