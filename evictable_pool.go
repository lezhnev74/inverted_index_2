package inverted_index_2

import (
	"sync"
	"time"
)

type poolItem[T any] struct {
	item     T
	lastUsed time.Time
}

// Pool works as sync.Pool but with eviction settings.
type Pool[T any] struct {
	list    []*poolItem[T]
	m       sync.Mutex
	factory func() T

	// maxAge controls how long an object is allowed to stay in the pool since the last usage
	maxAge time.Duration
}

// Get returns the oldest object from the pool,
// otherwise creates a new one.
func (p *Pool[T]) Get() T {
	p.m.Lock()
	if len(p.list) == 0 {
		p.m.Unlock()
		return p.factory()
	}

	r := p.list[0].item
	p.list = p.list[1:]
	p.m.Unlock()
	return r
}

// Put returns an object to the pool for future re-use
func (p *Pool[T]) Put(r T) {
	p.m.Lock()
	p.list = append(p.list, &poolItem[T]{r, time.Now()})
	p.m.Unlock()
}

func (p *Pool[T]) monitor() {
	t := time.NewTicker(p.maxAge)
	for {
		select {
		case <-t.C:
		}

		p.m.Lock()
		if p.list == nil { // stop monitor (see Close())
			p.m.Unlock()
			t.Stop()
			break
		}
		x := 0
		for _, i := range p.list {
			if time.Now().Sub(i.lastUsed) < p.maxAge {
				p.list[x] = i
				x++
			}
		}
		for j := x; j < len(p.list); j++ {
			p.list[j] = nil // gc
		}
		p.list = p.list[:x]
		p.m.Unlock()
	}
}

func (p *Pool[T]) Close() {
	p.list = nil
}

func NewPool[T any](ttl time.Duration, factory func() T) *Pool[T] {
	p := &Pool[T]{
		list:    make([]*poolItem[T], 0),
		factory: factory,
		maxAge:  ttl,
	}

	go p.monitor()

	return p
}
