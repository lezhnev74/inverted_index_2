package inverted_index_2

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
)

type Segments struct {
	list []*Segment
	m    sync.RWMutex
}

// Segment represents a single inverted index segment (possibly multiple files on disk)
type Segment struct {
	key              string
	terms            int
	minTerm, maxTerm []byte
	m                sync.RWMutex
	merging          atomic.Bool
}

func (s *Segments) safeRead(fn func()) {
	s.m.RLock()
	defer s.m.RUnlock()
	fn()
}

// readLockAll locks all current segments and returns them
func (s *Segments) readLockAll() (r []*Segment) {
	s.safeRead(func() {
		for _, segment := range s.list {
			segment.m.RLock()
			r = append(r, segment)
		}
	})
	return
}

func (s *Segments) readRelease(segments []*Segment) {
	for _, segment := range segments {
		segment.m.RUnlock()
	}
}

func (s *Segments) safeWrite(fn func()) {
	s.m.Lock()
	defer s.m.Unlock()

	fn()
}

// add maintains the order by size (for merging)
func (s *Segments) add(key string, terms int, termMin, termMax []byte) {
	s.safeWrite(func() {
		s.list = append(s.list, &Segment{key: key, terms: terms, minTerm: termMin, maxTerm: termMax})
		slices.SortFunc(s.list, func(a, b *Segment) int { return cmp.Compare(a.terms, b.terms) })
	})
}

func (s *Segments) Len() (size int) {
	s.safeRead(func() { size = len(s.list) })
	return
}

// detach removes merged segments
func (s *Segments) detach(segments []*Segment) {
	s.safeWrite(func() {
		x := 0
		for _, aSegment := range s.list {
			if !slices.Contains(segments, aSegment) {
				s.list[x] = aSegment
				x++
			}
		}
		// free
		for i := x; i < len(s.list); i++ {
			s.list[i] = nil
		}

		s.list = s.list[:x]

		// shrink
		if cap(s.list)/len(s.list) >= 2 {
			s.list = append([]*Segment{}, s.list...)
		}
	})
}
