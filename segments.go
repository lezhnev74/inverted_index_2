package inverted_index_2

import "sync"

type Segments struct {
	list []*Segment
	m    sync.RWMutex
}

// Segment represents a single inverted index segment (possibly multiple files on disk)
type Segment struct {
	key   string
	terms int
	m     sync.RWMutex
}

func (s *Segments) safeRead(fn func()) {
	s.m.RLock()
	defer s.m.RUnlock()

	fn()
}

// readLock locks all current segments and returns them
func (s *Segments) readLock() (r []*Segment) {
	s.safeRead(func() {
		for _, segment := range s.list {
			segment.m.RLock()
		}
		r = append(r, s.list...) // copy
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

func (s *Segments) Add(key string, terms int) {
	s.safeWrite(func() {
		s.list = append(s.list, &Segment{key: key, terms: terms})
	})
}

func (s *Segments) Len() (size int) {
	s.safeRead(func() { size = len(s.list) })
	return
}
