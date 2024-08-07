package inverted_index_2

import (
	"bytes"
	"encoding/gob"
	"slices"
	"sync"
)

// RemovedLists accumulates removed values.
// During merging of segment files it will discard removed values and possibly terms with no values.
// Each batch of removed values is timestamped and remains in the pool
// until all older files are merged.
type RemovedLists struct {
	// the values are batched as we push a removal list upon source files removal.
	// so a bunch of values appear at the same time.
	// the indexes are unix nano timestamps.
	lists map[int64][]uint64
	m     sync.Mutex
}

func NewRemovedList(lists map[int64][]uint64) *RemovedLists {
	return &RemovedLists{lists: lists}
}

func UnserializeRemovedList(s []byte) (*RemovedLists, error) {
	rl := &RemovedLists{}

	buf := bytes.NewBuffer(s)
	dec := gob.NewDecoder(buf)

	return rl, dec.Decode(&rl.lists)
}

// Put places the slice to the list (note that is does not copy the slice's array)
func (rm *RemovedLists) Put(timestamp int64, values []uint64) {
	rm.m.Lock()
	defer rm.m.Unlock()
	rm.lists[timestamp] = values
}

// Values returns all removed lists combined, and sorted,
// so During the merge it can use binary search.
func (rm *RemovedLists) Values() []uint64 {
	r := make([]uint64, 0)
	for t := range rm.lists {
		r = append(r, rm.lists[t]...)
	}
	slices.Sort(r)
	return r
}

// Sync accepts current file timestamps, so it removed lists for old.
func (rm *RemovedLists) Sync(timestamps []int64) {
	rm.m.Lock()
	defer rm.m.Unlock()

	oldest := slices.Min(timestamps)
	for t := range rm.lists {
		if t < oldest {
			delete(rm.lists, t)
		}
	}
}

func (rm *RemovedLists) Serialize() ([]byte, error) {
	rm.m.Lock()
	defer rm.m.Unlock()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(rm.lists)
	return buf.Bytes(), err
}