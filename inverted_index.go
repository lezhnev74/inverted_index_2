package inverted_index_2

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"runtime"
	"time"
)

// InvertedIndex manages all index segments, allows concurrent operations.
type InvertedIndex struct {
	segments Segments
	basedir  string
	fstPool  *Pool[*vellum.Builder]
}

// Put ingests one indexed document (all terms have the same value)
func (ii *InvertedIndex) Put(terms [][]byte, val uint64) error {
	w, err := file.NewWriter(ii.basedir, ii.fstPool.Get())
	if err != nil {
		return fmt.Errorf("ii: put: %w", err)
	}

	for _, term := range terms {
		err = w.Append(file.TermValues{term, []uint64{val}})
		if err != nil {
			return fmt.Errorf("index put: %w", err)
		}
	}

	err = w.Close()
	if err != nil {
		return fmt.Errorf("index put: %w", err)
	}

	// reuse FST
	ii.fstPool.Put(w.GetFst())

	// make the new segment visible
	ii.segments.add(w.GetName(), len(terms))

	return nil
}

// Read returns merging iterator for all available index segments.
// Must close to release segments for merging.
// min,max (inclusive) allows to skip irrelevant terms.
func (ii *InvertedIndex) Read(min, max []byte) (go_iterators.Iterator[file.TermValues], error) {
	segments := ii.segments.readLockAll()
	return ii.makeIterator(segments, min, max)
}

// Merge selects smallest segments to merge into a bigger one.
// Returns how many segments were merged together.
// Thread-safe.
// Select [MinMerge,MaxMerge] segments for merging.
func (ii *InvertedIndex) Merge(MinMerge, MaxMerge int) (mergedSegments int, err error) {
	start := time.Now()

	// Select segments for merge
	segments := make([]*Segment, 0, MaxMerge)
	ii.segments.safeRead(func() {
		limit := MaxMerge
		for _, segment := range ii.segments.list {
			if limit == 0 {
				break
			}
			ok := segment.merging.CompareAndSwap(false, true)
			if ok {
				limit--
				segments = append(segments, segment)
			}
		}
	})
	if len(segments) < MinMerge {
		return 0, nil // nothing to merge
	}

	// Merge the selected
	for _, segment := range segments {
		segment.m.RLock()
	}
	it, err := ii.makeIterator(segments, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("ii: merge: %w", err)
	}

	w, err := file.NewWriter(ii.basedir, ii.fstPool.Get())
	if err != nil {
		return 0, fmt.Errorf("ii: merge: %w", err)
	}

	termsCount := 0
	for {
		tv, err := it.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		} else if err != nil {
			return 0, fmt.Errorf("ii: merge: %w", err)
		}

		err = w.Append(tv)
		if err != nil {
			return 0, fmt.Errorf("ii: merge: %w", err)
		}
		termsCount++
	}
	err = w.Close()
	if err != nil {
		return 0, fmt.Errorf("ii: merge: writer close: %w", err)
	}

	// reuse FST
	ii.fstPool.Put(w.GetFst())

	err = it.Close()
	if err != nil {
		return 0, fmt.Errorf("ii: merge: iterator close: %w", err)
	}
	ii.segments.add(w.GetName(), termsCount)

	// Remove merged segments (make them invisible for new reads)
	ii.segments.detach(segments)
	mergedSegments = len(segments)

	// Wait until no one is reading merged segments
	for _, segment := range segments {
		// wait until nobody is holding the read lock, so the segment can be removed.
		// reads are rare (merge reads are not overlapping) in a typical index usage, so that should not take long
		for !segment.m.TryLock() {
			runtime.Gosched()
		}
		err1 := file.RemoveSegment(ii.basedir, segment.key)
		if err1 != nil {
			err = err1 // report the last error
		}
	}

	fmt.Printf("Merged %d terms in %s\n", termsCount, time.Now().Sub(start).String())

	return
}

func (ii *InvertedIndex) Close() error {
	return nil
}

func (ii *InvertedIndex) makeIterator(
	segments []*Segment,
	min, max []byte,
) (go_iterators.Iterator[file.TermValues], error) {
	readers := make([]go_iterators.Iterator[file.TermValues], 0, ii.segments.Len())
	for _, segment := range segments {
		r, err := file.NewReader(ii.basedir, segment.key, min, max)
		if err != nil {
			return nil, fmt.Errorf("index read: %w", err)
		}
		readers = append(readers, r)
	}

	it := go_iterators.NewMergingIterator(readers, file.CompareTermValues, file.MergeTermValues)
	cit := go_iterators.NewClosingIterator[file.TermValues](it, func(err error) error {
		ii.segments.readRelease(segments)
		return err
	})

	return cit, nil
}

func NewInvertedIndex(basedir string) (*InvertedIndex, error) {

	mockWriter := bytes.NewBuffer(nil)
	pool := NewPool(
		10*time.Second,
		func() *vellum.Builder {
			builder, _ := vellum.New(mockWriter, nil)
			return builder
		},
	)

	return &InvertedIndex{
		basedir: basedir,
		fstPool: pool,
	}, nil
}
