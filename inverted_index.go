package inverted_index_2

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"os"
	"path"
	"runtime"
	"slices"
	"time"
)

// InvertedIndex manages all index segments, allows concurrent operations.
type InvertedIndex struct {
	segments    Segments
	basedir     string
	fstPool     *Pool[*vellum.Builder]
	removedList *RemovedLists
}

// Put ingests one indexed document (all terms have the same value)
func (ii *InvertedIndex) Put(terms [][]byte, val uint64) error {
	w, err := file.NewDirectWriter(ii.basedir, ii.fstPool.Get())
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

// Remove remembers removed values, later they are accounted during merging.
func (ii *InvertedIndex) Remove(values []uint64) error {
	t := time.Now().UnixNano()
	ii.removedList.Put(t, values)

	return ii.WriteRemovedList()
}

func (ii *InvertedIndex) WriteRemovedList() error {
	s, err := ii.removedList.Serialize()
	if err != nil {
		return fmt.Errorf("write rem list: %w", err)
	}

	filepath := path.Join(ii.basedir, "removed.list")
	err = os.WriteFile(filepath, s, os.ModePerm)
	if err != nil {
		return fmt.Errorf("write rem list: %w", err)
	}

	return nil
}

// Merge selects smallest segments to merge into a bigger one.
// Returns how many segments were merged together.
// Thread-safe.
// Select [MinMerge,MaxMerge] segments for merging.
func (ii *InvertedIndex) Merge(MinMerge, MaxMerge int) (mergedSegmentsLen int, err error) {
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

	removedValues := ii.removedList.Values()
	termsCount := 0
	for {
		tv, err := it.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		} else if err != nil {
			return 0, fmt.Errorf("ii: merge: %w", err)
		}

		i := 0
		for _, v := range tv.Values {
			_, removed := slices.BinarySearch(removedValues, v)
			if removed {
				continue
			}
			tv.Values[i] = v
			i++
		}
		tv.Values = tv.Values[:i]

		if len(tv.Values) == 0 {
			continue
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
	mergedSegmentsLen = len(segments)

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

// makeIterator returns merging iterator to read through all segment files
// like from a simple sorted array.
func (ii *InvertedIndex) makeIterator(segments []*Segment, min, max []byte) (go_iterators.Iterator[file.TermValues], error) {
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

	rl := NewRemovedList(make(map[int64][]uint64))
	remListSerialized, err := os.ReadFile(path.Join(basedir, "removed.list"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("rem list: %w", err)
		}
	} else {
		rl, err = UnserializeRemovedList(remListSerialized)
		if err != nil {
			return nil, fmt.Errorf("rem list: %w", err)
		}
	}

	return &InvertedIndex{
		basedir:     basedir,
		fstPool:     pool,
		removedList: rl,
	}, nil
}
