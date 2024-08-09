package inverted_index_2

import (
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"os"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Shard manages index in one separate subdirectory.
// It is not aware of siblings.
type Shard struct {
	segments    *Segments
	basedir     string
	fstPool     *Pool[*vellum.Builder]
	removedList *RemovedLists
}

func (s *Shard) GetKey() string {
	return path.Base(s.basedir)
}

// Put ingests one indexed document (all terms have the same value)
func (s *Shard) Put(terms [][]byte, val uint32) error {
	w, err := file.NewDirectWriter(s.basedir, s.fstPool.Get())
	if err != nil {
		return fmt.Errorf("s: put: %w", err)
	}

	for _, term := range terms {
		err = w.Append(file.TermValues{term, []uint32{val}})
		if err != nil {
			return fmt.Errorf("index put: %w", err)
		}
	}

	err = w.Close()
	if err != nil {
		return fmt.Errorf("index put: %w", err)
	}

	// reuse FST
	s.fstPool.Put(w.GetFst())

	// make the new segment visible
	s.segments.add(w.GetKey(), len(terms))

	return nil
}

// Read returns merging iterator for all available index segments.
// Must close to release segments for merging.
// min,max (inclusive) allows to skip irrelevant terms.
func (s *Shard) Read(min, max []byte) (go_iterators.Iterator[file.TermValues], error) {
	segments := s.segments.readLockAll()
	return s.makeIterator(segments, min, max)
}

// Remove remembers removed values, later they are accounted during merging.
func (s *Shard) Remove(values []uint32) (err error) {
	t := time.Now().UnixNano()
	s.removedList.Put(t, values)

	timestamps := []int64{}
	key := 0
	s.segments.safeRead(func() {
		for _, segment := range s.segments.list {
			key, err = strconv.Atoi(segment.key)
			if err != nil {
				err = fmt.Errorf("key to int conversion: %w", err)
				break
			}
			timestamps = append(timestamps, int64(key))
		}
	})
	s.removedList.Sync(timestamps)

	return s.WriteRemovedList()
}

func (s *Shard) WriteRemovedList() error {
	rs, err := s.removedList.Serialize()
	if err != nil {
		return fmt.Errorf("write rem list: %w", err)
	}

	filepath := path.Join(s.basedir, "removed.list")
	err = os.WriteFile(filepath, rs, os.ModePerm)
	if err != nil {
		return fmt.Errorf("write rem list: %w", err)
	}

	return nil
}

// Merge selects smallest segments to merge into a bigger one.
// Returns how many segments were merged together.
// Thread-safe.
// Select [MinMerge,MaxMerge] segments for merging.
func (s *Shard) Merge(MinMerge, MaxMerge int) (mergedSegmentsLen int, err error) {
	start := time.Now()

	// Select segments for merge
	segments := make([]*Segment, 0, MaxMerge)
	s.segments.safeRead(func() {
		limit := MaxMerge
		for _, segment := range s.segments.list {
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
	it, err := s.makeIterator(segments, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("s: merge: %w", err)
	}

	w, err := file.NewWriter(s.basedir, s.fstPool.Get())
	if err != nil {
		return 0, fmt.Errorf("s: merge: %w", err)
	}

	removedValues := s.removedList.Values()
	termsCount := 0
	for {
		tv, err := it.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		} else if err != nil {
			return 0, fmt.Errorf("s: merge: %w", err)
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
			return 0, fmt.Errorf("s: merge: %w", err)
		}
		termsCount++
	}
	err = w.Close()
	if err != nil {
		return 0, fmt.Errorf("s: merge: writer close: %w", err)
	}

	// reuse FST
	s.fstPool.Put(w.GetFst())

	err = it.Close()
	if err != nil {
		return 0, fmt.Errorf("s: merge: iterator close: %w", err)
	}
	s.segments.add(w.GetKey(), termsCount)

	// Remove merged segments (make them invisible for new reads)
	s.segments.detach(segments)
	mergedSegmentsLen = len(segments)

	// Wait until no one is reading merged segments
	for _, segment := range segments {
		// wait until nobody is holding the read lock, so the segment can be removed.
		// reads are rare (merge reads are not overlapping) in a typical index usage, so that should not take long
		for !segment.m.TryLock() {
			runtime.Gosched()
		}
		err1 := file.RemoveSegment(s.basedir, segment.key)
		if err1 != nil {
			err = err1 // report the last error
		}
	}

	fmt.Printf("Merged %d terms in %s\n", termsCount, time.Now().Sub(start).String())

	return
}

func (s *Shard) Close() error {
	return nil
}

// makeIterator returns merging iterator to read through all segment files
// like from a simple sorted array.
func (s *Shard) makeIterator(segments []*Segment, min, max []byte) (go_iterators.Iterator[file.TermValues], error) {
	readers := make([]go_iterators.Iterator[file.TermValues], 0, s.segments.Len())
	for _, segment := range segments {
		r, err := file.NewReader(s.basedir, segment.key, min, max)
		if errors.Is(err, vellum.ErrIteratorDone) {
			// here we checked that this particular segment won't have terms for us
			// so do not include it to the selecting tree iterator.
			continue
		} else if err != nil {
			return nil, fmt.Errorf("index read: %w", err)
		}
		readers = append(readers, r)
	}

	it := go_iterators.NewMergingIterator(readers, file.CompareTermValues, file.MergeTermValues)
	cit := go_iterators.NewClosingIterator[file.TermValues](it, func(err error) error {
		err2 := it.Close()
		s.segments.readRelease(segments)
		if err != nil {
			err = err2
		}
		return err
	})

	return cit, nil
}

func NewShard(basedir string, sharedPool *Pool[*vellum.Builder], sharedRemovedList *RemovedLists) (*Shard, error) {

	// Init segments list (load all existing files)
	segments := &Segments{}
	entries, err := os.ReadDir(basedir)
	if err != nil {
		return nil, fmt.Errorf("load inverted index shard: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), "_fst") {
			continue
		}
		fpath := path.Join(basedir, entry.Name())
		key, _ := strings.CutSuffix(entry.Name(), "_fst")

		v, err := vellum.Open(fpath)
		if err != nil {
			return nil, fmt.Errorf("load inverted index: %w", err)
		}
		termsCount := v.Len()
		err = v.Close()
		if err != nil {
			return nil, fmt.Errorf("load inverted index: %w", err)
		}

		segments.add(key, termsCount)
	}

	return &Shard{
		basedir:     basedir,
		fstPool:     sharedPool,
		removedList: sharedRemovedList,
		segments:    segments,
	}, nil
}

// the key is used to separate shard and name the basedir of the shard
func shardKey(term []byte) string {
	if len(term) < 2 {
		return "00000"
	}

	// two first bytes are used for sharding,
	// parse them as uint16 (which is up to 65536 combinations)
	// and convert to a string (5 bytes long)

	key := uint16(term[0])
	key = key << 8
	key += uint16(term[1])

	return fmt.Sprintf("%05d", key)
}