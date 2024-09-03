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
	slices.SortFunc(terms, bytes.Compare)

	fstBuilder := s.fstPool.Get()
	w, err := file.NewDirectWriter(s.basedir, fstBuilder)
	if err != nil {
		return fmt.Errorf("s: put: %w", err)
	}

	var minTerm, maxTerm []byte
	for _, term := range terms {
		if minTerm == nil {
			minTerm = append([]byte{}, term...)
		}
		maxTerm = append([]byte{}, term...)

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
	s.fstPool.Put(fstBuilder)

	// make the new segment visible
	s.segments.add(w.GetKey(), len(terms), minTerm, maxTerm)

	return nil
}

// Read returns merging iterator for all available index segments.
// Must close to release segments for merging.
// [min,max] (inclusive) allows to skip irrelevant terms.
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
// If there are fewer than reqCount segments, then skip merging,
// otherwise merge at most mCount segments
func (s *Shard) Merge(reqCount, mCount int) (mergedSegmentsLen int, err error) {

	// Here we skip any work if not enough segments exist
	if s.segments.Len() < reqCount {
		return 0, nil
	}

	// Select segments for merge (possibly concurrent call)
	segments := make([]*Segment, 0, mCount)
	s.segments.safeRead(func() {
		for _, segment := range s.segments.list {
			if len(segments) == mCount {
				break
			}
			ok := segment.merging.CompareAndSwap(false, true)
			if ok {
				segments = append(segments, segment)
			}
		}
	})

	// Stop if not enough segments selected
	if len(segments) < 2 {
		return 0, nil
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
	minTerm, maxTerm := []byte(nil), []byte(nil)
	for {
		tv, err := it.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		} else if err != nil {
			return 0, fmt.Errorf("s: merge: %w", err)
		}

		if minTerm == nil {
			minTerm = tv.Term
		}
		maxTerm = tv.Term

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
	s.segments.add(w.GetKey(), termsCount, minTerm, maxTerm)

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
		if err2 != nil {
			err = err2
		}
		return err
	})

	return cit, nil
}

func (s *Shard) MinMax() (terms [][]byte) {
	terms = make([][]byte, 2)
	s.segments.safeRead(func() {
		for _, segment := range s.segments.list {
			if terms[0] == nil {
				terms[0] = segment.minTerm
			} else if bytes.Compare(terms[0], segment.minTerm) > 0 {
				terms[0] = segment.minTerm
			}

			if terms[1] == nil {
				terms[1] = segment.maxTerm
			} else if bytes.Compare(terms[1], segment.maxTerm) < 0 {
				terms[1] = segment.maxTerm
			}
		}
	})
	return
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
		minTerm, err := v.GetMinKey()
		if err != nil {
			return nil, fmt.Errorf("load inverted index: %w", err)
		}
		maxTerm, err := v.GetMaxKey()
		if err != nil {
			return nil, fmt.Errorf("load inverted index: %w", err)
		}
		err = v.Close()
		if err != nil {
			return nil, fmt.Errorf("load inverted index: %w", err)
		}

		segments.add(key, termsCount, minTerm, maxTerm)
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
		term = []byte{byte(0x00), byte(0x00)}
	}

	// two first bytes are used for sharding,
	// parse them as uint16 (which is up to 65536 combinations)
	// and convert to a string (5 bytes long)

	// use only first 10 bits -> 1024 combinations
	key := uint16(term[0])
	key = key << 8
	key += uint16(term[1])
	key = key >> 6

	return fmt.Sprintf("%04d", key)
}
