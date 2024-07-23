package inverted_index_2

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
)

// ReaderCache is used to re-fetch fresh values from i-th reader
// after the original value was pending
type ReaderCache struct {
	r       Iterator
	tv      TermValues
	pending bool
}

// MergingIterator accepts N readers and outputs pending TermValues
// When all readers return EOF, so does this iterator.
type MergingIterator struct {
	// buf contains topmost value for each reader,
	// when reader returns EOF, it is removed from the buf
	buf []ReaderCache
}

func (mi *MergingIterator) Next() (mergedTv TermValues, err error) {

	// It fetches from all readers into the buffer.
	// Whenever the value is pending the reader is marked as pending.
	// Pending readers are fetched on each run until exhausted.
	// When no readers left, it returns EOF.

	// 1. Fetch values from pending iterators
	err = mi.fetch()
	if err != nil {
		err = fmt.Errorf("merge iterator: %w", err)
		return
	}

	if len(mi.buf) == 0 {
		err = io.EOF
		return
	}

	// 2. Sort fetched values
	slices.SortFunc(mi.buf, func(a, b ReaderCache) int {
		return bytes.Compare(a.tv.Term, b.tv.Term)
	})

	// 3. Merge the first term
	for i := range mi.buf {
		if i == 0 {
			mergedTv = mi.buf[i].tv // the first term goes out
			mi.buf[i].pending = true
			continue
		}

		if !slices.Equal(mergedTv.Term, mi.buf[i].tv.Term) {
			break // a non-equal term will go out next time
		}
		mergedTv.Values = append(mergedTv.Values, mi.buf[i].tv.Values...) // another equal term is pending
		mi.buf[i].pending = true
	}

	// Make the pending values ordered and unique:
	slices.Sort(mergedTv.Values)
	mergedTv.Values = slices.Compact(mergedTv.Values)

	return
}

func (mi *MergingIterator) Close() (err error) {
	for _, rc := range mi.buf {
		lastErr := rc.r.Close()
		if lastErr != nil && err == nil {
			err = lastErr // remember the first one
		}
	}
	return
}

// fetch pulls data from each iterator which value was used in the merging,
// so the buffer contains the topmost value from each iterator.
func (mi *MergingIterator) fetch() (err error) {
	var i int
	for j, rc := range mi.buf {
		if !rc.pending {
			mi.buf[i] = rc // keep in the buf
			i++
			continue
		}
		rc.tv, err = rc.r.Next()
		if err == nil {
			rc.pending = false // just fetched
			mi.buf[i] = rc     // keep in the buf
			i++
			continue
		}
		if errors.Is(err, io.EOF) {
			// exhausted normally
			err = rc.r.Close()
			mi.buf[j].r = nil // gc
			continue
		}
		return err // something bad happened in the underlying iterator
	}
	mi.buf = mi.buf[:i]
	return nil
}

func NewMergingIterator(readers []Iterator) *MergingIterator {
	buf := make([]ReaderCache, 0, len(readers))
	for _, r := range readers {
		buf = append(buf, ReaderCache{r: r, pending: true})
	}

	return &MergingIterator{
		buf: buf,
	}
}
