package file

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"slices"
)

// Bitmask is not thread-safe.
// Each _val file contains each term's segments.
// It is wasteful to save (compressed) segments per term.
// Instead, we go with an optimized approach: save all segments as an array,
// and for each term save the bitmask of the segments.
// We hope that a bitmask will take far less space per term than actual values.
type Bitmask[T comparable] struct {
	values []T
}

func NewBitmask[T comparable](initValues []T) *Bitmask[T] {
	return &Bitmask[T]{values: initValues}
}

func (bm *Bitmask[T]) AllValues() []T {
	return bm.values
}

// Get decodes compressed bitset and returns corresponding values from
// the internal array.
func (bm *Bitmask[T]) Get(encoded []byte) ([]T, error) {
	rb := roaring.New()
	_, err := rb.ReadFrom(bytes.NewBuffer(encoded))
	if err != nil {
		return nil, err
	}

	values := make([]T, 0, rb.GetCardinality())
	it := rb.Iterator()
	for it.HasNext() {
		index := it.Next()
		if int(index) >= len(bm.values) {
			err = fmt.Errorf("bitmask is out of bound: %dth element in %d array", index, len(bm.values))
			return nil, err
		}
		values = append(values, bm.values[index])
	}

	return values, nil
}

// Put accepts a batch of values and returns encoded bitmask for storing on disk.
// Later origianl values can be reclaimed via [Get]
func (bm *Bitmask[T]) Put(values []T) ([]byte, error) {
	b := roaring.BitmapOf()
	for _, v := range values {
		b.Add(bm.indexOf(v))
	}
	return b.ToBytes()
}

// indexOf returns the bit offset for the value,
// if the value is not in the array, it is appended.
// So all the values are unique and bitsets are correct.
func (bm *Bitmask[T]) indexOf(v T) uint32 {
	pos := slices.Index(bm.values, v)
	if pos == -1 {
		bm.values = append(bm.values, v)
		pos = len(bm.values) - 1
	}
	return uint32(pos)
}
