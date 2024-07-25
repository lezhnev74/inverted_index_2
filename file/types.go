package file

import (
	"bytes"
	"slices"
)

// TermValues contain postings for the term (could be doc ids, offsets or whatever)
type TermValues struct {
	Term   []byte
	Values []uint64
}

func MergeTermValues(a, b TermValues) TermValues {
	uniqueValues := append(append([]uint64{}, a.Values...), b.Values...)
	slices.Sort(uniqueValues)
	uniqueValues = slices.Compact(uniqueValues)
	return TermValues{
		Term:   append([]byte{}, a.Term...),
		Values: append([]uint64{}, uniqueValues...),
	}
}

func CompareTermValues(a, b TermValues) int {
	return bytes.Compare(a.Term, b.Term)
}
