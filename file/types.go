package file

import "bytes"

// TermValues contain postings for the term (could be doc ids, offsets or whatever)
type TermValues struct {
	Term   []byte
	Values []uint64
}

func MergeTermValues(a, b TermValues) TermValues {
	return TermValues{
		Term:   append([]byte{}, a.Term...),
		Values: append([]uint64{}, append(a.Values, b.Values...)...),
	}
}

func CompareTermValues(a, b TermValues) int {
	return bytes.Compare(a.Term, b.Term)
}
