package inverted_index_2

import "io"

// TermValues contain postings for the term (could be doc ids, offsets or whatever)
type TermValues struct {
	Term   []byte
	Values []uint64
}

type Iterator interface {
	// Next returns io.EOF if empty. If err is nil, the value is good to use.
	Next() (TermValues, error)
	io.Closer
}
