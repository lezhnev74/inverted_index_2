package inverted_index_2

import (
	"fmt"
	go_iterators "github.com/lezhnev74/go-iterators"
	"inverted_index_2/file"
)

// InvertedIndex manages all index segments, allows concurrent operations.
type InvertedIndex struct {
	segments Segments
	basedir  string
}

// Put ingests one indexed document (all terms have the same value)
func (ii *InvertedIndex) Put(terms [][]byte, val uint64) error {
	w, err := file.NewWriter(ii.basedir)
	if err != nil {
		return fmt.Errorf("ii: put: %w", err)
	}

	ii.segments.Add(w.GetName(), len(terms))
	for _, term := range terms {
		err = w.Append(file.TermValues{term, []uint64{val}})
		if err != nil {
			return fmt.Errorf("index put: %w", err)
		}
	}

	return w.Close()
}

// Read returns merging iterator for all available index segments.
// Must close to release segments for merging.
// min,max (inclusive) allows to skip irrelevant terms.
func (ii *InvertedIndex) Read(min, max []byte) (go_iterators.Iterator[file.TermValues], error) {
	readers := make([]go_iterators.Iterator[file.TermValues], 0, ii.segments.Len())

	segments := ii.segments.readLock()

	for _, segment := range segments {
		r, err := file.NewReader(ii.basedir, segment.key)
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

func (ii *InvertedIndex) Close() error {
	return nil
}

func NewInvertedIndex(basedir string) (*InvertedIndex, error) {
	return &InvertedIndex{
		basedir: basedir,
	}, nil
}
