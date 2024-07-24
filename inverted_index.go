package inverted_index_2

import (
	"fmt"
	"inverted_index_2/file"
)

// InvertedIndex manages all index segments, allows concurrent operations.
type InvertedIndex struct {
	segments FileList
	basedir  string
}

func (ii *InvertedIndex) put(terms [][]byte, val uint64) error {
	w, err := file.NewWriter(ii.basedir)
	if err != nil {
		return fmt.Errorf("ii: put: %w", err)
	}

	ii.segments.m.Lock()
	ii.segments.keys = append(ii.segments.keys, &FileKey{key: w.GetName(), terms: len(terms)})
	ii.segments.m.Unlock()

	return nil
}
