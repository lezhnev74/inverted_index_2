package inverted_index_2

// TermValues contain postings for the term (could be doc ids, offsets or whatever)
type TermValues struct {
	Term   []byte
	Values []uint64
}
