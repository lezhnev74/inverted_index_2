package file

// TermValues contain postings for the term (could be doc ids, offsets or whatever)
type TermValues struct {
	Term   []byte
	Values []uint64
}
