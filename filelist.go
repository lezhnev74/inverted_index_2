package inverted_index_2

import "sync"

type FileList struct {
	keys []*FileKey
	m    sync.Mutex
}

// FileKey represents a single inverted index segment (possibly multiple files on disk)
type FileKey struct {
	key   string
	terms int
	m     sync.RWMutex
}
