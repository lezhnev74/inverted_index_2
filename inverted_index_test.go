package inverted_index_2

import (
	"os"
	"testing"
)

func TestIngestion(t *testing.T) {
	sequence := []any{
		IngestCmd(map[uint64][]string{
			1: {"term1"},
			2: {"term1", "term2"},
			3: {"term3"},
		}),
		CompareCmd(map[string][]uint64{
			"term1": {1, 2},
			"term2": {2},
			"term3": {3},
		}),
	}

	m := NewMachine(t)
	m.Run(sequence)
	m.Close()
}

func MakeTmpDir() string {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	return dir
}
