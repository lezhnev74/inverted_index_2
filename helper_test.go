package inverted_index_2

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"inverted_index_2/file"
	"os"
	"testing"
)

type TestingMachine struct {
	ii  *InvertedIndex
	dir string
	t   *testing.T
}

type IngestCmd map[uint64][]string  // one value for multiple terms (ingestion)
type CompareCmd map[string][]uint64 // multiple values per term

// Run follows commands in the sequence
func (m *TestingMachine) Run(testSequence []any) {
	for _, s := range testSequence {
		switch cmd := s.(type) {
		case CompareCmd:
			expectedTermValues := make([]file.TermValues, 0, len(cmd))
			for t, vs := range cmd {
				expectedTermValues = append(expectedTermValues, file.TermValues{[]byte(t), vs})
			}

			it, err := m.ii.Read(nil, nil)
			require.NoError(m.t, err)

			tvs := go_iterators.ToSlice(it)
			require.NoError(m.t, it.Close())
			require.Equal(m.t, expectedTermValues, tvs)
		case IngestCmd:
			for v, ts := range cmd {
				terms := make([][]byte, len(ts))
				for i, sterm := range ts {
					terms[i] = []byte(sterm)
				}
				err := m.ii.Put(terms, v)
				require.NoError(m.t, err)
			}
		}
	}
}

func (m *TestingMachine) Close() {
	err := m.ii.Close()
	require.NoError(m.t, err)

	err = os.RemoveAll(m.dir)
	require.NoError(m.t, err)
}

func NewMachine(t *testing.T) *TestingMachine {
	d := MakeTmpDir()
	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	return &TestingMachine{
		ii:  ii,
		dir: d,
		t:   t,
	}
}
