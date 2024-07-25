package file

import (
	"errors"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestWriter(t *testing.T) {

	input := []TermValues{
		{[]byte("term1"), []uint64{10, 500, 300}},
		{[]byte("term2"), []uint64{}},
		{[]byte("term3"), []uint64{66, 5513}},
	}

	d := MakeTmpDir()
	defer os.RemoveAll(d)

	// Write data
	w, err := NewWriter(d)
	require.NoError(t, err)
	for _, tv := range input {
		require.NoError(t, w.Append(tv))
	}
	require.NoError(t, w.Close())

	// Read it back
	r, err := NewReader(d, w.GetName(), nil, nil)
	require.NoError(t, err)

	actual := make([]TermValues, 0)
	for {
		tv, err := r.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		require.NoError(t, err)
		actual = append(actual, tv)
	}
	require.NoError(t, r.Close())

	require.Equal(t, input, actual)
}

func TestWriterDirect(t *testing.T) {

	// Direct only keeps one fst file, so each term has only one value (during ingestion)

	input := []TermValues{
		{[]byte("term1"), []uint64{10}},
		{[]byte("term2"), []uint64{11}},
	}

	d := MakeTmpDir()
	defer os.RemoveAll(d)

	// Write data
	w, err := NewDirectWriter(d)
	require.NoError(t, err)
	for _, tv := range input {
		require.NoError(t, w.Append(tv))
	}
	require.NoError(t, w.Close())

	// Read it back
	r, err := NewReader(d, w.GetName(), nil, nil)
	require.NoError(t, err)

	actual := make([]TermValues, 0)
	for {
		tv, err := r.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		require.NoError(t, err)
		actual = append(actual, tv)
	}
	require.NoError(t, r.Close())

	require.Equal(t, input, actual)
}

func MakeTmpDir() string {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	return dir
}
