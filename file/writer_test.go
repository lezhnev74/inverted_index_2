package file

import (
	"errors"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestWriter(t *testing.T) {

	input := []TermValues{
		{[]byte("term1"), []uint64{10, 500, 300}},
		{[]byte("term2"), []uint64{}},
		{[]byte("term3"), []uint64{66, 5513}},
	}

	d := makeTmpDir()
	defer os.RemoveAll(d)

	// Write data
	w, err := NewWriter(d)
	require.NoError(t, err)
	for _, tv := range input {
		require.NoError(t, w.Append(tv))
	}
	require.NoError(t, w.Close())

	// Read it back
	r, err := NewReader(d, w.GetName())
	require.NoError(t, err)

	actual := make([]TermValues, 0)
	for {
		tv, err := r.Next()
		if errors.Is(err, io.EOF) {
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

	d := makeTmpDir()
	defer os.RemoveAll(d)

	// Write data
	w, err := NewDirectWriter(d)
	require.NoError(t, err)
	for _, tv := range input {
		require.NoError(t, w.Append(tv))
	}
	require.NoError(t, w.Close())

	// Read it back
	r, err := NewReader(d, w.GetName())
	require.NoError(t, err)

	actual := make([]TermValues, 0)
	for {
		tv, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		actual = append(actual, tv)
	}
	require.NoError(t, r.Close())

	require.Equal(t, input, actual)
}
