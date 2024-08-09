package inverted_index_2

import (
	"errors"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"github.com/stretchr/testify/require"
	"os"
	"runtime"
	"testing"
)

func TestPut(t *testing.T) {

	d := MakeTmpDir()
	defer os.RemoveAll(d)
	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	err = ii.Put([][]byte{[]byte("ab1"), []byte("ab2")}, 1)
	require.NoError(t, err)
	err = ii.Put([][]byte{[]byte("ab2"), []byte("cd1")}, 2)
	require.NoError(t, err)

	it, err := ii.Read(nil, nil)
	require.NoError(t, err)
	tvs := make([]file.TermValues, 0)
	for {
		tv, err := it.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		require.NoError(t, err)
		tvs = append(tvs, tv)
	}
	require.NoError(t, it.Close())
	expectedTermValues := []file.TermValues{
		{[]byte("ab1"), []uint32{1}},
		{[]byte("ab2"), []uint32{1, 2}},
		{[]byte("cd1"), []uint32{2}},
	}
	require.Equal(t, expectedTermValues, tvs)
	require.Equal(t, 2, len(ii.shards))

	tvs = nil
	ii = nil
	runtime.GC()

	// Reinitiate index to see that shards are visible
	ii, err = NewInvertedIndex(d)
	require.NoError(t, err)

	it, err = ii.Read(nil, nil)
	require.NoError(t, err)
	tvs = make([]file.TermValues, 0)
	for {
		tv, err := it.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		require.NoError(t, err)
		tvs = append(tvs, tv)
	}
	require.NoError(t, it.Close())
	require.Equal(t, expectedTermValues, tvs)
	require.Equal(t, 2, len(ii.shards))
}
