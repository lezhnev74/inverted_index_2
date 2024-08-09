package inverted_index_2

import (
	"bytes"
	"errors"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sync"
	"testing"
)

func TestConcurrent(t *testing.T) {
	d := MakeTmpDir()
	defer os.RemoveAll(d)
	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		// PUT OPS
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rand.Intn(100); j++ {
				terms := [][]byte{
					[]byte(randomString(10, 20)),
					[]byte(randomString(10, 20)),
					[]byte(randomString(10, 20)),
				}
				slices.SortFunc(terms, bytes.Compare)
				require.NoError(t, ii.Put(terms, uint32(i)))
			}
		}()

		// READ OPS
		wg.Add(1)
		go func() {
			defer wg.Done()
			it, err := ii.Read(nil, nil)
			require.NoError(t, err)
			for {
				_, err := it.Next()
				if errors.Is(err, go_iterators.EmptyIterator) {
					break
				}
				require.NoError(t, err)
			}
			require.NoError(t, it.Close())
		}()
	}
	wg.Wait()

	// MERGE OPS
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			merged, err := ii.Merge(2, 100)
			require.NoError(t, err)
			if merged == 0 {
				break
			}
		}
	}()
	wg.Wait()
}

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

	// Re-init the index to see that shards are visible
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

func TestReadScoped(t *testing.T) {

	d := MakeTmpDir()
	defer os.RemoveAll(d)
	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	require.NoError(t, ii.Put([][]byte{[]byte("aa")}, 1))
	require.NoError(t, ii.Put([][]byte{[]byte("bb")}, 2))
	require.NoError(t, ii.Put([][]byte{[]byte("cc")}, 3))
	require.NoError(t, ii.Put([][]byte{[]byte("dd")}, 4))

	// Read All
	it, err := ii.Read(nil, nil)
	require.NoError(t, err)
	actualValues := go_iterators.ToSlice(it)
	require.NoError(t, it.Close())
	expectedTermValues := []file.TermValues{
		{[]byte("aa"), []uint32{1}},
		{[]byte("bb"), []uint32{2}},
		{[]byte("cc"), []uint32{3}},
		{[]byte("dd"), []uint32{4}},
	}
	require.Equal(t, expectedTermValues, actualValues)

	// Read with Left boundary
	it, err = ii.Read([]byte("a~"), nil)
	require.NoError(t, err)
	actualValues = go_iterators.ToSlice(it)
	require.NoError(t, it.Close())
	expectedTermValues = []file.TermValues{
		{[]byte("bb"), []uint32{2}},
		{[]byte("cc"), []uint32{3}},
		{[]byte("dd"), []uint32{4}},
	}
	require.Equal(t, expectedTermValues, actualValues)

	// Read with Right boundary (INCLUSIVE)
	it, err = ii.Read(nil, []byte("cc"))
	require.NoError(t, err)
	actualValues = go_iterators.ToSlice(it)
	require.NoError(t, it.Close())
	expectedTermValues = []file.TermValues{
		{[]byte("aa"), []uint32{1}},
		{[]byte("bb"), []uint32{2}},
		{[]byte("cc"), []uint32{3}},
	}
	require.Equal(t, expectedTermValues, actualValues)

	// Read with both boundaries (INCLUSIVE)
	it, err = ii.Read([]byte("bb"), []byte("cc"))
	require.NoError(t, err)
	actualValues = go_iterators.ToSlice(it)
	require.NoError(t, it.Close())
	expectedTermValues = []file.TermValues{
		{[]byte("bb"), []uint32{2}},
		{[]byte("cc"), []uint32{3}},
	}
	require.Equal(t, expectedTermValues, actualValues)
}
