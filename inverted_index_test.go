package inverted_index_2

import (
	"bytes"
	"errors"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"github.com/prometheus/procfs"
	"github.com/stretchr/testify/require"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"testing"
	"time"
)

func _TestMemoryLeaks(t *testing.T) {
	p, err := procfs.Self()
	if err != nil {
		log.Fatalf("could not get process: %s", err)
	}

	pm := func() {
		stat, err := p.Stat()
		if err != nil {
			log.Fatalf("could not get process stat: %s", err)
		}
		log.Printf("RSS: %d\n", stat.ResidentMemory())
	}
	go func() {
		for {
			time.Sleep(time.Second)
			//runtime.GC()
			//debug.FreeOSMemory()
			pm()
		}
	}()

	d := "/home/dmitry/Code/go/src/heaplog_2024/_local/s2"

	pm()
	debug.SetGCPercent(10)

	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	log.Printf("II loaded: %p", ii)
	pm()
	runtime.GC()
	pm()
	debug.FreeOSMemory()
	pm()
}

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

func TestSearchByPrefix(t *testing.T) {
	d := MakeTmpDir()
	defer os.RemoveAll(d)
	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	require.NoError(t, ii.Put([][]byte{[]byte("a12")}, 1))
	require.NoError(t, ii.Put([][]byte{[]byte("a13")}, 2))
	require.NoError(t, ii.Put([][]byte{[]byte("a20")}, 3))
	require.NoError(t, ii.Put([][]byte{[]byte("a30")}, 4))
	//
	require.NoError(t, ii.Put([][]byte{[]byte("termA")}, 5))
	require.NoError(t, ii.Put([][]byte{[]byte("termB")}, 6))
	require.NoError(t, ii.Put([][]byte{[]byte("termC")}, 7))

	//
	found, err := ii.PrefixSearch([][]byte{[]byte("a1")})
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{"a1": {1, 2}}, found)

	//
	found, err = ii.PrefixSearch([][]byte{[]byte("term"), []byte("unknown")})
	require.NoError(t, err)
	require.Equal(t, map[string][]uint32{"term": {5, 6, 7}}, found)
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
