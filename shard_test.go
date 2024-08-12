package inverted_index_2

import (
	"bytes"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMinMaxTerms(t *testing.T) {
	d := MakeTmpDir()
	defer os.RemoveAll(d)
	shard := makeTestShard(t, d)

	// Put one
	err := shard.Put([][]byte{[]byte("term1")}, 1)
	require.NoError(t, err)
	minmax := shard.MinMax()
	require.Equal(t, [][]byte{[]byte("term1"), []byte("term1")}, minmax)

	// Put two
	err = shard.Put([][]byte{[]byte("term2")}, 2)
	require.NoError(t, err)
	minmax = shard.MinMax()
	require.Equal(t, [][]byte{[]byte("term1"), []byte("term2")}, minmax)

	// Put many
	err = shard.Put([][]byte{[]byte("term1"), []byte("term2"), []byte("term3")}, 3)
	require.NoError(t, err)
	minmax = shard.MinMax()
	require.Equal(t, [][]byte{[]byte("term1"), []byte("term3")}, minmax)
}

func TestInitFromExistingFiles(t *testing.T) {
	d := MakeTmpDir()
	defer os.RemoveAll(d)
	ii := makeTestShard(t, d)

	err := ii.Put([][]byte{[]byte("term1"), []byte("term2")}, 1)
	require.NoError(t, err)
	err = ii.Put([][]byte{[]byte("term2"), []byte("term3")}, 2)
	require.NoError(t, err)

	require.NoError(t, ii.Close())

	// Open again and see the state caught up
	ii = makeTestShard(t, d)
	it, err := ii.Read(nil, nil)
	tvs := go_iterators.ToSlice(it)

	expected := []file.TermValues{
		{[]byte("term1"), []uint32{1}},
		{[]byte("term2"), []uint32{1, 2}},
		{[]byte("term3"), []uint32{2}},
	}
	require.Equal(t, expected, tvs)
}

func TestIngestion(t *testing.T) {
	sequence := []any{
		IngestBulkCmd(map[uint32][]string{
			1: {"term1"},
		}),
		CompareCmd(map[string][]uint32{
			"term1": {1},
		}),
		IngestBulkCmd(map[uint32][]string{
			1: {"term1"}, // idempotency test
			2: {"term1", "term2"},
			3: {"term3"},
		}),
		CompareCmd(map[string][]uint32{
			"term1": {1, 2},
			"term2": {2},
			"term3": {3},
		}),
	}

	m := NewMachine(t)
	m.Run(sequence)
	m.Close()
}

func TestReadPartial(t *testing.T) {

	initValues := map[uint32][][]byte{
		1: {[]byte("AA")},
		2: {[]byte("BB")},
		3: {[]byte("CC")},
	}

	testRun := func(shouldMerge bool) {
		d := MakeTmpDir()
		defer os.RemoveAll(d)

		ii := makeTestShard(t, d)

		for s, terms := range initValues {
			err := ii.Put(terms, s)
			require.NoError(t, err)
		}

		if shouldMerge {
			_, err := ii.Merge(2, 200)
			require.NoError(t, err)
		}

		// READ BACK: MIDDLE TERMS
		it, err := ii.Read([]byte("AA"), []byte("BB"))
		require.NoError(t, err)
		tvs := go_iterators.ToSlice(it)
		require.Equal(t, []file.TermValues{
			{[]byte("AA"), []uint32{1}},
			{[]byte("BB"), []uint32{2}},
		}, tvs)

		// READ BACK: END TERMS
		it, err = ii.Read([]byte("BB"), []byte("CC"))
		require.NoError(t, err)
		tvs = go_iterators.ToSlice(it)
		require.Equal(t, []file.TermValues{
			{[]byte("BB"), []uint32{2}},
			{[]byte("CC"), []uint32{3}},
		}, tvs)
	}

	// Make the same partial read on merged and then on direct files
	testRun(true)
	testRun(false)
}

func TestMerging(t *testing.T) {
	sequence := []any{
		IngestBulkCmd(map[uint32][]string{
			1: {"term1"}, // idempotency test
			2: {"term1", "term2"},
			3: {"term3"},
		}),
		CountSegmentsCmd(3),
		MergeCmd([]int{3, 2, 2}),
		CountSegmentsCmd(2),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(1),
		MergeCmd([]int{2, 2, 0}), // idempotency test
		CountSegmentsCmd(1),
		CompareCmd(map[string][]uint32{
			"term1": {1, 2},
			"term2": {2},
			"term3": {3},
		}),
	}

	m := NewMachine(t)
	m.Run(sequence)
	m.Close()
}

func TestMergeWithRemoval(t *testing.T) {
	sequence := []any{
		IngestBulkCmd(map[uint32][]string{
			1: {"term1", "term3"},
			2: {"term2"},
			3: {"term3"},
		}),
		CountSegmentsCmd(3),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(2),
		RemoveCmd([]uint32{2}),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(1),
		CompareCmd(map[string][]uint32{
			"term1": {1},
			"term3": {1, 3},
		}),
		RemoveCmd([]uint32{10}), // invoke sync to disk for the list
		CheckCmd(func(ii *Shard) {
			require.Equal(t, []uint32{10}, ii.removedList.Values()) // merged value has gone
		}),
	}

	m := NewMachine(t)
	m.Run(sequence)
	m.Close()
}

func TestConcurrentAccess(t *testing.T) {

	sequence := []any{
		IngestBulkCmd(map[uint32][]string{
			1: {"term1"}, // idempotency test
			2: {"term1", "term2"},
			3: {"term3"},
		}),
		MergeCmd([]int{2, 2, 2}),
		CompareCmd(map[string][]uint32{
			"term1": {1, 2},
			"term2": {2},
			"term3": {3},
		}),
	}

	m := NewMachine(t)
	begin := make(chan int)
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-begin
			m.Run(sequence)
		}()
	}

	close(begin)
	wg.Wait()
	m.Close()
}

func MakeTmpDir() string {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	return dir
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(min, max int) string {
	b := make([]rune, min+rand.Intn(max-min))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func makeTestShard(t *testing.T, dir string) *Shard {
	mockWriter := bytes.NewBuffer(nil)
	pool := NewPool(
		10*time.Second,
		func() *vellum.Builder {
			builder, _ := vellum.New(mockWriter, nil)
			return builder
		},
	)

	// Init removed list (load from disk if exists)
	rl := NewRemovedList(make(map[int64][]uint32))

	shard, err := NewShard(dir, pool, rl)
	require.NoError(t, err)

	return shard
}
