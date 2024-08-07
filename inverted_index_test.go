package inverted_index_2

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"sync"
	"testing"
)

func TestInitFromExistingFiles(t *testing.T) {
	d := MakeTmpDir()
	defer os.RemoveAll(d)

	ii, err := NewInvertedIndex(d)
	require.NoError(t, err)

	err = ii.Put([][]byte{[]byte("term1"), []byte("term2")}, 1)
	require.NoError(t, err)
	err = ii.Put([][]byte{[]byte("term2"), []byte("term3")}, 2)
	require.NoError(t, err)

	require.NoError(t, ii.Close())

	// Open again and see the state caught up
	ii, err = NewInvertedIndex(d)
	require.NoError(t, err)
	it, err := ii.Read(nil, nil)
	tvs := go_iterators.ToSlice(it)

	expected := []file.TermValues{
		{[]byte("term1"), []uint64{1}},
		{[]byte("term2"), []uint64{1, 2}},
		{[]byte("term3"), []uint64{2}},
	}
	require.Equal(t, expected, tvs)
}

func TestIngestion(t *testing.T) {
	sequence := []any{
		IngestBulkCmd(map[uint64][]string{
			1: {"term1"},
		}),
		CompareCmd(map[string][]uint64{
			"term1": {1},
		}),
		IngestBulkCmd(map[uint64][]string{
			1: {"term1"}, // idempotency test
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

func TestMerging(t *testing.T) {
	sequence := []any{
		IngestBulkCmd(map[uint64][]string{
			1: {"term1"}, // idempotency test
			2: {"term1", "term2"},
			3: {"term3"},
		}),
		CountSegmentsCmd(3),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(2),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(1),
		MergeCmd([]int{2, 2, 0}), // idempotency test
		CountSegmentsCmd(1),
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

func TestMergeWithRemoval(t *testing.T) {
	sequence := []any{
		IngestBulkCmd(map[uint64][]string{
			1: {"term1", "term3"},
			2: {"term2"},
			3: {"term3"},
		}),
		CountSegmentsCmd(3),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(2),
		RemoveCmd([]uint64{2}),
		MergeCmd([]int{2, 2, 2}),
		CountSegmentsCmd(1),
		CompareCmd(map[string][]uint64{
			"term1": {1},
			"term3": {1, 3},
		}),
		RemoveCmd([]uint64{10}), // invoke sync to disk for the list
		CheckCmd(func(ii *InvertedIndex) {
			require.Equal(t, []uint64{10}, ii.removedList.Values()) // merged value has gone
		}),
	}

	m := NewMachine(t)
	m.Run(sequence)
	m.Close()
}

func TestConcurrentAccess(t *testing.T) {

	sequence := []any{
		IngestBulkCmd(map[uint64][]string{
			1: {"term1"}, // idempotency test
			2: {"term1", "term2"},
			3: {"term3"},
		}),
		MergeCmd([]int{2, 2, 2}),
		CompareCmd(map[string][]uint64{
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

//func TestMergePerformance(t *testing.T) {
//
//	//fst, err := vellum.Open("./fst.sample2")
//	//require.NoError(t, err)
//	//fmt.Printf("Len: %d\n", fst.Len())
//	//return
//
//	//fstIterator, err := fst.Iterator(nil, nil)
//	//for {
//	//	t, _ := fstIterator.Current()
//	//	fmt.Printf("%s,", t)
//	//	err = fstIterator.Next()
//	//	if err != nil {
//	//		break
//	//	}
//	//}
//	//return
//
//	m := NewMachine(t)
//
//	// Ingest segments
//	f, _ := os.Open("./terms.1m.txt")
//	defer f.Close()
//
//	i := uint64(0)
//	scanner := bufio.NewScanner(f)
//
//	terms := make([]string, 0, 1000)
//	for scanner.Scan() {
//		terms = append(terms, scanner.Text())
//		if len(terms) == 1_000 {
//			slices.Sort(terms)
//			m.RunOne(IngestBulkCmd(map[uint64][]string{
//				i: terms,
//			}))
//			terms = terms[:0]
//			i++
//		}
//	}
//
//	// Merge
//	merges := 0
//	for {
//		mergedCount, err := m.ii.Merge(2, 10)
//		require.NoError(t, err)
//		if mergedCount == 0 {
//			break
//		}
//		merges++
//	}
//	fmt.Printf("merges: %d\n", merges)
//	m.Close()
//}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(min, max int) string {
	b := make([]rune, min+rand.Intn(max-min))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
