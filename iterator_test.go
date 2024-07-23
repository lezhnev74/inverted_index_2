package inverted_index_2

import (
	"errors"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMergingIterator(t *testing.T) {
	// Test Plan:
	// 1. Write a few files
	// 2. Open readers for each file
	// 3. Make merging iterator and see the resulting values

	// Exec:
	// 1. Write a few files
	input := [][]TermValues{
		// source 1
		{
			{[]byte("term1"), []uint64{10, 500, 300}},
			{[]byte("term2"), []uint64{1}},
		},
		// source 2
		{
			{[]byte("term2"), []uint64{99, 1}},
			{[]byte("term3"), []uint64{33}},
		},
		// source 3
		{
			{[]byte("term1a"), []uint64{0}},
			{[]byte("term2"), []uint64{5513}},
			{[]byte("term4"), []uint64{987, 11}},
		},
	}
	expected := []TermValues{
		{[]byte("term1"), []uint64{10, 300, 500}},
		{[]byte("term1a"), []uint64{0}},
		{[]byte("term2"), []uint64{1, 99, 5513}},
		{[]byte("term3"), []uint64{33}},
		{[]byte("term4"), []uint64{11, 987}},
	}

	// 2. Open readers for each file
	readers := make([]Iterator, 0, len(input))
	for _, tvs := range input {
		r := go_iterators.NewSliceIterator(tvs)
		readers = append(readers, r)
	}

	// 3. Make merging iterator and see the resulting values
	mi := NewMergingIterator(readers)
	actual := make([]TermValues, 0)
	for {
		tv, err := mi.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}
		require.NoError(t, err)
		actual = append(actual, tv)
	}
	require.NoError(t, mi.Close()) // todo test all readers are closed

	require.Equal(t, expected, actual)
}

//func TestMergingIterator(t *testing.T) {
//	// Test Plan:
//	// 1. Write a few files
//	// 2. Open readers for each file
//	// 3. Make merging iterator and see the resulting values
//
//	// Exec:
//	// 1. Write a few files
//	d := makeTmpDir()
//	defer os.RemoveAll(d)
//
//	input := [][]TermValues{
//		// file 1
//		{
//			{[]byte("term1"), []uint64{10, 500, 300}},
//			{[]byte("term2"), []uint64{1}},
//		},
//		// file 2
//		{
//			{[]byte("term2"), []uint64{99, 1}},
//			{[]byte("term3"), []uint64{33}},
//		},
//		// file 3
//		{
//			{[]byte("term1a"), []uint64{0}},
//			{[]byte("term2"), []uint64{5513}},
//			{[]byte("term4"), []uint64{987, 11}},
//		},
//	}
//	expected := []TermValues{
//		{[]byte("term1"), []uint64{10, 300, 500}},
//		{[]byte("term1a"), []uint64{0}},
//		{[]byte("term2"), []uint64{1, 99, 5513}},
//		{[]byte("term3"), []uint64{33}},
//		{[]byte("term4"), []uint64{11, 987}},
//	}
//
//	filenames := make([]string, 0, len(input))
//	for _, fileInput := range input {
//		w, err := NewWriter(d)
//		require.NoError(t, err)
//		for _, tv := range fileInput {
//			require.NoError(t, w.Append(tv))
//		}
//		require.NoError(t, w.Close())
//		filenames = append(filenames, w.GetName())
//	}
//
//	// 2. Open readers for each file
//	readers := make([]Iterator, 0, len(filenames))
//	for _, filename := range filenames {
//		r, err := NewReader(d, filename)
//		require.NoError(t, err)
//		readers = append(readers, r)
//	}
//
//	readers = readers[:0]
//	for _, tvs := range input {
//		r := go_iterators.NewSliceIterator(tvs)
//		readers = append(readers, r)
//	}
//
//	// 3. Make merging iterator and see the resulting values
//	mi := NewMergingIterator(readers)
//	actual := make([]TermValues, 0)
//	for {
//		tv, err := mi.Next()
//		if errors.Is(err, go_iterators.EmptyIterator) {
//			break
//		}
//		require.NoError(t, err)
//		actual = append(actual, tv)
//	}
//	require.NoError(t, mi.Close()) // todo test all readers are closed
//
//	require.Equal(t, expected, actual)
//}
