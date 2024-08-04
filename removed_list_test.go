package inverted_index_2

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRemovedLists(t *testing.T) {
	rl := NewRemovedList(make(map[int64][]uint64))

	t1 := time.Now().UnixNano()
	rl.Put(t1, []uint64{1, 5, 10})

	t2 := time.Now().UnixNano()
	rl.Put(t2, []uint64{2, 20, 30})

	require.Equal(t, []uint32{1, 2, 5, 10, 20, 30}, rl.Values())

	t3 := time.Now().UnixNano()
	rl.Sync([]int64{t2, t3})

	require.Equal(t, []uint32{2, 20, 30}, rl.Values())
}

func TestSerialize(t *testing.T) {
	rl := NewRemovedList(make(map[int64][]uint64))
	rl.Put(time.Now().UnixNano(), []uint64{1, 5, 10})
	rl.Put(time.Now().UnixNano(), []uint64{2, 20, 30})
	b, err := rl.Serialize()
	require.NoError(t, err)

	rl2, err := UnserializeRemovedList(b)
	require.NoError(t, err)

	require.Equal(t, rl.lists, rl2.lists)
}
