package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ronanh/intcomp"
	"github.com/stretchr/testify/require"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestCompression(t *testing.T) {
	values := []uint32{}
	for i := uint32(0); i < 1000; i++ {
		if rand.IntN(2) == 1 {
			continue
		}
		values = append(values, i)
	}

	b := NewBitmask([]uint32{})
	buf, err := b.Put(values)
	require.NoError(t, err)
	fmt.Printf("buf size: %d for %d items\n", len(buf), len(values))
	compressedIntegers := intcomp.CompressUint32(values, nil)

	buf2 := new(bytes.Buffer)
	binary.Write(buf2, binary.LittleEndian, compressedIntegers)
	fmt.Printf("compressed values take %d bytes\n", buf2.Len())
}

func TestBitmaskPut(t *testing.T) {
	b := NewBitmask[uint32](nil)
	v1, err := b.Put([]uint32{1, 10, 80})
	require.NoError(t, err)
	v2, err := b.Put([]uint32{9, 10, 11})
	require.NoError(t, err)

	buf := bytes.NewBuffer(nil)
	buf.Write(v1)
	buf.Write(v2)

	v1Values, err := b.Get(buf.Bytes()) // this proves serialized value has length embedded
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 10, 80}, v1Values)

	v2Values, err := b.Get(v2)
	require.NoError(t, err)
	slices.Sort(v2Values) // values can be sorted differently
	require.Equal(t, []uint32{9, 10, 11}, v2Values)
}
