package inverted_index_2

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrdering(t *testing.T) {
	l := &Segments{}
	l.add("a", 10, nil, nil)
	l.add("b", 1, nil, nil)
	l.add("c", 3, nil, nil)

	require.Equal(t, 1, l.list[0].terms)
	require.Equal(t, 3, l.list[1].terms)
	require.Equal(t, 10, l.list[2].terms)
}

func TestDetach(t *testing.T) {
	l := &Segments{}
	l.add("a", 10, nil, nil)
	l.add("b", 1, nil, nil)
	l.add("c", 3, nil, nil)

	detach := []*Segment{
		l.list[0],
		l.list[2],
	}
	l.detach(detach)

	require.Len(t, l.list, 1)
	require.Equal(t, 3, l.list[0].terms)

}
