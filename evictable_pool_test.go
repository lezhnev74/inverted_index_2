package inverted_index_2

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPoolEvict(t *testing.T) {
	i := 0
	p := NewPool[*int](
		time.Millisecond*20,
		func() (j *int) {
			j = new(int)
			*j = i
			i++
			return
		},
	)

	i1 := p.Get()
	p.Put(i1)                // reset the timer
	time.Sleep(p.maxAge * 2) // wait out
	i2 := p.Get()

	require.NotEqual(t, i1, i2)
	require.Equal(t, i, 2)
}

func TestPoolReuse(t *testing.T) {
	i := 0
	p := NewPool[*int](
		time.Second,
		func() (j *int) {
			j = new(int)
			*j = i
			i++
			return
		},
	)

	i1 := p.Get()
	p.Put(i1)
	i2 := p.Get()

	require.Equal(t, i1, i2)
}
