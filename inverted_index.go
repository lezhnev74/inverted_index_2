package inverted_index_2

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"
)

type InvertedIndex struct {
	shards  []*Shard
	shardsM sync.RWMutex

	basedir     string
	fstPool     *Pool[*vellum.Builder]
	removedList *RemovedLists
}

type ShardDescriptor struct {
	*Shard
	min, max []byte
}

// Put spreads terms into shards.
// terms must be sorted.
func (ii *InvertedIndex) Put(terms [][]byte, val uint32) error {

	termsIt := go_iterators.NewSliceIterator(terms)
	shardingIterator := go_iterators.NewGroupingIterator(termsIt, func(t []byte) any { return shardKey(t) })

	for {
		termsGroup, err := shardingIterator.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		}

		key := shardKey(termsGroup[0])
		shard := ii.findShard(key)
		if shard == nil {
			// new shard
			shard, err = ii.newShard(key)
			if err != nil {
				return fmt.Errorf("ii put: %w", err)
			}
		}

		err = shard.Put(termsGroup, val)
		if err != nil {
			return fmt.Errorf("ii put: %w", err)
		}
	}

	return nil
}

func (ii *InvertedIndex) findShard(key string) *Shard {
	ii.shardsM.RLock()
	defer ii.shardsM.RUnlock()

	shardIndex, ok := slices.BinarySearchFunc(ii.shards, key, func(s *Shard, key string) int {
		return strings.Compare(s.GetKey(), key)
	})
	if !ok {
		return nil
	}
	return ii.shards[shardIndex]
}

func (ii *InvertedIndex) newShard(key string) (*Shard, error) {
	ii.shardsM.Lock()
	defer ii.shardsM.Unlock()

	// check again if somebody made the shard already
	shardIndex, ok := slices.BinarySearchFunc(ii.shards, key, func(s *Shard, key string) int {
		return strings.Compare(s.GetKey(), key)
	})
	if ok {
		return ii.shards[shardIndex], nil // fast path
	}

	shardBaseDir := path.Join(ii.basedir, key)
	err := os.Mkdir(shardBaseDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("new shard: %w", err)
	}

	shard, err := NewShard(shardBaseDir, ii.fstPool, ii.removedList)
	if err != nil {
		return nil, fmt.Errorf("new shard: %w", err)
	}

	ii.shards = append(ii.shards, shard) // allocate once due to extending
	copy(ii.shards[shardIndex+1:], ii.shards[shardIndex:])
	ii.shards[shardIndex] = shard

	return shard, nil
}

// Read returns merging iterator for all available index shards.
// Must close to release segments for merging.
// [min,max] (inclusive) allows to skip irrelevant terms.
func (ii *InvertedIndex) Read(min, max []byte) (go_iterators.Iterator[file.TermValues], error) {

	// Reading from all shards at once is not efficient as we have to open a lot of files.
	// Since shards are sorted by the key, we can instead iterate over each shard sequentially,
	// merging shard iterators to a single stream.
	// Also, min/max can be easily respected by skipping shards which keys are lower
	// (min/max must be at least 2 bytes for that).

	ii.shardsM.RLock()
	shards := append([]*Shard{}, ii.shards...) // make a local copy of shards
	ii.shardsM.RUnlock()

	// Here filter the shards that are outside the min/max prefixes
	x := 0
	for _, s := range shards {
		minmax := s.MinMax()
		// left boundary
		if min != nil && bytes.Compare(min, minmax[1]) > 0 {
			continue
		}
		// right boundary
		if max != nil && bytes.Compare(max, minmax[0]) < 0 {
			continue
		}

		shards[x] = s
		x++
	}
	shards = shards[:x]

	var shard *Shard
	pickNextShard := func() (go_iterators.Iterator[file.TermValues], error) {
		if len(shards) == 0 {
			return nil, go_iterators.EmptyIterator
		}
		shard, shards = shards[0], shards[1:]
		return shard.Read(min, max)
	}
	it := go_iterators.NewSequentialDynamicIterator(pickNextShard)
	return it, nil
}

func NewInvertedIndex(basedir string) (*InvertedIndex, error) {

	// Init a pool of FST builders so we can reuse memory for building FSTs faster.
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
	remListSerialized, err := os.ReadFile(path.Join(basedir, "removed.list"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("rem list: %w", err)
		}
	} else {
		rl, err = UnserializeRemovedList(remListSerialized)
		if err != nil {
			return nil, fmt.Errorf("rem list: %w", err)
		}
	}

	// Load all shards from disk
	shards := make([]*Shard, 0)
	entries, err := os.ReadDir(basedir)
	if err != nil {
		return nil, fmt.Errorf("shards read: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		shardDir := path.Join(basedir, e.Name())
		shard, err := NewShard(shardDir, pool, rl)
		if err != nil {
			return nil, fmt.Errorf("shard init: %w", err)
		}

		shards = append(shards, shard)
	}

	return &InvertedIndex{
		basedir:     basedir,
		fstPool:     pool,
		removedList: rl,
		shards:      shards,
	}, nil
}
