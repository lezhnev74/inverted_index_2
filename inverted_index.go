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

	shardBaseDir := path.Join(ii.basedir, key)
	err := os.Mkdir(shardBaseDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("new shard: %w", err)
	}

	shard, err := NewShard(shardBaseDir, ii.fstPool, ii.removedList)
	if err != nil {
		return nil, fmt.Errorf("new shard: %w", err)
	}

	shardIndex, _ := slices.BinarySearchFunc(ii.shards, key, func(s *Shard, key string) int {
		return strings.Compare(s.GetKey(), key)
	})
	ii.shards = append(ii.shards, shard) // allocate once due to extending
	copy(ii.shards[shardIndex+1:], ii.shards[shardIndex:])
	ii.shards[shardIndex] = shard

	return shard, nil
}

func (ii *InvertedIndex) Read(min, max []byte) (go_iterators.Iterator[file.TermValues], error) {

	shardIterators := make([]go_iterators.Iterator[file.TermValues], 0, len(ii.shards))
	for _, shard := range ii.shards {
		shardIt, err := shard.Read(min, max)
		if err != nil {
			return nil, fmt.Errorf("index read: %w", err)
		}
		shardIterators = append(shardIterators, shardIt)
	}

	it := go_iterators.NewMergingIterator(shardIterators, file.CompareTermValues, file.MergeTermValues)

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
