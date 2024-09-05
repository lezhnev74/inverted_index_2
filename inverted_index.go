package inverted_index_2

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index_2/file"
	"golang.org/x/sync/errgroup"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type InvertedIndex struct {
	// Sorted by key
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

// PutRemoved appends to remove lists of shards.
// RemovedLists are used in merging.
func (ii *InvertedIndex) PutRemoved(values []uint32) (err error) {
	ii.shardsM.RLock()
	shards := append([]*Shard{}, ii.shards...)
	ii.shardsM.RUnlock()

	wg := errgroup.Group{}
	wg.SetLimit(runtime.NumCPU())
	for _, shard := range shards {
		wg.Go(func() error {
			return shard.Remove(values)
		})
	}
	return wg.Wait()
}

// Merge for each shard initiates a merging procedure.
// Returns how many segments were merged together.
// Thread-safe.
// If there are fewer than reqCount segments, then skip merging,
// otherwise merge at most mCount segments
func (ii *InvertedIndex) Merge(reqCount, mCount, concurrency int) (mergedSegmentsLen int64, err error) {

	// merging can be done in parallel if that is desired.
	// here it goes sequentially.

	ii.shardsM.RLock()
	shards := append([]*Shard{}, ii.shards...)
	ii.shardsM.RUnlock()

	workCh := make(chan *Shard)
	go func() {
		for _, shard := range shards {
			workCh <- shard
		}
		close(workCh)
	}()

	var (
		mergedAtomic atomic.Int64
		wg           sync.WaitGroup
	)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()

			for shard := range workCh {
				t0 := time.Now()
				shardMerged, serr := shard.Merge(reqCount, mCount)
				if serr != nil {
					err = serr
					return
				}
				if shardMerged > 0 {
					fmt.Printf("Shard %s merged %d segments in %s\n", shard.GetKey(), shardMerged, time.Now().Sub(t0).String())
				}
				mergedAtomic.Add(int64(shardMerged))
			}
		}()
	}

	wg.Wait()
	mergedSegmentsLen = mergedAtomic.Load()

	return
}

// Put spreads terms into shards.
// terms must be sorted.
func (ii *InvertedIndex) Put(terms [][]byte, val uint32) error {

	// sort terms, so they follow shards order
	slices.SortFunc(terms, func(t1, t2 []byte) int { return strings.Compare(shardKey(t1), shardKey(t2)) })

	termsIt := go_iterators.NewSliceIterator(terms)
	shardingIterator := go_iterators.NewGroupingIterator(termsIt, func(t []byte) any { return shardKey(t) })
	defer shardingIterator.Close()

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

	shard, err := NewShard(shardBaseDir, ii.fstPool)
	if err != nil {
		return nil, fmt.Errorf("new shard: %w", err)
	}

	ii.shards = append(ii.shards, shard) // allocate once due to extending
	copy(ii.shards[shardIndex+1:], ii.shards[shardIndex:])
	ii.shards[shardIndex] = shard

	return shard, nil
}

// PrefixSearch will test all terms in the index and compile a result only for those
// with the same prefix.
func (ii *InvertedIndex) PrefixSearch(prefixes [][]byte) (found map[string][]uint32, err error) {
	found = make(map[string][]uint32, len(prefixes))
	m := sync.Mutex{}

	slices.SortFunc(prefixes, bytes.Compare)
	concurrency := runtime.NumCPU()

	// Here we can search in every shard concurrently
	// First, we need to select which shards may contain the prefixes.
	ii.shardsM.RLock()
	shards := append([]*Shard{}, ii.shards...) // make a local copy of shards
	ii.shardsM.RUnlock()

	// Remember which prefixes matched which shards,
	// so later we can use it to catch when iteration can be stopped.
	shardPrefixes := map[*Shard][][]byte{}

	x := 0
	for _, shard := range shards {
		minmax := shard.MinMax()
		shardOk := false

		for _, prefix := range prefixes {
			// compare common part of a prefix and a min term in the shard
			l := min(len(prefix), len(minmax[0]))
			if bytes.Compare(prefix[:l], minmax[0][:l]) < 0 {
				continue
			}

			// compare common part of a prefix and a max term in the shard
			l = min(len(prefix), len(minmax[1]))
			if bytes.Compare(prefix[:l], minmax[1][:l]) > 0 {
				continue
			}

			shardPrefixes[shard] = append(shardPrefixes[shard], prefix)
			shardOk = true
		}

		if shardOk {
			shards[x] = shard
			x++
		}
	}
	shards = shards[:x]

	// Run concurrent reading from all selected shards, limiting the concurrency by the free-list.
	wg := errgroup.Group{}
	wg.SetLimit(concurrency)
	for _, shard := range shards {
		wg.Go(func() error {

			// select which prefixes are suitable for this shard
			prefixes := shardPrefixes[shard]
			greatestPrefix := prefixes[len(prefixes)-1]

			// we can set the left boundary as it is always smaller than any term that it may contain.
			// but the right boundary is open.
			it, err := shard.Read(prefixes[0], nil)
			if err != nil {
				err = fmt.Errorf("prefix search: %w", err)
			}
			defer it.Close()

			for {
				tv, err := it.Next()
				if err != nil {
					if errors.Is(err, go_iterators.EmptyIterator) {
						break
					}
					err = fmt.Errorf("prefix search: %w", err)
					return err
				}

				// Here we apply the right boundary of the iteration
				// if the term's prefix is greater than the greatest prefix than we should stop.
				termPrefix := tv.Term[:min(len(tv.Term), len(greatestPrefix))]
				if bytes.Compare(greatestPrefix, termPrefix) < 0 {
					// this term is greater than the greatest prefix, so no point in iterating further.
					break
				}

				for _, prefix := range prefixes {
					if bytes.HasPrefix(tv.Term, prefix) {
						m.Lock()
						found[string(prefix)] = append(found[string(prefix)], tv.Values...)
						m.Unlock()
					}
				}
			}

			return nil
		})
	}
	err = wg.Wait()

	// Deduplicate
	for k := range found {
		slices.Sort(found[k])
		found[k] = slices.Compact(found[k])
	}

	return
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

	ii := &InvertedIndex{
		basedir: basedir,
		fstPool: pool,
	}

	// Load all shards from disk
	// concurrent load:
	wg := sync.WaitGroup{}
	freeList := make(chan bool, runtime.NumCPU()) // limit concurrency

	shards := make([]*Shard, 0)
	entries, err := os.ReadDir(basedir)
	if err != nil {
		return nil, fmt.Errorf("shards read: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		freeList <- true // get a slot
		wg.Add(1)
		go func() {
			defer func() {
				<-freeList // release the slot
				wg.Done()
			}()
			var shard *Shard
			shardDir := path.Join(basedir, e.Name())
			shard, err = NewShard(shardDir, pool)
			if err != nil {
				err = fmt.Errorf("shard init: %w", err)
			}

			ii.shardsM.Lock()
			shards = append(shards, shard)
			ii.shardsM.Unlock()
		}()
	}
	if err != nil {
		return nil, fmt.Errorf("shards read: %w", err)
	}
	wg.Wait()

	slices.SortFunc(shards, func(s1, s2 *Shard) int { return strings.Compare(s1.GetKey(), s2.GetKey()) })
	ii.shards = shards

	return ii, nil
}
