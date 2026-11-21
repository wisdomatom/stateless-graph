/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/golang/glog"
)

type uidRange struct {
	start uint64
	end   uint64
}

var (
	tsCounter       uint64 = 100
	tsMax           uint64 = 0
	tsMu            sync.Mutex
	tsLeaser        func() (uint64, uint64, error)
	uidCounter      uint64 = 100
	uidMax          uint64 = 0
	uidPrefetchAt   uint64 = math.MaxUint64
	uidMu           sync.Mutex
	uidLeaser       func() (uint64, uint64, error)
	uidNext         *uidRange
	uidPrefetching  uint32
	uidPrefetchDone chan struct{}
)

func installUidRangeLocked(start, end uint64) {
	// Briefly set uidMax to 0 so concurrent fast-path readers fall back to uidMu
	// while counter and max are updated, avoiding cross-range allocation.
	atomic.StoreUint64(&uidMax, 0)
	atomic.StoreUint64(&uidCounter, start)
	if end > start {
		atomic.StoreUint64(&uidPrefetchAt, start+(end-start)/2)
	} else {
		atomic.StoreUint64(&uidPrefetchAt, math.MaxUint64)
	}
	atomic.StoreUint64(&uidMax, end)
}

func SetUidLeaser(f func() (uint64, uint64, error)) {
	uidMu.Lock()
	defer uidMu.Unlock()
	uidLeaser = f
	uidNext = nil
}

func SetTsLeaser(f func() (uint64, uint64, error)) {
	tsMu.Lock()
	defer tsMu.Unlock()
	tsLeaser = f
}

func GetNextTs() uint64 {
	for {
		cur := atomic.LoadUint64(&tsCounter)
		max := atomic.LoadUint64(&tsMax)
		if cur < max {
			if atomic.CompareAndSwapUint64(&tsCounter, cur, cur+1) {
				return cur + 1
			}
			continue
		}

		if tsLeaser == nil {
			return atomic.AddUint64(&tsCounter, 1)
		}

		tsMu.Lock()
		cur = atomic.LoadUint64(&tsCounter)
		max = atomic.LoadUint64(&tsMax)
		if cur >= max {
			start, end, err := tsLeaser()
			if err == nil {
				atomic.StoreUint64(&tsCounter, start)
				atomic.StoreUint64(&tsMax, end)
			} else {
				glog.Errorf("Failed to renew TS lease: %v", err)
				val := atomic.AddUint64(&tsCounter, 1)
				tsMu.Unlock()
				return val
			}
		}
		tsMu.Unlock()
	}
}

func SetNextTsRange(start, end uint64) {
	tsMu.Lock()
	defer tsMu.Unlock()
	atomic.StoreUint64(&tsCounter, start)
	atomic.StoreUint64(&tsMax, end)
	glog.Infof("TS range leased: [%d, %d]", start, end)
}

func maybeStartUidPrefetch() {
	uidMu.Lock()
	if uidLeaser == nil || uidNext != nil || atomic.LoadUint32(&uidPrefetching) == 1 {
		uidMu.Unlock()
		return
	}
	leaser := uidLeaser
	atomic.StoreUint32(&uidPrefetching, 1)
	// Suppress repeated fast-path prefetch triggers while prefetch is running or buffered.
	atomic.StoreUint64(&uidPrefetchAt, math.MaxUint64)
	done := make(chan struct{})
	uidPrefetchDone = done
	uidMu.Unlock()

	go func() {
		start, end, err := leaser()
		uidMu.Lock()
		if err == nil && end > start {
			uidNext = &uidRange{start: start, end: end}
			glog.Infof("UID next range prefetched: [%d, %d]", start, end)
		} else {
			if err != nil {
				glog.Errorf("Failed to async prefetch UID lease: %v", err)
			}
			// Re-arm prefetch trigger so a future allocation can retry.
			cur := atomic.LoadUint64(&uidCounter)
			max := atomic.LoadUint64(&uidMax)
			if cur < max {
				atomic.StoreUint64(&uidPrefetchAt, cur)
			}
		}
		atomic.StoreUint32(&uidPrefetching, 0)
		close(done)
		uidMu.Unlock()
	}()
}

// GetNextUidsWithErr allocates `howMany` monotonically increasing UIDs using a
// double-buffered async prefetch lease mechanism.
func GetNextUidsWithErr(howMany uint64) (uint64, uint64, error) {
	if howMany == 0 {
		return 0, 0, nil
	}
	for {
		cur := atomic.LoadUint64(&uidCounter)
		max := atomic.LoadUint64(&uidMax)
		if cur+howMany <= max {
			if atomic.CompareAndSwapUint64(&uidCounter, cur, cur+howMany) {
				newCur := cur + howMany
				if newCur >= atomic.LoadUint64(&uidPrefetchAt) {
					maybeStartUidPrefetch()
				}
				return cur + 1, newCur, nil
			}
			continue
		}

		uidMu.Lock()
		if uidLeaser == nil {
			uidMu.Unlock()
			newVal := atomic.AddUint64(&uidCounter, howMany)
			return newVal - howMany + 1, newVal, nil
		}

		cur = atomic.LoadUint64(&uidCounter)
		max = atomic.LoadUint64(&uidMax)
		if cur+howMany <= max {
			uidMu.Unlock()
			continue
		}

		// Case 1: Double-buffer hit — swap in the prefetched range in nanoseconds.
		if uidNext != nil {
			next := uidNext
			uidNext = nil
			installUidRangeLocked(next.start, next.end)
			uidMu.Unlock()
			if next.end <= next.start || next.end-next.start < howMany {
				return 0, 0, fmt.Errorf("prefetched UID range [%d, %d] is smaller than requested %d", next.start, next.end, howMany)
			}
			continue
		}

		// Case 2: Async prefetch is already in flight — wait without holding uidMu.
		if atomic.LoadUint32(&uidPrefetching) == 1 && uidPrefetchDone != nil {
			done := uidPrefetchDone
			uidMu.Unlock()
			<-done
			continue
		}

		// Case 3: No buffered range and no in-flight prefetch — lease synchronously (singleflight).
		leaser := uidLeaser
		atomic.StoreUint32(&uidPrefetching, 1)
		done := make(chan struct{})
		uidPrefetchDone = done
		uidMu.Unlock()

		start, end, err := leaser()

		uidMu.Lock()
		if err == nil && end > start {
			installUidRangeLocked(start, end)
		}
		atomic.StoreUint32(&uidPrefetching, 0)
		close(done)
		uidMu.Unlock()

		if err != nil {
			glog.Errorf("Failed to renew UID lease: %v", err)
			return 0, 0, err
		}
		if end <= start || end-start < howMany {
			return 0, 0, fmt.Errorf("leased UID range [%d, %d] is smaller than requested %d", start, end, howMany)
		}
	}
}

func GetNextUids(howMany uint64) (uint64, uint64) {
	start, end, err := GetNextUidsWithErr(howMany)
	if err != nil {
		glog.Errorf("GetNextUids failed: %v", err)
		return 0, 0
	}
	return start, end
}

func GetNextUid() uint64 {
	start, _ := GetNextUids(1)
	return start
}

func SetNextUid(val uint64) {
	uidMu.Lock()
	defer uidMu.Unlock()
	uidNext = nil
	installUidRangeLocked(val, val)
}

func SetNextUidRange(start, end uint64) {
	uidMu.Lock()
	defer uidMu.Unlock()
	uidNext = nil
	installUidRangeLocked(start, end)
	glog.Infof("UID range leased: [%d, %d]", start, end)
}

var ErrKeyNotFound = badger.ErrKeyNotFound

const (
	// BitSchemaPosting signals that the value stores a schema or type.
	BitSchemaPosting byte = 0x01
	// BitDeltaPosting signals that the value stores the delta of a posting list.
	BitDeltaPosting byte = 0x04
	// BitCompletePosting signals that the values stores a complete posting list.
	BitCompletePosting byte = 0x08
	// BitEmptyPosting signals that the value stores an empty posting list.
	BitEmptyPosting byte = 0x10
	// BitDeletePosting is used for Badger deletion.
	BitDeletePosting byte = 0x20
)

// KVDB is the interface for the underlying KV store.
type KVDB interface {
	// NewTransactionAt creates a new transaction at the given read timestamp.
	NewTransactionAt(readTs uint64, update bool) KVTxn
	// Close closes the database.
	Close() error
	// Sync syncs the database to disk.
	Sync() error
	// DropPrefix drops all keys with the given prefix.
	DropPrefix(prefixes ...[]byte) error
	// DropAll drops all keys in the database.
	DropAll() error
	// NewStreamAt creates a new stream for scanning the database at the given read timestamp.
	NewStreamAt(readTs uint64) KVStream
	// NewWriteBatch creates a new write batch.
	NewWriteBatch() KVWriteBatch
	// NewManagedWriteBatch creates a new write batch for managed mode.
	NewManagedWriteBatch() KVWriteBatch
	// RunValueLogGC runs value log garbage collection.
	RunValueLogGC(discardRatio float64) error
	// Size returns the size of the database.
	Size() (int64, int64)
	// BanNamespace bans the namespace.
	BanNamespace(ns uint64) error
	SetDiscardTs(ts uint64)
	Tables() []badger.TableInfo
	NewStreamWriter() KVStreamWriter
	Subscribe(ctx context.Context, cb func(*pb.KVList) error, matches []pb.Match) error
	CacheMaxCost(badger.CacheType, int64) (int64, error)
	GetTimestamp(ctx context.Context) (uint64, error)
	AllocateStartTs() uint64
}

// KVStreamWriter is the interface for bulk loading data.
type KVStreamWriter interface {
	Prepare() error
	PrepareIncremental() error
	Write(buf *z.Buffer) error
	Flush() error
	Cancel()
}

// KVTxn is the interface for a KV transaction.
type KVTxn interface {
	// Get retrieves the value for the given key.
	Get(key []byte) (KVItem, error)
	// BatchGet retrieves values for multiple keys concurrently.
	BatchGet(keys [][]byte) (map[string][]byte, error)
	// Set sets the value for the given key.
	Set(key, val []byte) error
	// SetWithMeta sets the value for the given key with metadata.
	SetWithMeta(key, val []byte, meta byte) error
	// SetWithDiscard sets the value and indicates earlier versions can be discarded.
	SetWithDiscard(key, val []byte, meta byte) error
	// SetEntry sets an entry.
	SetEntry(*badger.Entry) error
	// Delete deletes the key.
	Delete(key []byte) error
	// CommitAt commits the transaction at the given timestamp.
	CommitAt(commitTs uint64, callback func(error)) error
	// Discard discards the transaction.
	Discard()
	// NewIterator returns a new iterator for the transaction.
	NewIterator(opt KVIterOpts) KVIterator
	// LockKeys acquires pessimistic locks on the given keys.
	LockKeys(ctx context.Context, keys ...[]byte) error
}

// KVItem is the interface for a single KV pair returned by Get or Iterator.
type KVItem interface {
	Key() []byte
	KeyCopy([]byte) []byte
	Value(func(val []byte) error) error
	ValueCopy([]byte) ([]byte, error)
	UserMeta() byte
	Version() uint64
	IsDeletedOrExpired() bool
	ExpiresAt() uint64
}

// KVIterator is the interface for iterating over KV pairs.
type KVIterator interface {
	Rewind()
	Seek(key []byte)
	Valid() bool
	Next()
	Item() KVItem
	Close()
}

// KVStream is the interface for streaming KV pairs.
type KVStream interface {
	SetSend(func(buf *z.Buffer) error)
	SetKeyToList(func(key []byte, itr KVStreamIterator) (*pb.KVList, error))
	SetKeyToListWithThreadId(func(key []byte, itr KVStreamIterator, threadId int) (*pb.KVList, error))
	SetFinishThread(func(threadId int) (*pb.KVList, error))
	SetPrefix(prefix []byte)
	SetLogPrefix(prefix string)
	SetUseKeyToListWithThreadId(use bool)
	SetSinceTs(ts uint64)
	SetChooseKey(func(item KVItem) bool)
	Orchestrate(ctx context.Context) error
}

// KVStreamIterator is used within Stream.SetKeyToList to iterate over versions of a key.
type KVStreamIterator interface {
	Rewind()
	Seek(key []byte)
	Valid() bool
	Next()
	Item() KVItem
	Close()
}

// KVWriteBatch is the interface for bulk writes.
type KVWriteBatch interface {
	SetAt(key, val []byte, meta byte, ts uint64) error
	DeleteAt(key []byte, ts uint64) error
	Flush() error
	Write(buf *z.Buffer) error
}

// KVIterOpts contains options for the iterator.
type KVIterOpts struct {
	Prefix         []byte
	IsReverse      bool
	AllVersions    bool
	PrefetchValues bool
	PrefetchSize   int
}

type newUidsKey struct{}

func WithNewUids(ctx context.Context, uids map[uint64]struct{}) context.Context {
	if len(uids) == 0 {
		return ctx
	}
	return context.WithValue(ctx, newUidsKey{}, uids)
}

func IsNewUid(ctx context.Context, uid uint64) bool {
	if ctx == nil {
		return false
	}
	v := ctx.Value(newUidsKey{})
	if v == nil {
		return false
	}
	set, ok := v.(map[uint64]struct{})
	if !ok {
		return false
	}
	_, exists := set[uid]
	return exists
}

