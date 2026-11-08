/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	ostats "go.opencensus.io/stats"

	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/tok/index"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/x"
)

var (
	ErrNoValue       = errors.New("No value found")
	ErrRetry         = errors.New("Retry mutation")
	ErrStopIteration = errors.New("Stop iteration")
	ErrTsTooOld      = errors.New("Transaction timestamp is too old")
)

type ListOptions struct {
	ReadTs    uint64
	AfterUid  uint64
	Intersect *pb.List
	First     int
}

// Txn represents an in-flight transaction wrapper.
type Txn struct {
	StartTs     uint64
	KVTxn       x.KVTxn
	shouldAbort uint32
	sync.Mutex
	conflicts  map[uint64]struct{}
	lastUpdate time.Time
}

func NewTxn(startTs uint64) *Txn {
	var kvTxn x.KVTxn
	if pstore != nil {
		kvTxn = pstore.NewTransactionAt(startTs, true)
	}
	return &Txn{
		StartTs:    startTs,
		KVTxn:      kvTxn,
		lastUpdate: time.Now(),
		conflicts:  make(map[uint64]struct{}),
	}
}

func (txn *Txn) Update() {}

func (txn *Txn) ShouldAbort() bool {
	return atomic.LoadUint32(&txn.shouldAbort) > 0
}

func (txn *Txn) FillContext(ctx *api.TxnContext, gid uint32, isErrored bool) {
	txn.Lock()
	defer txn.Unlock()
	ctx.StartTs = txn.StartTs
	for key := range txn.conflicts {
		fps := strconv.FormatUint(key, 36)
		ctx.Keys = append(ctx.Keys, fps)
	}
}

func (txn *Txn) CommitToDisk(writer *TxnWriter, commitTs uint64) error {
	return nil
}

func (txn *Txn) UpdateCachedKeys(commitTs uint64) {}

func (txn *Txn) GetScalarList(key []byte) (*List, error) {
	return &List{}, nil
}

func (txn *Txn) Get(key []byte) (*List, error) {
	return &List{}, nil
}

func (txn *Txn) GetFromDelta(key []byte) (*List, error) {
	return &List{}, nil
}

var globalOracle *oracle

// Oracle returns the global oracle instance.
func Oracle() *oracle {
	return globalOracle
}

func init() {
	globalOracle = new(oracle)
	globalOracle.init()
}

type oracle struct {
	x.SafeMutex
	maxAssigned uint64
	pendingTxns map[uint64]*Txn
	waiters     map[uint64][]chan struct{}
}

func (o *oracle) init() {
	o.waiters = make(map[uint64][]chan struct{})
	o.pendingTxns = make(map[uint64]*Txn)
}

func (o *oracle) RegisterStartTs(ts uint64) *Txn {
	o.Lock()
	defer o.Unlock()
	txn, ok := o.pendingTxns[ts]
	if ok {
		txn.lastUpdate = time.Now()
	} else {
		txn = NewTxn(ts)
		o.pendingTxns[ts] = txn
	}
	return txn
}

func (o *oracle) MinPendingStartTs() uint64 {
	o.RLock()
	defer o.RUnlock()
	min := uint64(math.MaxUint64)
	for ts := range o.pendingTxns {
		if ts < min {
			min = ts
		}
	}
	return min
}

func (o *oracle) NumPendingTxns() int {
	o.RLock()
	defer o.RUnlock()
	return len(o.pendingTxns)
}

func (o *oracle) TxnOlderThan(dur time.Duration) (res []uint64) {
	o.RLock()
	defer o.RUnlock()

	cutoff := time.Now().Add(-dur)
	for startTs, txn := range o.pendingTxns {
		if txn.lastUpdate.Before(cutoff) {
			res = append(res, startTs)
		}
	}
	return res
}

func (o *oracle) addToWaiters(startTs uint64) (chan struct{}, bool) {
	if startTs <= o.MaxAssigned() {
		return nil, false
	}
	o.Lock()
	defer o.Unlock()
	if startTs <= o.MaxAssigned() {
		return nil, false
	}
	ch := make(chan struct{})
	o.waiters[startTs] = append(o.waiters[startTs], ch)
	return ch, true
}

func (o *oracle) MaxAssigned() uint64 {
	return atomic.LoadUint64(&o.maxAssigned)
}

func (o *oracle) SetMaxAssigned(ts uint64) {
	for {
		cur := atomic.LoadUint64(&o.maxAssigned)
		if ts <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(&o.maxAssigned, cur, ts) {
			o.Lock()
			for startTs, toNotify := range o.waiters {
				if startTs <= ts {
					for _, ch := range toNotify {
						close(ch)
					}
					delete(o.waiters, startTs)
				}
			}
			o.Unlock()
			return
		}
	}
}

func (o *oracle) WaitForTs(ctx context.Context, startTs uint64) error {
	ch, ok := o.addToWaiters(startTs)
	if !ok {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *oracle) ProcessDelta(delta *pb.OracleDelta) {
	if glog.V(3) {
		glog.Infof("ProcessDelta: Max Assigned: %d", delta.MaxAssigned)
		glog.Infof("ProcessDelta: Group checksum: %v", delta.GroupChecksums)
		for _, txn := range delta.Txns {
			if txn.CommitTs == 0 {
				glog.Infof("ProcessDelta Aborted: %d", txn.StartTs)
			} else {
				glog.Infof("ProcessDelta Committed: %d -> %d", txn.StartTs, txn.CommitTs)
			}
		}
	}

	o.Lock()
	defer o.Unlock()
	for _, status := range delta.Txns {
		delete(o.pendingTxns, status.StartTs)
	}
	curMax := o.MaxAssigned()
	if delta.MaxAssigned < curMax {
		return
	}

	for startTs, toNotify := range o.waiters {
		if startTs > delta.MaxAssigned {
			continue
		}
		for _, ch := range toNotify {
			close(ch)
		}
		delete(o.waiters, startTs)
	}
	x.AssertTrue(atomic.CompareAndSwapUint64(&o.maxAssigned, curMax, delta.MaxAssigned))
	ostats.Record(context.Background(),
		x.MaxAssignedTs.M(int64(delta.MaxAssigned)))
}

func (o *oracle) ResetTxns() {
	o.Lock()
	defer o.Unlock()
	o.pendingTxns = make(map[uint64]*Txn)
}

func (o *oracle) ResetTxnsForNs(ns uint64) {
	o.Lock()
	defer o.Unlock()
	o.pendingTxns = make(map[uint64]*Txn)
}

func (o *oracle) GetTxn(startTs uint64) *Txn {
	o.RLock()
	defer o.RUnlock()
	return o.pendingTxns[startTs]
}

func (o *oracle) IterateTxns(ok func(key []byte) bool) []uint64 {
	o.RLock()
	defer o.RUnlock()
	var timestamps []uint64
	for startTs := range o.pendingTxns {
		timestamps = append(timestamps, startTs)
	}
	return timestamps
}

func EdgeTypeID(edge *pb.DirectedEdge) types.TypeID {
	if edge.ValueId != 0 {
		return types.UidID
	}
	return types.TypeID(edge.ValueType)
}

func TypeID(edge *pb.DirectedEdge) types.TypeID {
	if edge.ValueId != 0 {
		return types.UidID
	}
	return types.TypeID(edge.ValueType)
}

func ResetCache() {}
func Cleanup()    {}
func SetEnabledDetailedMetrics(enable bool) {}

func DeletePredicate(ctx context.Context, attr string, ts uint64) error {
	return nil
}

func DeleteAll() error {
	return nil
}

func DeleteNamespace(ns uint64) error {
	return nil
}

func DeleteData(ns uint64) error {
	return nil
}

func DeleteAllForNs(ns uint64) error {
	return nil
}

type IncrRollupType struct{}

func (IncrRollupType) Process(closer any, getTs any) {}

var IncrRollup IncrRollupType

type List struct{}

func (l *List) Value(readTs uint64) (types.Val, error) {
	return types.Val{}, ErrNoValue
}

func (l *List) ValueFor(readTs uint64, langs []string) (types.Val, error) {
	return types.Val{}, ErrNoValue
}

func (l *List) ValueForTag(readTs uint64, tag string) (types.Val, error) {
	return types.Val{}, ErrNoValue
}

func (l *List) AllUntaggedValues(readTs uint64) ([]types.Val, error) {
	return nil, nil
}

func (l *List) Uids(opts ListOptions) (*pb.List, error) {
	return &pb.List{}, nil
}

func (l *List) Length(readTs, afterUid uint64) int {
	return 0
}

func (l *List) IsEmpty(readTs, afterUid uint64) (bool, error) {
	return true, nil
}

func (l *List) ApproxLen() int {
	return 0
}

func (l *List) GetLangTags(readTs uint64) ([]string, error) {
	return nil, nil
}

func (l *List) PostingFor(readTs uint64, langs []string) (*pb.Posting, error) {
	return nil, ErrNoValue
}

func (l *List) FindPosting(readTs, uid uint64) (bool, *pb.Posting, error) {
	return false, &pb.Posting{}, nil
}

func (l *List) RLock() {}
func (l *List) RUnlock() {}
func (l *List) GetLength(readTs uint64) int {
	return 0
}

func (l *List) Postings(opts ListOptions, f func(p *pb.Posting) error) error {
	return nil
}

func (l *List) Iterate(readTs, afterUid uint64, f func(obj *pb.Posting) error) error {
	return nil
}

func (l *List) Key() []byte {
	return nil
}

func (l *List) ToBackupPostingList(bpl any, alloc any, buf any) (*bpb.KV, error) {
	return &bpb.KV{}, nil
}

func (l *List) AllValues(readTs uint64) ([]types.Val, error) {
	return nil, nil
}

func (l *List) AddMutationWithIndex(ctx context.Context, edge *pb.DirectedEdge, txn *Txn) error {
	return nil
}

func GetNoStore(key []byte, readTs uint64) (*List, error) {
	return nil, ErrNoValue
}

func Get(key []byte, readTs uint64) (*List, error) {
	return nil, ErrNoValue
}

func ReadPostingList(key []byte, itr any) (*List, error) {
	return &List{}, nil
}

func FromBackupPostingList(b *pb.BackupPostingList) *pb.PostingList {
	return &pb.PostingList{}
}

func MarshalPostingList(l *pb.PostingList, alloc any) *bpb.KV {
	return &bpb.KV{}
}

func NewList(key []byte, l *pb.PostingList, version uint64) *List {
	return &List{}
}

func (l *List) Rollup(alloc any, readTs uint64) ([]*bpb.KV, error) {
	return nil, nil
}

const BitSchemaPosting byte = 0x01

type LocalCache struct{}

func NoCacheAt(readTs uint64) *LocalCache {
	return &LocalCache{}
}

func (lc *LocalCache) Get(key []byte) (*List, error) {
	return nil, ErrNoValue
}

func (lc *LocalCache) GetUids(key []byte) (*List, error) {
	return nil, ErrNoValue
}

func (lc *LocalCache) GetMultiPostings(keys [][]byte) (map[string]*pb.PostingList, error) {
	return make(map[string]*pb.PostingList), nil
}

func (lc *LocalCache) Set(key []byte, l *List) {}

func (lc *LocalCache) SetIfAbsent(key []byte, l *List) *List {
	return l
}

func (o *oracle) CacheAt(readTs uint64) *LocalCache {
	return &LocalCache{}
}

func NewViLocalCache(cache *LocalCache) index.LocalCache {
	return &flatViCache{}
}

type IterateDiskArgs struct {
	Prefix         []byte
	StartKey       []byte
	ReadTs         uint64
	AllVersions    bool
	Reverse        bool
	CheckInclusion func(uid uint64) error
	Function       func(l *List, pk x.ParsedKey) error
}

type memoryLayerDummy struct{}

func (memoryLayerDummy) IterateDisk(ctx context.Context, args IterateDiskArgs) error {
	return nil
}

func (memoryLayerDummy) UpdateMaxCost(cost int64) {}

var MemLayerInstance *memoryLayerDummy = &memoryLayerDummy{}

