/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"bytes"
	"context"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"google.golang.org/protobuf/proto"
)

type tikvDB struct {
	client  *txnkv.Client
	pdAddrs []string
}

var logOnce sync.Once
var tikvTxns sync.Map
var activeTikvTxns sync.Map // map[uint64]*tikvTxn

func NewTiKVKV(pdAddrs []string) (KVDB, error) {
	logOnce.Do(func() {
		l, p, _ := log.InitLogger(&log.Config{Level: "warn"})
		log.ReplaceGlobals(l, p)
	})
	client, err := txnkv.NewClient(pdAddrs)
	if err != nil {
		return nil, err
	}
	return &tikvDB{
		client:  client,
		pdAddrs: pdAddrs,
	}, nil
}

func (t *tikvDB) AllocateStartTs() uint64 {
	txn, err := t.client.Begin()
	if err != nil {
		panic(errors.Wrap(err, "failed to begin tikv transaction in AllocateStartTs"))
	}
	txn.SetEnable1PC(true)
	txn.SetEnableAsyncCommit(true)
	ts := txn.StartTS()
	tikvTxns.Store(ts, txn)
	return ts
}

func (t *tikvDB) NewTransactionAt(readTs uint64, update bool) KVTxn {
	if update {
		if readTs > 0 && readTs < math.MaxUint64 {
			if v, ok := activeTikvTxns.Load(readTs); ok {
				return v.(*tikvTxn)
			}
		}
		var txn *transaction.KVTxn
		var err error
		if readTs > 0 && readTs < math.MaxUint64 {
			txn, err = t.client.Begin(tikv.WithStartTS(readTs))
		} else {
			txn, err = t.client.Begin()
		}
		if err != nil {
			panic(errors.Wrap(err, "failed to begin tikv transaction in NewTransactionAt"))
		}
		txn.SetEnable1PC(true)
		txn.SetEnableAsyncCommit(true)
		tx := &tikvTxn{
			txn:     txn,
			ts:      txn.StartTS(),
			db:      t,
			isWrite: true,
		}
		if readTs > 0 && readTs < math.MaxUint64 {
			activeTikvTxns.Store(readTs, tx)
		}
		if tx.ts > 0 {
			activeTikvTxns.Store(tx.ts, tx)
		}
		return tx
	}

	// update == false: read-only query
	if readTs > 0 && readTs < math.MaxUint64 {
		if v, ok := activeTikvTxns.Load(readTs); ok {
			return &tikvSharedReadTxn{parent: v.(*tikvTxn)}
		}
	}

	var txn *transaction.KVTxn
	var err error
	if readTs > 0 && readTs < math.MaxUint64 {
		txn, err = t.client.Begin(tikv.WithStartTS(readTs))
	} else {
		txn, err = t.client.Begin()
	}
	if err != nil {
		panic(errors.Wrap(err, "failed to begin tikv transaction in NewTransactionAt"))
	}
	return &tikvTxn{
		txn:     txn,
		ts:      txn.StartTS(),
		db:      t,
		isWrite: false,
	}
}

func (t *tikvDB) Close() error {
	return t.client.Close()
}

func (t *tikvDB) Sync() error {
	// TiKV is distributed and syncs on commit.
	return nil
}

func (t *tikvDB) DropPrefix(prefixes ...[]byte) error {
	// TiKV can delete ranges.
	ctx := context.Background()
	for _, prefix := range prefixes {
		end := make([]byte, len(prefix))
		copy(end, prefix)
		// Increment the last byte to get the end of the range.
		for i := len(end) - 1; i >= 0; i-- {
			end[i]++
			if end[i] != 0 {
				break
			}
		}
		_, err := t.client.DeleteRange(ctx, prefix, end, 1000)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tikvDB) DropAll() error {
	activeTikvTxns.Range(func(k, v interface{}) bool {
		activeTikvTxns.Delete(k)
		return true
	})
	ctx := context.Background()
	for {
		txn, err := t.client.Begin()
		if err != nil {
			return err
		}
		it, err := txn.Iter([]byte{}, nil)
		if err != nil {
			_ = txn.Rollback()
			return err
		}

		var keys [][]byte
		for it.Valid() && len(keys) < 2000 {
			k := make([]byte, len(it.Key()))
			copy(k, it.Key())
			keys = append(keys, k)
			if err := it.Next(); err != nil {
				break
			}
		}
		it.Close()

		if len(keys) == 0 {
			_ = txn.Rollback()
			break
		}

		for _, k := range keys {
			_ = txn.Delete(k)
		}

		if err := txn.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *tikvDB) NewStreamAt(readTs uint64) KVStream {
	return &tikvStream{
		db:     t,
		readTs: readTs,
	}
}

type tikvStream struct {
	db        *tikvDB
	readTs    uint64
	prefix    []byte
	logPrefix string
	keyToList func(key []byte, itr KVStreamIterator) (*pb.KVList, error)
	send      func(buf *z.Buffer) error
	chooseKey func(item KVItem) bool
}

func (s *tikvStream) SetPrefix(prefix []byte)    { s.prefix = prefix }
func (s *tikvStream) SetLogPrefix(prefix string) { s.logPrefix = prefix }
func (s *tikvStream) SetKeyToList(f func(key []byte, itr KVStreamIterator) (*pb.KVList, error)) {
	s.keyToList = f
}
func (s *tikvStream) SetSend(f func(buf *z.Buffer) error) { s.send = f }
func (s *tikvStream) SetChooseKey(f func(item KVItem) bool) {
	s.chooseKey = f
}
func (s *tikvStream) SetKeyToListWithThreadId(f func(key []byte, itr KVStreamIterator, threadId int) (*pb.KVList, error)) {
}
func (s *tikvStream) SetFinishThread(f func(threadId int) (*pb.KVList, error)) {
}
func (s *tikvStream) SetUseKeyToListWithThreadId(use bool) {
}
func (s *tikvStream) SetSinceTs(ts uint64) {
}

func (s *tikvStream) Orchestrate(ctx context.Context) error {
	txn, err := s.db.client.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	iter, err := txn.Iter(s.prefix, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	// Simple implementation: iterate and call keyToList/send
	for iter.Valid() {
		key := iter.Key()
		if len(s.prefix) > 0 && !bytes.HasPrefix(key, s.prefix) {
			break
		}

		item := &tikvItem{key: key, value: iter.Value()}
		if s.chooseKey != nil && !s.chooseKey(item) {
			if err := iter.Next(); err != nil {
				return err
			}
			continue
		}

		if s.keyToList != nil {
			// This is a simplified version. Badger's Stream calls keyToList with an iterator
			// that can see multiple versions. Here we see only one.
			kvList, err := s.keyToList(key, &tikvStreamIterator{
				itr: &tikvInternalIteratorStub{item: item},
			})
			if err != nil {
				return err
			}
			if kvList != nil && s.send != nil {
				buf := z.NewBuffer(1024, "tikvStream")
				// We need a way to convert KVList to buffer.
				// In Badger, Orchestrate sends lists to Send.
				data, _ := proto.Marshal(kvList)
				buf.Write(data)
				if err := s.send(buf); err != nil {
					return err
				}
				buf.Release()
			}
		}

		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

type tikvStreamIterator struct {
	itr tikvInternalIterator
}

func (it *tikvStreamIterator) Seek(key []byte) {
	// Not supported by stub
}

func (it *tikvStreamIterator) Valid() bool {
	return it.itr.Valid()
}

func (it *tikvStreamIterator) Next() {
	_ = it.itr.Next()
}

func (it *tikvStreamIterator) Item() KVItem {
	return &tikvItem{key: it.itr.Key(), value: it.itr.Value()}
}

func (it *tikvStreamIterator) Close() {
	it.itr.Close()
}

func (it *tikvStreamIterator) Rewind() {
	// Not supported by stub
}

type tikvInternalIteratorStub struct {
	item  *tikvItem
	valid bool
}

func (s *tikvInternalIteratorStub) Valid() bool     { return !s.valid }
func (s *tikvInternalIteratorStub) Key() []byte     { return s.item.key }
func (s *tikvInternalIteratorStub) Value() []byte   { return s.item.value }
func (s *tikvInternalIteratorStub) Next() error     { s.valid = true; return nil }
func (s *tikvInternalIteratorStub) Close()          {}
func (s *tikvInternalIteratorStub) Rewind()         { s.valid = false }
func (s *tikvInternalIteratorStub) Seek(key []byte) {}
func (s *tikvInternalIteratorStub) Item() KVItem    { return s.item }

func (t *tikvDB) NewWriteBatch() KVWriteBatch {
	// TiKV transactions can act as write batches.
	txn, _ := t.client.Begin()
	return &tikvWriteBatch{txn: txn}
}

func (t *tikvDB) NewManagedWriteBatch() KVWriteBatch {
	return t.NewWriteBatch()
}

func (t *tikvDB) RunValueLogGC(discardRatio float64) error {
	return nil // TiKV handles GC internally.
}

func (t *tikvDB) Size() (int64, int64) {
	return 0, 0 // Need to query PD for cluster stats if needed.
}

func (t *tikvDB) BanNamespace(ns uint64) error {
	return nil
}

func (t *tikvDB) SetDiscardTs(ts uint64) {}

func (t *tikvDB) Tables() []badger.TableInfo {
	return nil
}

func (t *tikvDB) NewStreamWriter() KVStreamWriter {
	// TiKV has IngestSST for bulk loading, but for now we can stub it.
	return nil
}

func (t *tikvDB) Subscribe(ctx context.Context, cb func(*pb.KVList) error, matches []pb.Match) error {
	return nil
}

func (t *tikvDB) CacheMaxCost(badger.CacheType, int64) (int64, error) {
	return 0, nil
}

func (t *tikvDB) GetTimestamp(ctx context.Context) (uint64, error) {
	return t.client.GetTimestamp(ctx)
}

type tikvInternalIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

type tikvTxn struct {
	sync.RWMutex
	txn     *transaction.KVTxn
	ts      uint64
	db      *tikvDB
	isWrite bool
}

func (tx *tikvTxn) Get(key []byte) (KVItem, error) {
	tx.RLock()
	defer tx.RUnlock()
	return tx.getNoLock(key)
}

func (tx *tikvTxn) getNoLock(key []byte) (KVItem, error) {
	val, err := tx.txn.Get(context.Background(), key)
	if err != nil {
		if err.Error() == "not found" || err.Error() == "not exist" {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return &tikvItem{key: key, value: val}, nil
}

func (tx *tikvTxn) BatchGet(keys [][]byte) (map[string][]byte, error) {
	tx.RLock()
	defer tx.RUnlock()
	return tx.batchGetNoLock(keys)
}

func (tx *tikvTxn) batchGetNoLock(keys [][]byte) (map[string][]byte, error) {
	if len(keys) == 0 {
		return make(map[string][]byte), nil
	}
	res, err := tx.txn.BatchGet(context.Background(), keys)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(res))
	for k, val := range res {
		if len(val) >= 2 {
			out[k] = val[2:]
		} else {
			out[k] = val
		}
	}
	return out, nil
}

func (tx *tikvTxn) Set(key, val []byte) error {
	tx.Lock()
	defer tx.Unlock()
	return tx.setWithMetaNoLock(key, val, 0)
}

func (tx *tikvTxn) SetWithMeta(key, val []byte, meta byte) error {
	tx.Lock()
	defer tx.Unlock()
	return tx.setWithMetaNoLock(key, val, meta)
}

func (tx *tikvTxn) setWithMetaNoLock(key, val []byte, meta byte) error {
	buf := make([]byte, 2+len(val))
	buf[0] = meta
	buf[1] = 0 // Reserved
	copy(buf[2:], val)
	return tx.txn.Set(key, buf)
}

func (tx *tikvTxn) SetWithDiscard(key, val []byte, meta byte) error {
	return tx.SetWithMeta(key, val, meta)
}

func (tx *tikvTxn) SetEntry(e *badger.Entry) error {
	return tx.SetWithMeta(e.Key, e.Value, e.UserMeta)
}

func (tx *tikvTxn) Delete(key []byte) error {
	tx.Lock()
	defer tx.Unlock()
	return tx.txn.Delete(key)
}

func (tx *tikvTxn) CommitAt(commitTs uint64, callback func(error)) error {
	tx.Lock()
	defer tx.Unlock()
	if tx.isWrite && tx.ts > 0 {
		activeTikvTxns.Delete(tx.ts)
	}
	err := tx.txn.Commit(context.Background())
	if callback != nil {
		callback(err)
	}
	return err
}

func (tx *tikvTxn) Discard() {
	tx.Lock()
	defer tx.Unlock()
	if tx.isWrite && tx.ts > 0 {
		activeTikvTxns.Delete(tx.ts)
	}
	tx.txn.Rollback()
}

func (tx *tikvTxn) NewIterator(opt KVIterOpts) KVIterator {
	tx.RLock()
	defer tx.RUnlock()
	return tx.newIteratorNoLock(opt)
}

func PrefixNext(b []byte) []byte {
	res := make([]byte, len(b))
	copy(res, b)
	for i := len(res) - 1; i >= 0; i-- {
		res[i]++
		if res[i] != 0 {
			return res
		}
	}
	return nil
}

func (tx *tikvTxn) newIteratorNoLock(opt KVIterOpts) KVIterator {
	var iter interface{}
	var err error
	var initialKey []byte

	if opt.PrefetchSize > 1 {
		tx.txn.GetSnapshot().SetScanBatchSize(opt.PrefetchSize)
	} else if opt.PrefetchSize == 1 {
		tx.txn.GetSnapshot().SetScanBatchSize(2)
	}

	if opt.IsReverse {
		if len(opt.Prefix) > 0 {
			initialKey = PrefixNext(opt.Prefix)
		}
		iter, err = tx.txn.IterReverse(initialKey)
	} else {
		initialKey = opt.Prefix
		var upperBound []byte
		if len(opt.Prefix) > 0 {
			upperBound = PrefixNext(opt.Prefix)
		}
		iter, err = tx.txn.Iter(initialKey, upperBound)
	}
	if err != nil {
		return nil
	}
	return &tikvIterator{
		mu:        &tx.RWMutex,
		txn:       tx.txn,
		iter:      iter.(tikvInternalIterator),
		prefix:    opt.Prefix,
		isReverse: opt.IsReverse,
		currKey:   initialKey,
		consumed:  false,
	}
}

func (tx *tikvTxn) LockKeys(ctx context.Context, keys ...[]byte) error {
	if len(keys) == 0 {
		return nil
	}
	return tx.txn.LockKeys(ctx, &kv.LockCtx{}, keys...)
}

type tikvSharedReadTxn struct {
	parent *tikvTxn
}

func (tx *tikvSharedReadTxn) Get(key []byte) (KVItem, error) {
	tx.parent.RLock()
	defer tx.parent.RUnlock()
	return tx.parent.getNoLock(key)
}

func (tx *tikvSharedReadTxn) BatchGet(keys [][]byte) (map[string][]byte, error) {
	tx.parent.RLock()
	defer tx.parent.RUnlock()
	return tx.parent.batchGetNoLock(keys)
}

func (tx *tikvSharedReadTxn) Set(key, val []byte) error {
	return errors.New("cannot write on shared read transaction")
}

func (tx *tikvSharedReadTxn) SetWithMeta(key, val []byte, meta byte) error {
	return errors.New("cannot write on shared read transaction")
}

func (tx *tikvSharedReadTxn) SetWithDiscard(key, val []byte, meta byte) error {
	return errors.New("cannot write on shared read transaction")
}

func (tx *tikvSharedReadTxn) SetEntry(e *badger.Entry) error {
	return errors.New("cannot write on shared read transaction")
}

func (tx *tikvSharedReadTxn) Delete(key []byte) error {
	return errors.New("cannot delete on shared read transaction")
}

func (tx *tikvSharedReadTxn) CommitAt(commitTs uint64, callback func(error)) error {
	return errors.New("cannot commit shared read transaction")
}

// Discard on shared read txn is a no-op so that it does not rollback the write transaction.
func (tx *tikvSharedReadTxn) Discard() {
}

func (tx *tikvSharedReadTxn) NewIterator(opt KVIterOpts) KVIterator {
	tx.parent.RLock()
	defer tx.parent.RUnlock()
	return tx.parent.newIteratorNoLock(opt)
}

func (tx *tikvSharedReadTxn) LockKeys(ctx context.Context, keys ...[]byte) error {
	return nil
}

type tikvItem struct {
	key   []byte
	value []byte
}

func (i *tikvItem) Key() []byte { return i.key }
func (i *tikvItem) KeyCopy(dst []byte) []byte {
	return append(dst[:0], i.key...)
}
func (i *tikvItem) Value(f func(val []byte) error) error {
	if len(i.value) < 2 {
		return f(i.value)
	}
	return f(i.value[2:])
}
func (i *tikvItem) ValueCopy(dst []byte) ([]byte, error) {
	if len(i.value) < 2 {
		return append(dst[:0], i.value...), nil
	}
	return append(dst[:0], i.value[2:]...), nil
}
func (i *tikvItem) UserMeta() byte {
	if len(i.value) < 1 {
		return 0
	}
	return i.value[0]
}
func (i *tikvItem) Version() uint64 {
	if _, tag, ts, ok := SplitPostingKey(i.key); ok && tag == TagPostingDelta {
		return ts
	}
	return 0
}
func (i *tikvItem) IsDeletedOrExpired() bool { return false }
func (i *tikvItem) ExpiresAt() uint64        { return 0 }

type tikvIterator struct {
	mu        *sync.RWMutex
	txn       *transaction.KVTxn
	iter      tikvInternalIterator
	prefix    []byte
	isReverse bool
	currKey   []byte
	consumed  bool
}

func (it *tikvIterator) Rewind() {
	if it.mu != nil {
		it.mu.RLock()
		defer it.mu.RUnlock()
	}
	var startKey []byte
	if it.isReverse {
		if len(it.prefix) > 0 {
			startKey = PrefixNext(it.prefix)
		}
	} else {
		startKey = it.prefix
	}
	if it.iter != nil && !it.consumed && bytes.Equal(it.currKey, startKey) {
		return
	}
	if it.iter != nil {
		it.iter.Close()
	}
	var err error
	if it.isReverse {
		it.iter, err = it.txn.IterReverse(startKey)
	} else {
		var upperBound []byte
		if len(it.prefix) > 0 {
			upperBound = PrefixNext(it.prefix)
		}
		it.iter, err = it.txn.Iter(startKey, upperBound)
	}
	if err != nil {
		it.iter = nil
	}
	it.currKey = startKey
	it.consumed = false
}

func (it *tikvIterator) Seek(key []byte) {
	if it.mu != nil {
		it.mu.RLock()
		defer it.mu.RUnlock()
	}
	if len(key) == 0 {
		key = it.prefix
	}
	var startKey []byte
	if it.isReverse {
		if len(key) > 0 {
			startKey = key
		} else if len(it.prefix) > 0 {
			startKey = PrefixNext(it.prefix)
		}
	} else {
		startKey = key
	}

	if it.iter != nil && !it.consumed && bytes.Equal(it.currKey, startKey) {
		return
	}

	if it.iter != nil {
		it.iter.Close()
	}
	var err error
	if it.isReverse {
		it.iter, err = it.txn.IterReverse(startKey)
	} else {
		var upperBound []byte
		if len(it.prefix) > 0 {
			upperBound = PrefixNext(it.prefix)
		}
		it.iter, err = it.txn.Iter(startKey, upperBound)
	}
	if err != nil {
		it.iter = nil
	}
	it.currKey = startKey
	it.consumed = false
}

func (it *tikvIterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	if len(it.prefix) > 0 {
		return bytes.HasPrefix(it.iter.Key(), it.prefix)
	}
	return true
}

func (it *tikvIterator) Next() {
	it.consumed = true
	if err := it.iter.Next(); err != nil {
		// Log error or handle it. KVIterator.Next() doesn't return error.
	}
}

func (it *tikvIterator) Item() KVItem {
	return &tikvItem{key: it.iter.Key(), value: it.iter.Value()}
}

func (it *tikvIterator) Close() {
	it.iter.Close()
}

type tikvWriteBatch struct {
	txn *transaction.KVTxn
}

func (w *tikvWriteBatch) SetAt(key, val []byte, meta byte, ts uint64) error {
	buf := make([]byte, 2+len(val))
	buf[0] = meta
	buf[1] = 0 // Reserved
	copy(buf[2:], val)
	return w.txn.Set(key, buf)
}

func (w *tikvWriteBatch) DeleteAt(key []byte, ts uint64) error {
	return w.txn.Delete(key)
}

func (w *tikvWriteBatch) Flush() error {
	return w.txn.Commit(context.Background())
}

func (w *tikvWriteBatch) Write(buf *z.Buffer) error {
	// Parse buffer and write to txn.
	return nil
}
