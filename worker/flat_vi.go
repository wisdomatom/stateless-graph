/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/wisdomatom/stateless-graph/posting"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok/index"
	"github.com/wisdomatom/stateless-graph/x"
)

// flatViTxn implements index.Txn for writing vector indexes directly into FlatStorage.
type flatViTxn struct {
	sync.RWMutex
	txn     x.KVTxn
	startTs uint64
	mem     map[string][]byte
}

func newFlatViTxn(txn x.KVTxn, startTs uint64) *flatViTxn {
	return &flatViTxn{
		txn:     txn,
		startTs: startTs,
		mem:     make(map[string][]byte),
	}
}

func (t *flatViTxn) StartTs() uint64 {
	return t.startTs
}

func (t *flatViTxn) Get(key []byte) ([]byte, error) {
	t.RLock()
	if val, ok := t.mem[string(key)]; ok {
		t.RUnlock()
		return val, nil
	}
	t.RUnlock()

	pk, err := x.Parse(key)
	if err != nil {
		return nil, err
	}
	ns, rawAttr := parseNamespaceSafe(pk.Attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	shardID := flat.CalculateShardID(pk.Uid, 0)
	_, valBytes, err := GlobalFlatEngine.GetData(t.txn, shardID, ns, attrID, pk.Uid)
	if err != nil || len(valBytes) == 0 {
		return nil, posting.ErrNoValue
	}
	return valBytes, nil
}

func (t *flatViTxn) GetWithLockHeld(key []byte) ([]byte, error) {
	return t.Get(key)
}

func (t *flatViTxn) Find(pred []byte, filter func([]byte) bool) (uint64, error) {
	attr := string(pred)
	ns, rawAttr := parseNamespaceSafe(attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	prefix := make([]byte, 11)
	binary.BigEndian.PutUint16(prefix[0:2], 0)
	binary.BigEndian.PutUint32(prefix[2:6], ns)
	prefix[6] = flat.KeyTypeData
	binary.BigEndian.PutUint32(prefix[7:11], attrID)

	opt := x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: true,
	}
	it := t.txn.NewIterator(opt)
	defer it.Close()

	for it.Seek(prefix); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		if len(k) != 19 || !bytes.HasPrefix(k, prefix) {
			break
		}
		uid := binary.BigEndian.Uint64(k[11:19])
		val, err := item.ValueCopy(nil)
		if err != nil || len(val) <= 1 {
			continue
		}
		if filter(val[1:]) {
			return uid, nil
		}
	}
	return 0, nil
}

func (t *flatViTxn) AddMutation(ctx context.Context, key []byte, kv *index.KeyValue) error {
	t.Lock()
	t.mem[string(key)] = kv.Value
	t.Unlock()

	ns, rawAttr := parseNamespaceSafe(kv.Attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	shardID := flat.CalculateShardID(kv.Entity, 0)
	return GlobalFlatEngine.SetData(t.txn, shardID, ns, attrID, kv.Entity, 0, kv.Value)
}

func (t *flatViTxn) AddMutationWithLockHeld(ctx context.Context, key []byte, kv *index.KeyValue) error {
	return t.AddMutation(ctx, key, kv)
}

func (t *flatViTxn) LockKey(key []byte)   {}
func (t *flatViTxn) UnlockKey(key []byte) {}

// flatViCache implements index.LocalCache for reading vector indexes from FlatStorage.
type flatViCache struct {
	readTs uint64
}

func newFlatViCache(readTs uint64) *flatViCache {
	return &flatViCache{readTs: readTs}
}

func (c *flatViCache) Get(key []byte) ([]byte, error) {
	pk, err := x.Parse(key)
	if err != nil {
		return nil, err
	}
	ns, rawAttr := parseNamespaceSafe(pk.Attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	shardID := flat.CalculateShardID(pk.Uid, 0)

	readTxn := pstore.NewTransactionAt(c.readTs, false)
	defer readTxn.Discard()

	_, valBytes, err := GlobalFlatEngine.GetData(readTxn, shardID, ns, attrID, pk.Uid)
	if err != nil || len(valBytes) == 0 {
		return nil, posting.ErrNoValue
	}
	return valBytes, nil
}

func (c *flatViCache) GetWithLockHeld(key []byte) ([]byte, error) {
	return c.Get(key)
}

func (c *flatViCache) Find(pred []byte, filter func([]byte) bool) (uint64, error) {
	attr := string(pred)
	ns, rawAttr := parseNamespaceSafe(attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	prefix := make([]byte, 11)
	binary.BigEndian.PutUint16(prefix[0:2], 0)
	binary.BigEndian.PutUint32(prefix[2:6], ns)
	prefix[6] = flat.KeyTypeData
	binary.BigEndian.PutUint32(prefix[7:11], attrID)

	readTxn := pstore.NewTransactionAt(c.readTs, false)
	defer readTxn.Discard()

	opt := x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: true,
	}
	it := readTxn.NewIterator(opt)
	defer it.Close()

	for it.Seek(prefix); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		if len(k) != 19 || !bytes.HasPrefix(k, prefix) {
			break
		}
		uid := binary.BigEndian.Uint64(k[11:19])
		val, err := item.ValueCopy(nil)
		if err != nil || len(val) <= 1 {
			continue
		}
		if filter(val[1:]) {
			return uid, nil
		}
	}
	return 0, nil
}
