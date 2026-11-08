/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	bpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/x"
	"github.com/stretchr/testify/require"
)

func readPostingListFromDisk(key []byte, kv x.KVDB, readTs uint64) (*List, error) {
	txn := kv.NewTransactionAt(readTs, false)
	defer txn.Discard()

	opt := x.KVIterOpts{
		Prefix:      key,
		AllVersions: true,
	}
	itr := txn.NewIterator(opt)
	defer itr.Close()
	itr.Seek(key)
	return ReadPostingList(key, itr)
}

func rollup(t *testing.T, key []byte, kv x.KVDB, readTs uint64) {
	ol, err := readPostingListFromDisk(key, kv, readTs)
	require.NoError(t, err)
	kvs, err := ol.Rollup(nil, readTs+1)
	require.NoError(t, err)
	require.NoError(t, writePostingListToDisk(kvs))
	ResetCache()
}

func writePostingListToDisk(kvs []*bpb.KV) error {
	writer := NewTxnWriter(pstore)
	for _, kv := range kvs {
		if err := writer.SetAt(kv.Key, kv.Value, kv.UserMeta[0], kv.Version); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func TestEmptyTypeSchema(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)

	typeName := "1-temp"
	ts := uint64(10)
	txn := pstore.NewTransactionAt(ts, true)
	defer txn.Discard()
	e := &badger.Entry{
		Key:      x.TypeKey(typeName),
		Value:    make([]byte, 0),
		UserMeta: BitSchemaPosting,
	}
	require.Nil(t, txn.SetEntry(e.WithDiscard()))
	require.Nil(t, txn.CommitAt(ts, nil))

	schema.Init(bkv)
	require.Nil(t, schema.LoadFromDb(context.Background()))

	req := &pb.SchemaRequest{}
	types, err := GetTypes(context.Background(), req)
	require.Nil(t, err)

	require.Equal(t, 1, len(types))
	x.ParseNamespaceAttr(types[0].TypeName)
}

func TestDeleteSetWithVarEdgeCorruptsData(t *testing.T) {
	// Setup temporary directory for Badger DB
	dir, err := os.MkdirTemp("", "storetest_")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	require.NoError(t, err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)

	// Set schema
	schemaTxt := `
		room: string @index(hash) @upsert .
		person: string @index(hash) @upsert .
		office: uid @reverse @count .
	`
	err = schema.ParseBytes([]byte(schemaTxt), 1)
	require.NoError(t, err)

	ctx := context.Background()
	attrRoom := x.AttrInRootNamespace("room")
	attrPerson := x.AttrInRootNamespace("person")
	attrOffice := x.AttrInRootNamespace("office")

	uidRoom := uint64(1)
	uidJohn := uint64(2)

	runMutation := func(startTs, commitTs uint64, edges []*pb.DirectedEdge) {
		txn := Oracle().RegisterStartTs(startTs)
		for _, edge := range edges {
			require.NoError(t, runMutation(ctx, edge, txn))
		}
		txn.Update()
		writer := NewTxnWriter(bkv)
		require.NoError(t, txn.CommitToDisk(writer, commitTs))
		require.NoError(t, writer.Flush())
		txn.UpdateCachedKeys(commitTs)
	}

	// Initial mutation: Set John → Leopard
	runMutation(1, 3, []*pb.DirectedEdge{
		{
			Entity:    uidJohn,
			Attr:      attrPerson,
			Value:     []byte("John Smith"),
			ValueType: pb.Posting_STRING,
			Op:        pb.DirectedEdge_SET,
		},
		{
			Entity:    uidRoom,
			Attr:      attrRoom,
			Value:     []byte("Leopard"),
			ValueType: pb.Posting_STRING,
			Op:        pb.DirectedEdge_SET,
		},
		{
			Entity:    uidJohn,
			Attr:      attrOffice,
			ValueId:   uidRoom,
			ValueType: pb.Posting_UID,
			Op:        pb.DirectedEdge_SET,
		},
	})

	key := x.DataKey(attrOffice, uidJohn)
	rollup(t, key, bkv, 4)

	// Second mutation: Remove John from Leopard, assign Amanda
	uidAmanda := uint64(3)

	runMutation(6, 8, []*pb.DirectedEdge{
		{
			Entity:    uidJohn,
			Attr:      attrOffice,
			ValueId:   uidRoom,
			ValueType: pb.Posting_UID,
			Op:        pb.DirectedEdge_DEL,
		},
		{
			Entity:    uidAmanda,
			Attr:      attrPerson,
			Value:     []byte("Amanda Anderson"),
			ValueType: pb.Posting_STRING,
			Op:        pb.DirectedEdge_SET,
		},
		{
			Entity:    uidAmanda,
			Attr:      attrOffice,
			ValueId:   uidRoom,
			ValueType: pb.Posting_UID,
			Op:        pb.DirectedEdge_SET,
		},
	})

	// Read and validate: Amanda assigned, John unassigned
	txnRead := Oracle().RegisterStartTs(10)

	list, err := txnRead.Get(key)
	require.NoError(t, err)

	uids, err := list.Uids(ListOptions{ReadTs: 10})
	require.NoError(t, err)

	// This assertion FAILS in the broken case where both Amanda and John are assigned
	require.Equal(t, 0, len(uids.Uids), "John should no longer have an office assigned")

	keyRev := x.ReverseKey(attrOffice, uidRoom)
	listRev, err := txnRead.Get(keyRev)
	require.NoError(t, err)

	reverseUids, err := listRev.Uids(ListOptions{ReadTs: 10})
	require.NoError(t, err)

	require.Equal(t, []uint64{uidAmanda}, reverseUids.Uids, "Only Amanda should be assigned on reverse edge")
}

func TestGetScalarList(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("scalarPredicateCount4: uid ."), 1)
	require.NoError(t, err)

	runM := func(startTs, commitTs uint64, edges []*pb.DirectedEdge) {
		txn := Oracle().RegisterStartTs(startTs)
		for _, edge := range edges {
			x.Check(runMutation(context.Background(), edge, txn))
		}
		txn.Update()
		writer := NewTxnWriter(pstore)
		require.NoError(t, txn.CommitToDisk(writer, commitTs))
		require.NoError(t, writer.Flush())
		txn.UpdateCachedKeys(commitTs)
	}

	attr := x.AttrInRootNamespace("scalarPredicateCount4")

	runM(5, 7, []*pb.DirectedEdge{{
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	}})

	key := x.DataKey(attr, 1)
	rollup(t, key, bkv, 8)

	runM(9, 11, []*pb.DirectedEdge{{
		ValueId:   5,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	}})

	txn := Oracle().RegisterStartTs(13)
	l, err := txn.Get(key)
	require.Nil(t, err)
	uids, err := l.Uids(ListOptions{ReadTs: 13})
	require.Nil(t, err)
	require.Equal(t, 1, len(uids.Uids))
}

func TestMultipleTxnListCount(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("scalarPredicateCount3: [uid] @count ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	attr := x.AttrInRootNamespace("scalarPredicateCount3")

	runM := func(startTs, commitTs uint64, edges []*pb.DirectedEdge) {
		txn := Oracle().RegisterStartTs(startTs)
		for _, edge := range edges {
			x.Check(runMutation(ctx, edge, txn))
		}
		txn.Update()
		writer := NewTxnWriter(pstore)
		require.NoError(t, txn.CommitToDisk(writer, commitTs))
		require.NoError(t, writer.Flush())
		txn.UpdateCachedKeys(commitTs)
	}

	runM(9, 11, []*pb.DirectedEdge{{
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	}, {
		ValueId:   2,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	}})

	txn := Oracle().RegisterStartTs(13)
	key := x.CountKey(attr, 1, false)
	l, err := txn.Get(key)
	require.Nil(t, err)
	uids, err := l.Uids(ListOptions{ReadTs: 13})
	require.Nil(t, err)
	require.Equal(t, 0, len(uids.Uids))

	key = x.CountKey(attr, 2, false)
	l, err = txn.Get(key)
	require.Nil(t, err)
	uids, err = l.Uids(ListOptions{ReadTs: 13})
	require.Nil(t, err)
	require.Equal(t, 1, len(uids.Uids))
}

func TestScalarPredicateRevCount(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("scalarPredicateCount2: uid @reverse @count ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	attr := x.AttrInRootNamespace("scalarPredicateCount2")

	runM := func(startTs, commitTs uint64, edges []*pb.DirectedEdge) {
		txn := Oracle().RegisterStartTs(startTs)
		for _, edge := range edges {
			x.Check(runMutation(ctx, edge, txn))
		}
		txn.Update()
		writer := NewTxnWriter(pstore)
		require.NoError(t, txn.CommitToDisk(writer, commitTs))
		require.NoError(t, writer.Flush())
		txn.UpdateCachedKeys(commitTs)
	}

	runM(9, 11, []*pb.DirectedEdge{{
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	}, {
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_DEL,
	}})

	txn := Oracle().RegisterStartTs(13)
	key := x.DataKey(attr, 1)
	l, err := txn.Get(key)
	require.Nil(t, err)
	l.RLock()
	require.Equal(t, 0, l.GetLength(13))
	l.RUnlock()

	runM(15, 17, []*pb.DirectedEdge{{
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	}})

	txn = Oracle().RegisterStartTs(18)
	l, err = txn.Get(key)
	require.Nil(t, err)
	l.RLock()
	require.Equal(t, 1, l.GetLength(18))
	l.RUnlock()

	runM(18, 19, []*pb.DirectedEdge{{
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_DEL,
	}})

	txn = Oracle().RegisterStartTs(20)
	l, err = txn.Get(key)
	require.Nil(t, err)
	l.RLock()
	require.Equal(t, 0, l.GetLength(20))
	l.RUnlock()
}

func TestScalarPredicateIntCount(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("scalarPredicateCount1: string @count ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	attr := x.AttrInRootNamespace("scalarPredicateCount1")

	runM := func(startTs, commitTs uint64, edge *pb.DirectedEdge) {
		txn := Oracle().RegisterStartTs(startTs)
		x.Check(runMutation(ctx, edge, txn))
		txn.Update()
		writer := NewTxnWriter(pstore)
		require.NoError(t, txn.CommitToDisk(writer, commitTs))
		require.NoError(t, writer.Flush())
		txn.UpdateCachedKeys(commitTs)
	}

	runM(5, 7, &pb.DirectedEdge{
		Value:     []byte("a"),
		ValueType: pb.Posting_STRING,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	})

	key := x.CountKey(attr, 1, false)
	rollup(t, key, bkv, 8)

	runM(9, 11, &pb.DirectedEdge{
		Value:     []byte("a"),
		ValueType: pb.Posting_STRING,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_DEL,
	})

	txn := Oracle().RegisterStartTs(20)
	l, err := txn.Get(key)
	require.Nil(t, err)
	l.RLock()
	require.Equal(t, 0, l.GetLength(20))
	l.RUnlock()
}

func TestScalarPredicateCount(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("scalarPredicateCount: uid @count ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	attr := x.AttrInRootNamespace("scalarPredicateCount")

	runM := func(startTs, commitTs uint64, edge *pb.DirectedEdge) {
		txn := Oracle().RegisterStartTs(startTs)
		x.Check(runMutation(ctx, edge, txn))
		txn.Update()
		writer := NewTxnWriter(pstore)
		require.NoError(t, txn.CommitToDisk(writer, commitTs))
		require.NoError(t, writer.Flush())
		txn.UpdateCachedKeys(commitTs)
	}

	runM(5, 7, &pb.DirectedEdge{
		ValueId:   2,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	})

	key := x.CountKey(attr, 1, false)
	rollup(t, key, bkv, 8)

	runM(9, 11, &pb.DirectedEdge{
		ValueId:   3,
		ValueType: pb.Posting_UID,
		Attr:      attr,
		Entity:    1,
		Op:        pb.DirectedEdge_SET,
	})

	txn := Oracle().RegisterStartTs(15)
	l, err := txn.Get(key)
	require.Nil(t, err)
	l.RLock()
	require.Equal(t, 1, l.GetLength(15))
	l.RUnlock()
}

func TestSingleUidReplacement(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("singleUidReplaceTest: uid ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	txn := Oracle().RegisterStartTs(5)
	attr := x.AttrInRootNamespace("singleUidReplaceTest")

	// Txn 1. Set 1 -> 2
	x.Check(runMutation(ctx, &pb.DirectedEdge{
		ValueId: 2,
		Attr:    attr,
		Entity:  1,
		Op:      pb.DirectedEdge_SET,
	}, txn))

	txn.Update()
	writer := NewTxnWriter(pstore)
	require.NoError(t, txn.CommitToDisk(writer, 7))
	require.NoError(t, writer.Flush())
	txn.UpdateCachedKeys(7)

	// Txn 2. Set 1 -> 3
	txn = Oracle().RegisterStartTs(9)

	x.Check(runMutation(ctx, &pb.DirectedEdge{
		ValueId: 3,
		Attr:    attr,
		Entity:  1,
		Op:      pb.DirectedEdge_SET,
	}, txn))

	txn.Update()
	writer = NewTxnWriter(pstore)
	require.NoError(t, txn.CommitToDisk(writer, 11))
	require.NoError(t, writer.Flush())
	txn.UpdateCachedKeys(11)

	key := x.DataKey(attr, 1)

	// Reading the david index, we should see 2 inserted, 1 deleted
	txn = Oracle().RegisterStartTs(15)
	l, err := txn.Get(key)
	require.NoError(t, err)

	uids, err := l.Uids(ListOptions{ReadTs: 15})
	require.NoError(t, err)
	require.Equal(t, uids.Uids, []uint64{3})
}

func TestSingleString(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	Init(bkv)
	err = schema.ParseBytes([]byte("singleUidTest: string @index(exact) @unique ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	txn := Oracle().RegisterStartTs(5)
	attr := x.AttrInRootNamespace("singleUidTest")

	// Txn 1. Set 1 -> david 2 -> blush
	x.Check(runMutation(ctx, &pb.DirectedEdge{
		Value:  []byte("david"),
		Attr:   attr,
		Entity: 1,
		Op:     pb.DirectedEdge_SET,
	}, txn))

	x.Check(runMutation(ctx, &pb.DirectedEdge{
		Value:  []byte("blush"),
		Attr:   attr,
		Entity: 2,
		Op:     pb.DirectedEdge_SET,
	}, txn))

	txn.Update()
	writer := NewTxnWriter(pstore)
	require.NoError(t, txn.CommitToDisk(writer, 7))
	require.NoError(t, writer.Flush())
	txn.UpdateCachedKeys(7)

	// Txn 2. Set 2 -> david 1 -> blush
	txn = Oracle().RegisterStartTs(9)

	x.Check(runMutation(ctx, &pb.DirectedEdge{
		Value:  []byte("david"),
		Attr:   attr,
		Entity: 2,
		Op:     pb.DirectedEdge_SET,
	}, txn))

	x.Check(runMutation(ctx, &pb.DirectedEdge{
		Value:  []byte("blush"),
		Attr:   attr,
		Entity: 1,
		Op:     pb.DirectedEdge_SET,
	}, txn))

	txn.Update()
	writer = NewTxnWriter(pstore)
	require.NoError(t, txn.CommitToDisk(writer, 11))
	require.NoError(t, writer.Flush())
	txn.UpdateCachedKeys(11)

	key := x.IndexKey(attr, string([]byte{2, 100, 97, 118, 105, 100}))

	// Reading the david index, we should see 2 inserted, 1 deleted
	txn = Oracle().RegisterStartTs(15)
	l, err := txn.Get(key)
	require.NoError(t, err)

	found, mpost, err := l.FindPosting(15, 2)
	require.NoError(t, err)
	require.Equal(t, found, true)
	require.Equal(t, mpost.Op, uint32(0x1))

	found, _, err = l.FindPosting(15, 1)
	require.NoError(t, err)
	require.Equal(t, found, false)

	rollup(t, key, pstore, 16)

	txn = Oracle().RegisterStartTs(18)
	l, err = txn.Get(key)
	require.NoError(t, err)

	found, mpost, err = l.FindPosting(18, 2)
	require.NoError(t, err)
	require.Equal(t, found, true)
	require.Equal(t, mpost.Op, uint32(0x0))

	found, _, err = l.FindPosting(18, 1)
	require.NoError(t, err)
	require.Equal(t, found, false)
}

func TestLangExact(t *testing.T) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	// Not using posting list cache
	Init(bkv)
	err = schema.ParseBytes([]byte("testLang: string @index(term) @lang ."), 1)
	require.NoError(t, err)

	ctx := context.Background()
	txn := Oracle().RegisterStartTs(5)
	attr := x.AttrInRootNamespace("testLang")

	edge := &pb.DirectedEdge{
		Value:  []byte("english"),
		Attr:   attr,
		Entity: 1,
		Op:     pb.DirectedEdge_SET,
		Lang:   "en",
	}

	x.Check(runMutation(ctx, edge, txn))

	edge = &pb.DirectedEdge{
		Value:  []byte("hindi"),
		Attr:   attr,
		Entity: 1,
		Op:     pb.DirectedEdge_SET,
		Lang:   "hi",
	}

	x.Check(runMutation(ctx, edge, txn))

	txn.Update()
	writer := NewTxnWriter(pstore)
	require.NoError(t, txn.CommitToDisk(writer, 2))
	require.NoError(t, writer.Flush())
	txn.UpdateCachedKeys(2)

	key := x.DataKey(attr, 1)
	rollup(t, key, pstore, 4)

	pl, err := readPostingListFromDisk(key, pstore, 6)
	require.NoError(t, err)

	val, err := pl.ValueForTag(6, "hi")
	require.NoError(t, err)
	require.Equal(t, val.Value, []byte("hindi"))
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func BenchmarkAddMutationWithIndex(b *testing.B) {
	dir, err := os.MkdirTemp("", "storetest_")
	x.Check(err)
	defer os.RemoveAll(dir)

	opt := badger.DefaultOptions(dir)
	ps, err := badger.OpenManaged(opt)
	x.Check(err)
	bkv := x.NewBadgerKV(ps)
	pstore = bkv
	// Not using posting list cache
	Init(bkv)
	err = schema.ParseBytes([]byte("benchmarkadd: string @index(term) ."), 1)
	fmt.Println(err)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	txn := Oracle().RegisterStartTs(5)
	attr := x.AttrInRootNamespace("benchmarkadd")

	n := uint64(1000)
	values := make([]string, 0)
	for range n {
		values = append(values, randStringBytes(5))
	}

	for i := 0; i < b.N; i++ {
		edge := &pb.DirectedEdge{
			Value:  []byte(values[rand.Intn(int(n))]),
			Attr:   attr,
			Entity: rand.Uint64()%n + 1,
			Op:     pb.DirectedEdge_SET,
		}

		x.Check(runMutation(ctx, edge, txn))
	}
}

func TestRemoveDuplicates(t *testing.T) {
	toSet := func(uids []uint64) map[uint64]struct{} {
		m := make(map[uint64]struct{})
		for _, uid := range uids {
			m[uid] = struct{}{}
		}
		return m
	}

	for _, test := range []struct {
		setIn   []uint64
		setOut  []uint64
		uidsIn  []uint64
		uidsOut []uint64
	}{
		{setIn: nil, setOut: nil, uidsIn: nil, uidsOut: nil},
		{setIn: nil, setOut: []uint64{2}, uidsIn: []uint64{2}, uidsOut: []uint64{2}},
		{setIn: []uint64{2}, setOut: []uint64{2}, uidsIn: []uint64{2}, uidsOut: []uint64{}},
		{setIn: []uint64{2}, setOut: []uint64{2}, uidsIn: []uint64{2, 2}, uidsOut: []uint64{}},
		{
			setIn:   []uint64{2, 3},
			setOut:  []uint64{2, 3, 4, 5},
			uidsIn:  []uint64{3, 4, 5},
			uidsOut: []uint64{4, 5},
		},
	} {
		set := toSet(test.setIn)
		uidsOut := removeDuplicates(test.uidsIn, set)
		require.Equal(t, uidsOut, test.uidsOut)
		require.Equal(t, set, toSet(test.setOut))
	}
}
