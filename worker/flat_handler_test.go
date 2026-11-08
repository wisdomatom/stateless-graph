package worker

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"

	"github.com/wisdomatom/stateless-graph/posting"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/x"
)

func newTestFlatKV(t *testing.T) (x.KVDB, func()) {
	dir := t.TempDir()
	opt := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)
	db, err := badger.OpenManaged(opt)
	require.NoError(t, err)
	kv := x.NewBadgerKV(db)
	return kv, func() {
		_ = kv.Close()
	}
}

func TestFlatMutationAndScan(t *testing.T) {
	kv, cleanup := newTestFlatKV(t)
	defer cleanup()

	ctx := context.Background()
	schema.Init(kv)
	txn := kv.NewTransactionAt(1, true)
	defer txn.Discard()

	// 1. 初始化 Schema
	schema.State().Set("friend", &pb.SchemaUpdate{
		Predicate: "friend",
		ValueType: pb.Posting_UID,
		Directive: pb.SchemaUpdate_REVERSE, // 开启反向边
		List:      true,
	})
	schema.State().Set("name", &pb.SchemaUpdate{
		Predicate: "name",
		ValueType: pb.Posting_STRING,
	})

	user1 := uint64(100)
	user2 := uint64(200)
	user3 := uint64(300)

	// 2. 插入标量属性: user1 的名字为 "Alice"
	edgeName := &pb.DirectedEdge{
		Entity:    user1,
		Attr:      "name",
		Value:     []byte("Alice"),
		ValueType: pb.Posting_STRING,
		Op:        pb.DirectedEdge_SET,
	}
	err := ApplyFlatMutation(ctx, edgeName, txn)
	require.NoError(t, err)

	// 3. 插入关系边: user1 -> user2, user1 -> user3
	edgeFriend1 := &pb.DirectedEdge{
		Entity:  user1,
		Attr:    "friend",
		ValueId: user2,
		Facets: []*api.Facet{
			{Key: "since", Value: []byte("2024")},
		},
		Op: pb.DirectedEdge_SET,
	}
	edgeFriend2 := &pb.DirectedEdge{
		Entity:  user1,
		Attr:    "friend",
		ValueId: user3,
		Op:      pb.DirectedEdge_SET,
	}
	err = ApplyFlatMutation(ctx, edgeFriend1, txn)
	require.NoError(t, err)
	err = ApplyFlatMutation(ctx, edgeFriend2, txn)
	require.NoError(t, err)

	// 4. 扫描出边 (user1 的朋友)
	opts := posting.ListOptions{}
	res, facets, err := FlatScanEdges(ctx, txn, "friend", user1, opts)
	require.NoError(t, err)
	require.Equal(t, []uint64{user2, user3}, res.Uids)
	require.Len(t, facets, 2)
	require.Equal(t, "since", facets[0].Facets[0].Key)
	require.Equal(t, []byte("2024"), facets[0].Facets[0].Value)

	// 5. 扫描反向边 (谁的朋友是 user2? 应该是 user1)
	revRes, err := FlatScanReverseEdges(ctx, txn, "friend", user2, opts)
	require.NoError(t, err)
	require.Equal(t, []uint64{user1}, revRes.Uids)

	// 6. 测试分页截断: 只要 1 个好友
	optsLimit := posting.ListOptions{First: 1}
	resLimit, _, err := FlatScanEdges(ctx, txn, "friend", user1, optsLimit)
	require.NoError(t, err)
	require.Equal(t, []uint64{user2}, resLimit.Uids)

	// 7. 测试删除边: 删除 user1 -> user2
	delFriend := &pb.DirectedEdge{
		Entity:  user1,
		Attr:    "friend",
		ValueId: user2,
		Op:      pb.DirectedEdge_DEL,
	}
	err = ApplyFlatMutation(ctx, delFriend, txn)
	require.NoError(t, err)

	// 再次查询出边，应该只剩 user3
	resAfterDel, _, err := FlatScanEdges(ctx, txn, "friend", user1, opts)
	require.NoError(t, err)
	require.Equal(t, []uint64{user3}, resAfterDel.Uids)

	// 再次查询反向边，user2 应该没有入边了
	revResAfterDel, err := FlatScanReverseEdges(ctx, txn, "friend", user2, opts)
	require.NoError(t, err)
	require.Empty(t, revResAfterDel.Uids)
}

func TestFlatScalarListAndIndexedDelete(t *testing.T) {
	kv, cleanup := newTestFlatKV(t)
	defer cleanup()

	ctx := context.Background()
	schema.State().Set("tags", &pb.SchemaUpdate{
		Predicate: "tags",
		ValueType: pb.Posting_STRING,
		List:      true,
	})
	schema.State().Set("age", &pb.SchemaUpdate{
		Predicate: "age",
		ValueType: pb.Posting_INT,
		Directive: pb.SchemaUpdate_INDEX,
		Tokenizer: []string{"int"},
	})

	txn := kv.NewTransactionAt(10, true)
	defer txn.Discard()

	u := uint64(500)
	// 1. 添加 tag "tagA" 和 "tagB"
	require.NoError(t, ApplyFlatMutation(ctx, &pb.DirectedEdge{
		Entity:    u,
		Attr:      "tags",
		Value:     []byte("tagA"),
		ValueType: pb.Posting_STRING,
		Op:        pb.DirectedEdge_SET,
	}, txn))
	require.NoError(t, ApplyFlatMutation(ctx, &pb.DirectedEdge{
		Entity:    u,
		Attr:      "tags",
		Value:     []byte("tagB"),
		ValueType: pb.Posting_STRING,
		Op:        pb.DirectedEdge_SET,
	}, txn))

	// 2. 扫描 tags，应该有 2 个
	valTypes, valBytes, err := GlobalFlatEngine.ScanData(txn, 0, 0, flat.PredicateToAttrID("tags"), u)
	require.NoError(t, err)
	require.Len(t, valTypes, 2)
	require.Len(t, valBytes, 2)

	// 3. 删除单个 tag "tagA"
	require.NoError(t, ApplyFlatMutation(ctx, &pb.DirectedEdge{
		Entity:    u,
		Attr:      "tags",
		Value:     []byte("tagA"),
		ValueType: pb.Posting_STRING,
		Op:        pb.DirectedEdge_DEL,
	}, txn))
	valTypes, valBytes, err = GlobalFlatEngine.ScanData(txn, 0, 0, flat.PredicateToAttrID("tags"), u)
	require.NoError(t, err)
	require.Len(t, valBytes, 1)
	require.Equal(t, []byte("tagB"), valBytes[0])

	// 4. 测试带索引的整型标量写入与删除 (验证不触发 Tokenizer 类型转换 panic)
	ageBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(ageBytes, 30)
	require.NoError(t, ApplyFlatMutation(ctx, &pb.DirectedEdge{
		Entity:    u,
		Attr:      "age",
		Value:     ageBytes,
		ValueType: pb.Posting_INT,
		Op:        pb.DirectedEdge_SET,
	}, txn))

	tokenizer, ok := tok.GetTokenizer("int")
	require.True(t, ok)
	toks, err := tok.BuildTokens(int64(30), tokenizer)
	require.NoError(t, err)
	require.NotEmpty(t, toks)

	// 验证索引存在
	idxUIDs, err := GlobalFlatEngine.ScanIndex(txn, 0, 0, flat.PredicateToAttrID("age"), []byte(toks[0]))
	require.NoError(t, err)
	require.Contains(t, idxUIDs, u)

	// 删除 age，不应 panic，且索引被清空
	require.NoError(t, ApplyFlatMutation(ctx, &pb.DirectedEdge{
		Entity:    u,
		Attr:      "age",
		Op:        pb.DirectedEdge_DEL,
	}, txn))

	idxUIDs, err = GlobalFlatEngine.ScanIndex(txn, 0, 0, flat.PredicateToAttrID("age"), []byte(toks[0]))
	require.NoError(t, err)
	require.Empty(t, idxUIDs)
}

func TestFlatInequalityTokens(t *testing.T) {
	kv, cleanup := newTestFlatKV(t)
	defer cleanup()

	pstore = kv
	Init(pstore)
	schema.Init(kv)
	x.WorkerConfig.UseFlatStorage = true
	defer func() {
		x.WorkerConfig.UseFlatStorage = false
	}()

	ctx := context.Background()
	schema.State().Set("score", &pb.SchemaUpdate{
		Predicate: "score",
		ValueType: pb.Posting_INT,
		Directive: pb.SchemaUpdate_INDEX,
		Tokenizer: []string{"int"},
	})

	// 插入三条记录：score=10 (uid: 101), score=20 (uid: 102), score=30 (uid: 103)
	txn := kv.NewTransactionAt(10, true)
	for _, item := range []struct {
		uid   uint64
		score int64
	}{
		{101, 10},
		{102, 20},
		{103, 30},
	} {
		var valBytes [8]byte
		binary.LittleEndian.PutUint64(valBytes[:], uint64(item.score))
		require.NoError(t, ApplyFlatMutation(ctx, &pb.DirectedEdge{
			Entity:    item.uid,
			Attr:      "score",
			Value:     valBytes[:],
			ValueType: pb.Posting_INT,
			Op:        pb.DirectedEdge_SET,
		}, txn))
	}
	require.NoError(t, txn.CommitAt(10, nil))

	// 测试 ge(score, 20)
	{
		toks, _, err := getInequalityTokens(ctx, 10, "score", "ge", "", []types.Val{
			{Tid: types.IntID, Value: int64(20)},
		})
		require.NoError(t, err)
		require.Len(t, toks, 2) // 20, 30
		var matchedUIDs []uint64
		readTxn := pstore.NewTransactionAt(10, false)
		for _, tok := range toks {
			uids, err := GlobalFlatEngine.ScanIndex(readTxn, 0, 0, flat.PredicateToAttrID("score"), []byte(tok))
			require.NoError(t, err)
			matchedUIDs = append(matchedUIDs, uids...)
		}
		readTxn.Discard()
		require.ElementsMatch(t, []uint64{102, 103}, matchedUIDs)
	}

	// 测试 le(score, 20)
	{
		toks, _, err := getInequalityTokens(ctx, 10, "score", "le", "", []types.Val{
			{Tid: types.IntID, Value: int64(20)},
		})
		require.NoError(t, err)
		require.Len(t, toks, 2) // 20, 10
		var matchedUIDs []uint64
		readTxn := pstore.NewTransactionAt(10, false)
		for _, tok := range toks {
			uids, err := GlobalFlatEngine.ScanIndex(readTxn, 0, 0, flat.PredicateToAttrID("score"), []byte(tok))
			require.NoError(t, err)
			matchedUIDs = append(matchedUIDs, uids...)
		}
		readTxn.Discard()
		require.ElementsMatch(t, []uint64{101, 102}, matchedUIDs)
	}

	// 测试 between(score, 15, 25)
	{
		toks, _, err := getInequalityTokens(ctx, 10, "score", "between", "", []types.Val{
			{Tid: types.IntID, Value: int64(15)},
			{Tid: types.IntID, Value: int64(25)},
		})
		require.NoError(t, err)
		require.Len(t, toks, 1) // 20
		readTxn := pstore.NewTransactionAt(10, false)
		uids, err := GlobalFlatEngine.ScanIndex(readTxn, 0, 0, flat.PredicateToAttrID("score"), []byte(toks[0]))
		require.NoError(t, err)
		readTxn.Discard()
		require.Equal(t, []uint64{102}, uids)
	}

	// 测试 gt(score, 20)
	{
		toks, _, err := getInequalityTokens(ctx, 10, "score", "gt", "", []types.Val{
			{Tid: types.IntID, Value: int64(20)},
		})
		require.NoError(t, err)
		require.Len(t, toks, 1) // 30
		readTxn := pstore.NewTransactionAt(10, false)
		uids, err := GlobalFlatEngine.ScanIndex(readTxn, 0, 0, flat.PredicateToAttrID("score"), []byte(toks[0]))
		require.NoError(t, err)
		readTxn.Discard()
		require.Equal(t, []uint64{103}, uids)
	}

	// 测试 lt(score, 20)
	{
		toks, _, err := getInequalityTokens(ctx, 10, "score", "lt", "", []types.Val{
			{Tid: types.IntID, Value: int64(20)},
		})
		require.NoError(t, err)
		require.Len(t, toks, 1) // 10
		readTxn := pstore.NewTransactionAt(10, false)
		uids, err := GlobalFlatEngine.ScanIndex(readTxn, 0, 0, flat.PredicateToAttrID("score"), []byte(toks[0]))
		require.NoError(t, err)
		readTxn.Discard()
		require.Equal(t, []uint64{101}, uids)
	}
}
