package worker

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"

	"github.com/wisdomatom/stateless-graph/posting"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
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
