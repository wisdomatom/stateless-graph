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

func TestFlatStorageE2EWorkflow(t *testing.T) {
	dir := t.TempDir()
	opt := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)
	db, err := badger.OpenManaged(opt)
	require.NoError(t, err)
	defer db.Close()

	kv := x.NewBadgerKV(db)
	pstore = kv
	posting.Init(kv, 0, false)
	Init(kv)
	schema.Init(kv)

	// 开启扁平存储模式开关！
	oldFlag := x.WorkerConfig.UseFlatStorage
	x.WorkerConfig.UseFlatStorage = true
	defer func() {
		x.WorkerConfig.UseFlatStorage = oldFlag
	}()

	ctx := context.Background()

	// 1. 初始化 Schema
	schema.State().Set("friend", &pb.SchemaUpdate{
		Predicate: "friend",
		ValueType: pb.Posting_UID,
		Directive: pb.SchemaUpdate_REVERSE,
	})
	schema.State().Set("name", &pb.SchemaUpdate{
		Predicate: "name",
		ValueType: pb.Posting_STRING,
	})

	startTs := uint64(10)
	txn := posting.Oracle().RegisterStartTs(startTs)

	userA := uint64(1001)
	userB := uint64(2001)
	userC := uint64(3001)

	// 2. 执行 Mutation: userA 拥有 friend userB 与 userC
	edge1 := &pb.DirectedEdge{
		Entity:  userA,
		Attr:    "friend",
		ValueId: userB,
		Op:      pb.DirectedEdge_SET,
	}
	edge2 := &pb.DirectedEdge{
		Entity:  userA,
		Attr:    "friend",
		ValueId: userC,
		Op:      pb.DirectedEdge_SET,
	}
	require.NoError(t, runMutation(ctx, edge1, txn))
	require.NoError(t, runMutation(ctx, edge2, txn))

	// 3. 执行 CommitOverNetwork (2PC 提交到磁盘)
	tctx := &api.TxnContext{StartTs: startTs}
	commitTs, err := CommitOverNetwork(ctx, tctx)
	require.NoError(t, err)
	require.Greater(t, commitTs, startTs)

	// 4. 查询遍历: 查询 userA 的所有朋友
	q := &pb.Query{
		Attr:    "friend",
		ReadTs:  commitTs,
		UidList: &pb.List{Uids: []uint64{userA}},
	}
	srcFn, err := parseSrcFn(ctx, q)
	require.NoError(t, err)

	out := &pb.Result{}
	args := funcArgs{q: q, gid: 1, srcFn: srcFn, out: out}
	qs := &queryState{cache: posting.NoCache(commitTs)}

	err = qs.handleUidPostings(ctx, args, posting.ListOptions{})
	require.NoError(t, err)

	require.Len(t, out.UidMatrix, 1)
	require.Equal(t, []uint64{userB, userC}, out.UidMatrix[0].Uids)

	// 5. 反向边查询遍历: 查询谁的朋友是 userB (应该查出 userA)
	qRev := &pb.Query{
		Attr:    "friend",
		ReadTs:  commitTs,
		Reverse: true,
		UidList: &pb.List{Uids: []uint64{userB}},
	}
	srcFnRev, err := parseSrcFn(ctx, qRev)
	require.NoError(t, err)

	outRev := &pb.Result{}
	argsRev := funcArgs{q: qRev, gid: 1, srcFn: srcFnRev, out: outRev}
	err = qs.handleUidPostings(ctx, argsRev, posting.ListOptions{})
	require.NoError(t, err)

	require.Len(t, outRev.UidMatrix, 1)
	require.Equal(t, []uint64{userA}, outRev.UidMatrix[0].Uids)
}
