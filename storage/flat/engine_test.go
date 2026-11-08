package flat

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/wisdomatom/stateless-graph/x"
)

func newTestKV(t *testing.T) (x.KVDB, func()) {
	dir := t.TempDir()
	opt := badger.DefaultOptions(dir).WithLoggingLevel(badger.ERROR)
	db, err := badger.OpenManaged(opt)
	require.NoError(t, err)
	kv := x.NewBadgerKV(db)
	return kv, func() {
		_ = kv.Close()
	}
}

func TestEngineDataOperations(t *testing.T) {
	kv, cleanup := newTestKV(t)
	defer cleanup()

	eng := NewEngine()
	txn := kv.NewTransactionAt(1, true)
	defer txn.Discard()

	shardID := uint16(0)
	ns := uint32(0)
	attrID := uint32(10)
	uid := uint64(1001)

	// 1. 写入标量数据
	valType := byte(3) // e.g. String
	data := []byte("Alice")
	err := eng.SetData(txn, shardID, ns, attrID, uid, valType, data)
	require.NoError(t, err)

	// 2. 读取标量数据
	gotType, gotData, err := eng.GetData(txn, shardID, ns, attrID, uid)
	require.NoError(t, err)
	require.Equal(t, valType, gotType)
	require.Equal(t, data, gotData)

	// 3. 删除标量数据
	err = eng.DeleteData(txn, shardID, ns, attrID, uid)
	require.NoError(t, err)

	_, _, err = eng.GetData(txn, shardID, ns, attrID, uid)
	require.ErrorIs(t, err, x.ErrKeyNotFound)
}

func TestEngineEdgeOperations(t *testing.T) {
	kv, cleanup := newTestKV(t)
	defer cleanup()

	eng := NewEngine()
	txn := kv.NewTransactionAt(1, true)
	defer txn.Discard()

	shardID := uint16(0)
	ns := uint32(0)
	attrID := uint32(20) // friends
	userA := uint64(1001)

	// 为 userA 添加 3 个好友: 2001, 2002, 2003
	friends := []uint64{2001, 2002, 2003}
	for _, f := range friends {
		err := eng.AddEdge(txn, shardID, ns, attrID, userA, f, []byte("facet_data"))
		require.NoError(t, err)
	}

	// 另一个用户 userB 的好友 3001
	userB := uint64(1002)
	err := eng.AddEdge(txn, shardID, ns, attrID, userB, 3001, nil)
	require.NoError(t, err)

	// 遍历 userA 的出边
	it, err := eng.ScanOutEdges(txn, shardID, ns, attrID, userA)
	require.NoError(t, err)
	defer it.Close()

	var gotFriends []uint64
	for it.Next() {
		entry := it.Entry()
		require.Equal(t, userA, entry.FromUID)
		gotFriends = append(gotFriends, entry.ToUID)
		require.Equal(t, []byte("facet_data"), entry.Facets)
	}
	require.NoError(t, it.Error())
	require.Equal(t, friends, gotFriends)

	// 删除一个好友 2002
	err = eng.DeleteEdge(txn, shardID, ns, attrID, userA, 2002)
	require.NoError(t, err)

	// 再次遍历，确认只剩 2001 和 2003
	it2, err := eng.ScanOutEdges(txn, shardID, ns, attrID, userA)
	require.NoError(t, err)
	defer it2.Close()

	var remaining []uint64
	for it2.Next() {
		remaining = append(remaining, it2.Entry().ToUID)
	}
	require.Equal(t, []uint64{2001, 2003}, remaining)
}

func TestEngineIndexOperations(t *testing.T) {
	kv, cleanup := newTestKV(t)
	defer cleanup()

	eng := NewEngine()
	txn := kv.NewTransactionAt(1, true)
	defer txn.Discard()

	shardID := uint16(0)
	ns := uint32(0)
	attrID := uint32(30)
	tokenActive := []byte("active")

	// 3 个用户都是 active
	uids := []uint64{101, 202, 303}
	for _, uid := range uids {
		err := eng.AddIndex(txn, shardID, ns, attrID, tokenActive, uid)
		require.NoError(t, err)
	}

	// 扫描索引
	gotUIDs, err := eng.ScanIndex(txn, shardID, ns, attrID, tokenActive)
	require.NoError(t, err)
	require.Equal(t, uids, gotUIDs)

	// 删除 202
	err = eng.DeleteIndex(txn, shardID, ns, attrID, tokenActive, 202)
	require.NoError(t, err)

	gotUIDs2, err := eng.ScanIndex(txn, shardID, ns, attrID, tokenActive)
	require.NoError(t, err)
	require.Equal(t, []uint64{101, 303}, gotUIDs2)
}
