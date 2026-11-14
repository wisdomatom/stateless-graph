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

	// 使用 ScanOutEdgesFrom 测试精准 Seek 分页 (afterUID = 2001，应只读出 2003)
	itSeek, err := eng.ScanOutEdgesFrom(txn, shardID, ns, attrID, userA, 2001)
	require.NoError(t, err)
	defer itSeek.Close()

	var seekResults []uint64
	for itSeek.Next() {
		seekResults = append(seekResults, itSeek.Entry().ToUID)
	}
	require.Equal(t, []uint64{2003}, seekResults)
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

func TestEngineScanIndexExtremum(t *testing.T) {
	kv, cleanup := newTestKV(t)
	defer cleanup()

	eng := NewEngine()
	txn := kv.NewTransactionAt(1, true)
	defer txn.Discard()

	shardID := uint16(0)
	ns := uint32(0)
	attrID := uint32(40)

	// 模拟写入几个有序数值索引 (例如 age: 18 -> uid 1, 25 -> uid 2, 42 -> uid 3)
	// 使用固定宽度字节或字典序字符模拟
	err := eng.AddIndex(txn, shardID, ns, attrID, []byte("018"), 101)
	require.NoError(t, err)
	err = eng.AddIndex(txn, shardID, ns, attrID, []byte("025"), 102)
	require.NoError(t, err)
	err = eng.AddIndex(txn, shardID, ns, attrID, []byte("042"), 103)
	require.NoError(t, err)

	// 1. 测试取最小值 Min (isMax = false)
	minToken, minUID, foundMin, err := eng.ScanIndexExtremum(txn, shardID, ns, attrID, 0, false)
	require.NoError(t, err)
	require.True(t, foundMin)
	require.Equal(t, []byte("018"), minToken)
	require.Equal(t, uint64(101), minUID)

	// 2. 测试取最大值 Max (isMax = true)
	maxToken, maxUID, foundMax, err := eng.ScanIndexExtremum(txn, shardID, ns, attrID, 0, true)
	require.NoError(t, err)
	require.True(t, foundMax)
	require.Equal(t, []byte("042"), maxToken)
	require.Equal(t, uint64(103), maxUID)

	// 3. 测试空属性的极值获取
	_, _, foundEmpty, err := eng.ScanIndexExtremum(txn, shardID, ns, 99999, 0, false)
	require.NoError(t, err)
	require.False(t, foundEmpty)
}
