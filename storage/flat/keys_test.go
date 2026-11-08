package flat

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeDataKey(t *testing.T) {
	shardID := uint16(10)
	ns := uint32(1)
	attrID := uint32(100)
	uid := uint64(99999)

	k := EncodeDataKey(shardID, ns, attrID, uid)
	require.Equal(t, DataKeyLen, len(k))

	pk, err := DecodeKey(k)
	require.NoError(t, err)
	require.Equal(t, shardID, pk.ShardID)
	require.Equal(t, ns, pk.Namespace)
	require.Equal(t, KeyTypeData, pk.KeyType)
	require.Equal(t, attrID, pk.AttrID)
	require.Equal(t, uid, pk.Uid1)
}

func TestEncodeDecodeEdgeKey(t *testing.T) {
	shardID := uint16(55)
	ns := uint32(2)
	attrID := uint32(200)
	fromUID := uint64(1001)
	toUID := uint64(2002)

	k := EncodeEdgeKey(shardID, ns, attrID, fromUID, toUID)
	require.Equal(t, EdgeKeyLen, len(k))

	pk, err := DecodeKey(k)
	require.NoError(t, err)
	require.Equal(t, shardID, pk.ShardID)
	require.Equal(t, ns, pk.Namespace)
	require.Equal(t, KeyTypeEdge, pk.KeyType)
	require.Equal(t, attrID, pk.AttrID)
	require.Equal(t, fromUID, pk.Uid1)
	require.Equal(t, toUID, pk.Uid2)
}

func TestEncodeDecodeReverseKey(t *testing.T) {
	shardID := uint16(12)
	ns := uint32(0)
	attrID := uint32(300)
	toUID := uint64(5005)
	fromUID := uint64(6006)

	k := EncodeReverseKey(shardID, ns, attrID, toUID, fromUID)
	require.Equal(t, EdgeKeyLen, len(k))

	pk, err := DecodeKey(k)
	require.NoError(t, err)
	require.Equal(t, shardID, pk.ShardID)
	require.Equal(t, ns, pk.Namespace)
	require.Equal(t, KeyTypeReverse, pk.KeyType)
	require.Equal(t, attrID, pk.AttrID)
	require.Equal(t, toUID, pk.Uid1)
	require.Equal(t, fromUID, pk.Uid2)
}

func TestEncodeDecodeIndexKey(t *testing.T) {
	shardID := uint16(88)
	ns := uint32(0)
	attrID := uint32(400)
	token := []byte("alice_status_active")
	uid := uint64(7777)

	k := EncodeIndexKey(shardID, ns, attrID, token, uid)

	pk, err := DecodeKey(k)
	require.NoError(t, err)
	require.Equal(t, shardID, pk.ShardID)
	require.Equal(t, ns, pk.Namespace)
	require.Equal(t, KeyTypeIndex, pk.KeyType)
	require.Equal(t, attrID, pk.AttrID)
	require.Equal(t, token, pk.Token)
	require.Equal(t, uid, pk.Uid1)
}

func TestEncodeDecodeSchemaKey(t *testing.T) {
	ns := uint32(10)
	attrID := uint32(50)

	k := EncodeSchemaKey(ns, attrID)
	require.Equal(t, 11, len(k))

	pk, err := DecodeKey(k)
	require.NoError(t, err)
	require.Equal(t, uint16(0), pk.ShardID)
	require.Equal(t, ns, pk.Namespace)
	require.Equal(t, KeyTypeSchema, pk.KeyType)
	require.Equal(t, attrID, pk.AttrID)
}

func TestEdgePrefixOrdering(t *testing.T) {
	shardID := uint16(0)
	ns := uint32(0)
	attrID := uint32(1)
	userA := uint64(100)
	userB := uint64(101)

	// userA 的出边：指向 10, 20, 30
	e1 := EncodeEdgeKey(shardID, ns, attrID, userA, 10)
	e2 := EncodeEdgeKey(shardID, ns, attrID, userA, 20)
	e3 := EncodeEdgeKey(shardID, ns, attrID, userA, 30)

	// userB 的出边：指向 5
	e4 := EncodeEdgeKey(shardID, ns, attrID, userB, 5)

	keys := [][]byte{e3, e1, e4, e2}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// 排序后应该是 e1, e2, e3, e4
	require.True(t, bytes.Equal(e1, keys[0]))
	require.True(t, bytes.Equal(e2, keys[1]))
	require.True(t, bytes.Equal(e3, keys[2]))
	require.True(t, bytes.Equal(e4, keys[3]))

	// 验证 EdgePrefix 能精准匹配 userA 的边，而不匹配 userB
	prefixA := EdgePrefix(shardID, ns, attrID, userA)
	require.True(t, bytes.HasPrefix(e1, prefixA))
	require.True(t, bytes.HasPrefix(e2, prefixA))
	require.True(t, bytes.HasPrefix(e3, prefixA))
	require.False(t, bytes.HasPrefix(e4, prefixA))
}

func TestCalculateShardID(t *testing.T) {
	// 单分片模式恒返回 0
	require.Equal(t, uint16(0), CalculateShardID(12345, 0))
	require.Equal(t, uint16(0), CalculateShardID(12345, 1))

	// 多分片模式能够均匀哈希到 [0, N)
	numShards := uint16(16)
	buckets := make(map[uint16]int)
	for uid := uint64(1); uid <= 10000; uid++ {
		sid := CalculateShardID(uid, numShards)
		require.Less(t, sid, numShards)
		buckets[sid]++
	}
	// 确保每个桶都分到了数据
	for s := uint16(0); s < numShards; s++ {
		require.Greater(t, buckets[s], 100, "Shard %d should have received entries", s)
	}
}
