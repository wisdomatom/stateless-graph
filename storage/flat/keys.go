package flat

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgryski/go-farm"
)

// PredicateToAttrID 将 Predicate 字符串快速且确定性地映射为 32 位 AttrID
func PredicateToAttrID(attr string) uint32 {
	return farm.Fingerprint32([]byte(attr))
}

// KeyType 定义物理存储中的键分类（1 字节）
const (
	KeyTypeSchema  byte = 0x00 // Schema 元数据
	KeyTypeData    byte = 0x01 // 标量属性数据 (Scalar Attribute)
	KeyTypeEdge    byte = 0x02 // 正向关系边 (Forward Edge)
	KeyTypeReverse byte = 0x03 // 反向关系边 (Reverse Edge)
	KeyTypeIndex   byte = 0x04 // 二级索引项 (Secondary Index)
)

const (
	// 固定长度常量
	HeaderLen  = 7  // ShardID(2B) + Namespace(4B) + KeyType(1B)
	DataKeyLen = 19 // Header(7B) + AttrID(4B) + SubjectUID(8B)
	EdgeKeyLen = 27 // Header(7B) + AttrID(4B) + SubjectUID(8B) + ObjectUID(8B)
)

var (
	ErrInvalidKeyLength = errors.New("flat: invalid key length")
	ErrInvalidKeyType   = errors.New("flat: invalid key type")
)

// ParsedKey 解析后的键结构
type ParsedKey struct {
	ShardID   uint16
	Namespace uint32
	KeyType   byte
	AttrID    uint32
	Uid1      uint64 // Data: SubjectUID; Edge: SubjectUID; Reverse: ObjectUID; Index: SubjectUID
	Uid2      uint64 // Edge: ObjectUID; Reverse: SubjectUID
	Token     []byte // Index: TokenTerm
}

// CalculateShardID 根据节点 UID 计算分片 ShardID
// 当 numShards <= 1 时（第一期单桶模式），恒返回 0
func CalculateShardID(uid uint64, numShards uint16) uint16 {
	if numShards <= 1 {
		return 0
	}
	// 简单快速的高性能 Hash (利用位混淆)
	x := uid
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return uint16(x % uint64(numShards))
}

// writeHeader 写入头部 ShardID(2B) + Namespace(4B) + KeyType(1B)
func writeHeader(buf []byte, shardID uint16, ns uint32, keyType byte) {
	binary.BigEndian.PutUint16(buf[0:2], shardID)
	binary.BigEndian.PutUint32(buf[2:6], ns)
	buf[6] = keyType
}

// EncodeDataKey 编码标量属性键 (19 字节)
// 格式: ShardID(2B) || Namespace(4B) || 0x01 || AttrID(4B) || SubjectUID(8B)
func EncodeDataKey(shardID uint16, ns uint32, attrID uint32, uid uint64) []byte {
	buf := make([]byte, DataKeyLen)
	writeHeader(buf, shardID, ns, KeyTypeData)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	binary.BigEndian.PutUint64(buf[11:19], uid)
	return buf
}

// EncodeEdgeKey 编码正向关系边键 (27 字节)
// 格式: ShardID(2B) || Namespace(4B) || 0x02 || AttrID(4B) || SubjectUID(8B) || ObjectUID(8B)
func EncodeEdgeKey(shardID uint16, ns uint32, attrID uint32, fromUID, toUID uint64) []byte {
	buf := make([]byte, EdgeKeyLen)
	writeHeader(buf, shardID, ns, KeyTypeEdge)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	binary.BigEndian.PutUint64(buf[11:19], fromUID)
	binary.BigEndian.PutUint64(buf[19:27], toUID)
	return buf
}

// EncodeReverseKey 编码反向关系边键 (27 字节)
// 格式: ShardID(2B) || Namespace(4B) || 0x03 || AttrID(4B) || ObjectUID(8B) || SubjectUID(8B)
func EncodeReverseKey(shardID uint16, ns uint32, attrID uint32, toUID, fromUID uint64) []byte {
	buf := make([]byte, EdgeKeyLen)
	writeHeader(buf, shardID, ns, KeyTypeReverse)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	binary.BigEndian.PutUint64(buf[11:19], toUID)
	binary.BigEndian.PutUint64(buf[19:27], fromUID)
	return buf
}

// EncodeIndexKey 编码二级索引键
// 格式: ShardID(2B) || Namespace(4B) || 0x04 || AttrID(4B) || TokenTerm(变长) || 0x00 || SubjectUID(8B)
func EncodeIndexKey(shardID uint16, ns uint32, attrID uint32, token []byte, uid uint64) []byte {
	tokenLen := len(token)
	buf := make([]byte, HeaderLen+4+tokenLen+1+8)
	writeHeader(buf, shardID, ns, KeyTypeIndex)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	copy(buf[11:11+tokenLen], token)
	buf[11+tokenLen] = 0x00
	binary.BigEndian.PutUint64(buf[11+tokenLen+1:], uid)
	return buf
}

// EncodeSchemaKey 编码全局 Schema 元数据键 (11 字节)
// Schema 默认存放在 ShardID = 0x0000 的公共保留空间
// 格式: 0x0000(2B) || Namespace(4B) || 0x00 || AttrID(4B)
func EncodeSchemaKey(ns uint32, attrID uint32) []byte {
	buf := make([]byte, 11)
	writeHeader(buf, 0, ns, KeyTypeSchema)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	return buf
}

// EdgePrefix 构造单节点出边扫描的前缀 (19 字节)
// 扫描该前缀即可顺序遍历 fromUID 的所有关系出边
func EdgePrefix(shardID uint16, ns uint32, attrID uint32, fromUID uint64) []byte {
	buf := make([]byte, DataKeyLen)
	writeHeader(buf, shardID, ns, KeyTypeEdge)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	binary.BigEndian.PutUint64(buf[11:19], fromUID)
	return buf
}

// ReversePrefix 构造单节点入边（反向边）扫描的前缀 (19 字节)
func ReversePrefix(shardID uint16, ns uint32, attrID uint32, toUID uint64) []byte {
	buf := make([]byte, DataKeyLen)
	writeHeader(buf, shardID, ns, KeyTypeReverse)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	binary.BigEndian.PutUint64(buf[11:19], toUID)
	return buf
}

// IndexPrefix 构造二级索引精确匹配扫描的前缀
func IndexPrefix(shardID uint16, ns uint32, attrID uint32, token []byte) []byte {
	tokenLen := len(token)
	buf := make([]byte, HeaderLen+4+tokenLen+1)
	writeHeader(buf, shardID, ns, KeyTypeIndex)
	binary.BigEndian.PutUint32(buf[7:11], attrID)
	copy(buf[11:11+tokenLen], token)
	buf[11+tokenLen] = 0x00
	return buf
}

// DecodeKey 解析任意扁平 Key
func DecodeKey(key []byte) (ParsedKey, error) {
	if len(key) < HeaderLen+4 {
		return ParsedKey{}, ErrInvalidKeyLength
	}

	pk := ParsedKey{
		ShardID:   binary.BigEndian.Uint16(key[0:2]),
		Namespace: binary.BigEndian.Uint32(key[2:6]),
		KeyType:   key[6],
		AttrID:    binary.BigEndian.Uint32(key[7:11]),
	}

	switch pk.KeyType {
	case KeyTypeSchema:
		return pk, nil

	case KeyTypeData:
		if len(key) != DataKeyLen {
			return pk, ErrInvalidKeyLength
		}
		pk.Uid1 = binary.BigEndian.Uint64(key[11:19])
		return pk, nil

	case KeyTypeEdge, KeyTypeReverse:
		if len(key) != EdgeKeyLen {
			return pk, ErrInvalidKeyLength
		}
		pk.Uid1 = binary.BigEndian.Uint64(key[11:19])
		pk.Uid2 = binary.BigEndian.Uint64(key[19:27])
		return pk, nil

	case KeyTypeIndex:
		// 最短长度: HeaderLen(7) + AttrID(4) + 0x00(1) + UID(8) = 20
		if len(key) < HeaderLen+4+1+8 {
			return pk, ErrInvalidKeyLength
		}
		rem := key[11:]
		// 寻找末尾 8 字节 UID 前的 0x00 分隔符
		splitIdx := bytes.LastIndexByte(rem[:len(rem)-8], 0x00)
		if splitIdx == -1 {
			return pk, fmt.Errorf("flat: missing null delimiter in index key")
		}
		pk.Token = rem[:splitIdx]
		pk.Uid1 = binary.BigEndian.Uint64(rem[len(rem)-8:])
		return pk, nil

	default:
		return pk, ErrInvalidKeyType
	}
}
