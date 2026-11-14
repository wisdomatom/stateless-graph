package flat

import (
	"bytes"

	"github.com/wisdomatom/stateless-graph/x"
)

// EdgeEntry 代表一条图边记录
type EdgeEntry struct {
	FromUID uint64
	ToUID   uint64
	Facets  []byte
}

// EdgeIterator 用于流式遍历边的迭代器
type EdgeIterator interface {
	Next() bool
	Entry() EdgeEntry
	Close()
	Error() error
}

type edgeIterator struct {
	it     x.KVIterator
	prefix []byte
	cur    EdgeEntry
	err    error
	valid  bool
}

func (ei *edgeIterator) Next() bool {
	if !ei.it.Valid() {
		ei.valid = false
		return false
	}

	item := ei.it.Item()
	key := item.Key()
	if !bytes.HasPrefix(key, ei.prefix) {
		ei.valid = false
		return false
	}

	pk, err := DecodeKey(key)
	if err != nil {
		ei.err = err
		ei.valid = false
		return false
	}

	val, err := item.ValueCopy(nil)
	if err != nil {
		ei.err = err
		ei.valid = false
		return false
	}

	ei.cur = EdgeEntry{
		FromUID: pk.Uid1,
		ToUID:   pk.Uid2,
		Facets:  val,
	}
	ei.valid = true

	ei.it.Next()
	return true
}

func (ei *edgeIterator) Entry() EdgeEntry {
	return ei.cur
}

func (ei *edgeIterator) Close() {
	ei.it.Close()
}

func (ei *edgeIterator) Error() error {
	return ei.err
}

// Engine 扁平图存储引擎接口
type Engine interface {
	// 标量属性操作
	SetData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64, valType byte, data []byte) error
	GetData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) (byte, []byte, error)
	DeleteData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) error
	AddListData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64, itemHash uint64, valType byte, data []byte) error
	DeleteListData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64, itemHash uint64) error
	DeleteAllData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) error
	ScanData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) ([]byte, [][]byte, error)

	// 关系出边操作
	AddEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID, toUID uint64, facets []byte) error
	DeleteEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID, toUID uint64) error
	ScanOutEdges(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID uint64) (EdgeIterator, error)
	ScanOutEdgesFrom(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID, afterUID uint64) (EdgeIterator, error)

	// 反向入边操作
	AddReverseEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID, fromUID uint64, facets []byte) error
	DeleteReverseEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID, fromUID uint64) error
	ScanInEdges(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID uint64) (EdgeIterator, error)
	ScanInEdgesFrom(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID, afterUID uint64) (EdgeIterator, error)

	// 二级索引操作
	AddIndex(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, token []byte, uid uint64) error
	DeleteIndex(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, token []byte, uid uint64) error
	ScanIndex(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, token []byte) ([]uint64, error)
	ScanIndexExtremum(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, isMax bool) ([]byte, uint64, bool, error)
}

// DefaultEngine 默认扁平引擎实现
type DefaultEngine struct{}

func NewEngine() Engine {
	return &DefaultEngine{}
}

func (e *DefaultEngine) SetData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64, valType byte, data []byte) error {
	k := EncodeDataKey(shardID, ns, attrID, uid)
	val := make([]byte, 1+len(data))
	val[0] = valType
	copy(val[1:], data)
	return txn.Set(k, val)
}

func (e *DefaultEngine) GetData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) (byte, []byte, error) {
	k := EncodeDataKey(shardID, ns, attrID, uid)
	item, err := txn.Get(k)
	if err != nil {
		return 0, nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return 0, nil, err
	}
	if len(val) == 0 {
		return 0, nil, nil
	}
	return val[0], val[1:], nil
}

func (e *DefaultEngine) DeleteData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) error {
	k := EncodeDataKey(shardID, ns, attrID, uid)
	return txn.Delete(k)
}

func (e *DefaultEngine) AddListData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64, itemHash uint64, valType byte, data []byte) error {
	k := EncodeListDataKey(shardID, ns, attrID, uid, itemHash)
	val := make([]byte, 1+len(data))
	val[0] = valType
	copy(val[1:], data)
	return txn.Set(k, val)
}

func (e *DefaultEngine) DeleteListData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64, itemHash uint64) error {
	k := EncodeListDataKey(shardID, ns, attrID, uid, itemHash)
	return txn.Delete(k)
}

func (e *DefaultEngine) DeleteAllData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) error {
	prefix := EncodeDataKey(shardID, ns, attrID, uid)
	it := txn.NewIterator(x.KVIterOpts{Prefix: prefix})
	defer it.Close()
	var keysToDelete [][]byte
	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Item().Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
	}
	for _, k := range keysToDelete {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func (e *DefaultEngine) ScanData(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, uid uint64) ([]byte, [][]byte, error) {
	prefix := EncodeDataKey(shardID, ns, attrID, uid)
	it := txn.NewIterator(x.KVIterOpts{Prefix: prefix, PrefetchValues: true, PrefetchSize: 10})
	defer it.Close()
	var valTypes []byte
	var valBytesList [][]byte
	for it.Seek(prefix); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		val, err := item.ValueCopy(nil)
		if err != nil || len(val) == 0 {
			continue
		}
		valTypes = append(valTypes, val[0])
		valBytesList = append(valBytesList, val[1:])
	}
	return valTypes, valBytesList, nil
}

func (e *DefaultEngine) AddEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID, toUID uint64, facets []byte) error {
	k := EncodeEdgeKey(shardID, ns, attrID, fromUID, toUID)
	return txn.Set(k, facets)
}

func (e *DefaultEngine) DeleteEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID, toUID uint64) error {
	k := EncodeEdgeKey(shardID, ns, attrID, fromUID, toUID)
	return txn.Delete(k)
}

func (e *DefaultEngine) ScanOutEdges(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID uint64) (EdgeIterator, error) {
	return e.ScanOutEdgesFrom(txn, shardID, ns, attrID, fromUID, 0)
}

func (e *DefaultEngine) ScanOutEdgesFrom(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, fromUID, afterUID uint64) (EdgeIterator, error) {
	prefix := EdgePrefix(shardID, ns, attrID, fromUID)
	opt := x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: true,
		PrefetchSize:   100,
	}

	it := txn.NewIterator(opt)
	if afterUID > 0 {
		seekKey := EncodeEdgeKey(shardID, ns, attrID, fromUID, afterUID+1)
		it.Seek(seekKey)
	} else {
		it.Seek(prefix)
	}

	return &edgeIterator{
		it:     it,
		prefix: prefix,
	}, nil
}

func (e *DefaultEngine) AddReverseEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID, fromUID uint64, facets []byte) error {
	k := EncodeReverseKey(shardID, ns, attrID, toUID, fromUID)
	return txn.Set(k, facets)
}

func (e *DefaultEngine) DeleteReverseEdge(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID, fromUID uint64) error {
	k := EncodeReverseKey(shardID, ns, attrID, toUID, fromUID)
	return txn.Delete(k)
}

func (e *DefaultEngine) ScanInEdges(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID uint64) (EdgeIterator, error) {
	return e.ScanInEdgesFrom(txn, shardID, ns, attrID, toUID, 0)
}

func (e *DefaultEngine) ScanInEdgesFrom(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, toUID, afterUID uint64) (EdgeIterator, error) {
	prefix := ReversePrefix(shardID, ns, attrID, toUID)
	opt := x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: true,
		PrefetchSize:   100,
	}

	it := txn.NewIterator(opt)
	if afterUID > 0 {
		seekKey := EncodeReverseKey(shardID, ns, attrID, toUID, afterUID+1)
		it.Seek(seekKey)
	} else {
		it.Seek(prefix)
	}

	return &edgeIterator{
		it:     it,
		prefix: prefix,
	}, nil
}

func (e *DefaultEngine) AddIndex(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, token []byte, uid uint64) error {
	k := EncodeIndexKey(shardID, ns, attrID, token, uid)
	return txn.Set(k, nil)
}

func (e *DefaultEngine) DeleteIndex(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, token []byte, uid uint64) error {
	k := EncodeIndexKey(shardID, ns, attrID, token, uid)
	return txn.Delete(k)
}

func (e *DefaultEngine) ScanIndex(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, token []byte) ([]uint64, error) {
	prefix := IndexPrefix(shardID, ns, attrID, token)
	opt := x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: false, // 索引只需读 Key 中的末尾 8 字节 UID，无需拉取 Value
		PrefetchSize:   100,
	}

	it := txn.NewIterator(opt)
	defer it.Close()

	var uids []uint64
	for it.Seek(prefix); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		pk, err := DecodeKey(key)
		if err != nil {
			return nil, err
		}
		uids = append(uids, pk.Uid1)
	}
	return uids, nil
}

// ScanIndexExtremum 快速获取属性索引的极值（最小值或最大值），耗时为 O(1) 的单次 Seek
// isMax 为 false 时 Seek 索引最前端获取最小值，为 true 时反向 Seek 获取最大值
func (e *DefaultEngine) ScanIndexExtremum(txn x.KVTxn, shardID uint16, ns uint32, attrID uint32, isMax bool) ([]byte, uint64, bool, error) {
	prefix := AttrIndexPrefix(shardID, ns, attrID)
	opt := x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: false,
		PrefetchSize:   1,
		IsReverse:      isMax,
	}

	it := txn.NewIterator(opt)
	defer it.Close()

	if isMax {
		seekKey := PrefixNext(prefix)
		if seekKey != nil {
			it.Seek(seekKey)
		} else {
			it.Seek(prefix)
		}
	} else {
		it.Seek(prefix)
	}

	if it.Valid() {
		k := it.Item().Key()
		if bytes.HasPrefix(k, prefix) {
			pk, err := DecodeKey(k)
			if err != nil {
				return nil, 0, false, err
			}
			return pk.Token, pk.Uid1, true, nil
		}
	}
	return nil, 0, false, nil
}
