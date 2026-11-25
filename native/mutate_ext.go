package native

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgryski/go-farm"
	"google.golang.org/protobuf/proto"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/tok/hnsw"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/types/facets"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

// EncodeFacetsMap encodes a map of facet key -> Go value into serialized pb.Facets bytes.
func EncodeFacetsMap(fMap map[string]any) ([]byte, error) {
	if len(fMap) == 0 {
		return nil, nil
	}
	fl := &pb.Facets{
		Facets: make([]*api.Facet, 0, len(fMap)),
	}
	for k, v := range fMap {
		if v == nil {
			continue
		}
		var rawStr string
		switch tv := v.(type) {
		case string:
			rawStr = fmt.Sprintf("%q", tv)
		case time.Time:
			rawStr = tv.UTC().Format(time.RFC3339)
		case int:
			rawStr = fmt.Sprintf("%d", tv)
		case int64:
			rawStr = fmt.Sprintf("%d", tv)
		case float64:
			if tv == float64(int64(tv)) {
				rawStr = fmt.Sprintf("%.1f", tv)
			} else {
				rawStr = fmt.Sprintf("%g", tv)
			}
		case bool:
			if tv {
				rawStr = "true"
			} else {
				rawStr = "false"
			}
		default:
			rawStr = fmt.Sprintf("%q", fmt.Sprintf("%v", tv))
		}
		f, err := facets.FacetFor(k, rawStr)
		if err != nil {
			// Fallback to string facet
			f = &api.Facet{
				Key:     k,
				Value:   []byte(fmt.Sprintf("%v", v)),
				ValType: api.Facet_STRING,
			}
		}
		fl.Facets = append(fl.Facets, f)
	}
	if len(fl.Facets) == 0 {
		return nil, nil
	}
	return proto.Marshal(fl)
}

// DeleteEdge deletes a directed edge (and its reverse edge if configured in schema).
func (t *Txn) DeleteEdge(ctx context.Context, bctx *BatchWriteContext, fromUID, toUID uint64, rawAttr string) error {
	meta, err := bctx.getMeta(ctx, rawAttr)
	if err != nil {
		return err
	}
	shardID := flat.CalculateShardID(fromUID, 0)
	if err := t.store.engine.DeleteEdge(t.kvTxn, shardID, t.ns, meta.attrID, fromUID, toUID); err != nil {
		return err
	}
	if meta.isReversed {
		revShard := flat.CalculateShardID(toUID, 0)
		if err := t.store.engine.DeleteReverseEdge(t.kvTxn, revShard, t.ns, meta.attrID, toUID, fromUID); err != nil {
			return err
		}
	}
	return nil
}

// DeleteAllOutEdges deletes all outgoing edges of rawAttr from fromUID and returns the removed target UIDs.
func (t *Txn) DeleteAllOutEdges(ctx context.Context, bctx *BatchWriteContext, fromUID uint64, rawAttr string) ([]uint64, error) {
	meta, err := bctx.getMeta(ctx, rawAttr)
	if err != nil {
		return nil, err
	}
	shardID := flat.CalculateShardID(fromUID, 0)
	it, err := t.store.engine.ScanOutEdgesKeyOnly(t.kvTxn, shardID, t.ns, meta.attrID, fromUID, 0)
	if err != nil {
		return nil, err
	}
	var toUIDs []uint64
	for it.Next() {
		toUIDs = append(toUIDs, it.Entry().ToUID)
	}
	it.Close()
	for _, toUID := range toUIDs {
		if err := t.store.engine.DeleteEdge(t.kvTxn, shardID, t.ns, meta.attrID, fromUID, toUID); err != nil {
			return nil, err
		}
		if meta.isReversed {
			_ = t.store.engine.DeleteReverseEdge(t.kvTxn, flat.CalculateShardID(toUID, 0), t.ns, meta.attrID, toUID, fromUID)
		}
	}
	return toUIDs, nil
}

// UpdateListField applies Set, Append, and/or Remove operations to a scalar list predicate.
func (t *Txn) UpdateListField(ctx context.Context, bctx *BatchWriteContext, uid uint64, rawAttr string, hasSet bool, setVals, appendVals, removeVals []any) error {
	meta, err := bctx.getMeta(ctx, rawAttr)
	if err != nil {
		return err
	}
	shardID := flat.CalculateShardID(uid, 0)

	deleteItemIndexes := func(valType byte, valBytes []byte) {
		if !meta.isIndexed || len(meta.tokenizers) == 0 {
			return
		}
		if sv, cerr := types.Convert(types.Val{Tid: types.TypeID(valType), Value: valBytes}, meta.schemaType); cerr == nil {
			for _, tk := range meta.tokenizers {
				if tokens, terr := tok.BuildTokens(sv.Value, tk); terr == nil {
					for _, token := range tokens {
						_ = t.store.engine.DeleteIndex(t.kvTxn, shardID, t.ns, meta.attrID, []byte(token), uid)
					}
				}
			}
		}
	}

	addItem := func(v any) error {
		if v == nil {
			return nil
		}
		typedVal, err := toTypedVal(v, meta.schemaType)
		if err != nil {
			return err
		}
		binVal := types.Val{Tid: types.BinaryID}
		if err := types.Marshal(typedVal, &binVal); err != nil {
			return err
		}
		valBytes := binVal.Value.([]byte)
		itemHash := farm.Fingerprint64(valBytes)
		if err := t.store.engine.AddListData(t.kvTxn, shardID, t.ns, meta.attrID, uid, itemHash, byte(meta.schemaType), valBytes); err != nil {
			return err
		}
		if meta.isIndexed && len(meta.tokenizers) > 0 {
			for _, tk := range meta.tokenizers {
				tokens, terr := tok.BuildTokens(typedVal.Value, tk)
				if terr != nil {
					return terr
				}
				for _, token := range tokens {
					if err := t.store.engine.AddIndex(t.kvTxn, shardID, t.ns, meta.attrID, []byte(token), uid); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	if hasSet {
		// Remove all existing items + their indexes first.
		if valTypes, valBytesList, serr := t.store.engine.ScanData(t.kvTxn, shardID, t.ns, meta.attrID, uid); serr == nil {
			for i, vb := range valBytesList {
				deleteItemIndexes(valTypes[i], vb)
			}
		}
		if err := t.store.engine.DeleteAllData(t.kvTxn, shardID, t.ns, meta.attrID, uid); err != nil {
			return err
		}
		for _, v := range setVals {
			if err := addItem(v); err != nil {
				return err
			}
		}
	}

	for _, v := range removeVals {
		if v == nil {
			continue
		}
		typedVal, err := toTypedVal(v, meta.schemaType)
		if err != nil {
			continue
		}
		binVal := types.Val{Tid: types.BinaryID}
		if err := types.Marshal(typedVal, &binVal); err != nil {
			continue
		}
		valBytes := binVal.Value.([]byte)
		deleteItemIndexes(byte(meta.schemaType), valBytes)
		itemHash := farm.Fingerprint64(valBytes)
		_ = t.store.engine.DeleteListData(t.kvTxn, shardID, t.ns, meta.attrID, uid, itemHash)
	}

	for _, v := range appendVals {
		if err := addItem(v); err != nil {
			return err
		}
	}
	return nil
}

// insertVectorHNSW updates the HNSW vector index inside the current Flat Storage transaction.
func (t *Txn) insertVectorHNSW(ctx context.Context, fullAttr string, uid uint64, valBytes []byte) error {
	specs, err := schema.State().FactoryCreateSpec(ctx, fullAttr)
	if err != nil || len(specs) == 0 {
		return nil
	}
	inVec := types.BytesAsFloatArray(valBytes)
	indexer, err := specs[0].CreateIndex(fullAttr)
	if err != nil {
		return err
	}
	tc := hnsw.NewTxnCache(worker.NewFlatViTxn(t.kvTxn, 0), 0)
	edges, err := indexer.Insert(ctx, tc, uid, inVec)
	if err != nil {
		return err
	}
	for _, e := range edges {
		ns64, rawAttr := x.ParseNamespaceAttr(e.Attr)
		eAttrID := flat.PredicateToAttrID(rawAttr)
		eShardID := flat.CalculateShardID(e.Entity, 0)
		_ = t.store.engine.SetData(t.kvTxn, eShardID, uint32(ns64), eAttrID, e.Entity, 0, e.Value)
	}
	return nil
}

// CheckAndReserveCompositeUnique checks and reserves a composite unique key (schema + uniqueName + values)
// inside the current transaction. Returns (existingUID, nil) if conflict with another UID, or (0, nil) if ok.
func (t *Txn) CheckAndReserveCompositeUnique(uniqueKeyName string, compositeToken string, uid uint64, allowSelfUID uint64) (uint64, error) {
	attrID := flat.PredicateToAttrID("__uniq." + uniqueKeyName)
	key := flat.EncodeUniqueKey(0, t.ns, attrID, []byte(compositeToken))
	resMap, err := t.kvTxn.BatchGet([][]byte{key})
	if err != nil {
		return 0, err
	}
	if val, found := resMap[string(key)]; found && len(val) == 8 {
		existingUID := binary.BigEndian.Uint64(val)
		if existingUID != 0 && existingUID != allowSelfUID {
			return existingUID, nil
		}
	}
	var uidBuf [8]byte
	binary.BigEndian.PutUint64(uidBuf[:], uid)
	if err := t.kvTxn.Set(key, uidBuf[:]); err != nil {
		return 0, err
	}
	return 0, nil
}

// DeleteCompositeUnique removes a composite unique key reservation.
func (t *Txn) DeleteCompositeUnique(uniqueKeyName string, compositeToken string) error {
	attrID := flat.PredicateToAttrID("__uniq." + uniqueKeyName)
	key := flat.EncodeUniqueKey(0, t.ns, attrID, []byte(compositeToken))
	return t.kvTxn.Delete(key)
}

// LookupUniqueUID looks up the UID reserved for a single-field unique predicate value.
func (t *Txn) LookupUniqueUID(ctx context.Context, bctx *BatchWriteContext, rawAttr string, value any) (uint64, bool, error) {
	meta, err := bctx.getMeta(ctx, rawAttr)
	if err != nil {
		return 0, false, err
	}
	var eqTok tok.Tokenizer
	for _, tk := range meta.tokenizers {
		if tk.Name() == "exact" {
			eqTok = tk
			break
		}
		if tk.Name() == "hash" && eqTok == nil {
			eqTok = tk
		} else if eqTok == nil && tk.Identifier() != tok.IdentTrigram {
			eqTok = tk
		}
	}
	if eqTok == nil {
		return 0, false, nil
	}
	typedVal, err := toTypedVal(value, meta.schemaType)
	if err != nil {
		return 0, false, err
	}
	toks, err := tok.BuildTokens(typedVal.Value, eqTok)
	if err != nil || len(toks) == 0 {
		return 0, false, err
	}
	key := flat.EncodeUniqueKey(0, t.ns, meta.attrID, []byte(toks[0]))
	resMap, err := t.kvTxn.BatchGet([][]byte{key})
	if err != nil {
		return 0, false, err
	}
	if val, found := resMap[string(key)]; found && len(val) == 8 {
		return binary.BigEndian.Uint64(val), true, nil
	}
	// Fallback to secondary index scan in case data was written prior to UniqueKey.
	uids, err := t.store.engine.ScanIndexWithContext(ctx, t.kvTxn, 0, t.ns, meta.attrID, []byte(toks[0]), 0, 1)
	if err != nil || len(uids) == 0 {
		return 0, false, err
	}
	return uids[0], true, nil
}
