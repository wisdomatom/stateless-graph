package native

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/worker"
)

// EdgeResult holds a traversed target UID and optional decoded facets.
type EdgeResult struct {
	ToUID  uint64
	Facets []*api.Facet
}

// BatchScanOutEdges scans outgoing edges for multiple source UIDs concurrently.
// Uses KeyOnly mode automatically when needFacets is false.
func (s *Store) BatchScanOutEdges(ctx context.Context, ns uint32, rawAttr string, fromUIDs []uint64, needFacets bool) (map[uint64][]EdgeResult, error) {
	result := make(map[uint64][]EdgeResult, len(fromUIDs))
	if len(fromUIDs) == 0 {
		return result, nil
	}

	attrID := flat.PredicateToAttrID(rawAttr)
	if shared := TxnFromContext(ctx); shared != nil {
		for _, uid := range fromUIDs {
			shardID := flat.CalculateShardID(uid, 0)
			var it flat.EdgeIterator
			var err error
			if needFacets {
				it, err = s.engine.ScanOutEdgesFrom(shared.kvTxn, shardID, ns, attrID, uid, 0)
			} else {
				it, err = s.engine.ScanOutEdgesKeyOnly(shared.kvTxn, shardID, ns, attrID, uid, 0)
			}
			if err != nil {
				return nil, err
			}
			var edges []EdgeResult
			for it.Next() {
				entry := it.Entry()
				er := EdgeResult{ToUID: entry.ToUID}
				if needFacets && len(entry.Facets) > 0 {
					var f pb.Facets
					if uerr := proto.Unmarshal(entry.Facets, &f); uerr == nil {
						er.Facets = f.Facets
					}
				}
				edges = append(edges, er)
			}
			it.Close()
			if len(edges) > 0 {
				result[uid] = edges
			}
		}
		return result, nil
	}

	readTs := worker.State.GetTimestamp(true)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, 8)

	for _, uid := range fromUIDs {
		wg.Add(1)
		go func(fromUID uint64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			rTxn := s.db.NewTransactionAt(readTs, false)
			defer rTxn.Discard()

			shardID := flat.CalculateShardID(fromUID, 0)
			var it flat.EdgeIterator
			var err error
			if needFacets {
				it, err = s.engine.ScanOutEdgesFrom(rTxn, shardID, ns, attrID, fromUID, 0)
			} else {
				it, err = s.engine.ScanOutEdgesKeyOnly(rTxn, shardID, ns, attrID, fromUID, 0)
			}
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			defer it.Close()

			var edges []EdgeResult
			for it.Next() {
				entry := it.Entry()
				er := EdgeResult{ToUID: entry.ToUID}
				if needFacets && len(entry.Facets) > 0 {
					var f pb.Facets
					if uerr := proto.Unmarshal(entry.Facets, &f); uerr == nil {
						er.Facets = f.Facets
					}
				}
				edges = append(edges, er)
			}
			if it.Error() != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = it.Error()
				}
				errMu.Unlock()
				return
			}
			if len(edges) > 0 {
				mu.Lock()
				result[fromUID] = edges
				mu.Unlock()
			}
		}(uid)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return result, nil
}

// UpdateNodeFields updates scalar fields of an existing node, cleanly removing old secondary
// indexes and UniqueKeys before writing new values and indexes.
func (t *Txn) UpdateNodeFields(ctx context.Context, bctx *BatchWriteContext, uid uint64, fields []ScalarField, uniqueAttrs map[string]bool) error {
	shardID := flat.CalculateShardID(uid, 0)
	for _, f := range fields {
		meta, err := bctx.getMeta(ctx, f.Attr)
		if err != nil {
			return err
		}

		// Delete old index entries & UniqueKey for single-value fields.
		if !meta.isList && meta.isIndexed && len(meta.tokenizers) > 0 {
			if oldType, oldBytes, gerr := t.store.engine.GetData(t.kvTxn, shardID, t.ns, meta.attrID, uid); gerr == nil && len(oldBytes) > 0 {
				if oldSv, cerr := types.Convert(types.Val{Tid: types.TypeID(oldType), Value: oldBytes}, meta.schemaType); cerr == nil {
					for _, tk := range meta.tokenizers {
						if oldTokens, terr := tok.BuildTokens(oldSv.Value, tk); terr == nil {
							for _, token := range oldTokens {
								_ = t.store.engine.DeleteIndex(t.kvTxn, shardID, t.ns, meta.attrID, []byte(token), uid)
								if uniqueAttrs[f.Attr] && (tk.Identifier() == tok.IdentExact || tk.Identifier() == tok.IdentHash) {
									_ = t.kvTxn.Delete(flat.EncodeUniqueKey(0, t.ns, meta.attrID, []byte(token)))
								}
							}
						}
					}
				}
			}
		}

		if f.Value == nil {
			if err := t.store.engine.DeleteData(t.kvTxn, shardID, t.ns, meta.attrID, uid); err != nil {
				return err
			}
			continue
		}

		typedVal, err := toTypedVal(f.Value, meta.schemaType)
		if err != nil {
			return errors.Wrapf(err, "native: convert update value for %s", f.Attr)
		}
		binVal := types.Val{Tid: types.BinaryID}
		if err := types.Marshal(typedVal, &binVal); err != nil {
			return errors.Wrapf(err, "native: marshal update value for %s", f.Attr)
		}
		valBytes := binVal.Value.([]byte)

		if err := t.store.engine.SetData(t.kvTxn, shardID, t.ns, meta.attrID, uid, byte(meta.schemaType), valBytes); err != nil {
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
					if uniqueAttrs[f.Attr] && (tk.Identifier() == tok.IdentExact || tk.Identifier() == tok.IdentHash) {
						var uidBuf [8]byte
						binary.BigEndian.PutUint64(uidBuf[:], uid)
						if err := t.kvTxn.Set(flat.EncodeUniqueKey(0, t.ns, meta.attrID, []byte(token)), uidBuf[:]); err != nil {
							return err
						}
					}
				}
			}
		}
		if meta.isIndexed && meta.schemaType == types.VFloatID {
			if err := t.insertVectorHNSW(ctx, meta.fullAttr, uid, valBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteNode deletes all scalar attributes, indexes, UniqueKeys, and outgoing/reverse edges of a node.
func (t *Txn) DeleteNode(ctx context.Context, bctx *BatchWriteContext, uid uint64, scalarAttrs []string, edgeAttrs []string) error {
	shardID := flat.CalculateShardID(uid, 0)

	// Always clean dgraph.type list data & index.
	allScalars := append([]string{"dgraph.type"}, scalarAttrs...)
	for _, attr := range allScalars {
		meta, err := bctx.getMeta(ctx, attr)
		if err != nil {
			continue
		}
		if meta.isList {
			if meta.isIndexed && len(meta.tokenizers) > 0 {
				if valTypes, valBytesList, serr := t.store.engine.ScanData(t.kvTxn, shardID, t.ns, meta.attrID, uid); serr == nil {
					for i, vb := range valBytesList {
						if sv, cerr := types.Convert(types.Val{Tid: types.TypeID(valTypes[i]), Value: vb}, meta.schemaType); cerr == nil {
							for _, tk := range meta.tokenizers {
								if tokens, terr := tok.BuildTokens(sv.Value, tk); terr == nil {
									for _, token := range tokens {
										_ = t.store.engine.DeleteIndex(t.kvTxn, shardID, t.ns, meta.attrID, []byte(token), uid)
									}
								}
							}
						}
					}
				}
			}
			_ = t.store.engine.DeleteAllData(t.kvTxn, shardID, t.ns, meta.attrID, uid)
		} else {
			if meta.isIndexed && len(meta.tokenizers) > 0 {
				if vt, vb, gerr := t.store.engine.GetData(t.kvTxn, shardID, t.ns, meta.attrID, uid); gerr == nil && len(vb) > 0 {
					if sv, cerr := types.Convert(types.Val{Tid: types.TypeID(vt), Value: vb}, meta.schemaType); cerr == nil {
						for _, tk := range meta.tokenizers {
							if tokens, terr := tok.BuildTokens(sv.Value, tk); terr == nil {
								for _, token := range tokens {
									_ = t.store.engine.DeleteIndex(t.kvTxn, shardID, t.ns, meta.attrID, []byte(token), uid)
									if tk.Identifier() == tok.IdentExact || tk.Identifier() == tok.IdentHash {
										_ = t.kvTxn.Delete(flat.EncodeUniqueKey(0, t.ns, meta.attrID, []byte(token)))
									}
								}
							}
						}
					}
				}
			}
			_ = t.store.engine.DeleteData(t.kvTxn, shardID, t.ns, meta.attrID, uid)
		}
	}

	for _, attr := range edgeAttrs {
		meta, err := bctx.getMeta(ctx, attr)
		if err != nil {
			continue
		}
		if it, serr := t.store.engine.ScanOutEdgesKeyOnly(t.kvTxn, shardID, t.ns, meta.attrID, uid, 0); serr == nil {
			var toUIDs []uint64
			for it.Next() {
				toUIDs = append(toUIDs, it.Entry().ToUID)
			}
			it.Close()
			for _, toUID := range toUIDs {
				_ = t.store.engine.DeleteEdge(t.kvTxn, shardID, t.ns, meta.attrID, uid, toUID)
				if meta.isReversed {
					_ = t.store.engine.DeleteReverseEdge(t.kvTxn, flat.CalculateShardID(toUID, 0), t.ns, meta.attrID, toUID, uid)
				}
			}
		}
	}
	return nil
}
