package worker

import (
	"context"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/wisdomatom/stateless-graph/posting"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/tok/hnsw"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/x"
)

var GlobalFlatEngine = flat.NewEngine()

func parseNamespaceSafe(attr string) (uint32, string) {
	if !strings.Contains(attr, x.NsSeparator) {
		return 0, attr
	}
	parts := strings.SplitN(attr, x.NsSeparator, 2)
	ns, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return 0, attr
	}
	return uint32(ns), parts[1]
}

// ApplyFlatMutation 将一条 DirectedEdge 按照扁平 KV 模式直接写入底层存储
func ApplyFlatMutation(ctx context.Context, edge *pb.DirectedEdge, txn x.KVTxn) error {
	if edge.Attr == "" {
		return errors.Errorf("Predicate cannot be empty")
	}

	ns, rawAttr := parseNamespaceSafe(edge.Attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	shardID := flat.CalculateShardID(edge.Entity, 0) // 第一期单桶模式

	// 1. 关系边处理 (Edge.ValueId != 0)
	if edge.ValueId != 0 {
		isList := schema.State().IsList(edge.Attr)
		if !isList {
			lockKey := flat.EncodeDataKey(shardID, ns, attrID, edge.Entity)
			if err := txn.LockKeys(ctx, lockKey); err != nil {
				return err
			}
		}

		var facetsBytes []byte
		if len(edge.Facets) > 0 {
			facets := &pb.Facets{Facets: edge.Facets}
			var err error
			facetsBytes, err = proto.Marshal(facets)
			if err != nil {
				return errors.Wrap(err, "marshal facets failed")
			}
		}

		if edge.Op == pb.DirectedEdge_DEL {
			// 删除正向边
			if err := GlobalFlatEngine.DeleteEdge(txn, shardID, ns, attrID, edge.Entity, edge.ValueId); err != nil {
				return err
			}
			// 删除反向边 (如果已开启)
			if schema.State().IsReversed(ctx, edge.Attr) {
				revShard := flat.CalculateShardID(edge.ValueId, 0)
				if err := GlobalFlatEngine.DeleteReverseEdge(txn, revShard, ns, attrID, edge.ValueId, edge.Entity); err != nil {
					return err
				}
			}
		} else {
			if !isList {
				// 单值关系写入：清理同一 Entity 下可能存在的旧正向出边
				if it, err := GlobalFlatEngine.ScanOutEdges(txn, shardID, ns, attrID, edge.Entity); err == nil {
					for it.Next() {
						oldToUID := it.Entry().ToUID
						if oldToUID != edge.ValueId {
							_ = GlobalFlatEngine.DeleteEdge(txn, shardID, ns, attrID, edge.Entity, oldToUID)
							if schema.State().IsReversed(ctx, edge.Attr) {
								revShard := flat.CalculateShardID(oldToUID, 0)
								_ = GlobalFlatEngine.DeleteReverseEdge(txn, revShard, ns, attrID, oldToUID, edge.Entity)
							}
						}
					}
					it.Close()
				}
			}

			// 添加/覆盖正向边
			if err := GlobalFlatEngine.AddEdge(txn, shardID, ns, attrID, edge.Entity, edge.ValueId, facetsBytes); err != nil {
				return err
			}
			// 添加/覆盖反向边 (如果已开启)
			if schema.State().IsReversed(ctx, edge.Attr) {
				revShard := flat.CalculateShardID(edge.ValueId, 0)
				if err := GlobalFlatEngine.AddReverseEdge(txn, revShard, ns, attrID, edge.ValueId, edge.Entity, facetsBytes); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// 2. 标量属性处理 (Edge.ValueId == 0)
	isScalarList := schema.State().IsList(edge.Attr)
	if !isScalarList {
		lockKey := flat.EncodeDataKey(shardID, ns, attrID, edge.Entity)
		if err := txn.LockKeys(ctx, lockKey); err != nil {
			return err
		}
	}
	if edge.Op == pb.DirectedEdge_DEL {
		if isScalarList {
			if isStarAll(edge.Value) || len(edge.Value) == 0 {
				// 删除整个列表及索引
				if schema.State().IsIndexed(ctx, edge.Attr) {
					if _, valBytesList, err := GlobalFlatEngine.ScanData(txn, shardID, ns, attrID, edge.Entity); err == nil {
						if schemaType, err := schema.State().TypeOf(edge.Attr); err == nil && schemaType.IsScalar() {
							for _, oldValBytes := range valBytesList {
								if sv, err := types.Convert(types.Val{Tid: types.BinaryID, Value: oldValBytes}, schemaType); err == nil {
									for _, t := range schema.State().Tokenizer(ctx, edge.Attr) {
										if tokens, err := tok.BuildTokens(sv.Value, tok.GetTokenizerForLang(t, edge.Lang)); err == nil {
											for _, token := range tokens {
												_ = GlobalFlatEngine.DeleteIndex(txn, shardID, ns, attrID, []byte(token), edge.Entity)
											}
										}
									}
								}
							}
						}
					}
				}
				if err := GlobalFlatEngine.DeleteAllData(txn, shardID, ns, attrID, edge.Entity); err != nil {
					return err
				}
			} else {
				// 删除列表中的单个项
				itemHash := farm.Fingerprint64(edge.Value)
				if schema.State().IsIndexed(ctx, edge.Attr) {
					if schemaType, err := schema.State().TypeOf(edge.Attr); err == nil && schemaType.IsScalar() {
						if sv, err := types.Convert(types.Val{Tid: types.BinaryID, Value: edge.Value}, schemaType); err == nil {
							for _, t := range schema.State().Tokenizer(ctx, edge.Attr) {
								if tokens, err := tok.BuildTokens(sv.Value, tok.GetTokenizerForLang(t, edge.Lang)); err == nil {
									for _, token := range tokens {
										_ = GlobalFlatEngine.DeleteIndex(txn, shardID, ns, attrID, []byte(token), edge.Entity)
									}
								}
							}
						}
					}
				}
				if err := GlobalFlatEngine.DeleteListData(txn, shardID, ns, attrID, edge.Entity, itemHash); err != nil {
					return err
				}
			}
		} else {
			// 单值删除
			if schema.State().IsIndexed(ctx, edge.Attr) {
				_, oldValBytes, err := GlobalFlatEngine.GetData(txn, shardID, ns, attrID, edge.Entity)
				if err == nil && len(oldValBytes) > 0 {
					if schemaType, err := schema.State().TypeOf(edge.Attr); err == nil && schemaType.IsScalar() {
						if sv, err := types.Convert(types.Val{Tid: types.BinaryID, Value: oldValBytes}, schemaType); err == nil {
							for _, t := range schema.State().Tokenizer(ctx, edge.Attr) {
								if tokens, err := tok.BuildTokens(sv.Value, tok.GetTokenizerForLang(t, edge.Lang)); err == nil {
									for _, token := range tokens {
										_ = GlobalFlatEngine.DeleteIndex(txn, shardID, ns, attrID, []byte(token), edge.Entity)
									}
								}
							}
						}
					}
				}
			}
			if err := GlobalFlatEngine.DeleteData(txn, shardID, ns, attrID, edge.Entity); err != nil {
				return err
			}
		}
	} else {
		// 写入
		if isScalarList {
			itemHash := farm.Fingerprint64(edge.Value)
			if err := GlobalFlatEngine.AddListData(txn, shardID, ns, attrID, edge.Entity, itemHash, byte(edge.ValueType), edge.Value); err != nil {
				return err
			}
			if schema.State().IsIndexed(ctx, edge.Attr) {
				if schemaType, err := schema.State().TypeOf(edge.Attr); err == nil && schemaType.IsScalar() {
					if sv, err := types.Convert(types.Val{Tid: types.BinaryID, Value: edge.Value}, schemaType); err == nil {
						for _, t := range schema.State().Tokenizer(ctx, edge.Attr) {
							if tokens, err := tok.BuildTokens(sv.Value, tok.GetTokenizerForLang(t, edge.Lang)); err == nil {
								for _, token := range tokens {
									if err := GlobalFlatEngine.AddIndex(txn, shardID, ns, attrID, []byte(token), edge.Entity); err != nil {
										return err
									}
								}
							}
						}
					}
				}
			}
		} else {
			// 单值写入 (先清旧索引，再加新索引，再写入单值)
			if schema.State().IsIndexed(ctx, edge.Attr) {
				schemaType, err := schema.State().TypeOf(edge.Attr)
				if err == nil && schemaType.IsScalar() {
					if _, oldValBytes, err := GlobalFlatEngine.GetData(txn, shardID, ns, attrID, edge.Entity); err == nil && len(oldValBytes) > 0 {
						if oldSv, err := types.Convert(types.Val{Tid: types.BinaryID, Value: oldValBytes}, schemaType); err == nil {
							for _, t := range schema.State().Tokenizer(ctx, edge.Attr) {
								if tokens, err := tok.BuildTokens(oldSv.Value, tok.GetTokenizerForLang(t, edge.Lang)); err == nil {
									for _, token := range tokens {
										_ = GlobalFlatEngine.DeleteIndex(txn, shardID, ns, attrID, []byte(token), edge.Entity)
									}
								}
							}
						}
					}
					if sv, err := types.Convert(types.Val{Tid: types.BinaryID, Value: edge.Value}, schemaType); err == nil {
						for _, t := range schema.State().Tokenizer(ctx, edge.Attr) {
							if tokens, err := tok.BuildTokens(sv.Value, tok.GetTokenizerForLang(t, edge.Lang)); err == nil {
								for _, token := range tokens {
									if err := GlobalFlatEngine.AddIndex(txn, shardID, ns, attrID, []byte(token), edge.Entity); err != nil {
										return err
									}
								}
							}
						}
					}
				}
			}
			if err := GlobalFlatEngine.SetData(txn, shardID, ns, attrID, edge.Entity, byte(edge.ValueType), edge.Value); err != nil {
				return err
			}
			if schema.State().IsIndexed(ctx, edge.Attr) {
				if schemaType, err := schema.State().TypeOf(edge.Attr); err == nil && schemaType == types.VFloatID {
					if specs, err := schema.State().FactoryCreateSpec(ctx, edge.Attr); err == nil && len(specs) > 0 {
						inVec := types.BytesAsFloatArray(edge.Value)
						if indexer, err := specs[0].CreateIndex(edge.Attr); err == nil {
							tc := hnsw.NewTxnCache(newFlatViTxn(txn, 0), 0)
							if edges, err := indexer.Insert(ctx, tc, edge.Entity, inVec); err == nil {
								for _, e := range edges {
									ens, erawAttr := parseNamespaceSafe(e.Attr)
									eattrID := flat.PredicateToAttrID(erawAttr)
									eshardID := flat.CalculateShardID(e.Entity, 0)
									_ = GlobalFlatEngine.SetData(txn, eshardID, ens, eattrID, e.Entity, 0, e.Value)
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// FlatScanEdges 用于扫描出边，替换原有的 posting.List 读取
func FlatScanEdges(ctx context.Context, txn x.KVTxn, attr string, fromUID uint64, opts posting.ListOptions) (*pb.List, []*pb.Facets, error) {
	ns, rawAttr := parseNamespaceSafe(attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	shardID := flat.CalculateShardID(fromUID, 0)

	if opts.Intersect != nil && len(opts.Intersect.Uids) == 0 {
		return &pb.List{}, nil, nil
	}

	it, err := GlobalFlatEngine.ScanOutEdges(txn, shardID, ns, attrID, fromUID)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()

	var uids []uint64
	var fcsList []*pb.Facets

	skipped := 0
	for it.Next() {
		entry := it.Entry()
		toUID := entry.ToUID

		// 1. AfterUid 截断
		if opts.AfterUid > 0 && toUID <= opts.AfterUid {
			continue
		}

		// 2. 内存求交 (如果上游带有 Intersect 过滤列表)
		if opts.Intersect != nil {
			if !containsUID(opts.Intersect.Uids, toUID) {
				continue
			}
		}

		// 3. 收集 Facets (如果有)
		var edgeFacets *pb.Facets
		if len(entry.Facets) > 0 {
			var f pb.Facets
			if err := proto.Unmarshal(entry.Facets, &f); err == nil {
				edgeFacets = &f
			}
		}
		if edgeFacets == nil {
			edgeFacets = &pb.Facets{}
		}

		uids = append(uids, toUID)
		fcsList = append(fcsList, edgeFacets)

		// 4. Limit/First 截断 (如果有)
		if opts.First > 0 && len(uids) >= opts.First {
			break
		}
	}

	if err := it.Error(); err != nil {
		return nil, nil, err
	}

	_ = skipped
	return &pb.List{Uids: uids}, fcsList, nil
}

// FlatScanReverseEdges 用于扫描反向边
func FlatScanReverseEdges(ctx context.Context, txn x.KVTxn, attr string, toUID uint64, opts posting.ListOptions) (*pb.List, error) {
	ns, rawAttr := parseNamespaceSafe(attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	shardID := flat.CalculateShardID(toUID, 0)

	if opts.Intersect != nil && len(opts.Intersect.Uids) == 0 {
		return &pb.List{}, nil
	}

	it, err := GlobalFlatEngine.ScanInEdges(txn, shardID, ns, attrID, toUID)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var uids []uint64
	for it.Next() {
		entry := it.Entry()
		fromUID := entry.ToUID // 在 ReverseKey 中，Uid2 是 fromUID

		if opts.AfterUid > 0 && fromUID <= opts.AfterUid {
			continue
		}
		if opts.Intersect != nil {
			if !containsUID(opts.Intersect.Uids, fromUID) {
				continue
			}
		}

		uids = append(uids, fromUID)
		if opts.First > 0 && len(uids) >= opts.First {
			break
		}
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	return &pb.List{Uids: uids}, nil
}

func containsUID(uids []uint64, target uint64) bool {
	// 二分查找 (假设 uids 升序)
	low, high := 0, len(uids)-1
	for low <= high {
		mid := (low + high) / 2
		if uids[mid] == target {
			return true
		} else if uids[mid] < target {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return false
}
