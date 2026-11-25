package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"

	"github.com/wisdomatom/stateless-graph/algo"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/tok/hnsw"
	"github.com/wisdomatom/stateless-graph/tok/index"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

// VectorMatch holds a matched UID along with its computed similarity score and distance.
type VectorMatch struct {
	UID      uint64
	Score    float64
	Distance float64
}

// decodeStoredVal converts a stored types.Val into a Go value suitable for GraphQL.
func decodeStoredVal(rawVal []byte, targetType types.TypeID) (any, bool) {
	if len(rawVal) <= 1 {
		return nil, false
	}
	if targetType == types.PasswordID {
		return nil, false
	}
	dst, err := types.Convert(types.Val{
		Tid:   types.TypeID(rawVal[0]),
		Value: rawVal[1:],
	}, targetType)
	if err != nil {
		return nil, false
	}
	switch v := dst.Value.(type) {
	case geom.T:
		b, merr := geojson.Marshal(v)
		if merr != nil {
			return nil, false
		}
		var m map[string]any
		if uerr := json.Unmarshal(b, &m); uerr != nil {
			return nil, false
		}
		return m, true
	case []float32:
		out := make([]float64, len(v))
		for i, f := range v {
			out[i] = float64(f)
		}
		return out, true
	default:
		return v, true
	}
}

// BatchGetNodeListFields reads scalar list attributes (p.List = true) for a set of UIDs.
func (s *Store) BatchGetNodeListFields(ctx context.Context, ns uint32, uids []uint64, rawAttrs []string) ([]map[string][]any, error) {
	rows := make([]map[string][]any, len(uids))
	for i := range rows {
		rows[i] = make(map[string][]any, len(rawAttrs))
	}
	if len(uids) == 0 || len(rawAttrs) == 0 {
		return rows, nil
	}

	attrIDs := make([]uint32, len(rawAttrs))
	attrTypes := make([]types.TypeID, len(rawAttrs))
	for j, ra := range rawAttrs {
		attrIDs[j] = flat.PredicateToAttrID(ra)
		fullAttr := x.NamespaceAttr(uint64(ns), ra)
		if su, ok := schema.State().Get(ctx, fullAttr); ok {
			attrTypes[j] = types.TypeID(su.ValueType)
		} else {
			attrTypes[j] = types.StringID
		}
	}

	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	for i, uid := range uids {
		sid := flat.CalculateShardID(uid, 0)
		for j, attrID := range attrIDs {
			valTypes, valBytesList, err := s.engine.ScanData(rTxn, sid, ns, attrID, uid)
			if err != nil || len(valBytesList) == 0 {
				continue
			}
			vals := make([]any, 0, len(valBytesList))
			for k, vb := range valBytesList {
				raw := make([]byte, 1+len(vb))
				raw[0] = valTypes[k]
				copy(raw[1:], vb)
				if decoded, ok := decodeStoredVal(raw, attrTypes[j]); ok {
					vals = append(vals, decoded)
				}
			}
			if len(vals) > 0 {
				rows[i][rawAttrs[j]] = vals
			}
		}
	}
	return rows, nil
}

// LookupIndexGeo executes geo queries (near, within, contains, intersects) directly on Flat Storage.
func (s *Store) LookupIndexGeo(ctx context.Context, ns uint32, rawAttr string, funcName string, args []string) ([]uint64, error) {
	tokens, qData, err := types.GetGeoTokens(&pb.SrcFunction{
		Name: funcName,
		Args: args,
	})
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, nil
	}
	tok.EncodeGeoTokens(tokens)

	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrID := flat.PredicateToAttrID(rawAttr)
	lists := make([]*pb.List, 0, len(tokens))
	for _, token := range tokens {
		uids, serr := s.engine.ScanIndex(rTxn, 0, ns, attrID, []byte(token))
		if serr != nil {
			return nil, serr
		}
		if len(uids) > 0 {
			lists = append(lists, &pb.List{Uids: uids})
		}
	}
	if len(lists) == 0 {
		return nil, nil
	}
	merged := algo.MergeSorted(lists)
	if merged == nil || len(merged.Uids) == 0 {
		return nil, nil
	}

	// Exact verification against stored geometry using types.MatchGeo.
	vals, present, err := s.batchGetSingleAttr(ctx, ns, attrID, merged.Uids, readTs)
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(merged.Uids))
	for i, uid := range merged.Uids {
		if !present[i] {
			continue
		}
		rawBytes, ok := vals[i].Value.([]byte)
		if !ok {
			continue
		}
		tv := pb.TaskValue{
			ValType: pb.Posting_ValType(vals[i].Tid),
			Val:     rawBytes,
		}
		if types.MatchGeo(&tv, qData) {
			out = append(out, uid)
		}
	}
	return out, nil
}

// LookupIndexVector executes HNSW vector similarity search directly on Flat Storage and computes
// exact similarity scores and distances for the returned candidates.
func (s *Store) LookupIndexVector(ctx context.Context, ns uint32, rawAttr string, queryVec []float32, topK int, ef int, distThreshold *float64, metric string) ([]VectorMatch, error) {
	if len(queryVec) == 0 || topK <= 0 {
		return nil, nil
	}
	fullAttr := x.NamespaceAttr(uint64(ns), rawAttr)
	specs, err := schema.State().FactoryCreateSpec(ctx, fullAttr)
	if err != nil || len(specs) == 0 {
		return nil, err
	}
	readTs := worker.State.GetTimestamp(true)
	viCache := worker.NewFlatViCache(readTs)
	qc := hnsw.NewQueryCache(viCache, readTs)

	indexer, err := specs[0].CreateIndex(fullAttr)
	if err != nil {
		return nil, err
	}

	var nnUids []uint64
	opts := index.VectorIndexOptions[float32]{
		Filter:            index.AcceptAll[float32],
		EfOverride:        ef,
		DistanceThreshold: distThreshold,
	}
	hasOpts := ef > 0 || distThreshold != nil
	if o, ok := indexer.(index.OptionalSearchOptions[float32]); ok && hasOpts {
		nnUids, err = o.SearchWithOptions(ctx, qc, queryVec, topK, opts)
	} else {
		nnUids, err = indexer.Search(ctx, qc, queryVec, topK, index.AcceptAll[float32])
	}
	if err != nil {
		if strings.Contains(err.Error(), hnsw.EmptyHNSWTreeError+": "+badger.ErrKeyNotFound.Error()) {
			return nil, nil
		}
		return nil, err
	}
	if len(nnUids) == 0 {
		return nil, nil
	}

	// Deduplicate and sort for BatchGet.
	sort.Slice(nnUids, func(i, j int) bool { return nnUids[i] < nnUids[j] })
	uniq := nnUids[:0]
	for _, u := range nnUids {
		if len(uniq) == 0 || uniq[len(uniq)-1] != u {
			uniq = append(uniq, u)
		}
	}

	attrID := flat.PredicateToAttrID(rawAttr)
	vals, present, err := s.batchGetSingleAttr(ctx, ns, attrID, uniq, readTs)
	if err != nil {
		return nil, err
	}

	matches := make([]VectorMatch, 0, len(uniq))
	mLower := strings.ToLower(metric)
	for i, uid := range uniq {
		if !present[i] {
			continue
		}
		vecBytes, ok := vals[i].Value.([]byte)
		if !ok {
			continue
		}
		candVec := types.BytesAsFloatArray(vecBytes)
		dist, score := computeVectorMetric(queryVec, candVec, mLower)
		matches = append(matches, VectorMatch{
			UID:      uid,
			Score:    score,
			Distance: dist,
		})
	}
	return matches, nil
}

func computeVectorMetric(a, b []float32, metric string) (dist float64, score float64) {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 0, 0
	}
	switch metric {
	case "euclidean", "l2":
		var sumSq float64
		for i := 0; i < n; i++ {
			d := float64(a[i]) - float64(b[i])
			sumSq += d * d
		}
		dist = math.Sqrt(sumSq)
		score = 1.0 / (1.0 + dist)
		return dist, score
	case "dotproduct", "dot":
		var dot float64
		for i := 0; i < n; i++ {
			dot += float64(a[i]) * float64(b[i])
		}
		return -dot, dot
	default: // cosine
		var dot, na, nb float64
		for i := 0; i < n; i++ {
			fa, fb := float64(a[i]), float64(b[i])
			dot += fa * fb
			na += fa * fa
			nb += fb * fb
		}
		if na == 0 || nb == 0 {
			return 1.0, 0.0
		}
		cosSim := dot / (math.Sqrt(na) * math.Sqrt(nb))
		return 1.0 - cosSim, cosSim
	}
}

// LookupIndexHas returns all sorted UIDs that have at least one value (or outgoing edge) for rawAttr.
func (s *Store) LookupIndexHas(ctx context.Context, ns uint32, rawAttr string, isEdge bool) ([]uint64, error) {
	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrID := flat.PredicateToAttrID(rawAttr)
	prefix := make([]byte, 11)
	binary.BigEndian.PutUint16(prefix[0:2], 0)
	binary.BigEndian.PutUint32(prefix[2:6], ns)
	if isEdge {
		prefix[6] = flat.KeyTypeEdge
	} else {
		prefix[6] = flat.KeyTypeData
	}
	binary.BigEndian.PutUint32(prefix[7:11], attrID)

	it := rTxn.NewIterator(x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: false,
	})
	defer it.Close()

	var uids []uint64
	var lastUID uint64
	for it.Seek(prefix); it.Valid(); it.Next() {
		k := it.Item().Key()
		if len(k) < 19 || !bytes.HasPrefix(k, prefix) {
			break
		}
		uid := binary.BigEndian.Uint64(k[11:19])
		if uid != lastUID {
			uids = append(uids, uid)
			lastUID = uid
		}
	}
	return uids, nil
}

// ScanEdgeSourcesMatchingTargets performs a single KeyOnly prefix scan over all edges of rawAttr
// and returns sorted unique source UIDs whose target UID is in targets.
func (s *Store) ScanEdgeSourcesMatchingTargets(ctx context.Context, ns uint32, rawAttr string, targets map[uint64]struct{}) ([]uint64, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrID := flat.PredicateToAttrID(rawAttr)
	prefix := make([]byte, 11)
	binary.BigEndian.PutUint16(prefix[0:2], 0)
	binary.BigEndian.PutUint32(prefix[2:6], ns)
	prefix[6] = flat.KeyTypeEdge
	binary.BigEndian.PutUint32(prefix[7:11], attrID)

	it := rTxn.NewIterator(x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: false,
	})
	defer it.Close()

	var uids []uint64
	var lastUID uint64
	for it.Seek(prefix); it.Valid(); it.Next() {
		k := it.Item().Key()
		if len(k) < 27 || !bytes.HasPrefix(k, prefix) {
			break
		}
		fromUID := binary.BigEndian.Uint64(k[11:19])
		toUID := binary.BigEndian.Uint64(k[19:27])
		if _, ok := targets[toUID]; ok && fromUID != lastUID {
			uids = append(uids, fromUID)
			lastUID = fromUID
		}
	}
	return uids, nil
}

// CheckPassword filters candidate UIDs to those whose stored password hash matches plainPwd.
func (s *Store) CheckPassword(ctx context.Context, ns uint32, rawAttr string, candidateUIDs []uint64, plainPwd string) ([]uint64, error) {
	if len(candidateUIDs) == 0 {
		return nil, nil
	}
	readTs := worker.State.GetTimestamp(true)
	attrID := flat.PredicateToAttrID(rawAttr)
	vals, present, err := s.batchGetSingleAttr(ctx, ns, attrID, candidateUIDs, readTs)
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0)
	for i, uid := range candidateUIDs {
		if !present[i] {
			continue
		}
		hashBytes, ok := vals[i].Value.([]byte)
		if !ok {
			continue
		}
		if err := types.VerifyPassword(plainPwd, string(hashBytes)); err == nil {
			out = append(out, uid)
		}
	}
	return out, nil
}

// LevenshteinDistance computes the edit distance between two rune slices.
func LevenshteinDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	la, lb := len(ra), len(rb)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	cur := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		cur[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := cur[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			cur[j] = m
		}
		prev, cur = cur, prev
	}
	return prev[lb]
}
