package native

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/wisdomatom/stateless-graph/algo"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

// ScanTypeUIDs scans UIDs belonging to `typeName` via dgraph.type exact index (KeyOnly).
func (s *Store) ScanTypeUIDs(ctx context.Context, ns uint32, typeName string, afterUID uint64, limit int) ([]uint64, error) {
	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	tk, _ := tok.GetTokenizer("exact")
	tokens, err := tok.BuildTokens(typeName, tk)
	if err != nil || len(tokens) == 0 {
		return nil, err
	}
	attrID := flat.PredicateToAttrID("dgraph.type")
	return s.engine.ScanIndexWithContext(ctx, rTxn, 0, ns, attrID, []byte(tokens[0]), afterUID, limit)
}

// CountTypeUIDs counts all UIDs of `typeName` using a single KeyOnly index scan without allocating a full slice.
func (s *Store) CountTypeUIDs(ctx context.Context, ns uint32, typeName string) (int, error) {
	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	tk, _ := tok.GetTokenizer("exact")
	tokens, err := tok.BuildTokens(typeName, tk)
	if err != nil || len(tokens) == 0 {
		return 0, err
	}
	attrID := flat.PredicateToAttrID("dgraph.type")
	prefix := flat.IndexPrefix(0, ns, attrID, []byte(tokens[0]))
	it := rTxn.NewIterator(x.KVIterOpts{
		Prefix:         prefix,
		PrefetchValues: false,
		PrefetchSize:   5000,
		KeyOnly:        true,
	})
	defer it.Close()

	count := 0
	for it.Seek(prefix); it.Valid(); it.Next() {
		if count&1023 == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if !bytes.HasPrefix(it.Item().Key(), prefix) {
			break
		}
		count++
	}
	return count, nil
}

// LookupIndexEq finds UIDs matching `rawAttr == value` (supports exact and hash indexes).
func (s *Store) LookupIndexEq(ctx context.Context, ns uint32, rawAttr string, value any, afterUID uint64, limit int) ([]uint64, error) {
	fullAttr := x.NamespaceAttr(uint64(ns), rawAttr)
	su, ok := schema.State().Get(ctx, fullAttr)
	if !ok {
		return nil, errors.Errorf("native: schema not found for %s", rawAttr)
	}
	schemaType := types.TypeID(su.ValueType)

	var eqTok tok.Tokenizer
	for _, tname := range su.Tokenizer {
		tk, has := tok.GetTokenizer(tname)
		if !has {
			continue
		}
		if tname == "exact" || !tk.IsLossy() {
			eqTok = tk
			break
		}
		if tname == "hash" && eqTok == nil {
			eqTok = tk
		} else if eqTok == nil && tk.Identifier() != tok.IdentTrigram {
			eqTok = tk
		}
	}
	if eqTok == nil {
		return nil, errors.Errorf("native: no equality tokenizer for %s", rawAttr)
	}

	typedVal, err := toTypedVal(value, schemaType)
	if err != nil {
		return nil, err
	}
	tokens, err := tok.BuildTokens(typedVal.Value, eqTok)
	if err != nil || len(tokens) == 0 {
		return nil, err
	}

	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	attrID := flat.PredicateToAttrID(rawAttr)
	scanLimit := limit
	if eqTok.IsLossy() {
		scanLimit = 0 // Must filter false positives before applying limit
	}
	uids, err := s.engine.ScanIndexWithContext(ctx, rTxn, 0, ns, attrID, []byte(tokens[0]), afterUID, scanLimit)
	if err != nil || len(uids) == 0 || !eqTok.IsLossy() {
		return uids, err
	}

	// Lossy tokenizer (e.g. hash): verify via BatchGet.
	vals, present, err := s.batchGetSingleAttr(ctx, ns, attrID, uids, 0)
	if err != nil {
		return nil, err
	}
	filtered := make([]uint64, 0, len(uids))
	for i, uid := range uids {
		if !present[i] {
			continue
		}
		dst, cerr := types.Convert(vals[i], schemaType)
		if cerr == nil && types.CompareVals("eq", dst, typedVal) {
			filtered = append(filtered, uid)
			if limit > 0 && len(filtered) >= limit {
				break
			}
		}
	}
	return filtered, nil
}

// LookupIndexIn finds UIDs matching `rawAttr IN values` concurrently.
func (s *Store) LookupIndexIn(ctx context.Context, ns uint32, rawAttr string, values []any) ([]uint64, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) == 1 {
		return s.LookupIndexEq(ctx, ns, rawAttr, values[0], 0, 0)
	}
	lists := make([]*pb.List, len(values))
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, 8)
	for i, v := range values {
		wg.Add(1)
		go func(idx int, val any) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			uids, err := s.LookupIndexEq(ctx, ns, rawAttr, val, 0, 0)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			lists[idx] = &pb.List{Uids: uids}
		}(i, v)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	merged := algo.MergeSorted(lists)
	return merged.Uids, nil
}

// LookupIndexTokens executes fulltext/term token queries (alloftext, anyoftext, allofterms, anyofterms)
// directly against the Flat Storage index using KeyOnly concurrent scans.
func (s *Store) LookupIndexTokens(ctx context.Context, ns uint32, rawAttr string, tokName string, text string, matchAll bool) ([]uint64, error) {
	tk, has := tok.GetTokenizer(tokName)
	if !has {
		return nil, errors.Errorf("native: tokenizer %s not found", tokName)
	}
	tokens, err := tok.BuildTokens(text, tk)
	if err != nil || len(tokens) == 0 {
		return nil, err
	}

	readTs := worker.State.GetTimestamp(true)
	attrID := flat.PredicateToAttrID(rawAttr)
	lists := make([]*pb.List, len(tokens))

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, 8)
	for i, token := range tokens {
		wg.Add(1)
		go func(idx int, t string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rTxn := s.db.NewTransactionAt(readTs, false)
			defer rTxn.Discard()
			uids, serr := s.engine.ScanIndexWithContext(ctx, rTxn, 0, ns, attrID, []byte(t), 0, 0)
			if serr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = serr
				}
				errMu.Unlock()
				return
			}
			lists[idx] = &pb.List{Uids: uids}
		}(i, token)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	if matchAll {
		res := lists[0]
		for i := 1; i < len(lists); i++ {
			algo.IntersectWith(res, lists[i], res)
			if len(res.Uids) == 0 {
				return nil, nil
			}
		}
		return res.Uids, nil
	}
	return algo.MergeSorted(lists).Uids, nil
}

// LookupIndexRange executes range queries (gt, ge, lt, le, between) on any sortable index
// with **Boundary-Only Verification Pruning**:
// For lossy sortable indexes (like datetime `hour`/`day`/`month`/`year`), interior buckets
// strictly between startToken and endToken are 100% guaranteed to satisfy the predicate and
// SKIP BatchGet verification completely; ONLY boundary-bucket UIDs are verified via BatchGet!
func (s *Store) LookupIndexRange(ctx context.Context, ns uint32, rawAttr string, op string, val1, val2 any) ([]uint64, error) {
	fullAttr := x.NamespaceAttr(uint64(ns), rawAttr)
	su, ok := schema.State().Get(ctx, fullAttr)
	if !ok {
		return nil, errors.Errorf("native: schema not found for %s", rawAttr)
	}
	schemaType := types.TypeID(su.ValueType)

	var sortTok tok.Tokenizer
	for _, tname := range su.Tokenizer {
		tk, has := tok.GetTokenizer(tname)
		if has && tk.IsSortable() {
			sortTok = tk
			break
		}
	}
	if sortTok == nil {
		return nil, errors.Errorf("native: no sortable index tokenizer for %s", rawAttr)
	}

	tv1, err := toTypedVal(val1, schemaType)
	if err != nil {
		return nil, err
	}
	toks1, err := tok.BuildTokens(tv1.Value, sortTok)
	if err != nil || len(toks1) == 0 {
		return nil, err
	}
	tok1 := []byte(toks1[0])

	var tv2 types.Val
	var tok2 []byte
	if op == "between" {
		tv2, err = toTypedVal(val2, schemaType)
		if err != nil {
			return nil, err
		}
		toks2, err := tok.BuildTokens(tv2.Value, sortTok)
		if err != nil || len(toks2) == 0 {
			return nil, err
		}
		tok2 = []byte(toks2[0])
	}

	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	attrID := flat.PredicateToAttrID(rawAttr)
	basePrefix := flat.AttrIndexPrefix(0, ns, attrID)
	tokPrefix := make([]byte, len(basePrefix)+1)
	copy(tokPrefix, basePrefix)
	tokPrefix[len(basePrefix)] = sortTok.Identifier()

	var seekKey []byte
	switch op {
	case "gt", "ge", "between":
		seekKey = flat.IndexPrefix(0, ns, attrID, tok1)
	case "lt", "le":
		seekKey = tokPrefix
	default:
		return nil, errors.Errorf("native: unsupported range op %s", op)
	}

	it := rTxn.NewIterator(x.KVIterOpts{
		Prefix:         tokPrefix,
		PrefetchValues: false,
		PrefetchSize:   5000,
		KeyOnly:        true,
	})
	defer it.Close()

	isLossy := sortTok.IsLossy()
	var guaranteedUIDs []uint64
	var boundaryUIDs []uint64

	steps := 0
	for it.Seek(seekKey); it.Valid(); it.Next() {
		steps++
		if steps&1023 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		key := it.Item().Key()
		if !bytes.HasPrefix(key, tokPrefix) || len(key) < len(basePrefix)+9 {
			break
		}
		// EncodeIndexKey layout: AttrIndexPrefix || Token || 0x00 || UID(8B)
		tokenBytes := key[len(basePrefix) : len(key)-9]
		uid := binary.BigEndian.Uint64(key[len(key)-8:])

		switch op {
		case "gt":
			cmp := bytes.Compare(tokenBytes, tok1)
			if cmp < 0 {
				continue
			}
			if cmp == 0 {
				if isLossy {
					boundaryUIDs = append(boundaryUIDs, uid)
				}
				continue
			}
			guaranteedUIDs = append(guaranteedUIDs, uid)

		case "ge":
			cmp := bytes.Compare(tokenBytes, tok1)
			if cmp < 0 {
				continue
			}
			if cmp == 0 && isLossy {
				boundaryUIDs = append(boundaryUIDs, uid)
				continue
			}
			guaranteedUIDs = append(guaranteedUIDs, uid)

		case "lt":
			cmp := bytes.Compare(tokenBytes, tok1)
			if cmp > 0 {
				goto doneScan
			}
			if cmp == 0 {
				if isLossy {
					boundaryUIDs = append(boundaryUIDs, uid)
					continue
				}
				goto doneScan
			}
			guaranteedUIDs = append(guaranteedUIDs, uid)

		case "le":
			cmp := bytes.Compare(tokenBytes, tok1)
			if cmp > 0 {
				goto doneScan
			}
			if cmp == 0 && isLossy {
				boundaryUIDs = append(boundaryUIDs, uid)
				continue
			}
			guaranteedUIDs = append(guaranteedUIDs, uid)

		case "between":
			cmpHi := bytes.Compare(tokenBytes, tok2)
			if cmpHi > 0 {
				goto doneScan
			}
			cmpLo := bytes.Compare(tokenBytes, tok1)
			if cmpLo < 0 {
				continue
			}
			if isLossy && (cmpLo == 0 || cmpHi == 0) {
				boundaryUIDs = append(boundaryUIDs, uid)
				continue
			}
			guaranteedUIDs = append(guaranteedUIDs, uid)
		}
	}
doneScan:

	// Verify ONLY boundary-bucket UIDs via BatchGet!
	if len(boundaryUIDs) > 0 {
		vals, present, err := s.batchGetSingleAttr(ctx, ns, attrID, boundaryUIDs, 0)
		if err != nil {
			return nil, err
		}
		for i, uid := range boundaryUIDs {
			if !present[i] {
				continue
			}
			dst, cerr := types.Convert(vals[i], schemaType)
			if cerr != nil {
				continue
			}
			matched := false
			if op == "between" {
				matched = types.CompareBetween(dst, tv1, tv2)
			} else {
				matched = types.CompareVals(op, dst, tv1)
			}
			if matched {
				guaranteedUIDs = append(guaranteedUIDs, uid)
			}
		}
	}

	sort.Slice(guaranteedUIDs, func(i, j int) bool { return guaranteedUIDs[i] < guaranteedUIDs[j] })
	// Deduplicate in-place.
	if len(guaranteedUIDs) > 1 {
		out := guaranteedUIDs[:1]
		for _, u := range guaranteedUIDs[1:] {
			if u != out[len(out)-1] {
				out = append(out, u)
			}
		}
		guaranteedUIDs = out
	}
	return guaranteedUIDs, nil
}

// ScanExtremum returns the min (isMax=false) or max (isMax=true) value of a sortable indexed predicate in O(1).
func (s *Store) ScanExtremum(ctx context.Context, ns uint32, rawAttr string, isMax bool) (any, bool, error) {
	fullAttr := x.NamespaceAttr(uint64(ns), rawAttr)
	su, ok := schema.State().Get(ctx, fullAttr)
	if !ok {
		return nil, false, errors.Errorf("native: schema not found for %s", rawAttr)
	}
	schemaType := types.TypeID(su.ValueType)

	var sortTok tok.Tokenizer
	for _, tname := range su.Tokenizer {
		tk, has := tok.GetTokenizer(tname)
		if has && tk.IsSortable() {
			sortTok = tk
			break
		}
	}
	if sortTok == nil {
		return nil, false, errors.Errorf("native: no sortable tokenizer for extremum on %s", rawAttr)
	}

	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	attrID := flat.PredicateToAttrID(rawAttr)
	_, uid, found, err := s.engine.ScanIndexExtremum(rTxn, 0, ns, attrID, sortTok.Identifier(), isMax)
	if err != nil || !found {
		return nil, false, err
	}
	// If lossy (e.g. hour), fall back; for exact/int/float/bool/exact string, read the actual value of uid.
	if sortTok.IsLossy() {
		return nil, false, errors.Errorf("native: extremum on lossy tokenizer %s requires full scan", sortTok.Name())
	}
	valType, valBytes, err := s.engine.GetData(rTxn, flat.CalculateShardID(uid, 0), ns, attrID, uid)
	if err != nil || len(valBytes) == 0 {
		return nil, false, err
	}
	dst, err := types.Convert(types.Val{Tid: types.TypeID(valType), Value: valBytes}, schemaType)
	if err != nil {
		return nil, false, err
	}
	return dst.Value, true, nil
}

// BatchGetNodesFields fetches multiple single-value scalar fields for a list of UIDs
// in a SINGLE combined TiKV BatchGet RPC (e.g., 100 rows x 5 fields = 500 keys in 1 RPC)
// and decodes them directly into native Go values (zero JSON round-trip).
func (s *Store) BatchGetNodesFields(ctx context.Context, ns uint32, uids []uint64, rawAttrs []string) ([]map[string]any, error) {
	numNodes := len(uids)
	numAttrs := len(rawAttrs)
	rows := make([]map[string]any, numNodes)
	for i := range rows {
		rows[i] = make(map[string]any, numAttrs+1)
	}
	if numNodes == 0 || numAttrs == 0 {
		return rows, nil
	}

	attrIDs := make([]uint32, numAttrs)
	attrTypes := make([]types.TypeID, numAttrs)
	for j, ra := range rawAttrs {
		attrIDs[j] = flat.PredicateToAttrID(ra)
		fullAttr := x.NamespaceAttr(uint64(ns), ra)
		if su, ok := schema.State().Get(ctx, fullAttr); ok {
			attrTypes[j] = types.TypeID(su.ValueType)
		} else {
			attrTypes[j] = types.StringID
		}
	}

	totalKeys := numNodes * numAttrs
	keys := make([][]byte, totalKeys)
	for i, uid := range uids {
		sid := flat.CalculateShardID(uid, 0)
		base := i * numAttrs
		for j := 0; j < numAttrs; j++ {
			keys[base+j] = flat.EncodeDataKey(sid, ns, attrIDs[j], uid)
		}
	}

	rTxn, cleanup := s.readKVTxn(ctx)
	defer cleanup()

	// Chunk into batches of 2000 keys if very large.
	const batchSize = 2000
	for start := 0; start < totalKeys; start += batchSize {
		end := start + batchSize
		if end > totalKeys {
			end = totalKeys
		}
		resMap, err := rTxn.BatchGet(keys[start:end])
		if err != nil {
			return nil, err
		}
		for kIdx := start; kIdx < end; kIdx++ {
			rawVal, ok := resMap[string(keys[kIdx])]
			if !ok || len(rawVal) <= 1 {
				continue
			}
			nodeIdx := kIdx / numAttrs
			attrIdx := kIdx % numAttrs
			if decoded, ok := decodeStoredVal(rawVal, attrTypes[attrIdx]); ok {
				rows[nodeIdx][rawAttrs[attrIdx]] = decoded
			}
		}
	}
	return rows, nil
}

func (s *Store) batchGetSingleAttr(ctx context.Context, ns uint32, attrID uint32, uids []uint64, readTs uint64) ([]types.Val, []bool, error) {
	n := len(uids)
	vals := make([]types.Val, n)
	present := make([]bool, n)
	if n == 0 {
		return vals, present, nil
	}
	const batchSize = 2000
	var rTxn x.KVTxn
	var cleanup func()
	if readTs == 0 {
		rTxn, cleanup = s.readKVTxn(ctx)
	} else {
		rTxn = s.db.NewTransactionAt(readTs, false)
		cleanup = func() { rTxn.Discard() }
	}
	defer cleanup()

	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		keys := make([][]byte, end-start)
		for i, uid := range uids[start:end] {
			keys[i] = flat.EncodeDataKey(flat.CalculateShardID(uid, 0), ns, attrID, uid)
		}
		resMap, err := rTxn.BatchGet(keys)
		if err != nil {
			return nil, nil, err
		}
		for i := range uids[start:end] {
			rawVal, ok := resMap[string(keys[i])]
			if !ok || len(rawVal) <= 1 {
				continue
			}
			vals[start+i] = types.Val{
				Tid:   types.TypeID(rawVal[0]),
				Value: rawVal[1:],
			}
			present[start+i] = true
		}
	}
	return vals, present, nil
}
