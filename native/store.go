package native

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pkg/errors"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

// Store provides a direct, zero-DQL, zero-gRPC native storage execution layer
// on top of Flat Storage and TiKV/Badger transactions.
type Store struct {
	db     x.KVDB
	engine flat.Engine
}

// DefaultStore returns a Store backed by the active embedded storage engine.
func DefaultStore() *Store {
	if worker.State.Pstore == nil {
		return nil
	}
	return &Store{
		db:     worker.State.Pstore,
		engine: worker.GlobalFlatEngine,
	}
}

// AllocateUids allocates `n` unique UIDs directly from the double-buffered in-memory leaser.
func AllocateUids(n uint64) (uint64, uint64, error) {
	return x.GetNextUidsWithErr(n)
}

// Txn represents a native distributed transaction directly backed by TiKV/Badger.
type Txn struct {
	store     *Store
	kvTxn     x.KVTxn
	ns        uint32
	isWrite   bool
	committed bool
	discarded bool
}

// Begin starts a new native read or read-write transaction.
func (s *Store) Begin(ns uint32, update bool) *Txn {
	kvTxn := s.db.NewTransactionAt(math.MaxUint64, update)
	return &Txn{
		store:   s,
		kvTxn:   kvTxn,
		ns:      ns,
		isWrite: update,
	}
}

// Commit commits all buffered writes in a single batched TiKV 1PC / AsyncCommit round-trip.
func (t *Txn) Commit(ctx context.Context) error {
	if t.committed || t.discarded {
		return nil
	}
	t.committed = true
	return t.kvTxn.CommitAt(math.MaxUint64, nil)
}

// Discard aborts and releases the transaction if not yet committed.
func (t *Txn) Discard() {
	if t.committed || t.discarded {
		return
	}
	t.discarded = true
	t.kvTxn.Discard()
}

// ScalarField represents a single scalar attribute value to write directly to Flat Storage.
type ScalarField struct {
	Attr  string // Predicate name without namespace prefix, e.g. "MetaUser.name" or "dgraph.type"
	Value any    // Native Go value: string, int64, int, float64, bool, time.Time
}

type cachedSchemaMeta struct {
	attrID     uint32
	fullAttr   string
	isList     bool
	isIndexed  bool
	isReversed bool
	schemaType types.TypeID
	tokenizers []tok.Tokenizer
}

// BatchWriteContext caches predicate schema lookups across a whole batch of nodes
// so 100 nodes x 5 fields only takes 5 schema lookups instead of 2,500.
type BatchWriteContext struct {
	ns    uint32
	cache map[string]*cachedSchemaMeta
}

func NewBatchWriteContext(ns uint32) *BatchWriteContext {
	return &BatchWriteContext{
		ns:    ns,
		cache: make(map[string]*cachedSchemaMeta, 16),
	}
}

func (b *BatchWriteContext) getMeta(ctx context.Context, rawAttr string) (*cachedSchemaMeta, error) {
	if m, ok := b.cache[rawAttr]; ok {
		return m, nil
	}
	fullAttr := x.NamespaceAttr(uint64(b.ns), rawAttr)
	su, ok := schema.State().Get(ctx, fullAttr)
	if !ok {
		return nil, errors.Errorf("native: schema not found for predicate %s", rawAttr)
	}
	m := &cachedSchemaMeta{
		attrID:     flat.PredicateToAttrID(rawAttr),
		fullAttr:   fullAttr,
		isList:     su.List,
		isIndexed:  len(su.Tokenizer) > 0 || len(su.IndexSpecs) > 0,
		isReversed: su.Directive == pb.SchemaUpdate_REVERSE,
		schemaType: types.TypeID(su.ValueType),
	}
	if m.isIndexed && len(su.Tokenizer) > 0 {
		m.tokenizers = make([]tok.Tokenizer, 0, len(su.Tokenizer))
		for _, tname := range su.Tokenizer {
			if tk, has := tok.GetTokenizer(tname); has {
				m.tokenizers = append(m.tokenizers, tk)
			}
		}
	}
	b.cache[rawAttr] = m
	return m, nil
}

// BatchCheckIndexExists checks in parallel whether any of the provided values already
// exist in the index for `rawAttr`. Used for zero-DQL unique constraint validation.
func (s *Store) BatchCheckIndexExists(ctx context.Context, ns uint32, rawAttr string, values []any) ([]bool, error) {
	n := len(values)
	exists := make([]bool, n)
	if n == 0 {
		return exists, nil
	}

	fullAttr := x.NamespaceAttr(uint64(ns), rawAttr)
	su, ok := schema.State().Get(ctx, fullAttr)
	if !ok {
		return nil, errors.Errorf("native: schema not found for unique predicate %s", rawAttr)
	}
	schemaType := types.TypeID(su.ValueType)

	// Pick the best equality tokenizer (exact > hash > first non-trigram).
	var chosenTok tok.Tokenizer
	for _, tname := range su.Tokenizer {
		tk, has := tok.GetTokenizer(tname)
		if !has {
			continue
		}
		if tname == "exact" {
			chosenTok = tk
			break
		}
		if tname == "hash" && chosenTok == nil {
			chosenTok = tk
		} else if chosenTok == nil && tk.Identifier() != tok.IdentTrigram {
			chosenTok = tk
		}
	}
	if chosenTok == nil {
		return nil, errors.Errorf("native: no equality index tokenizer found for predicate %s", rawAttr)
	}

	attrID := flat.PredicateToAttrID(rawAttr)
	encodedTokens := make([][]byte, n)
	for i, v := range values {
		typedVal, err := toTypedVal(v, schemaType)
		if err != nil {
			return nil, err
		}
		toks, err := tok.BuildTokens(typedVal.Value, chosenTok)
		if err != nil {
			return nil, err
		}
		if len(toks) > 0 {
			encodedTokens[i] = []byte(toks[0])
		}
	}

	const maxWorkers = 8
	workers := n
	if workers > maxWorkers {
		workers = maxWorkers
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Share a single read timestamp across all concurrent workers.
	readTs := worker.State.GetTimestamp(true)
	jobs := make(chan int, n)
	for i := 0; i < n; i++ {
		if len(encodedTokens[i]) > 0 {
			jobs <- i
		}
	}
	close(jobs)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rTxn := s.db.NewTransactionAt(readTs, false)
			defer rTxn.Discard()
			for idx := range jobs {
				if subCtx.Err() != nil {
					return
				}
				// KeyOnly + limit=1: stops at the very first matching UID!
				uids, err := s.engine.ScanIndexWithContext(subCtx, rTxn, 0, ns, attrID, encodedTokens[idx], 0, 1)
				if err != nil {
					errMu.Lock()
					if firstErr == nil && !errors.Is(err, context.Canceled) {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
					return
				}
				if len(uids) > 0 {
					exists[idx] = true
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return exists, nil
}

// BatchCheckAndReserveUnique checks uniqueness for a batch of values in a SINGLE
// TiKV BatchGet RPC (replacing N separate Scan RPCs) and writes UniqueKey -> UID
// entries into the current transaction so TiKV's row-level transaction locks
// automatically prevent concurrent duplicate writes.
// Returns (duplicateIndex, nil) where duplicateIndex >= 0 if a duplicate exists, or (-1, nil) if all unique.
func (t *Txn) BatchCheckAndReserveUnique(ctx context.Context, bctx *BatchWriteContext, rawAttr string, values []any, uids []uint64) (int, error) {
	n := len(values)
	if n == 0 {
		return -1, nil
	}
	meta, err := bctx.getMeta(ctx, rawAttr)
	if err != nil {
		return -1, err
	}

	// Pick deterministic equality tokenizer (exact > hash > first non-trigram).
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
		return -1, errors.Errorf("native: no equality index tokenizer found for unique predicate %s", rawAttr)
	}

	keys := make([][]byte, n)
	for i, v := range values {
		typedVal, cerr := toTypedVal(v, meta.schemaType)
		if cerr != nil {
			return -1, cerr
		}
		toks, terr := tok.BuildTokens(typedVal.Value, eqTok)
		if terr != nil {
			return -1, terr
		}
		if len(toks) > 0 {
			keys[i] = flat.EncodeUniqueKey(0, t.ns, meta.attrID, []byte(toks[0]))
		}
	}

	// 1 single BatchGet RPC for all N unique values!
	existing, err := t.kvTxn.BatchGet(keys)
	if err != nil {
		return -1, err
	}
	for i, k := range keys {
		if len(k) == 0 {
			continue
		}
		if val, found := existing[string(k)]; found && len(val) > 0 {
			return i, nil
		}
	}

	// Reserve all unique keys inside this write transaction (prevents concurrent race!).
	for i, k := range keys {
		if len(k) == 0 {
			continue
		}
		var uidBuf [8]byte
		binary.BigEndian.PutUint64(uidBuf[:], uids[i])
		if err := t.kvTxn.Set(k, uidBuf[:]); err != nil {
			return -1, err
		}
	}
	return -1, nil
}

// SetNodeFields writes all scalar fields (and their indexes) of a new node directly into
// the transaction's local MemBuffer without any DQL/JSON/DirectedEdge overhead.
func (t *Txn) SetNodeFields(ctx context.Context, bctx *BatchWriteContext, uid uint64, fields []ScalarField) error {
	shardID := flat.CalculateShardID(uid, 0)
	for _, f := range fields {
		if f.Value == nil {
			continue
		}
		meta, err := bctx.getMeta(ctx, f.Attr)
		if err != nil {
			return err
		}
		typedVal, err := toTypedVal(f.Value, meta.schemaType)
		if err != nil {
			return errors.Wrapf(err, "native: convert value for %s", f.Attr)
		}
		binVal := types.Val{Tid: types.BinaryID}
		if err := types.Marshal(typedVal, &binVal); err != nil {
			return errors.Wrapf(err, "native: marshal value for %s", f.Attr)
		}
		valBytes := binVal.Value.([]byte)

		if meta.isList {
			itemHash := farm.Fingerprint64(valBytes)
			if err := t.store.engine.AddListData(t.kvTxn, shardID, t.ns, meta.attrID, uid, itemHash, byte(meta.schemaType), valBytes); err != nil {
				return err
			}
		} else {
			if err := t.store.engine.SetData(t.kvTxn, shardID, t.ns, meta.attrID, uid, byte(meta.schemaType), valBytes); err != nil {
				return err
			}
		}

		if meta.isIndexed && len(meta.tokenizers) > 0 {
			for _, tk := range meta.tokenizers {
				tokens, err := tok.BuildTokens(typedVal.Value, tk)
				if err != nil {
					return errors.Wrapf(err, "native: tokenize %s", f.Attr)
				}
				for _, token := range tokens {
					if err := t.store.engine.AddIndex(t.kvTxn, shardID, t.ns, meta.attrID, []byte(token), uid); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// SetEdge writes a forward edge (and reverse edge if configured in schema) directly into Flat Storage.
func (t *Txn) SetEdge(ctx context.Context, bctx *BatchWriteContext, fromUID, toUID uint64, rawAttr string, facets []byte) error {
	meta, err := bctx.getMeta(ctx, rawAttr)
	if err != nil {
		return err
	}
	shardID := flat.CalculateShardID(fromUID, 0)
	if err := t.store.engine.AddEdge(t.kvTxn, shardID, t.ns, meta.attrID, fromUID, toUID, facets); err != nil {
		return err
	}
	if meta.isReversed {
		revShard := flat.CalculateShardID(toUID, 0)
		if err := t.store.engine.AddReverseEdge(t.kvTxn, revShard, t.ns, meta.attrID, toUID, fromUID, facets); err != nil {
			return err
		}
	}
	return nil
}

func toTypedVal(v any, targetType types.TypeID) (types.Val, error) {
	switch targetType {
	case types.StringID, types.DefaultID:
		if s, ok := v.(string); ok {
			return types.Val{Tid: targetType, Value: s}, nil
		}
	case types.IntID:
		switch iv := v.(type) {
		case int:
			return types.Val{Tid: types.IntID, Value: int64(iv)}, nil
		case int64:
			return types.Val{Tid: types.IntID, Value: iv}, nil
		case float64:
			return types.Val{Tid: types.IntID, Value: int64(iv)}, nil
		}
	case types.FloatID:
		switch fv := v.(type) {
		case float64:
			return types.Val{Tid: types.FloatID, Value: fv}, nil
		case int:
			return types.Val{Tid: types.FloatID, Value: float64(fv)}, nil
		case int64:
			return types.Val{Tid: types.FloatID, Value: float64(fv)}, nil
		}
	case types.BoolID:
		if b, ok := v.(bool); ok {
			return types.Val{Tid: types.BoolID, Value: b}, nil
		}
	case types.DateTimeID:
		switch tv := v.(type) {
		case time.Time:
			return types.Val{Tid: types.DateTimeID, Value: tv}, nil
		case string:
			parsed, err := types.ParseTime(tv)
			if err != nil {
				return types.Val{}, err
			}
			return types.Val{Tid: types.DateTimeID, Value: parsed}, nil
		}
	}
	// Fallback through standard types.Convert from string/binary.
	if s, ok := v.(string); ok {
		return types.Convert(types.Val{Tid: types.StringID, Value: s}, targetType)
	}
	if b, ok := v.([]byte); ok {
		return types.Convert(types.Val{Tid: types.BinaryID, Value: b}, targetType)
	}
	return types.Val{}, errors.Errorf("native: unsupported value type %T for target %s", v, targetType.Name())
}
