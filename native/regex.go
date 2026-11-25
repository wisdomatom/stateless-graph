package native

import (
	"context"
	"errors"
	"regexp/syntax"
	"sort"
	"sync"

	cindex "github.com/google/codesearch/index"
	cregexp "github.com/google/codesearch/regexp"

	"github.com/wisdomatom/stateless-graph/algo"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

const (
	nativeTrigramEarlyStop   = 64
	nativeTrigramParallelism = 8
)

// LookupIndexRegex executes a case-insensitive or case-sensitive regex match directly
// against the ASCII-lowercased Trigram index with parallel racing, early-stop pruning,
// and chunked BatchGet exact verification.
func (s *Store) LookupIndexRegex(ctx context.Context, ns uint32, rawAttr string, pattern string, caseInsensitive bool) ([]uint64, error) {
	fullAttr := x.NamespaceAttr(uint64(ns), rawAttr)
	if !schema.State().HasTokenizer(ctx, tok.IdentTrigram, fullAttr) {
		return nil, errors.New("native: attribute does not have trigram index for regex")
	}

	prefix := "(?m)"
	if caseInsensitive {
		prefix = "(?m)(?i)"
	}
	re, err := cregexp.Compile(prefix + pattern)
	if err != nil {
		return nil, err
	}

	folded := foldRegexSyntax(re.Syntax)
	q := cindex.RegexpQuery(folded)

	readTs := worker.State.GetTimestamp(true)
	attrID := flat.PredicateToAttrID(rawAttr)

	candidates, err := s.evalTrigramQuery(ctx, ns, attrID, readTs, q)
	if err != nil || len(candidates) == 0 {
		return candidates, err
	}

	// Exact regex verification via chunked BatchGet.
	vals, present, err := s.batchGetSingleAttr(ctx, ns, attrID, candidates, readTs)
	if err != nil {
		return nil, err
	}
	filtered := make([]uint64, 0, len(candidates))
	for i, uid := range candidates {
		if !present[i] {
			continue
		}
		sv, cerr := types.Convert(vals[i], types.StringID)
		if cerr == nil {
			if str, ok := sv.Value.(string); ok && len(str) > 0 && re.MatchString(str, true, true) > 0 {
				filtered = append(filtered, uid)
			}
		}
	}
	return filtered, nil
}

func (s *Store) evalTrigramQuery(ctx context.Context, ns uint32, attrID uint32, readTs uint64, q *cindex.Query) ([]uint64, error) {
	scanToken := func(scanCtx context.Context, token string) ([]uint64, error) {
		rTxn := s.db.NewTransactionAt(readTs, false)
		defer rTxn.Discard()
		return s.engine.ScanIndexWithContext(scanCtx, rTxn, 0, ns, attrID, []byte(token), 0, 0)
	}

	switch q.Op {
	case cindex.QAnd:
		tokens := prepareNativeTrigrams(q.Trigram)
		var results *pb.List
		if len(tokens) == 1 {
			uids, err := scanToken(ctx, tokens[0])
			if err != nil {
				return nil, err
			}
			results = &pb.List{Uids: uids}
		} else if len(tokens) > 1 {
			subCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			type resItem struct {
				uids []uint64
				err  error
			}
			resCh := make(chan resItem, len(tokens))
			sem := make(chan struct{}, nativeTrigramParallelism)
			var wg sync.WaitGroup
			for _, tk := range tokens {
				wg.Add(1)
				go func(token string) {
					defer wg.Done()
					select {
					case sem <- struct{}{}:
						defer func() { <-sem }()
					case <-subCtx.Done():
						resCh <- resItem{err: subCtx.Err()}
						return
					}
					uids, err := scanToken(subCtx, token)
					resCh <- resItem{uids: uids, err: err}
				}(tk)
			}
			go func() {
				wg.Wait()
				close(resCh)
			}()

			var firstErr error
			stopped := false
			for r := range resCh {
				if stopped {
					continue
				}
				if r.err != nil {
					if !errors.Is(r.err, context.Canceled) && firstErr == nil {
						firstErr = r.err
						cancel()
						stopped = true
					}
					continue
				}
				if results == nil {
					results = &pb.List{Uids: r.uids}
				} else {
					algo.IntersectWith(results, &pb.List{Uids: r.uids}, results)
				}
				if len(results.Uids) <= nativeTrigramEarlyStop {
					cancel()
					stopped = true
				}
			}
			if firstErr != nil {
				return nil, firstErr
			}
		}

		for _, sub := range q.Sub {
			if results != nil && len(results.Uids) <= nativeTrigramEarlyStop {
				return results.Uids, nil
			}
			subUIDs, err := s.evalTrigramQuery(ctx, ns, attrID, readTs, sub)
			if err != nil {
				return nil, err
			}
			if results == nil {
				results = &pb.List{Uids: subUIDs}
			} else {
				algo.IntersectWith(results, &pb.List{Uids: subUIDs}, results)
			}
			if len(results.Uids) <= nativeTrigramEarlyStop {
				return results.Uids, nil
			}
		}
		if results == nil {
			return nil, nil
		}
		return results.Uids, nil

	case cindex.QOr:
		tokens := prepareNativeTrigrams(q.Trigram)
		lists := make([]*pb.List, 0, len(tokens)+len(q.Sub))
		if len(tokens) > 0 {
			tokLists := make([]*pb.List, len(tokens))
			var wg sync.WaitGroup
			var errMu sync.Mutex
			var firstErr error
			sem := make(chan struct{}, nativeTrigramParallelism)
			for i, tk := range tokens {
				wg.Add(1)
				go func(idx int, token string) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					uids, err := scanToken(ctx, token)
					if err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						return
					}
					tokLists[idx] = &pb.List{Uids: uids}
				}(i, tk)
			}
			wg.Wait()
			if firstErr != nil {
				return nil, firstErr
			}
			lists = append(lists, tokLists...)
		}
		for _, sub := range q.Sub {
			subUIDs, err := s.evalTrigramQuery(ctx, ns, attrID, readTs, sub)
			if err != nil {
				return nil, err
			}
			lists = append(lists, &pb.List{Uids: subUIDs})
		}
		return algo.MergeSorted(lists).Uids, nil

	default:
		return nil, errors.New("native: regular expression is too wide-ranging")
	}
}

func foldRegexSyntax(re *syntax.Regexp) *syntax.Regexp {
	if re == nil {
		return nil
	}
	cp := *re
	cp.Flags &^= syntax.FoldCase
	switch cp.Op {
	case syntax.OpLiteral:
		if len(re.Rune) > 0 {
			cp.Rune = make([]rune, len(re.Rune))
			for i, r := range re.Rune {
				if r >= 'A' && r <= 'Z' {
					r += 'a' - 'A'
				}
				cp.Rune[i] = r
			}
		}
	case syntax.OpCharClass:
		if len(re.Rune) > 0 {
			var folded []rune
			for i := 0; i+1 < len(re.Rune); i += 2 {
				lo, hi := re.Rune[i], re.Rune[i+1]
				if hi < 'A' || lo > 'Z' {
					folded = append(folded, lo, hi)
					continue
				}
				if lo < 'A' {
					folded = append(folded, lo, 'A'-1)
				}
				uLo, uHi := lo, hi
				if uLo < 'A' {
					uLo = 'A'
				}
				if uHi > 'Z' {
					uHi = 'Z'
				}
				folded = append(folded, uLo+('a'-'A'), uHi+('a'-'A'))
				if hi > 'Z' {
					folded = append(folded, 'Z'+1, hi)
				}
			}
			cp.Rune = folded
		}
	}
	if len(re.Sub) > 0 {
		cp.Sub = make([]*syntax.Regexp, len(re.Sub))
		for i, sub := range re.Sub {
			cp.Sub[i] = foldRegexSyntax(sub)
		}
	}
	return &cp
}

func prepareNativeTrigrams(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(raw))
	uniq := make([]string, 0, len(raw))
	for _, t := range raw {
		lt := tok.ASCIILower(t)
		if _, ok := seen[lt]; ok {
			continue
		}
		seen[lt] = struct{}{}
		uniq = append(uniq, lt)
	}
	score := func(s string) int {
		hasLetter, hasNonDigit := false, false
		for i := 0; i < len(s); i++ {
			b := s[i]
			if (b >= 'a' && b <= 'z') || b >= 0x80 {
				hasLetter, hasNonDigit = true, true
			} else if b < '0' || b > '9' {
				hasNonDigit = true
			}
		}
		if hasLetter {
			return 0
		}
		if hasNonDigit {
			return 1
		}
		return 2
	}
	sort.SliceStable(uniq, func(i, j int) bool { return score(uniq[i]) < score(uniq[j]) })
	tok.EncodeRegexTokens(uniq)
	return uniq
}
