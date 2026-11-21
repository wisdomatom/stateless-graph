/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"errors"
	"regexp/syntax"
	"sort"
	"sync"

	cindex "github.com/google/codesearch/index"
	"google.golang.org/protobuf/proto"

	"github.com/wisdomatom/stateless-graph/algo"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/x"
)

var errRegexTooWide = errors.New(
	"regular expression is too wide-ranging and can't be executed efficiently")

const (
	// trigramEarlyStopThreshold: once a QAnd intersection narrows the candidate
	// UIDs to at most this count, stop scanning remaining trigram posting lists
	// and let the exact regex verification filter the small candidate set.
	trigramEarlyStopThreshold = 64

	// trigramMaxParallelScans limits concurrent TiKV index scans per regex query.
	trigramMaxParallelScans = 8
)

// foldRegexSyntaxForTrigram clones a regexp/syntax.Regexp AST and folds ASCII
// uppercase letters ('A'-'Z') to lowercase while clearing syntax.FoldCase.
// Because TrigramTokenizer indexes ASCII-lowercased trigrams, this eliminates
// the 2^3=8x QOr blowup caused by /i (case-insensitive) regexes while keeping
// 100% recall for both case-sensitive and case-insensitive exact verification.
func foldRegexSyntaxForTrigram(re *syntax.Regexp) *syntax.Regexp {
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
				// Split off any overlap with ASCII 'A'-'Z' and map it to 'a'-'z'.
				if hi < 'A' || lo > 'Z' {
					folded = append(folded, lo, hi)
					continue
				}
				if lo < 'A' {
					folded = append(folded, lo, 'A'-1)
				}
				uLo := lo
				if uLo < 'A' {
					uLo = 'A'
				}
				uHi := hi
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
			cp.Sub[i] = foldRegexSyntaxForTrigram(sub)
		}
	}
	return &cp
}

// trigramSelectivityScore assigns a heuristic priority score to a raw trigram.
// Trigrams containing letters/non-ASCII bytes are generally far more selective
// than pure-digit trigrams (e.g. shared timestamps like "179", "795", "952").
func trigramSelectivityScore(raw string) int {
	hasLetter := false
	hasNonDigit := false
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b >= 0x80 {
			hasLetter = true
			hasNonDigit = true
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

// prepareTrigrams deduplicates, ASCII-lowercases, prioritizes by selectivity,
// and encodes raw trigrams into index tokens.
func prepareTrigrams(rawTrigrams []string) []string {
	if len(rawTrigrams) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(rawTrigrams))
	unique := make([]string, 0, len(rawTrigrams))
	for _, t := range rawTrigrams {
		lt := tok.ASCIILower(t)
		if _, ok := seen[lt]; ok {
			continue
		}
		seen[lt] = struct{}{}
		unique = append(unique, lt)
	}
	sort.SliceStable(unique, func(i, j int) bool {
		return trigramSelectivityScore(unique[i]) < trigramSelectivityScore(unique[j])
	})
	tok.EncodeRegexTokens(unique)
	return unique
}

func uidsForRegex(ctx context.Context, readTxn x.KVTxn, attr string, arg funcArgs,
	query *cindex.Query, intersect *pb.List) (*pb.List, error) {
	var results *pb.List
	opts := ListOptions{
		ReadTs:   arg.q.ReadTs,
		First:    int(arg.q.First),
		AfterUid: arg.q.AfterUid,
	}
	if proto.Size(intersect) > 0 {
		opts.Intersect = intersect
	}

	ns, rawAttr := parseNamespaceSafe(attr)
	attrID := flat.PredicateToAttrID(rawAttr)
	canParallel := pstore != nil && !x.IsActiveWriteTs(arg.q.ReadTs)

	scanOne := func(scanCtx context.Context, useOwnTxn bool, trigram string) (*pb.List, error) {
		txn := readTxn
		if useOwnTxn {
			txn = pstore.NewTransactionAt(arg.q.ReadTs, false)
			defer txn.Discard()
		}
		uids, err := GlobalFlatEngine.ScanIndexWithContext(scanCtx, txn, 0, ns, attrID, []byte(trigram), opts.AfterUid, 0)
		if err != nil {
			return nil, err
		}
		return &pb.List{Uids: uids}, nil
	}

	switch query.Op {
	case cindex.QAnd:
		encoded := prepareTrigrams(query.Trigram)
		if len(encoded) > 0 {
			if canParallel && len(encoded) > 1 {
				subCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				type scanRes struct {
					list *pb.List
					err  error
				}
				resCh := make(chan scanRes, len(encoded))
				sem := make(chan struct{}, trigramMaxParallelScans)

				var wg sync.WaitGroup
				for _, t := range encoded {
					wg.Add(1)
					go func(token string) {
						defer wg.Done()
						select {
						case sem <- struct{}{}:
							defer func() { <-sem }()
						case <-subCtx.Done():
							resCh <- scanRes{err: subCtx.Err()}
							return
						}
						list, err := scanOne(subCtx, true, token)
						resCh <- scanRes{list: list, err: err}
					}(t)
				}

				// Close resCh once all workers finish so we never leak goroutines.
				go func() {
					wg.Wait()
					close(resCh)
				}()

				var firstErr error
				earlyStopped := false
				for r := range resCh {
					if earlyStopped {
						continue
					}
					if r.err != nil {
						if !errors.Is(r.err, context.Canceled) && firstErr == nil {
							firstErr = r.err
							cancel()
							earlyStopped = true
						}
						continue
					}
					if results == nil {
						results = r.list
					} else {
						algo.IntersectWith(results, r.list, results)
					}
					if len(results.Uids) <= trigramEarlyStopThreshold {
						// Small candidate set (or empty) reached: cancel all slower scans!
						cancel()
						earlyStopped = true
					}
				}
				if firstErr != nil {
					return nil, firstErr
				}
				if results == nil {
					results = &pb.List{}
				}
				if len(results.Uids) <= trigramEarlyStopThreshold {
					return results, nil
				}
			} else {
				for _, t := range encoded {
					trigramUids, err := scanOne(ctx, false, t)
					if err != nil {
						return nil, err
					}
					if results == nil {
						results = trigramUids
					} else {
						algo.IntersectWith(results, trigramUids, results)
					}
					if len(results.Uids) <= trigramEarlyStopThreshold {
						return results, nil
					}
				}
			}
		}

		for _, sub := range query.Sub {
			if results != nil && len(results.Uids) <= trigramEarlyStopThreshold {
				return results, nil
			}
			subIntersect := results
			if subIntersect == nil {
				subIntersect = intersect
			}
			subUids, err := uidsForRegex(ctx, readTxn, attr, arg, sub, subIntersect)
			if err != nil {
				return nil, err
			}
			if results == nil {
				results = subUids
			} else {
				algo.IntersectWith(results, subUids, results)
			}
			if len(results.Uids) <= trigramEarlyStopThreshold {
				return results, nil
			}
		}

	case cindex.QOr:
		encoded := prepareTrigrams(query.Trigram)
		if len(encoded) > 0 {
			uidMatrix := make([]*pb.List, len(encoded))
			if canParallel && len(encoded) > 1 {
				subCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				sem := make(chan struct{}, trigramMaxParallelScans)
				var wg sync.WaitGroup
				var errMu sync.Mutex
				var firstErr error
				for i, t := range encoded {
					wg.Add(1)
					go func(idx int, token string) {
						defer wg.Done()
						select {
						case sem <- struct{}{}:
							defer func() { <-sem }()
						case <-subCtx.Done():
							return
						}
						list, err := scanOne(subCtx, true, token)
						if err != nil {
							errMu.Lock()
							if firstErr == nil && !errors.Is(err, context.Canceled) {
								firstErr = err
								cancel()
							}
							errMu.Unlock()
							return
						}
						uidMatrix[idx] = list
					}(i, t)
				}
				wg.Wait()
				if firstErr != nil {
					return nil, firstErr
				}
			} else {
				var err error
				for i, t := range encoded {
					uidMatrix[i], err = scanOne(ctx, false, t)
					if err != nil {
						return nil, err
					}
				}
			}
			results = algo.MergeSorted(uidMatrix)
		}
		for _, sub := range query.Sub {
			if results == nil {
				results = intersect
			}
			subUids, err := uidsForRegex(ctx, readTxn, attr, arg, sub, intersect)
			if err != nil {
				return nil, err
			}
			results = algo.MergeSorted([]*pb.List{results, subUids})
		}

	default:
		return nil, errRegexTooWide
	}
	if results == nil {
		results = &pb.List{}
	}
	return results, nil
}
