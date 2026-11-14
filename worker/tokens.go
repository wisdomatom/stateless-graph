/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pkg/errors"

	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/x"
)

func verifyStringIndex(ctx context.Context, attr string, funcType FuncType) (string, bool) {
	var requiredTokenizer tok.Tokenizer
	switch funcType {
	case ngramFn:
		requiredTokenizer = tok.NGramTokenizer{}
	case fullTextSearchFn:
		requiredTokenizer = tok.FullTextTokenizer{}
	case matchFn:
		requiredTokenizer = tok.TrigramTokenizer{}
	default:
		requiredTokenizer = tok.TermTokenizer{}
	}

	if !schema.State().IsIndexed(ctx, attr) {
		return requiredTokenizer.Name(), false
	}

	id := requiredTokenizer.Identifier()
	for _, t := range schema.State().Tokenizer(ctx, attr) {
		if t.Identifier() == id {
			return requiredTokenizer.Name(), true
		}
	}
	return requiredTokenizer.Name(), false
}

func verifyCustomIndex(ctx context.Context, attr string, tokenizerName string) bool {
	if !schema.State().IsIndexed(ctx, attr) {
		return false
	}
	for _, t := range schema.State().Tokenizer(ctx, attr) {
		if t.Identifier() >= tok.IdentCustom && t.Name() == tokenizerName {
			return true
		}
	}
	return false
}

// Return string tokens from function arguments. It maps function type to correct tokenizer.
// Note: regexp functions require regexp compilation of argument, not tokenization.
func getStringTokens(funcArgs []string, lang string, funcType FuncType, query bool) ([]string, error) {
	if lang == "." {
		lang = "en"
	}
	if funcType == fullTextSearchFn {
		return tok.GetFullTextTokens(funcArgs, lang)
	}
	if funcType == ngramFn {
		if query {
			return tok.GetNGramQueryTokens(funcArgs, lang)
		} else {
			return tok.GetNGramTokens(funcArgs, lang)
		}
	}
	return tok.GetTermTokens(funcArgs)
}

func pickTokenizer(ctx context.Context, attr string, f string) (tok.Tokenizer, error) {
	// Get the tokenizers and choose the corresponding one.
	if !schema.State().IsIndexed(ctx, attr) {
		return nil, errors.Errorf("Attribute %s is not indexed.", attr)
	}

	tokenizers := schema.State().Tokenizer(ctx, attr)
	if tokenizers == nil {
		return nil, errors.Errorf("Schema state not found for %s.", attr)
	}
	for _, t := range tokenizers {
		// If function is eq and we found a tokenizer that's !Lossy(), lets return it
		switch f {
		case "eq":
			// For equality, find a non-lossy tokenizer.
			if !t.IsLossy() {
				return t, nil
			}
		default:
			// rest of the cases: ge, gt, le, lt require a sortable tokenizer.
			if t.IsSortable() {
				return t, nil
			}
		}
	}

	// Should we return an error if we don't find a non-lossy tokenizer for eq function.
	if f != "eq" {
		return nil, errors.Errorf("Attribute:%s does not have proper index for comparison", attr)
	}

	// If we didn't find a !isLossy() tokenizer for eq function on string type predicates,
	// then let's see if we can find a non-trigram tokenizer
	if typ, err := schema.State().TypeOf(attr); err == nil && typ == types.StringID {
		for _, t := range tokenizers {
			if t.Identifier() != tok.IdentTrigram {
				return t, nil
			}
		}
	}

	// otherwise, lets return the first one.
	return tokenizers[0], nil
}

// pickFactoryCreateSpec(ctx, attr) will find the FactoryCreateSpec (i.e.,
// index name + options) for the given attribute "attr".
// Note that unlike pickTokenizer(ctx, attr, f), we do not include the
// parameter "f" (for function name), as we do not take action with it.
// This is otherwise similar to pickTokenizer.
func pickFactoryCreateSpec(ctx context.Context, attr string) (*tok.FactoryCreateSpec, error) {
	// Get the tokenizers and choose the corresponding one.
	if !schema.State().IsIndexed(ctx, attr) {
		return nil, errors.Errorf("Attribute %s is not indexed.", attr)
	}

	cspecs, err := schema.State().FactoryCreateSpec(ctx, attr)
	if err != nil {
		return nil, err
	}
	if len(cspecs) == 0 {
		return nil, errors.Errorf("Schema state not found for %s.", attr)
	}

	// At the moment, it would only be relevant to consider the first one.
	// This is similar to pickTokenizer in behavior.
	return cspecs[0], nil
}

// getInequalityTokens gets tokens ge/le/between compared to given tokens using the first sortable
// index that is found for the predicate.
// In case of ge/gt/le/lt/eq len(ineqValues) should be 1, else(between) len(ineqValues) should be 2.
func getInequalityTokens(ctx context.Context, readTs uint64, attr, f, lang string,
	ineqValues []types.Val) ([]string, []string, error) {

	tokenizer, err := pickTokenizer(ctx, attr, f)
	if err != nil {
		return nil, nil, err
	}

	// Get the token for the value passed in function.
	// XXX: the lang should be query.Langs, but it only matters in edge case test below.
	tokenizer = tok.GetTokenizerForLang(tokenizer, lang)

	var ineqTokensFinal []string
	for _, ineqValue := range ineqValues {
		ineqTokens, err := tok.BuildTokens(ineqValue.Value, tokenizer)
		if err != nil {
			return nil, nil, err
		}

		switch {
		case len(ineqTokens) == 0:
			return nil, nil, nil

		// Allow eq with term/fulltext tokenizers, even though they give multiple tokens.
		case f == "eq" &&
			(tokenizer.Identifier() == tok.IdentTerm || tokenizer.Identifier() == tok.IdentFullText):
			// nothing to do

		case len(ineqTokens) > 1:
			return nil, nil, errors.Errorf("Attribute %s does not have a valid tokenizer.", attr)
		}

		ineqToken := ineqTokens[0]
		ineqTokensFinal = append(ineqTokensFinal, ineqToken)

		if f == "eq" {
			return []string{ineqToken}, ineqTokensFinal, nil
		}
	}

	// If some new index key was written as part of same transaction it won't be on disk
	// until the txn is committed. This is OK, we don't need to overlay in-memory contents on the
	// DB, to keep the design simple and efficient.
	txn := pstore.NewTransactionAt(readTs, false)
	defer txn.Discard()

	isgeOrGt := f == "ge" || f == "gt" || f == "between"

	var seekKey []byte
	var prefix []byte

	ns, rawAttr := x.ParseNamespaceSafe(attr)
	attrID := flat.PredicateToAttrID(rawAttr)

	// Prefix: 7B header + 4B attrID + 1B tokenizer.Identifier() = 12B
	prefix = make([]byte, 12)
	binary.BigEndian.PutUint16(prefix[0:2], 0) // ShardID = 0
	binary.BigEndian.PutUint32(prefix[2:6], ns)
	prefix[6] = flat.KeyTypeIndex
	binary.BigEndian.PutUint32(prefix[7:11], attrID)
	prefix[11] = tokenizer.Identifier()

	seekKey = make([]byte, 11+len(ineqTokensFinal[0]))
	binary.BigEndian.PutUint16(seekKey[0:2], 0)
	binary.BigEndian.PutUint32(seekKey[2:6], ns)
	seekKey[6] = flat.KeyTypeIndex
	binary.BigEndian.PutUint32(seekKey[7:11], attrID)
	copy(seekKey[11:], ineqTokensFinal[0])

	if !isgeOrGt {
		seekKey = append(seekKey, 0xFF)
	}

	itOpt := x.KVIterOpts{
		PrefetchValues: false,
		IsReverse:      !isgeOrGt,
		Prefix:         prefix,
	}
	itr := txn.NewIterator(itOpt)
	defer itr.Close()

	// used for inequality comparison below
	ineqTokenInBytes1 := []byte(ineqTokensFinal[0])

	var ineqTokenInBytes2 []byte
	if f == "between" {
		ineqTokenInBytes2 = []byte(ineqTokensFinal[1])
	}

	var out []string
	addTerm := func(term string) {
		if len(out) == 0 || out[len(out)-1] != term {
			out = append(out, term)
		}
	}

	itr.Seek(seekKey)
LOOP:
	for ; itr.Valid(); itr.Next() {
		item := itr.Item()
		key := item.Key()

		if len(key) < 20 || key[len(key)-9] != 0x00 {
			continue
		}
		term := string(key[11 : len(key)-9])

		switch {
		// if its lossy then we handle inequality comparison later
		// in handleCompareFunction
		case tokenizer.IsLossy():
			if f == "between" && bytes.Compare([]byte(term), ineqTokenInBytes2) > 0 {
				break LOOP
			}
			addTerm(term)

		// for non Lossy lets compare for inequality (gt & lt)
		// to see if key needs to be included
		case f == "gt":
			if bytes.Compare([]byte(term), ineqTokenInBytes1) > 0 {
				addTerm(term)
			}
		case f == "lt":
			if bytes.Compare([]byte(term), ineqTokenInBytes1) < 0 {
				addTerm(term)
			}
		case f == "between":
			if bytes.Compare([]byte(term), ineqTokenInBytes1) >= 0 &&
				bytes.Compare([]byte(term), ineqTokenInBytes2) <= 0 {
				addTerm(term)
			} else { // We should break out of loop as soon as we are out of between range.
				break LOOP
			}
		default:
			// for le or ge or any other fn consider the key
			addTerm(term)
		}
	}

	return out, ineqTokensFinal, nil
}
