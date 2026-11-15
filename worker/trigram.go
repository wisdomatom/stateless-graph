/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"errors"

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

func uidsForRegex(readTxn x.KVTxn, attr string, arg funcArgs,
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

	uidsForTrigram := func(trigram string) (*pb.List, error) {
		uids, err := GlobalFlatEngine.ScanIndexWithOptions(readTxn, 0, ns, attrID, []byte(trigram), opts.AfterUid, 0)
		if err != nil {
			return nil, err
		}
		return &pb.List{Uids: uids}, nil
	}

	switch query.Op {
	case cindex.QAnd:
		tok.EncodeRegexTokens(query.Trigram)
		for _, t := range query.Trigram {
			trigramUids, err := uidsForTrigram(t)
			if err != nil {
				return nil, err
			}
			if results == nil {
				results = trigramUids
			} else {
				algo.IntersectWith(results, trigramUids, results)
			}

			if proto.Size(results) == 0 {
				return results, nil
			}
		}
		for _, sub := range query.Sub {
			if results == nil {
				results = intersect
			}
			// current list of result is passed for intersection
			var err error
			results, err = uidsForRegex(readTxn, attr, arg, sub, results)
			if err != nil {
				return nil, err
			}
			if proto.Size(results) == 0 {
				return results, nil
			}
		}
	case cindex.QOr:
		tok.EncodeRegexTokens(query.Trigram)
		uidMatrix := make([]*pb.List, len(query.Trigram))
		var err error
		for i, t := range query.Trigram {
			uidMatrix[i], err = uidsForTrigram(t)
			if err != nil {
				return nil, err
			}
		}
		results = algo.MergeSorted(uidMatrix)
		for _, sub := range query.Sub {
			if results == nil {
				results = intersect
			}
			subUids, err := uidsForRegex(readTxn, attr, arg, sub, intersect)
			if err != nil {
				return nil, err
			}
			results = algo.MergeSorted([]*pb.List{results, subUids})
		}
	default:
		return nil, errRegexTooWide
	}
	return results, nil
}
