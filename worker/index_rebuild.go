/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/tok"
	"github.com/wisdomatom/stateless-graph/x"
)

type IndexRebuild struct {
	Attr          string
	StartTs       uint64
	OldSchema     *pb.SchemaUpdate
	CurrentSchema *pb.SchemaUpdate
}

type indexOp int

const (
	indexNoop    indexOp = iota
	indexDelete
	indexRebuild
)

type indexRebuildInfo struct {
	op                     indexOp
	attr                   string
	tokenizersToDelete     []string
	tokenizersToRebuild    []string
	vectorIndexesToDelete  []*pb.VectorIndexSpec
	vectorIndexesToRebuild []*pb.VectorIndexSpec
}

func (rb *IndexRebuild) needsTokIndexRebuild() *indexRebuildInfo {
	x.AssertTruef(rb.CurrentSchema != nil, "Current schema cannot be nil.")

	old := rb.OldSchema
	if old == nil {
		old = &pb.SchemaUpdate{}
	}

	currIndex := rb.CurrentSchema.Directive == pb.SchemaUpdate_INDEX
	prevIndex := old.Directive == pb.SchemaUpdate_INDEX

	if !currIndex && !prevIndex {
		return &indexRebuildInfo{
			op:   indexNoop,
			attr: rb.Attr,
		}
	}

	if !currIndex {
		return &indexRebuildInfo{
			op:                    indexDelete,
			attr:                  rb.Attr,
			tokenizersToDelete:    old.Tokenizer,
			vectorIndexesToDelete: old.IndexSpecs,
		}
	}

	if currIndex && rb.CurrentSchema.ValueType != old.ValueType {
		return &indexRebuildInfo{
			op:                     indexRebuild,
			attr:                   rb.Attr,
			tokenizersToDelete:     old.Tokenizer,
			tokenizersToRebuild:    rb.CurrentSchema.Tokenizer,
			vectorIndexesToDelete:  old.IndexSpecs,
			vectorIndexesToRebuild: rb.CurrentSchema.IndexSpecs,
		}
	}

	prevTokens := make(map[string]struct{})
	for _, t := range old.Tokenizer {
		prevTokens[t] = struct{}{}
	}
	currTokens := make(map[string]struct{})
	for _, t := range rb.CurrentSchema.Tokenizer {
		currTokens[t] = struct{}{}
	}

	newTokenizers, deletedTokenizers := x.Diff(currTokens, prevTokens)

	prevFactoryNames := make(map[string]struct{})
	prevFactories := make(map[string]*pb.VectorIndexSpec)
	for _, t := range old.IndexSpecs {
		spec, err := tok.GetFactoryCreateSpecFromSpec(t)
		x.AssertTruef(err == nil, "Error while building index spec %s", err)
		name := spec.Name()

		prevFactoryNames[name] = struct{}{}
		prevFactories[name] = t
	}
	currFactoryNames := make(map[string]struct{})
	currFactories := make(map[string]*pb.VectorIndexSpec)
	for _, t := range rb.CurrentSchema.IndexSpecs {
		spec, err := tok.GetFactoryCreateSpecFromSpec(t)
		x.AssertTruef(err == nil, "Error while building index spec %s", err)
		name := spec.Name()

		currFactoryNames[name] = struct{}{}
		currFactories[name] = t
	}

	newFactoryNames, deletedFactoryNames := x.Diff(currFactoryNames, prevFactoryNames)

	if len(newTokenizers) == 0 && len(deletedTokenizers) == 0 &&
		len(newFactoryNames) == 0 && len(deletedFactoryNames) == 0 {
		return &indexRebuildInfo{
			op:   indexNoop,
			attr: rb.Attr,
		}
	}
	newFactories := []*pb.VectorIndexSpec{}
	for _, name := range newFactoryNames {
		newFactories = append(newFactories, currFactories[name])
	}
	deletedFactories := []*pb.VectorIndexSpec{}
	for _, name := range deletedFactoryNames {
		deletedFactories = append(deletedFactories, prevFactories[name])
	}

	return &indexRebuildInfo{
		op:                     indexRebuild,
		attr:                   rb.Attr,
		tokenizersToDelete:     deletedTokenizers,
		tokenizersToRebuild:    newTokenizers,
		vectorIndexesToDelete:  deletedFactories,
		vectorIndexesToRebuild: newFactories,
	}
}

func (rb *IndexRebuild) needsReverseEdgesRebuild() indexOp {
	x.AssertTruef(rb.CurrentSchema != nil, "Current schema cannot be nil.")

	old := rb.OldSchema
	if old == nil {
		old = &pb.SchemaUpdate{}
	}

	currIndex := rb.CurrentSchema.Directive == pb.SchemaUpdate_REVERSE
	prevIndex := old.Directive == pb.SchemaUpdate_REVERSE

	if currIndex == prevIndex {
		return indexNoop
	}

	if currIndex {
		return indexRebuild
	}
	return indexDelete
}

func (rb *IndexRebuild) needsCountIndexRebuild() indexOp {
	x.AssertTruef(rb.CurrentSchema != nil, "Current schema cannot be nil.")

	old := rb.OldSchema
	if old == nil {
		old = &pb.SchemaUpdate{}
	}

	currIndex := rb.CurrentSchema.Count
	prevIndex := old.Count

	if currIndex == prevIndex {
		return indexNoop
	}

	if currIndex {
		return indexRebuild
	}
	return indexDelete
}

func (rb *IndexRebuild) NeedIndexRebuild() bool {
	return rb.needsTokIndexRebuild().op == indexRebuild ||
		rb.needsReverseEdgesRebuild() == indexRebuild ||
		rb.needsCountIndexRebuild() == indexRebuild
}

func (rb *IndexRebuild) GetQuerySchema() *pb.SchemaUpdate {
	querySchema := *rb.CurrentSchema
	info := rb.needsTokIndexRebuild()

	interimTokenizers := make([]string, 0)
	for _, t1 := range rb.OldSchema.Tokenizer {
		found := false
		for _, t2 := range info.tokenizersToDelete {
			if t1 == t2 {
				found = true
				break
			}
		}
		if !found {
			interimTokenizers = append(interimTokenizers, t1)
		}
	}
	querySchema.Tokenizer = interimTokenizers

	if rb.needsCountIndexRebuild() == indexRebuild {
		querySchema.Count = false
	}
	if rb.needsReverseEdgesRebuild() == indexRebuild {
		querySchema.Directive = pb.SchemaUpdate_NONE
	}
	return &querySchema
}

func (rb *IndexRebuild) DropIndexes(ctx context.Context) error {
	return nil
}

func (rb *IndexRebuild) BuildData(ctx context.Context) error {
	return nil
}

func (rb *IndexRebuild) BuildIndexes(ctx context.Context) error {
	return nil
}
