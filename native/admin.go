package native

import (
	"context"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

// SchemaPredicate is a native representation of a registered predicate in schema.State().
type SchemaPredicate struct {
	Predicate string
	Type      string
	List      bool
	Index     bool
	Tokenizer []string
	Upsert    bool
}

// GetSchema returns all predicates registered in the given namespace directly from schema.State().
func (s *Store) GetSchema(ctx context.Context, ns uint32) ([]SchemaPredicate, error) {
	preds := schema.State().Predicates()
	out := make([]SchemaPredicate, 0, len(preds))
	for _, fullAttr := range preds {
		attrNs, rawAttr := x.ParseNamespaceAttr(fullAttr)
		if uint32(attrNs) != ns {
			continue
		}
		su, ok := schema.State().Get(ctx, fullAttr)
		if !ok {
			continue
		}
		isIndexed := len(su.Tokenizer) > 0 || len(su.IndexSpecs) > 0
		out = append(out, SchemaPredicate{
			Predicate: rawAttr,
			Type:      types.TypeID(su.ValueType).Name(),
			List:      su.List,
			Index:     isIndexed,
			Tokenizer: su.Tokenizer,
			Upsert:    su.Upsert,
		})
	}
	return out, nil
}

// AlterSchema parses a schema definition string and applies it directly via worker.MutateOverNetwork.
func (s *Store) AlterSchema(ctx context.Context, ns uint32, schemaStr string) error {
	res, err := schema.ParseWithNamespace(schemaStr, uint64(ns))
	if err != nil {
		return err
	}
	m := &pb.Mutations{
		StartTs: worker.State.GetTimestamp(false),
		Schema:  res.Preds,
		Types:   res.Types,
	}
	if _, err := worker.MutateOverNetwork(ctx, m); err != nil {
		return err
	}
	return worker.WaitForIndexing(ctx, true)
}

// DropAll drops all data and schema directly via worker.MutateOverNetwork.
func (s *Store) DropAll(ctx context.Context) error {
	m := &pb.Mutations{
		StartTs: worker.State.GetTimestamp(false),
		DropOp:  pb.Mutations_ALL,
	}
	_, err := worker.MutateOverNetwork(ctx, m)
	return err
}

// DropData drops all data while keeping schema directly via worker.MutateOverNetwork.
func (s *Store) DropData(ctx context.Context) error {
	m := &pb.Mutations{
		StartTs: worker.State.GetTimestamp(false),
		DropOp:  pb.Mutations_DATA,
	}
	_, err := worker.MutateOverNetwork(ctx, m)
	return err
}

// DropAttr drops a single predicate and its data/schema directly via worker.MutateOverNetwork.
func (s *Store) DropAttr(ctx context.Context, ns uint32, attr string) error {
	fullAttr := x.NamespaceAttr(uint64(ns), attr)
	m := &pb.Mutations{
		StartTs: worker.State.GetTimestamp(false),
		Edges: []*pb.DirectedEdge{
			{
				Attr:  fullAttr,
				Op:    pb.DirectedEdge_DEL,
				Value: []byte(x.Star),
			},
		},
	}
	_, err := worker.MutateOverNetwork(ctx, m)
	return err
}
