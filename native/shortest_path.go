package native

import (
	"container/heap"
	"context"
	"math"
	"strconv"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/protobuf/proto"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/types/facets"
	"github.com/wisdomatom/stateless-graph/worker"
)

// PathStep represents a single directed edge hop in a shortest path.
type PathStep struct {
	FromUID   uint64
	ToUID     uint64
	Predicate string
	Weight    float64
	Facets    map[string]any
}

// PathResult represents a complete shortest path of UIDs, steps, and total weight.
type PathResult struct {
	UIDs   []uint64
	Steps  []PathStep
	Weight float64
}

// ShortestPathOptions configures native K-Shortest-Path execution.
type ShortestPathOptions struct {
	FromUID     uint64
	ToUID       uint64
	EdgeAttrs   []string
	NumPaths    int
	MinWeight   float64
	MaxWeight   float64
	WeightFacet string
	NeedFacets  bool
	MaxDepth    int
	EdgeFilter  func(predicate string, fMap map[string]any) bool
}

type pathState struct {
	uid    uint64
	dist   float64
	steps  []PathStep
	inPath map[uint64]bool
}

type pathHeap []*pathState

func (h pathHeap) Len() int           { return len(h) }
func (h pathHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h pathHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pathHeap) Push(x any)        { *h = append(*h, x.(*pathState)) }
func (h *pathHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// DecodeFacetValue decodes a single protobuf Facet into a Go value.
func DecodeFacetValue(f *api.Facet) any {
	if f == nil {
		return nil
	}
	v, err := facets.ValFor(f)
	if err != nil {
		return string(f.Value)
	}
	if t, ok := v.Value.(time.Time); ok {
		return t.UTC().Format(time.RFC3339)
	}
	return v.Value
}

// DecodeFacetsSlice decodes a slice of protobuf Facets into a map[string]any.
func DecodeFacetsSlice(fl []*api.Facet) map[string]any {
	res := make(map[string]any, len(fl))
	for _, f := range fl {
		res[f.Key] = DecodeFacetValue(f)
	}
	return res
}

func decodeProtoFacets(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var fl pb.Facets
	if err := proto.Unmarshal(raw, &fl); err != nil {
		return map[string]any{}
	}
	return DecodeFacetsSlice(fl.Facets)
}

func extractFacetWeight(fMap map[string]any, weightFacet string) (float64, bool) {
	if weightFacet == "" {
		return 1.0, true
	}
	v, ok := fMap[weightFacet]
	if !ok || v == nil {
		return 0, false
	}
	switch nv := v.(type) {
	case float64:
		return nv, true
	case int64:
		return float64(nv), true
	case int:
		return float64(nv), true
	case string:
		if parsed, err := strconv.ParseFloat(nv, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// KShortestPaths finds up to `opts.NumPaths` simple shortest paths directly on Flat Storage
// using Dijkstra priority-queue expansion (KeyOnly when unweighted & facets not needed).
func (s *Store) KShortestPaths(ctx context.Context, ns uint32, opts ShortestPathOptions) ([]*PathResult, error) {
	if opts.FromUID == 0 || opts.ToUID == 0 || len(opts.EdgeAttrs) == 0 {
		return nil, nil
	}
	if opts.NumPaths <= 0 {
		opts.NumPaths = 1
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = 32
	}
	if opts.FromUID == opts.ToUID {
		return []*PathResult{{UIDs: []uint64{opts.FromUID}, Weight: 0}}, nil
	}

	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrIDs := make([]uint32, len(opts.EdgeAttrs))
	for i, a := range opts.EdgeAttrs {
		attrIDs[i] = flat.PredicateToAttrID(a)
	}

	needFacets := opts.NeedFacets || opts.WeightFacet != "" || opts.EdgeFilter != nil

	// Cache outgoing edges per visited UID to avoid rescanning TiKV during K-shortest expansion.
	type cachedEdge struct {
		toUID     uint64
		predicate string
		facets    map[string]any
		weight    float64
	}
	adjCache := make(map[uint64][]cachedEdge)

	getNeighbors := func(u uint64) ([]cachedEdge, error) {
		if edges, ok := adjCache[u]; ok {
			return edges, nil
		}
		shardID := flat.CalculateShardID(u, 0)
		var out []cachedEdge
		for idx, attrID := range attrIDs {
			pred := opts.EdgeAttrs[idx]
			var it flat.EdgeIterator
			var err error
			if needFacets {
				it, err = s.engine.ScanOutEdgesFrom(rTxn, shardID, ns, attrID, u, 0)
			} else {
				it, err = s.engine.ScanOutEdgesKeyOnly(rTxn, shardID, ns, attrID, u, 0)
			}
			if err != nil {
				return nil, err
			}
			for it.Next() {
				entry := it.Entry()
				var fMap map[string]any
				w := 1.0
				if needFacets {
					fMap = decodeProtoFacets(entry.Facets)
					if opts.EdgeFilter != nil && !opts.EdgeFilter(pred, fMap) {
						continue
					}
					var ok bool
					w, ok = extractFacetWeight(fMap, opts.WeightFacet)
					if !ok || w < 0 || math.IsNaN(w) {
						continue
					}
				} else {
					fMap = map[string]any{}
				}
				out = append(out, cachedEdge{
					toUID:     entry.ToUID,
					predicate: pred,
					facets:    fMap,
					weight:    w,
				})
			}
			it.Close()
		}
		adjCache[u] = out
		return out, nil
	}

	pq := &pathHeap{
		&pathState{
			uid:    opts.FromUID,
			dist:   0,
			inPath: map[uint64]bool{opts.FromUID: true},
		},
	}
	heap.Init(pq)

	visitCount := make(map[uint64]int)
	var results []*PathResult

	for pq.Len() > 0 && len(results) < opts.NumPaths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cur := heap.Pop(pq).(*pathState)
		if opts.MaxWeight > 0 && cur.dist > opts.MaxWeight {
			break
		}

		if cur.uid == opts.ToUID {
			if opts.MinWeight > 0 && cur.dist < opts.MinWeight {
				continue
			}
			uids := make([]uint64, 0, len(cur.steps)+1)
			uids = append(uids, opts.FromUID)
			for _, st := range cur.steps {
				uids = append(uids, st.ToUID)
			}
			results = append(results, &PathResult{
				UIDs:   uids,
				Steps:  cur.steps,
				Weight: cur.dist,
			})
			continue
		}

		visitCount[cur.uid]++
		if visitCount[cur.uid] > opts.NumPaths {
			continue
		}
		if len(cur.steps) >= opts.MaxDepth {
			continue
		}

		neighbors, err := getNeighbors(cur.uid)
		if err != nil {
			return nil, err
		}
		for _, ne := range neighbors {
			if cur.inPath[ne.toUID] {
				continue // Enforce simple paths (no cycles).
			}
			nd := cur.dist + ne.weight
			if opts.MaxWeight > 0 && nd > opts.MaxWeight {
				continue
			}
			nextSteps := make([]PathStep, len(cur.steps)+1)
			copy(nextSteps, cur.steps)
			nextSteps[len(cur.steps)] = PathStep{
				FromUID:   cur.uid,
				ToUID:     ne.toUID,
				Predicate: ne.predicate,
				Weight:    ne.weight,
				Facets:    ne.facets,
			}
			nextInPath := make(map[uint64]bool, len(cur.inPath)+1)
			for k, v := range cur.inPath {
				nextInPath[k] = v
			}
			nextInPath[ne.toUID] = true
			heap.Push(pq, &pathState{
				uid:    ne.toUID,
				dist:   nd,
				steps:  nextSteps,
				inPath: nextInPath,
			})
		}
	}

	return results, nil
}

// GetNodeTypes batch-reads the primary dgraph.type for each UID.
func (s *Store) GetNodeTypes(ctx context.Context, ns uint32, uids []uint64) (map[uint64]string, error) {
	out := make(map[uint64]string, len(uids))
	if len(uids) == 0 {
		return out, nil
	}
	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrID := flat.PredicateToAttrID("dgraph.type")
	for _, uid := range uids {
		_, valBytesList, err := s.engine.ScanData(rTxn, flat.CalculateShardID(uid, 0), ns, attrID, uid)
		if err == nil && len(valBytesList) > 0 {
			out[uid] = string(valBytesList[0])
		} else {
			out[uid] = "Unknown"
		}
	}
	return out, nil
}
