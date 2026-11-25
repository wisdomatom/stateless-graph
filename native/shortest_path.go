package native

import (
	"container/heap"
	"context"
	"math"

	"github.com/wisdomatom/stateless-graph/storage/flat"
	"github.com/wisdomatom/stateless-graph/worker"
)

// PathResult represents a shortest path of UIDs and its total weight.
type PathResult struct {
	UIDs   []uint64
	Weight float64
}

type pqItem struct {
	uid  uint64
	dist float64
}

type minHeap []pqItem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(pqItem)) }
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// ShortestPath computes the shortest path between fromUID and toUID directly on Flat Storage
// using KeyOnly BFS (unweighted) or Dijkstra (weighted by edge facet), with zero DQL overhead.
func (s *Store) ShortestPath(ctx context.Context, ns uint32, fromUID, toUID uint64, edgeAttrs []string, maxDepth int) (*PathResult, error) {
	if fromUID == 0 || toUID == 0 || len(edgeAttrs) == 0 {
		return nil, nil
	}
	if fromUID == toUID {
		return &PathResult{UIDs: []uint64{fromUID}, Weight: 0}, nil
	}
	if maxDepth <= 0 {
		maxDepth = 16
	}

	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrIDs := make([]uint32, len(edgeAttrs))
	for i, a := range edgeAttrs {
		attrIDs[i] = flat.PredicateToAttrID(a)
	}

	// Unweighted shortest path via KeyOnly BFS.
	type bfsNode struct {
		uid   uint64
		depth int
	}
	visited := map[uint64]uint64{fromUID: 0} // child -> parent
	queue := []bfsNode{{uid: fromUID, depth: 0}}

	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.depth >= maxDepth {
			continue
		}
		shardID := flat.CalculateShardID(cur.uid, 0)
		for _, attrID := range attrIDs {
			it, err := s.engine.ScanOutEdgesKeyOnly(rTxn, shardID, ns, attrID, cur.uid, 0)
			if err != nil {
				return nil, err
			}
			for it.Next() {
				nextUID := it.Entry().ToUID
				if _, seen := visited[nextUID]; seen {
					continue
				}
				visited[nextUID] = cur.uid
				if nextUID == toUID {
					it.Close()
					return reconstructPath(visited, fromUID, toUID), nil
				}
				queue = append(queue, bfsNode{uid: nextUID, depth: cur.depth + 1})
			}
			it.Close()
		}
	}
	return nil, nil
}

// ShortestPathWeighted computes a weighted shortest path via Dijkstra using a custom weight extractor.
func (s *Store) ShortestPathWeighted(ctx context.Context, ns uint32, fromUID, toUID uint64, edgeAttrs []string, weightFn func(facets []byte) (float64, bool)) (*PathResult, error) {
	if fromUID == 0 || toUID == 0 || len(edgeAttrs) == 0 {
		return nil, nil
	}
	if fromUID == toUID {
		return &PathResult{UIDs: []uint64{fromUID}, Weight: 0}, nil
	}

	readTs := worker.State.GetTimestamp(true)
	rTxn := s.db.NewTransactionAt(readTs, false)
	defer rTxn.Discard()

	attrIDs := make([]uint32, len(edgeAttrs))
	for i, a := range edgeAttrs {
		attrIDs[i] = flat.PredicateToAttrID(a)
	}

	dist := map[uint64]float64{fromUID: 0}
	parent := map[uint64]uint64{fromUID: 0}
	pq := &minHeap{{uid: fromUID, dist: 0}}
	heap.Init(pq)

	for pq.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cur := heap.Pop(pq).(pqItem)
		if cur.uid == toUID {
			res := reconstructPath(parent, fromUID, toUID)
			res.Weight = cur.dist
			return res, nil
		}
		if cur.dist > dist[cur.uid] {
			continue
		}
		shardID := flat.CalculateShardID(cur.uid, 0)
		for _, attrID := range attrIDs {
			it, err := s.engine.ScanOutEdgesFrom(rTxn, shardID, ns, attrID, cur.uid, 0)
			if err != nil {
				return nil, err
			}
			for it.Next() {
				entry := it.Entry()
				w, ok := weightFn(entry.Facets)
				if !ok || w < 0 || math.IsNaN(w) {
					continue
				}
				nd := cur.dist + w
				if old, exists := dist[entry.ToUID]; !exists || nd < old {
					dist[entry.ToUID] = nd
					parent[entry.ToUID] = cur.uid
					heap.Push(pq, pqItem{uid: entry.ToUID, dist: nd})
				}
			}
			it.Close()
		}
	}
	return nil, nil
}

func reconstructPath(parent map[uint64]uint64, fromUID, toUID uint64) *PathResult {
	var rev []uint64
	for cur := toUID; cur != 0; cur = parent[cur] {
		rev = append(rev, cur)
		if cur == fromUID {
			break
		}
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return &PathResult{
		UIDs:   rev,
		Weight: float64(len(rev) - 1),
	}
}
