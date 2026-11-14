/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package conn

import (
	"context"
	"sync"

	"github.com/wisdomatom/stateless-graph/protos/pb"
)

// ProposalCtx stores the context for a proposal with extra information.
type ProposalCtx struct {
	Found uint32
	ErrCh chan error
	Ctx   context.Context
}

type proposals struct {
	sync.RWMutex
	all map[uint64]*ProposalCtx
}

func (p *proposals) Store(key uint64, pctx *ProposalCtx) bool {
	p.Lock()
	defer p.Unlock()
	if p.all == nil {
		p.all = make(map[uint64]*ProposalCtx)
	}
	if _, ok := p.all[key]; ok {
		return false
	}
	p.all[key] = pctx
	return true
}

func (p *proposals) Delete(key uint64) {
	p.Lock()
	defer p.Unlock()
	delete(p.all, key)
}

func (p *proposals) Get(key uint64) *ProposalCtx {
	p.RLock()
	defer p.RUnlock()
	if p.all == nil {
		return nil
	}
	return p.all[key]
}

// RaftServer is a wrapper around node that implements the Raft service using UnimplementedRaftServer.
type RaftServer struct {
	pb.UnimplementedRaftServer
	m    sync.RWMutex
	node *Node
}

func (w *RaftServer) UpdateNode(n *Node) {
	w.m.Lock()
	defer w.m.Unlock()
	w.node = n
}

func (w *RaftServer) GetNode() *Node {
	w.m.RLock()
	defer w.m.RUnlock()
	return w.node
}

// NewRaftServer returns a pointer to a new RaftServer instance.
func NewRaftServer(n *Node) *RaftServer {
	return &RaftServer{node: n}
}
