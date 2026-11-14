/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package conn

import (
	"time"

	"github.com/dgraph-io/badger/v4/y"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/x"
)

var (
	// ErrNoNode is returned when no node has been set up.
	ErrNoNode = errors.Errorf("No node has been set up yet")
)

type RaftStorage interface {
	FirstIndex() (uint64, error)
	Entries(lo, hi, maxsize uint64) ([]raftpb.Entry, error)
}

// Node represents a node in the graph cluster.
type Node struct {
	x.SafeMutex

	Applied y.WaterMark

	StartTime   time.Time
	Id          uint64
	MyAddr      string
	RaftContext *pb.RaftContext
	Store       RaftStorage

	Proposals proposals
}

func (n *Node) Raft() raft.Node {
	return nil
}

func (n *Node) ConfState() interface{} {
	return nil
}
