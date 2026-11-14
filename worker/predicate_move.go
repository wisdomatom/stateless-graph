/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/wisdomatom/stateless-graph/protos/pb"
)

// ReceivePredicate is a stub for the legacy WorkerServer gRPC method.
func (w *grpcWorker) ReceivePredicate(stream pb.Worker_ReceivePredicateServer) error {
	return status.Errorf(codes.Unimplemented, "method ReceivePredicate not supported in stateless mode")
}

// MovePredicate is a stub for the legacy WorkerServer gRPC method.
func (w *grpcWorker) MovePredicate(ctx context.Context, in *pb.MovePredicatePayload) (*api.Payload, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MovePredicate not supported in stateless mode")
}
