/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (w *grpcWorker) StreamSnapshot(stream pb.Worker_StreamSnapshotServer) error {
	return status.Errorf(codes.Unimplemented, "StreamSnapshot is not supported in stateless/single-node mode")
}
