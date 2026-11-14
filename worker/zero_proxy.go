/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/x"
)

func forwardAssignUidsToZero(ctx context.Context, in *pb.Num) (*pb.AssignedIds, error) {
	if in.Type != pb.Num_UID {
		return &pb.AssignedIds{}, errors.Errorf("Cannot lease %s via zero proxy", in.Type.String())
	}

	start, end := x.GetNextUids(in.Val)
	return &pb.AssignedIds{
		StartId: start,
		EndId:   end,
	}, nil
}

// RegisterZeroProxyServer forwards select GRPC calls over to Zero
func RegisterZeroProxyServer(s *grpc.Server) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "pb.Zero",
		HandlerType: (*interface{})(nil), // Don't really need complex type checking here
		Methods: []grpc.MethodDesc{
			{
				MethodName: "AssignIds",
				Handler: func(
					srv interface{},
					ctx context.Context,
					dec func(interface{}) error,
					_ grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(pb.Num)
					if err := dec(in); err != nil {
						return nil, err
					}
					return forwardAssignUidsToZero(ctx, in)
				},
			},
		},
	}, &struct{}{})
}
