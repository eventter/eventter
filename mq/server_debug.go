package mq

import (
	"context"
	"fmt"
)

func (s *Server) Debug(ctx context.Context, request *DebugRequest) (*DebugResponse, error) {
	var segmentDumps []string

	state := s.clusterState.Current()

	for _, seg := range state.FindOpenSegmentsIn(s.nodeID) {
		segmentFile, err := s.segmentDir.Open(seg.ID)
		if err == nil {
			segmentDumps = append(segmentDumps, segmentFile.String())
			s.segmentDir.Release(segmentFile)
		} else {
			segmentDumps = append(segmentDumps, fmt.Sprintf("could not open segment %016x: %s", seg.ID, err))
		}
	}

	return &DebugResponse{
		ClusterState: state.String(),
		Segments:     segmentDumps,
	}, nil
}
