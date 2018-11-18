package mq

import (
	"context"
	"crypto/sha1"

	"github.com/pkg/errors"
)

func (s *Server) SegmentSum(ctx context.Context, request *SegmentSumRequest) (*SegmentSumResponse, error) {
	if !s.segmentDir.Exists(request.SegmentID) {
		return &SegmentSumResponse{
			SegmentID: request.SegmentID,
			Size_:     -1,
		}, nil
	}

	segment, err := s.segmentDir.Open(request.SegmentID)
	if err != nil {
		return nil, errors.Wrap(err, "open segment failed")
	}
	defer s.segmentDir.Release(segment)

	sha1Sum, size, err := segment.Sum(sha1.New(), request.Size_)
	if err != nil {
		return nil, err
	}

	return &SegmentSumResponse{
		SegmentID: request.SegmentID,
		Size_:     size,
		Sha1:      sha1Sum,
	}, nil
}
