package mq

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) Apply(cmd interface{}) (index uint64, err error) {
	outer := &ClusterCommand{}
	switch cmd := cmd.(type) {
	case *ClusterCommandTopicCreate:
		outer.Command = &ClusterCommand_CreateTopic{cmd}
	case *ClusterCommandTopicDelete:
		outer.Command = &ClusterCommand_DeleteTopic{cmd}
	case *ClusterCommandConsumerGroupCreate:
		outer.Command = &ClusterCommand_CreateConsumerGroup{cmd}
	case *ClusterCommandConsumerGroupDelete:
		outer.Command = &ClusterCommand_DeleteConsumerGroup{cmd}
	case *ClusterCommandSegmentOpen:
		outer.Command = &ClusterCommand_OpenSegment{cmd}
	case *ClusterCommandSegmentClose:
		outer.Command = &ClusterCommand_CloseSegment{cmd}
	case *ClusterCommandNodeUpdate:
		outer.Command = &ClusterCommand_UpdateNode{cmd}
	case *ClusterCommandSegmentNodesUpdate:
		outer.Command = &ClusterCommand_UpdateSegmentNodes{cmd}
	case *ClusterCommandSegmentDelete:
		outer.Command = &ClusterCommand_DeleteSegment{cmd}
	case *ClusterCommandConsumerGroupOffsetCommitsUpdate:
		outer.Command = &ClusterCommand_UpdateConsumerGroupOffsetCommits{cmd}
	default:
		return 0, errors.Errorf("unhandled command of type: %T", cmd)
	}

	buf, err := proto.Marshal(outer)
	if err != nil {
		return 0, err
	}

	future := s.raftNode.Apply(buf, applyTimeout)
	if err := future.Error(); err != nil {
		return 0, err
	}

	return future.Index(), nil
}
