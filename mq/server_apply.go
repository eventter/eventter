package mq

import (
	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) Apply(cmd interface{}) (index uint64, err error) {
	outer := &ClusterCommand{}
	switch cmd := cmd.(type) {
	case *client.ConfigureTopicRequest:
		outer.Command = &ClusterCommand_ConfigureTopic{cmd}
	case *client.DeleteTopicRequest:
		outer.Command = &ClusterCommand_DeleteTopic{cmd}
	case *client.ConfigureConsumerGroupRequest:
		outer.Command = &ClusterCommand_ConfigureConsumerGroup{cmd}
	case *client.DeleteConsumerGroupRequest:
		outer.Command = &ClusterCommand_DeleteConsumerGroup{cmd}
	case *ClusterOpenSegmentCommand:
		outer.Command = &ClusterCommand_OpenSegment{cmd}
	case *ClusterCloseSegmentCommand:
		outer.Command = &ClusterCommand_CloseSegment{cmd}
	case *ClusterUpdateNodeCommand:
		outer.Command = &ClusterCommand_UpdateNode{cmd}
	case *ClusterUpdateSegmentNodesCommand:
		outer.Command = &ClusterCommand_UpdateSegmentNodes{cmd}
	case *ClusterDeleteSegmentCommand:
		outer.Command = &ClusterCommand_DeleteSegment{cmd}
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
