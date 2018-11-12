package mq

import (
	"time"

	"eventter.io/mq/client"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func (s *Server) apply(cmd interface{}, timeout time.Duration) (index uint64, err error) {
	outer := &Command{}
	switch cmd := cmd.(type) {
	case *client.ConfigureTopicRequest:
		outer.Command = &Command_ConfigureTopic{cmd}
	case *client.DeleteTopicRequest:
		outer.Command = &Command_DeleteTopic{cmd}
	case *client.ConfigureConsumerGroupRequest:
		outer.Command = &Command_ConfigureConsumerGroup{cmd}
	case *client.DeleteConsumerGroupRequest:
		outer.Command = &Command_DeleteConsumerGroup{cmd}
	case *OpenSegmentCommand:
		outer.Command = &Command_OpenSegment{cmd}
	case *CloseSegmentCommand:
		outer.Command = &Command_CloseSegment{cmd}
	case *UpdateNodeCommand:
		outer.Command = &Command_UpdateNode{cmd}
	case *UpdateSegmentNodesCommand:
		outer.Command = &Command_UpdateSegmentNodes{cmd}
	default:
		return 0, errors.Errorf("unhandled command of type: %T", cmd)
	}

	buf, err := proto.Marshal(outer)
	if err != nil {
		return 0, err
	}

	future := s.raftNode.Apply(buf, timeout)
	if err := future.Error(); err != nil {
		return 0, err
	}

	return future.Index(), nil
}
