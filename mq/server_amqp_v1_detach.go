package mq

import (
	"context"
	"log"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (s *sessionAMQPv1) Detach(ctx context.Context, frame *v1.Detach) error {
	link, ok := s.links[frame.Handle]
	if !ok {
		s.state = sessionStateEnding
		return s.Send(&v1.End{
			Error: &v1.Error{
				Condition:   v1.UnattachedHandleSessionError,
				Description: errors.Errorf("link handle %v not found", frame.Handle).Error(),
			},
		})
	}

	if frame.Error != nil {
		log.Printf(
			"received link error from client [condition=%s, description=%s]",
			frame.Error.Condition,
			frame.Error.Description,
		)
	}

	delete(s.links, frame.Handle)
	linkState := link.State()
	err := link.Close()
	var detachError *v1.Error
	if err != nil {
		detachError = &v1.Error{
			Condition:   v1.InternalErrorAMQPError,
			Description: err.Error(),
		}
		if linkState == linkStateDetaching {
			log.Printf("link close failed: %s", err)
		}
	}

	if linkState != linkStateDetaching {
		err = s.Send(&v1.Detach{
			Handle: frame.Handle,
			Closed: true,
			Error:  detachError,
		})
		return errors.Wrap(err, "send detach failed")
	}

	return nil
}
