package mq

import (
	"context"
	"log"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

const (
	sessionStateReady  = 0
	sessionStateEnding = 1
)

type sessionAMQPv1 struct {
	connection            *connectionAMQPv1
	state                 int
	remoteChannel         uint16
	channel               uint16
	nextIncomingID        v1.TransferNumber
	incomingWindow        uint32
	nextOutgoingID        v1.TransferNumber
	outgoingWindow        uint32
	remoteIncomingWindow  uint32
	remoteOutgoingWindow  uint32
	initialIncomingWindow uint32
	namespace             string
	links                 map[v1.Handle]*linkAMQPv1
}

func (s *sessionAMQPv1) Send(frame v1.Frame) error {
	meta := frame.GetFrameMeta()
	meta.Channel = s.channel
	return s.connection.Send(frame)
}

func (s *sessionAMQPv1) Close() error {
	for _, link := range s.links {
		err := link.Close()
		if err != nil {
			log.Printf("link close failed with: %v", err)
		}
	}
	s.links = make(map[v1.Handle]*linkAMQPv1)
	return nil
}

func (s *sessionAMQPv1) Process(ctx context.Context, frame v1.Frame) (err error) {
	if s.state == sessionStateEnding {
		return nil
	}

	var handle v1.Handle

	switch frame := frame.(type) {
	case *v1.Attach:
		return s.Attach(ctx, frame)
	case *v1.Detach:
		return s.Detach(ctx, frame)
	case *v1.Flow:
		err = s.Flow(ctx, frame)
		if err != nil {
			return errors.Wrap(err, "session flow failed")
		}
		if frame.Handle != v1.HandleNull {
			link, ok := s.links[frame.Handle]
			if !ok {
				handle = frame.Handle
				goto EndHandleNotFound
			}
			err = link.Flow(ctx, frame)
			if err != nil {
				goto EndErrantLink
			}
		}
		return nil
	case *v1.Disposition:
		return s.Disposition(ctx, frame)
	case *v1.Transfer:
		s.nextIncomingID += 1
		s.incomingWindow -= 1
		s.remoteOutgoingWindow -= 1

		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto EndHandleNotFound
		}
		err = link.Transfer(ctx, frame)
		if err != nil {
			goto EndErrantLink
		}

		if s.incomingWindow <= s.initialIncomingWindow/2 {
			s.incomingWindow = s.initialIncomingWindow
			err = s.Send(&v1.Flow{
				NextIncomingID: s.nextIncomingID,
				IncomingWindow: s.incomingWindow,
				NextOutgoingID: s.nextOutgoingID,
				OutgoingWindow: s.outgoingWindow,
				Handle:         v1.HandleNull,
			})
			if err != nil {
				return errors.Wrap(err, "send session flow failed")
			}
		}

		return nil
	default:
		return errors.Errorf("unexpected frame %T", frame)
	}

EndHandleNotFound:
	s.state = sessionStateEnding
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   v1.UnattachedHandleSessionError,
			Description: errors.Errorf("link handle %v not found", handle).Error(),
		},
	})

EndErrantLink:
	s.state = sessionStateEnding
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   v1.ErrantLinkSessionError,
			Description: err.Error(),
		},
	})
}
