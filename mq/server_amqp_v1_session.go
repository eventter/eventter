package mq

import (
	"context"
	"log"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

const (
	sessionStateReady   = 0
	sessionStateClosing = 1
)

type sessionAMQPv1 struct {
	connection           *connectionAMQPv1
	state                int
	remoteChannel        uint16
	channel              uint16
	nextIncomingID       v1.TransferNumber
	incomingWindow       uint32
	nextOutgoingID       v1.TransferNumber
	outgoingWindow       uint32
	remoteIncomingWindow uint32
	remoteOutgoingWindow uint32
	namespace            string
	links                map[v1.Handle]*linkAMQPv1
}

func (s *sessionAMQPv1) Send(frame v1.Frame) error {
	meta := frame.GetFrameMeta()
	meta.Channel = s.channel
	return s.connection.transport.Send(frame)
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
	var handle v1.Handle

	switch frame := frame.(type) {
	case *v1.Attach:
		return s.Attach(ctx, frame)
	case *v1.Detach:
		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto EndHandleNotFound
		}
		return link.Detach(ctx, frame)
	case *v1.Flow:
		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto EndHandleNotFound
		}
		err = link.Flow(ctx, frame)
		if err != nil {
			goto EndErrantLink
		}
		return nil
	case *v1.Disposition:
		return s.Disposition(ctx, frame)
	case *v1.Transfer:
		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto EndHandleNotFound
		}
		err = link.Transfer(ctx, frame)
		if err != nil {
			goto EndErrantLink
		}
		return nil
	default:
		return errors.Errorf("unexpected frame %T", frame)
	}

EndHandleNotFound:
	s.state = sessionStateClosing
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   string(v1.UnattachedHandleSessionError),
			Description: errors.Errorf("link handle %v not found", handle).Error(),
		},
	})

EndErrantLink:
	s.state = sessionStateClosing
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   string(v1.ErrantLinkSessionError),
			Description: err.Error(),
		},
	})
}
