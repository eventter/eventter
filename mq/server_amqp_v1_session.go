package mq

import (
	"context"
	"log"
	"sync"

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
	mutex                 sync.Mutex
	cond                  sync.Cond
	nextIncomingID        v1.TransferNumber
	incomingWindow        uint32
	nextOutgoingID        v1.TransferNumber
	outgoingWindow        uint32
	remoteIncomingWindow  uint32
	remoteOutgoingWindow  uint32
	initialIncomingWindow uint32
	initialOutgoingWindow uint32
	initialOutgoingID     v1.TransferNumber
	namespace             string
	links                 map[v1.Handle]linkAMQPv1
	deliveryID            v1.DeliveryNumber
	inflight              []sessionAMQPv1Inflight
}

type sessionAMQPv1Inflight struct {
	deliveryID     v1.DeliveryNumber
	nodeID         uint64
	subscriptionID uint64
	seqNo          uint64
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
	s.links = make(map[v1.Handle]linkAMQPv1)
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
		var link linkAMQPv1
		if frame.Handle != v1.HandleNull {
			var ok bool
			link, ok = s.links[frame.Handle]
			if !ok {
				handle = frame.Handle
				goto HandleNotFound
			}
			err = link.Flow(ctx, frame)
			if err != nil {
				return errors.Wrap(err, "link flow failed")
			}
		}
		if frame.Echo {
			s.mutex.Lock()
			echoFrame := &v1.Flow{
				NextIncomingID: s.nextIncomingID,
				IncomingWindow: s.incomingWindow,
				NextOutgoingID: s.nextOutgoingID,
				OutgoingWindow: s.outgoingWindow,
			}
			s.mutex.Unlock()
			if link != nil {
				echoFrame.Handle = frame.Handle
				echoFrame.DeliveryCount, echoFrame.LinkCredit = link.Credit()
			}
			err = s.Send(echoFrame)
			if err != nil {
				return errors.Wrap(err, "send flow echo failed")
			}
		}
		return nil
	case *v1.Disposition:
		return s.Disposition(ctx, frame)
	case *v1.Transfer:
		s.mutex.Lock()
		s.nextIncomingID++
		s.incomingWindow--
		s.remoteOutgoingWindow--
		doFlow := s.incomingWindow <= s.initialIncomingWindow/2
		s.mutex.Unlock()

		link, ok := s.links[frame.Handle]
		if !ok {
			handle = frame.Handle
			goto HandleNotFound
		}
		err = link.Transfer(ctx, frame)
		if err != nil {
			return errors.Wrap(err, "transfer failed")
		}

		if doFlow {
			s.mutex.Lock()
			s.incomingWindow = s.initialIncomingWindow
			flowFrame := &v1.Flow{
				NextIncomingID: s.nextIncomingID,
				IncomingWindow: s.incomingWindow,
				NextOutgoingID: s.nextOutgoingID,
				OutgoingWindow: s.outgoingWindow,
				Handle:         v1.HandleNull,
			}
			s.mutex.Unlock()

			err = s.Send(flowFrame)
			if err != nil {
				return errors.Wrap(err, "send session flow failed")
			}
		}

		return nil
	default:
		return errors.Errorf("unexpected frame %T", frame)
	}

HandleNotFound:
	s.state = sessionStateEnding
	return s.Send(&v1.End{
		Error: &v1.Error{
			Condition:   v1.UnattachedHandleSessionError,
			Description: errors.Errorf("link handle %v not found", handle).Error(),
		},
	})
}
