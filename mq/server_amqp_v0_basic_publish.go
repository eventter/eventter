package mq

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/emq"
	"github.com/pkg/errors"
)

func (s *Server) handleAMQPv0BasicPublish(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.BasicPublish) error {
	state := s.clusterState.Current()
	namespace, _ := state.FindNamespace(namespaceName)
	if namespace == nil {
		return s.makeChannelClose(ch, v0.NotFound, errors.Errorf("vhost %q not found", namespaceName))
	}

	ch.publishExchange = frame.Exchange
	ch.publishRoutingKey = frame.RoutingKey
	ch.state = channelStateAwaitingHeader
	return nil
}

func (s *Server) handleAMQPv0ChannelContentHeader(ctx context.Context, transport *v0.Transport, namespace string, ch *serverAMQPv0Channel, frame *v0.ContentHeaderFrame) error {
	if ch.state == channelStateClosing {
		return nil
	} else if ch.state != channelStateAwaitingHeader {
		return s.makeConnectionClose(v0.UnexpectedFrame, errors.New("expected header frame"))
	}

	ch.publishRemaining = int(frame.BodySize)

	if frame.ContentType != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.ContentType = frame.ContentType
	}
	if frame.ContentEncoding != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.ContentEncoding = frame.ContentEncoding
	}
	ch.publishHeaders = frame.Headers
	if frame.DeliveryMode != 0 {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.DeliveryMode = int32(frame.DeliveryMode)
	}
	if frame.Priority != 0 {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.Priority = int32(frame.Priority)
	}
	if frame.CorrelationID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.CorrelationID = frame.CorrelationID
	}
	if frame.ReplyTo != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.ReplyTo = frame.ReplyTo
	}
	if frame.Expiration != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.Expiration = frame.Expiration
	}
	if frame.MessageID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.MessageID = frame.MessageID
	}
	if !frame.Timestamp.IsZero() {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.Timestamp = frame.Timestamp
	}
	if frame.Type != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.Type = frame.Type
	}
	if frame.UserID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.UserID = frame.UserID
	}
	if frame.AppID != "" {
		if ch.publishProperties == nil {
			ch.publishProperties = &emq.Message_Properties{}
		}
		ch.publishProperties.AppID = frame.AppID
	}

	ch.state = channelStateAwaitingBody

	return nil
}

func (s *Server) handleAMQPv0ChannelContentBody(ctx context.Context, transport *v0.Transport, namespaceName string, ch *serverAMQPv0Channel, frame *v0.ContentBodyFrame) error {
	if ch.state == channelStateClosing {
		return nil
	} else if ch.state != channelStateAwaitingBody {
		return s.makeConnectionClose(v0.UnexpectedFrame, errors.New("expected body frame"))
	}

	if len(frame.Data) > ch.publishRemaining {
		return s.makeConnectionClose(v0.FrameError, errors.Errorf("body remaining %d, got %d", ch.publishRemaining, len(frame.Data)))
	}

	ch.publishData = append(ch.publishData, frame.Data...)
	ch.publishRemaining -= len(frame.Data)

	if ch.publishRemaining > 0 {
		return nil
	}

	_, err := s.Publish(ctx, &emq.PublishRequest{
		Topic: emq.NamespaceName{
			Namespace: namespaceName,
			Name:      ch.publishExchange,
		},
		Message: &emq.Message{
			RoutingKey: ch.publishRoutingKey,
			Properties: ch.publishProperties,
			Headers:    ch.publishHeaders,
			Data:       ch.publishData,
		},
	})
	if err != nil {
		return s.makeConnectionClose(v0.InternalError, errors.Wrap(err, "publish failed"))
	}

	ch.ResetPublish()
	ch.state = channelStateReady

	return nil
}
