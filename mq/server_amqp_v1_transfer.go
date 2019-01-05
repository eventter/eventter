package mq

import (
	"context"
	"fmt"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (l *topicLinkAMQPV1) Transfer(ctx context.Context, frame *v1.Transfer) (err error) {
	var detachCondition v1.ErrorCondition
	var request *emq.TopicPublishRequest

	if frame.MessageFormat != 0 {
		detachCondition = v1.DetachForcedLinkError
		err = errors.Errorf("message format 0x%02x not implemented", frame.MessageFormat)
		goto Detach
	}

	if l.currentTransfer != nil {
		if l.currentTransfer.DeliveryID != frame.DeliveryID {
			detachCondition = v1.DetachForcedLinkError
			err = errors.New("mixed delivery ids")
			goto Detach
		}
	} else {
		if l.base.linkCredit == 0 {
			detachCondition = v1.TransferLimitExceededLinkError
			err = errors.New("link credit zero")
			goto Detach
		}
		if l.base.session.incomingWindow == 0 {
			detachCondition = v1.WindowViolationSessionError
			err = errors.New("incoming window zero")
			goto Detach
		}

		l.base.deliveryCount += 1
		l.base.linkCredit -= 1

		l.currentTransfer = frame
		l.buf.Reset()
	}

	if frame.Aborted {
		l.currentTransfer = nil
		l.buf.Reset()
		return nil
	}

	l.buf.Write(frame.FrameMeta.Payload)
	if frame.More {
		return nil
	}

	request = &emq.TopicPublishRequest{
		Namespace: l.namespace,
		Name:      l.name,
		Message:   &emq.Message{},
	}

	for l.buf.Len() > 0 {
		var section v1.Section
		err = v1.UnmarshalSection(&section, &l.buf)
		if err != nil {
			detachCondition = v1.DecodeErrorAMQPError
			err = errors.Wrap(err, "malformed message")
			goto Detach
		}

		switch section := section.(type) {
		case *v1.Properties:
			properties := &emq.Message_Properties{}
			count := 0

			if section.MessageID != nil {
				switch messageID := section.MessageID.(type) {
				case v1.MessageIDBinary:
					properties.MessageID = string(messageID)
				case v1.MessageIDString:
					properties.MessageID = string(messageID)
				case v1.MessageIDUUID:
					properties.MessageID = v1.UUID(messageID).String()
				case v1.MessageIDUlong:
					properties.MessageID = fmt.Sprintf("%d", messageID)
				default:
					detachCondition = v1.DecodeErrorAMQPError
					err = errors.Errorf("unhandled message-id type %T", messageID)
					goto Detach
				}
				count++
			}
			if section.UserID != nil {
				properties.UserID = string(section.UserID)
				count++
			}
			if section.To != nil {
				switch address := section.To.(type) {
				case v1.AddressString:
					properties.To = string(address)
				default:
					detachCondition = v1.DecodeErrorAMQPError
					err = errors.Errorf("unhandled to type %T", address)
					goto Detach
				}
				count++
			}
			request.Message.RoutingKey = section.Subject
			if section.ReplyTo != nil {
				switch address := section.ReplyTo.(type) {
				case v1.AddressString:
					properties.ReplyTo = string(address)
				default:
					detachCondition = v1.DecodeErrorAMQPError
					err = errors.Errorf("unhandled reply-to type %t", address)
				}
				count++
			}
			if section.CorrelationID != nil {
				switch correlationID := section.CorrelationID.(type) {
				case v1.MessageIDBinary:
					properties.CorrelationID = string(correlationID)
				case v1.MessageIDString:
					properties.CorrelationID = string(correlationID)
				case v1.MessageIDUUID:
					properties.CorrelationID = v1.UUID(correlationID).String()
				case v1.MessageIDUlong:
					properties.CorrelationID = fmt.Sprintf("%d", correlationID)
				default:
					detachCondition = v1.DecodeErrorAMQPError
					err = errors.Errorf("unhandled correlation-id type %T", correlationID)
					goto Detach
				}
				count++
			}
			if section.ContentType != "" {
				properties.ContentType = section.ContentType
				count++
			}
			if section.ContentEncoding != "" {
				properties.ContentEncoding = section.ContentEncoding
				count++
			}
			if !section.AbsoluteExpiryTime.IsZero() {
				properties.Expiration = section.AbsoluteExpiryTime.Format("2006-01-02T15:04:05.999Z07:00")
				count++
			}
			if !section.CreationTime.IsZero() {
				properties.Timestamp = section.CreationTime
				count++
			}
			if section.GroupID != "" {
				properties.GroupID = section.GroupID
				count++
			}
			if section.GroupSequence != 0 {
				properties.GroupSequence = uint32(section.GroupSequence)
				count++
			}
			if section.ReplyToGroupID != "" {
				properties.ReplyToGroupID = section.ReplyToGroupID
				count++
			}

			if count > 0 {
				request.Message.Properties = properties
			}

		case *v1.ApplicationProperties:
			request.Message.Headers = (*types.Struct)(section)
		case v1.Data:
			request.Message.Data = []byte(section)
		default:
			// other sections ignored
		}
	}

	_, err = l.base.session.connection.server.Publish(ctx, request)
	if err != nil {
		detachCondition = v1.InternalErrorAMQPError
		err = errors.Wrap(err, "publish failed")
		goto Detach
	}

	if !l.currentTransfer.Settled {
		err = l.base.session.Send(&v1.Disposition{
			Role:    l.base.role,
			First:   l.currentTransfer.DeliveryID,
			Settled: true,
			State:   &v1.Accepted{},
		})
		if err != nil {
			return errors.Wrap(err, "send disposition failed")
		}
	}

	if l.base.linkCredit <= l.initialLinkCredit/2 {
		l.base.linkCredit = l.initialLinkCredit

		l.base.session.mutex.Lock()
		flowFrame := &v1.Flow{
			NextIncomingID: l.base.session.nextIncomingID,
			IncomingWindow: l.base.session.incomingWindow,
			NextOutgoingID: l.base.session.nextOutgoingID,
			OutgoingWindow: l.base.session.outgoingWindow,
			Handle:         frame.Handle,
			DeliveryCount:  l.base.deliveryCount,
			LinkCredit:     l.base.linkCredit,
		}
		l.base.session.mutex.Unlock()

		err = l.base.session.Send(flowFrame)
		if err != nil {
			return errors.Wrap(err, "send link flow failed")
		}
	}

	l.currentTransfer = nil
	l.buf.Reset()

	return nil

Detach:
	l.base.state = linkStateDetaching
	return l.base.session.Send(&v1.Detach{
		Handle: l.base.handle,
		Closed: true,
		Error: &v1.Error{
			Condition:   detachCondition,
			Description: err.Error(),
		},
	})
}

func (l *consumerGroupLinkAMQPv1) Transfer(ctx context.Context, frame *v1.Transfer) error {
	return errors.New("did not expect transfer on consumer group link")
}
