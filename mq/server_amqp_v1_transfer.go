package mq

import (
	"context"
	"fmt"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
)

func (l *linkAMQPv1) Transfer(ctx context.Context, frame *v1.Transfer) (err error) {
	var detachCondition v1.ErrorCondition
	var request *emq.TopicPublishRequest

	if frame.MessageFormat != 0 {
		detachCondition = v1.DetachForcedLinkError
		err = errors.Errorf("message format 0x%02x not implemented", frame.MessageFormat)
		goto Detach
	}

	if l.role != v1.ReceiverRole {
		detachCondition = v1.DetachForcedLinkError
		err = errors.New("not receiving endpoint of the link")
		goto Detach
	}

	if l.currentTransfer != nil {
		if l.currentTransfer.DeliveryID != frame.DeliveryID {
			detachCondition = v1.DetachForcedLinkError
			err = errors.New("mixed delivery ids")
			goto Detach
		}
	} else {
		if l.linkCredit == 0 {
			detachCondition = v1.TransferLimitExceededLinkError
			err = errors.New("link credit zero")
			goto Detach
		}
		if l.session.incomingWindow == 0 {
			detachCondition = v1.WindowViolationSessionError
			err = errors.New("incoming window zero")
			goto Detach
		}

		l.deliveryCount += 1
		l.linkCredit -= 1

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
		Name:      l.topic,
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
			request.Message.Properties = properties

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
			}
			if section.UserID != nil {
				properties.UserID = string(section.UserID)
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
			}
			properties.ContentType = section.ContentType
			properties.ContentEncoding = section.ContentType
			if !section.AbsoluteExpiryTime.IsZero() {
				properties.Expiration = section.AbsoluteExpiryTime.Format("2006-01-02T15:04:05.999Z07:00")
			}
			if !section.CreationTime.IsZero() {
				properties.Timestamp = section.CreationTime
			}
			properties.GroupID = section.GroupID
			properties.GroupSequence = uint32(section.GroupSequence)
			properties.ReplyToGroupID = section.ReplyToGroupID

		case *v1.ApplicationProperties:
			request.Message.Headers = (*types.Struct)(section)
		case v1.Data:
			request.Message.Data = []byte(section)
		default:
			// other sections ignored
		}
	}

	_, err = l.session.connection.server.Publish(ctx, request)
	if err != nil {
		detachCondition = v1.InternalErrorAMQPError
		err = errors.Wrap(err, "publish failed")
		goto Detach
	}

	if !l.currentTransfer.Settled {
		err = l.session.Send(&v1.Disposition{
			Role:    l.role,
			First:   l.currentTransfer.DeliveryID,
			Settled: true,
			State:   &v1.Accepted{},
		})
		if err != nil {
			return errors.Wrap(err, "send disposition failed")
		}
	}

	if l.linkCredit <= l.initialLinkCredit/2 {
		l.linkCredit = l.initialLinkCredit
		err = l.session.Send(&v1.Flow{
			NextIncomingID: l.session.nextIncomingID,
			IncomingWindow: l.session.incomingWindow,
			NextOutgoingID: l.session.nextOutgoingID,
			OutgoingWindow: l.session.outgoingWindow,
			Handle:         frame.Handle,
			DeliveryCount:  l.deliveryCount,
			LinkCredit:     l.linkCredit,
		})
		if err != nil {
			return errors.Wrap(err, "send link flow failed")
		}
	}

	l.currentTransfer = nil
	l.buf.Reset()

	return nil

Detach:
	l.state = linkStateDetaching
	return l.session.Send(&v1.Detach{
		Handle: l.handle,
		Closed: true,
		Error: &v1.Error{
			Condition:   detachCondition,
			Description: err.Error(),
		},
	})
}
