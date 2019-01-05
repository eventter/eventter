package mq

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/emq"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

const (
	linkStateReady     = 0
	linkStateDetaching = 1
)

type linkAMQPv1 interface {
	State() int
	Credit() (deliveryCount v1.SequenceNo, linkCredit uint32)
	Transfer(ctx context.Context, frame *v1.Transfer) error
	Flow(ctx context.Context, frame *v1.Flow) error
	Close() error
}

type baseLinkAMQPv1 struct {
	state         int
	session       *sessionAMQPv1
	handle        v1.Handle
	role          v1.Role
	deliveryCount v1.SequenceNo
	linkCredit    uint32
	available     uint32
	drain         bool
}

type topicLinkAMQPV1 struct {
	base              baseLinkAMQPv1
	initialLinkCredit uint32
	namespace         string
	name              string
	currentTransfer   *v1.Transfer
	buf               bytes.Buffer
}

func (l *topicLinkAMQPV1) State() int {
	return l.base.state
}

func (l *topicLinkAMQPV1) Credit() (v1.SequenceNo, uint32) {
	return l.base.deliveryCount, l.base.linkCredit
}

func (l *topicLinkAMQPV1) Close() error {
	return nil
}

type consumerGroupLinkAMQPv1 struct {
	base             baseLinkAMQPv1
	subscriptionID   uint64
	subscriptionSize uint32
	ctx              context.Context
	cancel           func()
	mutex            sync.Mutex
	cond             sync.Cond
	buf              bytes.Buffer
}

func (l *consumerGroupLinkAMQPv1) State() int {
	return l.base.state
}

func (l *consumerGroupLinkAMQPv1) Credit() (deliveryCount v1.SequenceNo, linkCredit uint32) {
	l.mutex.Lock()
	deliveryCount = l.base.deliveryCount
	linkCredit = l.base.linkCredit
	l.mutex.Unlock()
	return
}

func (l *consumerGroupLinkAMQPv1) Close() error {
	l.cancel()
	return nil
}

func (l *consumerGroupLinkAMQPv1) Send(response *emq.ConsumerGroupSubscribeResponse) (err error) {
	// serialize message payload
	l.buf.Reset()

	sendProperties := &v1.Properties{}

	if properties := response.Message.Properties; properties != nil {
		sendProperties.ContentType = properties.ContentType
		sendProperties.ContentEncoding = properties.ContentEncoding
		if properties.CorrelationID != "" {
			sendProperties.CorrelationID = v1.MessageIDString(properties.CorrelationID)
		}
		if properties.ReplyTo != "" {
			sendProperties.ReplyTo = v1.AddressString(properties.ReplyTo)
		}
		if properties.Expiration != "" {
			if t, err := time.Parse("2006-01-02T15:04:05.999Z07:00", properties.Expiration); err != nil {
				sendProperties.AbsoluteExpiryTime = t
			}
		}
		if properties.MessageID != "" {
			sendProperties.MessageID = v1.MessageIDString(properties.MessageID)
		}
		sendProperties.CreationTime = properties.Timestamp
		if properties.UserID != "" {
			sendProperties.UserID = []byte(properties.UserID)
		}
		if properties.To != "" {
			sendProperties.To = v1.AddressString(properties.To)
		}
		sendProperties.GroupID = properties.GroupID
		sendProperties.GroupSequence = v1.SequenceNo(properties.GroupSequence)
		sendProperties.ReplyToGroupID = properties.ReplyToGroupID
	}

	sendProperties.Subject = response.Message.RoutingKey

	err = sendProperties.MarshalBuffer(&l.buf)
	if err != nil {
		return errors.Wrap(err, "marshal properties section failed")
	}

	if headers := response.Message.Headers; headers != nil {
		err = (*v1.ApplicationProperties)(headers).MarshalBuffer(&l.buf)
		if err != nil {
			return errors.Wrap(err, "marshal headers section failed")
		}
	}

	err = v1.Data(response.Message.Data).MarshalBuffer(&l.buf)
	if err != nil {
		return errors.Wrap(err, "marshal data section failed")
	}

	// link flow control
	resizeSubscription := false
	var newSubscriptionSize uint32
	l.mutex.Lock()
	for l.base.linkCredit == 0 {
		l.cond.Wait()
	}
	if l.base.linkCredit > l.subscriptionSize {
		resizeSubscription = true
		newSubscriptionSize = l.base.linkCredit
		l.subscriptionSize = l.base.linkCredit
	}
	l.base.deliveryCount++
	l.base.linkCredit--
	l.mutex.Unlock()

	// resize subscription
	if resizeSubscription {
		_, err = l.base.session.connection.server.SubscriptionResize(l.ctx, &SubscriptionResizeRequest{
			NodeID:         response.NodeID,
			SubscriptionID: response.SubscriptionID,
			Size_:          newSubscriptionSize,
		})
		if err != nil {
			return errors.Wrap(err, "resize subscription failed")
		}
	}

	// create next delivery ID
	l.base.session.deliveryID++
	deliveryID := l.base.session.deliveryID

	// transfer
	transfer := &v1.Transfer{
		Handle:        l.base.handle,
		DeliveryID:    deliveryID,
		DeliveryTag:   v1.DeliveryTag(strconv.Itoa(int(deliveryID))),
		MessageFormat: 0,
		Settled:       true, // FIXME
		More:          true,
	}

	approxPayloadMax := int(l.base.session.connection.transport.GetFrameMax()) - (8 + // frame header
		1 + 1 + 8 + 1 + 4 + // descriptor and list header
		1 + 4 + // handle
		1 + 4 + // delivery-id
		1 + 1 + len(transfer.DeliveryTag) + // delivery-tag
		1 + 4 + // message-format
		1 + // settled
		1 + // more
		0)

	for l.buf.Len() > 0 {
		transfer.FrameMeta.Payload = l.buf.Next(approxPayloadMax)
		transfer.More = l.buf.Len() > 0

		// session flow control
		l.base.session.mutex.Lock()
		for l.base.session.remoteIncomingWindow == 0 {
			l.base.session.cond.Wait()
		}
		l.base.session.nextOutgoingID++
		l.base.session.outgoingWindow--
		l.base.session.remoteIncomingWindow--
		doFlow := l.base.session.outgoingWindow <= l.base.session.initialOutgoingWindow/2
		l.base.session.mutex.Unlock()

		err = l.base.session.Send(transfer)
		if err != nil {
			return errors.Wrap(err, "send transfer failed")
		}
		transfer.DeliveryTag = nil

		if doFlow {
			l.base.session.mutex.Lock()
			l.base.session.outgoingWindow = l.base.session.initialIncomingWindow
			flowFrame := &v1.Flow{
				NextIncomingID: l.base.session.nextIncomingID,
				IncomingWindow: l.base.session.incomingWindow,
				NextOutgoingID: l.base.session.nextOutgoingID,
				OutgoingWindow: l.base.session.outgoingWindow,
				Handle:         v1.HandleNull,
			}
			l.base.session.mutex.Unlock()

			err = l.base.session.Send(flowFrame)
			if err != nil {
				return errors.Wrap(err, "send flow failed")
			}
		}
	}

	return nil
}

func (l *consumerGroupLinkAMQPv1) SetHeader(metadata.MD) error {
	panic("local")
}

func (l *consumerGroupLinkAMQPv1) SendHeader(metadata.MD) error {
	panic("local")
}

func (l *consumerGroupLinkAMQPv1) SetTrailer(metadata.MD) {
	panic("local")
}

func (l *consumerGroupLinkAMQPv1) Context() context.Context {
	return l.ctx
}

func (l *consumerGroupLinkAMQPv1) SendMsg(m interface{}) error {
	panic("local")
}

func (l *consumerGroupLinkAMQPv1) RecvMsg(m interface{}) error {
	panic("local")
}
