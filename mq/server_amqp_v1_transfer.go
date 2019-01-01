package mq

import (
	"context"
	"fmt"

	"eventter.io/mq/amqp/v1"
	"github.com/pkg/errors"
)

func (l *linkAMQPv1) Transfer(ctx context.Context, frame *v1.Transfer) (err error) {
	l.session.nextIncomingID += 1
	l.session.incomingWindow -= 1
	l.session.remoteOutgoingWindow -= 1

	if l.role != v1.ReceiverRole {
		l.state = linkStateDetaching
		return l.session.Send(&v1.Detach{
			Handle: l.handle,
			Closed: true,
			Error: &v1.Error{
				Condition:   v1.DetachForcedLinkError,
				Description: "not receiving endpoint of the link",
			},
		})
	}

	if l.currentTransfer != nil {
		if l.currentTransfer.DeliveryID != frame.DeliveryID {
			l.state = linkStateDetaching
			return l.session.Send(&v1.Detach{
				Handle: l.handle,
				Closed: true,
				Error: &v1.Error{
					Condition:   v1.DetachForcedLinkError,
					Description: "mixed delivery ids",
				},
			})
		}
	} else {
		if l.linkCredit == 0 {
			l.state = linkStateDetaching
			return l.session.Send(&v1.Detach{
				Handle: l.handle,
				Closed: true,
				Error: &v1.Error{
					Condition:   v1.TransferLimitExceededLinkError,
					Description: "link credit zero",
				},
			})
		}
		if l.session.incomingWindow == 0 {
			l.session.state = sessionStateEnding
			return l.session.Send(&v1.End{
				Error: &v1.Error{
					Condition:   v1.WindowViolationSessionError,
					Description: "incoming window zero",
				},
			})
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

	fmt.Printf("buf = %q = %v [len=%d]\n", l.buf.Bytes(), l.buf.Bytes(), len(l.buf.Bytes()))

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
}
