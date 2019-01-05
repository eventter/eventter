package example

import (
	"context"

	"github.com/pkg/errors"
	"pack.ag/amqp"
)

func receiver(ctx context.Context) error {
	client, err := amqp.Dial("amqp://127.0.0.1:16001")
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return errors.Wrap(err, "session failed")
	}

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress("/my-ns/my-cg"),
		amqp.LinkCredit(1),
		amqp.LinkSenderSettle(amqp.ModeUnsettled), // or amqp.LinkSenderSettle(amqp.ModeSettled)
	)
	if err != nil {
		return errors.Wrap(err, "receiver failed")
	}

	for {
		message, err := receiver.Receive(ctx)
		if err != nil {
			return errors.Wrap(err, "receive failed")
		}

		// ... work with message ...

		err = message.Accept()
		if err != nil {
			return errors.Wrap(err, "accept failed")
		}

		// or message.Release()
	}
}
