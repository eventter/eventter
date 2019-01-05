package example

import (
	"context"

	"github.com/pkg/errors"
	"pack.ag/amqp"
)

func sender(ctx context.Context) error {
	client, err := amqp.Dial("amqp://127.0.0.1:16001")
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return errors.Wrap(err, "session failed")
	}

	sender, err := session.NewSender(amqp.LinkTargetAddress("/my-ns/my-topic"))
	if err != nil {
		return errors.Wrap(err, "sender failed")
	}

	err = sender.Send(ctx, &amqp.Message{
		Data: [][]byte{
			[]byte("hello, world"),
		},
	})
	if err != nil {
		return errors.Wrap(err, "send failed")
	}

	return nil
}
