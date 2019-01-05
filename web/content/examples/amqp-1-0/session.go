package example

import (
	"github.com/pkg/errors"
	"pack.ag/amqp"
)

func session() error {
	client, err := amqp.Dial("amqp://127.0.0.1:16001")
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer client.Close()

	session, err := client.NewSession(
		amqp.SessionIncomingWindow(1024),
		amqp.SessionOutgoingWindow(1024),
	)
	if err != nil {
		return errors.Wrap(err, "session failed")
	}

	// ... work with session ...

	_ = session

	return nil
}
