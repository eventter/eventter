package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func connect() error {
	conn, err := amqp.DialConfig("amqp://127.0.0.1:16000", amqp.Config{
		Vhost: "default",
	})
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer conn.Close()

	// ... work with connection ...

	return nil
}
