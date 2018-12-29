package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func publishToExchange(channel *amqp.Channel) error {
	err := channel.Publish(
		"my-exchange", // exchange
		"",            // routing key = because exchange has type fanout, all messages are passed to bound queues
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			Body: []byte("hello, exchange"),
		}, // message
	)

	if err != nil {
		return errors.Wrap(err, "publish failed")
	}

	// ...

	return nil
}
