package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func publishToQueue(channel *amqp.Channel) error {
	err := channel.Publish(
		"",         // exchange = empty string means default exchange
		"my-queue", // routing key = queue name
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Body: []byte("hello, queue"),
		}, // message
	)

	if err != nil {
		return errors.Wrap(err, "publish failed")
	}

	// ...

	return nil
}
