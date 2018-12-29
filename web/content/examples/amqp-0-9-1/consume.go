package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func consume(channel *amqp.Channel) error {
	deliveries, err := channel.Consume(
		"my-queue", // queue
		"",         // consumer tag
		false,      // no-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return errors.Wrap(err, "consume failed")
	}

	for delivery := range deliveries {
		// ... process message ...

		err := delivery.Ack(false)
		if err != nil {
			return errors.Wrap(err, "ack failed")
		}
	}

	return nil
}
