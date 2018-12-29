package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func bind(channel *amqp.Channel) error {
	err := channel.QueueBind(
		"my-queue",    // queue
		"",            // routing key
		"my-exchange", // exchange
		false,         // no-wait
		nil,           // binding arguments
	)
	if err != nil {
		return errors.Wrap(err, "bind failed")
	}

	return nil
}
