package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func queue(channel *amqp.Channel) error {
	queue, err := channel.QueueDeclare(
		"my-queue", // name
		true,       // durable
		false,      // auto-delete
		false,      // exclusive
		false,      // no-wait
		amqp.Table{
			"size": 1024,
		}, // arguments
	)
	if err != nil {
		return errors.Wrap(err, "queue declare failed")
	}

	// ... work with queue ...

	_ = queue

	return nil
}
