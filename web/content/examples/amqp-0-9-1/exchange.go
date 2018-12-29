package example

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func exchange(channel *amqp.Channel) error {
	err := channel.ExchangeDeclare(
		"my-exchange", // name
		"fanout",      // type
		true,          // durable
		false,         // auto-delete
		false,         // internal
		false,         // no-wait
		amqp.Table{
			"shards":             1,
			"replication-factor": 3,
			"retention":          "24h",
		}, // arguments
	)
	if err != nil {
		return errors.Wrap(err, "exchange declare failed")
	}

	// ... work with exchange (i.e. bind queues to it, publish messages to it) ...

	return nil
}
