package example

import (
	"github.com/pkg/errors"
	"pack.ag/amqp"
)

func connectSASL() error {
	client, err := amqp.Dial("amqp://127.0.0.1:16001", amqp.ConnSASLPlain("user", "pass"))
	if err != nil {
		return errors.Wrap(err, "dial failed")
	}
	defer client.Close()

	// ... work with the client ...

	return nil
}
