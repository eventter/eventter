package main

import (
	"pack.ag/amqp"
)

func main() {
	client, err := amqp.Dial("amqp://127.0.0.1:16001", amqp.ConnSASLPlain("guest", "guest"))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		panic(err)
	}

	_ = session
}
