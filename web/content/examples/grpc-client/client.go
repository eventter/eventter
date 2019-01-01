package example

import (
	"context"

	"eventter.io/mq/emq"
	"google.golang.org/grpc"
)

func client() {
	// at the moment the broker doesn't support TLS-encrypted connections,
	// always use grpc.WithInsecure() option

	client, err := emq.Dial("127.0.0.1:16000", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// work with client, e.g.:

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	response, err := client.CreateNamespace(ctx, &emq.NamespaceCreateRequest{
		Namespace: "foo",
	})
	if err != nil {
		panic(err)
	}

	_ = response
}
