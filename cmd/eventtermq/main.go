package main

import (
	"context"
	"fmt"
	"os"

	"eventter.io/mq"
	"eventter.io/mq/client"
	"google.golang.org/grpc"
)

var rootConfig = &mq.Config{}

func newClient(ctx context.Context) (client.Client, error) {
	return client.DialContext(ctx, fmt.Sprintf("%s:%d", rootConfig.BindHost, rootConfig.Port), grpc.WithInsecure())
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
