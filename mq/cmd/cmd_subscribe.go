package cmd

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func subscribeCmd() *cobra.Command {
	request := &client.SubscribeRequest{}

	cmd := &cobra.Command{
		Use:     "subscribe",
		Short:   "Consume messages from consumer group.",
		Aliases: []string{"sub"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if rootConfig.BindHost == "" {
				rootConfig.BindHost = "localhost"
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			c, err := newClient(ctx)
			if err != nil {
				return err
			}
			defer c.Close()

			stream, err := c.Subscribe(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")

			for {
				response, err := stream.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}

				encoder.Encode(response)

				if !request.NoAck {
					_, err = c.Ack(ctx, &client.AckRequest{
						ConsumerGroup:  request.ConsumerGroup,
						SubscriptionID: response.SubscriptionID,
						DeliveryTag:    response.DeliveryTag,
					})
					if err != nil {
						return err
					}
				}
			}

			return nil
		},
	}

	rand.Seed(time.Now().UnixNano())
	buf := make([]byte, 16)
	rand.Read(buf)

	cmd.Flags().StringVar(&request.ConsumerGroup.Namespace, "namespace", "default", "Consumer group namespace.")
	cmd.Flags().StringVarP(&request.ConsumerGroup.Name, "name", "n", "", "Consumer group name.")
	cmd.Flags().BoolVar(&request.NoAck, "no-ack", false, "No ack.")
	cmd.Flags().BoolVar(&request.Exclusive, "exclusive", false, "Exclusive.")

	return cmd
}
