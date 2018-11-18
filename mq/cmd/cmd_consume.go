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

func consumeCmd() *cobra.Command {
	request := &client.ConsumeRequest_Request{}

	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume messages from consumer group.",
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

			stream, err := c.Consume(ctx)
			if err != nil {
				return err
			}

			err = stream.Send(&client.ConsumeRequest{
				Body: &client.ConsumeRequest_Request_{
					Request: request,
				},
			})
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")

			for {
				in, err := stream.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}

				encoder.Encode(in)

				if !request.NoAck {
					err = stream.Send(&client.ConsumeRequest{
						Body: &client.ConsumeRequest_Ack_{
							Ack: &client.ConsumeRequest_Ack{
								DeliveryTag: in.DeliveryTag,
							},
						},
					})
					if err != nil {
						return err
					}
				}
			}

			return stream.CloseSend()
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
