package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"time"

	"eventter.io/mq/client"
	"github.com/pkg/errors"
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

			ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
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

				switch body := in.Body.(type) {
				case *client.ConsumeResponse_Delivery_:
					encoder.Encode(body.Delivery)

					if !request.NoAck {
						err = stream.Send(&client.ConsumeRequest{
							Body: &client.ConsumeRequest_Ack_{
								Ack: &client.ConsumeRequest_Ack{
									DeliveryTag: body.Delivery.DeliveryTag,
								},
							},
						})
						if err != nil {
							return err
						}
					}
				default:
					return errors.Errorf("unhandled type: %T", in.Body)
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
	cmd.Flags().StringVarP(&request.ConsumerTag, "consumer-tag", "t", hex.EncodeToString(buf), "Consumer tag.")
	cmd.Flags().BoolVar(&request.NoAck, "no-ack", false, "No ack.")
	cmd.Flags().BoolVar(&request.Exclusive, "exclusive", false, "Exclusive.")

	return cmd
}
