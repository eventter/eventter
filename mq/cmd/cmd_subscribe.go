package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func subscribeCmd() *cobra.Command {
	request := &emq.SubscribeRequest{}

	cmd := &cobra.Command{
		Use:     "subscribe <consumer-group>",
		Short:   "Consume messages from consumer group.",
		Aliases: []string{"sub"},
		Args:    cobra.ExactArgs(1),
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

			request.ConsumerGroup.Name = args[0]
			stream, err := c.Subscribe(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxUint32))
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

				if err := encoder.Encode(response); err != nil {
					return err
				}
				fmt.Println(string(response.Message.Data))

				_, err = c.Ack(ctx, &emq.AckRequest{
					NodeID:         response.NodeID,
					SubscriptionID: response.SubscriptionID,
					SeqNo:          response.SeqNo,
				})
				if err != nil {
					return err
				}
			}

			return nil
		},
	}

	rand.Seed(time.Now().UnixNano())
	buf := make([]byte, 16)
	rand.Read(buf)

	cmd.Flags().StringVarP(&request.ConsumerGroup.Namespace, "namespace", "n", emq.DefaultNamespace, "Consumer group namespace.")

	return cmd
}
