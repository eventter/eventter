package cmd

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"math/rand"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func subscribeCmd() *cobra.Command {
	request := &emq.ConsumerGroupSubscribeRequest{
		AutoAck: true,
	}

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
				return errors.Wrap(err, "dial failed")
			}
			defer c.Close()

			request.Name = args[0]
			stream, err := c.Subscribe(ctx, request, grpc.MaxCallRecvMsgSize(math.MaxUint32))
			if err != nil {
				return errors.Wrap(err, "subscribe failed")
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")

			for {
				response, err := stream.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					return errors.Wrap(err, "receive failed")
				}

				if err := encoder.Encode(response); err != nil {
					return errors.Wrap(err, "encode failed")
				}
			}

			return nil
		},
	}

	rand.Seed(time.Now().UnixNano())
	buf := make([]byte, 16)
	rand.Read(buf)

	cmd.Flags().StringVarP(&request.Namespace, "namespace", "n", emq.DefaultNamespace, "Consumer group namespace.")
	cmd.Flags().Uint32VarP(&request.Size_, "size", "s", 0, "Max number of messages in-flight. Zero means there is no limit.")
	cmd.Flags().BoolVar(&request.DoNotBlock, "do-not-block", false, "Do not block if there are no messages to be consumed.")
	cmd.Flags().Uint64VarP(&request.MaxMessages, "max-messages", "m", 0, "Max number of messages to be consumed. After this number of messages was consumed (i.e. received and (n)acked), stream will be closed.")

	return cmd
}
