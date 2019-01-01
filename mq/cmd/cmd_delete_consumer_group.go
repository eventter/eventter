package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func deleteConsumerGroupCmd() *cobra.Command {
	request := &emq.ConsumerGroupDeleteRequest{}

	cmd := &cobra.Command{
		Use:   "delete-consumer-group <name>",
		Short: "Delete consumer group.",
		Args:  cobra.ExactArgs(1),
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

			request.Name = args[0]
			response, err := c.DeleteConsumerGroup(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.Namespace, "namespace", "n", emq.DefaultNamespace, "Consumer group namespace.")

	return cmd
}
