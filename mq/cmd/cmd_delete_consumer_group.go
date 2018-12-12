package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func deleteConsumerGroupCmd() *cobra.Command {
	request := &client.DeleteConsumerGroupRequest{}

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

			request.ConsumerGroup.Name = args[0]
			response, err := c.DeleteConsumerGroup(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.ConsumerGroup.Namespace, "namespace", "n", defaultNamespace, "Consumer group namespace.")

	return cmd
}
