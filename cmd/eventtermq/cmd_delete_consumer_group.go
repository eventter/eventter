package main

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
		Use:   "delete-consumer-group",
		Short: "Delete consumer group.",
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

			response, err := c.DeleteConsumerGroup(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVar(&request.ConsumerGroup.Namespace, "namespace", "default", "Consumer group namespace.")
	cmd.Flags().StringVarP(&request.ConsumerGroup.Name, "name", "n", "", "Consumer group name.")
	cmd.Flags().BoolVar(&request.IfUnused, "if-unused", false, "If unused.")
	cmd.Flags().BoolVar(&request.IfEmpty, "if-empty", false, "If empty.")

	return cmd
}
