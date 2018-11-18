package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func listConsumerGroupsCmd() *cobra.Command {
	request := &client.ListConsumerGroupsRequest{}

	cmd := &cobra.Command{
		Use:     "list-consumer-groups",
		Short:   "List consumer groups.",
		Aliases: []string{"cgs"},
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

			response, err := c.ListConsumerGroups(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVar(&request.ConsumerGroup.Namespace, "namespace", "default", "Consumer groups namespace.")
	cmd.Flags().StringVar(&request.ConsumerGroup.Name, "name", "", "Consumer group name.")

	return cmd
}
