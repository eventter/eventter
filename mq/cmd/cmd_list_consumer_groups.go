package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func listConsumerGroupsCmd() *cobra.Command {
	request := &emq.ListConsumerGroupsRequest{}

	cmd := &cobra.Command{
		Use:     "list-consumer-groups [name]",
		Short:   "List consumer groups.",
		Aliases: []string{"cgs"},
		Args:    cobra.MaximumNArgs(1),
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

			if len(args) > 0 {
				request.ConsumerGroup.Name = args[0]
			}
			response, err := c.ListConsumerGroups(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.ConsumerGroup.Namespace, "namespace", "n", emq.DefaultNamespace, "Consumer groups namespace.")

	return cmd
}
