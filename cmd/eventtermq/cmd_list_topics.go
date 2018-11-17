package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func listTopicsCmd() *cobra.Command {
	request := &client.ListTopicsRequest{}

	cmd := &cobra.Command{
		Use:     "list-topics",
		Short:   "List topics.",
		Aliases: []string{"topics", "tps"},
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

			response, err := c.ListTopics(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVar(&request.Topic.Namespace, "namespace", "default", "Topics namespace.")
	cmd.Flags().StringVar(&request.Topic.Name, "name", "", "Topic name.")

	return cmd
}
