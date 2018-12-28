package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func listTopicsCmd() *cobra.Command {
	request := &emq.ListTopicsRequest{}

	cmd := &cobra.Command{
		Use:     "list-topics [name]",
		Short:   "List topics.",
		Aliases: []string{"topics", "tps"},
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

			if len(args) > 1 {
				request.Topic.Name = args[0]
			}
			response, err := c.ListTopics(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.Topic.Namespace, "namespace", "n", defaultNamespace, "Topics namespace.")

	return cmd
}
