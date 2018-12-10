package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func configureTopicCmd() *cobra.Command {
	request := &client.ConfigureTopicRequest{}

	cmd := &cobra.Command{
		Use:     "configure-topic <name>",
		Short:   "Configure topic.",
		Aliases: []string{"topic", "tp"},
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

			request.Topic.Name = args[0]
			response, err := c.ConfigureTopic(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.Topic.Namespace, "namespace", "n", defaultNamespace, "Topic namespace.")
	cmd.Flags().StringVarP(&request.Type, "type", "t", "direct", "Topic type.")
	cmd.Flags().Uint32VarP(&request.Shards, "shards", "s", 1, "# of shards.")
	cmd.Flags().Uint32VarP(&request.ReplicationFactor, "replication-factor", "f", 0, "Replication factor.")
	cmd.Flags().DurationVarP(&request.Retention, "retention", "r", 0, "Topic retention.")

	return cmd
}
