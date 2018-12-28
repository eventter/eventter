package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func createTopicCmd() *cobra.Command {
	request := &emq.CreateTopicRequest{}

	cmd := &cobra.Command{
		Use:     "create-topic <name>",
		Short:   "Create/update topic.",
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

			request.Topic.Name.Name = args[0]
			response, err := c.CreateTopic(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.Topic.Name.Namespace, "namespace", "n", emq.DefaultNamespace, "Topic namespace.")
	cmd.Flags().StringVarP(&request.Topic.DefaultExchangeType, "type", "t", emq.ExchangeTypeFanout, "Topic type.")
	cmd.Flags().Uint32VarP(&request.Topic.Shards, "shards", "s", 1, "# of shards.")
	cmd.Flags().Uint32VarP(&request.Topic.ReplicationFactor, "replication-factor", "f", 0, "Replication factor.")
	cmd.Flags().DurationVarP(&request.Topic.Retention, "retention", "r", 0, "Topic retention.")

	return cmd
}
