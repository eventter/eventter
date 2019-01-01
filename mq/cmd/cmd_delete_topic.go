package cmd

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func deleteTopicCmd() *cobra.Command {
	request := &emq.TopicDeleteRequest{}

	cmd := &cobra.Command{
		Use:   "delete-topic <name>",
		Short: "Delete topic.",
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
			response, err := c.DeleteTopic(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.Namespace, "namespace", "n", emq.DefaultNamespace, "Topic namespace.")
	cmd.Flags().BoolVar(&request.IfUnused, "if-unused", false, "If unused.")

	return cmd
}
