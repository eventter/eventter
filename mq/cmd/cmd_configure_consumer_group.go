package cmd

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func configureConsumerGroupCmd() *cobra.Command {
	request := &client.ConfigureConsumerGroupRequest{}
	var bindings []string

	cmd := &cobra.Command{
		Use:     "configure-consumer-group <name>",
		Short:   "Configure consumer group.",
		Aliases: []string{"cg"},
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

			for _, binding := range bindings {
				parts := strings.SplitN(binding, ":", 2)
				request.Bindings = append(request.Bindings, &client.ConfigureConsumerGroupRequest_Binding{
					TopicName: parts[0],
					By: &client.ConfigureConsumerGroupRequest_Binding_RoutingKey{
						RoutingKey: parts[1],
					},
				})
			}

			request.ConsumerGroup.Name = args[0]

			response, err := c.ConfigureConsumerGroup(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.ConsumerGroup.Namespace, "namespace", "n", defaultNamespace, "Consumer group namespace.")
	cmd.Flags().StringSliceVarP(&bindings, "bind", "b", nil, "Bindings in form of <topic>:<routing key>.")
	cmd.Flags().Uint32VarP(&request.Size_, "size", "s", 0, "Max count of in-flight messages. Zero means that the server chooses sensible defaults.")

	return cmd
}
