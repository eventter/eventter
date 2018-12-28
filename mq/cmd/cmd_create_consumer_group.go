package cmd

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func createConsumerGroupCmd() *cobra.Command {
	request := &emq.CreateConsumerGroupRequest{}
	var bindings []string

	cmd := &cobra.Command{
		Use:     "create-consumer-group <name>",
		Short:   "Create/update consumer group.",
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
				binding := &emq.ConsumerGroup_Binding{
					TopicName: parts[0],
				}
				if len(parts) > 1 {
					binding.By = &emq.ConsumerGroup_Binding_RoutingKey{
						RoutingKey: parts[1],
					}
				}
				request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, binding)
			}

			request.ConsumerGroup.Name.Name = args[0]

			response, err := c.CreateConsumerGroup(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVarP(&request.ConsumerGroup.Name.Namespace, "namespace", "n", emq.DefaultNamespace, "Consumer group namespace.")
	cmd.Flags().StringSliceVarP(&bindings, "bind", "b", nil, "Bindings in form of <topic>:<routing key>.")
	cmd.Flags().Uint32VarP(&request.ConsumerGroup.Size_, "size", "s", 0, "Max count of in-flight messages. Zero means that the server chooses sensible defaults.")

	return cmd
}
