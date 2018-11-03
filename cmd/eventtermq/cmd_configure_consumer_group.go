package main

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
		Use:     "configure-consumer-group",
		Short:   "Configure consumer group.",
		Aliases: []string{"cg"},
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

			for _, binding := range bindings {
				parts := strings.SplitN(binding, ":", 2)
				request.Bindings = append(request.Bindings, &client.ConfigureConsumerGroupRequest_Binding{
					Topic: client.NamespaceName{
						Namespace: request.ConsumerGroup.Namespace,
						Name:      parts[0],
					},
					RoutingKey: parts[1],
				})
			}

			response, err := c.ConfigureConsumerGroup(ctx, request)
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
	cmd.Flags().StringSliceVarP(&bindings, "bind", "b", nil, "Bindings in form of <topic>:<routing key>.")

	return cmd
}
