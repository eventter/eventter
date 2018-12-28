package cmd

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"eventter.io/mq/emq"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func createConsumerGroupCmd() *cobra.Command {
	request := &emq.CreateConsumerGroupRequest{}
	var fanoutBindings, directBindings, topicBindings []string
	var since time.Duration

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

			if since != 0 {
				request.ConsumerGroup.Since = time.Now().Add(since)
			}

			for _, binding := range fanoutBindings {
				binding := &emq.ConsumerGroup_Binding{
					TopicName:    binding,
					ExchangeType: emq.ExchangeTypeFanout,
				}
				request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, binding)
			}

			for exchangeType, bindings := range map[string][]string{emq.ExchangeTypeDirect: directBindings, emq.ExchangeTypeTopic: topicBindings} {
				for i, binding := range bindings {
					parts := strings.SplitN(binding, ":", 2)
					if len(parts) != 2 {
						return errors.Errorf("%s binding %d does not contain colon", exchangeType, i)
					}
					binding := &emq.ConsumerGroup_Binding{
						TopicName:    parts[0],
						ExchangeType: exchangeType,
						By:           &emq.ConsumerGroup_Binding_RoutingKey{RoutingKey: parts[1]},
					}
					request.ConsumerGroup.Bindings = append(request.ConsumerGroup.Bindings, binding)
				}
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
	cmd.Flags().StringSliceVarP(&fanoutBindings, "bind", "b", nil, "Fanout bindings in form of <topic>.")
	cmd.Flags().StringSliceVarP(&directBindings, "bind-direct", "d", nil, "Direct bindings in form of <topic>:<routing key>.")
	cmd.Flags().StringSliceVarP(&topicBindings, "bind-topic", "t", nil, "Topic bindings in form of <topic>:<routing key>.")
	cmd.Flags().Uint32VarP(&request.ConsumerGroup.Size_, "size", "s", 0, "Max count of in-flight messages. Zero means that the server chooses sensible defaults.")
	cmd.Flags().DurationVarP(&since, "since", "f", 0, "Time from which to consider messages eligible to be consumed by this consumer group.")

	return cmd
}
