package cmd

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"eventter.io/mq/emq"
	"github.com/spf13/cobra"
)

func publishCmd() *cobra.Command {
	request := &emq.PublishRequest{
		Message: &emq.Message{},
	}

	properties := &emq.Message_Properties{}

	cmd := &cobra.Command{
		Use:     "publish <topic> [message1] [message2] ... [messageN]",
		Short:   "Publish message to topic.",
		Aliases: []string{"pub"},
		Args:    cobra.MinimumNArgs(2),
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

			request.Topic.Name = args[0]
			args = args[1:]

			zeroProperties := emq.Message_Properties{}
			if *properties != zeroProperties {
				request.Message.Properties = properties
			}

			for _, arg := range args {
				if strings.HasPrefix(arg, "@") {
					data, err := ioutil.ReadFile(arg[1:])
					if err != nil {
						return err
					}
					request.Message.Data = data
				} else {
					request.Message.Data = []byte(arg)
				}

				response, err := c.Publish(ctx, request)
				if err != nil {
					return err
				}

				encoder := json.NewEncoder(os.Stdout)
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(response); err != nil {
					return err
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&request.Topic.Namespace, "namespace", "n", defaultNamespace, "Topic namespace.")
	cmd.Flags().StringVarP(&request.Message.RoutingKey, "routing-key", "k", "", "Routing key.")
	cmd.Flags().StringVar(&properties.ContentType, "content-type", "", "Content type.")
	cmd.Flags().StringVar(&properties.ContentEncoding, "content-encoding", "", "Content encoding.")
	cmd.Flags().Int32Var(&properties.DeliveryMode, "delivery-mode", 0, "Delivery mode.")
	cmd.Flags().Int32Var(&properties.Priority, "priority", 0, "Delivery mode.")
	cmd.Flags().StringVar(&properties.CorrelationID, "correlation-id", "", "Correlation ID.")
	cmd.Flags().StringVar(&properties.ReplyTo, "reply-to", "", "Reply to.")
	cmd.Flags().StringVar(&properties.Expiration, "expiration", "", "Expiration.")
	cmd.Flags().StringVar(&properties.MessageID, "message-id", "", "Message ID.")
	cmd.Flags().StringVar(&properties.Type, "type", "", "Type.")
	cmd.Flags().StringVar(&properties.UserID, "user-id", "", "User ID.")

	return cmd
}
