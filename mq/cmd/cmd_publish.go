package cmd

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"eventter.io/mq/client"
	"github.com/spf13/cobra"
)

func publishCmd() *cobra.Command {
	request := &client.PublishRequest{
		Message: &client.Message{
			Properties: &client.Message_Properties{},
		},
	}

	cmd := &cobra.Command{
		Use:     "publish",
		Short:   "Publish message to topic.",
		Aliases: []string{"pub"},
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

	cmd.Flags().StringVar(&request.Topic.Namespace, "namespace", "default", "Topic namespace.")
	cmd.Flags().StringVarP(&request.Topic.Name, "name", "n", "", "Topic name.")
	cmd.Flags().StringVarP(&request.Message.RoutingKey, "routing-key", "k", "", "Routing key.")
	cmd.Flags().StringVar(&request.Message.Properties.ContentType, "content-type", "", "Content type.")
	cmd.Flags().StringVar(&request.Message.Properties.ContentEncoding, "content-encoding", "", "Content encoding.")
	cmd.Flags().Int32Var(&request.Message.Properties.DeliveryMode, "delivery-mode", 0, "Delivery mode.")
	cmd.Flags().Int32Var(&request.Message.Properties.Priority, "priority", 0, "Delivery mode.")
	cmd.Flags().StringVar(&request.Message.Properties.CorrelationID, "correlation-id", "", "Correlation ID.")
	cmd.Flags().StringVar(&request.Message.Properties.ReplyTo, "reply-to", "", "Reply to.")
	cmd.Flags().StringVar(&request.Message.Properties.Expiration, "expiration", "", "Expiration.")
	cmd.Flags().StringVar(&request.Message.Properties.MessageID, "message-id", "", "Message ID.")
	cmd.Flags().StringVar(&request.Message.Properties.Type, "type", "", "Type.")
	cmd.Flags().StringVar(&request.Message.Properties.UserID, "user-id", "", "User ID.")

	return cmd
}
