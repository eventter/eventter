package main

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
		Message: client.Message{
			Properties: &client.Message_Properties{},
		},
	}

	cmd := &cobra.Command{
		Use:     "publish",
		Short:   "Publish message to topic.",
		Aliases: []string{"pub", "p"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if rootConfig.BindHost == "" {
				rootConfig.BindHost = "localhost"
			}

			ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
			c, err := newClient(ctx)
			if err != nil {
				return err
			}

			if strings.HasPrefix(args[0], "@") {
				data, err := ioutil.ReadFile(args[0][1:])
				if err != nil {
					return err
				}
				request.Message.Data = data
			} else {
				request.Message.Data = []byte(args[0])
			}

			response, err := c.Publish(ctx, request)
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "  ")
			return encoder.Encode(response)
		},
	}

	cmd.Flags().StringVar(&request.Namespace, "namespace", "default", "Topic namespace.")
	cmd.Flags().StringVarP(&request.Name, "name", "n", "", "Topic name.")
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

	cmd.Args = cobra.ExactArgs(1)

	return cmd
}
