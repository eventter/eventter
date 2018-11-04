package main

import (
	"context"
	"fmt"
	"time"

	"eventter.io/mq"
	"eventter.io/mq/client"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func debugCmd() *cobra.Command {
	request := &client.ListTopicsRequest{}

	cmd := &cobra.Command{
		Use:     "debug",
		Short:   "Dump node debug info.",
		Aliases: []string{"dump"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if rootConfig.BindHost == "" {
				rootConfig.BindHost = "localhost"
			}

			ctx, _ := context.WithTimeout(context.Background(), 1*time.Minute)
			conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", rootConfig.BindHost, rootConfig.Port), grpc.WithInsecure())
			if err != nil {
				return err
			}
			defer conn.Close()

			c := mq.NewNodeRPCClient(conn)

			response, err := c.Debug(ctx, &mq.DebugRequest{})
			if err != nil {
				return err
			}

			fmt.Println("=== Cluster state ===")
			fmt.Println(response.ClusterState)

			fmt.Print("\n")

			fmt.Println("=== Segments ===")
			for _, dump := range response.Segments {
				fmt.Println(dump)
			}

			fmt.Print("\n")

			return nil
		},
	}

	cmd.Flags().StringVar(&request.Topic.Namespace, "namespace", "default", "Topics namespace.")
	cmd.Flags().StringVar(&request.Topic.Name, "name", "", "Topic name.")

	return cmd
}
