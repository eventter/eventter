package mq

import (
	"context"

	"eventter.io/mq/client"
)

type clientRPCServer struct {
}

func NewClientRPCServer() client.ClientRPCServer {
	return &clientRPCServer{}
}

func (*clientRPCServer) CreateTopic(context.Context, *client.CreateTopicRequest) (*client.OkResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) ListTopics(context.Context, *client.ListTopicsRequest) (*client.ListTopicsResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) DeleteTopic(context.Context, *client.DeleteTopicRequest) (*client.OkResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) CreateConsumerGroup(context.Context, *client.CreateConsumerGroupRequest) (*client.OkResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) ListConsumerGroups(context.Context, *client.ListConsumerGroupsRequest) (*client.ListConsumerGroupsResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) DeleteConsumerGroup(context.Context, *client.DeleteConsumerGroupRequest) (*client.OkResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) Publish(context.Context, *client.PublishRequest) (*client.PublishResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) Consume(client.ClientRPC_ConsumeServer) error {
	panic("implement me")
}
