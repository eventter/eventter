package mq

import (
	"context"

	"eventter.io/mq/client"
)

type clientRPCServer struct {
}

func NewClientRPCServer() client.EventterMQServer {
	return &clientRPCServer{}
}

func (*clientRPCServer) ConfigureTopic(context.Context, *client.ConfigureTopicRequest) (*client.ConfigureTopicResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) ListTopics(context.Context, *client.ListTopicsRequest) (*client.ListTopicsResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) DeleteTopic(context.Context, *client.DeleteTopicRequest) (*client.DeleteTopicResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) ConfigureConsumerGroup(context.Context, *client.ConfigureConsumerGroupRequest) (*client.ConfigureConsumerGroupResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) ListConsumerGroups(context.Context, *client.ListConsumerGroupsRequest) (*client.ListConsumerGroupsResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) DeleteConsumerGroup(context.Context, *client.DeleteConsumerGroupRequest) (*client.DeleteConsumerGroupResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) Publish(context.Context, *client.PublishRequest) (*client.PublishResponse, error) {
	panic("implement me")
}

func (*clientRPCServer) Consume(client.EventterMQ_ConsumeServer) error {
	panic("implement me")
}
