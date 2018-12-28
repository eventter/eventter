package emq

import (
	"context"

	"google.golang.org/grpc"
)

type Client interface {
	EventterMQClient
	Close() error
}

type client struct {
	EventterMQClient
	conn *grpc.ClientConn
}

func Dial(target string, opts ...grpc.DialOption) (Client, error) {
	return DialContext(context.Background(), target, opts...)
}

func DialContext(ctx context.Context, target string, opts ...grpc.DialOption) (Client, error) {
	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, err
	}
	return &client{
		EventterMQClient: NewEventterMQClient(conn),
		conn:             conn,
	}, nil
}

func (c *client) Close() error {
	return c.conn.Close()
}
