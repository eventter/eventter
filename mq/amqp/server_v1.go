package amqp

import (
	"context"

	"eventter.io/mq/amqp/v1"
	"eventter.io/mq/sasl"
)

type HandlerV1 interface {
	ServeAMQPv1(ctx context.Context, transport *v1.Transport) error // TODO: should serve accept connection / session / link?
}

func NewContextV1(parent context.Context, token sasl.Token) context.Context {
	return context.WithValue(parent, contextKey, &contextValueV0{
		token: token,
	})
}

type contextValueV1 struct {
	token sasl.Token
}
