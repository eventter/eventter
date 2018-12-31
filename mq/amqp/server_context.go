package amqp

import (
	"context"

	"eventter.io/mq/sasl"
	"github.com/pkg/errors"
)

type contextKeyType int

const (
	tokenContextKey contextKeyType = 0
)

func NewServerContext(parent context.Context, token sasl.Token) context.Context {
	return context.WithValue(parent, tokenContextKey, token)
}

func TokenFromContext(ctx context.Context) (sasl.Token, error) {
	token, ok := ctx.Value(tokenContextKey).(sasl.Token)
	if !ok {
		return nil, errors.New("context key not found")
	}
	return token, nil
}
