package amqp

import (
	"context"
	"time"

	"eventter.io/mq/amqp/authentication"
	"github.com/pkg/errors"
)

type contextKeyType int

const contextKey contextKeyType = 0

func Token(ctx context.Context) (authentication.Token, error) {
	switch value := ctx.Value(contextKey).(type) {
	case nil:
		return nil, errors.New("context key not found")
	case *contextValueV0:
		return value.token, nil
	default:
		return nil, errors.Errorf("unhandled value type %T", value)
	}
}

func Heartbeat(ctx context.Context) (time.Duration, error) {
	switch value := ctx.Value(contextKey).(type) {
	case nil:
		return 0, errors.New("context key not found")
	case *contextValueV0:
		return value.heartbeat, nil
	default:
		return 0, errors.Errorf("unhandled value type %T", value)
	}
}

func VirtualHost(ctx context.Context) (string, error) {
	switch value := ctx.Value(contextKey).(type) {
	case nil:
		return "", errors.New("context key not found")
	case *contextValueV0:
		return value.virtualHost, nil
	default:
		return "", errors.Errorf("unhandled value type %T", value)
	}
}
