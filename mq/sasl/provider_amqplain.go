package sasl

import (
	"context"

	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/structvalue"
	"github.com/pkg/errors"
)

type amqplainProvider struct {
	directory UserDirectory
}

func NewAMQPLAIN(directory UserDirectory) Provider {
	return &amqplainProvider{directory: directory}
}

func (p *amqplainProvider) Mechanism() string {
	return "AMQPLAIN"
}

func (p *amqplainProvider) Authenticate(ctx context.Context, challenge []byte, response []byte) (token Token, nextChallenge []byte, err error) {
	table, err := v0.UnmarshalTable(response)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unmarshal failed")
	}

	username, err := structvalue.String(table, "LOGIN", "")
	if err != nil {
		return nil, nil, errors.Wrap(err, "get LOGIN failed")
	} else if username == "" {
		return nil, nil, errors.New("LOGIN not found / empty")
	}

	password, err := structvalue.String(table, "PASSWORD", "")
	if err != nil {
		return nil, nil, errors.Wrap(err, "get PASSWORD failed")
	} else if password == "" {
		return nil, nil, errors.New("PASSWORD not found / empty")
	}

	ok, err := p.directory.Verify(ctx, username, password)
	if err != nil {
		return nil, nil, errors.Wrap(err, "authentication failed")
	}
	if !ok {
		return nil, nil, nil
	}
	return &UsernamePasswordToken{
		Username: username,
		Password: password,
	}, nil, nil
}
