package sasl

import (
	"eventter.io/mq/amqp/v0"
	"eventter.io/mq/structvalue"
	"github.com/pkg/errors"
)

type amqplainProvider struct {
	verify UsernamePasswordVerifier
}

func NewAMQPLAIN(verify UsernamePasswordVerifier) Provider {
	return &amqplainProvider{verify: verify}
}

func (p *amqplainProvider) Mechanism() string {
	return "AMQPLAIN"
}

func (p *amqplainProvider) Authenticate(challenge string, response string) (token Token, nextChallenge string, err error) {
	table, err := v0.UnmarshalTable([]byte(response))
	if err != nil {
		return nil, "", errors.Wrap(err, "unmarshal failed")
	}

	username, err := structvalue.String(table, "LOGIN", "")
	if err != nil {
		return nil, "", errors.Wrap(err, "get LOGIN failed")
	} else if username == "" {
		return nil, "", errors.New("LOGIN not found / empty")
	}

	password, err := structvalue.String(table, "PASSWORD", "")
	if err != nil {
		return nil, "", errors.Wrap(err, "get PASSWORD failed")
	} else if password == "" {
		return nil, "", errors.New("PASSWORD not found / empty")
	}

	ok, err := p.verify(username, password)
	if err != nil {
		return nil, "", errors.Wrap(err, "authentication failed")
	}
	if !ok {
		return nil, "", nil
	}
	return &UsernamePasswordToken{
		Username: username,
		Password: password,
	}, "", nil
}
