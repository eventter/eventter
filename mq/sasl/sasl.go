package sasl

import (
	"context"
)

type Provider interface {
	Mechanism() string
	Authenticate(ctx context.Context, challenge string, response string) (token Token, nextChallenge string, err error)
}

type Token interface {
	Subject() string
}

type AnonymousToken struct{}

func (*AnonymousToken) Subject() string {
	return "<anonymous>"
}

func (*AnonymousToken) IsAuthenticated() bool {
	return true
}

type UsernamePasswordToken struct {
	Username string
	Password string
}

func (t *UsernamePasswordToken) Subject() string {
	return t.Username
}

func (t *UsernamePasswordToken) IsAuthenticated() bool {
	return true
}

type UsernamePasswordVerifier func(ctx context.Context, username, password string) (bool, error)
