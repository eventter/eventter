package sasl

import (
	"context"
)

type anonymousProvider struct {
}

func NewANONYMOUS() Provider {
	return &anonymousProvider{}
}

func (*anonymousProvider) Mechanism() string {
	return "ANONYMOUS"
}

func (*anonymousProvider) Authenticate(ctx context.Context, challenge string, response string) (token Token, nextChallenge string, err error) {
	return &AnonymousToken{}, "", nil
}
