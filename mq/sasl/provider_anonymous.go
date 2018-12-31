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

func (*anonymousProvider) Authenticate(ctx context.Context, challenge []byte, response []byte) (token Token, nextChallenge []byte, err error) {
	return &AnonymousToken{}, nil, nil
}
