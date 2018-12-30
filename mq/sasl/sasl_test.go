package sasl

import (
	"context"
)

type allowAllDirectory struct{}

func (*allowAllDirectory) Verify(ctx context.Context, username, password string) (bool, error) {
	return true, nil
}

type denyAllDirectory struct{}

func (*denyAllDirectory) Verify(ctx context.Context, username, password string) (bool, error) {
	return false, nil
}
