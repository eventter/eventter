package sasl

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
)

type plainProvider struct {
	directory UserDirectory
}

func NewPLAIN(directory UserDirectory) Provider {
	return &plainProvider{directory: directory}
}

func (p *plainProvider) Mechanism() string {
	return "PLAIN"
}

func (p *plainProvider) Authenticate(ctx context.Context, challenge []byte, response []byte) (token Token, nextChallenge []byte, err error) {
	parts := bytes.Split(response, []byte{0})
	if len(parts) != 3 {
		return nil, nil, errors.Errorf("expected %d parts, got %d parts", 3, len(parts))
	}

	username := string(parts[1])
	password := string(parts[2])

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
