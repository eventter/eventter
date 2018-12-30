package sasl

import (
	"context"
	"strings"

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

func (p *plainProvider) Authenticate(ctx context.Context, challenge string, response string) (token Token, nextChallenge string, err error) {
	parts := strings.Split(response, "\000")
	if len(parts) != 3 {
		return nil, "", errors.Errorf("expected %d parts, got %d parts", 3, len(parts))
	}

	ok, err := p.directory.Verify(ctx, parts[1], parts[2])
	if err != nil {
		return nil, "", errors.Wrap(err, "authentication failed")
	}
	if !ok {
		return nil, "", nil
	}
	return &UsernamePasswordToken{
		Username: parts[1],
		Password: parts[2],
	}, "", nil
}
