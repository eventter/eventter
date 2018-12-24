package authentication

import (
	"strings"

	"github.com/pkg/errors"
)

type plainProvider struct {
	verify UsernamePasswordVerifier
}

func NewPLAIN(verify UsernamePasswordVerifier) Provider {
	return &plainProvider{verify: verify}
}

func (p *plainProvider) Mechanism() string {
	return "PLAIN"
}

func (p *plainProvider) Authenticate(challenge string, response string) (tok Token, nextChallenge string, err error) {
	parts := strings.Split(response, "\000")
	if len(parts) != 3 {
		return nil, "", errors.Errorf("expected %d parts, got %d parts", 3, len(parts))
	}

	ok, err := p.verify(parts[1], parts[2])
	if err != nil {
		return nil, "", errors.Wrap(err, "authentication failed")
	}
	if !ok {
		return &NotAuthenticatedToken{Username: parts[1]}, "", nil
	}
	return &UsernamePasswordToken{
		Username: parts[1],
		Password: parts[2],
	}, "", nil
}
