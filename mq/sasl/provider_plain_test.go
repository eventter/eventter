package sasl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlainProvider_Authenticate_Success(t *testing.T) {
	assert := require.New(t)

	provider := NewPLAIN(&allowAllDirectory{})

	token, challenge, err := provider.Authenticate(context.Background(), nil, []byte("\000user\000pass"))
	assert.NoError(err)
	assert.Empty(challenge)
	assert.NotNil(token)

	assert.Equal(&UsernamePasswordToken{
		Username: "user",
		Password: "pass",
	}, token)
}

func TestPlainProvider_Authenticate_BadResponse(t *testing.T) {
	assert := require.New(t)

	provider := NewPLAIN(&allowAllDirectory{})

	token, challenge, err := provider.Authenticate(context.Background(), nil, nil)
	assert.Error(err)
	assert.Empty(challenge)
	assert.Nil(token)
}

func TestPlainProvider_Authenticate_NotVerified(t *testing.T) {
	assert := require.New(t)

	provider := NewPLAIN(&denyAllDirectory{})

	token, challenge, err := provider.Authenticate(context.Background(), nil, []byte("\000user\000pass"))
	assert.NoError(err)
	assert.Empty(challenge)
	assert.Nil(token)
}
