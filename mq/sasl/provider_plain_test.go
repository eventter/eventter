package sasl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlainProvider_Authenticate_Success(t *testing.T) {
	assert := require.New(t)

	provider := NewPLAIN(func(ctx context.Context, username, password string) (ok bool, err error) {
		return true, nil
	})

	token, challenge, err := provider.Authenticate(context.Background(), "", "\000user\000pass")
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

	provider := NewPLAIN(func(ctx context.Context, username, password string) (ok bool, err error) {
		return true, nil
	})

	token, challenge, err := provider.Authenticate(context.Background(), "", "")
	assert.Error(err)
	assert.Empty(challenge)
	assert.Nil(token)
}

func TestPlainProvider_Authenticate_NotVerified(t *testing.T) {
	assert := require.New(t)

	provider := NewPLAIN(func(ctx context.Context, username, password string) (ok bool, err error) {
		return false, nil
	})

	token, challenge, err := provider.Authenticate(context.Background(), "", "\000user\000pass")
	assert.NoError(err)
	assert.Empty(challenge)
	assert.Nil(token)
}
