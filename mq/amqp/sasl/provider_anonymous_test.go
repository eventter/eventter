package sasl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnonymousProvider_Authenticate(t *testing.T) {
	assert := require.New(t)

	provider := NewANONYMOUS()

	token, challenge, err := provider.Authenticate("", "")
	assert.NoError(err)
	assert.Empty(challenge)
	assert.NotNil(token)

	assert.Equal(&AnonymousToken{}, token)
}
