package about

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	assert := require.New(t)
	assert.Equal("EventterMQ", Name)
}
