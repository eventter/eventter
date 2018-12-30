package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCmd(t *testing.T) {
	assert := require.New(t)
	assert.NotNil(Cmd())
}
