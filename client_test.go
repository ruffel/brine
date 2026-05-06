package brine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseHandlerReturnsTransport(t *testing.T) {
	t.Parallel()

	transport := &baseHandlerTransport{}
	client, err := New(transport)
	require.NoError(t, err)

	assert.Equal(t, Handler(transport), client.BaseHandler())
	assert.Equal(t, client.BaseHandler(), client.Unwrap())
}

type baseHandlerTransport struct {
	UnsupportedTransport
}

func (t *baseHandlerTransport) Run(context.Context, Request) (*Result, error) {
	return &Result{}, nil
}
