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

func TestRunRecoversPanicAsTransportError(t *testing.T) {
	t.Parallel()

	panicMW := func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, req Request) (*Result, error) {
			panic("middleware exploded")
		})
	}

	transport := &baseHandlerTransport{}
	client, err := New(transport, WithMiddleware(panicMW))
	require.NoError(t, err)

	_, runErr := client.Run(context.Background(), Local("test.ping", Glob("*")))
	require.Error(t, runErr)
	assert.ErrorIs(t, runErr, ErrTransport, "panic should be wrapped as ErrTransport")

	var transportErr *TransportError
	require.ErrorAs(t, runErr, &transportErr)
	assert.Equal(t, "panic", transportErr.Op)
	assert.Contains(t, transportErr.Error(), "middleware exploded")
}

type baseHandlerTransport struct {
	UnsupportedTransport
}

func (t *baseHandlerTransport) Run(context.Context, Request) (*Result, error) {
	return &Result{}, nil
}
