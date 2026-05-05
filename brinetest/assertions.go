package brinetest

import (
	"context"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	defaultRunTimeout   = 90 * time.Second
	defaultAsyncTimeout = 120 * time.Second
)

func contractContext(t *testing.T, timeout time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()

	return context.WithTimeout(context.Background(), timeout)
}

func assertReturnedMinions(t *testing.T, result *brine.Result, want []string) {
	t.Helper()
	require.NotNil(t, result)
	assert.Equal(t, want, result.Returned())
}

func requireExecutionError(t *testing.T, err error) *brine.ExecutionError {
	t.Helper()

	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	require.NotNil(t, executionError.Result)

	return executionError
}

func requireStateName(t *testing.T, name string) string {
	t.Helper()
	if name == "" {
		t.Fatalf("brinetest: missing required state name")
	}

	return name
}
