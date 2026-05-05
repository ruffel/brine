package brine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithRetryRetriesSelectedMinionsAndPreservesSuccess(t *testing.T) {
	t.Parallel()

	transport := &scriptedRetryTransport{}
	client, err := New(transport, WithMiddleware(WithRetry(RetryConfig{
		MaxAttempts: 2,
		Predicate: func(req Request, result MinionResult) bool {
			return req.Kind == KindLocal && req.Function == "state.sls" && result.Minion == "minion-2"
		},
	})))
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Local("state.sls", Glob("*"), Args("brine.test")))
	require.NoError(t, err)
	require.True(t, result.OK(), "result should be OK after retry")
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
	assert.Equal(t, 2, transport.calls)

	target, ok := transport.retryTarget.(ListTarget)
	require.True(t, ok, "retry target should be a list target")
	assert.Equal(t, ListTarget{"minion-2"}, target)
}

type scriptedRetryTransport struct {
	UnsupportedTransport
	calls       int
	retryTarget Target
}

func (t *scriptedRetryTransport) Run(_ context.Context, req Request) (*Result, error) {
	t.calls++
	if t.calls == 1 {
		return &Result{
			Request:  &req,
			Expected: []string{"minion-1", "minion-2"},
			ByMinion: map[string]MinionResult{
				"minion-1": successfulRetryMinion("minion-1"),
				"minion-2": {
					Minion:  "minion-2",
					RetCode: 1,
					Return:  json.RawMessage(`"malformed state return"`),
					Failure: &Failure{Kind: FailureMalformed, Message: "malformed state return"},
				},
			},
		}, nil
	}

	t.retryTarget = req.Target
	return &Result{
		Request:  &req,
		Expected: []string{"minion-2"},
		ByMinion: map[string]MinionResult{
			"minion-2": successfulRetryMinion("minion-2"),
		},
	}, nil
}

func successfulRetryMinion(minion string) MinionResult {
	return MinionResult{
		Minion: minion,
		Return: json.RawMessage(`{"ok":true}`),
	}
}
