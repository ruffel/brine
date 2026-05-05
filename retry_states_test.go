package brine_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithRetryUsesMalformedStateRetryPredicate(t *testing.T) {
	t.Parallel()

	transport := &stateRetryTransport{}
	client, err := brine.New(transport, brine.WithMiddleware(brine.WithRetry(brine.RetryConfig{
		MaxAttempts: 2,
		Predicate:   states.MalformedStateRetryPredicate,
	})))
	require.NoError(t, err)

	result, err := client.Run(context.Background(), states.SLS(brine.Glob("*"), "brine.success"))
	require.NoError(t, err)
	require.True(t, result.OK(), "result should be OK after malformed state retry")
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
	assert.Equal(t, 2, transport.calls)

	target, ok := transport.retryTarget.(brine.ListTarget)
	require.True(t, ok, "retry target should be a list target")
	assert.Equal(t, brine.ListTarget{"minion-2"}, target)
}

type stateRetryTransport struct {
	brine.UnsupportedTransport
	calls       int
	retryTarget brine.Target
}

func (t *stateRetryTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	t.calls++
	if t.calls == 1 {
		return &brine.Result{
			Request:  &req,
			Expected: []string{"minion-1", "minion-2"},
			ByMinion: map[string]brine.MinionResult{
				"minion-1": successfulStateMinion("minion-1"),
				"minion-2": {
					Minion:  "minion-2",
					RetCode: 1,
					Return:  json.RawMessage(`"State lock is held by another process"`),
					Failure: &brine.Failure{Kind: brine.FailureMalformed, Message: "malformed state return"},
				},
			},
		}, nil
	}

	t.retryTarget = req.Target
	return &brine.Result{
		Request:  &req,
		Expected: []string{"minion-2"},
		ByMinion: map[string]brine.MinionResult{
			"minion-2": successfulStateMinion("minion-2"),
		},
	}, nil
}

func successfulStateMinion(minion string) brine.MinionResult {
	return brine.MinionResult{
		Minion: minion,
		Return: json.RawMessage(`{"test_|-ok_|-ok_|-succeed_without_changes":{"__id__":"ok","name":"ok","result":true,"changes":{},"comment":"Success!"}}`),
	}
}
