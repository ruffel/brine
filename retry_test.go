package brine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithRetryRetriesSelectedMinionsAndPreservesSuccess(t *testing.T) {
	t.Parallel()

	transport := &scriptedRetryTransport{}
	events := make([]Event, 0)
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 2,
			Predicate: func(req Request, result MinionResult) bool {
				return req.Kind == KindLocal && req.Function == "state.sls" && result.Minion == "minion-2"
			},
			Backoff: func(int) time.Duration { return time.Millisecond },
		})),
		WithObserver(ObserverFunc(func(_ context.Context, event Event) { events = append(events, event) })),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Local("state.sls", Glob("*"), Args("brine.test")))
	require.NoError(t, err)
	require.True(t, result.OK(), "result should be OK after retry")
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
	assert.Equal(t, 2, transport.calls)

	target, ok := transport.retryTarget.(ListTarget)
	require.True(t, ok, "retry target should be a list target")
	assert.Equal(t, ListTarget{"minion-2"}, target)
	assertRetryEvent(t, events, EventRetryScheduled, "minion-2", 2, time.Millisecond)
	assertRetryEvent(t, events, EventRetryStarted, "minion-2", 2, 0)
}

func TestWithRetryReportsExecutionErrorWhenRetriesExhausted(t *testing.T) {
	t.Parallel()

	transport := &exhaustedRetryTransport{}
	events := make([]Event, 0)
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 3,
			Predicate: func(req Request, result MinionResult) bool {
				return req.Kind == KindLocal && result.Minion == "minion-2"
			},
		})),
		WithObserver(ObserverFunc(func(_ context.Context, event Event) { events = append(events, event) })),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Local("state.sls", Glob("*"), Args("brine.test")))
	require.Error(t, err)

	var executionError *ExecutionError
	require.ErrorAs(t, err, &executionError)
	require.NotNil(t, result)
	assert.False(t, result.OK())
	assert.Equal(t, []string{"minion-2"}, executionError.Failed())
	assert.Equal(t, 3, transport.calls)
	assertRetryEvent(t, events, EventRetryExhausted, "minion-2", 3, 0)
}

type scriptedRetryTransport struct {
	UnsupportedTransport

	calls       int
	retryTarget Target
}

type exhaustedRetryTransport struct {
	UnsupportedTransport

	calls int
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

func (t *exhaustedRetryTransport) Run(_ context.Context, req Request) (*Result, error) {
	t.calls++

	return &Result{
		Request:  &req,
		Expected: []string{"minion-2"},
		ByMinion: map[string]MinionResult{
			"minion-2": failedRetryMinion("minion-2"),
		},
	}, nil
}

func successfulRetryMinion(minion string) MinionResult {
	return MinionResult{
		Minion: minion,
		Return: json.RawMessage(`{"ok":true}`),
	}
}

func failedRetryMinion(minion string) MinionResult {
	return MinionResult{
		Minion:  minion,
		RetCode: 1,
		Return:  json.RawMessage(`"malformed state return"`),
		Failure: &Failure{Kind: FailureMalformed, Message: "malformed state return"},
	}
}

func assertRetryEvent(t *testing.T, events []Event, eventType EventType, minion string, attempt int, delay time.Duration) {
	t.Helper()

	for _, event := range events {
		if event.Type != eventType {
			continue
		}

		payload, ok := event.Payload.(RetryPayload)
		if !ok || payload.Minion != minion || payload.Attempt != attempt {
			continue
		}

		assert.Equal(t, delay, payload.Delay)

		return
	}

	t.Fatalf("missing retry event %s for %s attempt %d in %#v", eventType, minion, attempt, events)
}
