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

func TestWithRetryBypassesNonLocalRequests(t *testing.T) {
	t.Parallel()

	transport := &exhaustedRetryTransport{}
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 3,
			Predicate:   func(Request, MinionResult) bool { return true },
		})),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Runner("manage.alived"))
	require.NoError(t, err)
	assert.Equal(t, 1, transport.calls, "runner request should not trigger retry")
	assert.NotNil(t, result)
}

func TestWithRetryBypassesNilPredicate(t *testing.T) {
	t.Parallel()

	transport := &exhaustedRetryTransport{}
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 3,
			Predicate:   nil,
		})),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Local("state.sls", Glob("*")))
	// The transport returns a failed minion, so ExecutionError is expected.
	// The key assertion is that retry was bypassed (1 call, not 3).
	_ = err
	assert.Equal(t, 1, transport.calls, "nil predicate should bypass retry")
	assert.NotNil(t, result)
}

func TestWithRetryBypassesMaxAttemptsOne(t *testing.T) {
	t.Parallel()

	transport := &exhaustedRetryTransport{}
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 1,
			Predicate:   func(Request, MinionResult) bool { return true },
		})),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Local("state.sls", Glob("*")))
	_ = err
	assert.Equal(t, 1, transport.calls, "MaxAttempts=1 should bypass retry")
	assert.NotNil(t, result)
}

func TestWithRetryContextCancellationMidRetry(t *testing.T) {
	t.Parallel()

	transport := &exhaustedRetryTransport{}
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 5,
			Predicate:   func(_ Request, ret MinionResult) bool { return ret.Minion == "minion-2" },
			Backoff:     func(int) time.Duration { return 100 * time.Millisecond },
		})),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = client.Run(ctx, Local("state.sls", Glob("*")))
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, transport.calls, "should not reach retry after cancellation")
}

func TestWithRetryBackoffCalledWithCorrectAttempt(t *testing.T) {
	t.Parallel()

	transport := &exhaustedRetryTransport{}
	var attempts []int
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 4,
			Predicate:   func(_ Request, ret MinionResult) bool { return ret.Minion == "minion-2" },
			Backoff: func(attempt int) time.Duration {
				attempts = append(attempts, attempt)

				return 0
			},
		})),
	)
	require.NoError(t, err)

	_, _ = client.Run(context.Background(), Local("state.sls", Glob("*")))
	assert.Equal(t, []int{2, 3, 4}, attempts, "backoff should receive attempt 2, 3, 4")
}

func TestWithRetryClearsStaleResultFailure(t *testing.T) {
	t.Parallel()

	// Transport returns a result-level Failure on first attempt, then
	// succeeds for all minions on retry.
	transport := &staleFailureTransport{}
	client, err := New(transport,
		WithMiddleware(WithRetry(RetryConfig{
			MaxAttempts: 2,
			Predicate:   func(_ Request, ret MinionResult) bool { return ret.Failure != nil },
		})),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), Local("test.ping", Glob("*")))
	require.NoError(t, err, "stale Failure should be cleared after successful retry")
	require.NotNil(t, result)
	assert.True(t, result.OK())
	assert.Nil(t, result.Failure)
}

func TestCloneResultIsolatesFailurePointer(t *testing.T) {
	t.Parallel()

	original := &Result{
		Request: &Request{Kind: KindLocal},
		Failure: &Failure{Kind: FailureNoReturn, Message: "original", Raw: json.RawMessage(`"raw"`)},
		ByMinion: map[string]MinionResult{
			"minion-1": {
				Minion:  "minion-1",
				Failure: &Failure{Kind: FailureRetCode, Message: "retcode 1", Raw: json.RawMessage(`"m1"`)},
				Return:  json.RawMessage(`"fail"`),
			},
		},
	}

	cloned := cloneResult(original)

	// Mutate cloned failures.
	cloned.Failure.Message = "mutated"
	cloned.ByMinion["minion-1"] = MinionResult{
		Minion:  "minion-1",
		Failure: &Failure{Kind: FailureUnknown, Message: "replaced"},
	}

	assert.Equal(t, "original", original.Failure.Message, "original result-level Failure should not be mutated")
	assert.Equal(t, "retcode 1", original.ByMinion["minion-1"].Failure.Message, "original minion Failure should not be mutated")
}

type staleFailureTransport struct {
	UnsupportedTransport

	calls int
}

func (t *staleFailureTransport) Run(_ context.Context, req Request) (*Result, error) {
	t.calls++
	if t.calls == 1 {
		result := &Result{
			Request:  &req,
			Failure:  &Failure{Kind: FailureNoReturn, Message: "no minion returned"},
			Expected: []string{"minion-1"},
			ByMinion: map[string]MinionResult{
				"minion-1": failedRetryMinion("minion-1"),
			},
		}

		return result, NewExecutionError(result, nil)
	}

	return &Result{
		Request:  &req,
		Expected: []string{"minion-1"},
		ByMinion: map[string]MinionResult{
			"minion-1": successfulRetryMinion("minion-1"),
		},
	}, nil
}
