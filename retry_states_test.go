package brine_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
)

func TestWithRetryUsesMalformedStateRetryPredicate(t *testing.T) {
	t.Parallel()

	transport := &stateRetryTransport{}
	client, err := brine.New(transport, brine.WithMiddleware(brine.WithRetry(brine.RetryConfig{
		MaxAttempts: 2,
		Predicate:   states.MalformedStateRetryPredicate,
	})))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	result, err := client.Run(context.Background(), states.SLS(brine.Glob("*"), "brine.success"))
	if err != nil {
		t.Fatalf("run with retry: %v", err)
	}

	if !result.OK() {
		t.Fatalf("result should be OK after malformed state retry: %#v", result)
	}

	assertTestStrings(t, result.Returned(), []string{"minion-1", "minion-2"})
	if transport.calls != 2 {
		t.Fatalf("calls = %d, want 2", transport.calls)
	}

	target, ok := transport.retryTarget.(brine.ListTarget)
	if !ok || len(target) != 1 || target[0] != "minion-2" {
		t.Fatalf("retry target = %#v, want list target for minion-2", transport.retryTarget)
	}
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

func assertTestStrings(t *testing.T, got []string, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("values = %#v, want %#v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("values = %#v, want %#v", got, want)
		}
	}
}
