package brine

import (
	"context"
	"encoding/json"
	"testing"
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
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	result, err := client.Run(context.Background(), Local("state.sls", Glob("*"), Args("brine.test")))
	if err != nil {
		t.Fatalf("run with retry: %v", err)
	}

	if !result.OK() {
		t.Fatalf("result should be OK after retry: %#v", result)
	}

	returned := result.Returned()
	assertRetryStrings(t, returned, []string{"minion-1", "minion-2"})

	if transport.calls != 2 {
		t.Fatalf("calls = %d, want 2", transport.calls)
	}

	if target, ok := transport.retryTarget.(ListTarget); !ok || len(target) != 1 || target[0] != "minion-2" {
		t.Fatalf("retry target = %#v, want list target for minion-2", transport.retryTarget)
	}
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

func assertRetryStrings(t *testing.T, got []string, want []string) {
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
