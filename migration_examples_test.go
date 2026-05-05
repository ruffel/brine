package brine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
)

func Example_migrationPartialResults() {
	client, err := brine.New(migrationPartialTransport{})
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), states.SLS(brine.List("minion-1", "minion-2"), "app.deploy"))
	if err != nil {
		var executionError *brine.ExecutionError
		if !errors.As(err, &executionError) {
			panic(err)
		}

		result = executionError.Result
		fmt.Println("failed:", executionError.Failed())
	}

	fmt.Println("returned:", result.Returned())
	// Output:
	// failed: [minion-2]
	// returned: [minion-1 minion-2]
}

func Example_migrationMalformedStateRetry() {
	transport := &migrationRetryTransport{}
	client, err := brine.New(transport, brine.WithMiddleware(brine.WithRetry(brine.RetryConfig{
		MaxAttempts: 2,
		Predicate:   states.MalformedStateRetryPredicate,
	})))
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), states.SLS(brine.Glob("*"), "app.deploy"))
	if err != nil {
		panic(err)
	}

	fmt.Println("ok:", result.OK())
	fmt.Println("attempts:", transport.calls)
	fmt.Println("retry target:", transport.retryTarget)
	// Output:
	// ok: true
	// attempts: 2
	// retry target: [minion-2]
}

type migrationPartialTransport struct {
	brine.UnsupportedTransport
}

func (migrationPartialTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	return &brine.Result{
		Request:  &req,
		Expected: []string{"minion-1", "minion-2"},
		ByMinion: map[string]brine.MinionResult{
			"minion-1": successfulStateMinion("minion-1"),
			"minion-2": failedStateMinion("minion-2"),
		},
	}, nil
}

type migrationRetryTransport struct {
	brine.UnsupportedTransport

	calls       int
	retryTarget brine.Target
}

func (t *migrationRetryTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	t.calls++
	if t.calls == 1 {
		return &brine.Result{
			Request:  &req,
			Expected: []string{"minion-1", "minion-2"},
			ByMinion: map[string]brine.MinionResult{
				"minion-1": successfulStateMinion("minion-1"),
				"minion-2": malformedStateMinion("minion-2"),
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

func failedStateMinion(minion string) brine.MinionResult {
	return brine.MinionResult{
		Minion:  minion,
		RetCode: 1,
		Return:  json.RawMessage(`{"test_|-fail_|-fail_|-fail_without_changes":{"__id__":"fail","name":"fail","result":false,"changes":{},"comment":"Failure!"}}`),
		Failure: &brine.Failure{Kind: brine.FailureUnknown, Message: "state return contains failed state"},
	}
}

func malformedStateMinion(minion string) brine.MinionResult {
	return brine.MinionResult{
		Minion:  minion,
		RetCode: 1,
		Return:  json.RawMessage(`"State lock is held by another process"`),
		Failure: &brine.Failure{Kind: brine.FailureMalformed, Message: "malformed state return"},
	}
}
