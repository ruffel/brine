package mock_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transports/mock"
)

func TestScriptLocalSuccess(t *testing.T) {
	t.Parallel()

	transport := mock.ScriptLocalSuccess("minion-1", "minion-2")

	client, err := brine.New(transport)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	result, err := client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if !result.OK() {
		t.Fatal("result should be OK")
	}

	if got := result.Returned(); len(got) != 2 || got[0] != "minion-1" || got[1] != "minion-2" {
		t.Fatalf("unexpected returned minions: %#v", got)
	}

	calls := transport.Calls()
	if len(calls) != 1 || calls[0].Operation != "run" {
		t.Fatalf("unexpected calls: %#v", calls)
	}

	transport.AssertExpectations(t)
}

func TestExpectLocalSuccess(t *testing.T) {
	t.Parallel()

	transport := mock.ExpectLocalSuccess("test.echo", brine.List("minion-1"), map[string]any{"minion-1": "hello"})

	client, err := brine.New(transport)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	result, err := client.Run(context.Background(), brine.Local("test.echo", brine.List("minion-1")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	var decoded string
	if err := result.ByMinion["minion-1"].Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded != "hello" {
		t.Fatalf("unexpected decoded value: %q", decoded)
	}

	transport.AssertExpectations(t)
}

func TestScriptExecutionError(t *testing.T) {
	t.Parallel()

	client, err := brine.New(mock.ScriptExecutionError("minion-1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	result, err := client.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1")))
	if err == nil {
		t.Fatal("expected execution error")
	}

	var executionError *brine.ExecutionError
	if !errors.As(err, &executionError) {
		t.Fatalf("expected ExecutionError, got %T", err)
	}

	if result == nil || result.OK() {
		t.Fatalf("unexpected result: %#v", result)
	}

	if got := executionError.Failed(); len(got) != 1 || got[0] != "minion-1" {
		t.Fatalf("unexpected failed minions: %#v", got)
	}
}

func TestStream(t *testing.T) {
	t.Parallel()

	stream := mock.NewStream(brine.NewEvent(brine.EventRawSalt, brine.RawSaltPayload{Tag: "salt/test"}))

	event, err := stream.Recv(context.Background())
	if err != nil {
		t.Fatalf("recv: %v", err)
	}

	if event.Type != brine.EventRawSalt {
		t.Fatalf("unexpected event type: %s", event.Type)
	}

	_, err = stream.Recv(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}
