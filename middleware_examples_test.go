package brine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
	"github.com/ruffel/brine/transports/mock"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const exampleJID = "20240101000000000000"

func staticPillarMiddleware(pillar map[string]any) brine.Middleware {
	return func(next brine.Handler) brine.Handler {
		return brine.HandlerFunc(func(ctx context.Context, req brine.Request) (*brine.Result, error) {
			brine.PillarData(pillar)(&req)

			return next.Run(ctx, req)
		})
	}
}

func targetTransformMiddleware(transform func(brine.Target) brine.Target) brine.Middleware {
	return func(next brine.Handler) brine.Handler {
		return brine.HandlerFunc(func(ctx context.Context, req brine.Request) (*brine.Result, error) {
			if req.Kind == brine.KindLocal {
				req.Target = transform(req.Target)
			}

			return next.Run(ctx, req)
		})
	}
}

func aliveMinionsPillarMiddleware(unwrapped brine.Handler) brine.Middleware {
	return func(next brine.Handler) brine.Handler {
		return brine.HandlerFunc(func(ctx context.Context, req brine.Request) (*brine.Result, error) {
			if req.Kind != brine.KindLocal || req.Function != "state.sls" {
				return next.Run(ctx, req)
			}

			aliveResult, err := unwrapped.Run(ctx, brine.Runner("manage.alived"))
			if err != nil {
				return nil, err
			}

			var alive []string
			if err := aliveResult.DecodeScalar(&alive); err != nil {
				return nil, err
			}

			brine.PillarData(map[string]any{"orchestration": map[string]any{"alive_minions": alive}})(&req)

			return next.Run(ctx, req)
		})
	}
}

func terminalProgressObserver(w io.Writer) brine.Observer {
	return brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		switch payload := event.Payload.(type) {
		case brine.RequestStartedPayload:
			_, _ = fmt.Fprintf(w, "started %s %s\n", payload.Request.Kind, payload.Request.Function)
		case brine.RequestCompletedPayload:
			_, _ = fmt.Fprintf(w, "completed ok=%t\n", payload.Result.OK())
		case brine.RequestFailedPayload:
			_, _ = fmt.Fprintf(w, "failed %v\n", payload.Err)
		}
	})
}

func jsonLineObserver(w io.Writer) brine.Observer {
	encoder := json.NewEncoder(w)

	return brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		_ = encoder.Encode(map[string]any{
			"type":   event.Type,
			"jid":    event.JID,
			"minion": event.Minion,
		})
	})
}

func TestMiddlewareExamplesCanBeTestedWithMockTransport(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun, brine.CapRunnerRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			switch req.Kind {
			case brine.KindRunner:
				require.Equal(t, "manage.alived", req.Function)

				return &brine.Result{Request: &req, Scalar: json.RawMessage(`["minion-1","minion-2"]`)}, nil
			case brine.KindLocal:
				require.Equal(t, brine.List("minion-1", "minion-2"), req.Target)
				assertNestedPillarValue(t, req, []string{"minion-1", "minion-2"})
				assertNestedPillarValue(t, req, "dev")

				return mock.LocalSuccessResult(req, "minion-1", "minion-2"), nil
			case brine.KindWheel, brine.KindLowstate:
				return nil, fmt.Errorf("unexpected request kind %s", req.Kind)
			default:
				return nil, fmt.Errorf("unknown request kind %s", req.Kind)
			}
		})

	client, err := brine.New(
		transport,
		brine.WithMiddleware(
			targetTransformMiddleware(func(brine.Target) brine.Target { return brine.List("minion-1", "minion-2") }),
			staticPillarMiddleware(map[string]any{"orchestration": map[string]any{"environment": "dev"}}),
			aliveMinionsPillarMiddleware(transport),
		),
	)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), states.SLS(brine.Glob("*"), "brine.success"))
	require.NoError(t, err)
	assert.True(t, result.OK())

	calls := transport.Calls()
	require.Len(t, calls, 2)
	assert.Equal(t, brine.KindRunner, calls[0].Request.Kind)
	assert.Equal(t, brine.KindLocal, calls[1].Request.Kind)
	transport.AssertExpectations(t)
}

func TestObserverAdapters(t *testing.T) {
	t.Parallel()

	var progress bytes.Buffer
	client, err := brine.New(mock.ScriptLocalSuccess("minion-1"), brine.WithObserver(terminalProgressObserver(&progress)))
	require.NoError(t, err)

	_, err = client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.Contains(t, progress.String(), "started local test.ping")
	assert.Contains(t, progress.String(), "completed ok=true")

	var lines bytes.Buffer
	_, err = client.Run(
		context.Background(),
		brine.Local("test.ping", brine.Glob("*")),
		brine.WithRunObserver(jsonLineObserver(&lines)),
	)
	require.NoError(t, err)
	assert.Contains(t, lines.String(), `"type":"request.started"`)
	assert.Contains(t, lines.String(), `"type":"request.completed"`)
}

func ExampleClient_middleware() {
	middleware := staticPillarMiddleware(map[string]any{"example": map[string]any{"message": "hello"}})
	client, err := brine.New(exampleTransport{}, brine.WithMiddleware(middleware))
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), states.SLS(brine.Glob("*"), "brine.success"))
	if err != nil {
		panic(err)
	}

	pillar, err := json.Marshal(result.Request.Kwargs["pillar"])
	if err != nil {
		panic(err)
	}

	fmt.Println(string(pillar))
	// Output:
	// {"example":{"message":"hello"}}
}

func ExampleClient_unwrap() {
	transport := orchestrationExampleTransport{}
	baseClient, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	client, err := brine.New(transport, brine.WithMiddleware(aliveMinionsPillarMiddleware(baseClient.Unwrap())))
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), states.SLS(brine.Glob("*"), "brine.success"))
	if err != nil {
		panic(err)
	}

	pillar, err := json.Marshal(result.Request.Kwargs["pillar"])
	if err != nil {
		panic(err)
	}

	fmt.Println(string(pillar))
	// Output:
	// {"orchestration":{"alive_minions":["minion-1"]}}
}

func ExampleObserverFunc_jsonLines() {
	var out bytes.Buffer
	client, err := brine.New(exampleTransport{}, brine.WithObserver(jsonLineObserver(&out)))
	if err != nil {
		panic(err)
	}

	_, err = client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		panic(err)
	}

	fmt.Print(out.String())
	// Output:
	// {"jid":"","minion":"","type":"request.started"}
	// {"jid":"","minion":"","type":"request.completed"}
}

func assertNestedPillarValue(t *testing.T, req brine.Request, want any) {
	t.Helper()

	pillar, ok := req.Kwargs["pillar"].(map[string]any)
	require.True(t, ok)
	orchestration, ok := pillar["orchestration"].(map[string]any)
	require.True(t, ok)

	for _, value := range orchestration {
		if reflect.DeepEqual(value, want) {
			return
		}
	}

	t.Fatalf("pillar orchestration values %#v did not contain %#v", orchestration, want)
}

type orchestrationExampleTransport struct {
	brine.UnsupportedTransport
}

func (orchestrationExampleTransport) Capabilities() brine.Capabilities {
	return brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun, brine.CapRunnerRun)
}

func (orchestrationExampleTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	if req.Kind == brine.KindRunner {
		return &brine.Result{Request: &req, Scalar: json.RawMessage(`["minion-1"]`)}, nil
	}

	return &brine.Result{
		JID:      exampleJID,
		Request:  &req,
		Expected: []string{"minion-1"},
		ByMinion: map[string]brine.MinionResult{
			"minion-1": {
				Minion:  "minion-1",
				JID:     exampleJID,
				RetCode: 0,
				Return:  json.RawMessage(`true`),
			},
		},
	}, nil
}
