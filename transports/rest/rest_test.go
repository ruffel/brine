package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const asyncStateFailureResponse = `{
  "return": [{
    "minion-1": {
      "test_|-ok_|-ok_|-succeed_without_changes": {
        "__id__": "ok",
        "name": "ok",
        "result": true,
        "changes": {},
        "comment": "Success!"
      }
    },
    "minion-2": {
      "test_|-fail_|-fail_|-fail_without_changes": {
        "__id__": "fail",
        "name": "fail",
        "result": false,
        "changes": {},
        "comment": "Failure!"
      }
    }
  }]
}`

func TestRunLocalDirectModePing(t *testing.T) {
	t.Parallel()

	var captured []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "/", request.URL.Path)
		assert.Equal(t, "token", request.Header.Get("X-Auth-Token"))
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))

		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token"), LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	require.Len(t, captured, 1)
	assert.Equal(t, "local", captured[0]["client"])
	assert.Equal(t, "test.ping", captured[0]["fun"])
	assert.Equal(t, "*", captured[0]["tgt"])
}

func TestRunLocalDirectListTargetMarksMissingMinions(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.List("minion-1", "minion-2")))
	require.NoError(t, err)
	assert.False(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Expected)
	assert.Equal(t, []string{"minion-2"}, result.Missing)
	assert.Equal(t, []string{"minion-1"}, result.Returned())
}

func TestRunLocalDefaultUsesAsyncLookupProgress(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == eventStreamPath {
			writer.Header().Set("Content-Type", "text/event-stream")

			return
		}

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)
		captured = append(captured, payload[0])

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1","minion-2"]}]}`))
		case "runner":
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	result, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, "jid-1", result.JID)
	assert.Len(t, captured, 2)
	assert.Equal(t, "local_async", captured[0]["client"])
	assert.Equal(t, "runner", captured[1]["client"])
	assert.Equal(t, []brine.EventType{brine.EventExpectedMinions, brine.EventMinionReturned, brine.EventMinionReturned}, recorder.types())
}

func TestLocalRunModeCapabilities(t *testing.T) {
	t.Parallel()

	asyncTransport, err := New(Config{BaseURL: "http://127.0.0.1:8000"})
	require.NoError(t, err)
	assert.True(t, asyncTransport.Capabilities().Supports(brine.CapRunScopedReturns))
	assert.True(t, asyncTransport.Capabilities().Supports(brine.CapBatch))
	assert.True(t, asyncTransport.Capabilities().Supports(brine.CapLowstate))
	assert.False(t, asyncTransport.Capabilities().Supports(brine.CapLowstateStart))

	directTransport, err := New(Config{BaseURL: "http://127.0.0.1:8000", LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)
	assert.False(t, directTransport.Capabilities().Supports(brine.CapRunScopedReturns))
	assert.True(t, directTransport.Capabilities().Supports(brine.CapBatch))
}

func TestRunLocalObservedConsumesSSEBeforeLookupReconciliation(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	lookupCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == eventStreamPath {
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = writer.Write([]byte("tag: salt/job/jid-1/ret/minion-1\n"))
			_, _ = writer.Write([]byte("data: {\"jid\":\"jid-1\",\"id\":\"minion-1\",\"return\":true,\"retcode\":0,\"success\":true}\n\n"))
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}

			return
		}

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1","minion-2"]}]}`))
		case "runner":
			mu.Lock()
			lookupCount++
			count := lookupCount
			mu.Unlock()
			if count == 1 {
				_, _ = writer.Write([]byte(`{"return":[{}]}`))

				return
			}

			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, JobPollInterval: 10 * time.Millisecond})
	require.NoError(t, err)

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	result, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())

	mu.Lock()
	assert.GreaterOrEqual(t, lookupCount, 2)
	mu.Unlock()

	assert.True(t, recorder.hasMinionReturnRaw("minion-1", `"jid":"jid-1"`))
}

func TestRunLocalAutoModeUsesDirectWithoutObserver(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeAuto})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	require.Len(t, captured, 1)
	assert.Equal(t, "local", captured[0]["client"])
}

func TestRunLocalAutoModeUsesAsyncWithObserver(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == eventStreamPath {
			writer.Header().Set("Content-Type", "text/event-stream")

			return
		}

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)
		captured = append(captured, payload[0])

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1"]}]}`))
		case "runner":
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeAuto})
	require.NoError(t, err)

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	result, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Len(t, captured, 2)
	assert.Equal(t, "local_async", captured[0]["client"])
	assert.Equal(t, "runner", captured[1]["client"])
	assert.Equal(t, []brine.EventType{brine.EventExpectedMinions, brine.EventMinionReturned}, recorder.types())
}

func TestRunLocalObservedFallsBackToLookupWhenSSEFails(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == eventStreamPath {
			http.Error(writer, "events unavailable", http.StatusInternalServerError)

			return
		}

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)
		captured = append(captured, payload[0])

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1","minion-2"]}]}`))
		case "runner":
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	result, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
	assert.Equal(t, []brine.EventType{brine.EventExpectedMinions, brine.EventMinionReturned, brine.EventMinionReturned}, recorder.types())
}

func TestRunLocalObservedReturnsPartialWhenLookupFailsAfterSSE(t *testing.T) {
	t.Parallel()

	lookupCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == eventStreamPath {
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = writer.Write([]byte("tag: salt/job/jid-1/ret/minion-1\n"))
			_, _ = writer.Write([]byte("data: {\"jid\":\"jid-1\",\"id\":\"minion-1\",\"return\":true,\"retcode\":0,\"success\":true}\n\n"))
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}

			return
		}

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1","minion-2"]}]}`))
		case "runner":
			lookupCount++
			if lookupCount == 1 {
				_, _ = writer.Write([]byte(`{"return":[{}]}`))

				return
			}

			http.Error(writer, "lookup failed", http.StatusInternalServerError)
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, JobPollInterval: 10 * time.Millisecond})
	require.NoError(t, err)

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	result, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
	require.Error(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []string{"minion-1"}, result.Returned())
	assert.Equal(t, []string{"minion-2"}, result.Missing)
	assert.True(t, recorder.hasMinionReturnRaw("minion-1", `"jid":"jid-1"`))
}

func TestRejectsBatchForRunnerRequests(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{BaseURL: "http://127.0.0.1:8000", Auth: NoAuth{}})
	require.NoError(t, err)

	tests := []struct {
		name string
		req  brine.Request
	}{
		{name: "runner", req: brine.Runner("manage.alived", brine.BatchCount(1))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := transport.Run(context.Background(), tt.req)
			require.ErrorIs(t, err, brine.ErrUnsupported)

			var unsupported *brine.UnsupportedError
			require.ErrorAs(t, err, &unsupported)
			assert.Equal(t, brine.CapBatch, unsupported.Capability)
		})
	}
}

func TestRunLocalAsyncModePreservesTargetArgsKwargsAndOptions(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)
		captured = append(captured, payload[0])

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1","minion-2"]}]}`))
		case "runner":
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local(
		"state.sls",
		brine.List("minion-1", "minion-2"),
		brine.Args("brine.success"),
		brine.Kwargs(map[string]any{"test": true}),
		brine.FullReturn(true),
		brine.ModuleTimeout(3*time.Second),
		brine.GatherJobTimeout(4*time.Second),
	))
	require.NoError(t, err)
	require.True(t, result.OK())
	require.Len(t, captured, 2)

	start := captured[0]
	assert.Equal(t, "local_async", start["client"])
	assert.Equal(t, "state.sls", start["fun"])
	assert.Equal(t, []any{"minion-1", "minion-2"}, start["tgt"])
	assert.Equal(t, "list", start["tgt_type"])
	assert.Equal(t, []any{"brine.success"}, start["arg"])
	assert.Equal(t, map[string]any{"test": true}, start["kwarg"])
	assert.Equal(t, true, start["full_return"])
	assert.Equal(t, float64(3), start["timeout"])
	assert.Equal(t, float64(4), start["gather_job_timeout"])
	assert.NotContains(t, start, "batch")

	lookup := captured[1]
	assert.Equal(t, "runner", lookup["client"])
	assert.Equal(t, "jobs.lookup_jid", lookup["fun"])
	assert.Equal(t, []any{"jid-1"}, lookup["arg"])
}

func TestRunLocalAsyncModeIncludesBatch(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)
		captured = append(captured, payload[0])

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1","minion-2"]}]}`))
		case "runner":
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.List("minion-1", "minion-2"), brine.BatchCount(1)))
	require.NoError(t, err)
	require.True(t, result.OK())
	require.Len(t, captured, 2)
	assert.Equal(t, "local_async", captured[0]["client"])
	assert.Equal(t, "1", captured[0]["batch"])
}

func TestRunLocalDefaultUsesAsyncLookupWithoutObserver(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.Len(t, payload, 1)
		captured = append(captured, payload[0])

		switch payload[0]["client"] {
		case "local_async":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid-1","minions":["minion-1"]}]}`))
		case "runner":
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected client", "client: %v", payload[0]["client"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, "jid-1", result.JID)
	assert.Len(t, captured, 2)
	assert.Equal(t, "local_async", captured[0]["client"])
	assert.Equal(t, "runner", captured[1]["client"])
}

func TestResolveTarget(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	minions, err := transport.Resolve(context.Background(), brine.Glob("*"))
	require.NoError(t, err)
	assert.Equal(t, []string{"minion-1", "minion-2"}, minions)
	require.Len(t, captured, 1)
	assert.Equal(t, "local", captured[0]["client"])
	assert.Equal(t, "test.ping", captured[0]["fun"])
	assert.Equal(t, "*", captured[0]["tgt"])
}

type eventRecorder struct{ events []brine.Event }

func (r *eventRecorder) Emit(_ context.Context, event brine.Event) {
	r.events = append(r.events, event)
}

func (r *eventRecorder) types() []brine.EventType {
	types := make([]brine.EventType, 0, len(r.events))
	for _, event := range r.events {
		types = append(types, event.Type)
	}

	return types
}

func (r *eventRecorder) hasMinionReturnRaw(minion string, raw string) bool {
	for _, event := range r.events {
		if event.Type == brine.EventMinionReturned && event.Minion == minion && strings.Contains(string(event.Raw), raw) {
			return true
		}
	}

	return false
}
