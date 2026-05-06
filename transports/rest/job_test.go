package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/lowstate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalJobRejectsMalformedStartResponses(t *testing.T) {
	t.Parallel()

	req := brine.Local("test.ping", brine.Glob("*"))
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{`},
		{name: "missing return", body: `{}`},
		{name: "empty return", body: `{"return":[]}`},
		{name: "missing jid", body: `{"return":[{"minions":["minion-1"]}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newLocalJob(nil, req, []byte(tt.body))
			require.ErrorIs(t, err, brine.ErrProtocol)
		})
	}
}

func TestNormalizeJobLookupRejectsMalformedResponses(t *testing.T) {
	t.Parallel()

	req := brine.Local("test.ping", brine.Glob("*"))
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{`},
		{name: "missing return", body: `{}`},
		{name: "invalid local return", body: `{"return":["not-a-minion-map"]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := normalizeJobLookup(req, "jid", []string{"minion-1"}, []byte(tt.body))
			require.ErrorIs(t, err, brine.ErrProtocol)
		})
	}
}

func TestNormalizeJobLookupWrappedData(t *testing.T) {
	t.Parallel()

	req := brine.Local("test.ping", brine.Glob("*"))
	result, err := normalizeJobLookup(
		req,
		"jid",
		[]string{"minion-1", "minion-2"},
		[]byte(`{"return":[{"data":{"minion-1":true},"outputter":"nested"}]}`),
	)
	require.NoError(t, err)
	assert.Equal(t, "jid", result.JID)
	assert.Equal(t, []string{"minion-1"}, result.Returned())
	assert.Equal(t, []string{"minion-2"}, result.Missing)
}

func TestStartLocalAsyncAndWait(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++

		var payload []map[string]any
		if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&payload)) {
			return
		}

		if !assert.Len(t, payload, 1) {
			return
		}

		switch requestCount {
		case 1:
			assert.Equal(t, "local_async", payload[0]["client"])
			assert.Equal(t, "test.ping", payload[0]["fun"])
			assert.Equal(t, "*", payload[0]["tgt"])
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1","minion-2"]}]}`))
		case 2:
			assert.Equal(t, "runner", payload[0]["client"])
			assert.Equal(t, "jobs.lookup_jid", payload[0]["fun"])
			assert.Equal(t, []any{"jid"}, payload[0]["arg"])
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			t.Fatalf("unexpected request %d: %#v", requestCount, payload)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.Equal(t, "jid", job.ID())

	local, ok := job.(brine.LocalJob)
	require.True(t, ok)
	assert.Equal(t, []string{"minion-1", "minion-2"}, local.ExpectedMinions())

	result, err := job.Wait(context.Background())
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Equal(t, "jid", result.JID)
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())

	cached, err := job.Wait(context.Background())
	require.NoError(t, err)
	assert.Same(t, result, cached)
	assert.Equal(t, 2, requestCount)
}

func TestStartLocalAsyncWaitUsesListTargetWhenStartOmitsMinions(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		switch requestCount {
		case 1:
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid"}]}`))
		case 2:
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		case 3:
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		default:
			t.Fatalf("unexpected request %d", requestCount)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, JobPollInterval: time.Millisecond})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.List("minion-1", "minion-2")))
	require.NoError(t, err)

	local, ok := job.(brine.LocalJob)
	require.True(t, ok)
	assert.Equal(t, []string{"minion-1", "minion-2"}, local.ExpectedMinions())

	result, err := job.Wait(context.Background())
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
	assert.Equal(t, 3, requestCount)
}

func TestStartLocalAsyncWaitFailsWhenTargetMatchesNoMinions(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":[]}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.Glob("does-not-exist")))
	require.NoError(t, err)

	local, ok := job.(brine.LocalJob)
	require.True(t, ok)
	assert.Empty(t, local.ExpectedMinions())

	result, err := job.Wait(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, brine.ErrExecution)
	require.NotNil(t, result)
	assert.False(t, result.OK())
	assert.Equal(t, brine.FailureNoReturn, result.Failure.Kind)
	assert.Empty(t, result.Returned())
	assert.Equal(t, 1, requestCount)
}

func TestStartLocalAsyncWaitWrappedLookupData(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		switch requestCount {
		case 1:
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1","minion-2"]}]}`))
		case 2:
			_, _ = writer.Write([]byte(`{"return":[{"data":{"minion-1":true,"minion-2":true},"outputter":"nested"}]}`))
		default:
			t.Fatalf("unexpected request %d", requestCount)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)

	result, err := job.Wait(context.Background())
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
}

func TestStartLocalAsyncWaitExecutionError(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		switch requestCount {
		case 1:
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1","minion-2"]}]}`))
		case 2:
			_, _ = writer.Write([]byte(asyncStateFailureResponse))
		default:
			t.Fatalf("unexpected request %d", requestCount)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("state.sls", brine.Glob("*"), brine.Args("brine.conditional_fail")))
	require.NoError(t, err)

	result, err := job.Wait(context.Background())
	require.Error(t, err)

	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	require.NotNil(t, result)
	assert.False(t, result.OK())
	assert.True(t, executionError.Partial())
	assert.Equal(t, []string{"minion-2"}, executionError.Failed())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
}

func TestStartLocalAsyncWaitReturnsPartialOnMissingMinionCancellation(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		switch requestCount {
		case 1:
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1","minion-2"]}]}`))
		default:
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	result, err := job.Wait(ctx)
	require.Error(t, err)

	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	require.NotNil(t, result)
	assert.Equal(t, []string{"minion-2"}, executionError.Missing())
	assert.Equal(t, []string{"minion-1"}, result.Returned())
	assert.Equal(t, 2, requestCount)
}

func TestStartLocalAsyncWaitTimeoutIsCached(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		switch requestCount {
		case 1:
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1","minion-2"]}]}`))
		default:
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		}
	}))
	defer server.Close()

	transport, err := New(Config{
		BaseURL:         server.URL,
		Auth:            NoAuth{},
		JobPollInterval: 500 * time.Millisecond,
		JobWaitTimeout:  10 * time.Millisecond,
	})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)

	result, err := job.Wait(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, brine.ErrExecution)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, result)
	assert.Equal(t, []string{"minion-2"}, result.Missing)
	assert.Equal(t, 2, requestCount)

	cached, err := job.Wait(context.Background())
	require.Error(t, err)
	assert.Same(t, result, cached)
	assert.Equal(t, 2, requestCount)
}

func TestStartLocalAsyncWaitCancellationIsNotCached(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		decodeRESTPayload(t, request)

		switch requestCount {
		case 1:
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1","minion-2"]}]}`))
		case 2:
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, JobPollInterval: 500 * time.Millisecond})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	partial, err := job.Wait(ctx)
	require.Error(t, err)
	require.NotNil(t, partial)
	assert.Equal(t, []string{"minion-2"}, partial.Missing)

	result, err := job.Wait(context.Background())
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
	assert.GreaterOrEqual(t, requestCount, 3)
}

func TestStartRejectsUnsupportedAsyncKinds(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{BaseURL: "http://127.0.0.1:8000", Auth: NoAuth{}})
	require.NoError(t, err)

	tests := []struct {
		name string
		req  brine.Request
		cap  brine.Capability
	}{
		{name: "runner", req: brine.Runner("manage.alived"), cap: brine.CapRunnerStart},
		{name: "wheel", req: brine.Wheel("key.list_all"), cap: brine.CapWheelStart},
		{name: "lowstate", req: lowstate.Request(lowstate.Entry{Client: "local", Fun: "test.ping", Target: "*"}), cap: brine.CapLowstateStart},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := transport.Start(context.Background(), tt.req)
			require.ErrorIs(t, err, brine.ErrUnsupported)

			var unsupported *brine.UnsupportedError
			require.ErrorAs(t, err, &unsupported)
			assert.Equal(t, tt.cap, unsupported.Capability)
		})
	}
}

func decodeRESTPayload(t *testing.T, request *http.Request) {
	t.Helper()

	var payload []map[string]any
	require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
	require.Len(t, payload, 1)
}
