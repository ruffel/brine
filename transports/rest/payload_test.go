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

func TestRESTPayloadTargetsAndOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  brine.Request
		want map[string]any
	}{
		{
			name: "glob target",
			req:  brine.Local("test.ping", brine.Glob("minion-*")),
			want: map[string]any{"client": "local", "fun": "test.ping", "tgt": "minion-*"},
		},
		{
			name: "list target",
			req:  brine.Local("test.ping", brine.List("minion-1", "minion-2")),
			want: map[string]any{"client": "local", "fun": "test.ping", "tgt": []any{"minion-1", "minion-2"}, "tgt_type": "list"},
		},
		{
			name: "compound target",
			req:  brine.Local("test.ping", brine.Compound("G@os:Debian and minion-*")),
			want: map[string]any{"client": "local", "fun": "test.ping", "tgt": "G@os:Debian and minion-*", "tgt_type": "compound"},
		},
		{
			name: "grain target",
			req:  brine.Local("test.ping", brine.Grain("os:Debian")),
			want: map[string]any{"client": "local", "fun": "test.ping", "tgt": "os:Debian", "tgt_type": "grain"},
		},
		{
			name: "pillar target",
			req:  brine.Local("test.ping", brine.Pillar("role:web")),
			want: map[string]any{"client": "local", "fun": "test.ping", "tgt": "role:web", "tgt_type": "pillar"},
		},
		{
			name: "nodegroup target",
			req:  brine.Local("test.ping", brine.NodeGroup("web")),
			want: map[string]any{"client": "local", "fun": "test.ping", "tgt": "web", "tgt_type": "nodegroup"},
		},
		{
			name: "options",
			req: brine.Local("state.sls", brine.Glob("*"),
				brine.Args("brine.success"),
				brine.Kwargs(map[string]any{"test": true}),
				brine.FullReturn(true),
				brine.ModuleTimeout(3*time.Second),
				brine.GatherJobTimeout(4*time.Second),
			),
			want: map[string]any{
				"client":             "local",
				"fun":                "state.sls",
				"tgt":                "*",
				"arg":                []any{"brine.success"},
				"kwarg":              map[string]any{"test": true},
				"full_return":        true,
				"timeout":            float64(3),
				"gather_job_timeout": float64(4),
			},
		},
		{
			name: "sub-second timeouts",
			req: brine.Local("state.sls", brine.Glob("*"),
				brine.ModuleTimeout(500*time.Millisecond),
				brine.GatherJobTimeout(1500*time.Millisecond),
			),
			want: map[string]any{
				"timeout":            float64(1),
				"gather_job_timeout": float64(2),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			captured := runAndCapturePayload(t, tt.req, `{"return":[{"minion-1":true}]}`)
			require.Len(t, captured, 1)
			for key, want := range tt.want {
				assert.Equal(t, want, captured[0][key], key)
			}
		})
	}
}

func TestRunDirectBatchPayload(t *testing.T) {
	t.Parallel()

	captured := runAndCapturePayload(t, brine.Local("test.ping", brine.Glob("*"), brine.BatchCount(2)), `{"return":[{"minion-1":true}]}`)
	require.Len(t, captured, 1)
	assert.Equal(t, "2", captured[0]["batch"])
}

func TestRESTPayloadOmitsRequestMetadata(t *testing.T) {
	t.Parallel()

	captured := runAndCapturePayload(
		t,
		brine.Local("test.ping", brine.Glob("*"), brine.Metadata("trace_id", "abc")),
		`{"return":[{"minion-1":true}]}`,
	)
	require.Len(t, captured, 1)
	assert.NotContains(t, captured[0], "metadata")
	assert.NotContains(t, captured[0], "trace_id")
}

func TestRunRawLowstatePayloadIncludesClient(t *testing.T) {
	t.Parallel()

	req := lowstate.Request(lowstate.Entry{
		Client:  "local",
		Fun:     "test.ping",
		Target:  []string{"minion-1", "minion-2"},
		TgtType: "list",
	})

	captured := runAndCapturePayload(t, req, `{"return":[{"minion-1":true,"minion-2":true}]}`)
	require.Len(t, captured, 1)
	assert.Equal(t, "local", captured[0]["client"])
	assert.Equal(t, "test.ping", captured[0]["fun"])
	assert.Equal(t, "list", captured[0]["tgt_type"])
	assert.Equal(t, []any{"minion-1", "minion-2"}, captured[0]["tgt"])
}

func TestRunRawLowstateMultipleEntriesPreservesAllReturns(t *testing.T) {
	t.Parallel()

	req := lowstate.Request(
		lowstate.Entry{Client: "local", Fun: "test.ping", Target: "*"},
		lowstate.Entry{Client: "runner", Fun: "jobs.active"},
	)

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true},{}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), req)
	require.NoError(t, err)
	require.True(t, result.OK())

	var values []map[string]any
	require.NoError(t, result.DecodeScalar(&values))
	assert.Equal(t, []map[string]any{{"minion-1": true}, {}}, values)
	require.Len(t, captured, 2)
	assert.Equal(t, "local", captured[0]["client"])
	assert.Equal(t, "runner", captured[1]["client"])
}

func TestRunRawLowstateMultipleEntriesMarksScalarFailure(t *testing.T) {
	t.Parallel()

	req := lowstate.Request(
		lowstate.Entry{Client: "local", Fun: "test.ping", Target: "*"},
		lowstate.Entry{Client: "runner", Fun: "bad.runner"},
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true},{"error":"boom"}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, result.Failure)
	assert.False(t, result.OK())
	assert.Equal(t, brine.FailureMalformed, result.Failure.Kind)
}

func runAndCapturePayload(t *testing.T, req brine.Request, response string) []map[string]any {
	t.Helper()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
		_, _ = writer.Write([]byte(response))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), req)
	require.NoError(t, err)

	return captured
}
