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

func TestRunLocalPing(t *testing.T) {
	t.Parallel()

	var captured []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "/", request.URL.Path)
		assert.Equal(t, "token", request.Header.Get("X-Auth-Token"))
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))

		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	require.Len(t, captured, 1)
	assert.Equal(t, "local", captured[0]["client"])
	assert.Equal(t, "test.ping", captured[0]["fun"])
	assert.Equal(t, "*", captured[0]["tgt"])
}

func TestRunListTargetFullReturnFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":{"jid":"jid","ret":false,"retcode":1}}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1"), brine.Args("brine.fail")))
	require.NoError(t, err)
	assert.False(t, result.OK())
	assert.Equal(t, "jid", result.JID)

	failure := result.ByMinion["minion-1"].Failure
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureRetCode, failure.Kind)
}

func TestRunBareFalseMinionReturn(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":false}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.False(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())

	assert.Nil(t, result.ByMinion["minion-1"].Failure)
	assert.Equal(t, 0, result.ByMinion["minion-1"].RetCode)

	failure := result.ByMinion["minion-2"].Failure
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureNoReturn, failure.Kind)
	assert.Equal(t, 1, result.ByMinion["minion-2"].RetCode)
}

func TestRunRunnerScalar(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[["minion-1","minion-2"]]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Runner("manage.alived"))
	require.NoError(t, err)

	var minions []string
	require.NoError(t, result.DecodeScalar(&minions))
	assert.Equal(t, []string{"minion-1", "minion-2"}, minions)
}

func TestNoAuthOmitsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Empty(t, request.Header.Get("X-Auth-Token"))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.True(t, result.OK())
}

func TestNilAuthOmitsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Empty(t, request.Header.Get("X-Auth-Token"))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.True(t, result.OK())
}

func TestPAMAuthLogin(t *testing.T) {
	t.Parallel()

	loginCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/login":
			loginCount++
			_, _ = writer.Write([]byte(`{"return":[{"token":"abc","expire":4102444800}]}`))
		case "/":
			assert.Equal(t, "abc", request.Header.Get("X-Auth-Token"))
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected path", "path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi")})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.Equal(t, 1, loginCount)
}

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
				brine.BatchPercent(25),
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
				"batch":              "25%",
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

func TestPAMAuthRetriesOnceAfterUnauthorized(t *testing.T) {
	t.Parallel()

	loginCount := 0
	postCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/login":
			loginCount++
			token := "expired"
			if loginCount > 1 {
				token = "fresh"
			}

			_, _ = writer.Write([]byte(`{"return":[{"token":"` + token + `","expire":4102444800}]}`))
		case "/":
			postCount++
			if request.Header.Get("X-Auth-Token") == "expired" {
				http.Error(writer, "expired", http.StatusUnauthorized)

				return
			}

			assert.Equal(t, "fresh", request.Header.Get("X-Auth-Token"))
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected path", "path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi")})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Equal(t, 2, loginCount)
	assert.Equal(t, 2, postCount)
}

func runAndCapturePayload(t *testing.T, req brine.Request, response string) []map[string]any {
	t.Helper()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
		_, _ = writer.Write([]byte(response))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), req)
	require.NoError(t, err)

	return captured
}

func TestInfoDetectsSaltVersion(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		var payload []map[string]any
		if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&payload)) || !assert.Len(t, payload, 1) {
			writer.WriteHeader(http.StatusBadRequest)

			return
		}

		assert.Equal(t, "runner", payload[0]["client"])
		assert.Equal(t, "test.get_opts", payload[0]["fun"])
		_, _ = writer.Write([]byte(`{"return":[{"saltversion":"3006.9"}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rest", info.Name)
	assert.Equal(t, "3006.9", info.SaltVersion)
	assert.ElementsMatch(t, transport.Capabilities().List(), info.Capabilities.List())

	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, 1, requestCount)
}

func TestInfoIgnoresSaltVersionProbeFailure(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requestCount++
		http.Error(writer, "no", http.StatusInternalServerError)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rest", info.Name)
	assert.Empty(t, info.SaltVersion)
	assert.ElementsMatch(t, transport.Capabilities().List(), info.Capabilities.List())

	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, 1, requestCount)
}

func TestSaltVersionFromGetOpts(t *testing.T) {
	t.Parallel()

	version, ok := saltVersionFromGetOpts([]byte(`{"return":[{"saltversioninfo":[3006,9]}]}`))
	require.True(t, ok)
	assert.Equal(t, "3006.9", version)
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

	_, err = transport.Start(context.Background(), brine.Runner("manage.alived"))
	require.ErrorIs(t, err, brine.ErrUnsupported)
}

func decodeRESTPayload(t *testing.T, request *http.Request) {
	t.Helper()

	var payload []map[string]any
	require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
	require.Len(t, payload, 1)
}

func TestUnauthorized(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "no", http.StatusUnauthorized)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.ErrorIs(t, err, brine.ErrAuth)
}
