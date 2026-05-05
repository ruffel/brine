package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	_, err = job.Events(context.Background())
	require.ErrorIs(t, err, brine.ErrUnsupported)

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

func TestStartRejectsUnsupportedAsyncKinds(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{BaseURL: "http://127.0.0.1:8000", Auth: NoAuth{}})
	require.NoError(t, err)

	_, err = transport.Start(context.Background(), brine.Runner("manage.alived"))
	require.ErrorIs(t, err, brine.ErrUnsupported)
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
