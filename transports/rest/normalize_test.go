package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadLimitedBodyWithLimitRejectsOversizedBody(t *testing.T) {
	t.Parallel()

	_, err := readLimitedBodyWithLimit(strings.NewReader("12345"), "read response", 4)
	require.ErrorIs(t, err, brine.ErrProtocol)
}

func TestRunListTargetFullReturnFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":{"jid":"jid","ret":false,"retcode":1}}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token"), LocalRunMode: LocalRunModeDirect})
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

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.False(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())

	assert.Nil(t, result.ByMinion["minion-1"].Failure)
	assert.Equal(t, 0, result.ByMinion["minion-1"].RetCode)

	failure := result.ByMinion["minion-2"].Failure
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureUnknown, failure.Kind)
	assert.Empty(t, result.Missing)
	assert.Equal(t, 1, result.ByMinion["minion-2"].RetCode)
}

func TestRunBareFalseServiceStatusIsData(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":false}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("service.status", brine.Glob("*"), brine.Args("sshd")))
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Nil(t, result.ByMinion["minion-1"].Failure)
	assert.JSONEq(t, `false`, string(result.ByMinion["minion-1"].Return))
}

func TestRunFullReturnSuccessFalseFails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":{"ret":true,"retcode":0,"success":false}}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("service.status", brine.Glob("*"), brine.Args("sshd"), brine.FullReturn(true)))
	require.NoError(t, err)
	assert.False(t, result.OK())
	require.NotNil(t, result.ByMinion["minion-1"].Failure)
	assert.Equal(t, brine.FailureUnknown, result.ByMinion["minion-1"].Failure.Kind)
}

func TestNormalizeMinionFullReturnDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		req             brine.Request
		raw             string
		wantJID         string
		wantRetCode     int
		wantFailureKind brine.FailureKind
		wantFailure     bool
		wantReturn      string
	}{
		{
			name:        "full return envelope requires ret field",
			req:         brine.Local("test.echo", brine.List("minion-1")),
			raw:         `{"jid":"domain-job","retcode":17,"error":"domain payload"}`,
			wantReturn:  `{"jid":"domain-job","retcode":17,"error":"domain payload"}`,
			wantRetCode: 0,
		},
		{
			name:            "full return envelope with null ret is recognized",
			req:             brine.Local("test.echo", brine.List("minion-1"), brine.FullReturn(true)),
			raw:             `{"jid":"jid-1","ret":null,"retcode":1,"error":"boom"}`,
			wantJID:         "jid-1",
			wantRetCode:     1,
			wantFailure:     true,
			wantFailureKind: brine.FailureMinionException,
			wantReturn:      `null`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ret := normalizeMinion(&tt.req, "minion-1", json.RawMessage(tt.raw))

			assert.Equal(t, tt.wantJID, ret.JID)
			assert.Equal(t, tt.wantRetCode, ret.RetCode)
			assert.JSONEq(t, tt.wantReturn, string(ret.Return))
			if !tt.wantFailure {
				assert.Nil(t, ret.Failure)

				return
			}

			require.NotNil(t, ret.Failure)
			assert.Equal(t, tt.wantFailureKind, ret.Failure.Kind)
		})
	}
}

func TestRunMalformedStateReturn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ret    string
		failed bool
	}{
		{name: "render string", ret: `"Rendering SLS failed"`, failed: true},
		{name: "render messages", ret: `["Rendering SLS failed"]`, failed: true},
		{name: "non-state string", ret: `"plain string"`, failed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(`{"return":[{"minion-1":` + tt.ret + `}]}`))
			}))
			defer server.Close()

			transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
			require.NoError(t, err)

			function := "state.sls"
			if !tt.failed {
				function = "test.echo"
			}

			result, err := transport.Run(context.Background(), brine.Local(function, brine.List("minion-1")))
			require.NoError(t, err)
			failure := result.ByMinion["minion-1"].Failure
			if tt.failed {
				require.NotNil(t, failure)
				assert.Equal(t, brine.FailureMalformed, failure.Kind)
				assert.False(t, result.OK())

				return
			}

			assert.Nil(t, failure)
			assert.True(t, result.OK())
		})
	}
}

func TestRunRunnerScalar(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
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
	require.Len(t, captured, 1)
	assert.Equal(t, "runner", captured[0]["client"])
	assert.Equal(t, "manage.alived", captured[0]["fun"])
}

func TestRunScalarFailureMarksResultNotOK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		kind brine.FailureKind
	}{
		{
			name: "top-level error",
			body: `{"return":[{"error":"boom"}]}`,
			kind: brine.FailureMalformed,
		},
		{
			name: "runner success false",
			body: `{"return":[{"success":false,"data":{"return":true}}]}`,
			kind: brine.FailureUnknown,
		},
		{
			name: "runner retcode",
			body: `{"return":[{"data":{"retcode":2,"return":"failed"}}]}`,
			kind: brine.FailureRetCode,
		},
		{
			name: "nested exception",
			body: `{"return":[{"data":{"return":[{"exception":"boom"}]}}]}`,
			kind: brine.FailureMinionException,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(tt.body))
			}))
			defer server.Close()

			transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
			require.NoError(t, err)

			result, err := transport.Run(context.Background(), brine.Wheel("key.list_all"))
			require.NoError(t, err)
			require.NotNil(t, result.Failure)
			assert.False(t, result.OK())
			assert.Equal(t, tt.kind, result.Failure.Kind)
		})
	}
}

func TestRunWheelScalarPayload(t *testing.T) {
	t.Parallel()

	var captured []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&captured))
		_, _ = writer.Write([]byte(`{"return":[{"minions":["minion-1"]}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Wheel("key.list_all", brine.Kwargs(map[string]any{"match": "minion-*"})))
	require.NoError(t, err)

	var keys map[string][]string
	require.NoError(t, result.DecodeScalar(&keys))
	assert.Equal(t, []string{"minion-1"}, keys["minions"])
	require.Len(t, captured, 1)
	assert.Equal(t, "wheel", captured[0]["client"])
	assert.Equal(t, "key.list_all", captured[0]["fun"])
	assert.Equal(t, map[string]any{"match": "minion-*"}, captured[0]["kwarg"])
}
