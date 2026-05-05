package python

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunLocalSuccess(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"local":{"by_minion":{"minion-1":{"jid":"jid","retcode":0,"return":true}}}}`)
	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.List("minion-1"), brine.Metadata("trace", "abc")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, "jid", result.JID)
	assert.Equal(t, []string{"minion-1"}, result.Returned())
	assert.JSONEq(t, `true`, string(result.ByMinion["minion-1"].Return))
}

func TestRunLocalStateFailure(t *testing.T) {
	t.Parallel()

	response := `{"local":{"by_minion":{"minion-1":{"retcode":0,"return":{` +
		`"test_|-fail_|-fail_|-fail_without_changes":{` +
		`"__id__":"fail","name":"fail","result":false,"changes":{},"comment":"Failure!"` +
		`}}}}}}`
	transport := newHelperTransport(t, response)
	result, err := transport.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1"), brine.Args("brine.fail")))
	require.NoError(t, err)
	require.False(t, result.OK())
	failure := result.ByMinion["minion-1"].Failure
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureUnknown, failure.Kind)
}

func TestRunRejectsUnsupportedKinds(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{}`)
	_, err := transport.Run(context.Background(), brine.Runner("manage.alived"))
	require.ErrorIs(t, err, brine.ErrUnsupported)
}

func TestResolveUsesLocalPing(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"local":{"by_minion":{"minion-1":{"return":true},"minion-2":{"return":true}}}}`)
	minions, err := transport.Resolve(context.Background(), brine.Glob("*"))
	require.NoError(t, err)
	assert.Equal(t, []string{"minion-1", "minion-2"}, minions)
}

func TestBridgeError(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"error":{"kind":"exception","message":"boom"}}`)
	_, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.ErrorIs(t, err, brine.ErrTransport)
}

func TestMakeBridgeRequest(t *testing.T) {
	t.Parallel()

	payload, err := makeBridgeRequest(brine.Local(
		"cmd.run",
		brine.Compound("G@role:web"),
		brine.Args("uptime"),
		brine.Kwargs(map[string]any{"prepend_path": "/usr/local/bin"}),
		brine.Metadata("trace_id", "abc"),
	))
	require.NoError(t, err)
	assert.Equal(t, "local", payload.Kind)
	assert.Equal(t, brine.TargetCompound, payload.Target.Type)
	assert.Equal(t, "G@role:web", payload.Target.Expression)
	assert.Equal(t, []any{"uptime"}, payload.Args)
	assert.Equal(t, map[string]any{"prepend_path": "/usr/local/bin"}, payload.Kwargs)
	assert.Equal(t, map[string]any{"trace_id": "abc"}, payload.Metadata)
}

//nolint:paralleltest // This helper runs as a subprocess for other parallel tests.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("BRINE_PYTHON_HELPER_TEST") != "1" {
		return
	}

	var request bridgeRequest
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		_, _ = io.WriteString(os.Stdout, `{"error":{"kind":"protocol","message":"`+err.Error()+`"}}`)
		os.Exit(0)
	}

	response := os.Getenv("BRINE_PYTHON_HELPER_RESPONSE")
	if response == "" {
		response = `{"local":{"by_minion":{}}}`
	}

	_, _ = io.WriteString(os.Stdout, response)
	os.Exit(0)
}

func newHelperTransport(t *testing.T, response string) *Transport {
	t.Helper()

	transport, err := New(Config{
		Command: os.Args[0],
		Args:    []string{"-test.run=TestHelperProcess", "--"},
		Env: []string{
			"BRINE_PYTHON_HELPER_TEST=1",
			"BRINE_PYTHON_HELPER_RESPONSE=" + response,
		},
	})
	require.NoError(t, err)

	return transport
}

func TestNewRequiresCommand(t *testing.T) {
	t.Parallel()

	_, err := New(Config{})
	require.Error(t, err)
}

func TestNormalizeMissingLocalResult(t *testing.T) {
	t.Parallel()

	_, err := normalizeBridgeLocal(brine.Local("test.ping", brine.Glob("*")), []byte(`{}`))
	require.ErrorIs(t, err, brine.ErrProtocol)
}

func TestHelperExitFailure(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{Command: os.Args[0], Args: []string{"-test.run=TestNoSuchHelper"}})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.Error(t, err)
	assert.True(t, errors.Is(err, brine.ErrTransport) || errors.Is(err, brine.ErrProtocol))
}
