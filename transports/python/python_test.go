package python

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

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

func TestRunLocalStateSuccessIgnoresBridgeRetcode(t *testing.T) {
	t.Parallel()

	response := `{"local":{"by_minion":{"minion-1":{"retcode":2,"return":{` +
		`"test_|-ok_|-ok_|-succeed_without_changes":{` +
		`"__id__":"ok","name":"ok","result":true,"changes":{},"comment":"Success!"` +
		`}}}}}}`
	transport := newHelperTransport(t, response)
	result, err := transport.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1"), brine.Args("brine.success")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Zero(t, result.ByMinion["minion-1"].RetCode)
	assert.Nil(t, result.ByMinion["minion-1"].Failure)
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

func TestRunLocalBareFalseIsNotNoReturn(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"local":{"by_minion":{"minion-1":{"return":false}}}}`)
	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.List("minion-1")))
	require.NoError(t, err)
	require.False(t, result.OK())
	failure := result.ByMinion["minion-1"].Failure
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureUnknown, failure.Kind)
	assert.Empty(t, result.Missing)
}

func TestRunLocalServiceStatusFalseIsData(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"local":{"by_minion":{"minion-1":{"return":false}}}}`)
	result, err := transport.Run(context.Background(), brine.Local("service.status", brine.List("minion-1"), brine.Args("sshd")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Nil(t, result.ByMinion["minion-1"].Failure)
	assert.JSONEq(t, `false`, string(result.ByMinion["minion-1"].Return))
}

func TestRunLocalMalformedStateReturn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "render string", body: `"Rendering SLS failed"`},
		{name: "render messages", body: `["Rendering SLS failed"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transport := newHelperTransport(t, `{"local":{"by_minion":{"minion-1":{"return":`+tt.body+`}}}}`)
			result, err := transport.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1")))
			require.NoError(t, err)
			require.False(t, result.OK())
			failure := result.ByMinion["minion-1"].Failure
			require.NotNil(t, failure)
			assert.Equal(t, brine.FailureMalformed, failure.Kind)
		})
	}
}

func TestCapabilitiesAdvertiseRunScopedLocalReturns(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{}`)
	caps := transport.Capabilities()
	assert.True(t, caps.Supports(brine.CapSynchronousRun))
	assert.True(t, caps.Supports(brine.CapLocalRun))
	assert.True(t, caps.Supports(brine.CapLocalStart))
	assert.True(t, caps.Supports(brine.CapRunnerRun))
	assert.True(t, caps.Supports(brine.CapJobLookup))
	assert.True(t, caps.Supports(brine.CapTargetResolution))
	assert.True(t, caps.Supports(brine.CapRunScopedReturns))
	assert.False(t, caps.Supports(brine.CapEvents))
}

func TestRunRunnerScalar(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"type":"scalar","scalar":["minion-1","minion-2"]}`)
	result, err := transport.Run(context.Background(), brine.Runner("manage.alived"))
	require.NoError(t, err)
	require.True(t, result.OK())

	var alive []string
	require.NoError(t, result.DecodeScalar(&alive))
	assert.Equal(t, []string{"minion-1", "minion-2"}, alive)
}

func TestRunRunnerScalarFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response string
		kind     brine.FailureKind
	}{
		{
			name:     "top-level error",
			response: `{"type":"scalar","scalar":{"error":"boom"}}`,
			kind:     brine.FailureMalformed,
		},
		{
			name:     "success false",
			response: `{"type":"scalar","scalar":{"success":false,"data":{"return":true}}}`,
			kind:     brine.FailureUnknown,
		},
		{
			name:     "nested retcode",
			response: `{"type":"scalar","scalar":{"data":{"retcode":2,"return":"failed"}}}`,
			kind:     brine.FailureRetCode,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transport := newHelperTransport(t, tt.response)
			result, err := transport.Run(context.Background(), brine.Runner("state.orchestrate"))
			require.NoError(t, err)
			require.NotNil(t, result.Failure)
			assert.False(t, result.OK())
			assert.Equal(t, tt.kind, result.Failure.Kind)
		})
	}
}

func TestRunRejectsUnsupportedKinds(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{}`)
	tests := []struct {
		name string
		req  brine.Request
		cap  brine.Capability
	}{
		{name: "lowstate", req: brine.Lowstate(brine.LowstateEntry{Client: "local", Fun: "test.ping", Target: "*"}), cap: brine.CapLowstate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := transport.Run(context.Background(), tt.req)
			require.ErrorIs(t, err, brine.ErrUnsupported)

			var unsupported *brine.UnsupportedError
			require.ErrorAs(t, err, &unsupported)
			assert.Equal(t, tt.cap, unsupported.Capability)
		})
	}
}

func TestBridgeUnsupportedErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		req       brine.Request
		response  string
		operation string
		cap       brine.Capability
		caps      []brine.Capability
	}{
		{
			name:      "inferred local run capability",
			req:       brine.Local("test.ping", brine.Glob("*")),
			response:  `{"error":{"kind":"unsupported","message":"no local"}}`,
			operation: "Run",
			cap:       brine.CapLocalRun,
		},
		{
			name:      "inferred runner run capability",
			req:       brine.Runner("manage.alived"),
			response:  `{"error":{"kind":"unsupported","message":"no runner"}}`,
			operation: "Run",
			cap:       brine.CapRunnerRun,
		},
		{
			name:      "explicit operation and capability",
			req:       brine.Local("test.ping", brine.Glob("*")),
			response:  `{"error":{"kind":"unsupported","message":"no stream","operation":"Subscribe","capability":"events"}}`,
			operation: "Subscribe",
			cap:       brine.CapEvents,
		},
		{
			name:      "explicit capability set",
			req:       brine.Runner("manage.alived"),
			response:  `{"error":{"kind":"unsupported","message":"no async","capabilities":["runner.start","lowstate.start"]}}`,
			operation: "Run",
			caps:      []brine.Capability{brine.CapRunnerStart, brine.CapLowstateStart},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			transport := newHelperTransport(t, tt.response)
			_, err := transport.Run(context.Background(), tt.req)
			require.ErrorIs(t, err, brine.ErrUnsupported)

			var unsupported *brine.UnsupportedError
			require.ErrorAs(t, err, &unsupported)
			assert.Equal(t, tt.operation, unsupported.Operation)
			assert.Equal(t, tt.cap, unsupported.Capability)
			assert.Equal(t, tt.caps, unsupported.Capabilities)
		})
	}
}

func TestResolveUsesLocalPing(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"local":{"by_minion":{"minion-1":{"return":true},"minion-2":{"return":true}}}}`)
	minions, err := transport.Resolve(context.Background(), brine.Glob("*"))
	require.NoError(t, err)
	assert.Equal(t, []string{"minion-1", "minion-2"}, minions)
}

func TestStartLocalAsyncWait(t *testing.T) {
	t.Parallel()

	transport := newAsyncHelperTransport(t,
		`{"type":"started","jid":"jid","minions":["minion-1","minion-2"]}`,
		`{"type":"minions","jid":"jid","minions":["minion-1","minion-2"]}
{"type":"return","minion":"minion-1","jid":"jid","body":true}
{"type":"return","minion":"minion-2","jid":"jid","body":true}
{"type":"done","jid":"jid"}
`,
	)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.List("minion-1", "minion-2")))
	require.NoError(t, err)
	assert.Equal(t, "jid", job.ID())

	localJob, ok := job.(brine.LocalJob)
	require.True(t, ok)
	assert.Equal(t, []string{"minion-1", "minion-2"}, localJob.ExpectedMinions())

	result, err := job.Wait(context.Background())
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())
}

func TestStartLocalAsyncWaitMissingMinion(t *testing.T) {
	t.Parallel()

	transport := newAsyncHelperTransport(t,
		`{"type":"started","jid":"jid","minions":["minion-1","minion-2"]}`,
		`{"type":"minions","jid":"jid","minions":["minion-1","minion-2"]}
{"type":"return","minion":"minion-1","jid":"jid","body":true}
{"type":"done","jid":"jid"}
`,
	)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.List("minion-1", "minion-2")))
	require.NoError(t, err)

	result, err := job.Wait(context.Background())
	require.Error(t, err)
	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	assert.Equal(t, []string{"minion-2"}, result.Missing)
}

func TestStartLocalAsyncWaitDoesNotCacheTransientFailures(t *testing.T) {
	t.Parallel()

	transport := newAsyncHelperTransport(t,
		`{"type":"started","jid":"jid","minions":["minion-1"]}`,
		`{"error":{"kind":"exception","message":"lookup down"}}`,
	)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.List("minion-1")))
	require.NoError(t, err)

	result, err := job.Wait(context.Background())
	require.Error(t, err)
	assert.NotNil(t, result)

	transport.env = []string{
		"BRINE_PYTHON_HELPER_TEST=1",
		`BRINE_PYTHON_HELPER_WAIT_RESPONSE={"type":"minions","jid":"jid","minions":["minion-1"]}
{"type":"return","minion":"minion-1","jid":"jid","body":true}
{"type":"done","jid":"jid"}
`,
	}

	result, err = job.Wait(context.Background())
	require.NoError(t, err)
	assert.True(t, result.OK())
}

func TestRunLocalDomainSuccessFalseIsData(t *testing.T) {
	t.Parallel()

	ret := normalizeBridgeMinion(brine.Local("test.echo", brine.List("minion-1")), "minion-1", bridgeMinionResult{
		Return: json.RawMessage(`{"success":false}`),
	})

	assert.Nil(t, ret.Failure)
	assert.Zero(t, ret.RetCode)
	assert.JSONEq(t, `{"success":false}`, string(ret.Return))
}

func TestRunLocalFullReturnSuccessFalseFails(t *testing.T) {
	t.Parallel()

	transport := newHelperTransport(t, `{"type":"minions","minions":["minion-1"]}
{"type":"return","minion":"minion-1","jid":"jid","retcode":0,"success":false,"body":true}
{"type":"done"}
`)
	result, err := transport.Run(context.Background(), brine.Local("test.echo", brine.List("minion-1")))
	require.NoError(t, err)
	require.False(t, result.OK())
	require.NotNil(t, result.ByMinion["minion-1"].Failure)
	assert.Equal(t, brine.FailureUnknown, result.ByMinion["minion-1"].Failure.Kind)
}

func TestRunLocalStreamingFrames(t *testing.T) {
	t.Parallel()

	response := `{"type":"minions","minions":["minion-1","minion-2"]}
{"type":"return","minion":"minion-1","jid":"jid","retcode":0,"body":true}
{"type":"done"}
`
	transport := newHelperTransport(t, response)

	var events []brine.Event
	client, err := brine.New(transport, brine.WithObserver(brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		events = append(events, event)
	})))
	require.NoError(t, err)

	result, err := client.Run(context.Background(), brine.Local("test.ping", brine.List("minion-1", "minion-2")))
	require.Error(t, err)
	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Expected)
	assert.Equal(t, []string{"minion-2"}, result.Missing)
	assert.Equal(t, []string{"minion-1"}, result.Returned())
	assert.Equal(t, []brine.EventType{
		brine.EventRequestStarted,
		brine.EventExpectedMinions,
		brine.EventExpectedMinions,
		brine.EventMinionReturned,
		brine.EventRequestFailed,
	}, eventTypes(events))
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
		brine.FullReturn(true),
		brine.ModuleTimeout(1500*time.Millisecond),
		brine.Metadata("trace_id", "abc"),
	))
	require.NoError(t, err)
	assert.Equal(t, bridgeProtocolVersion, payload.ProtocolVersion)
	assert.Equal(t, "local", payload.Kind)
	assert.Equal(t, brine.TargetCompound, payload.Target.Type)
	assert.Equal(t, "G@role:web", payload.Target.Expression)
	assert.Equal(t, []any{"uptime"}, payload.Args)
	assert.Equal(t, map[string]any{"prepend_path": "/usr/local/bin"}, payload.Kwargs)
	assert.True(t, payload.Options.FullReturn)
	assert.Equal(t, 2, payload.Options.TimeoutSeconds)
	assert.Equal(t, map[string]any{"trace_id": "abc"}, payload.Metadata)

	runner, err := makeBridgeRequest(brine.Runner("manage.alived"))
	require.NoError(t, err)
	assert.Equal(t, bridgeProtocolVersion, runner.ProtocolVersion)
	assert.Equal(t, "runner", runner.Kind)
	assert.Empty(t, runner.Target.Expression)
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
	if request.Operation == "start" && os.Getenv("BRINE_PYTHON_HELPER_START_RESPONSE") != "" {
		response = os.Getenv("BRINE_PYTHON_HELPER_START_RESPONSE")
	}
	if request.Operation == "wait" && os.Getenv("BRINE_PYTHON_HELPER_WAIT_RESPONSE") != "" {
		response = os.Getenv("BRINE_PYTHON_HELPER_WAIT_RESPONSE")
	}
	if response == "" {
		response = `{"local":{"by_minion":{}}}`
	}

	_, _ = io.WriteString(os.Stdout, response)
	os.Exit(0)
}

func eventTypes(events []brine.Event) []brine.EventType {
	types := make([]brine.EventType, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}

	return types
}

func newHelperTransport(t *testing.T, response string) *Transport {
	t.Helper()

	return newHelperTransportWithEnv(t, []string{"BRINE_PYTHON_HELPER_RESPONSE=" + response})
}

func newAsyncHelperTransport(t *testing.T, startResponse string, waitResponse string) *Transport {
	t.Helper()

	return newHelperTransportWithEnv(t, []string{
		"BRINE_PYTHON_HELPER_START_RESPONSE=" + startResponse,
		"BRINE_PYTHON_HELPER_WAIT_RESPONSE=" + waitResponse,
	})
}

func newHelperTransportWithEnv(t *testing.T, env []string) *Transport {
	t.Helper()

	transport, err := New(Config{
		Command: os.Args[0],
		Args:    []string{"-test.run=TestHelperProcess", "--"},
		Env:     append([]string{"BRINE_PYTHON_HELPER_TEST=1"}, env...),
	})
	require.NoError(t, err)

	return transport
}

func TestNewRequiresCommand(t *testing.T) {
	t.Parallel()

	_, err := New(Config{})
	require.Error(t, err)
}

func TestCommandEnvAddsSaltMasterConfig(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{
		Command:          "helper",
		Env:              []string{saltMasterConfigEnv + "=/old/master", "OTHER=1"},
		SaltMasterConfig: "/custom/master",
	})
	require.NoError(t, err)

	env := transport.commandEnv([]string{"BASE=1"})

	assert.Contains(t, env, "BASE=1")
	assert.Contains(t, env, "OTHER=1")
	assert.Contains(t, env, saltMasterConfigEnv+"=/custom/master")
	assert.NotContains(t, env, saltMasterConfigEnv+"=/old/master")
}

func TestCommandEnvPreservesConfiguredEnvWithoutSaltMasterConfig(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{
		Command: "helper",
		Env:     []string{saltMasterConfigEnv + "=/env/master"},
	})
	require.NoError(t, err)

	env := transport.commandEnv([]string{"BASE=1"})

	assert.Contains(t, env, "BASE=1")
	assert.Contains(t, env, saltMasterConfigEnv+"=/env/master")
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
