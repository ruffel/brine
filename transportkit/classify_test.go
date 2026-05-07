package transportkit_test

import (
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBareFalseFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		function string
		raw      json.RawMessage
		wantFail bool
		wantMsg  string
	}{
		// Allowlisted modules — bare false is a failure.
		{name: "test.ping false", function: "test.ping", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "test.ping returned false"},
		{name: "service.start false", function: "service.start", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "service.start returned false"},
		{name: "service.stop false", function: "service.stop", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "service.stop returned false"},
		{name: "service.restart false", function: "service.restart", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "service.restart returned false"},
		{name: "service.reload false", function: "service.reload", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "service.reload returned false"},
		{name: "service.enable false", function: "service.enable", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "service.enable returned false"},
		{name: "service.disable false", function: "service.disable", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "service.disable returned false"},
		{name: "file.copy false", function: "file.copy", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "file.copy returned false"},
		{name: "file.rename false", function: "file.rename", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "file.rename returned false"},
		{name: "file.move false", function: "file.move", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "file.move returned false"},
		{name: "user.add false", function: "user.add", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "user.add returned false"},
		{name: "user.delete false", function: "user.delete", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "user.delete returned false"},
		{name: "group.add false", function: "group.add", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "group.add returned false"},
		{name: "group.delete false", function: "group.delete", raw: json.RawMessage(`false`), wantFail: true, wantMsg: "group.delete returned false"},

		// Non-allowlisted modules — bare false is domain data.
		{name: "service.status false is data", function: "service.status", raw: json.RawMessage(`false`)},
		{name: "service.available false is data", function: "service.available", raw: json.RawMessage(`false`)},
		{name: "file.file_exists false is data", function: "file.file_exists", raw: json.RawMessage(`false`)},
		{name: "file.directory_exists false is data", function: "file.directory_exists", raw: json.RawMessage(`false`)},
		{name: "user.info false is data", function: "user.info", raw: json.RawMessage(`false`)},
		{name: "custom.check false is data", function: "custom.check", raw: json.RawMessage(`false`)},

		// Allowlisted modules with non-false values — no failure.
		{name: "test.ping true succeeds", function: "test.ping", raw: json.RawMessage(`true`)},
		{name: "service.start string is not bare false", function: "service.start", raw: json.RawMessage(`"already running"`)},
		{name: "file.copy null passes through", function: "file.copy", raw: json.RawMessage(`null`), wantFail: true, wantMsg: "file.copy returned false"},
		{name: "user.add object is not bare false", function: "user.add", raw: json.RawMessage(`{"uid":1000}`)},

		// Edge case: invalid JSON.
		{name: "invalid json", function: "test.ping", raw: json.RawMessage(`not json`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := transportkit.BareFalseFailure(tt.function, tt.raw)
			if tt.wantFail {
				require.NotNil(t, failure)
				assert.Equal(t, brine.FailureUnknown, failure.Kind)
				assert.Equal(t, tt.wantMsg, failure.Message)
				assert.NotEmpty(t, failure.Raw, "failure should preserve raw payload")

				return
			}

			assert.Nil(t, failure)
		})
	}
}

func TestBareFalsePredicates(t *testing.T) {
	t.Parallel()

	assert.True(t, transportkit.IsBareFalse(json.RawMessage(`false`)))
	assert.False(t, transportkit.IsBareFalse(json.RawMessage(`true`)))
	assert.False(t, transportkit.IsBareFalse(json.RawMessage(`0`)))
	assert.False(t, transportkit.IsBareFalse(json.RawMessage(`"false"`)))
	assert.True(t, transportkit.IsBareFalse(json.RawMessage(`null`)), "null unmarshals to false zero value")
	assert.False(t, transportkit.IsBareFalse(json.RawMessage(``)))
}

func TestStateFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		function string
		raw      json.RawMessage
		wantFail bool
		kind     brine.FailureKind
	}{
		// Failure cases.
		{
			name:     "failed chunk",
			function: "state.sls",
			raw:      json.RawMessage(`{"file_|-example_|-/tmp/example_|-managed":{"result":false}}`),
			wantFail: true,
			kind:     brine.FailureUnknown,
		},
		{name: "render string", function: "state.sls", raw: json.RawMessage(`"Rendering SLS failed"`), wantFail: true, kind: brine.FailureMalformed},
		{name: "render messages", function: "state.highstate", raw: json.RawMessage(`["Rendering SLS failed"]`), wantFail: true, kind: brine.FailureMalformed},

		// Success cases — should return nil.
		{
			name:     "successful chunk",
			function: "state.sls",
			raw:      json.RawMessage(`{"file_|-example_|-/tmp/example_|-managed":{"result":true}}`),
		},
		{name: "non-state function ignored", function: "cmd.run", raw: json.RawMessage(`"output"`)},
		{name: "empty chunks", function: "state.sls", raw: json.RawMessage(`{}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := transportkit.StateFailure(tt.function, tt.raw)
			if tt.wantFail {
				require.NotNil(t, failure)
				assert.Equal(t, tt.kind, failure.Kind)

				return
			}

			assert.Nil(t, failure)
		})
	}
}

func TestStatePredicates(t *testing.T) {
	t.Parallel()

	assert.True(t, transportkit.IsStateFunction("state.sls"))
	assert.True(t, transportkit.IsStateFunction("state.highstate"))
	assert.True(t, transportkit.IsStateFunction("state.apply"))
	assert.False(t, transportkit.IsStateFunction("test.ping"))
	assert.False(t, transportkit.IsStateFunction("cmd.run"))

	assert.True(t, transportkit.IsMalformedState(json.RawMessage(`"render failed"`)))
	assert.True(t, transportkit.IsMalformedState(json.RawMessage(`["render failed"]`)))
	assert.False(t, transportkit.IsMalformedState(json.RawMessage(`{"state":{"result":true}}`)))
	assert.False(t, transportkit.IsMalformedState(json.RawMessage(`false`)))
}

func TestScalarFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      json.RawMessage
		wantFail bool
		kind     brine.FailureKind
	}{
		// Failure cases.
		{name: "error key", raw: json.RawMessage(`{"error":"boom"}`), wantFail: true, kind: brine.FailureMalformed},
		{name: "exception key", raw: json.RawMessage(`{"exception":"boom"}`), wantFail: true, kind: brine.FailureMinionException},
		{name: "success false", raw: json.RawMessage(`{"success":false}`), wantFail: true, kind: brine.FailureUnknown},
		{name: "retcode non-zero", raw: json.RawMessage(`{"retcode":2}`), wantFail: true, kind: brine.FailureRetCode},
		{name: "nested retcode in data", raw: json.RawMessage(`{"data":{"retcode":2}}`), wantFail: true, kind: brine.FailureRetCode},
		{name: "nested retcode in return", raw: json.RawMessage(`{"return":{"retcode":1}}`), wantFail: true, kind: brine.FailureRetCode},
		{name: "nested retcode in ret", raw: json.RawMessage(`{"ret":{"retcode":3}}`), wantFail: true, kind: brine.FailureRetCode},
		{name: "nested error in data", raw: json.RawMessage(`{"data":{"error":"boom"}}`), wantFail: true, kind: brine.FailureMalformed},
		{name: "list of maps with error", raw: json.RawMessage(`[{"error":"boom"}]`), wantFail: true, kind: brine.FailureMalformed},

		// Success cases.
		{name: "success true", raw: json.RawMessage(`{"success":true}`)},
		{name: "retcode zero", raw: json.RawMessage(`{"retcode":0}`)},
		{name: "plain object", raw: json.RawMessage(`{"foo":"bar"}`)},
		{name: "plain string", raw: json.RawMessage(`"hello"`)},
		{name: "plain number", raw: json.RawMessage(`42`)},
		{name: "plain bool", raw: json.RawMessage(`true`)},
		{name: "empty object", raw: json.RawMessage(`{}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := transportkit.ScalarFailure(tt.raw)
			if tt.wantFail {
				require.NotNil(t, failure)
				assert.Equal(t, tt.kind, failure.Kind)

				return
			}

			assert.Nil(t, failure)
		})
	}
}

func TestRetcodeFailure(t *testing.T) {
	t.Parallel()

	raw := json.RawMessage(`{"retcode":3}`)

	assert.Nil(t, transportkit.RetcodeFailure(0, raw), "retcode 0 should not produce failure")

	failure := transportkit.RetcodeFailure(3, raw)
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureRetCode, failure.Kind)
	assert.Equal(t, "retcode 3", failure.Message)
	assert.NotEmpty(t, failure.Raw)

	negative := transportkit.RetcodeFailure(-1, raw)
	require.NotNil(t, negative, "negative retcode should produce failure")
	assert.Equal(t, "retcode -1", negative.Message)
}

// TestBareFalseModulesIsExtensible verifies that callers can add entries to
// BareFalseModules to classify custom Salt module false returns as failures.
func TestBareFalseModulesIsExtensible(t *testing.T) { //nolint:paralleltest // modifies the global BareFalseModules map; must run serially.
	// Note: this test modifies the global map; it cannot run in parallel with
	// other tests that exercise BareFalseModules.  It cleans up after itself.
	original, ok := transportkit.BareFalseModules["myorg.create"]
	defer func() {
		if ok {
			transportkit.BareFalseModules["myorg.create"] = original
		} else {
			delete(transportkit.BareFalseModules, "myorg.create")
		}
	}()

	transportkit.BareFalseModules["myorg.create"] = struct{}{}

	failure := transportkit.BareFalseFailure("myorg.create", []byte(`false`))
	require.NotNil(t, failure)
	assert.Equal(t, brine.FailureUnknown, failure.Kind)

	// A function not in the map must not produce a failure.
	assert.Nil(t, transportkit.BareFalseFailure("myorg.other", []byte(`false`)))
}
