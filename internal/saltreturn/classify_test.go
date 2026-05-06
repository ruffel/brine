package saltreturn

import (
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
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
	}{
		{name: "test ping false fails", function: "test.ping", raw: json.RawMessage(`false`), wantFail: true},
		{name: "service false is data", function: "service.status", raw: json.RawMessage(`false`)},
		{name: "test ping true succeeds", function: "test.ping", raw: json.RawMessage(`true`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := BareFalseFailure(tt.function, tt.raw)
			if tt.wantFail {
				require.NotNil(t, failure)
				assert.Equal(t, brine.FailureUnknown, failure.Kind)

				return
			}

			assert.Nil(t, failure)
		})
	}
}

func TestStateFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  json.RawMessage
		kind brine.FailureKind
	}{
		{
			name: "failed chunk",
			raw:  json.RawMessage(`{"file_|-example_|-/tmp/example_|-managed":{"result":false}}`),
			kind: brine.FailureUnknown,
		},
		{name: "render string", raw: json.RawMessage(`"Rendering SLS failed"`), kind: brine.FailureMalformed},
		{name: "render messages", raw: json.RawMessage(`["Rendering SLS failed"]`), kind: brine.FailureMalformed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := StateFailure("state.sls", tt.raw)
			require.NotNil(t, failure)
			assert.Equal(t, tt.kind, failure.Kind)
		})
	}
}

func TestScalarFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  json.RawMessage
		kind brine.FailureKind
	}{
		{name: "error", raw: json.RawMessage(`{"error":"boom"}`), kind: brine.FailureMalformed},
		{name: "exception", raw: json.RawMessage(`{"exception":"boom"}`), kind: brine.FailureMinionException},
		{name: "success false", raw: json.RawMessage(`{"success":false}`), kind: brine.FailureUnknown},
		{name: "nested retcode", raw: json.RawMessage(`{"data":{"retcode":2}}`), kind: brine.FailureRetCode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failure := ScalarFailure(tt.raw)
			require.NotNil(t, failure)
			assert.Equal(t, tt.kind, failure.Kind)
		})
	}
}
