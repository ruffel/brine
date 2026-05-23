package transportkit_test

import (
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeMinionReturnPrefersOnlyRecognizedCleanStatePayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        json.RawMessage
		wantOK      bool
		wantRetCode int
		wantKind    brine.FailureKind
	}{
		{
			name: "successful state chunks override bridge retcode",
			body: json.RawMessage(`{
				"test_|-ok_|-ok_|-succeed_without_changes": {
					"__id__":"ok",
					"name":"ok",
					"result":true,
					"changes":{},
					"comment":"Success!"
				}
			}`),
			wantOK:      true,
			wantRetCode: 0,
		},
		{name: "null does not override bridge retcode", body: json.RawMessage(`null`), wantRetCode: 2, wantKind: brine.FailureMalformed},
		{name: "false does not override bridge retcode", body: json.RawMessage(`false`), wantRetCode: 2, wantKind: brine.FailureRetCode},
		{name: "empty object does not override bridge retcode", body: json.RawMessage(`{}`), wantRetCode: 2, wantKind: brine.FailureRetCode},
		{name: "test-mode state does not override bridge retcode", body: json.RawMessage(`{"x":{"__id__":"x","name":"x","result":null}}`), wantRetCode: 2, wantKind: brine.FailureRetCode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ret := transportkit.NormalizeMinionReturn(transportkit.MinionReturn{
				Minion:            "minion-1",
				Function:          "state.sls",
				Return:            tt.body,
				Raw:               tt.body,
				RetCode:           2,
				RetCodeKnown:      true,
				PreferStateReturn: true,
			})

			assert.Equal(t, tt.wantRetCode, ret.RetCode)
			if tt.wantOK {
				assert.Nil(t, ret.Failure)

				return
			}

			require.NotNil(t, ret.Failure)
			assert.Equal(t, tt.wantKind, ret.Failure.Kind)
		})
	}
}

func TestNormalizeMinionReturnUsesSuccessMetadata(t *testing.T) {
	t.Parallel()

	failure := transportkit.NormalizeMinionReturn(transportkit.MinionReturn{
		Minion:       "minion-1",
		Function:     "test.echo",
		Return:       json.RawMessage(`true`),
		Raw:          json.RawMessage(`{"success":false,"ret":true}`),
		RetCodeKnown: true,
		Success:      boolPtr(false),
	})

	require.NotNil(t, failure.Failure)
	assert.Equal(t, brine.FailureUnknown, failure.Failure.Kind)
}

func TestNormalizeMinionReturnSuccessFalseBeatsStatePreference(t *testing.T) {
	t.Parallel()

	ret := transportkit.NormalizeMinionReturn(transportkit.MinionReturn{
		Minion:            "minion-1",
		Function:          "state.sls",
		Return:            json.RawMessage(`{"x":{"__id__":"x","name":"x","result":true}}`),
		Raw:               json.RawMessage(`{"success":false}`),
		RetCodeKnown:      true,
		Success:           boolPtr(false),
		PreferStateReturn: true,
	})

	require.NotNil(t, ret.Failure)
	assert.Equal(t, brine.FailureUnknown, ret.Failure.Kind)
}

func boolPtr(value bool) *bool { return &value }
