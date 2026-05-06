package transportkit

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccumulatorBuildsResultWithExpectedMinions(t *testing.T) {
	t.Parallel()

	req := brine.Local("test.ping", brine.List("minion-1", "minion-2"))
	accumulator := NewAccumulator(req)
	accumulator.SetExpected(context.Background(), "jid", []string{"minion-1", "minion-2"})
	accumulator.AddRaw(json.RawMessage(`{"frame":1}`))
	accumulator.AddMinion(context.Background(), brine.MinionResult{
		Minion: "minion-1",
		Return: json.RawMessage(`true`),
	})

	result := accumulator.Result()
	require.NotNil(t, result)
	assert.Equal(t, "jid", result.JID)
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Expected)
	assert.Equal(t, []string{"minion-2"}, result.Missing)
	assert.Equal(t, []string{"minion-1"}, result.Returned())
	assert.JSONEq(t, `{"frame":1}`, string(result.Raw))
}

func TestFailureClassifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  *brine.Failure
		kind brine.FailureKind
	}{
		{name: "bare false ping", got: BareFalseFailure("test.ping", json.RawMessage(`false`)), kind: brine.FailureUnknown},
		{name: "failed state", got: StateFailure("state.sls", json.RawMessage(`{"state":{"result":false}}`)), kind: brine.FailureUnknown},
		{name: "scalar retcode", got: ScalarFailure(json.RawMessage(`{"retcode":2,"return":"failed"}`)), kind: brine.FailureRetCode},
		{name: "retcode", got: RetcodeFailure(2, json.RawMessage(`{"retcode":2}`)), kind: brine.FailureRetCode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotNil(t, tt.got)
			assert.Equal(t, tt.kind, tt.got.Kind)
		})
	}
}

func TestClassifierPredicates(t *testing.T) {
	t.Parallel()

	assert.True(t, IsBareFalse(json.RawMessage(`false`)))
	assert.False(t, IsBareFalse(json.RawMessage(`true`)))
	assert.True(t, IsStateFunction("state.sls"))
	assert.False(t, IsStateFunction("test.ping"))
	assert.True(t, IsMalformedState(json.RawMessage(`"render failed"`)))
	assert.False(t, IsMalformedState(json.RawMessage(`{"state":{"result":true}}`)))
}
