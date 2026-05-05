package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stateContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryState,
			Name:         "state-success",
			Description:  "state.sls success decodes into successful state summaries",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyStateSuccess,
		},
		{
			Category:     CategoryState,
			Name:         "state-full-failure-execution-error",
			Description:  "state.sls full failure returns ExecutionError with result",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyStateFullFailure,
		},
		{
			Category:     CategoryState,
			Name:         "state-partial-failure-preserves-success",
			Description:  "state.sls partial failure preserves successful returns",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyStatePartialFailure,
		},
	}
}

func verifyStateSuccess(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, states.SLS(h.Target, requireStateName(t, h.States.Success)))
	require.NoError(t, err)
	require.True(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)

	decoded, err := states.Decode(result)
	require.NoError(t, err)
	for _, minion := range h.Minions {
		summary := decoded[minion].Summary()
		assert.Equal(t, 1, summary.Succeeded, "%s succeeded count", minion)
		assert.Zero(t, summary.Failed, "%s failed count", minion)
	}
}

func verifyStateFullFailure(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, states.SLS(h.Target, requireStateName(t, h.States.Failure)))
	require.Error(t, err)
	require.NotNil(t, result)

	executionError := requireExecutionError(t, err)
	assert.Equal(t, h.Minions, executionError.Failed())
	assert.False(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)
}

func verifyStatePartialFailure(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, states.SLS(h.Target, requireStateName(t, h.States.PartialFailure)))
	require.Error(t, err)
	require.NotNil(t, result)

	executionError := requireExecutionError(t, err)
	assert.True(t, executionError.Partial())
	assert.Equal(t, h.PartialFailedMinions, executionError.Failed())
	assertReturnedMinions(t, result, h.Minions)
}
