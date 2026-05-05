package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func asyncContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryAsync,
			Name:         "local-start-wait-success",
			Description:  "local async job waits to a normalized successful result",
			Capabilities: []brine.Capability{brine.CapLocalStart, brine.CapJobLookup},
			Run:          verifyAsyncLocalWaitSuccess,
		},
		{
			Category:     CategoryAsync,
			Name:         "local-job-expected-minions",
			Description:  "local async jobs expose expected minions through LocalJob",
			Capabilities: []brine.Capability{brine.CapLocalStart},
			Run:          verifyAsyncLocalJobExpectedMinions,
		},
		{
			Category:     CategoryAsync,
			Name:         "wait-idempotent",
			Description:  "terminal Job.Wait results are cached and idempotent",
			Capabilities: []brine.Capability{brine.CapLocalStart, brine.CapJobLookup},
			Run:          verifyAsyncWaitIdempotent,
		},
		{
			Category:     CategoryAsync,
			Name:         "local-start-wait-partial-failure",
			Description:  "local async state failure returns ExecutionError with partial result",
			Capabilities: []brine.Capability{brine.CapLocalStart, brine.CapJobLookup},
			Run:          verifyAsyncLocalWaitPartialFailure,
		},
		{
			Category:     CategoryAsync,
			Name:         "failed-wait-idempotent",
			Description:  "terminal failed Job.Wait results are cached and idempotent",
			Capabilities: []brine.Capability{brine.CapLocalStart, brine.CapJobLookup},
			Run:          verifyAsyncFailedWaitIdempotent,
		},
	}
}

func verifyAsyncLocalWaitSuccess(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, brine.Local("test.ping", h.Target))
	require.NoError(t, err)
	assert.NotEmpty(t, job.ID())

	result, err := job.Wait(ctx)
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, job.ID(), result.JID)
	assertReturnedMinions(t, result, h.Minions)
}

func verifyAsyncLocalJobExpectedMinions(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, brine.Local("test.ping", h.Target))
	require.NoError(t, err)

	local, ok := job.(brine.LocalJob)
	require.True(t, ok, "local async jobs must implement brine.LocalJob")
	assert.ElementsMatch(t, h.Minions, local.ExpectedMinions())
}

func verifyAsyncWaitIdempotent(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, brine.Local("test.ping", h.Target))
	require.NoError(t, err)

	result1, err1 := job.Wait(ctx)
	result2, err2 := job.Wait(ctx)
	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Same(t, result1, result2)
}

func verifyAsyncLocalWaitPartialFailure(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, states.SLS(h.Target, requireStateName(t, h.States.PartialFailure)))
	require.NoError(t, err)

	result, err := job.Wait(ctx)
	require.Error(t, err)
	require.NotNil(t, result)

	executionError := requireExecutionError(t, err)
	assert.True(t, executionError.Partial())
	assert.Equal(t, h.PartialFailedMinions, executionError.Failed())
	assertReturnedMinions(t, result, h.Minions)
}

func verifyAsyncFailedWaitIdempotent(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, states.SLS(h.Target, requireStateName(t, h.States.PartialFailure)))
	require.NoError(t, err)

	result1, err1 := job.Wait(ctx)
	result2, err2 := job.Wait(ctx)
	require.Error(t, err1)
	require.Error(t, err2)
	assert.True(t, requireExecutionError(t, err1).Partial())
	assert.True(t, requireExecutionError(t, err2).Partial())
	assert.Same(t, result1, result2)
	assertReturnedMinions(t, result1, h.Minions)
}
