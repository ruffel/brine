package brinetest

import (
	"slices"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func failureContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategorySync,
			Name:         "runner-invalid-function-returns-failure",
			Description:  "runner call to a non-existent module returns classified failure",
			Capabilities: []brine.Capability{brine.CapRunnerRun},
			Run:          verifyRunnerInvalidFunction,
		},
		{
			Category:     CategorySync,
			Name:         "local-missing-minion-populates-missing",
			Description:  "targeting a non-existent minion populates result.Missing",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyLocalMissingMinion,
		},
		{
			Category:     CategorySync,
			Name:         "local-full-return-false-is-data",
			Description:  "a service.status false with FullReturn is classified as data, not failure",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyFullReturnFalseIsData,
		},
	}
}

// verifyRunnerInvalidFunction calls a non-existent runner module and asserts
// that the result is classified as a failure. Salt returns an error envelope
// for unknown modules; transports must not silently drop it.
func verifyRunnerInvalidFunction(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Runner("brine_nonexistent.function"))
	if err == nil {
		// Some transports surface the failure through result.OK() rather
		// than as a Go error, both are acceptable.
		require.NotNil(t, result)
		assert.False(t, result.OK(), "result for invalid runner should not be OK")

		return
	}

	// The error should be an ExecutionError with the result attached.
	executionError := requireExecutionError(t, err)
	assert.False(t, executionError.Result.OK())
}

// verifyLocalMissingMinion targets a minion that does not exist alongside a
// real minion. The result must include the fake minion in Missing and the real
// minion in Returned.
func verifyLocalMissingMinion(t *testing.T, h Harness) {
	t.Helper()

	if h.FakeMinion == "" {
		t.Skip("brinetest: Harness.FakeMinion not configured")
	}

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	realMinion := h.Minions[0]
	target := brine.List(realMinion, h.FakeMinion)

	result, err := h.Client.Run(ctx, brine.Local("test.ping", target))
	// Missing minions cause a non-OK result, so ExecutionError is expected.
	if err != nil {
		executionError := requireExecutionError(t, err)
		result = executionError.Result
	}

	require.NotNil(t, result)
	assert.False(t, result.OK(), "result with missing minion should not be OK")
	assert.Contains(t, result.Returned(), realMinion, "real minion should have returned")
	assert.True(t, slices.Contains(result.Missing, h.FakeMinion), "fake minion should be in Missing")
}

// verifyFullReturnFalseIsData runs service.status for a stopped service with
// FullReturn(true). The false return is domain data (the service is stopped),
// not a failure. The result must be OK.
func verifyFullReturnFalseIsData(t *testing.T, h Harness) {
	t.Helper()

	if h.StoppedService == "" {
		t.Skip("brinetest: Harness.StoppedService not configured")
	}

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Local(
		"service.status",
		h.Target,
		brine.Args(h.StoppedService),
		brine.FullReturn(true),
	))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.OK(), "service.status false with FullReturn should be OK (false is data)")
}
