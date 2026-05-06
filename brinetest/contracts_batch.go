package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const halfBatchPercent = 50

func batchContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryBatch,
			Name:         "local-run-batch-count",
			Description:  "local Run supports fixed-size batch execution while returning all expected minions",
			Capabilities: []brine.Capability{brine.CapLocalRun, brine.CapBatch},
			Run:          verifyLocalRunBatchCount,
		},
		{
			Category:     CategoryBatch,
			Name:         "local-run-batch-percent",
			Description:  "local Run supports percentage batch execution while returning all expected minions",
			Capabilities: []brine.Capability{brine.CapLocalRun, brine.CapBatch},
			Run:          verifyLocalRunBatchPercent,
		},
	}
}

func verifyLocalRunBatchCount(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Local("test.ping", h.Target, brine.BatchCount(1)))
	require.NoError(t, err)
	require.True(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)

	pings, err := brine.DecodeByMinion[bool](result)
	require.NoError(t, err)
	for _, minion := range h.Minions {
		assert.True(t, pings[minion], "%s should return true", minion)
	}
}

func verifyLocalRunBatchPercent(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Local("test.ping", h.Target, brine.BatchPercent(halfBatchPercent)))
	require.NoError(t, err)
	require.True(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)

	pings, err := brine.DecodeByMinion[bool](result)
	require.NoError(t, err)
	for _, minion := range h.Minions {
		assert.True(t, pings[minion], "%s should return true", minion)
	}
}
