package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func syncContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategorySync,
			Name:         "local-test-ping",
			Description:  "local test.ping succeeds and returns expected minions",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyLocalPing,
		},
		{
			Category:     CategorySync,
			Name:         "local-cmd-run",
			Description:  "local cmd.run supports positional args, kwargs, string output, and retcodes",
			Capabilities: []brine.Capability{brine.CapLocalRun},
			Run:          verifyLocalCmdRun,
		},
		{
			Category:     CategorySync,
			Name:         "runner-scalar-result",
			Description:  "runner scalar results decode without fake minion IDs",
			Capabilities: []brine.Capability{brine.CapRunnerRun},
			Run:          verifyRunnerScalar,
		},
	}
}

func verifyLocalPing(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Local("test.ping", h.Target))
	require.NoError(t, err)
	require.True(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)

	pings, err := brine.DecodeByMinion[bool](result)
	require.NoError(t, err)
	for _, minion := range h.Minions {
		assert.True(t, pings[minion], "%s should return true", minion)
	}
}

func verifyLocalCmdRun(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Local(
		"cmd.run",
		h.Target,
		brine.Args("printf brine"),
		brine.Kwargs(map[string]any{"prepend_path": "/usr/local/bin"}),
	))
	require.NoError(t, err)
	require.True(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)

	outputs, err := brine.DecodeByMinion[string](result)
	require.NoError(t, err)
	for _, minion := range h.Minions {
		assert.Equal(t, "brine", outputs[minion], "%s output", minion)
		assert.Zero(t, result.ByMinion[minion].RetCode, "%s retcode", minion)
	}
}

func verifyRunnerScalar(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Runner("manage.alived"))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.False(t, result.IsLocal())
	assert.Empty(t, result.ByMinion)

	var alive []string
	require.NoError(t, result.DecodeScalar(&alive))
	for _, minion := range h.Minions {
		assert.Contains(t, alive, minion)
	}
}
