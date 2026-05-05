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
			Name:         "runner-scalar-result",
			Description:  "runner scalar results decode without fake minion IDs",
			Capabilities: []brine.Capability{brine.CapRunnerRun},
			Run:          verifyRunnerScalar,
		},
		{
			Category:     CategorySync,
			Name:         "wheel-scalar-result",
			Description:  "wheel scalar results decode without fake minion IDs",
			Capabilities: []brine.Capability{brine.CapWheelRun},
			Run:          verifyWheelScalar,
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

func verifyWheelScalar(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	result, err := h.Client.Run(ctx, brine.Wheel("key.list_all"))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.False(t, result.IsLocal())
	assert.Empty(t, result.ByMinion)

	var keys map[string]any
	require.NoError(t, result.DecodeScalar(&keys))
	assert.NotEmpty(t, keys)
}
