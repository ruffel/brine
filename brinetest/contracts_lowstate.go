package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/lowstate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lowstateContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryLowstate,
			Name:         "local-test-ping-lowstate",
			Description:  "raw lowstate local test.ping returns the expected minions as a scalar Salt payload",
			Capabilities: []brine.Capability{brine.CapLowstate},
			Run:          verifyLowstateLocalPing,
		},
	}
}

func verifyLowstateLocalPing(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	target, targetType := lowstateTarget(t, h.Target)
	entry := lowstate.Entry{
		Client:  "local",
		Fun:     "test.ping",
		Target:  target,
		TgtType: targetType,
	}

	result, err := h.Client.Run(ctx, lowstate.Request(entry))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.False(t, result.IsLocal())

	var byMinion map[string]bool
	require.NoError(t, result.DecodeScalar(&byMinion))
	for _, minion := range h.Minions {
		assert.True(t, byMinion[minion], "%s should return true", minion)
	}
}

func lowstateTarget(t *testing.T, target brine.Target) (any, string) {
	t.Helper()

	spec, err := brine.DescribeTarget(target)
	require.NoError(t, err)

	if spec.Type == brine.TargetGlob {
		return spec.Expression, ""
	}

	return spec.Expression, string(spec.Type)
}
