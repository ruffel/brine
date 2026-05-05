package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func targetContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryTargets,
			Name:         "resolve-target",
			Description:  "target resolution returns expected minions",
			Capabilities: []brine.Capability{brine.CapTargetResolution},
			Run:          verifyResolveTarget,
		},
	}
}

func verifyResolveTarget(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	minions, err := h.Client.Resolve(ctx, h.Target)
	require.NoError(t, err)
	assert.Equal(t, h.Minions, minions)
}
