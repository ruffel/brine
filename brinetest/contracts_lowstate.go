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

	switch value := target.(type) {
	case brine.GlobTarget:
		return string(value), ""
	case brine.CompoundTarget:
		return string(value), "compound"
	case brine.GrainTarget:
		return string(value), "grain"
	case brine.PillarTarget:
		return string(value), "pillar"
	case brine.NodeGroupTarget:
		return string(value), "nodegroup"
	case brine.ListTarget:
		return []string(value), "list"
	default:
		t.Fatalf("brinetest: unsupported target type %T", target)
		return nil, ""
	}
}
