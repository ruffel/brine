package brinetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func infoContracts() []TestCase {
	return []TestCase{
		{
			Category:    CategoryInfo,
			Name:        "transport-info",
			Description: "transport info reports a stable name and the advertised capability set",
			Run:         verifyTransportInfo,
		},
	}
}

func verifyTransportInfo(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	info, err := h.Client.Info(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, info.Name)
	assert.ElementsMatch(t, h.Client.Capabilities().List(), info.Capabilities.List())
	if h.ExpectedSaltVersion != "" {
		assert.Equal(t, h.ExpectedSaltVersion, info.SaltVersion)
	}
}
