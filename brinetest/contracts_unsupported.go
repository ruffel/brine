package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/require"
)

func unsupportedContracts() []TestCase {
	return []TestCase{
		{
			Category:           CategoryUnsupported,
			Name:               "runner-start-reports-unsupported",
			Description:        "transports without CapRunnerStart reject runner async explicitly",
			AbsentCapabilities: []brine.Capability{brine.CapRunnerStart},
			Run:                verifyRunnerStartUnsupported,
		},
		{
			Category:           CategoryUnsupported,
			Name:               "target-resolution-reports-unsupported",
			Description:        "transports without CapTargetResolution reject target resolution explicitly",
			AbsentCapabilities: []brine.Capability{brine.CapTargetResolution},
			Run:                verifyTargetResolutionUnsupported,
		},
		{
			Category:           CategoryUnsupported,
			Name:               "events-reports-unsupported",
			Description:        "transports without CapEvents reject global event subscriptions explicitly",
			AbsentCapabilities: []brine.Capability{brine.CapEvents},
			Run:                verifyEventsUnsupported,
		},
	}
}

func verifyRunnerStartUnsupported(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	_, err := h.Client.Start(ctx, brine.Runner("manage.alived"))
	require.ErrorIs(t, err, brine.ErrUnsupported)
}

func verifyTargetResolutionUnsupported(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	_, err := h.Client.Resolve(ctx, h.Target)
	require.ErrorIs(t, err, brine.ErrUnsupported)
}

func verifyEventsUnsupported(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	_, err := h.Client.Events(ctx, brine.EventFilter{})
	require.ErrorIs(t, err, brine.ErrUnsupported)
}
