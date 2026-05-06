package brinetest

import (
	"context"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func progressContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryProgress,
			Name:         "run-scoped-minion-returns",
			Description:  "synchronous Run emits expected-minion and per-minion return progress events",
			Capabilities: []brine.Capability{brine.CapLocalRun, brine.CapRunScopedReturns},
			Run:          verifyRunScopedMinionReturns,
		},
	}
}

func verifyRunScopedMinionReturns(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultRunTimeout)
	defer cancel()

	recorder := &progressRecorder{}
	result, err := h.Client.Run(ctx, brine.Local("test.ping", h.Target), brine.WithRunObserver(recorder))
	require.NoError(t, err)
	require.True(t, result.OK())
	assertReturnedMinions(t, result, h.Minions)

	assert.ElementsMatch(t, h.Minions, recorder.expected)
	assert.ElementsMatch(t, h.Minions, recorder.returned)
}

type progressRecorder struct {
	expected []string
	returned []string
}

func (r *progressRecorder) OnEvent(_ context.Context, event brine.Event) {
	switch payload := event.Payload.(type) {
	case brine.ExpectedMinionsPayload:
		r.expected = append([]string(nil), payload.Minions...)
	case brine.MinionReturnedPayload:
		r.returned = append(r.returned, payload.Result.Minion)
	}
}
