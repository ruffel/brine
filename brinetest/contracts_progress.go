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
		{
			Category: CategoryProgress,
			Name:     "async-backed-run-scoped-returns",
			Description: "async-backed local Run emits a consistent JID through expected-minion and minion-return " +
				"progress events while reconciling a complete final result",
			Capabilities: []brine.Capability{
				brine.CapLocalRun,
				brine.CapLocalStart,
				brine.CapJobLookup,
				brine.CapRunScopedReturns,
			},
			Run: verifyAsyncBackedRunScopedReturns,
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

func verifyAsyncBackedRunScopedReturns(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	recorder := &progressRecorder{}
	result, err := h.Client.Run(ctx, brine.Local("test.sleep", h.Target, brine.Args(1)), brine.WithRunObserver(recorder))
	require.NoError(t, err)
	require.True(t, result.OK())
	require.NotEmpty(t, result.JID)
	assertReturnedMinions(t, result, h.Minions)

	assert.Equal(t, recorder.expectedJID, result.JID)
	assert.ElementsMatch(t, h.Minions, recorder.expected)
	assert.ElementsMatch(t, h.Minions, recorder.returned)
	for _, eventJID := range recorder.returnJIDs {
		assert.Equal(t, eventJID, result.JID)
	}
}

type progressRecorder struct {
	expected    []string
	expectedJID string
	returned    []string
	returnJIDs  []string
}

func (r *progressRecorder) OnEvent(_ context.Context, event brine.Event) {
	switch payload := event.Payload.(type) {
	case brine.ExpectedMinionsPayload:
		r.expected = append([]string(nil), payload.Minions...)
		r.expectedJID = payload.JID
	case brine.MinionReturnedPayload:
		r.returned = append(r.returned, payload.Result.Minion)
		r.returnJIDs = append(r.returnJIDs, payload.Result.JID)
	}
}
