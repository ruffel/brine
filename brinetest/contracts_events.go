package brinetest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func eventContracts() []TestCase {
	return []TestCase{
		{
			Category:     CategoryEvents,
			Name:         "job-event-stream-opens",
			Description:  "job events opens an event stream filtered to the job JID",
			Capabilities: []brine.Capability{brine.CapLocalStart, brine.CapEvents},
			Run:          verifyJobEventStreamOpens,
		},
		{
			Category:     CategoryEvents,
			Name:         "job-event-receives-matching-jid",
			Description:  "job events receives at least one matching job event",
			Capabilities: []brine.Capability{brine.CapLocalStart, brine.CapEvents},
			Run:          verifyJobEventReceivesMatchingJID,
		},
	}
}

func verifyJobEventStreamOpens(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, brine.Local("test.sleep", h.Target, brine.Args(1)))
	require.NoError(t, err)

	stream, err := job.Events(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, stream.Close()) }()

	result, err := job.Wait(ctx)
	require.NoError(t, err)
	assert.True(t, result.OK())
}

func verifyJobEventReceivesMatchingJID(t *testing.T, h Harness) {
	t.Helper()

	ctx, cancel := contractContext(t, defaultAsyncTimeout)
	defer cancel()

	job, err := h.Client.Start(ctx, brine.Local("test.sleep", h.Target, brine.Args(2)))
	require.NoError(t, err)

	stream, err := job.Events(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, stream.Close()) }()

	event, err := stream.Recv(ctx)
	require.NoError(t, err)
	assert.Equal(t, job.ID(), event.JID)
	assert.Contains(t, []brine.EventType{brine.EventRawSalt, brine.EventMinionReturned}, event.Type)

	result, err := job.Wait(ctx)
	require.NoError(t, err)
	assert.True(t, result.OK())
}
