package resultaccumulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/internal/resultaccumulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccumulatorBuildsResultAndEmitsProgress(t *testing.T) {
	t.Parallel()

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	req := brine.Local("test.ping", brine.Glob("*"))
	acc := resultaccumulator.New(req)

	acc.AddRaw(json.RawMessage(`{"type":"minions"}`))
	acc.SetExpected(ctx, "jid-1", []string{"minion-2", "minion-1"})
	acc.AddMinion(ctx, brine.MinionResult{
		Minion:  "minion-1",
		RetCode: 0,
		Return:  json.RawMessage(`true`),
		Raw:     json.RawMessage(`{"return":true}`),
	})

	result := acc.Result()
	require.NotNil(t, result)
	assert.Equal(t, "jid-1", result.JID)
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Expected)
	assert.Equal(t, []string{"minion-2"}, result.Missing)
	assert.Equal(t, "jid-1", result.ByMinion["minion-1"].JID)
	assert.Contains(t, string(result.Raw), `"type":"minions"`)

	require.Len(t, recorder.events, 2)
	assert.Equal(t, brine.EventExpectedMinions, recorder.events[0].Type)
	assert.Equal(t, brine.EventMinionReturned, recorder.events[1].Type)
	assert.Equal(t, "minion-1", recorder.events[1].Minion)
}

func TestAccumulatorReplacesDuplicateReturnButEmitsOnce(t *testing.T) {
	t.Parallel()

	recorder := &eventRecorder{}
	ctx := brine.WithEmitter(context.Background(), recorder)
	acc := resultaccumulator.New(brine.Local("cmd.run", brine.List("minion-1")))

	acc.AddMinion(ctx, brine.MinionResult{Minion: "minion-1", RetCode: 0, Return: json.RawMessage(`"old"`)})
	acc.AddMinion(ctx, brine.MinionResult{Minion: "minion-1", RetCode: 0, Return: json.RawMessage(`"new"`)})

	result := acc.Result()
	var output string
	require.NoError(t, result.ByMinion["minion-1"].Decode(&output))
	assert.Equal(t, "new", output)

	returned := 0
	for _, event := range recorder.events {
		if event.Type == brine.EventMinionReturned {
			returned++
		}
	}
	assert.Equal(t, 1, returned)
}

func TestAccumulatorResultWithExecutionError(t *testing.T) {
	t.Parallel()

	acc := resultaccumulator.New(brine.Local("cmd.run", brine.List("minion-1")))
	acc.SetExpected(context.Background(), "jid-1", []string{"minion-1"})
	acc.AddMinion(context.Background(), brine.MinionResult{
		Minion:  "minion-1",
		JID:     "jid-1",
		RetCode: 1,
		Return:  json.RawMessage(`"failed"`),
		Failure: &brine.Failure{Kind: brine.FailureRetCode, Message: "retcode 1"},
	})

	result, err := acc.ResultWithExecutionError()
	require.Error(t, err)
	require.NotNil(t, result)
	assert.False(t, result.OK())
	assert.True(t, errors.Is(err, brine.ErrExecution))
}

type eventRecorder struct{ events []brine.Event }

func (r *eventRecorder) Emit(_ context.Context, event brine.Event) {
	r.events = append(r.events, event)
}
