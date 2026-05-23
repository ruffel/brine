package transportkit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccumulatorLargeExplicitListMissingMinions(t *testing.T) {
	t.Parallel()

	const total = 150
	missing := map[int]struct{}{
		17:  {},
		88:  {},
		149: {},
	}

	expected := make([]string, 0, total)
	for i := range total {
		expected = append(expected, fmt.Sprintf("node-%03d", i))
	}

	req := brine.Local("test.ping", brine.List(expected...))
	acc := transportkit.NewAccumulator(req)
	acc.SetExpected(context.Background(), "jid-large", expected)

	for i, minion := range expected {
		if _, skip := missing[i]; skip {
			continue
		}

		acc.AddMinion(context.Background(), brine.MinionResult{
			Minion:  minion,
			JID:     "jid-large",
			RetCode: 0,
			Return:  json.RawMessage(`true`),
		})
	}

	result, err := acc.ResultWithExecutionError()
	require.Error(t, err)
	var execution *brine.ExecutionError
	require.ErrorAs(t, err, &execution)
	require.NotNil(t, result)

	assert.False(t, result.OK())
	assert.Len(t, result.Expected, total)
	assert.Len(t, result.ByMinion, total-len(missing))
	assert.Equal(t, []string{"node-017", "node-088", "node-149"}, result.Missing)
	assert.Equal(t, result.Missing, execution.Missing())
}
