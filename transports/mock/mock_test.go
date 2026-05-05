package mock_test

import (
	"context"
	"io"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transports/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScriptLocalSuccess(t *testing.T) {
	t.Parallel()

	transport := mock.ScriptLocalSuccess("minion-1", "minion-2")

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	require.True(t, result.OK())
	assert.Equal(t, []string{"minion-1", "minion-2"}, result.Returned())

	calls := transport.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "run", calls[0].Operation)

	transport.AssertExpectations(t)
}

func TestExpectLocalSuccess(t *testing.T) {
	t.Parallel()

	transport := mock.ExpectLocalSuccess("test.echo", brine.List("minion-1"), map[string]any{"minion-1": "hello"})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := client.Run(context.Background(), brine.Local("test.echo", brine.List("minion-1")))
	require.NoError(t, err)

	var decoded string
	require.NoError(t, result.ByMinion["minion-1"].Decode(&decoded))
	assert.Equal(t, "hello", decoded)

	transport.AssertExpectations(t)
}

func TestScriptExecutionError(t *testing.T) {
	t.Parallel()

	client, err := brine.New(mock.ScriptExecutionError("minion-1"))
	require.NoError(t, err)

	result, err := client.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1")))
	require.Error(t, err)

	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	require.NotNil(t, result)
	assert.False(t, result.OK())
	assert.Equal(t, []string{"minion-1"}, executionError.Failed())
}

func TestStream(t *testing.T) {
	t.Parallel()

	stream := mock.NewStream(brine.NewEvent(brine.EventRawSalt, brine.RawSaltPayload{Tag: "salt/test"}))

	event, err := stream.Recv(context.Background())
	require.NoError(t, err)
	assert.Equal(t, brine.EventRawSalt, event.Type)

	_, err = stream.Recv(context.Background())
	require.ErrorIs(t, err, io.EOF)
}
