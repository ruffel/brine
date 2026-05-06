package jobs_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/jobs"
	"github.com/ruffel/brine/transports/mock"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestActive(t *testing.T) {
	t.Parallel()

	client := clientForRunner(t, "jobs.active", nil, `{}`)
	result, err := jobs.Active(context.Background(), client)
	require.NoError(t, err)
	assert.Empty(t, result.Value)
	assert.True(t, result.Raw.IsRunner())
}

func TestList(t *testing.T) {
	t.Parallel()

	client := clientForRunner(t, "jobs.list_jobs", nil, `{"jid-1":{"Function":"test.ping"}}`)
	result, err := jobs.List(context.Background(), client)
	require.NoError(t, err)
	assert.Contains(t, result.Value, "jid-1")
}

func TestLookup(t *testing.T) {
	t.Parallel()

	client := clientForRunner(t, "jobs.lookup_jid", []any{"jid-1"}, `{"minion-1":true}`)
	result, err := jobs.Lookup[map[string]bool](context.Background(), client, "jid-1")
	require.NoError(t, err)
	assert.True(t, result.Value["minion-1"])
}

func TestLookupRequiresJID(t *testing.T) {
	t.Parallel()

	_, err := jobs.Lookup[map[string]bool](context.Background(), nil, "")
	require.Error(t, err)
}

func clientForRunner(t *testing.T, function string, args []any, scalar string) *brine.Client {
	t.Helper()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapRunnerRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, brine.KindRunner, req.Kind)
			require.Equal(t, function, req.Function)
			if args != nil {
				require.Equal(t, args, req.Args)
			}

			return &brine.Result{Request: &req, Scalar: json.RawMessage(scalar)}, nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	return client
}
