//go:build integration

package python

import (
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/brinetest"
	"github.com/stretchr/testify/require"
)

func TestIntegrationPythonContracts(t *testing.T) {
	env := brinetest.Salt(t)
	client := newIntegrationClient(t)
	minions := expectedMinionIDs(env.ExpectedMinions)

	brinetest.Verify(t, brinetest.Harness{
		Name:    "python",
		Client:  client,
		Target:  brine.List(minions...),
		Minions: minions,
		States: brinetest.StateNames{
			Success:        "brine.success",
			Failure:        "brine.fail",
			PartialFailure: "brine.conditional_fail",
		},
		PartialFailedMinions: []string{"minion-2"},
	})
}

func newIntegrationClient(t *testing.T) *brine.Client {
	t.Helper()

	transport, err := New(Config{Command: integrationBridgeScript(t)})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func integrationBridgeScript(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "test", "integration", "scripts", "python-bridge.sh"))
}

func expectedMinionIDs(count int) []string {
	minions := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		minions = append(minions, "minion-"+strconv.Itoa(i))
	}

	return minions
}
