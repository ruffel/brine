//go:build integration

package jobs_test

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/brinetest"
	"github.com/ruffel/brine/jobs"
	pytransport "github.com/ruffel/brine/transports/python"
	resttransport "github.com/ruffel/brine/transports/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationJobsREST(t *testing.T) {
	env := brinetest.Salt(t)
	transport, err := resttransport.New(resttransport.Config{BaseURL: env.URL, Auth: integrationAuth(env)})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	verifyJobsActive(t, client)
	verifyJobsLookup(t, client, expectedMinionIDs(env.ExpectedMinions))
}

func TestIntegrationJobsPython(t *testing.T) {
	brinetest.Salt(t)
	transport, err := pytransport.New(pytransport.Config{Command: integrationBridgeScript(t)})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	verifyJobsActive(t, client)
}

func verifyJobsActive(t *testing.T, client *brine.Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	active, err := jobs.Active(ctx, client)
	require.NoError(t, err)
	require.NotNil(t, active.Raw)
}

func verifyJobsLookup(t *testing.T, client *brine.Client, minions []string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	lookupTarget := brine.List(minions...)
	result, err := client.Run(ctx, brine.Local("test.ping", lookupTarget))
	require.NoError(t, err)
	require.NotEmpty(t, result.JID)

	lookup, err := jobs.Lookup[map[string]bool](ctx, client, result.JID)
	require.NoError(t, err)
	for _, minion := range minions {
		assert.True(t, lookup.Value[minion], "%s should return true", minion)
	}
}

func integrationAuth(env brinetest.SaltEnv) resttransport.Authenticator {
	if env.AuthMode == "noauth" {
		return resttransport.NoAuth{}
	}

	return &resttransport.EAuth{Username: env.Username, Password: env.Password, EAuth: env.EAuth}
}

func integrationBridgeScript(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "test", "integration", "scripts", "python-bridge.sh"))
}

func expectedMinionIDs(count int) []string {
	minions := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		minions = append(minions, "minion-"+strconv.Itoa(i))
	}

	return minions
}
