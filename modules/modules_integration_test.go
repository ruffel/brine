//go:build integration

package modules_test

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/brinetest"
	"github.com/ruffel/brine/modules"
	pytransport "github.com/ruffel/brine/transports/python"
	resttransport "github.com/ruffel/brine/transports/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationModulesREST(t *testing.T) {
	env := brinetest.Salt(t)
	transport, err := resttransport.New(resttransport.Config{BaseURL: env.URL, Auth: integrationAuth(env)})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	verifyFoundationalModules(t, client, expectedMinionIDs(env.ExpectedMinions))
}

func TestIntegrationModulesPython(t *testing.T) {
	env := brinetest.Salt(t)
	transport, err := pytransport.New(pytransport.Config{Command: integrationBridgeScript(t)})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	verifyFoundationalModules(t, client, expectedMinionIDs(env.ExpectedMinions))
}

func verifyFoundationalModules(t *testing.T, client *brine.Client, minions []string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	target := brine.List(minions...)

	cmd, err := modules.CmdRun(ctx, client, target, "printf brine", modules.CmdRunOptions{PrependPath: "/usr/local/bin"})
	require.NoError(t, err)
	assert.ElementsMatch(t, minions, keys(cmd.Nodes))
	for _, minion := range minions {
		assert.Equal(t, "brine", cmd.Nodes[minion])
		assert.Zero(t, cmd.RetCodes[minion])
	}

	hostnames, err := modules.NetworkHostnames(ctx, client, target)
	require.NoError(t, err)
	assert.ElementsMatch(t, minions, keys(hostnames.Nodes))
	for _, minion := range minions {
		assert.NotEmpty(t, hostnames.Nodes[minion])
	}

	ips, err := modules.NetworkIPAddrs(ctx, client, target)
	require.NoError(t, err)
	assert.ElementsMatch(t, minions, keys(ips.Nodes))
	for _, minion := range minions {
		assert.NotEmpty(t, ips.Nodes[minion])
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

func keys[T any](input map[string]T) []string {
	out := make([]string, 0, len(input))
	for key := range input {
		out = append(out, key)
	}

	return out
}
