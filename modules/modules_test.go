package modules_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/modules"
	"github.com/ruffel/brine/transports/mock"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCmdRun(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "cmd.run", req.Function)
			require.Equal(t, []any{"printf brine"}, req.Args)
			require.Equal(t, "/usr/local/bin", req.Kwargs["prepend_path"])

			return localResult(req, map[string]returnValue{"minion-1": {body: `"brine"`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.CmdRun(ctx(), client, brine.List("minion-1"), "printf brine", modules.CmdRunOptions{PrependPath: "/usr/local/bin"})
	require.NoError(t, err)
	assert.Equal(t, "brine", result.Nodes["minion-1"])
	assert.Zero(t, result.RetCodes["minion-1"])
}

func TestCmdRetcode(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "cmd.retcode", req.Function)
			require.Equal(t, []any{"true"}, req.Args)
			require.Equal(t, "/usr/local/bin", req.Kwargs["prepend_path"])

			return localResult(req, map[string]returnValue{"minion-1": {body: `0`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.CmdRetcode(ctx(), client, brine.List("minion-1"), "true", modules.CmdRunOptions{PrependPath: "/usr/local/bin"})
	require.NoError(t, err)
	assert.Zero(t, result.Nodes["minion-1"])
}

func TestCmdRunOneRetCodeError(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			result := localResult(req, map[string]returnValue{"minion-1": {body: `"no"`, retcode: 2}})

			return result, brine.NewExecutionError(result, nil)
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	output, retcode, err := modules.CmdRunOne(ctx(), client, "minion-1", "false", modules.CmdRunOptions{})
	require.Error(t, err)
	assert.Equal(t, "no", output)
	assert.Equal(t, 2, retcode)
}

func TestTestPing(t *testing.T) {
	t.Parallel()

	client := clientForReturn(t, "test.ping", `true`)
	result, err := modules.TestPing(ctx(), client, brine.List("minion-1"))
	require.NoError(t, err)
	assert.True(t, result.Nodes["minion-1"])
}

func TestTestVersion(t *testing.T) {
	t.Parallel()

	client := clientForReturn(t, "test.version", `"3006.9"`)
	result, err := modules.TestVersion(ctx(), client, brine.List("minion-1"))
	require.NoError(t, err)
	assert.Equal(t, "3006.9", result.Nodes["minion-1"])
}

func TestGrainsID(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "grains.get", req.Function)
			require.Equal(t, []any{"id"}, req.Args)

			return localResult(req, map[string]returnValue{"minion-1": {body: `"minion-1"`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.GrainsID(ctx(), client, brine.List("minion-1"))
	require.NoError(t, err)
	assert.Equal(t, "minion-1", result.Nodes["minion-1"])
}

func TestGrainsGet(t *testing.T) {
	t.Parallel()

	client := clientForReturn(t, "grains.get", `"Debian"`)
	result, err := modules.GrainsGet[string](ctx(), client, brine.List("minion-1"), "os")
	require.NoError(t, err)
	assert.Equal(t, "Debian", result.Nodes["minion-1"])
}

func TestFileExists(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "file.file_exists", req.Function)
			require.Equal(t, []any{"/etc/salt/minion.d/brine.conf"}, req.Args)
			require.True(t, req.Options.FullReturn)

			return localResult(req, map[string]returnValue{"minion-1": {body: `true`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.FileExists(ctx(), client, brine.List("minion-1"), "/etc/salt/minion.d/brine.conf")
	require.NoError(t, err)
	assert.True(t, result.Nodes["minion-1"])
}

func TestDirectoryExists(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "file.directory_exists", req.Function)
			require.Equal(t, []any{"/etc/salt/minion.d"}, req.Args)
			require.True(t, req.Options.FullReturn)

			return localResult(req, map[string]returnValue{"minion-1": {body: `true`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.DirectoryExists(ctx(), client, brine.List("minion-1"), "/etc/salt/minion.d")
	require.NoError(t, err)
	assert.True(t, result.Nodes["minion-1"])
}

func TestServiceStatus(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "service.status", req.Function)
			require.Equal(t, []any{"sshd"}, req.Args)
			require.Empty(t, req.Kwargs)
			require.True(t, req.Options.FullReturn)

			return localResult(req, map[string]returnValue{"minion-1": {body: `true`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.ServiceStatus(ctx(), client, brine.Glob("*"), "sshd", modules.ServiceStatusOptions{})
	require.NoError(t, err)
	assert.True(t, result.Nodes["minion-1"])
}

func TestServiceStatusRegex(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, "service.status", req.Function)
			require.Equal(t, []any{"^(web|db).*"}, req.Args)
			require.Equal(t, true, req.Kwargs["regex"])
			require.True(t, req.Options.FullReturn)

			return localResult(req, map[string]returnValue{"minion-1": {body: `{"web":true,"db":false}`}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.ServiceStatusRegex(ctx(), client, brine.Glob("*"), "^(web|db).*", modules.ServiceStatusRegexOptions{})
	require.NoError(t, err)
	assert.True(t, result.Nodes["minion-1"]["web"])
	assert.False(t, result.Nodes["minion-1"]["db"])
}

func TestNetworkInterfaces(t *testing.T) {
	t.Parallel()

	client := clientForReturn(t, "network.interfaces", `{"eth0":{"hwaddr":"aa:bb","up":true,"inet":[{"address":"10.0.0.1"}]}}`)
	result, err := modules.NetworkInterfaces(ctx(), client, brine.List("minion-1"))
	require.NoError(t, err)

	ifaces := result.Nodes["minion-1"]
	assert.True(t, ifaces.Has("eth0"))
	assert.True(t, ifaces.IsUp("eth0"))
	assert.Equal(t, []string{"10.0.0.1"}, ifaces.IPs("eth0"))
	name, ok := ifaces.FindByIP("10.0.0.1")
	assert.True(t, ok)
	assert.Equal(t, "eth0", name)
}

func TestNetworkIPAddrs(t *testing.T) {
	t.Parallel()

	client := clientForReturn(t, "network.ip_addrs", `["10.0.0.1","127.0.0.1"]`)
	result, err := modules.NetworkIPAddrs(ctx(), client, brine.List("minion-1"))
	require.NoError(t, err)
	assert.True(t, result.Nodes["minion-1"].Has("10.0.0.1"))
	assert.False(t, result.Nodes["minion-1"].Has("192.0.2.1"))
}

func TestNetworkHostnames(t *testing.T) {
	t.Parallel()

	client := clientForReturn(t, "network.get_hostname", `"minion-1.example"`)
	result, err := modules.NetworkHostnames(ctx(), client, brine.List("minion-1"))
	require.NoError(t, err)
	assert.Equal(t, "minion-1.example", result.Nodes["minion-1"])
}

func TestRunLocalReturnsPartialResultWithExecutionError(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			result := localResult(req, map[string]returnValue{
				"minion-1": {body: `"ok"`},
				"minion-2": {body: `"bad"`, retcode: 1},
			})

			return result, brine.NewExecutionError(result, nil)
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.RunLocal[string](ctx(), client, brine.Local("cmd.run", brine.Glob("*"), brine.Args("true")))
	require.Error(t, err)
	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	assert.Equal(t, "ok", result.Nodes["minion-1"])
	assert.Equal(t, []string{"minion-2"}, result.FailedNodes)
}

func TestRunLocalReturnsPartialResultWithDecodeError(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			return localResult(req, map[string]returnValue{
				"minion-1": {body: `"ok"`},
				"minion-2": {body: `{"unexpected":true}`},
			}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := modules.RunLocal[string](ctx(), client, brine.Local("cmd.run", brine.Glob("*"), brine.Args("true")))
	require.Error(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "ok", result.Nodes["minion-1"])
	assert.NotContains(t, result.Nodes, "minion-2")

	var decodeError *modules.DecodeError
	require.ErrorAs(t, err, &decodeError)
	assert.Equal(t, "minion-2", decodeError.Minion)
	assert.Equal(t, "cmd.run", decodeError.Function)
}

func clientForReturn(t *testing.T, function string, body string) *brine.Client {
	t.Helper()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			require.Equal(t, function, req.Function)

			return localResult(req, map[string]returnValue{"minion-1": {body: body}}), nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	return client
}

type returnValue struct {
	body    string
	retcode int
}

func localResult(req brine.Request, returns map[string]returnValue) *brine.Result {
	result := &brine.Result{
		JID:      "mock-jid",
		Request:  &req,
		Expected: make([]string, 0, len(returns)),
		ByMinion: make(map[string]brine.MinionResult, len(returns)),
	}

	for minion, value := range returns {
		ret := brine.MinionResult{Minion: minion, JID: result.JID, RetCode: value.retcode, Return: json.RawMessage(value.body)}
		if value.retcode != 0 {
			ret.Failure = &brine.Failure{Kind: brine.FailureRetCode, Message: "retcode"}
		}

		result.Expected = append(result.Expected, minion)
		result.ByMinion[minion] = ret
	}

	return result
}

func ctx() context.Context { return context.Background() }
