# Python command bridge transport

The Python transport is an MVP compatibility backend for environments where a
process can run with Salt's Python libraries and Salt master configuration. It is
not a full REST-parity backend.

## Capabilities

The command bridge currently advertises:

- synchronous local execution (`CapSynchronousRun`, `CapLocalRun`);
- responsive target resolution through `test.ping` (`CapTargetResolution`).

It intentionally does not advertise runner, wheel, async jobs, global events,
streaming returns, or raw lowstate. Those operations return `UnsupportedError`
through the normal Brine transport interface.

## Runtime model

For each request, the Go transport starts a configured command, sends one JSON
request to stdin, and reads newline-delimited JSON frames from stdout. The helper
first emits the resolved minion list, then emits one return frame per minion as
Salt's `cmd_iter` yields it, and finally emits a done frame. The Go transport
accumulates those frames into the final `Result` and emits Brine observer events
for expected minions and per-minion returns while `Client.Run` is still in
progress.

The bundled helper script, `brine_salt_bridge.py`, imports
`salt.client.LocalClient`, so it must run in an environment where Salt's Python
libraries and master configuration are available.

In practice this means the helper usually runs on the Salt master host, or via a
remote command such as SSH that executes on the Salt master host.

## Deployment options

### Installed helper on the Salt master

Install the helper somewhere root-owned and not writable by untrusted users:

```sh
install -o root -g root -m 0755 transports/python/brine_salt_bridge.py /usr/local/libexec/brine/brine_salt_bridge.py
```

Configure Brine with the Python interpreter that has Salt installed:

```go
transport, err := python.New(python.Config{
    Command: "/usr/bin/python3",
    Args:    []string{"/usr/local/libexec/brine/brine_salt_bridge.py"},
})
```

Salt onedir installations may use a different interpreter, such as
`/opt/saltstack/salt/bin/python3`.

### Operator-owned wrapper

A wrapper is often easier because Salt Python paths vary by packaging method:

```sh
#!/usr/bin/env bash
exec /opt/saltstack/salt/bin/python3 /usr/local/libexec/brine/brine_salt_bridge.py
```

Then configure:

```go
transport, err := python.New(python.Config{
    Command: "/usr/local/bin/brine-python-bridge",
})
```

### SSH wrapper

If the Go process does not run on the Salt master, execute the bridge remotely:

```go
transport, err := python.New(python.Config{
    Command: "ssh",
    Args:    []string{"salt-master", "/usr/local/bin/brine-python-bridge"},
})
```

The SSH account and wrapper should be locked down like any other Salt execution
entry point.

### Docker integration wrapper

The integration harness mounts the helper into the `salt-master` container and
runs it with:

```sh
test/integration/scripts/python-bridge.sh
```

This lets `just contract-python` run without Salt's Python runtime installed on
the Go test runner host.

## Security and operations

The bridge can execute Salt local functions on targeted minions. Treat the
configured command, wrapper, and helper file as trusted operational components.
Do not let untrusted users edit the helper or wrapper. Prefer root-owned files
with restrictive permissions and run the Go process with the minimum privileges
needed to execute the bridge.

The command bridge starts one process per request. Its per-minion frames provide
run-scoped progress, but it is not an async job API and it does not expose the
global Salt event stream. Use REST for richer production features such as async
job handling and event streams, or build a future long-lived Python helper if
Python parity and high concurrency become required.
