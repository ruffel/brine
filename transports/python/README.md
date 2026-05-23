# Python command bridge transport

The Python transport is an MVP compatibility backend for environments where a
process can run with Salt's Python libraries and Salt master configuration. It is
not a full REST-parity backend.

## Capabilities

The command bridge currently advertises:

- synchronous local execution (`CapSynchronousRun`, `CapLocalRun`);
- asynchronous local dispatch and wait (`CapLocalStart`, `CapJobLookup`);
- synchronous runner execution (`CapRunnerRun`);
- responsive target resolution through `test.ping` (`CapTargetResolution`);
- run-scoped minion return progress during local runs (`CapRunScopedReturns`).

It intentionally does not advertise global events, generic streaming-return
subscriptions, batch execution, or raw lowstate. Those operations return
`UnsupportedError` through the normal Brine transport interface.

## Runtime model

For each operation, the Go transport starts a configured command, sends one JSON
request to stdin, and reads newline-delimited JSON frames from stdout. Requests
include `protocol_version: 1`; helpers should reject unknown protocol versions
rather than guessing at compatibility. Local `Run` first emits the expected
minion list, then emits one return frame per minion as Salt's `cmd_iter` yields
it, and finally emits a done frame. For explicit list targets, the expected list
is the original target list so offline or nonexistent minions can be marked
missing even though execution is sent only to responsive gathered minions. Local
`Start` sends operation `start` and expects a `started` frame with a jid and
expected minions; `Job.Wait` sends operation `wait` with that jid and expected
minions, then consumes the same minion/return/done frame stream while the helper
polls `jobs.lookup_jid`. The Go transport accumulates those frames into the
final `Result` and emits Brine observer events for expected minions and
per-minion returns while `Client.Run` or `Job.Wait` is collecting.

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

### Go configuration examples

Local installed helper:

```go
transport, err := python.New(python.Config{
    Command: "/usr/bin/python3",
    Args:    []string{"/usr/local/libexec/brine/brine_salt_bridge.py"},
})
```

Operator wrapper with async polling hints:

```go
transport, err := python.New(python.Config{
    Command:          "/usr/local/bin/brine-python-bridge",
    SaltMasterConfig: "/etc/salt/master",
    JobPollInterval:  time.Second,
    JobWaitTimeout:   10 * time.Minute,
})
```

`SaltMasterConfig` is optional. The bundled helper defaults to
`/etc/salt/master`, but Salt onedir or custom master layouts can point it at a
different file. The Go transport sends the value to the helper as
`BRINE_SALT_MASTER_CONFIG`; wrappers may also set that environment variable
directly when they need to keep Go configuration minimal.

When missing-minion detection matters under Python, prefer explicit list targets:

```go
req := brine.Local("state.sls", brine.List("web-1", "web-2", "web-3"), brine.Args("app"))
job, err := client.Start(ctx, req)
result, err := job.Wait(ctx)
```

For dynamic targets such as glob or compound, Python can only know the minions
Salt gathered before execution.

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

## Smoke tests

After installing a helper or wrapper, run a few live checks before handing it to
an application:

```sh
# Transport metadata and capabilities.
BRINE_BRIDGE_CMD=/usr/local/bin/brine-python-bridge \
  go run ./cmd/brine --transport python info

# Basic connectivity and target resolution.
BRINE_BRIDGE_CMD=/usr/local/bin/brine-python-bridge \
  go run ./cmd/brine --transport python local test.ping 'minion-*'

# State execution with normalized result data.
BRINE_BRIDGE_CMD=/usr/local/bin/brine-python-bridge \
  go run ./cmd/brine --transport python state sls 'minion-*' brine.success
```

Use `--target-type list` for explicit minion lists when missing-minion detection
is important:

```sh
BRINE_BRIDGE_CMD=/usr/local/bin/brine-python-bridge \
  go run ./cmd/brine --transport python --target-type list local test.ping 'web-1,web-2,web-3'
```

## Timeout and polling guidance

Use Go contexts for caller-side cancellation and deadlines. Use
`python.Config.JobWaitTimeout` to bound helper-side async wait loops and
`JobPollInterval` to control how often the helper asks Salt for
`jobs.lookup_jid`. For large clusters, start with:

- `JobPollInterval`: `1s`;
- `JobWaitTimeout`: `10m` to `30m`, depending on state duration;
- request context timeout: slightly longer than `JobWaitTimeout` to allow final
  result collection and rendering.

Salt module timeout remains separate and is configured with
`brine.ModuleTimeout(...)` on the request.

## Security and operations

The bridge can execute Salt local functions on targeted minions. Treat the
configured command, wrapper, and helper file as trusted operational components.
Do not let untrusted users edit the helper or wrapper. Prefer root-owned files
with restrictive permissions and run the Go process with the minimum privileges
needed to execute the bridge.

The command bridge starts one process per operation. It supports local async
`Start`/`Wait` by dispatching a Salt JID in one helper process and polling
`jobs.lookup_jid` in a later helper process. It still does not expose the global
Salt event stream or support high-concurrency multiplexing. Use REST for richer
production features such as global events and server-side event streaming, or
build a future long-lived Python helper if Python parity and high concurrency
become required.
