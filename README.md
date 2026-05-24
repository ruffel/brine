# brine

Brine is a Go library for working with [SaltStack](https://saltproject.io/)
through a transport-neutral API.

## Install

Brine currently targets Go `1.25.6` and is tested against Salt `3006.9` in the
integration topology. Until the first tagged release, install from the current
module head:

```sh
go get github.com/ruffel/brine@latest
```

## Quick start

Create a Salt REST transport, wrap it in a `brine.Client`, and execute Salt work
with a bounded context:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
defer cancel()

transport, err := rest.New(rest.Config{
    BaseURL: "https://salt.example.com:8000",
    Auth:    rest.PAMAuth("saltapi", password),
})
if err != nil {
    return err
}

client, err := brine.New(transport)
if err != nil {
    return err
}
defer client.Close()

result, err := client.Run(ctx, brine.Local("test.ping", brine.List("minion-1")))
if err != nil {
    var execution *brine.ExecutionError
    if errors.As(err, &execution) {
        // execution.Result contains partial returns and missing minions.
        return fmt.Errorf("salt execution failed on %v: %w", execution.Failed(), err)
    }

    return err
}

pings, err := brine.DecodeByMinion[bool](result)
```

## Choose a transport

REST is the default production transport when `rest_cherrypy` is available.
It supports the broadest MVP surface: local and runner execution, raw lowstate,
local async jobs, events, run-scoped progress, batch execution, target
resolution, and the strongest missing-minion detection.

The Python bridge is a compatibility backend for environments where REST access
is unavailable. It supports local and runner execution, local async jobs,
local batch execution, run-scoped progress, and target resolution, but
intentionally does not support global events or raw lowstate. It starts one
helper process per operation, so REST is still the better fit for
high-concurrency services.

See `COMPATIBILITY.md` for the live-tested matrix and Python-specific caveats.
For a short hands-on path through the API, read `doc/TUTORIAL.md`.

## REST setup checklist

For REST deployments, configure Salt's `rest_cherrypy` endpoint and allow the
Salt API user to run the clients Brine needs:

- `local` and `local_async` for minion execution;
- `runner` for job lookup and runner calls;
- event stream access when using `Client.Events`, `Job.Events`, or progress
  observers;
- batch execution permissions if callers use `brine.BatchCount` or
  `brine.BatchPercent`.

The integration topology keeps the minimal local test shape in
`test/integration/salt/master.d/brine.conf`. Production environments should use
their normal TLS, authentication, and eauth policies rather than the test
credentials from that file.

## Execution safety

The REST transport uses Salt `local_async` plus `jobs.lookup_jid` for local
`Run` calls by default. That allows Brine to track the minions Salt expected to
return and mark missing minions as execution failures instead of silently
succeeding with only responders.

If Salt reports that an async target matched zero minions, Brine treats that as
an execution failure. In infrastructure workflows, applying a state to no
minions is usually safer to surface than to ignore.

`LocalRunModeDirect` uses Salt's synchronous `local` client. For explicit list
targets, Brine treats the list as the expected minion set and marks omitted
returns as missing. For glob or compound targets, prefer the default async mode
when offline-minion detection matters because synchronous Salt returns may only
include responders.

## Boolean returns

Salt modules often use `false` as meaningful data. For example,
`service.status` returns `false` when a service is stopped, and
`file.file_exists` returns `false` when a path is absent. Brine therefore does
not treat every bare `false` as an execution failure. Typed helpers for these
modules request Salt full returns so retcodes and `success=false` can still be
classified as failures.

## Errors

Transport, authentication, protocol, unsupported-operation, and Salt execution
failures are exposed through typed errors that match these sentinels:

- `brine.ErrTransport`
- `brine.ErrAuth`
- `brine.ErrProtocol`
- `brine.ErrUnsupported`
- `brine.ErrExecution`

When Salt communication succeeds but execution fails, `Client.Run` and
`Job.Wait` return `*brine.ExecutionError`. The embedded `Result` preserves raw
payloads, returned minions, failed minions, and missing minions for diagnostics
and recovery.

## Python bridge transport

`transports/python` is a capability-limited command bridge for environments
where direct REST access is unavailable or unsuitable. Brine starts the helper
process configured by `python.Config.Command`, sends one JSON request on stdin,
and reads JSON response or streaming frames from stdout. See the
`transports/python` package documentation for the bridge protocol, supported
capabilities, and unsupported-error mapping.

## MVP limitations

- Python bridge missing-minion detection is strongest for explicit list targets;
  dynamic targets depend on what Salt's `gather_minions` returns before
  execution.
- Python bridge async waits use a short-lived helper process that polls
  `jobs.lookup_jid`; prefer REST for global event streams or high-concurrency
  services.
- Runner async and lowstate async dispatch are intentionally unsupported until
  their Salt response and lookup semantics are covered by fixtures.
- Typed wheel APIs were removed from the root API. Use local execution, runner
  execution, or raw lowstate where a transport advertises lowstate support.

## Testing and implementing transports

Use `transports/mock` for unit tests and the public `brinetest` package as a
transport-author contract suite. `brinetest` verifies normalized Brine semantics
for the capabilities a transport advertises; it does not start, stop, or
configure Salt. Docker/Salt lifecycle is owned by `test/integration` and the
Justfile recipes.

Transport authors can use `transportkit` to build normalized results and
classify common Salt failure shapes consistently with Brine's built-in
transports. See `doc/TUTORIAL.md`, `COMPATIBILITY.md`, `TESTING.md`,
`RELEASE.md`, and `test/integration/README.md` for the quick-start path,
REST/Python capability matrix, contract coverage, and release-readiness
workflow. `DESIGN.md` is an archived design snapshot; newer design decisions
live in `doc/adr/`.

## Repository tools

- `cmd/brine-compatcheck` runs integration-tagged contract suites and prints the
  REST/Python compatibility matrix as a styled table or JSON. It can list
  contracts and filter by category or contract ID. It is a developer
  compatibility reporter, not a live diagnostic CLI.
- `cmd/brine` is the live diagnostic CLI for exercising a configured transport
  and printing normalized JSON or a generic summary for manual debugging. It
  uses Cobra for command structure and Koanf for flag/env configuration merging.
- `examples/...` contains deterministic API examples for custom typed wrappers,
  partial failures, progress observers, app-owned formatting, and a runnable
  scripted demo.
- `tools/...` is a separate module for optional demos and richer tooling. Root
  commands can use focused CLI dependencies when useful; richer experiments and
  TUI-style tools belong under `tools`, not in root examples or commands.
