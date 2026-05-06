# brine

Brine is a Go library for working with [SaltStack](https://saltproject.io/)
through a transport-neutral API.

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

## Testing transports

Use `transports/mock` for unit tests and `brinetest` contract tests for custom
transport implementations. The repository also includes an opt-in Docker Salt
integration harness under `test/integration` for validating REST and Python
bridge behavior against real Salt.
