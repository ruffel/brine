# Brine tutorial

This is the five-minute path through Brine: create a client, run Salt work,
wrap common calls in application-owned helpers, and handle partial failures.
For architectural background, see [DESIGN.md](../DESIGN.md).

## 1. Create a client

REST is the production-oriented transport when `rest_cherrypy` is available:

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
```

Use the Python bridge only when the Go process can run a helper with Salt's
Python libraries and REST is unavailable:

```go
transport, err := python.New(python.Config{
    Command: "/usr/local/bin/brine-python-bridge",
})
```

Check [COMPATIBILITY.md](../COMPATIBILITY.md) before choosing a transport.

## 2. Run Salt work

Build a transport-neutral request, run it, and decode per-minion returns:

```go
result, err := client.Run(ctx, brine.Local(
    "test.ping",
    brine.List("web-1", "web-2"),
))
if err != nil {
    return err
}

pings, err := brine.DecodeByMinion[bool](result)
if err != nil {
    return err
}
```

Targets stay visible because Salt operators need to reason about them:
`brine.List`, `brine.Glob`, `brine.Compound`, and the other target builders map
to Salt targeting semantics.

## 3. Add a small typed wrapper

Keep application policy in your code. Brine provides the execution boundary and
normalization; your wrapper can expose the domain shape your app wants:

```go
type PackageAudit struct {
    Name      string
    Version   string
    Installed bool
}

func AuditPackage(
    ctx context.Context,
    client *brine.Client,
    target brine.Target,
    name string,
) (map[string]PackageAudit, error) {
    result, err := client.Run(ctx, brine.Local("pkg.version", target, brine.Args(name)))
    if err != nil {
        return nil, err
    }

    versions, err := brine.DecodeByMinion[string](result)
    if err != nil {
        return nil, err
    }

    audits := make(map[string]PackageAudit, len(versions))
    for minion, version := range versions {
        audits[minion] = PackageAudit{
            Name:      name,
            Version:   version,
            Installed: version != "",
        }
    }

    return audits, nil
}
```

The deterministic version of this example lives in
[`examples/customwrappers`](../examples/customwrappers/wrappers_test.go).

## 4. Handle partial failure

Salt jobs can partially succeed. Brine preserves returned minions, failed
minions, missing minions, and raw payloads:

```go
result, err := client.Run(ctx, brine.Local(
    "pkg.install",
    brine.List("api-1", "api-2", "api-3"),
    brine.Args("nginx"),
))

var execution *brine.ExecutionError
if errors.As(err, &execution) {
    log.Printf("partial=%t failed=%v missing=%v",
        execution.Partial(),
        execution.Failed(),
        execution.Missing(),
    )

    for _, failure := range execution.Result.Failures() {
        log.Printf("%s: %s", failure.Minion, failure.Failure.Message)
    }
}

_ = result
```

See [`examples/failures`](../examples/failures/failures_test.go) for a runnable
test example that includes a failed minion and a missing minion.

## 5. Try a runnable demo

For a deterministic tour that does not require Salt, run:

```sh
go run ./examples/demo
```

It uses the scripted transport under `examples/scripted` to show typed wrappers,
partial failures, and progress events without connecting to infrastructure.
