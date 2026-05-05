# Migrating process-wrapper callers to Brine

This guide is for callers that currently shell out to Salt commands or a Python
helper, parse ad-hoc stdout, and render progress directly from subprocess output.
Brine moves those concerns onto the public `Client`, `Middleware`, `Observer`,
and `Result` APIs.

## Recommended migration shape

1. Construct one transport at the process boundary.
2. Wrap it in a `brine.Client`.
3. Replace subprocess calls with `Client.Run` or `Client.Start` calls.
4. Decode normalized `Result` values instead of parsing stdout.
5. Render progress through observers or async event streams.
6. Move request mutation, such as pillar or target expansion, to caller-owned
   middleware.
7. Configure retry policy explicitly with `brine.WithRetry`.

## Replacing synchronous subprocess calls

Old process-wrapper code usually has to infer success from exit status and parse
Salt output manually. With Brine, execution success and partial failure are
represented directly:

- `Client.Run` returns a normalized `Result`;
- execution failures return `ExecutionError` with the same `Result` attached;
- `Result.Returned`, `Result.Failures`, and `states.Decode` provide structured
  rendering inputs;
- raw payloads remain available on `Result.Raw` and `MinionResult.Raw` for
  diagnostics.

When preserving existing user-visible output, handle `ExecutionError`
deliberately and render the attached partial result instead of discarding it.
See `Example_migrationPartialResults` in `migration_examples_test.go`.

## Moving progress rendering

Do not parse subprocess progress lines for normal operation. Prefer one of these
patterns:

- pass `brine.WithObserver` when constructing the client for process-wide
  progress;
- pass `brine.WithRunObserver` for one call;
- use `Client.Start`, `Job.Events`, and `Job.Wait` for async workflows that need
  Salt event-stream progress and final job lookup;
- keep output formatting in the caller, not in the transport.

`middleware_examples_test.go` includes small terminal-progress and JSON-line
observer adapters that callers can copy and adapt.

## Moving orchestration-specific request mutation

Product-specific orchestration logic should not be added to core transports.
Use middleware for policies such as:

- adding kwargs;
- merging per-run pillar;
- transforming targets;
- fetching runner/local data and injecting it into pillar.

If middleware needs supporting Salt data, call an unwrapped handler via
`Client.Unwrap()` or the bare transport. This avoids recursively applying the
same middleware to its own helper requests.

## Retrying malformed state returns

Malformed state returns should be retried by policy, not hidden in transport
normalization. Configure retry middleware explicitly:

```go
client, err := brine.New(transport, brine.WithMiddleware(brine.WithRetry(brine.RetryConfig{
    MaxAttempts: 2,
    Predicate:   states.MalformedStateRetryPredicate,
})))
```

The retry middleware retargets only retryable failed minions with a list target
and merges retry results back into the original result. See
`Example_migrationMalformedStateRetry` in `migration_examples_test.go`.

## Testing migrated workflows

Use `transports/mock` for fast unit tests around caller policy and output
rendering. Use the REST integration harness and `brinetest` contracts to verify
transport-neutral behavior against a live Salt topology.

For a migrated workflow, prefer tests that assert semantic projections:

- request kind, function, target, args, kwargs, and pillar shape;
- returned, failed, and missing minions;
- decoded state summaries;
- progress events emitted to the caller's observer;
- retry count and retry target subset.

Avoid raw JSON equality except for fixture normalizer tests.
