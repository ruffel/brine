# brine

A Go library for working with [SaltStack](https://saltproject.io/) through a transport-neutral API.

## Status

Brine is under active implementation. The root API, mock transport, state helpers, retry middleware, integration harness, REST synchronous transport, REST local async jobs, REST event streaming groundwork, and the `brinetest` transport contract suite are in place.

Current production-oriented support target:

- Salt `v3006`
- `rest_cherrypy` exposed as a local Salt master endpoint
- synchronous local, runner, wheel, and lowstate REST calls
- asynchronous local REST dispatch with final result collection through `jobs.lookup_jid`
- REST event stream subscription through `rest_cherrypy` server-sent events

See also:

- `DESIGN.md` for API and transport design
- `IMPLEMENTATION_PLAN.md` for phased work
- `TESTING.md` for fixture and integration strategy
- `MIGRATION.md` for moving process-wrapper callers to Brine

## Tiny example

```go
package main

import (
	"context"
	"fmt"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transports/rest"
)

func main() {
	transport, err := rest.New(rest.Config{
		BaseURL: "http://127.0.0.1:8000",
		Auth:    rest.PAMAuth("saltapi", "saltapi"),
	})
	if err != nil {
		panic(err)
	}

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	result, err := client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Returned())
}
```

For hermetic unit tests, use `transports/mock` instead of a real Salt master. Transport implementations should also run the `brinetest` contract suite against a deterministic Salt environment to verify normalized API parity across advertised capabilities. For REST, start the integration harness and run `just contract-rest`.

Use `Client.Capabilities` for cheap feature checks. `Client.Info` is diagnostic metadata and transports may perform network probes; REST probes Salt's `test.get_opts` runner at most once to populate `SaltVersion` when available.

REST remains the production-oriented backend for the current Salt `v3006` localhost-master target. A minimal Python command bridge is also available for compatibility-oriented local execution in environments where running against Salt's Python libraries is practical. The Python bridge intentionally advertises a smaller capability set than REST.

## MVP compatibility matrix

| Capability | REST transport | Python command bridge |
| --- | --- | --- |
| local `test.ping` / `cmd.run` / `service.status` | yes | yes |
| local `state.sls` | yes | yes, through synchronous local execution |
| structured args/kwargs | yes | yes |
| responsive target resolution | yes | yes |
| runner sync calls | yes | no |
| wheel sync calls | yes | no |
| local async start/wait | yes | no |
| event stream | best-effort SSE | no |
| raw lowstate | yes | no |

Use `just contract-rest` and `just contract-python` against the Docker Salt topology to verify advertised transport behavior.

## Middleware and orchestration boundaries

Use `brine.WithMiddleware` for caller-owned synchronous `Run` policy such as
adding kwargs, merging per-run pillar, or rewriting targets. Middleware receives
and returns transport-neutral `brine.Request` values, so orchestration-specific
state expansion belongs in caller middleware rather than in core transports.
`Client.Start` dispatches directly to the transport today; async request mutation
and retry policies should be applied by the caller before `Start` unless a future
async middleware chain is added.

Middleware that needs supporting Salt data, such as runner output used to build
pillar, should call an unwrapped handler (`Client.Unwrap()` or the bare
transport) for its internal runner/local requests. That avoids recursively
applying the same middleware to the middleware's own helper calls.

See the compile-time examples in `middleware_examples_test.go` for:

- static pillar injection;
- target transformation;
- fetching `manage.alived` through an unwrapped handler and merging it into
  pillar;
- terminal progress and JSON-line observer adapters;
- mock-backed tests for caller-owned middleware.

## Request metadata

Use `brine.Metadata` or `brine.MetadataMap` for caller-owned annotations such as
trace IDs, change tickets, workflow names, or UI correlation data. Metadata is
not sent to Salt by core transports. It is carried on `brine.Request`, emitted to
observers in request events, and preserved on `Result.Request` so middleware and
callers can make policy decisions without changing Salt kwargs.

If metadata should affect Salt execution, make that conversion explicit in
caller middleware. For example, middleware can read a `ticket` metadata value and
merge it into per-run pillar. Request observer events contain the caller's
original request; inspect `Result.Request` on completion when you need the final
transport-level request after middleware. See `metadata_examples_test.go` for
compile-time examples of metadata-driven middleware and observer access.

## Transport author notes

Transport implementations should use `brine.DescribeTarget` rather than writing
their own target type switches. The helper centralizes Brine's sealed target
mapping and gives future target types one exhaustiveness point. REST uses this
helper before converting target descriptors to Salt's `tgt` and `tgt_type`
fields.

## Async jobs and events

`Client.Start` returns the broad `brine.Job` interface. Local async jobs may also implement `brine.LocalJob`, which exposes expected minions:

```go
job, err := client.Start(ctx, brine.Local("test.ping", brine.Glob("*")))
if err != nil {
	panic(err)
}

if local, ok := job.(brine.LocalJob); ok {
	fmt.Println(local.ExpectedMinions())
}

stream, err := job.Events(ctx)
if err != nil {
	panic(err)
}
defer stream.Close()

result, err := job.Wait(ctx)
if err != nil {
	panic(err)
}
fmt.Println(result.OK())
```

For the REST transport today:

- local async jobs are dispatched with Salt's `local_async` client;
- `Job.Wait` polls `runner.jobs.lookup_jid`, using `rest.Config.JobPollInterval` when set, and caches terminal results;
- `Job.Events` is backed by the global `/events` SSE stream filtered by JID;
- event filtering is best-effort because it depends on Salt's emitted tags/data;
- Salt return events such as `salt/job/<jid>/ret/<minion>` are normalized to `EventMinionReturned` when possible;
- other Salt events are returned as `EventRawSalt` with the raw payload preserved.
