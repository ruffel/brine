# brine

A Go library for working with [SaltStack](https://saltproject.io/) through a transport-neutral API.

## Status

Brine is under active implementation. The root API, mock transport, state helpers, retry middleware, integration harness, REST synchronous transport, REST local async jobs, and REST event streaming groundwork are in place.

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

For hermetic unit tests, use `transports/mock` instead of a real Salt master.

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
- `Job.Wait` polls `runner.jobs.lookup_jid` and caches the final result;
- `Job.Events` is backed by the global `/events` SSE stream filtered by JID;
- event filtering is best-effort because it depends on Salt's emitted tags/data;
- Salt return events such as `salt/job/<jid>/ret/<minion>` are normalized to `EventMinionReturned` when possible;
- other Salt events are returned as `EventRawSalt` with the raw payload preserved.
