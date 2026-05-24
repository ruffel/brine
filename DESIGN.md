# Brine Design

Status: archived design snapshot. The root API, mock transport, REST
transport, MVP Python command bridge, integration harness, contract suite,
middleware examples, and migration guidance are implemented. Python support is
intentionally capability-limited unless a future parity backend is required.
Future design changes are recorded as ADRs under `doc/adr/`.

Brine is a Go package for interacting with SaltStack from applications and CLI
programs. The package should replace ad-hoc process invocation with a clean,
testable API while still exposing the Salt concepts that operators need to
reason about distributed execution.

The design assumes there is an existing Go caller and an existing Python-based
bridge whose behavior must be accounted for. Those details are treated as
requirements and migration constraints, not as public API concepts.

Initial support target:

- Salt `v3006`.
- `rest_cherrypy` is available as a localhost endpoint on the Salt master node.
- The CLI and Salt master run on the same node for the primary deployment shape.
- REST is the current production-oriented backend.
- The Python command bridge targets a reduced compatibility capability set;
  future REST-level Python parity would require a long-lived helper. Python must
  advertise exact capabilities and return `UnsupportedError` for gaps. Its
  public helper protocol is documented in `transports/python` package docs so
  bridge implementers can match Brine's normalized result and progress
  semantics.

## Goals

- Provide a small, robust Go API for Salt execution.
- Support synchronous execution, asynchronous jobs, event streaming, and
  run-with-progress.
- Make the preferred transport REST, while allowing a Python transport when REST
  is unavailable or unsuitable.
- Preserve raw Salt responses and expose normalized, typed helpers for common
  cases.
- Support large clustered environments: partial results, missing minions,
  cancellation, retries, batching, and progress reporting.
- Keep orchestration policy outside the generic Salt client package.
- Make the package easy to mock in tests.

## Non-goals

- Reimplement Salt.
- Hide Salt's operational model. Targets, target types, functions, JIDs,
  retcodes, runners, and minion returns should remain visible.
- Encode product-specific state dependency rules, pillar prefetching, or CLI
  output formatting in the core package.
- Guarantee identical feature support across every transport. Capabilities must
  be explicit.

## Salt return shape stability

Salt execution and state return payloads are relatively stable at their top-level
conceptual shape across supported Salt versions, but individual module/state
fields are not a formal Brine API. For state returns, Brine relies only on the
long-standing fields needed for generic summaries: `result`, `changes`,
`comment`, `name`, `__id__`, `__sls__`, `__run_num__`, `duration`, and
`start_time`. Unknown fields are preserved in raw JSON and ignored by typed
helpers. For execution-module helpers, Brine decodes narrow projections only;
for example, `modules.NetworkInterfaces` reads `hwaddr`, `up`, and `inet` while
leaving vendor/platform-specific fields to the raw result.

This means minor Salt version changes should not break foundational helpers when
Salt preserves these common fields, but callers that depend on module-specific
or platform-specific fields should either decode `Raw` themselves or add explicit
fixture/contract coverage for their deployed Salt versions.

## Design principles

1. **Transport-neutral public API**: callers build a Salt request once and choose
   a transport separately.
2. **Middleware around execution**: validation, retry, observation,
   instrumentation, and caller-owned request mutation should be implemented as a
   handler chain around synchronous `Run`.
3. **Capability-driven behavior**: advanced features such as event streaming,
   runner calls, batching, and job lookup are checked through
   transport capabilities.
4. **Raw-preserving normalization**: normalize enough to make common workflows
   safe, but retain raw JSON for module-specific data.
5. **Partial success is first-class**: a Salt call can succeed at the transport
   layer while only some minions succeed.
6. **Streaming is not just a channel**: streams need cancellation, close, and
   receive error semantics.
7. **High-level helpers are additive**: typed helpers for state runs and common
   modules should sit on top of the generic request/result model.

## Result, error, and progress semantics

Brine deliberately treats `result != nil && err != nil` as a useful and normal
outcome for distributed execution. Transport, authentication, and protocol
failures usually return `nil, err` because Salt did not provide usable execution
data. Salt execution failures return the normalized partial or complete
`*Result` with an `*ExecutionError`, preserving successful minion returns,
failed minion returns, retcodes, and missing minions.

Typed helpers in packages such as `modules` and `states` are projections over the
same normalized result model. They should decode every minion that can be
decoded, preserve the raw `*brine.Result`, and return partial typed data with a
projection/decode error when only some minion payloads fail to decode. If Salt
execution and typed projection both fail, helpers should return partial typed
data and preserve both errors with `errors.Join`.

Run-scoped progress is exposed through `Client.Run` observers. Transports that
support run-scoped returns emit `EventExpectedMinions` when the expected minion
set is known and `EventMinionReturned` as individual minion returns are
normalized. Python produces these events from newline-delimited bridge frames.
REST local `Run` defaults to dispatching `local_async` and reconciling with
`runner.jobs.lookup_jid`, so the blocking synchronous API is implemented as an
async dispatch plus collection. REST also exposes direct and auto local-run modes
for compatibility/performance tuning. Salt's event stream remains available
through `Job.Events`/`Client.Events` for transports that support global events.

## Proposed package layout

```text
brine/
  client.go              Client orchestration, observers, handler chain
  request.go             Request, options, validation, lowstate escape hatch
  target.go              Target interface and target constructors
  result.go              Result, MinionResult, partial-result helpers
  job.go                 Job handle and async behavior
  events.go              Event, EventStream, filters, observer events
  errors.go              Error taxonomy
  capabilities.go        Capability set and UnsupportedError helpers
  middleware.go          Handler, HandlerFunc, Middleware, chain builder
  retry.go               Generic retry middleware and predicates
  brinetest/             Public transport-author contract suite
  lowstate/              Raw Salt lowstate escape hatch
  states/                State result decoders and summaries
  transports/rest/       Salt rest_cherrypy transport
  transports/python/     Python compatibility transport
  transports/mock/       Deterministic tests and examples
  cmd/brine/             Live diagnostic CLI
  cmd/brine-compatcheck/ Contract compatibility reporter
  test/integration/      Docker/Salt topology, scripts, and fixtures
  tools/                 Optional rich demos, experiments, and TUI tools
```

The current repository has a much smaller interface. That should be considered a
starting point, not a constraint. The API should be allowed to change before the
first stable release.

The testing strategy is described in `TESTING.md`. `brinetest` is a public
transport-author contract suite over the normalized Brine API. It is not an
environment manager; Docker/Salt lifecycle belongs to `test/integration` and the
Justfile recipes. The compose-backed Salt topology validates REST and Python
command bridge behavior and captures REST fixtures.

Tooling boundaries are intentionally narrow: `cmd/brine` is a live diagnostic
CLI, `cmd/brine-compatcheck` is a developer compatibility reporter that runs
contract suites, and `examples/...` plus root `*_example_test.go` files are
lightweight API examples. Root commands may use focused CLI dependencies such as
Cobra, Koanf, and Lip Gloss when they improve command structure, configuration,
or readability. Rich demos, experiments, and TUI-style tools belong under
`tools` instead of root examples or commands.

## Public API shape

### Client

The root package should expose a `Client` that wraps a transport and provides
stable execution methods:

```go
type Client struct { /* ... */ }

func New(transport Transport, opts ...ClientOption) (*Client, error)

func (c *Client) Run(ctx context.Context, req Request, opts ...RunOption) (*Result, error)
func (c *Client) Start(ctx context.Context, req Request) (Job, error)
func (c *Client) Events(ctx context.Context, filter EventFilter) (EventStream, error)
func (c *Client) Resolve(ctx context.Context, target Target) ([]string, error)
func (c *Client) Capabilities() Capabilities
func (c *Client) Info(ctx context.Context) (TransportInfo, error)
func (c *Client) Close() error
```

`Run` is synchronous from the caller's perspective. It may internally use a
transport's async/job APIs when that is the most reliable implementation, but the
caller receives a collected `Result`.

`Start` dispatches asynchronous Salt work and returns a `Job` handle.

`Events` opens a global Salt event stream if the selected transport supports it.

`Resolve` returns the minions matched by a target if the selected transport
supports target resolution. This is useful for dry-run behavior and progress
accounting.

`Info` returns transport and Salt version metadata where available. It is for
diagnostics and compatibility checks, may perform transport-specific network
probes, and should not be used as a cheap feature check. Callers should use
capabilities for feature decisions.

### Handler and middleware

Synchronous execution should be modeled like a Go handler chain. This makes
request mutation, retry, tracing, metrics, observation, and validation composable
without special-purpose hooks. The current middleware chain applies to
synchronous `Run`; asynchronous `Start` dispatches directly to the transport.

```go
type Handler interface {
    Run(ctx context.Context, req Request) (*Result, error)
}

type HandlerFunc func(ctx context.Context, req Request) (*Result, error)
func (f HandlerFunc) Run(ctx context.Context, req Request) (*Result, error) {
    return f(ctx, req)
}

type Middleware func(next Handler) Handler

func Chain(mw ...Middleware) Middleware
```

Client configuration should make default ordering explicit:

```go
type ClientOption func(*clientConfig)

func WithMiddleware(mw ...Middleware) ClientOption
func WithHandlerChain(mw ...Middleware) ClientOption
```

The default chain should be documented in construction order. `WithMiddleware`
appends middleware in declaration order at the documented caller-extension point.
It does not replace previously registered middleware. Advanced callers that need
complete ordering control, such as tracing outside the retry loop, should use
`WithHandlerChain` to replace the default chain deliberately.

The `Client` builds a chain around the transport's `Run` method:

1. request validation;
2. caller-provided middleware;
3. retry middleware;
4. observer/instrumentation middleware;
5. transport.

The exact order should be configurable where order matters. For example,
observation can be outside retry to report the logical request once, or inside
retry to report each attempt.

Middleware that needs to run additional Salt calls should close over the bare
transport or a deliberately unwrapped handler to avoid accidental recursion.
This is important for middleware that prefetches data using runner calls before
mutating a state request. The client should expose this deliberately, for
example:

```go
func (c *Client) Unwrap() Handler
```

`Unwrap` returns the bare execution handler underneath the client middleware
chain. Middleware examples should use this handler for internal Salt calls rather
than calling `next` recursively.

`Start` and `Subscribe` have different return shapes and should not be forced
through the same handler chain. They can get smaller, explicit middleware later
if real use cases appear. `Resolve` is also outside the `Run` handler chain;
middleware that needs target resolution should call `Client.Resolve` or the bare
transport deliberately.

### Transport

A transport is the boundary between the public API and a concrete Salt
integration.

```go
type Transport interface {
    io.Closer
    Handler
    Capabilities() Capabilities
    Info(ctx context.Context) (TransportInfo, error)
    Start(ctx context.Context, req Request) (Job, error)
    Subscribe(ctx context.Context, filter EventFilter) (EventStream, error)
    Resolve(ctx context.Context, target Target) ([]string, error)
}
```

The root package should own request validation, observer emission, common retry
policy, and normalized result helpers where possible. Transports should own only
backend-specific details: REST authentication, REST payload shapes, Python helper
protocols, Salt event parsing, and backend-specific failures.

The public transport interface intentionally remains a single interface, even for
sync-only transports. Unsupported operations should return `UnsupportedError` and
be reflected in capabilities. Smaller optional interfaces such as
`AsyncTransport` or `EventTransport` can be introduced internally later if they
simplify implementation, but callers should rely on `Capabilities` rather than Go
type assertions.

To reduce boilerplate, the root package should provide an embeddable helper for
unsupported optional operations:

```go
type UnsupportedTransport struct{}
```

`UnsupportedTransport` should implement `Start`, `Subscribe`, `Resolve`, and
`Info` with useful `UnsupportedError` values so simple transports can embed it
and override only supported methods.

### Capabilities

Capabilities should be forward-compatible. A typed set is preferred over a struct
of booleans because new capabilities can be added without silently relying on
zero values in struct literals.

```go
type Capability string

const (
    CapSynchronousRun   Capability = "run.sync"
    CapLocalRun         Capability = "local.run"
    CapLocalStart       Capability = "local.start"
    CapRunnerRun        Capability = "runner.run"
    CapRunnerStart      Capability = "runner.start"
    CapLowstate         Capability = "lowstate"
    CapEvents           Capability = "events"
    CapJobLookup        Capability = "jobs.lookup"
    CapTargetResolution Capability = "targets.resolve"
    CapBatch            Capability = "batch"
    CapStreamingReturns Capability = "returns.stream"
)

type Capabilities struct {
    caps map[Capability]struct{}
}

func NewCapabilities(caps ...Capability) Capabilities
func (c Capabilities) Supports(cap Capability) bool
func (c Capabilities) Require(cap Capability) error
func (c Capabilities) RequireAny(caps ...Capability) error
func (c Capabilities) RequireAll(caps ...Capability) error
func (c Capabilities) List() []Capability
func (c Capabilities) MarshalJSON() ([]byte, error)
func (c *Capabilities) UnmarshalJSON(data []byte) error
```

`Require` should return `*UnsupportedError` so callers can use `errors.As` and
`Capabilities` should be treated as immutable after construction:
`NewCapabilities` copies input values, there should be no public `Add` or
`Remove`, and `List`/`MarshalJSON` should return a stable sorted representation.
`CapSynchronousRun` means the transport can satisfy `Run` as a first-class
operation, even if it internally uses an async Salt job and waits for completion.
`CapLowstate` means the transport supports raw lowstate requests.

### Transport info

Transport info is diagnostic metadata, not the feature contract. Capabilities are
still authoritative for behavior.

```go
type TransportInfo struct {
    Name         string
    Version      string
    SaltVersion  string
    APIVersion   string
    Capabilities Capabilities
}
```

REST can populate Salt/API versions when endpoints expose them. Python can report
helper protocol version and imported Salt version. Mock can report scripted
metadata for tests.

### Request

A request should represent Salt work, not a transport payload.

```go
type RequestKind int

const (
    KindLocal RequestKind = iota
    KindRunner
    KindLowstate
)

type Request struct {
    Kind     RequestKind
    Target   Target
    Function string
    Args     []any
    Kwargs   map[string]any
    Options  RequestOptions
    Metadata map[string]any
    Lowstate []LowstateEntry
}

type RequestOptions struct {
    Batch            Batch
    ModuleTimeout    time.Duration
    GatherJobTimeout time.Duration
    FullReturn       bool
}

type Batch struct {
    Count   int
    Percent float64
}

type RequestOption func(*Request)

func Local(function string, target Target, opts ...RequestOption) Request
func Runner(function string, opts ...RequestOption) Request

func Args(args ...any) RequestOption
func Kwargs(kwargs map[string]any) RequestOption
func PillarData(pillar map[string]any) RequestOption
func ReplacePillar(pillar map[string]any) RequestOption
func BatchCount(count int) RequestOption
func BatchPercent(percent float64) RequestOption
func ModuleTimeout(d time.Duration) RequestOption
func GatherJobTimeout(d time.Duration) RequestOption
func FullReturn(v bool) RequestOption
```

`Request` uses exported fields for ergonomic inspection and transport mapping.
Constructors are preferred because they set a coherent `Kind`, target, function,
and options. Callers and middleware must treat requests as immutable after
handing them to a client, job, result, or event: maps and slices are retained by
reference unless an implementation explicitly deep-copies them. To modify a
request, copy it and apply `With`/option helpers rather than mutating maps or
slices in place.

`Metadata` is free-form caller data carried through middleware and observers. It
must not be serialized to Salt by transports. Use metadata for caller-side hints,
correlation IDs, or predicates that middleware needs to inspect without changing
Salt execution kwargs. REST and Python request mappers must explicitly ignore
`Metadata`; only `Kind`, `Target`, `Function`, `Args`, `Kwargs`, `Options`, and
`Lowstate` should influence Salt wire payloads.

`PillarData` should recursively merge `map[string]any` values into the existing
`pillar` kwarg, copying input maps so callers cannot mutate requests after
construction by holding references. On scalar or slice collisions, later options
win. `ReplacePillar` replaces the `pillar` kwarg entirely.

Recommended constructors:

```go
req := brine.Local("state.sls", brine.Compound("G@role:storage"),
    brine.Args("s3,firewall"),
    brine.Kwargs(map[string]any{"pillar": pillar}),
    brine.GatherJobTimeout(30*time.Minute),
)

runnerReq := brine.Runner("manage.alived")
```

Synchronous versus asynchronous dispatch should be selected by calling `Run` or
`Start`, not by setting `local_async` in the request. Transports can map
`Start(local)` to REST's `local_async` or to Python's async client internally.

Go context deadlines and Salt execution timeouts are different concepts:

- `context.Context` controls client-side cancellation and deadlines.
- `ModuleTimeout` and `GatherJobTimeout` are Salt-side hints/options.

Avoid naming Salt-side options simply `Timeout`, because it will be confused with
Go context deadlines.

Batching should be modeled as a typed `Batch`, not as a `parallel` boolean or a
free-form string. Salt supports concrete values and percentages; constructors
such as `BatchCount(10)` and `BatchPercent(25)` should validate values before a
transport serializes them to Salt's `"10"` or `"25%"` wire format.

A raw lowstate escape hatch should exist in a subpackage:

```go
req := lowstate.Request(entries...)
```

Raw lowstate is useful for advanced Salt features and migration, but it should
not be the default API. To avoid a vague side-channel, the root request shape
should contain a dedicated lowstate payload field:

```go
type Request struct {
    Kind     RequestKind
    Target   Target
    Function string
    Args     []any
    Kwargs   map[string]any
    Options  RequestOptions
    Metadata map[string]any
    Lowstate []LowstateEntry
}

type LowstateEntry struct {
    Fun     string         `json:"fun"`
    Target  string         `json:"tgt,omitempty"`
    TgtType string         `json:"tgt_type,omitempty"`
    Args    []any          `json:"arg,omitempty"`
    Kwargs  map[string]any `json:"kwarg,omitempty"`
}
```

Normal callers should discover and use `lowstate.Request` rather than
constructing raw requests directly. The lowstate subpackage can type-alias or wrap
`brine.LowstateEntry` for ergonomics:

```go
type Entry = brine.LowstateEntry

func Request(entries ...Entry) brine.Request
```

Lowstate semantics:

- lowstate requests go through the same validation, middleware, observer, and
  retry chain as other requests unless middleware explicitly opts out;
- `Kind` is `KindLowstate`;
- `Function` is empty unless the lowstate subpackage uses it for diagnostics;
- the raw entries are carried in `Request.Lowstate`;
- transports switch on `KindLowstate` to serialize the wire payload;
- `Start` support is capability-gated just like local/runner async support.

### Target

Targets should be strongly typed. A sealed interface is preferred because it
prevents invalid field combinations and lets transports switch exhaustively over
known target shapes.

```go
type Target interface {
    isTarget()
}

type GlobTarget string
type CompoundTarget string
type GrainTarget string
type PillarTarget string
type NodeGroupTarget string
type ListTarget []string
```

Suggested constructors:

- `Glob(expr string) Target`
- `List(minions ...string) Target`
- `Compound(expr string) Target`
- `Grain(expr string) Target`
- `Pillar(expr string) Target`
- `NodeGroup(name string) Target`

List targeting should preserve the minion slice rather than encoding it as a
comma-separated string. Transports can serialize it appropriately. `List()` with
no minions should be invalid at request validation time.

### Result

A result should normalize execution without losing raw Salt data. Local requests
have per-minion returns. Runner requests may return scalar data with no minion
concept.

```go
type Result struct {
    JID      string
    Request  *Request
    Expected []string
    ByMinion map[string]MinionResult
    Missing  []string
    Scalar   json.RawMessage
    Failure  *Failure
    Raw      json.RawMessage
}

type MinionResult struct {
    Minion  string
    JID     string
    RetCode int
    Return  json.RawMessage
    Failure *Failure
    Raw     json.RawMessage
}

type Failure struct {
    Kind    FailureKind
    Message string
    Raw     json.RawMessage
}

type FailureKind string

const (
    FailureRetCode         FailureKind = "retcode"
    FailureMalformed       FailureKind = "malformed"
    FailureNoReturn        FailureKind = "no_return"
    FailureMinionException FailureKind = "minion_exception"
    FailureUnknown         FailureKind = "unknown"
)
```

Important behavior:

- `ByMinion`, `Expected`, and `Missing` are populated for local requests.
- `Scalar` is populated for runner responses that are not minion-scoped.
- `Failure` is populated for scalar runner failures that Salt reports in a
  successful transport response.
- `Raw` is always the full Salt envelope when available; `Scalar` is only the
  runner return body.
- `Result.OK()` returns true for local results only when all expected minions
  returned successfully with no failures. For runner results it returns true when
  `Failure` is nil and the transport successfully normalized the scalar
  response.
- `Result.IsLocal()` and `Result.IsRunner()` are convenience helpers based on
  `Request.Kind`.
- `Result.Returned()` returns the subset of minions that returned.
- `Result.Failures()` returns failed and missing minions for rendering.
- `Result.Partial()` returns true when at least one minion returned but the whole
  result is not successful.
- `MinionResult.Decode(&v)` decodes one minion's module-specific return body.
- `DecodeByMinion[T](result)` can decode homogeneous module returns.

Invariant: when `Run` returns a non-nil `*ExecutionError`, the returned `*Result`
should also be non-nil whenever Salt produced any normalized data. Callers must
be able to render partial results from either the explicit result return or the
error's embedded result.

The package should not assume every Salt function returns the same shape.

Result field semantics:

| Field | Local requests | Runner requests |
|---|---|---|
| `ByMinion` / `Expected` / `Missing` | Populated | Empty |
| `Scalar` | Empty | Return body |
| `Failure` | Empty unless the whole local request fails before minion returns | Scalar failure, if Salt reports one |
| `Raw` | Full Salt envelope | Full Salt envelope |
| `MinionResult.Return` | Per-minion return body | Not used |
| `MinionResult.Raw` | Per-minion envelope | Not used |

Known question: if runner fixtures show meaningful JSON `null` scalar returns,
`Scalar` may need an additional boolean to distinguish “unset” from “explicit
null”. Failing runner fixtures must be captured early so the normalizer can
distinguish scalar failure payloads from successful falsey returns.

### Typed helper packages

Typed helpers should decode known Salt module response shapes on top of raw
results. State execution is the first required helper because state return data
is nested and failure-prone.

State helpers should live in a subpackage so they can grow without bloating the
root package:

```go
stateReturns, err := states.Decode(result)
summary := stateReturns[minion].Summary()
```

State helpers should model:

- state ID;
- name;
- result: true, false, or null;
- changes;
- comment;
- duration;
- start time;
- aggregate summaries;
- invalid state return detection.

Request helpers can also be added:

```go
req := states.SLS(brine.Compound("..."), "s3,firewall", brine.PillarData(pillar))
req := states.Highstate(brine.List("node-a", "node-b"))
```

These helpers should remain thin wrappers over `Request`.

## Synchronous, asynchronous, and streaming behavior

### Synchronous `Run`

`Run` should return when one of these happens:

- all expected minions have returned;
- Salt reports completion with missing minions;
- the provided context is cancelled;
- the transport fails;
- a Salt-side execution timeout is reached and reported.

For large clusters, the implementation should prefer streaming or job lookup
internally when available so progress can be emitted while the final result is
being collected.

### Run options

Per-call `RunOption` should start deliberately small. Initially it should support
only per-call observers. Request mutation, retry policy, and instrumentation
belong in middleware so ordering remains explicit.

```go
type RunOption func(*RunConfig)

func WithRunObserver(observer Observer) RunOption
```

### Run-with-progress

Run-with-progress is expected to be a common call shape. Callers should not have
to switch to `Start` and manually consume job events just to update a progress
bar.

Observers provide the progress API:

```go
type Observer interface {
    OnEvent(ctx context.Context, event Event)
}

type ObserverFunc func(ctx context.Context, event Event)

type AsyncObserver struct { /* ... */ }

func NewAsyncObserver(next Observer, bufferSize int) *AsyncObserver
func MultiObserver(observers ...Observer) Observer
func WithObserver(observer Observer) ClientOption
func WithRunObserver(observer Observer) RunOption
```

`AsyncObserver` should process events in a background goroutine with bounded
buffering. The default policy should be drop-newest when the buffer is full so
Salt event consumption and transport loops do not stall behind terminal, log, or
metrics output. Durable observers that cannot drop events should implement their
own backpressure and error reporting explicitly.

Transport implementations and middleware should emit normalized events during
`Run`, especially:

- expected minions resolved;
- minion returned;
- retry scheduled;
- retry started;
- retry exhausted;
- request completed;
- request failed.

Progress bars, JSON-line output, logging, metrics, and tracing should be observer
adapters outside the core request model. `Request` should never contain UI types.

Middleware and transports should not close over observer slices directly. The
client should attach an emitter to the `Run` context before invoking the handler
chain:

```go
type Emitter interface {
    Emit(ctx context.Context, event Event)
}

func Emit(ctx context.Context, event Event)
```

Middleware such as retry can call `Emit(ctx, retryEvent)` without knowing how
observers are registered. The concrete emitter is owned by the client. `Emit`
should be a no-op when no emitter is attached to the context so middleware can
call it unconditionally.

Terminal events must be delivered even when the run context has already been
cancelled. Non-terminal events should use the original context. Terminal events
such as `request.completed`, `request.failed`, and `retry.exhausted` should be
sent to observers with cancellation stripped, for example via
`context.WithoutCancel(ctx)`, so UIs and logs can render final state.

A `RunStream` helper can be layered on top later if a pull-based per-minion API
is needed, but observers should be the first mechanism.

### Asynchronous `Start`

A `Job` should expose:

```go
type Job interface {
    ID() string
    Request() *Request
    Wait(ctx context.Context) (*Result, error)
    Events(ctx context.Context) (EventStream, error)
}

type LocalJob interface {
    Job
    ExpectedMinions() []string
}
```

`ExpectedMinions` is only meaningful for local jobs. Runner jobs may not have
minions, so the base `Job` interface should not force that concept.
`Client.Start` intentionally returns the broad `Job` interface; callers that
need local-job expectations should use a type assertion:

```go
job, err := client.Start(ctx, req)
if local, ok := job.(brine.LocalJob); ok {
    expected := local.ExpectedMinions()
    _ = expected
}
```

REST local async should return a `LocalJob`; runner async should return plain
`Job` values unless a future Salt shape provides meaningful minion expectations.

Job semantics:

- `Wait` should be idempotent. Multiple calls return the cached final result once
  available.
- Call `Events` before `Wait` when the caller needs guaranteed event delivery.
  `Events` after `Wait` is not guaranteed because the transport may have already
  closed the job-specific stream.
- The common usage pattern is: call `Events`, start one goroutine or loop to
  consume that stream, then call `Wait`.
- `Events` is single-consumer by default. If called again, a transport may return
  an independent filtered stream, but callers must not rely on that unless the
  transport documents support.
- Transports may return `ErrEventStreamConsumed`, `UnsupportedError`, or a closed
  stream if events are requested after consumption or completion.
- If the event stream drops during `Wait`, the job should fall back to job lookup
  when `CapJobLookup` is available.
- If no fallback is available and a partial result exists, `Wait` should return
  `*ExecutionError` containing the partial result and wrapping the underlying
  `*TransportError`. `ExecutionError` is the only error type that carries partial
  execution data.

### Event streams

Use a pull-based stream rather than only returning a channel:

```go
type EventStream interface {
    Recv(ctx context.Context) (Event, error)
    Close() error
}

func StreamEvents(ctx context.Context, stream EventStream) iter.Seq2[Event, error]
```

`StreamEvents` is an additive convenience for Go versions with iterator support.
`Recv` remains the primary transport interface.

This shape supports receive errors, reconnect handling, context cancellation, and
clean shutdown. Channel helpers can be layered on top for convenience. Normal
stream exhaustion should return `io.EOF`; attempts to reuse a consumed stream can
return `ErrEventStreamConsumed`.

Use `Recv` rather than a `Next`/`Err` scanner-style API because event stream
errors are operationally important and should be handled at the receive site.

`Job.Events` streams are subject to the same single-consumer `Recv` rules as
other event streams.

`EventFilter` should be defined in the root package:

```go
type EventFilter struct {
    Tags    []string
    JID     string
    Minions []string
}
```

Filtering is best-effort. A transport may filter server-side, client-side, or not
at all if the backend cannot support it reliably. Tag filters use Salt-style glob
matching, OR semantics within `Tags`, and AND semantics across other populated
filter fields such as `JID` and `Minions`.

Events should use a structured envelope with well-known event types and optional
type-specific payloads:

```go
type EventType string

const (
    EventRequestStarted   EventType = "request.started"
    EventExpectedMinions  EventType = "request.expected_minions"
    EventRequestCompleted EventType = "request.completed"
    EventRequestFailed    EventType = "request.failed"
    EventJobStarted       EventType = "job.started"
    EventMinionReturned   EventType = "minion.returned"
    EventJobCompleted     EventType = "job.completed"
    EventRetryScheduled   EventType = "retry.scheduled"
    EventRetryStarted     EventType = "retry.started"
    EventRetryExhausted   EventType = "retry.exhausted"
    EventRawSalt          EventType = "salt.raw"
)

type Event struct {
    Type      EventType
    Timestamp time.Time
    JID       string
    Minion    string
    Payload   any
    Raw       json.RawMessage
}
```

`Payload` is intentionally flexible, but each event type must document its
payload type. Common payloads should have named structs and ok-bool accessors,
for example `Event.MinionReturned() (MinionReturnedPayload, bool)`, so observers
avoid unchecked type assertions.

Initial payload mapping:

| Event type | Payload |
|---|---|
| `request.started` | `RequestStartedPayload` |
| `request.expected_minions` | `ExpectedMinionsPayload` |
| `request.completed` | `RequestCompletedPayload` |
| `request.failed` | `RequestFailedPayload` |
| `job.started` | `JobStartedPayload` |
| `minion.returned` | `MinionReturnedPayload` |
| `job.completed` | `JobCompletedPayload` |
| `retry.scheduled` / `retry.started` / `retry.exhausted` | `RetryPayload` |
| `salt.raw` | `RawSaltPayload` |

Raw events should preserve the Salt tag in the payload and the original Salt body
in `Raw`. Specific payload structs can be added for common event types without
changing the stream API.

Event sources should be explicit:

- `Client.Events` is a Salt event-bus subscription and should emit only
  `salt.raw` events.
- `Job.Events` is a job-scoped Salt event stream and should emit `salt.raw`
  events plus normalized job/minion events only when the transport can derive
  them directly from the job stream.
- Brine lifecycle/progress events such as request started, retry scheduled, and
  request completed are delivered to observers, not to `Client.Events`.

## Cancellation, timeouts, and cleanup

`context.Context` cancellation means the caller no longer wants to wait for the
client operation. It does not necessarily mean the Salt job has been cancelled on
the master or minions.

Guidance:

- If the context deadline is shorter than Salt-side timeouts, the client should
  return `context.DeadlineExceeded` or `context.Canceled` promptly, with any
  partial result available.
- Already-dispatched Salt jobs may continue after the Go context is cancelled.
  This should be documented clearly to avoid surprising operators.
- Automatic job cancellation should not be the default. Killing Salt jobs can be
  operationally risky and may not be supported uniformly.
- A future explicit option such as `CancelDispatchedJobOnContextDone` can be
  added for transports that can safely map it to Salt job cancellation.
- Salt-side `ModuleTimeout` and `GatherJobTimeout` remain request options sent to
  Salt; they are not replacements for Go context deadlines.

Resource cleanup rules:

- `EventStream.Close` should be idempotent.
- `Client.Close` should be idempotent and close the underlying transport.
- Callers should close event streams they open. Transports may also close open
  streams when the client/transport is closed, but callers should not rely on
  that as their only cleanup path.
- `Job` does not need explicit cleanup. Its final result should be cached by
  `Wait` where possible.

## Concurrency and immutability

Concurrency guarantees should be explicit:

- `Client` should be safe for concurrent use after construction.
- Transports should be safe for concurrent use or document stricter limits; the
  preferred contract is concurrent-safe.
- `Result`, `Request`, and `MinionResult` should be treated as immutable after
  being returned, retained by a job, or emitted in an event.
- `Job.Wait` should be safe to call concurrently and should return the same
  cached final result after completion.
- `EventStream.Recv` should not be called concurrently by multiple goroutines
  unless a transport explicitly documents support. `Close` should be safe to call
  while another goroutine is blocked in `Recv`.
- Observers should be assumed to run synchronously unless wrapped by an async
  adapter. Observer failures should not change Salt execution semantics.
- Ordinary observer errors are outside the core error model because `OnEvent` has
  no return value. Observers that need durable output should expose their own
  error state.
- Observer panics should not be recovered by the client; a panic in rendering or
  telemetry is a programmer error and should fail loudly.
- Panics from caller middleware or transport execution should be recovered at the
  client boundary where possible so a terminal `request.failed` event can be
  emitted and the caller receives an error instead of losing lifecycle reporting.

## Error model

The package needs a clear distinction between communication failure and Salt
execution failure.

Suggested errors:

- `TransportError`: network, subprocess, process protocol, or I/O failure.
- `AuthError`: authentication or authorization failure.
- `ProtocolError`: malformed or unexpected response from the transport.
- `UnsupportedError`: selected transport lacks a required capability.
- `ErrEventStreamConsumed`: a job/event stream has already been consumed or is no
  longer available.
- `ExecutionError`: Salt accepted the request, but one or more minions failed or
  did not return.

Sketch:

```go
type ExecutionError struct {
    JID    string
    Result *Result
    cause  error
}

func (e *ExecutionError) Failed() []string
func (e *ExecutionError) Missing() []string
func (e *ExecutionError) Partial() bool
func (e *ExecutionError) Unwrap() error

type UnsupportedError struct {
    Capability Capability
    Operation  string
    cause      error
}

func (e *UnsupportedError) Unwrap() error

type AuthError struct {
    Status int
    cause  error
}

func (e *AuthError) Unwrap() error

type ProtocolError struct {
    Snippet string
    cause   error
}

func (e *ProtocolError) Unwrap() error
```

Errors should support `errors.As` and `Unwrap`. Where useful, they should also
support `errors.Is` through package sentinels such as `ErrAuth`,
`ErrUnsupported`, and `ErrProtocol`. `ExecutionError` should keep `Result` as the
source of truth; `Failed`, `Missing`, and `Partial` should be convenience methods
derived from `Result` rather than duplicated fields.

`ExecutionError` should carry the partial `Result` when available. This lets a
caller show successful minion results and still treat the operation as failed.
Its `Error()` string should be operationally useful, for example: `salt
execution failed: 3 of 10 minions failed (jid: ...)`.

`ProtocolError.Snippet` should contain a bounded diagnostic excerpt, such as the
first few kilobytes of an unexpected REST or helper response, so HTML error pages
or malformed JSON are debuggable without logging unbounded payloads.

Recommended pattern:

```go
result, err := client.Run(ctx, req)
if err != nil {
    var execErr *brine.ExecutionError
    if errors.As(err, &execErr) {
        renderPartial(execErr.Result)
    }
    return err
}
```

## Retry model

Retries should be policy-driven and optional. The generic package should provide
mechanisms; callers should provide orchestration policy.

Core retry concepts:

- retry transport errors only when configured;
- retry execution failures at minion granularity where possible;
- retry selected minions using list targeting;
- emit retry events for progress and logging;
- preserve the final successful return for each minion;
- keep earlier failed attempts available for diagnostics if requested.

The retry middleware API should make minion-granular behavior explicit:

```go
type RetryPredicate func(req Request, result MinionResult) bool

type RetryConfig struct {
    MaxAttempts int
    Predicate   RetryPredicate
    Backoff     func(attempt int) time.Duration
}

func WithRetry(config RetryConfig) Middleware
```

`MaxAttempts` includes the initial attempt. The predicate is evaluated against
failed minion returns from a local result; scalar runner retries should use
a separate transport/error predicate if needed.

A built-in state retry predicate is justified because malformed state return data
is a known Salt failure mode in large deployments. It should be exported from the
state helper package:

```go
func MalformedStateRetryPredicate(req brine.Request, result brine.MinionResult) bool
```

The generic retry middleware must be able to retry only selected minions by
constructing a new local request with a `List` target containing the retryable
minions.

The predicate should match:

- function starts with `state.`;
- minion retcode is non-zero;
- return body is a string or list of strings instead of a normal state return
  map;
- retry only the affected minions.

Application-specific state expansion, dependency injection, and pillar
prefetching should be modeled as middleware outside the core transport.

## Pillar and orchestration context

Salt's `pillar` kwarg for state execution is a legitimate Salt mechanism for
passing per-run context. It is reasonable for Brine to make that easy to express:

```go
req := states.SLS(target, "s3", brine.PillarData(pillar))
```

However, Brine should not know how to build a product-specific pillar payload.
Prefetching mine data, cluster membership, service status, or other orchestration
context depends on the caller's state tree, performance constraints, and data
schema. That belongs in caller-owned middleware or an optional integration
package, not in the core Salt client.

Recommended boundary:

- Core Brine provides request options for merging pillar data.
- Core Brine provides runner/local requests that middleware can use to fetch
  data.
- Caller-owned middleware decides what to fetch, how to shape it, and when to add
  it to a request.
- The middleware should use an unwrapped handler or bare transport for internal
  Salt calls to avoid recursively applying itself.

Alternative Salt-native approaches to consider before building large middleware:

- ext_pillar or custom pillar modules for reusable server-side data generation;
- orchestration states that compute and pass context explicitly;
- scheduled mine updates or precomputed cache data;
- custom Salt runner/execution modules that return exactly the context needed by
  the state tree.

Ephemeral pillar injection is pragmatic and often appropriate for one-off state
runs, especially when it avoids repeated expensive calls from every minion. It
should still be modeled as caller policy because the schema and safety tradeoffs
are application-specific.

## Transport strategy

### REST transport

The REST transport should be the preferred implementation. The initial deployment
assumption is a localhost `rest_cherrypy` endpoint on the Salt master node rather
than an arbitrary remote Salt API endpoint. The API should not bake in localhost,
but tests and early configuration can optimize for that shape.

Expected support:

- pluggable authentication and token refresh;
- local execution;
- async local execution;
- runner execution;
- event stream via REST event endpoint;
- job lookup;
- target resolution where available;
- response normalization into `Result`.

Authentication should be pluggable rather than hard-coded into the constructor:

```go
type Authenticator interface {
    Token(ctx context.Context) (string, error)
}

transport := rest.New(baseURL, rest.WithAuth(rest.PAMAuth(user, pass, "pam")))
```

Provided authenticators can include username/password eauth, static token, and
external token callback implementations.

REST advantages:

- remote operation without shelling out;
- clearer authentication boundary;
- native access to async jobs and event streams;
- easier testing with HTTP fixtures;
- no dependency on a local Salt Python runtime in the Go process.

REST risks:

- Salt API service must be enabled and secured;
- REST endpoint behavior varies by Salt version and configuration;
- event streaming may require reconnect and heartbeat handling;
- some local Python client capabilities may not map perfectly to REST.

### Python transport

Python support is feasible, and the compatibility goal should be as close to REST
as possible for the Salt `v3006` local-master deployment. It should still be
capability-driven rather than assumed identical to REST, because helper mode,
process lifetime, permissions, and event access can differ.

A Python transport can provide strong support when all of the following are true:

- it runs on or near the Salt master;
- the Salt Python libraries are installed and importable;
- the process can read Salt configuration and access the master interfaces;
- the Python Salt version matches the deployed master;
- the helper protocol is stable JSON, not ad-hoc human output.

Feasible Python capabilities in a full local-master mode:

- local synchronous execution via Salt's local client APIs;
- local asynchronous dispatch and JID return;
- iterative per-minion returns;
- runner calls;
- target resolution;
- master event bus streaming through Salt event APIs.

Capabilities that are harder or may require a subset:

- global event streaming from a short-lived helper process;
- robust reconnect semantics for event streams;
- remote authentication semantics equivalent to REST;
- cross-version compatibility across Salt releases;
- clean cancellation of already-dispatched Salt jobs;
- high-concurrency execution without a long-lived helper process.

Recommended Python modes:

1. **Full Python transport**: a long-lived helper process with a JSON protocol.
   This can support run, start, wait, and event streaming if it runs with access
   to the Salt master environment.
2. **Command bridge transport**: a short-lived helper process. This should be
   limited to synchronous run, target resolution, and synthetic per-command
   progress. It should not claim global event streaming.

Therefore the API should expose a capability subset. Python should not be a
second-class concept, and the implementation should pursue compatibility with the
REST fixture matrix wherever the Salt Python APIs make that reliable. It should
not silently pretend to support features it cannot reliably implement in the
current deployment mode.

Recommendation:

- Implement REST first against the localhost Salt API endpoint.
- Design Python for broad compatibility with the same request/result/event model
  rather than as a throwaway shim.
- If Python is needed only for migration, start with command bridge mode and
  expose limited capabilities.
- If Python is needed as a long-term first-class backend or REST parity is
  required, invest in long-lived helper mode and test it against the same fixture
  matrix as REST.

A new Python helper protocol should be explicitly versioned. The helper should
start by emitting a machine-readable hello message with a protocol version, and
Go should refuse unknown major versions. Frame envelope, request/response
correlation, error frame schemas, async operations, and multiplexing strategy are
intentionally deferred to the Phase 6 Python transport design.

```go
const (
    ProtocolVersionMajor = 1
    ProtocolVersionMinor = 0
)

type HelloMessage struct {
    Version      [2]int        `json:"version"`
    Capabilities []Capability `json:"capabilities"`
    SaltVersion  string       `json:"salt_version,omitempty"`
}
```

### ZMQ transport

A direct ZeroMQ transport is not planned for the initial implementation. Salt's
ZeroMQ channels are internal transport machinery rather than the preferred public
client API. Implementing them directly would require Brine to own Salt protocol,
authentication, encryption, serialization, event, and version-compatibility
details.

Most mature Go ZeroMQ bindings require CGO and a native `libzmq` dependency,
which would complicate packaging and cross-compilation. Pure-Go options should
not be assumed compatible with Salt's usage without proof.

If added later, ZMQ should be experimental, capability-gated, and likely scoped
first to event streaming rather than full command execution.

### Mock transport

The mock transport should support deterministic tests:

- expected request matching;
- scripted responses;
- scripted transport errors;
- scripted execution errors;
- scripted event sequences;
- job handles with controllable wait behavior;
- capability configuration.

Suggested shape:

```go
type Transport struct {
    OnRun       func(context.Context, brine.Request) (*brine.Result, error)
    OnStart     func(context.Context, brine.Request) (brine.Job, error)
    OnSubscribe func(context.Context, brine.EventFilter) (brine.EventStream, error)
    OnResolve   func(context.Context, brine.Target) ([]string, error)
    Caps        brine.Capabilities
    Info        brine.TransportInfo
}

func ScriptLocalSuccess(minions ...string) *Transport
func ScriptExecutionError(failedMinions ...string) *Transport
func ExpectLocalSuccess(function string, target brine.Target, returns map[string]any) *Transport
func RecordCalls() (*Transport, *[]RecordedCall)
```

The mock should be the first transport implemented because it validates API
ergonomics before committing to REST or Python details.

## Migration behavior mapping

Existing caller behavior should be represented as middleware, options, observers,
or helper packages rather than transport features.

| Existing behavior | Proposed Brine location |
|---|---|
| State-list expansion based on external configuration changes | Caller-owned request middleware |
| Prefetch cluster/mine/runner context and inject it as pillar | Caller-owned request middleware using Brine runner/local calls |
| Retry malformed state returns for selected minions | Generic retry middleware plus exported state predicate |
| Resolve target without running the function | `Client.Resolve` with `CapTargetResolution` |
| Run all minions together vs limited concurrency | `RequestOptions.Batch` via `BatchCount` / `BatchPercent` |
| Line-buffered JSON progress output | Observer adapter in the caller |
| Conditional pillar injection skip rules | Caller-owned middleware predicate |

This table should become migration test coverage for the mock transport and later
for integration tests.

## Resolved design decisions

1. `Run` returns `(*Result, error)`. For execution failures, both values should
   be populated when normalized Salt data exists.
2. Raw lowstate should be public, but in a `lowstate` subpackage rather than the
   main API path.
3. Target resolution should be part of `Client` and gated by capability.
4. Observers should live in the root package, be optional, and be
   side-effect-only.
5. Python event streaming should not be required; it is capability-gated.
6. State helpers should live in a `states` subpackage because they are likely to
   grow.
7. `EventStream` should use `Recv(ctx)` rather than a scanner-style `Next`/`Err`
   API.
8. Targets should use a sealed interface rather than `any`.
9. Capabilities should use a typed set rather than a struct of booleans.
10. Keep one public `Transport` interface for now; use capabilities and
    `UnsupportedError` instead of public optional transport interfaces.
