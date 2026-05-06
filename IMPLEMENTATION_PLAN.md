# Brine Implementation Plan

Status: implementation in progress.

The initial design has been validated far enough to support implementation. The
root API, mock transport, Salt integration harness, REST synchronous transport,
state helpers, retry middleware, REST local async jobs, REST job lookup, and REST
event stream groundwork are implemented. The next stabilization milestone is a
transport contract/parity suite (`brinetest`) before any Python transport work.

## Guiding constraints

- The public API can change during the design phase.
- The package should be transport-neutral at the root.
- REST should be implemented first as the preferred production transport.
- Python should be implemented through explicit capabilities, not assumed parity.
- Mock support should arrive early so API ergonomics can be tested without a Salt
  master.
- Existing application behavior should be migrated as orchestration policy around
  the library, not embedded inside the core Salt package.

## Current implementation snapshot

Completed or substantially complete:

- root transport-neutral API skeleton;
- mock transport for unit testing;
- Docker Compose Salt `v3006` integration harness;
- sanitized REST fixture capture for sync and async workflows;
- REST synchronous `Run` for local, runner, wheel, and lowstate requests;
- REST local async `Start`, `LocalJob`, idempotent `Wait`, and job lookup;
- REST `/events` SSE subscription and minion-return event normalization;
- `states` helper package and malformed state retry predicate;
- generic retry middleware with retry lifecycle events;
- lint-clean baseline with `just test` and `just lint` passing.

Known gaps before the next transport implementation:

- REST `TransportInfo` does not detect Salt/API versions yet;
- REST async runner/wheel remain unsupported;
- REST event stream reconnect/heartbeat behavior is not implemented;
- REST target resolution uses `test.ping`, so it resolves responsive minions rather
  than all accepted keys;
- Python transport mode is undecided.

Immediate next milestone:

1. Harden REST behavior that is straightforward to express in `brinetest`.
2. Use `brinetest` as the acceptance gate for any Python transport.
3. Then decide whether to harden REST further or begin Phase 6.

## Phase 0: Design validation

Implementation status: mostly complete; Python mode remains open.

Deliverables:

- Finalize `DESIGN.md`.
- Agree on the `Client`, `Handler`, `Transport`, `Request`, `Target`, `Result`,
  `Job`, and `EventStream` shapes.
- Decide the initial capability constants.
- Confirm explicit request kinds, including local, runner, wheel, and lowstate.
- Confirm sealed target types.
- Confirm `states` and `lowstate` subpackages.
- Confirm observer semantics, async observer backpressure policy, and event
  envelope types for run-with-progress.
- Confirm context-carried emitter semantics.
- Confirm middleware ordering, chain construction, and recursion-avoidance
  guidance.
- Confirm cancellation, resource cleanup, and concurrency guarantees.
- Confirm `TransportInfo` diagnostic metadata.
- Salt `v3006` is the initial support target.
- `rest_cherrypy` is available and acceptable in the target deployment as a
  localhost endpoint on the Salt master node.
- Finalize `TESTING.md`.
- Decide whether the integration harness will use upstream Salt `v3006` images,
  distro packages, or a custom test image.
- Write compile-time API examples before any transport implementation.
- Capture example workflows as API sketches:
  - ping targeted minions;
  - run `state.sls` with args and kwargs;
  - run highstate;
  - resolve target to expected minions;
  - start async job and wait;
  - subscribe to job events;
  - run a runner function;
  - run a wheel function;
  - test retry of transient state failures;
  - inject caller-owned pillar context through middleware.

Acceptance criteria:

- The design can express current required workflows without transport-specific
  types leaking into caller code.
- Unsupported features have an explicit capability story.
- Partial success and missing minions have a clear result shape.
- Python fallback requirements are explicit.
- Pillar/context injection is clearly caller policy, while the core package makes
  it easy to express.

## Phase 1: Root API skeleton

Implementation status: substantially complete. Remaining cleanup is limited to
minor validation/documentation hardening.

Deliverables:

- Define public interfaces and structs in the root package.
- Define `UnsupportedTransport` embeddable helpers for unsupported optional
  transport methods.
- Define `Handler`, `HandlerFunc`, `Middleware`, `Chain`, and chain construction.
- Define typed capabilities and `Capabilities.Supports`, `Require`,
  `RequireAny`, and `RequireAll`.
- Define `TransportInfo`.
- Define explicit `RequestKind` constants for local, runner, wheel, and lowstate.
- Define request builders for local, runner, and wheel requests.
- Define the raw lowstate construction path, any required root constructor, and
  `lowstate.Entry` API.
- Define sealed target types and constructors.
- Add linting or tests to catch target type-switch exhaustiveness in transport
  packages when new target types are added.
- Define request options for typed batch count/percent, module timeout, gather
  job timeout, and full return.
- Validate invalid batch values such as count <= 0 or percent outside 0..100.
- Define error taxonomy.
- Define result helpers, scalar runner/wheel failure representation, failure
  kinds, and `IsLocal`, `IsRunner`, and `IsWheel` helpers.
- Define event envelope types, payload structs/accessors, and event stream
  interfaces.
- Define observer options, `ObserverFunc`, `MultiObserver`, `AsyncObserver`, and
  context-carried emitter helpers.
- Document request metadata semantics.
- Document concurrency, immutability, cleanup, panic recovery, and terminal-event
  cancellation-stripping contracts.
- Add compile-time API examples.

Acceptance criteria:

- The package builds without any real transport.
- API examples are readable and do not require REST or Python imports.
- No product-specific orchestration logic appears in the root package.
- The current minimal Go skeleton is aligned with the design or deliberately
  removed/replaced.

## Phase 1.5: Integration harness and fixture capture

Implementation status: substantially complete for REST. The harness captures
sync and async REST fixtures and supports opt-in integration testing.

Deliverables:

- Add an opt-in Docker Compose Salt topology for local and CI integration tests.
- Run one Salt `v3006` master with rest_cherrypy enabled on localhost semantics
  and at least three minions with stable IDs.
- Mount deterministic test states and pillar data into the topology.
- Use test-only automatic minion key acceptance or deterministic pre-seeded
  minion keys. Prefer `auto_accept: True` initially for simplicity, clearly
  documented as test-only.
- Provide a readiness script that waits for minion key acceptance and `test.ping`
  success.
- Support running the same integration tests against an externally managed Salt
  cluster through environment variables.
- Capture representative raw responses from the compose Salt environment before
  designing normalizers in detail.
- Save fixtures for:
  - `test.ping` success and partial success;
  - `state.sls` success;
  - `state.sls` state failure;
  - malformed state return as a string;
  - malformed state return as a list of strings;
  - `state.highstate`;
  - runner success;
  - deliberately failing runner result;
  - runner failure;
  - target resolution;
  - missing or non-returning minion.
- Record both per-minion streamed shapes and final collected shapes when they
  differ.
- Add a fixture sanitizer script with a fixed schema-aware redaction pass for
  timestamps, tokens, hostnames, container IPs, and other volatile fields.
- Document how to run REST integration tests and, if needed, Python transport
  integration tests.

Acceptance criteria:

- `go test ./...` remains fast and does not require Docker.
- Integration tests run only with an explicit build tag or environment flag.
- Result normalization tests are fixture-driven rather than guessed.
- Fixture metadata records exact Salt version, transport/source, transport
  configuration, and any relevant master configuration.
- Fixture sanitizer is used for every committed fixture.
- Fixtures contain no secrets or environment-specific sensitive data.
- REST fixture capture works before the REST transport is considered complete.

## Phase 2: Mock transport and core result normalization

Implementation status: partially complete. Mock support is usable; normalization
is currently fixture-driven primarily in the REST transport and state helpers.

Deliverables:

- `transports/mock` implementation.
- Scripted synchronous responses.
- Scripted asynchronous jobs.
- Scripted event streams.
- Request assertion helpers.
- Call recording helpers.
- Convenience scripts such as local success and execution failure.
- Capability and transport info configuration.
- Core `Result` and `MinionResult` normalizers for fixture shapes.
- Partial success, missing minion, scalar result, and execution error behavior.

Acceptance criteria:

- Unit tests can cover success, partial success, missing minions, execution
  failure, transport failure, retries, and event consumption.
- API ergonomics can be evaluated without a Salt master.
- `ExecutionError` carries a non-nil result whenever normalized data exists.

## Phase 3: REST synchronous transport

Implementation status: substantially complete. Remaining work is version
detection and any additional deployed Salt response shapes discovered by
fixtures or contract tests. REST target resolution is implemented through
`test.ping`.

Deliverables:

- REST configuration and constructor.
- Pluggable REST authentication interface.
- Provided authenticators for common token flows.
- Login/token refresh where applicable.
- Request-to-lowstate mapping.
- Local synchronous execution.
- Runner synchronous execution if supported by the deployment.
- Wheel synchronous execution if supported by the deployment.
- Response normalization using fixture-informed tests.
- Transport/auth/protocol error handling.
- HTTP fixture tests.
- Compose-backed integration tests for the REST transport.
- Salt/API version detection where available through `TransportInfo`.

Acceptance criteria:

- `Client.Run` works for local functions through REST.
- Auth failures are distinguishable from execution failures.
- Unexpected REST payloads produce protocol errors with useful diagnostics.
- Raw Salt payloads are retained for debugging.
- Runner/wheel scalar results do not require fake minion IDs.

## Phase 4: State helper package and retry middleware

Implementation status: substantially complete for the known malformed state
retry workflow. Attempt history diagnostics remain deferred.

Deliverables:

- `states` subpackage.
- Typed state return decoder.
- State summary helpers.
- Invalid state return detection.
- Exported retry predicate for malformed state returns.
- Generic retry middleware.
- Retry events emitted through the context-carried emitter.
- Attempt history capture if needed for diagnostics.

Acceptance criteria:

- State success, state failure, state no-op, and malformed state return cases are
  covered by tests using captured fixtures.
- Retrying selected minions does not discard successful minion returns.
- Callers can render partial results from an execution error.
- State helpers tolerate differences between Python-derived and REST-derived
  payloads where Salt semantics are equivalent.
- Where payloads differ irreconcilably, the divergence is documented rather than
  silently normalized.

## Phase 5: REST async jobs and event streaming

Implementation status: partially complete. REST local async dispatch, `LocalJob`,
`Job.Wait`, job lookup, event stream subscription, and minion-return event
normalization are implemented. Reconnect/heartbeat behavior and async
runner/wheel support are deferred.

Deliverables:

- Async local dispatch.
- JID extraction and `Job` implementation.
- Idempotent `Job.Wait` with cached result.
- Job lookup fallback.
- REST event stream reader.
- Event filtering by JID, tag, and minion where feasible.
- Heartbeat and reconnect strategy. Deferred.
- Progress events for per-minion returns.
- Documented behavior for `Events` before and after `Wait`.
- Context cancellation behavior, including whether dispatched Salt jobs continue.

Acceptance criteria:

- `Client.Start` returns a usable job handle.
- `Job.Wait` returns a normalized result.
- Event stream shutdown honors context cancellation.
- Lost or interrupted event streams can fall back to job lookup where supported.
  Partially satisfied because final result collection uses job lookup; event
  stream reconnect itself is not implemented.
- Runner and wheel `Start` calls return `Job`, not `LocalJob`, and have clear
  semantics without expected minions. Deferred; runner/wheel async currently
  return `UnsupportedError`.

## Phase 5.5: Transport contract/parity suite (`brinetest`)

Implementation status: implemented for the first REST parity gate. The suite now
covers transport info, sync local/runner/wheel calls, foundational local
`cmd.run`, state calls, raw lowstate, async wait including success and failure
idempotency, event stream opening/matching, target resolution, and
unsupported-capability contracts. Minion-return event normalization remains
capability-gated and unit-covered for REST-supported Salt tag shapes, but REST no
longer advertises guaranteed `CapStreamingReturns` because live Salt event tags
are timing/version dependent.

Goal: lock down Brine's transport-neutral semantics before adding another real
transport.

`brinetest` should follow the pattern used by `ruffel/invoke/invoketest`: a
reusable contract suite that transport authors can run against a configured
client/harness. Contracts should compare normalized public API behavior, not raw
transport payloads.

Deliverables:

- Add a `brinetest` package with `TestCase`, categories, prereq/capability
  checks, absent-capability checks, and `Verify(t, Harness)`.
- Define a `Harness` containing a `*brine.Client`, target, expected minions,
  state SLS names, and optional cleanup.
- Add info contracts for stable transport names and advertised capabilities.
- Add sync contracts for:
  - local `test.ping` success;
  - runner scalar result;
  - wheel scalar result;
  - state success;
  - state full failure with `ExecutionError`;
  - state partial failure with successful returns preserved;
  - raw lowstate local `test.ping` scalar behavior.
- Add async contracts for:
  - local async start/wait success;
  - local async start/wait partial failure;
  - successful and failed wait idempotency;
  - `LocalJob` expected-minion behavior.
- Add event contracts gated by `CapEvents` and `CapStreamingReturns`:
  - job event stream opens;
  - a matching job event can be received;
  - minion return events normalize to `EventMinionReturned` when Salt emits a
    supported return tag shape.
- Add unsupported-capability contracts for explicit `UnsupportedError` behavior.
- Run `brinetest` against REST integration.
- Optionally run a smaller scripted subset against `transports/mock` if useful.
- Document which contracts are mandatory, capability-gated, or best-effort due
  to Salt event timing.

Acceptance criteria:

- Contract tests skip when required capabilities are absent.
- REST passes the info, sync, state, lowstate, async wait, event stream, target,
  and unsupported-capability contracts in the compose harness.
- Event contracts are stable enough for opt-in integration runs and do not make
  `go test ./...` require Docker.
- The contract suite becomes the acceptance gate for Python transport parity.
- Contract assertions compare semantic projections: OK status, returned/missing
  minions, failed minions, scalar decode shape, state summaries, failure kinds,
  JID presence, and event type/JID/minion/payload kind.
- Raw JSON equality is explicitly avoided except in fixture normalizer tests.

## Phase 6: Python transport decision point

Python should aim to be as compatible as practical with the REST transport for
the Salt `v3006` local-master deployment. Before implementation, finish the
`brinetest` contract suite and choose which Python mode is required to reach the
needed compatibility level.

### Option A: Full Python transport

A long-lived Python helper process with a stable, versioned JSON protocol.

Expected capabilities:

- local synchronous execution;
- local asynchronous dispatch;
- per-minion iterative returns;
- job wait or job lookup where available;
- runner support;
- selected wheel support;
- target resolution;
- event streaming if the helper can access the master event bus.

Tradeoffs:

- more complex process lifecycle;
- must manage helper startup, shutdown, logging, and protocol versioning;
- better fit for streaming events and repeated calls;
- closer to feature parity with REST when running on the Salt master.

Protocol deliverables:

- first message is a hello frame with major/minor protocol version;
- hello frame includes advertised capabilities and detectable Salt version;
- all frames are JSON objects with a type field;
- errors are structured JSON, not human-only strings;
- the Go side rejects unsupported major versions;
- stderr is reserved for diagnostics and never parsed as protocol.

### Option B: Command bridge transport

A short-lived helper process per request.

Expected capabilities:

- local synchronous execution;
- target resolution;
- streamed per-command JSON lines if the helper emits them;
- synthetic progress events for the current command.

Unsupported or limited capabilities:

- global event stream;
- robust reconnect;
- efficient high-concurrency operation;
- clean async job management across process boundaries.

Implementation status: MVP Option B command bridge is implemented. It starts a
short-lived helper process per request and advertises only synchronous local
execution plus responsive target resolution. REST remains the production-oriented
backend; Python provides compatibility coverage for foundational local workflows
where Salt's Python libraries are available.

Recommendation:

- Implement REST first against the localhost Salt API endpoint.
- Keep Python in the design as a compatibility backend, not merely a throwaway
  shim.
- Use the implemented Option B bridge for MVP migration/no-REST environments and
  advertise only local synchronous execution and target resolution.
- If Python is needed as a long-term first-class backend or REST parity is
  required, invest in Option A and run it against the same fixture matrix and
  full `brinetest` suite as REST.

Acceptance criteria:

- Python transport advertises exactly what it supports.
- Unsupported methods return `UnsupportedError` rather than silently degrading.
- The helper protocol is JSON; version negotiation is deferred until a
  long-lived or externally distributed helper is needed.
- The Go API remains unchanged regardless of Python mode.
- Python contract coverage identifies divergence from REST behavior through
  capability-gated skips or failures; future fixture coverage should document
  whether any raw payload divergence is a bug, unsupported capability, or
  intentional transport difference.
- Python is run through `brinetest`; unsupported contracts are skipped only when
  the advertised capability set makes the skip explicit.

## Phase 7: Request middleware and orchestration integration

Implementation status: initial caller-owned middleware and observer examples are
implemented and covered with mock-backed tests. Migration-specific workflows can
now build on these examples without adding product policy to the core package.

Deliverables:

- Optional caller-owned middleware examples.
- Example middleware for adding kwargs, pillar data, or target transformations.
- Example middleware that fetches runner/local data through an unwrapped handler
  and merges it into pillar data.
- Documentation showing where orchestration-specific state expansion belongs.
- Observer adapters for terminal progress and JSON-line output.
- Example showing middleware using an unwrapped handler for internal runner/local
  calls to avoid recursion.

Acceptance criteria:

- Product-specific behavior is expressible outside the core package.
- Middleware can be tested with the mock transport.
- Middleware can safely perform internal Salt calls without recursive
  self-application.
- Core transport implementations remain generic Salt integrations.

## Phase 8: Migration of existing callers

Implementation status: migration guidance, compile-time examples, and
foundational typed module helpers are in place. This repository does not
currently contain product process-wrapper callers to replace directly; downstream
callers can use `MIGRATION.md`, `modules`, and the migration examples as the
migration checklist.

Deliverables:

- Replace process-wrapper calls with `Client` calls.
- Preserve existing user-visible behavior where required.
- Move progress rendering to observers or caller-side event handling.
- Move orchestration-specific request mutation to middleware.
- Configure malformed-state retry through generic retry middleware.
- Add integration tests for representative workflows.

Acceptance criteria:

- The caller no longer parses ad-hoc subprocess output for normal operation.
- Partial results are rendered deliberately.
- Retries are configured by policy.
- REST and mock transports exercise the same public API.
- Python remains available only to the extent advertised by capabilities.

## Risks and mitigations

### REST API unavailable

Mitigation: retain Python transport option with capability subsets.

### REST event streaming unreliable

Mitigation: use event streaming for progress, but fall back to job lookup for
final result collection where possible.

### Python helper becomes a second API

Mitigation: keep Python-specific protocol private to `transports/python`; expose
only the root `Transport` interface.

### Salt response shapes vary by version

Mitigation: preserve raw payloads, keep normalizers tolerant, and add fixture
coverage for deployed Salt versions.

### Product-specific policy leaks into the core package

Mitigation: provide middleware hooks and examples, but keep concrete policy in
callers or separate packages.

### Pillar context grows into hidden global behavior

Mitigation: Brine exposes pillar request options and middleware examples, but the
caller owns schema, data source selection, and predicates for when context is
injected.

## Current work checklist

Completed:

- [x] Review and edit `DESIGN.md`.
- [x] Confirm the handler/middleware design and chain builder.
- [x] Confirm the initial capability set.
- [x] Confirm explicit request kinds and lowstate construction.
- [x] Confirm sealed target types.
- [x] Confirm event envelope types, payload accessors, emitter, and async
  observer behavior.
- [x] Document cancellation/concurrency/resource cleanup contracts.
- [x] Write compile-time API examples.
- [x] Finalize the compose-backed integration harness strategy.
- [x] Capture real Salt response fixtures for REST sync and async workflows.
- [x] Implement root API skeleton.
- [x] Implement mock transport.
- [x] Implement REST synchronous run.
- [x] Add state decoding and retry tests.
- [x] Implement REST local async, job lookup, and event stream groundwork.
- [x] Keep `just test` and `just lint` green.

Recently completed:

- [x] Implement `brinetest` contract/parity suite.
- [x] Run `brinetest` against REST integration.
- [x] Add REST hardening covered by `brinetest` where practical.
- [x] Decide whether REST needs more hardening before Python.
- [x] Confirm Python mode requirements and target compatibility level.
- [x] Re-evaluate Python transport need.
- [x] Implement MVP Python command bridge for local synchronous execution.
- [x] Add Docker-backed Python `brinetest` contract recipe.
- [x] Add foundational local execution examples for service status and command output.
- [x] Add foundational typed helpers for cmd, service, and network execution modules.
- [x] Start Phase 7 request middleware and orchestration integration examples.
- [x] Start Phase 8 migration of existing callers.

Next:

- [x] Run `just contract-rest` and `just contract-python` against the live compose harness before release or handoff.
- [ ] Begin downstream caller migration using `MIGRATION.md`, or select one of the intentionally deferred REST/Python capabilities below if a concrete workflow requires it.

Intentionally deferred until needed:

- [x] Request metadata semantics need more concrete caller-facing examples.
- [x] Target type-switch exhaustiveness guard for transports.
- [x] REST `TransportInfo` Salt version probing; API version remains empty because rest_cherrypy exposes no stable API-version endpoint.
- [ ] REST target resolution semantics beyond responsive-minion resolution, if needed.
- [ ] REST event heartbeat/reconnect strategy.
- [ ] REST runner/wheel async semantics, if needed.
