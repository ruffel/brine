# Brine compatibility

Brine compatibility is expressed in transport capabilities and verified with the
`brinetest` contract suite. The matrix below describes the intended MVP support
level for the built-in transports; run `just compat` against the integration
Salt topology for the live contract report.

## Transport capability matrix

| Capability area | REST transport | Python bridge transport |
| --- | --- | --- |
| Transport info | Supported, including best-effort Salt version detection | Supported, limited to bridge metadata |
| Local `Run` | Supported; defaults to async-backed collection | Supported through Salt `LocalClient` |
| Runner `Run` | Supported | Supported through Salt `RunnerClient` |
| Wheel `Run` | Supported | Unsupported |
| Raw lowstate `Run` | Supported | Unsupported |
| Local async `Start` / `Wait` | Supported | Unsupported |
| Job events / global events | Supported through `rest_cherrypy` SSE | Unsupported |
| Run-scoped progress | Supported | Supported when the bridge emits streaming frames |
| Batch execution | Supported | Unsupported |
| Target resolution | Supported through `test.ping` | Supported through `test.ping` |
| Missing-minion detection | Strongest in REST async/list-target flows; direct list targets also mark missing returns | Limited to minions the bridge can gather before execution |

## Python bridge status

The Python bridge is useful when direct REST access is unavailable, but it is
not feature-equivalent with REST. It starts one helper process per request and
currently focuses on synchronous local and runner workflows. Contract coverage
for Python is expected to pass for:

- transport info;
- local `test.ping`, `cmd.run`, and state execution;
- runner scalar results;
- run-scoped progress from streaming local frames;
- target resolution through responsive minions;
- explicit unsupported-operation errors for capabilities it does not advertise.

Python intentionally does not advertise wheel, lowstate, async job, batch, or
global event capabilities. Those contracts should be skipped or should verify
`brine.ErrUnsupported` behavior rather than pass as supported features.

The largest Python compatibility caveat is missing-minion semantics. The bridge
uses Salt's `gather_minions` before execution, then runs the command against the
responsive gathered list. That means it can report which minions it expects from
that gathered set, but it cannot always prove that an explicit target contained
offline or nonexistent minions. Use REST for infrastructure workflows where
missing-minion detection is safety-critical.

## Checking compatibility

Start the integration topology and run the matrix reporter:

```sh
just integration-up
just compat
```

For machine-readable output:

```sh
just compat-json
```

To see the contract IDs without starting Salt:

```sh
go run ./cmd/brine-compatcheck --list-contracts
```
