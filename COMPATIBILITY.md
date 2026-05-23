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
| Raw lowstate `Run` | Supported | Unsupported |
| Local async `Start` / `Wait` | Supported | Supported through short-lived bridge operations |
| Job events / global events | Supported through `rest_cherrypy` SSE | Unsupported |
| Run-scoped progress | Supported | Supported when the bridge emits streaming frames |
| Batch execution | Supported | Supported for local `Run` |
| Target resolution | Supported through `test.ping` | Supported through `test.ping` |
| Missing-minion detection | Strongest in REST async/list-target flows; direct list targets also mark missing returns | Limited to minions the bridge can gather before execution |

## Python bridge status

The Python bridge is useful when direct REST access is unavailable, but it is
not feature-equivalent with REST. It starts one helper process per request and
currently focuses on local and runner workflows. Local async uses one helper
process to dispatch a jid and another helper process to poll `jobs.lookup_jid`
and stream minion-return frames. Contract coverage for Python is expected to
pass for:

- transport info;
- local `test.ping`, `cmd.run`, and state execution;
- runner scalar results;
- local async `Start` / `Wait`;
- run-scoped progress from streaming local frames;
- local batch execution;
- target resolution through responsive minions;
- explicit unsupported-operation errors for capabilities it does not advertise.

Python intentionally does not advertise lowstate or global event capabilities.
Those contracts should be skipped or should verify `brine.ErrUnsupported`
behavior rather than pass as supported features.

The largest Python compatibility caveat is missing-minion semantics for glob,
compound, grain, pillar, and nodegroup targets. For explicit list targets, the
bridge reports the original list as expected so offline or nonexistent entries
can be marked missing while execution is sent only to responsive gathered
minions. For dynamic target expressions, the bridge can only report the minions
Salt's `gather_minions` found before execution. Use REST when missing-minion
detection for non-list target expressions is safety-critical.

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
