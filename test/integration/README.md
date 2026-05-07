# Brine Salt integration harness

This directory contains an opt-in Salt `v3006` test topology for capturing real
REST fixtures and running REST/Python integration and contract tests.

The harness is intentionally separate from normal unit tests. `go test ./...`
should not require Docker or Salt.

## Ownership

`test/integration` owns Docker/Salt lifecycle for this repository, with Justfile
recipes as the normal entry points. The public `brinetest` package is only the
transport contract suite; it assumes a deterministic environment already exists
and does not start, stop, or configure containers, Salt masters, minions, or
volumes.

## Topology

- `salt-master`
  - runs `salt-master` and `salt-api`
  - exposes rest_cherrypy on `127.0.0.1:8000` by default
  - uses test-only `auto_accept: True`
  - serves states from `salt/states`
  - serves pillar from `salt/pillar`
- `minion-1`, `minion-2`, `minion-3`
  - stable minion IDs
  - connect to `salt-master`

The image is built from `image/Dockerfile` with Salt `3006.9` by default.
Override with `BRINE_SALT_VERSION` if a different Salt `v3006` patch level is required. The older `SALT_VERSION` variable is also honored as a fallback.

## Usage

Start the environment from the repository root:

```sh
just integration-up
```

Manual equivalent; choose one compose command, then wait for readiness:

```sh
docker compose -f test/integration/compose.yaml up -d --build --force-recreate
BRINE_SALT_VERSION=3006.9 docker compose -f test/integration/compose.yaml up -d --build --force-recreate
test/integration/scripts/wait-ready.sh
```

Run REST contract/parity tests against the live Salt environment:

```sh
just contract-rest
```

Run Python command bridge contract/parity tests against the live Salt environment:

```sh
just contract-python
```

Print a REST/Python compatibility table from the contract suites:

```
just compat
```

Emit the same compatibility report as JSON for CI artifacts or downstream
processing:

```
just compat-json
```

`just compat` and `just compat-json` run `cmd/brine-compatcheck`, a developer
compatibility reporter that invokes the integration-tagged contract suites. The
reporter can also list and filter contracts directly, for example:

```
go run ./cmd/brine-compatcheck --list-contracts
go run ./cmd/brine-compatcheck --category state
go run ./cmd/brine-compatcheck --contract sync/local-test-ping
```

For live Salt diagnostics, use the separate `cmd/brine` CLI, for example via
`just cli local test.ping '*'`.

Event-stream contracts use `test.sleep` to avoid racing Salt return events before
REST `/events` subscriptions are established. The default sleep is two seconds;
set `BRINE_EVENT_SLEEP_SECONDS` to tune stability versus runtime on slower
machines:

```
BRINE_EVENT_SLEEP_SECONDS=5 just compat
```

Run both contract suites against the live Salt environment:

```sh
just contract
```

Capture sanitized REST fixtures:

```sh
test/integration/scripts/capture-rest-fixtures.sh
```

Stop and remove containers/volumes:

```sh
just integration-down
```

Manual equivalent:

```sh
docker compose -f test/integration/compose.yaml down -v
```

## REST defaults

The fixture script defaults to:

```sh
BRINE_SALT_URL=http://127.0.0.1:8000
BRINE_SALT_USERNAME=saltapi
BRINE_SALT_PASSWORD=saltapi
BRINE_SALT_EAUTH=pam
BRINE_SALT_AUTH_MODE=pam
BRINE_EXPECTED_MINIONS=3
```

These credentials and `auto_accept: True` are for local test use only.

To capture against an endpoint that accepts unauthenticated localhost requests,
set:

```sh
BRINE_SALT_AUTH_MODE=noauth
```

In `noauth` mode the capture script skips `/login` and does not send
`X-Auth-Token`.

## Captured matrix

`capture-rest-fixtures.sh` captures:

- login, except when `BRINE_SALT_AUTH_MODE=noauth`
- `test.ping` against glob target
- `test.ping` against list target
- `state.sls brine.success`
- `state.sls brine.changed`
- `state.sls brine.unchanged`
- `state.sls brine.fail`
- `state.sls brine.conditional_fail`
- `state.sls brine.pillar_echo` with per-run pillar
- `runner.manage.alived`
- `runner.jobs.active`
- async `test.ping` start and `jobs.lookup_jid`
- async `state.sls brine.conditional_fail` start and `jobs.lookup_jid`

Fixtures are sanitized in place by `sanitize-fixtures.sh`.

## Notes

- REST sync integration tests expect deterministic minion IDs beginning at
  `minion-1` through `minion-$BRINE_EXPECTED_MINIONS` and the test states in
  this directory to be available on the target Salt master.
- Event stream fixtures are intentionally not captured yet; event behavior is
  covered by opt-in REST integration and `brinetest` contract tests.
- The Python command bridge contracts execute the mounted helper inside the
  `salt-master` container so the Go test runner does not need Salt's Python
  runtime installed locally. A future long-lived Python helper should use this
  same topology and advertise only the capabilities it implements.
