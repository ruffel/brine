# Brine Salt integration harness

This directory contains an opt-in Salt `v3006` test topology for capturing real
REST fixtures and, later, running integration tests.

The harness is intentionally separate from normal unit tests. `go test ./...`
should not require Docker or Salt.

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

Start the environment:

```sh
test/integration/scripts/compose.sh -f test/integration/compose.yaml up -d --build
```

Start with a specific Salt patch version:

```sh
BRINE_SALT_VERSION=3006.9 test/integration/scripts/compose.sh -f test/integration/compose.yaml up -d --build
```

The `compose.sh` wrapper auto-detects Docker Compose v2 (`docker compose`) or the legacy standalone `docker-compose`. You can override detection with:

```sh
export BRINE_COMPOSE=docker-compose
```

Wait for all minions to respond:

```sh
test/integration/scripts/wait-ready.sh
```

Capture sanitized REST fixtures:

```sh
test/integration/scripts/capture-rest-fixtures.sh
```

Stop and remove containers/volumes:

```sh
test/integration/scripts/compose.sh -f test/integration/compose.yaml down -v
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
- Event stream capture is intentionally not part of v0. Add it after REST sync
  fixture capture is stable.
- Python transport fixtures should use this same topology. Prefer a separate
  compose service for a future long-lived Python helper rather than requiring the
  Go test runner to include Salt's Python runtime.
