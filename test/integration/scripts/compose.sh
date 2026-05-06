#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${BRINE_COMPOSE:-}" ]]; then
  # shellcheck disable=SC2206 # BRINE_COMPOSE intentionally contains a command plus optional fixed args.
  compose_cmd=(${BRINE_COMPOSE})
  exec "${compose_cmd[@]}" "$@"
fi

if docker compose version >/dev/null 2>&1; then
  exec docker compose "$@"
fi

if command -v docker-compose >/dev/null 2>&1; then
  exec docker-compose "$@"
fi

printf 'Could not find Docker Compose. Install the docker compose plugin, docker-compose, or set BRINE_COMPOSE.\n' >&2
exit 127
