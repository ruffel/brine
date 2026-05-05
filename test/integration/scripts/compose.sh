#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${BRINE_COMPOSE:-}" ]]; then
  read -r -a compose_cmd <<< "${BRINE_COMPOSE}"
  exec "${compose_cmd[@]}" "$@"
fi

if docker compose version >/dev/null 2>&1; then
  exec docker compose "$@"
fi

if command -v docker-compose >/dev/null 2>&1; then
  exec docker-compose "$@"
fi

cat >&2 <<'EOF'
Could not find a usable Docker Compose command.

Install Docker Compose v2 (`docker compose`) or the legacy standalone
`docker-compose`, or set BRINE_COMPOSE to the command you want to use, e.g.:

  BRINE_COMPOSE=docker-compose just integration-up
EOF

exit 127
