#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/compose.yaml"
COMPOSE_CMD=("${ROOT_DIR}/scripts/compose.sh")

exec "${COMPOSE_CMD[@]}" -f "${COMPOSE_FILE}" exec -T salt-master python3 /opt/brine/brine_salt_bridge.py
