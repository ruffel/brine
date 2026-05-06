#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/compose.yaml"
EXPECTED_MINIONS="${BRINE_EXPECTED_MINIONS:-3}"
TIMEOUT_SECONDS="${BRINE_READY_TIMEOUT:-180}"

end=$((SECONDS + TIMEOUT_SECONDS))
last_report=0
accepted_count=0
responding_count=0

printf 'Waiting for Salt master and %s minions to become ready...\n' "${EXPECTED_MINIONS}"

while (( SECONDS < end )); do
  if docker compose -f "${COMPOSE_FILE}" exec -T salt-master salt-key --list=accepted --out=json >/tmp/brine-salt-keys.json 2>/dev/null; then
    accepted_count="$(python3 - <<'PY'
import json
from pathlib import Path
try:
    data = json.loads(Path('/tmp/brine-salt-keys.json').read_text())
    print(len(data.get('minions', [])))
except Exception:
    print(0)
PY
)"
    if [[ "${accepted_count}" -ge "${EXPECTED_MINIONS}" ]]; then
      if docker compose -f "${COMPOSE_FILE}" exec -T salt-master salt '*' test.ping --out=json --static >/tmp/brine-test-ping.json 2>/dev/null; then
        responding_count="$(python3 - <<'PY'
import json
from pathlib import Path
try:
    data = json.loads(Path('/tmp/brine-test-ping.json').read_text())
    print(sum(1 for value in data.values() if value is True))
except Exception:
    print(0)
PY
)"
        if [[ "${responding_count}" -ge "${EXPECTED_MINIONS}" ]]; then
          printf 'Salt integration environment is ready (%s minions responding).\n' "${responding_count}"
          exit 0
        fi
      fi
    fi
  fi

  if (( SECONDS - last_report >= 15 )); then
    last_report=${SECONDS}
    printf 'Still waiting: %s accepted, %s responding. Current containers:\n' "${accepted_count}" "${responding_count}"
    docker compose -f "${COMPOSE_FILE}" ps || true
  fi

  sleep 3
done

printf 'Timed out waiting for Salt integration environment: %s accepted, %s responding.\n' "${accepted_count}" "${responding_count}" >&2
docker compose -f "${COMPOSE_FILE}" ps >&2 || true
printf '\nRecent salt-master logs:\n' >&2
docker compose -f "${COMPOSE_FILE}" logs --tail=80 salt-master >&2 || true
printf '\nRecent minion logs:\n' >&2
minion_names=()
for i in $(seq 1 "${EXPECTED_MINIONS}"); do
  minion_names+=("minion-${i}")
done
docker compose -f "${COMPOSE_FILE}" logs --tail=80 "${minion_names[@]}" >&2 || true
exit 1
