#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/compose.yaml"
read -r -a COMPOSE_CMD <<< "${BRINE_COMPOSE:-docker compose}"
EXPECTED_MINIONS="${BRINE_EXPECTED_MINIONS:-3}"
TIMEOUT_SECONDS="${BRINE_READY_TIMEOUT:-180}"

end=$((SECONDS + TIMEOUT_SECONDS))

printf 'Waiting for Salt master and %s minions to become ready...\n' "${EXPECTED_MINIONS}"

while (( SECONDS < end )); do
  if "${COMPOSE_CMD[@]}" -f "${COMPOSE_FILE}" exec -T salt-master salt-key --list=accepted --out=json >/tmp/brine-salt-keys.json 2>/dev/null; then
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
      if "${COMPOSE_CMD[@]}" -f "${COMPOSE_FILE}" exec -T salt-master salt '*' test.ping --out=json >/tmp/brine-test-ping.json 2>/dev/null; then
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

  sleep 3
done

printf 'Timed out waiting for Salt integration environment.\n' >&2
"${COMPOSE_CMD[@]}" -f "${COMPOSE_FILE}" ps >&2 || true
exit 1
