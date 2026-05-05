#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="${ROOT_DIR}/fixtures/rest"
SALT_URL="${BRINE_SALT_URL:-http://127.0.0.1:${BRINE_SALT_API_PORT:-8000}}"
USERNAME="${BRINE_SALT_USERNAME:-saltapi}"
PASSWORD="${BRINE_SALT_PASSWORD:-saltapi}"
EAUTH="${BRINE_SALT_EAUTH:-pam}"
AUTH_MODE="${BRINE_SALT_AUTH_MODE:-pam}"
TOKEN=""

mkdir -p "${FIXTURE_DIR}"

curl_json() {
  name="$1"
  url="$2"
  payload="$3"
  output="$4"
  shift 4

  tmp="${output}.tmp"
  status="$(curl -sS -o "${tmp}" -w '%{http_code}' "$@" -d "${payload}" "${url}" || true)"
  if [[ "${status}" -lt 200 || "${status}" -ge 300 ]]; then
    printf 'REST fixture capture failed for %s: HTTP %s from %s\n' "${name}" "${status}" "${url}" >&2
    printf 'Response body:\n' >&2
    cat "${tmp}" >&2 || true
    printf '\n' >&2
    rm -f "${tmp}"
    exit 1
  fi

  mv "${tmp}" "${output}"
}

if [[ "${AUTH_MODE}" != "noauth" ]]; then
  login_payload="$(USERNAME="${USERNAME}" PASSWORD="${PASSWORD}" EAUTH="${EAUTH}" python3 - <<'PY'
import json
import os
print(json.dumps({
    'username': os.environ['USERNAME'],
    'password': os.environ['PASSWORD'],
    'eauth': os.environ['EAUTH'],
}))
PY
)"

  login_response="${FIXTURE_DIR}/login.json"
  curl_json login "${SALT_URL}/login" "${login_payload}" "${login_response}" \
    -H 'Accept: application/json' -H 'Content-Type: application/json'

  TOKEN="$(python3 - "${login_response}" <<'PY'
import json
import sys
from pathlib import Path
body = json.loads(Path(sys.argv[1]).read_text())
print(body['return'][0]['token'])
PY
)"
fi

post_lowstate() {
  local name="$1"
  local payload="$2"
  local headers=(-H 'Accept: application/json' -H 'Content-Type: application/json')
  if [[ -n "${TOKEN}" ]]; then
    headers+=(-H "X-Auth-Token: ${TOKEN}")
  fi
  curl_json "${name}" "${SALT_URL}/" "${payload}" "${FIXTURE_DIR}/${name}.json" "${headers[@]}"
}

fixture_jid() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys
from pathlib import Path
body = json.loads(Path(sys.argv[1]).read_text())
print(body['return'][0]['jid'])
PY
}

lookup_count() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys
from pathlib import Path
try:
    item = json.loads(Path(sys.argv[1]).read_text())['return'][0]
    if isinstance(item, dict) and isinstance(item.get('data'), dict):
        item = item['data']
    print(len(item) if isinstance(item, dict) else 0)
except Exception:
    print(0)
PY
}

capture_async() {
  local name="$1"
  local payload="$2"
  local expected_count="${3:-3}"
  local jid
  local lookup_payload

  post_lowstate "${name}_start" "${payload}"
  jid="$(fixture_jid "${FIXTURE_DIR}/${name}_start.json")"
  lookup_payload="$(JID="${jid}" python3 - <<'PY'
import json
import os
print(json.dumps([{'client': 'runner', 'fun': 'jobs.lookup_jid', 'arg': [os.environ['JID']]}]))
PY
)"

  for _ in 1 2 3 4 5 6 7 8 9 10; do
    post_lowstate "${name}_lookup" "${lookup_payload}"
    if [[ "$(lookup_count "${FIXTURE_DIR}/${name}_lookup.json")" -ge "${expected_count}" ]]; then
      return
    fi
    sleep 1
  done

  printf 'Timed out waiting for async fixture %s lookup to contain %s returns\n' "${name}" "${expected_count}" >&2
  exit 1
}

post_lowstate test_ping '[{"client":"local","tgt":"*","fun":"test.ping"}]'
post_lowstate test_ping_list '[{"client":"local","tgt":["minion-1","minion-2"],"tgt_type":"list","fun":"test.ping"}]'
post_lowstate state_success '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.success"]}]'
post_lowstate state_changed '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.changed"]}]'
post_lowstate state_unchanged '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.unchanged"]}]'
post_lowstate state_fail '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.fail"]}]'
post_lowstate state_conditional_fail '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.conditional_fail"]}]'
post_lowstate state_pillar_echo_warmup '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.pillar_echo"],"kwarg":{"pillar":{"brine":{"message":"hello from per-run pillar"}}}}]'
post_lowstate state_pillar_echo '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.pillar_echo"],"kwarg":{"pillar":{"brine":{"message":"hello from per-run pillar"}}}}]'
rm -f "${FIXTURE_DIR}/state_pillar_echo_warmup.json"
post_lowstate runner_manage_alived '[{"client":"runner","fun":"manage.alived"}]'
post_lowstate runner_jobs_active '[{"client":"runner","fun":"jobs.active"}]'

capture_async async_test_ping '[{"client":"local_async","tgt":"*","fun":"test.ping"}]'
capture_async async_state_conditional_fail '[{"client":"local_async","tgt":"*","fun":"state.sls","arg":["brine.conditional_fail"]}]'

"${ROOT_DIR}/scripts/sanitize-fixtures.sh" "${FIXTURE_DIR}"/*.json
printf 'Captured sanitized REST fixtures in %s\n' "${FIXTURE_DIR}"
