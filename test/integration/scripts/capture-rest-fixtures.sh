#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="${ROOT_DIR}/fixtures/rest"
SALT_URL="${BRINE_SALT_URL:-http://127.0.0.1:${BRINE_SALT_API_PORT:-8000}}"
USERNAME="${BRINE_SALT_USERNAME:-saltapi}"
PASSWORD="${BRINE_SALT_PASSWORD:-saltapi}"
EAUTH="${BRINE_SALT_EAUTH:-pam}"

mkdir -p "${FIXTURE_DIR}"

login_payload="$(python3 - <<PY
import json
print(json.dumps({'username': '${USERNAME}', 'password': '${PASSWORD}', 'eauth': '${EAUTH}'}))
PY
)"

login_response="${FIXTURE_DIR}/login.json"
curl -fsS -H 'Accept: application/json' -H 'Content-Type: application/json' \
  -d "${login_payload}" "${SALT_URL}/login" > "${login_response}"

TOKEN="$(python3 - "${login_response}" <<'PY'
import json
import sys
from pathlib import Path
body = json.loads(Path(sys.argv[1]).read_text())
print(body['return'][0]['token'])
PY
)"

post_lowstate() {
  name="$1"
  payload="$2"
  curl -fsS -H 'Accept: application/json' -H 'Content-Type: application/json' -H "X-Auth-Token: ${TOKEN}" \
    -d "${payload}" "${SALT_URL}/" > "${FIXTURE_DIR}/${name}.json"
}

post_lowstate test_ping '[{"client":"local","tgt":"*","fun":"test.ping"}]'
post_lowstate test_ping_list '[{"client":"local","tgt":["minion-1","minion-2"],"tgt_type":"list","fun":"test.ping"}]'
post_lowstate state_success '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.success"]}]'
post_lowstate state_fail '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.fail"]}]'
post_lowstate state_conditional_fail '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.conditional_fail"]}]'
post_lowstate state_pillar_echo '[{"client":"local","tgt":"*","fun":"state.sls","arg":["brine.pillar_echo"],"kwarg":{"pillar":{"brine":{"message":"hello from per-run pillar"}}}}]'
post_lowstate runner_manage_alived '[{"client":"runner","fun":"manage.alived"}]'
post_lowstate runner_jobs_active '[{"client":"runner","fun":"jobs.active"}]'

"${ROOT_DIR}/scripts/sanitize-fixtures.sh" "${FIXTURE_DIR}"/*.json
printf 'Captured sanitized REST fixtures in %s\n' "${FIXTURE_DIR}"
