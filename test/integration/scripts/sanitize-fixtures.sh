#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 ]]; then
  printf 'usage: %s <fixture.json> [fixture.json...]\n' "$0" >&2
  exit 2
fi

python3 - "$@" <<'PY'
import json
import re
import sys
from pathlib import Path

REDACTED = '<redacted>'
VOLATILE_KEYS = {
    'token', 'expire', 'start_time', '_stamp', 'tgt_uuid', 'jid', 'id',
    'fqdn_ip4', 'ipv4', 'ipv6', 'master', 'localhost'
}
IP_RE = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
JID_RE = re.compile(r'\b20\d{18}\b')

def sanitize(value):
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key in VOLATILE_KEYS or key.endswith('_token'):
                out[key] = REDACTED
            else:
                out[key] = sanitize(item)
        return out
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        value = IP_RE.sub('<ip>', value)
        value = JID_RE.sub('<jid>', value)
        return value
    return value

for arg in sys.argv[1:]:
    path = Path(arg)
    data = json.loads(path.read_text())
    path.write_text(json.dumps(sanitize(data), indent=2, sort_keys=True) + '\n')
PY
