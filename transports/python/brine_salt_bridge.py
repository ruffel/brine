#!/usr/bin/env python3
# pyright: reportMissingImports=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportExplicitAny=false, reportAny=false, reportUnusedCallResult=false
"""Minimal JSON bridge from Brine to Salt's Python LocalClient.

The bridge intentionally supports a narrow MVP protocol: synchronous local
execution. It reads one JSON request from stdin and writes one JSON response to
stdout. Diagnostics and tracebacks go into the JSON error object rather than
stderr so the Go side never has to parse human-oriented output.
"""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any


def main() -> int:
    try:
        request = json.load(sys.stdin)
        response = handle(request)
    except Exception as exc:  # pragma: no cover - defensive protocol guard.
        response = {
            "error": {
                "kind": "exception",
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
        }

    _ = json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


def handle(request: dict[str, Any]) -> dict[str, Any]:
    kind = request.get("kind")
    if kind != "local":
        return {
            "error": {
                "kind": "unsupported",
                "message": f"unsupported request kind {kind!r}",
            }
        }

    return run_local(request)


def run_local(request: dict[str, Any]) -> dict[str, Any]:
    import salt.client  # Imported lazily so protocol tests do not need Salt installed.

    function = request.get("function")
    target = request.get("target") or {}
    target_expr = target.get("expression")
    target_type = target.get("type") or "glob"
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}

    if not function:
        return {"error": {"kind": "protocol", "message": "missing function"}}
    if target_expr is None or target_expr == "" or target_expr == []:
        return {"error": {"kind": "protocol", "message": "missing target"}}

    client = salt.client.LocalClient()
    raw = client.cmd(
        target_expr,
        function,
        args,
        kwarg=kwargs,
        tgt_type=target_type,
        full_return=True,
    )

    return {"local": normalize_local(raw)}


def normalize_local(raw: Any) -> dict[str, Any]:
    by_minion: dict[str, Any] = {}
    if not isinstance(raw, dict):
        return {"by_minion": by_minion, "raw": raw}

    for minion, value in raw.items():
        ret = value
        retcode = 0
        jid = ""
        error = ""

        if isinstance(value, dict) and (
            "ret" in value
            or "return" in value
            or "retcode" in value
            or "jid" in value
            or "error" in value
        ):
            if "ret" in value:
                ret = value.get("ret")
            elif "return" in value:
                ret = value.get("return")
            retcode = int(value.get("retcode") or 0)
            jid = str(value.get("jid") or "")
            error = str(value.get("error") or "")

        by_minion[str(minion)] = {
            "jid": jid,
            "retcode": retcode,
            "return": ret,
            "error": error,
            "raw": value,
        }

    return {"by_minion": by_minion, "raw": raw}


if __name__ == "__main__":
    raise SystemExit(main())
