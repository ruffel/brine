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

    emit(response)
    return 0


def handle(request: dict[str, Any]) -> dict[str, Any]:
    kind = request.get("kind")
    if kind == "local":
        return run_local(request)
    if kind == "runner":
        return run_runner(request)

    return {
        "error": {
            "kind": "unsupported",
            "message": f"unsupported request kind {kind!r}",
        }
    }


def run_local(request: dict[str, Any]) -> dict[str, Any]:
    import salt.client  # Imported lazily so protocol tests do not need Salt installed.

    function = request.get("function")
    target = request.get("target") or {}
    target_expr = target.get("expression")
    target_type = target.get("type") or "glob"
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}
    options = request.get("options") or {}
    timeout = options.get("timeout") or None

    if not function:
        return {"error": {"kind": "protocol", "message": "missing function"}}
    if target_expr is None or target_expr == "" or target_expr == []:
        return {"error": {"kind": "protocol", "message": "missing target"}}

    client = salt.client.LocalClient()
    minions = list(client.gather_minions(target_expr, target_type) or [])
    emit({"type": "minions", "minions": minions})

    for node in client.cmd_iter(
        minions,
        function,
        args,
        tgt_type="list",
        kwarg=kwargs,
        timeout=timeout,
    ):
        if not isinstance(node, dict) or not node:
            continue

        minion, data = next(iter(node.items()))
        emit(minion_frame(str(minion), data))

    return {"type": "done"}


def run_runner(request: dict[str, Any]) -> dict[str, Any]:
    import salt.config  # Imported lazily so protocol tests do not need Salt installed.
    import salt.runner  # Imported lazily so protocol tests do not need Salt installed.

    function = request.get("function")
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}
    if not function:
        return {"error": {"kind": "protocol", "message": "missing function"}}

    opts = salt.config.master_config("/etc/salt/master")
    opts.update({"quiet": True})
    runner = salt.runner.RunnerClient(opts)
    try:
        value = runner.cmd(function, args, kwarg=kwargs)
    except TypeError:
        # Older Salt runner.cmd signatures do not accept kwarg.
        value = runner.cmd(function, args)

    return {"type": "scalar", "scalar": value}


def emit(frame: dict[str, Any]) -> None:
    _ = json.dump(frame, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


def minion_frame(minion: str, value: Any) -> dict[str, Any]:
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

    return {
        "type": "return",
        "minion": minion,
        "jid": jid,
        "retcode": retcode,
        "body": ret,
        "error_message": error,
        "raw": value,
    }


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
