#!/usr/bin/env python3
# pyright: reportMissingImports=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportExplicitAny=false, reportAny=false, reportUnusedCallResult=false
"""JSON bridge from Brine to Salt's Python clients.

The bridge is intentionally small and process-per-operation. It reads one JSON
request from stdin and writes newline-delimited JSON frames to stdout.
Diagnostics and tracebacks go into JSON error objects rather than stderr so the
Go side never has to parse human-oriented output.

Supported operations:

* local/run: gather responsive minions, execute cmd_iter, and stream minion
  return frames.
* local/start: dispatch cmd_async and return a started frame with the jid and
  responsive minions gathered before dispatch.
* local/wait: poll jobs.lookup_jid for a jid and stream newly observed minion
  returns until all expected minions have returned or the optional wait timeout
  expires.
* runner/run: execute a runner function and return a scalar frame.
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from typing import Any, Optional

PROTOCOL_VERSION = 1


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

    if response is not None:
        emit(response)
    return 0


def handle(request: dict[str, Any]) -> Any:
    protocol_version = int(request.get("protocol_version") or PROTOCOL_VERSION)
    if protocol_version != PROTOCOL_VERSION:
        return {
            "error": {
                "kind": "protocol",
                "message": f"unsupported protocol_version {protocol_version}",
            }
        }

    kind = request.get("kind")
    operation = request.get("operation") or "run"

    if kind == "local":
        if operation == "run":
            return run_local(request)
        if operation == "start":
            return start_local(request)
        if operation == "wait":
            return wait_local(request)

        return unsupported(
            f"unsupported local operation {operation!r}", operation=operation
        )

    if kind == "runner" and operation == "run":
        return run_runner(request)

    return unsupported(f"unsupported request kind {kind!r}")


def unsupported(message: str, operation: Optional[str] = None) -> dict[str, Any]:
    error: dict[str, Any] = {"kind": "unsupported", "message": message}
    if operation:
        error["operation"] = operation

    return {"error": error}


def local_fields(request: dict[str, Any]) -> Any:
    function = request.get("function")
    target = request.get("target") or {}
    target_expr = target.get("expression")
    target_type = target.get("type") or "glob"
    args = request.get("args") or []
    kwargs = request.get("kwargs") or {}
    options = request.get("options") or {}

    if not function:
        return {"error": {"kind": "protocol", "message": "missing function"}}
    if target_expr is None or target_expr == "" or target_expr == []:
        return {"error": {"kind": "protocol", "message": "missing target"}}

    return (
        str(function),
        target_expr,
        str(target_type),
        list(args),
        dict(kwargs),
        dict(options),
    )


def run_local(request: dict[str, Any]) -> dict[str, Any]:
    import salt.client  # Imported lazily so protocol tests do not need Salt installed.

    fields = local_fields(request)
    if isinstance(fields, dict):
        return fields

    function, target_expr, target_type, args, kwargs, options = fields
    timeout = options.get("timeout") or None

    client = salt.client.LocalClient()
    minions = list(client.gather_minions(target_expr, target_type) or [])
    expected = expected_minions(target_expr, target_type, minions)
    emit({"type": "minions", "minions": expected})

    cmd_kwargs: dict[str, Any] = {
        "tgt_type": "list",
        "kwarg": kwargs,
        "timeout": timeout,
    }
    if options.get("full_return"):
        cmd_kwargs["full_return"] = True

    try:
        iterator = client.cmd_iter(minions, function, args, **cmd_kwargs)
    except TypeError:
        cmd_kwargs.pop("full_return", None)
        iterator = client.cmd_iter(minions, function, args, **cmd_kwargs)

    for node in iterator:
        if not isinstance(node, dict) or not node:
            continue

        minion, data = next(iter(node.items()))
        emit(minion_frame(str(minion), data))

    return {"type": "done"}


def start_local(request: dict[str, Any]) -> dict[str, Any]:
    import salt.client  # Imported lazily so protocol tests do not need Salt installed.

    fields = local_fields(request)
    if isinstance(fields, dict):
        return fields

    function, target_expr, target_type, args, kwargs, options = fields
    timeout = options.get("timeout") or None

    client = salt.client.LocalClient()
    minions = list(client.gather_minions(target_expr, target_type) or [])
    expected = expected_minions(target_expr, target_type, minions)

    cmd_kwargs: dict[str, Any] = {
        "tgt_type": target_type,
        "kwarg": kwargs,
    }
    if timeout:
        cmd_kwargs["timeout"] = timeout

    try:
        jid = client.cmd_async(target_expr, function, args, **cmd_kwargs)
    except TypeError:
        cmd_kwargs.pop("timeout", None)
        jid = client.cmd_async(target_expr, function, args, **cmd_kwargs)

    return {"type": "started", "jid": str(jid or ""), "minions": expected}


def expected_minions(
    target_expr: Any, target_type: str, gathered: list[Any]
) -> list[str]:
    if target_type == "list" and isinstance(target_expr, list):
        return [str(minion) for minion in target_expr]

    return [str(minion) for minion in gathered]


def wait_local(request: dict[str, Any]) -> dict[str, Any]:
    jid = str(request.get("jid") or "")
    if not jid:
        return {"error": {"kind": "protocol", "message": "missing jid"}}

    expected_value = request.get("expected")
    expected = (
        [str(minion) for minion in expected_value]
        if isinstance(expected_value, list)
        else []
    )
    expected_set = set(expected)
    expected_known = isinstance(expected_value, list)

    options = request.get("options") or {}
    poll_interval = float(options.get("poll_interval_ms") or 1000) / 1000.0
    if poll_interval <= 0:
        poll_interval = 1.0

    wait_timeout = float(options.get("wait_timeout") or 0)
    deadline = time.monotonic() + wait_timeout if wait_timeout > 0 else None

    emit({"type": "minions", "jid": jid, "minions": expected})
    seen: set[str] = set()

    while True:
        raw = lookup_jid(jid)
        local = normalize_local(job_return_data(raw))
        by_minion = local.get("by_minion") or {}
        if isinstance(by_minion, dict):
            for minion, item in by_minion.items():
                minion_id = str(minion)
                if minion_id in seen or not isinstance(item, dict):
                    continue

                emit(minion_result_frame(minion_id, item, fallback_jid=jid))
                seen.add(minion_id)

        if expected_known:
            if len(expected) == 0 or expected_set.issubset(seen):
                break
        elif seen:
            break

        if deadline is not None and time.monotonic() >= deadline:
            break

        time.sleep(poll_interval)

    return {"type": "done", "jid": jid}


def lookup_jid(jid: str) -> Any:
    import salt.config  # Imported lazily so protocol tests do not need Salt installed.
    import salt.runner  # Imported lazily so protocol tests do not need Salt installed.

    opts = salt.config.master_config("/etc/salt/master")
    opts.update({"quiet": True})
    runner = salt.runner.RunnerClient(opts)
    try:
        return runner.cmd("jobs.lookup_jid", [jid])
    except TypeError:
        return runner.cmd("jobs.lookup_jid", [jid], kwarg={})


def job_return_data(raw: Any) -> Any:
    if (
        isinstance(raw, dict)
        and "data" in raw
        and (len(raw) == 1 or "outputter" in raw)
    ):
        return raw.get("data")

    return raw


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
        try:
            value = runner.cmd(function, args)
        except KeyError as exc:
            return {"type": "scalar", "scalar": {"error": str(exc)}}
    except KeyError as exc:
        return {"type": "scalar", "scalar": {"error": str(exc)}}

    return {"type": "scalar", "scalar": value}


def emit(frame: dict[str, Any]) -> None:
    _ = json.dump(frame, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


def minion_frame(minion: str, value: Any) -> dict[str, Any]:
    ret = value
    retcode: Optional[int] = None
    success: Optional[bool] = None
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
        if "retcode" in value:
            retcode = int(value.get("retcode") or 0)
        if "success" in value and isinstance(value.get("success"), bool):
            success = value.get("success")
        jid = str(value.get("jid") or "")
        error = str(value.get("error") or "")

    frame: dict[str, Any] = {
        "type": "return",
        "minion": minion,
        "jid": jid,
        "body": ret,
        "error_message": error,
        "raw": value,
    }
    if retcode is not None:
        frame["retcode"] = retcode
    if success is not None:
        frame["success"] = success

    return frame


def minion_result_frame(
    minion: str, item: dict[str, Any], fallback_jid: str
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "type": "return",
        "minion": minion,
        "jid": str(item.get("jid") or fallback_jid),
        "body": item.get("return"),
        "error_message": str(item.get("error") or ""),
        "raw": item.get("raw"),
    }
    if "retcode" in item:
        frame["retcode"] = int(item.get("retcode") or 0)
    if "success" in item and isinstance(item.get("success"), bool):
        frame["success"] = item.get("success")

    return frame


def normalize_local(raw: Any) -> dict[str, Any]:
    by_minion: dict[str, Any] = {}
    if not isinstance(raw, dict):
        return {"by_minion": by_minion, "raw": raw}

    for minion, value in raw.items():
        ret = value
        retcode: Optional[int] = None
        success: Optional[bool] = None
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
            if "retcode" in value:
                retcode = int(value.get("retcode") or 0)
            if "success" in value and isinstance(value.get("success"), bool):
                success = value.get("success")
            jid = str(value.get("jid") or "")
            error = str(value.get("error") or "")

        item: dict[str, Any] = {
            "jid": jid,
            "return": ret,
            "error": error,
            "raw": value,
        }
        if retcode is not None:
            item["retcode"] = retcode
        if success is not None:
            item["success"] = success

        by_minion[str(minion)] = item

    return {"by_minion": by_minion, "raw": raw}


if __name__ == "__main__":
    raise SystemExit(main())
