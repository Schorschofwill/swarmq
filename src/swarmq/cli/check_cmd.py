"""``swarmq worker --check`` dry-run validator.

Has its own module to make the JSON-contract perimeter visible: every
payload that leaves this file includes the fields listed in
:data:`_WORKER_CHECK_PAYLOAD_KEYS`, regardless of which validation
step decides the outcome. Agents and CI scripts consume this surface.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any

from swarmq.exceptions import ConfigurationError

#: Keys that ``--check`` always emits on its JSON payload, regardless of
#: which validation step fails. Callers may rely on ``dict.get(key)`` +
#: None for uniform handling across success + failure paths
#: (ce-review #1 / P1: stable field-set contract).
_WORKER_CHECK_PAYLOAD_KEYS = (
    "ok",
    "module",
    "module_origin",
    "reload",
    "watch_dirs",
    "warnings",
    "error",
    "error_type",
)


def _seed_check_payload(args: argparse.Namespace) -> dict[str, Any]:
    """Return a payload dict with every ce-review #1 key present.

    Fields are set to ``None`` until the validator fills them in;
    the JSON contract stays shape-stable regardless of where the
    validation terminates.
    """
    return {
        "ok": False,
        "module": getattr(args, "module", None),
        "module_origin": None,
        "reload": bool(getattr(args, "reload", False)),
        "watch_dirs": None,
        "warnings": [],
        "error": None,
        "error_type": None,
    }


def _run_worker_check(args: argparse.Namespace) -> int:
    """``swarmq worker --check`` dry-run entry-point.

    Delegates the validation to
    :func:`swarmq.process_manager.check_worker_config` and renders the
    result as a one-line JSON payload on stdout. The payload shape
    is stable across success and failure — every key defined in
    :data:`_WORKER_CHECK_PAYLOAD_KEYS` is always present (``None``
    when unused).

    Exit codes:

    - ``0`` — all validations passed.
    - ``1`` — any validation error (module unresolvable, missing
      ``watchfiles`` dep, invalid watch-dir, unexpected exception).

    Unexpected exceptions are caught too (R5.3 ce-review #3 /
    P1): a :exc:`PermissionError` from ``importlib.util.find_spec``
    or a ``.pth``-related ``OSError`` must not escape as a raw
    traceback or the stdout-JSON contract is violated for agents
    that parse the output.
    """
    from swarmq.process_manager import check_worker_config

    payload = _seed_check_payload(args)
    try:
        result = check_worker_config(
            module=args.module,
            reload=payload["reload"],
            watch_dir=getattr(args, "watch_dir", None),
            burst=bool(getattr(args, "burst", False)),
        )
        payload.update(
            ok=True,
            module_origin=result["module_origin"],
            watch_dirs=result["watch_dirs"],
            warnings=result["warnings"],
        )
        sys.stdout.write(json.dumps(payload) + "\n")
        return 0
    except ConfigurationError as exc:
        payload["error"] = str(exc)
        payload["error_type"] = "ConfigurationError"
        sys.stdout.write(json.dumps(payload) + "\n")
        return 1
    except Exception as exc:
        payload["error"] = f"{type(exc).__name__}: {exc}"
        payload["error_type"] = type(exc).__name__
        sys.stdout.write(json.dumps(payload) + "\n")
        return 1


def _is_check_invocation(argv: Sequence[str] | None) -> bool:
    """Detect ``swarmq worker --check`` from the raw argv.

    ce-review pass 2 #2 / AC-06: when ``--check`` is on the command
    line, the caller (agent, CI script) expects JSON on stdout even
    for argparse-shape errors (bad ``--watch-dir`` path, missing
    ``--module``). Looking at the raw argv lets us catch argparse's
    ``SystemExit(2)`` and emit JSON instead of the default unhelpful
    exit.

    Returns True when the argv has both ``worker`` and ``--check`` as
    separate tokens. Does not parse exhaustively — a false positive
    (``--check`` mentioned in a quoted string) would at worst emit
    JSON twice; the impact is bounded.
    """
    if argv is None:
        argv = sys.argv[1:]
    tokens = list(argv)
    return "worker" in tokens and "--check" in tokens


def _emit_argparse_check_error(message: str) -> int:
    """Render an argparse failure as a ``--check`` JSON payload.

    Keeps the stable field-set (ok, module, module_origin, reload,
    watch_dirs, warnings, error, error_type) so agents consuming
    ``--check`` can parse any failure uniformly.
    """
    payload = {
        "ok": False,
        "module": None,
        "module_origin": None,
        "reload": None,
        "watch_dirs": None,
        "warnings": [],
        "error": message.strip() or "argparse rejected the invocation",
        "error_type": "ArgparseError",
    }
    sys.stdout.write(json.dumps(payload) + "\n")
    return 1
