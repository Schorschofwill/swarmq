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
#: (stable field-set contract).
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
    """Return a payload dict with every check-contract key present.

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

    Unexpected exceptions are caught too: a :exc:`PermissionError` from ``importlib.util.find_spec``
    or a ``.pth``-related ``OSError`` must not escape as a raw
    traceback or the stdout-JSON contract is violated for agents
    that parse the output.
    """
    from swarmq.process_manager.check import check_worker_config

    payload = _seed_check_payload(args)
    try:
        result = check_worker_config(
            module=args.module,
            reload=payload["reload"],
            watch_dir=getattr(args, "watch_dir", None),
            burst=bool(getattr(args, "burst", False)),
            # Ohne diese Argumente meldete --check "in Ordnung" fuer genau
            # die Kombination, die der Start danach ablehnt — und braeche
            # damit die Zusage, dass ein erfolgreiches Ergebnis den
            # Pre-Flight des Supervisors impliziert (R5).
            processes=int(getattr(args, "processes", 1) or 1),
            autoscale=getattr(args, "autoscale", None),
            metrics_port=getattr(args, "metrics_port", None),
            health_port=getattr(args, "health_port", None),
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

    When ``--check`` is on the command
    line, the caller (agent, CI script) expects JSON on stdout even
    for argparse-shape errors (bad ``--watch-dir`` path, missing
    ``--module``). Looking at the raw argv lets us catch argparse's
    ``SystemExit(2)`` and emit JSON instead of the default unhelpful
    exit.

    The detection uses a *minimal* argparse parser — not the full
    ``_build_parser`` tree — to avoid argparse's type-validators
    aborting on, e.g., ``--watch-dir /nonexistent`` (that abort is
    exactly what AC-06 wants the outer ``main()`` to convert into a
    JSON payload; if the detector aborted first, ``check_mode`` would
    be ``False`` and the JSON path would never engage).

    Plan 2026-04-23-001 CLI-02: the old token-membership heuristic
    mistakenly flagged ``swarmq dlq retry worker --check`` as a check
    invocation (because ``worker`` + ``--check`` are both tokens) and
    likewise ``swarmq worker --task --check`` (where ``--check`` is
    the value of ``--task``, not a flag). The minimal parser knows
    the subcommand names and the argument-consuming worker flags, so
    ``parse_known_args`` produces the correct answer for both cases
    without requiring the heavyweight full parser.
    """
    # Materialise the argv into a list up-front so a caller passing a
    # single-shot iterator (generator, ``iter(...)``) is not exhausted
    # by the detector — the outer ``main()`` still needs to feed the
    # same tokens to the real parser.
    argv_list = list(argv) if argv is not None else sys.argv[1:]

    detector = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    subs = detector.add_subparsers(dest="command")
    worker_sub = subs.add_parser("worker", add_help=False, exit_on_error=False)
    # Mirror the argument-consuming worker flags so their values can't
    # be misread as the ``--check`` token. Only flags that take a
    # string (no type validators) are needed — validators run in the
    # real parser, not here.
    worker_sub.add_argument("--task", default=None)
    worker_sub.add_argument("--module", default=None)
    worker_sub.add_argument("--queue", default=None)
    worker_sub.add_argument("--watch-dir", action="append", default=None)
    worker_sub.add_argument("--log-level", default=None)
    worker_sub.add_argument("--log-format", default=None)
    worker_sub.add_argument("--check", action="store_true", default=False)
    # Siblings — recognised only so ``parse_known_args`` on their
    # argv prefix does not misclassify them as ``worker``.
    for name in ("schedule", "dlq", "inspect"):
        subs.add_parser(name, add_help=False, exit_on_error=False)

    try:
        parsed, _ = detector.parse_known_args(argv_list)
    except (argparse.ArgumentError, SystemExit, ValueError):
        # Detector could not decide — defer to the outer parser.
        # ``False`` means ``main()`` takes the non-check branch; if
        # the user really meant ``--check``, they will get argparse's
        # default error. This is the conservative fallback.
        parsed = None

    if (
        parsed is not None
        and getattr(parsed, "command", None) == "worker"
        and getattr(parsed, "check", False)
    ):
        return True

    # Fallback heuristic for the ``--check=VALUE`` form (CLI-04
    # followup, finding #13). argparse rejects ``=``-bound values on
    # ``store_true`` actions, so ``--check=true`` makes the detector
    # raise and the JSON-on-error contract would silently disengage.
    # Restrict to the ``worker`` subcommand position so ``dlq retry
    # worker --check=...`` (where ``worker`` is a positional task-name)
    # does NOT trip the fallback.
    #
    # NOTE: the *bare* ``--check`` token is intentionally NOT a fallback
    # trigger — ``worker --task --check`` deliberately reads ``--check``
    # as the value of ``--task`` (existing R-2 contract). Finding #14
    # would invert that contract; out of fixer scope.
    if argv_list and argv_list[0] == "worker":
        for tok in argv_list[1:]:
            if tok.startswith("--check="):
                return True
    return False


def _emit_argparse_check_error(message: str) -> int:
    """Render an argparse failure as a ``--check`` JSON payload.

    Keeps the stable field-set (ok, module, module_origin, reload,
    watch_dirs, warnings, error, error_type) so agents consuming
    ``--check`` can parse any failure uniformly.
    """
    payload: dict[str, Any] = {
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
