"""Command-line interface for SwarmQ.

This package is the target of the ``swarmq`` console script declared
in ``pyproject.toml`` (entry: ``swarmq = "swarmq.cli:main"``).

Package layout:

- :mod:`swarmq.cli._shared` — argparse type-functions, connection
  factories, generic table renderer.
- :mod:`swarmq.cli.parser` — :func:`_build_parser` + ``--version``
  helper. Holds the full argparse tree.
- :mod:`swarmq.cli.worker_cmd` — :func:`run_worker_async`. Also
  imported by :mod:`swarmq.process_manager` via
  ``from swarmq.cli import run_worker_async``.
- :mod:`swarmq.cli.schedule_cmd`, :mod:`swarmq.cli.dlq_cmd`,
  :mod:`swarmq.cli.inspect_cmd` — one module per command family.
- :mod:`swarmq.cli.check_cmd` — ``swarmq worker --check`` JSON-
  contract validator. Separate module to keep the contract perimeter
  visible.

``main()`` itself lives in THIS module, not in a submodule. The
public-API contract (tests patch ``swarmq.cli.run_worker_async`` via
string paths; see ``tests/unit/test_cli_module_surface.py``) requires
that ``main()`` resolve symbols via the ``swarmq.cli`` namespace, not
via a private ``from … import …`` in a submodule. Extracting ``main``
into ``cli/main.py`` would bind ``run_worker_async`` in that
submodule's globals and ``patch('swarmq.cli.run_worker_async')`` would
silently stop intercepting the real call.

Configuration precedence: explicit CLI flag > environment variable
(``SWARMQ_*``) > built-in default in :class:`SwarmQConfig`.
"""

from __future__ import annotations

# argparse is part of the frozen surface (tests construct
# argparse.Namespace directly and dispatch through main()).
import argparse  # noqa: F401 — frozen surface: used by tests via Namespace

# tests/unit/test_cli_multiprocess.py:144 does
# patch("swarmq.cli.asyncio.run", ...). Removing this import breaks
# that test. Do NOT let ruff autoremove, isort "cleanup", or a future
# contributor trim it. Since ``asyncio`` is a shared singleton module
# object, patching via any dotted path resolving to it works — but the
# dotted-path lookup still needs this name bound at the facade level.
import asyncio
import importlib
import json  # noqa: F401
import os  # noqa: F401
import re  # noqa: F401
import sys
import time  # noqa: F401
import traceback
from collections.abc import Sequence
from datetime import UTC, datetime  # noqa: F401
from pathlib import Path  # noqa: F401
from typing import Any  # noqa: F401

import msgspec  # noqa: F401

# Submodule imports — the symbols they define become swarmq.cli.* attributes.
from swarmq.app import SwarmQ  # noqa: F401
from swarmq.cli._shared import (
    _DOTTED_IDENTIFIER_RE,
    _FORBIDDEN_WATCH_PREFIXES,
    _VALID_LOG_FORMATS,
    _VALID_LOG_LEVELS,
    _autoscale_range,
    _backend_from_env,
    _broker_from_env,
    _dotted_module_name,
    _positive_float,
    _positive_int,
    _queue_list,
    _render_table,
    _watch_dir_path,
)
from swarmq.cli.check_cmd import (
    _WORKER_CHECK_PAYLOAD_KEYS,
    _emit_argparse_check_error,
    _is_check_invocation,
    _run_worker_check,
    _seed_check_payload,
)
from swarmq.cli.dlq_cmd import (
    _DLQ_NOT_FOUND_EXIT_CODE,
    _DLQNotFound,
    _format_dlq_list_table,
    _normalise_headers_for_json,
    _run_dlq_async,
    _run_dlq_inspect,
    _run_dlq_list,
    _run_dlq_purge,
    _run_dlq_retry,
)
from swarmq.cli.inspect_cmd import _format_inspect_table, _run_inspect_async
from swarmq.cli.parser import _build_parser, _swarmq_version
from swarmq.cli.schedule_cmd import (
    _format_schedule_table,
    _run_schedule_add,
    _run_schedule_async,
    _run_schedule_list,
    _run_schedule_remove,
    _run_schedule_status,
)
from swarmq.cli.worker_cmd import run_worker_async
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ConfigurationError,
)
from swarmq.scheduler import (  # noqa: F401
    SCHEDULER_LOCK_KEY,
    ScheduleDefinition,
    ScheduleStore,
)
from swarmq.serialization import compute_dedup_hash  # noqa: F401
from swarmq.task import Task  # noqa: F401

# ---------------------------------------------------------------------------
# Lazy surface — heavy re-exports that must not trigger at import time.
#
# ``ValkeyBackend`` (~88 ms) and ``RabbitMQBroker`` (~21 ms) used to be
# re-exported as ambient top-level attributes. They are part of the
# frozen surface whitelist (``test_cli_module_surface.py``) for identity
# continuity, but loading them on every ``swarmq --help`` is wasteful.
#
# PEP-562 ``__getattr__`` resolves them on first access; the canonical
# class object is returned, so the identity check
# ``swarmq.cli.ValkeyBackend is swarmq.backend.valkey.ValkeyBackend``
# stays true. See plan 2026-04-23-001 Unit 1 (R-1a).
# ---------------------------------------------------------------------------

_LAZY_SURFACE: dict[str, tuple[str, str]] = {
    "ValkeyBackend": ("swarmq.backend.valkey", "ValkeyBackend"),
    "RabbitMQBroker": ("swarmq.broker.rabbitmq", "RabbitMQBroker"),
}


def __getattr__(name: str) -> object:
    """PEP-562 module attribute hook for lazy-loaded surface symbols."""
    target = _LAZY_SURFACE.get(name)
    if target is not None:
        module_name, attr_name = target
        module = importlib.import_module(module_name)
        return getattr(module, attr_name)
    raise AttributeError(f"module 'swarmq.cli' has no attribute {name!r}")


def __dir__() -> list[str]:
    """PEP-562 ``dir()`` hook — advertise lazy-surface names.

    Without this override, ``dir(swarmq.cli)`` would not include
    ``ValkeyBackend`` / ``RabbitMQBroker`` (they live only in
    ``_LAZY_SURFACE``, not in ``globals()``). The surface-whitelist
    equality check in ``test_cli_module_surface.py`` relies on
    ``dir()``; keeping the lazy names visible preserves R-8 without
    re-eagerifying the imports.
    """
    return [*globals(), *_LAZY_SURFACE]

# ``__all__`` controls two contracts at once:
#
# 1. ruff ``F401`` — each ``from X import Y`` above is treated as a
#    deliberate re-export (no "unused import" warning).
# 2. mypy strict-mode attribute resolution — downstream imports like
#    ``from swarmq.cli import run_worker_async`` in
#    ``swarmq/process_manager.py`` need the symbol marked as an
#    intentional export here.
#
# Ambient-imported names (``Path``, ``Sequence``, ``argparse`` etc.)
# are intentionally omitted — they remain reachable via ``dir()`` and
# ``getattr()`` for the surface audit, but they are not part of the
# deliberate public contract. Keep this list aligned with
# ``tests/unit/test_cli_module_surface.py::_CLI_PUBLIC_SURFACE``
# minus the ambient tier.
__all__ = (
    "_DLQ_NOT_FOUND_EXIT_CODE",
    "_DOTTED_IDENTIFIER_RE",
    "_FORBIDDEN_WATCH_PREFIXES",
    "_VALID_LOG_FORMATS",
    "_VALID_LOG_LEVELS",
    "_WORKER_CHECK_PAYLOAD_KEYS",
    "_DLQNotFound",
    "_autoscale_range",
    "_backend_from_env",
    "_broker_from_env",
    "_build_parser",
    "_dotted_module_name",
    "_emit_argparse_check_error",
    "_format_dlq_list_table",
    "_format_inspect_table",
    "_format_schedule_table",
    "_is_check_invocation",
    "_normalise_headers_for_json",
    "_positive_float",
    "_positive_int",
    "_queue_list",
    "_render_table",
    "_run_dlq_async",
    "_run_dlq_inspect",
    "_run_dlq_list",
    "_run_dlq_purge",
    "_run_dlq_retry",
    "_run_inspect_async",
    "_run_schedule_add",
    "_run_schedule_async",
    "_run_schedule_list",
    "_run_schedule_remove",
    "_run_schedule_status",
    "_run_worker_check",
    "_seed_check_payload",
    "_swarmq_version",
    "_watch_dir_path",
    "main",
    "run_worker_async",
)


def main(argv: Sequence[str] | None = None) -> int:
    """Console-script entry point. Returns the process exit code.

    Returns 0 on graceful shutdown, 2 on bad arguments (argparse default),
    1 on any other startup or runtime error. Errors render as a single
    stderr line at INFO+ log levels; ``--log-level DEBUG`` adds the
    full traceback so users can debug their own task modules.

    Exception: when ``swarmq worker --check`` is the invocation, an
    argparse failure is rendered as a JSON payload on stdout and
    returns ``1`` (matching the documented ``--check`` exit codes)
    rather than letting argparse's ``SystemExit(2)`` escape. This
    keeps the stdout-JSON contract of ``--check`` stable for every
    failure mode.
    """
    parser = _build_parser()
    check_mode = _is_check_invocation(argv)
    if check_mode:
        # Redirect argparse's stderr-rendered error into our JSON
        # payload so a machine-readable caller gets a single,
        # parseable output stream.
        import io

        argparse_err = io.StringIO()
        try:
            import contextlib as _ctxlib

            with _ctxlib.redirect_stderr(argparse_err):
                args = parser.parse_args(argv)
        except SystemExit as exc:
            # argparse exits on parse error (code 2) or on --help/--version
            # (code 0). Only wrap the error path.
            if exc.code == 0:
                raise
            return _emit_argparse_check_error(argparse_err.getvalue())
    else:
        args = parser.parse_args(argv)

    # Plan 2026-04-23-001 CLI-04 / Unit 5: resolve the TTY-aware
    # ``--format`` default (+ SWARMQ_DEFAULT_FORMAT escape) centrally
    # so each subcommand handler reads a concrete value. Absent
    # ``--format`` flags stay untouched (``hasattr`` guard).
    if hasattr(args, "format"):
        from swarmq.cli._shared import resolve_format
        args.format = resolve_format(args.format)

    if args.command == "worker":
        # R5.3 ce-review #23: dry-run validation. Never starts the
        # supervisor or touches the broker — just checks whether the
        # invocation would get past pre-flight.
        if getattr(args, "check", False):
            return _run_worker_check(args)

        # R4.6/R4.7: when the user opted into multi-process mode,
        # dispatch to the synchronous supervisor. The supervisor
        # itself runs no asyncio loop — subprocesses do.
        # R5.3 ce-review #1: --reload requires the ProcessManager
        # supervisor — single-process path has no SIGHUP handler and
        # no execvpe pipeline. Activating multi-process implicitly
        # keeps the default invocation (`swarmq worker --reload`)
        # working.
        use_multi = (
            getattr(args, "processes", 1) > 1
            or getattr(args, "autoscale", None) is not None
            or getattr(args, "reload", False)
        )
        if use_multi:
            from swarmq.process_manager import run_multi_process_worker
            try:
                return run_multi_process_worker(args)
            except (
                ConfigurationError,
                BrokerConnectionError,
                BackendConnectionError,
            ) as exc:
                sys.stderr.write(f"swarmq worker: {exc}\n")
                if getattr(args, "log_level", "INFO") == "DEBUG":
                    traceback.print_exc(file=sys.stderr)
                return 1
        runner = run_worker_async(args)
        error_prefix = "swarmq worker"
    elif args.command == "schedule":
        runner = _run_schedule_async(args)
        error_prefix = f"swarmq schedule {args.schedule_command}"
    elif args.command == "dlq":
        runner = _run_dlq_async(args)
        error_prefix = f"swarmq dlq {args.dlq_command}"
    elif args.command == "inspect":
        runner = _run_inspect_async(args)
        error_prefix = "swarmq inspect"
    else:  # pragma: no cover — argparse required=True guards this
        parser.error(f"unknown command: {args.command!r}")

    try:
        asyncio.run(runner)
    except KeyboardInterrupt:
        # Ctrl-C before the worker installed its own SIGINT handler;
        # treat as graceful exit, not a crash.
        return 0
    except _DLQNotFound:
        # dlq inspect / retry <task_id> found no match — distinct exit
        # code so agent scripts can branch without parsing JSON
        # (R3.10 P3 #19). User-facing line was already written by the
        # handler; we emit no additional noise here.
        return _DLQ_NOT_FOUND_EXIT_CODE
    except (
        ModuleNotFoundError,
        ConfigurationError,
        BrokerConnectionError,
        BackendConnectionError,
    ) as exc:
        sys.stderr.write(f"{error_prefix}: {exc}\n")
        if getattr(args, "log_level", "INFO") == "DEBUG":
            traceback.print_exc(file=sys.stderr)
        return 1
    return 0
