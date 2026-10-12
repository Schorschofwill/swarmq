"""``swarmq inspect`` subcommand — registered TaskInfo introspection.

Read-only: imports the user's module, enumerates ``Task`` subclasses,
and prints their declarative ``TaskInfo`` fields. No broker/backend
connection — safe in CI, safe without credentials.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from typing import Any

from swarmq.cli._shared import _render_table
from swarmq.exceptions import ConfigurationError
from swarmq.process_manager.check import app_dir_hint
from swarmq.task import Task


def _format_inspect_table(entries: list[dict[str, Any]]) -> str:
    """Plain-text rendering for 'swarmq inspect'. Shows one row per task."""
    rows = [
        (
            "TASK",
            "QUEUES",
            "DELIVERY",
            "RETRIES",
            "RATE_LIMIT",
            "LOCK",
            "UNIQUE_FOR",
            "UNIQUE_UNTIL",
            "CRON",
        )
    ]
    rows.extend(
        (
            e["name"],
            ",".join(e["queues"]),
            e["delivery"],
            str(e["max_retries"]),
            e["rate_limit"] or "-",
            e["lock"] or "-",
            "-" if e["unique_for"] is None else str(e["unique_for"]),
            e["unique_until"] if e["unique_for"] is not None else "-",
            e["cron"] or "-",
        )
        for e in entries
    )
    return _render_table(rows, "(no tasks registered)")


async def _run_inspect_async(args: argparse.Namespace) -> None:
    """Import *args.module* and print TaskInfo metadata for each task.

    Read-only; no broker / backend connection. Exposes the declarative dedup configuration so agents and operators
    can discover which tasks have unique_for set without reading source.
    """
    try:
        importlib.import_module(args.module)
    except ModuleNotFoundError as exc:
        # B25: derselbe Hinweis wie bei ``swarmq worker`` (cli/worker_cmd.py),
        # nur wenn ``--module`` selbst fehlt, nicht eine seiner Abhaengigkeiten.
        missing = exc.name or ""
        if args.module != missing and not args.module.startswith(missing + "."):
            raise
        raise ModuleNotFoundError(f"{exc}; {app_dir_hint()}", name=exc.name, path=exc.path) from exc
    if not Task._registered_tasks:
        raise ConfigurationError(
            f"no tasks registered after importing {args.module!r}; "
            "ensure your tasks subclass swarmq.Task and the module's "
            "top-level imports execute the class definitions"
        )

    entries: list[dict[str, Any]] = []
    for task_class in Task._registered_tasks:
        ti = task_class.task_info
        entries.append(
            {
                "name": ti.name,
                "class": f"{task_class.__module__}.{task_class.__qualname__}",
                "queues": list(ti.queues),
                "delivery": ti.delivery.value,
                "max_retries": ti.max_retries,
                "retry_backoff": ti.retry_backoff,
                "retry_base_delay": ti.retry_base_delay,
                "soft_timeout": ti.soft_timeout,
                "hard_timeout": ti.hard_timeout,
                "result_ttl": ti.result_ttl,
                "rate_limit": ti.rate_limit,
                "lock": ti.lock,
                "max_concurrency": ti.max_concurrency,
                "cron": ti.cron,
                "unique_for": ti.unique_for,
                "unique_until": ti.unique_until,
            }
        )
    entries.sort(key=lambda e: e["name"])

    if args.format == "json":
        sys.stdout.write(json.dumps(entries, indent=2) + "\n")
    else:
        sys.stdout.write(_format_inspect_table(entries) + "\n")
