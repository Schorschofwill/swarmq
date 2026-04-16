"""Command-line interface for SwarmQ.

This module is the target of the ``swarmq`` console script declared in
``pyproject.toml``. It deliberately exposes a single subcommand
(``worker``) — the smallest surface that lets a stranger run SwarmQ
without writing a Python entrypoint. Future subcommands (``dlq``,
``schedule``, ``tasks``) live in their own plans; this file is the
extension point but does not pre-build for them.

Configuration precedence: explicit CLI flag > environment variable
(``SWARMQ_*``) > built-in default in :class:`SwarmQConfig`.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.metadata
import os
import sys
import traceback
from typing import Sequence

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ConfigurationError,
)
from swarmq.task import Task

_VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
_VALID_LOG_FORMATS = ("human", "json")


def _positive_int(raw: str) -> int:
    """argparse type-function — rejects values < 1 with a clear message."""
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an integer: {raw!r}") from exc
    if value < 1:
        raise argparse.ArgumentTypeError(
            f"must be >= 1 (got {value})"
        )
    return value


def _queue_list(raw: str) -> list[str]:
    """Comma-separated queue names with whitespace stripped.

    Mirrors :class:`SwarmQConfig`'s ``SWARMQ_QUEUES`` env-var parsing:
    splits on commas, strips whitespace per item, drops empties. An
    all-empty result is rejected with a clear message rather than
    silently giving the worker no queues to consume.
    """
    queues = [q.strip() for q in raw.split(",") if q.strip()]
    if not queues:
        raise argparse.ArgumentTypeError(
            f"--queues must list at least one non-empty name (got {raw!r})"
        )
    return queues


def _swarmq_version() -> str:
    """Best-effort version string for ``swarmq --version``.

    Tries the installed package metadata first (works for both editable
    and built installs); falls back to ``"unknown"`` rather than
    crashing the CLI when version discovery fails.
    """
    try:
        return importlib.metadata.version("swarmq")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="swarmq",
        description=(
            "SwarmQ — Python task queue on RabbitMQ + Valkey. "
            "Run a worker against tasks defined in a Python module."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"swarmq {_swarmq_version()}",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        metavar="COMMAND",
        required=True,
    )

    worker = subparsers.add_parser(
        "worker",
        help="Start a worker that consumes tasks from one or more queues.",
        description=(
            "Import a Python module so its Task subclasses register, "
            "then connect to broker + backend and consume. "
            "Broker and backend URLs come from SWARMQ_BROKER_URL and "
            "SWARMQ_BACKEND_URL environment variables. "
            "Note: importing the module runs its top-level code — "
            "this is the standard Python convention."
        ),
    )
    worker.add_argument(
        "--module",
        required=True,
        metavar="DOTTED.PATH",
        help=(
            "Python module containing Task subclasses to register, "
            "e.g. 'myapp.tasks'. Must be importable from the current "
            "Python path."
        ),
    )
    worker.add_argument(
        "--queues",
        type=_queue_list,
        default=["default"],
        metavar="A,B,C",
        help="Comma-separated queue names to consume from (default: default).",
    )
    worker.add_argument(
        "--concurrency",
        type=_positive_int,
        default=10,
        metavar="N",
        help="Maximum tasks executed in parallel (default: 10).",
    )
    worker.add_argument(
        "--log-level",
        choices=_VALID_LOG_LEVELS,
        default="INFO",
        help="Log verbosity (default: INFO). DEBUG also surfaces full tracebacks on CLI errors.",
    )
    worker.add_argument(
        "--log-format",
        choices=_VALID_LOG_FORMATS,
        default="human",
        help="Log output format (default: human). 'json' for agent/log-aggregator consumption.",
    )

    return parser


async def _run_worker_async(args: argparse.Namespace) -> None:
    """Import the user's module, build SwarmQ, run start_worker.

    Errors propagate to the synchronous ``main()`` for uniform
    rendering. The only thing this does itself is the ``no tasks
    registered`` check, which surfaces a config error rather than
    starting an idle worker that will silently never execute anything.
    """
    importlib.import_module(args.module)
    if not Task._registered_tasks:
        raise ConfigurationError(
            f"no tasks registered after importing {args.module!r}; "
            "ensure your tasks subclass swarmq.Task and the module's "
            "top-level imports execute the class definitions"
        )

    # Pre-check the env vars before SwarmQConfig so we can give a CLI-aware
    # error message that names the variable, instead of SwarmQConfig's
    # generic 'broker_url must not be empty'.
    broker_url = os.environ.get("SWARMQ_BROKER_URL", "")
    backend_url = os.environ.get("SWARMQ_BACKEND_URL", "")
    if not broker_url:
        raise ConfigurationError(
            "SWARMQ_BROKER_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BROKER_URL=amqp://guest:guest@localhost:5672/"
        )
    if not backend_url:
        raise ConfigurationError(
            "SWARMQ_BACKEND_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0"
        )

    broker = RabbitMQBroker(broker_url)
    backend = ValkeyBackend(backend_url)
    app = SwarmQ(
        broker,
        backend,
        queues=args.queues,
        concurrency=args.concurrency,
        log_level=args.log_level,
        log_json=(args.log_format == "json"),
    )
    async with app:
        await app.start_worker()


def main(argv: Sequence[str] | None = None) -> int:
    """Console-script entry point. Returns the process exit code.

    Returns 0 on graceful shutdown, 2 on bad arguments (argparse default),
    1 on any other startup or runtime error. Errors render as a single
    stderr line at INFO+ log levels; ``--log-level DEBUG`` adds the
    full traceback so users can debug their own task modules.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command != "worker":
        # argparse should have caught this via required=True, but guard.
        parser.error(f"unknown command: {args.command!r}")

    try:
        asyncio.run(_run_worker_async(args))
    except KeyboardInterrupt:
        # Ctrl-C before the worker installed its own SIGINT handler;
        # treat as graceful exit, not a crash.
        return 0
    except (
        ModuleNotFoundError,
        ConfigurationError,
        BrokerConnectionError,
        BackendConnectionError,
    ) as exc:
        sys.stderr.write(f"swarmq worker: {exc}\n")
        if args.log_level == "DEBUG":
            traceback.print_exc(file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover — exercised via console_script
    raise SystemExit(main())
