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
import json
import os
import sys
import time
import traceback
from datetime import UTC, datetime
from typing import Any, Sequence

import msgspec

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ConfigurationError,
)
from swarmq.scheduler import SCHEDULER_LOCK_KEY, ScheduleDefinition, ScheduleStore
from swarmq.serialization import compute_dedup_hash
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

    # ---- schedule subcommand group (Phase 3c Unit 12) --------------------
    schedule = subparsers.add_parser(
        "schedule",
        help="Inspect or manage schedules without restarting workers.",
        description=(
            "Manage the embedded scheduler's schedule definitions in "
            "Valkey. Requires SWARMQ_BACKEND_URL (broker is not used "
            "by these subcommands). The 'prune' subcommand is not in "
            "v1 — use 'remove --permanent' instead."
        ),
    )
    schedule_sub = schedule.add_subparsers(
        dest="schedule_command",
        metavar="SUBCOMMAND",
        required=True,
    )

    # list
    sched_list = schedule_sub.add_parser(
        "list",
        help="List all schedules in sq:sched:defs.",
    )
    sched_list.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="Output format (default: table).",
    )

    # add
    sched_add = schedule_sub.add_parser(
        "add",
        help="Create a new schedule (insert-if-absent).",
    )
    sched_add.add_argument("name", help="Schedule name (unique key).")
    group = sched_add.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--cron",
        metavar="EXPR",
        help="5-field cron expression, e.g. '*/5 * * * *'.",
    )
    group.add_argument(
        "--at",
        metavar="ISO_DATETIME",
        help="One-shot fire time, ISO 8601 (e.g. '2026-05-01T03:00:00Z').",
    )
    sched_add.add_argument(
        "--task",
        required=True,
        metavar="NAME",
        help="Registered task name to invoke.",
    )
    sched_add.add_argument(
        "--args",
        default="[]",
        metavar="JSON_ARRAY",
        help="Positional args as JSON array (default: []).",
    )
    sched_add.add_argument(
        "--kwargs",
        default="{}",
        metavar="JSON_OBJECT",
        help="Keyword args as JSON object (default: {}).",
    )
    sched_add.add_argument(
        "--queue",
        default="default",
        help="Target queue (default: default).",
    )
    sched_add.add_argument(
        "--unique-for",
        type=_positive_int,
        default=None,
        metavar="SECONDS",
        help=(
            "R3.5 dedup TTL in seconds. When set, the CLI claims the "
            "dedup flag at enqueue time (source=cli_unique). A second "
            "'schedule add --unique-for=...' with the same task + args "
            "+ kwargs within the TTL window is rejected as a duplicate."
        ),
    )
    sched_add.add_argument(
        "--unique-until",
        choices=("start", "completion"),
        default="completion",
        help=(
            "R3.5 dedup release mode (default: completion). 'start' "
            "releases the flag at worker pickup so a re-schedule can "
            "enqueue before the task finishes; 'completion' keeps the "
            "flag until the task reaches a terminal state. Ignored "
            "when --unique-for is not set."
        ),
    )
    sched_add.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help=(
            "Output format (default: table). Use 'json' for agent-"
            "parseable output with an explicit 'inserted' boolean."
        ),
    )

    # remove
    sched_remove = schedule_sub.add_parser(
        "remove",
        help="Disable or permanently delete a schedule.",
    )
    sched_remove.add_argument("name", help="Schedule name to remove.")
    sched_remove.add_argument(
        "--permanent",
        action="store_true",
        help=(
            "Hard-delete (ZREM + HDEL). Without this flag the schedule "
            "is only disabled — useful when you want insert-if-absent "
            "to keep blocking re-syncs from code."
        ),
    )
    sched_remove.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help=(
            "Output format (default: table). Use 'json' for agent-"
            "parseable output with an explicit 'removed' boolean."
        ),
    )

    # inspect — introspect registered TaskInfo (including R3.5 dedup)
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="List registered tasks and their TaskInfo metadata.",
        description=(
            "Import a module, register its Task subclasses, and print "
            "their declarative TaskInfo fields — queues, retry config, "
            "rate_limit, lock, R3.5 unique_for / unique_until. Read-only; "
            "does not connect to broker or backend."
        ),
    )
    inspect_parser.add_argument(
        "--module",
        required=True,
        metavar="DOTTED.PATH",
        help="Python module whose Task subclasses to introspect.",
    )
    inspect_parser.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="Output format (default: table).",
    )

    # status
    sched_status = schedule_sub.add_parser(
        "status",
        help="Show current leader + next-N due schedules.",
    )
    sched_status.add_argument(
        "--limit",
        type=_positive_int,
        default=10,
        help="How many upcoming schedules to list (default: 10).",
    )
    sched_status.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="Output format (default: table).",
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


async def _backend_from_env() -> ValkeyBackend:
    """Construct + connect a ValkeyBackend from SWARMQ_BACKEND_URL.

    Shared helper for the schedule subcommands which only need the
    backend (no broker). Raises :class:`ConfigurationError` when the
    env var is missing so the CLI can surface a helpful message.
    """
    backend_url = os.environ.get("SWARMQ_BACKEND_URL", "")
    if not backend_url:
        raise ConfigurationError(
            "SWARMQ_BACKEND_URL is not set; export it or pass it via env, "
            "e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0"
        )
    backend = ValkeyBackend(backend_url)
    await backend.connect()
    return backend


def _format_schedule_table(schedules: list[ScheduleDefinition]) -> str:
    """Plain-text table for terminal output — stable column ordering.

    Columns include R3.5 dedup state (DEDUP + UNIQUE_UNTIL) so operators
    can tell client-originated deferred-dedup entries from plain CLI or
    static-cron ones at a glance (ce-review P3 #23).
    """
    if not schedules:
        return "(no schedules)"
    rows = [(
        "NAME", "CRON", "TASK", "QUEUE", "NEXT_FIRE",
        "DISABLED", "SOURCE", "DEDUP", "UNIQUE_UNTIL",
    )]
    for s in schedules:
        next_fire = (
            datetime.fromtimestamp(s.fire_ts_ms / 1000, tz=UTC).isoformat()
            if s.fire_ts_ms is not None
            else "-"
        )
        dedup_state = f"{s.dedup_hash[:8]}…" if s.dedup_hash else "-"
        unique_until = s.unique_until if s.dedup_hash else "-"
        rows.append((
            s.schedule_name,
            s.cron or "-",
            s.task_name,
            s.queue,
            next_fire,
            "yes" if s.disabled else "no",
            s.source,
            dedup_state,
            unique_until,
        ))
    widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    lines = [
        "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
        for row in rows
    ]
    return "\n".join(lines)


async def _run_schedule_list(args: argparse.Namespace) -> None:
    backend = await _backend_from_env()
    try:
        store = ScheduleStore(backend)
        schedules = await store.list_schedules()
        schedules.sort(key=lambda s: s.schedule_name)
        if args.format == "json":
            payload = [msgspec.to_builtins(s) for s in schedules]
            sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        else:
            sys.stdout.write(_format_schedule_table(schedules) + "\n")
    finally:
        await backend.close()


async def _run_schedule_add(args: argparse.Namespace) -> None:
    try:
        parsed_args = json.loads(args.args)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"--args is not valid JSON: {exc}") from exc
    if not isinstance(parsed_args, list):
        raise ConfigurationError("--args must be a JSON array")

    try:
        parsed_kwargs = json.loads(args.kwargs)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"--kwargs is not valid JSON: {exc}") from exc
    if not isinstance(parsed_kwargs, dict):
        raise ConfigurationError("--kwargs must be a JSON object")

    if args.cron is not None:
        cron = args.cron
        # Compute first fire via croniter so the zset has a real score.
        from croniter import CroniterBadCronError, croniter
        try:
            next_dt = croniter(cron, datetime.now(UTC)).get_next(datetime)
        except (CroniterBadCronError, ValueError) as exc:
            raise ConfigurationError(
                f"invalid cron expression: {cron!r} ({exc})"
            ) from exc
        fire_ts_ms = int(next_dt.timestamp() * 1000)
    else:
        # --at <iso-datetime>
        try:
            dt = datetime.fromisoformat(args.at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ConfigurationError(
                f"invalid --at datetime: {args.at!r} ({exc})"
            ) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        cron = None
        fire_ts_ms = int(dt.timestamp() * 1000)

    # R3.5 ce-review P3 #23: --unique-for claims the dedup flag before
    # the ScheduleStore insert, matching Client._schedule_deferred
    # semantics. On duplicate, the insert is skipped and the existing
    # claim's owner is reported so operators can correlate.
    unique_for: int | None = getattr(args, "unique_for", None)
    unique_until: str = getattr(args, "unique_until", "completion")

    backend = await _backend_from_env()
    dedup_hash: str | None = None
    existing_owner: str | None = None
    try:
        if unique_for is not None:
            dedup_hash = compute_dedup_hash(
                args.task, tuple(parsed_args), parsed_kwargs,
            )
            # TTL = unique_for + fire_offset, ceiling-div so sub-second
            # deltas still get a full second of padding (same formula
            # as Client._schedule_deferred).
            now_ms = int(time.time() * 1000)
            fire_offset_ms = max(0, fire_ts_ms - now_ms)
            fire_offset_s = (fire_offset_ms + 999) // 1000
            ttl_ms = (unique_for + int(fire_offset_s)) * 1000
            existing_owner = await backend.setnx_unique(
                dedup_hash, args.name, ttl_ms,
            )

        definition = ScheduleDefinition(
            schedule_name=args.name,
            task_name=args.task,
            args=tuple(parsed_args),
            kwargs=parsed_kwargs,
            queue=args.queue,
            cron=cron,
            fire_ts_ms=fire_ts_ms,
            source="cli_unique" if dedup_hash is not None else "cli",
            dedup_hash=dedup_hash,
            unique_until=unique_until,
        )

        if existing_owner is not None:
            # Dup detected — don't insert; surface the clash to the
            # operator. The flag already belongs to another schedule.
            inserted = False
        else:
            store = ScheduleStore(backend)
            try:
                inserted = await store.add_schedule(definition)
            except BaseException:
                # Rollback the dedup claim on store failure (P1 #1).
                if dedup_hash is not None:
                    try:
                        await backend.release_unique(dedup_hash, args.name)
                    except Exception:
                        pass
                raise

        if getattr(args, "format", "table") == "json":
            payload = {
                "inserted": inserted,
                "name": args.name,
                "already_exists": not inserted,
            }
            if unique_for is not None:
                payload["dedup_hash"] = dedup_hash
                payload["unique_until"] = unique_until
                if existing_owner is not None:
                    payload["duplicate_of"] = existing_owner
            sys.stdout.write(json.dumps(payload) + "\n")
        elif inserted:
            sys.stdout.write(f"added schedule {args.name!r}\n")
        elif existing_owner is not None:
            sys.stdout.write(
                f"schedule {args.name!r} rejected — dedup flag already "
                f"held by {existing_owner!r} (args duplicate within "
                f"{unique_for}s TTL)\n"
            )
        else:
            sys.stdout.write(
                f"schedule {args.name!r} already exists; use "
                f"'schedule remove --permanent' first to replace it\n"
            )
    finally:
        await backend.close()


async def _run_schedule_remove(args: argparse.Namespace) -> None:
    backend = await _backend_from_env()
    try:
        store = ScheduleStore(backend)
        if args.permanent:
            changed = await store.remove_schedule(args.name)
            action = "removed"
        else:
            changed = await store.disable_schedule(args.name)
            action = "disabled"

        if getattr(args, "format", "table") == "json":
            sys.stdout.write(json.dumps({
                "removed": changed,
                "permanent": bool(args.permanent),
                "name": args.name,
            }) + "\n")
            return

        if changed:
            verb = "permanently removed" if args.permanent else "disabled"
            sys.stdout.write(f"{verb} schedule {args.name!r}\n")
        else:
            sys.stdout.write(
                f"schedule {args.name!r} not found — {action} was a no-op\n"
            )
    finally:
        await backend.close()


async def _run_schedule_status(args: argparse.Namespace) -> None:
    backend = await _backend_from_env()
    try:
        # Read the leader lock directly.
        owner_raw = await backend._client.get(SCHEDULER_LOCK_KEY)
        leader = owner_raw.decode() if owner_raw else None

        store = ScheduleStore(backend)
        now_ms = int(time.time() * 1000)
        # Look ahead 24h for the "next N" view.
        horizon_ms = now_ms + 24 * 3600 * 1000
        upcoming = await store.poll_due(horizon_ms, limit=args.limit)
        upcoming.sort(key=lambda t: t[1])  # sort by score

        if args.format == "json":
            payload = {
                "leader": leader,
                "now_ms": now_ms,
                "upcoming": [
                    {
                        "name": name,
                        "fire_ts_ms": score,
                        "fire_at": datetime.fromtimestamp(
                            score / 1000, tz=UTC
                        ).isoformat(),
                        "task": defn.task_name,
                        "queue": defn.queue,
                        "disabled": defn.disabled,
                    }
                    for name, score, defn in upcoming
                ],
            }
            sys.stdout.write(json.dumps(payload, indent=2) + "\n")
            return

        # Table format
        sys.stdout.write(f"LEADER: {leader or '(none)'}\n")
        sys.stdout.write(f"NOW:    {datetime.fromtimestamp(now_ms/1000, tz=UTC).isoformat()}\n")
        sys.stdout.write("UPCOMING:\n")
        if not upcoming:
            sys.stdout.write("  (none in next 24h)\n")
        else:
            for name, score, defn in upcoming:
                fire_at = datetime.fromtimestamp(score / 1000, tz=UTC).isoformat()
                disabled = " [disabled]" if defn.disabled else ""
                sys.stdout.write(
                    f"  {fire_at}  {name}  task={defn.task_name}"
                    f"  queue={defn.queue}{disabled}\n"
                )
    finally:
        await backend.close()


def _format_inspect_table(entries: list[dict[str, Any]]) -> str:
    """Plain-text rendering for 'swarmq inspect'. Shows one row per task."""
    if not entries:
        return "(no tasks registered)"
    rows = [(
        "TASK", "QUEUES", "DELIVERY", "RETRIES",
        "RATE_LIMIT", "LOCK", "UNIQUE_FOR", "UNIQUE_UNTIL", "CRON",
    )]
    for e in entries:
        rows.append((
            e["name"],
            ",".join(e["queues"]),
            e["delivery"],
            str(e["max_retries"]),
            e["rate_limit"] or "-",
            e["lock"] or "-",
            "-" if e["unique_for"] is None else str(e["unique_for"]),
            e["unique_until"] if e["unique_for"] is not None else "-",
            e["cron"] or "-",
        ))
    widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    return "\n".join(
        "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
        for row in rows
    )


async def _run_inspect_async(args: argparse.Namespace) -> None:
    """Import *args.module* and print TaskInfo metadata for each task.

    Read-only; no broker / backend connection. R3.5 ce-review P3 #23:
    exposes the declarative dedup configuration so agents and operators
    can discover which tasks have unique_for set without reading source.
    """
    importlib.import_module(args.module)
    if not Task._registered_tasks:
        raise ConfigurationError(
            f"no tasks registered after importing {args.module!r}; "
            "ensure your tasks subclass swarmq.Task and the module's "
            "top-level imports execute the class definitions"
        )

    entries: list[dict[str, Any]] = []
    for task_class in Task._registered_tasks:
        ti = task_class.task_info
        entries.append({
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
        })
    entries.sort(key=lambda e: e["name"])

    if args.format == "json":
        sys.stdout.write(json.dumps(entries, indent=2) + "\n")
    else:
        sys.stdout.write(_format_inspect_table(entries) + "\n")


async def _run_schedule_async(args: argparse.Namespace) -> None:
    """Dispatch a parsed 'swarmq schedule <subcommand>' invocation."""
    if args.schedule_command == "list":
        await _run_schedule_list(args)
    elif args.schedule_command == "add":
        await _run_schedule_add(args)
    elif args.schedule_command == "remove":
        await _run_schedule_remove(args)
    elif args.schedule_command == "status":
        await _run_schedule_status(args)
    else:  # pragma: no cover — argparse required=True guards this
        raise ConfigurationError(
            f"unknown schedule subcommand: {args.schedule_command!r}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Console-script entry point. Returns the process exit code.

    Returns 0 on graceful shutdown, 2 on bad arguments (argparse default),
    1 on any other startup or runtime error. Errors render as a single
    stderr line at INFO+ log levels; ``--log-level DEBUG`` adds the
    full traceback so users can debug their own task modules.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "worker":
        runner = _run_worker_async(args)
        error_prefix = "swarmq worker"
    elif args.command == "schedule":
        runner = _run_schedule_async(args)
        error_prefix = f"swarmq schedule {args.schedule_command}"
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


if __name__ == "__main__":  # pragma: no cover — exercised via console_script
    raise SystemExit(main())
