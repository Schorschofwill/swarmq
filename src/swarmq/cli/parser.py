"""argparse parser for the ``swarmq`` console script.

The parser is built lazily by :func:`_build_parser`. Keeping it in a
dedicated module means ``swarmq.cli.__init__`` stays small and the
1100-line parser definition can be read without scrolling past
subcommand handlers.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
from typing import Any

from swarmq.cli._shared import (
    _FORMAT_CHOICES,
    _VALID_LOG_FORMATS,
    _VALID_LOG_LEVELS,
    _autoscale_range,
    _configure_hook,
    _dotted_module_name,
    _positive_float,
    _positive_int,
    _queue_list,
    _watch_dir_path,
)


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

    _add_worker_parser(subparsers)
    _add_schedule_parser(subparsers)
    _add_dlq_parser(subparsers)
    _add_inspect_parser(subparsers)
    return parser


def _add_worker_parser(subparsers: Any) -> None:
    """``swarmq worker`` — Flags des (Multi-Process-)Workers."""
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
        type=_dotted_module_name,
        metavar="DOTTED.PATH",
        help=(
            "Python module containing Task subclasses to register, "
            "e.g. 'myapp.tasks'. Must be importable from the current "
            "Python path."
        ),
    )
    worker.add_argument(
        "--configure",
        type=_configure_hook,
        metavar="MODULE:CALLABLE",
        help=(
            "Callable invoked with the SwarmQ app after construction and "
            "before the worker starts, e.g. 'myapp.hooks:setup'. Use it to "
            "register middleware and signal handlers without writing your "
            "own worker entry point. Runs once per worker process; an "
            "awaitable return value is awaited."
        ),
    )
    worker.add_argument(
        "--queues",
        type=_queue_list,
        default=None,  # aufgeloest in cli.worker_options: Flag > SWARMQ_QUEUES > default
        metavar="A,B,C",
        help="Comma-separated queue names to consume from (default: default).",
    )
    worker.add_argument(
        "--concurrency",
        type=_positive_int,
        default=None,
        metavar="N",
        help=(
            "Maximum tasks executed in parallel (default: 10). With "
            "--pool cpu this is the number of compute slots; the default "
            "is then the number of CPUs assigned to the worker (affinity "
            "and cgroup quota), divided by --processes."
        ),
    )
    worker.add_argument(
        "--pool",
        choices=("io", "cpu"),
        default=None,  # aufgeloest in cli.pool.resolve_pool: Flag > SWARMQ_POOL > io
        help=(
            "Execution pool (default: io). 'cpu' runs every task body in a "
            "compute child forked from a pre-warmed forkserver: the loop "
            "process never computes, hard_timeout and cancel kill the child, "
            "--concurrency means compute slots. POSIX only; not combinable "
            "with --autoscale."
        ),
    )
    worker.add_argument(
        "--log-level",
        choices=_VALID_LOG_LEVELS,
        default=None,
        help=(
            "Log verbosity (default: INFO). DEBUG also surfaces full tracebacks "
            "on CLI errors. Overrides SWARMQ_LOG_LEVEL."
        ),
    )
    worker.add_argument(
        "--log-format",
        choices=_VALID_LOG_FORMATS,
        default=None,
        help=(
            "Log output format (default: human). 'json' for agent/log-aggregator "
            "consumption. Overrides SWARMQ_LOG_FORMAT (and its alias SWARMQ_LOG_JSON)."
        ),
    )
    worker.add_argument(
        "--log-config",
        default=None,
        metavar="PATH",
        help=(
            "Logging dictConfig file (JSON; YAML with PyYAML installed) applied "
            "in every swarmq process instead of the built-in setup. Not "
            "combinable with --log-format or --no-configure-logging. "
            "Overrides SWARMQ_LOG_CONFIG."
        ),
    )
    worker.add_argument(
        "--no-configure-logging",
        dest="configure_logging",
        action="store_const",
        const=False,
        default=None,
        help=(
            "Do not set up logging in any swarmq process. "
            "Overrides SWARMQ_CONFIGURE_LOGGING."
        ),
    )
    worker.add_argument(
        "--logger-level",
        dest="logger_levels",
        action="append",
        default=None,
        metavar="NAME=LEVEL",
        help=(
            "Level for one logger, e.g. aio_pika=WARNING (repeatable). "
            "Overrides SWARMQ_LOGGER_LEVELS (comma-separated NAME=LEVEL)."
        ),
    )
    worker.add_argument(
        "--log-queue-size",
        default=None,
        metavar="N",
        help=(
            "Capacity of the in-process log queue (default: 10000). "
            "Overrides SWARMQ_LOG_QUEUE_SIZE."
        ),
    )
    worker.add_argument(
        "--log-lifecycle-level",
        choices=_VALID_LOG_LEVELS,
        default=None,
        help=(
            "Level of the task_received/task_started lifecycle events "
            "(default: DEBUG). Overrides SWARMQ_LOG_LIFECYCLE_LEVEL."
        ),
    )
    worker.add_argument(
        "--burst",
        action="store_true",
        default=None,  # aufgeloest in cli.worker_options: Flag > SWARMQ_BURST_MODE > aus
        help=(
            "Burst mode (R4.5): exit cleanly once all configured queues "
            "have been idle for --burst-idle-timeout seconds. Useful for "
            "batch / cron-style deployments where a worker should drain "
            "and exit rather than long-poll forever."
        ),
    )
    worker.add_argument(
        "--metrics-port",
        type=_positive_int,
        default=None,
        metavar="PORT",
        help=(
            "R5.1: serve Prometheus /metrics on the given port. "
            "Auto-wires the signal-based collectors (task_total, "
            "retries_total, etc.). Requires 'pip install swarmq[metrics]'. "
            "Default bind: 127.0.0.1."
        ),
    )
    worker.add_argument(
        "--health-port",
        type=_positive_int,
        default=None,
        metavar="PORT",
        help=(
            "R5.2: serve /health + /ready on the given port. /ready "
            "pings broker + backend (2s timeout) and returns structured "
            "JSON. Default bind: 127.0.0.1."
        ),
    )
    worker.add_argument(
        "--priorities",
        action="store_true",
        default=None,  # aufgeloest in cli.worker_options: Flag > SWARMQ_PRIORITIES > aus
        help=(
            "R4.3: auto-declare + consume the high/normal/low triplet "
            "for every --queues base queue, so tasks with "
            "TaskInfo(priority='high'|'low') route correctly. Without "
            "this flag, priority-tagged tasks publish to "
            "{queue}.high/.low which are never consumed."
        ),
    )
    worker.add_argument(
        "--processes",
        "-p",
        type=_positive_int,
        default=1,
        metavar="N",
        help=(
            "R4.6: run N worker subprocesses under a supervisor "
            "(ProcessManager). Default 1 still runs under the supervisor. "
            "Subprocesses use multiprocessing spawn — fresh "
            "broker/backend connections per worker. The supervisor "
            "auto-restarts crashed workers (max 5 restarts per hour), "
            "and reloads on SIGHUP via os.execvp()."
        ),
    )
    worker.add_argument(
        "--autoscale",
        type=_autoscale_range,
        default=None,
        metavar="MIN,MAX",
        help=(
            "R4.7: enable autoscaling between MIN..MAX workers based "
            "on queue depth. Scale-up when pending tasks exceed "
            "50 * active workers; scale-down after 60s of sustained "
            "idle. 30s cooldown between decisions. Implies "
            "--processes (the initial count is clamped into [MIN,MAX])."
        ),
    )
    worker.add_argument(
        "--max-processes",
        type=_positive_int,
        default=None,
        metavar="N",
        help=(
            "Enable the control channel and declare the headroom: a "
            "setpoint stored in the backend (sq:control:setpoint:<group>) "
            "may raise this supervisor to at most N worker processes. "
            "Slot table, observability port ranges and the default compute "
            "slots per worker are sized from N at start-up; N must be at "
            "least --processes and the --autoscale upper bound. Without "
            "this flag the supervisor never reads or writes setpoints."
        ),
    )
    worker.add_argument(
        "--group",
        default=None,
        metavar="LABEL",
        help=(
            "Group label for the control channel. Supervisors sharing a "
            "label (and a backend) follow the same setpoint, each on its "
            "own. Default: the sorted set of --queues."
        ),
    )
    worker.add_argument(
        "--reload",
        action="store_true",
        help=(
            "R5.3: watch task modules for changes and trigger the "
            "existing SIGHUP reload pipeline (ProcessManager execvp). "
            "Implies --processes. Developer-only — do "
            "not use in production. Requires "
            "'pip install \"swarmq[reload]\"'. "
            "Missing 'watchfiles' exits with code 78 (1 under --check); "
            "invalid --watch-dir exits 2."
        ),
    )
    worker.add_argument(
        "--watch-dir",
        action="append",
        type=_watch_dir_path,
        dest="watch_dir",
        metavar="PATH",
        default=None,
        help=(
            "R5.3: directory to watch (repeatable). Without this flag, "
            "the parent directory of --module's resolved source file is "
            "used. Ignored without --reload (a WARNING is logged)."
        ),
    )
    worker.add_argument(
        "--check",
        action="store_true",
        help=(
            "Dry-run validation mode. Verify --module is importable, "
            "(with --reload) that 'watchfiles' is installed + watch-dirs "
            "resolve, that the --metrics-port/--health-port ranges across "
            "all worker processes stay <= 65535 and do not overlap, the "
            "logging settings and --log-config (dictConfig probe), pool "
            "constraints (--pool cpu with --autoscale) and the "
            "--max-processes headroom. Prints a JSON summary to "
            "stdout, exits 0 on success or 1 on any validation error. "
            "Does NOT connect to broker or backend."
        ),
    )
    worker.add_argument(
        "--burst-idle-timeout",
        type=_positive_float,
        default=None,  # aufgeloest in cli.worker_options: Flag > SWARMQ_BURST_IDLE_TIMEOUT > 5.0
        metavar="SECONDS",
        help=(
            "Seconds of queue silence required before --burst exits "
            "(default: 5.0). The idle clock starts at worker-ready and "
            "resets on every delivered message. Ignored without --burst. "
            "Exit code 0 on clean drain (indistinguishable from SIGTERM; "
            "grep logs for 'burst_idle_exit' to distinguish)."
        ),
    )

    # ---- schedule subcommand group (Phase 3c Unit 12) --------------------


def _add_schedule_parser(subparsers: Any) -> None:
    """``swarmq schedule list|add|remove|status``."""
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
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
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
        # schedule add is backend-only — the name is NOT
        # validated against any registry here; a typo surfaces only at
        # fire time as TaskNotFoundError on the worker (plus the
        # scheduler_task_not_in_local_registry warning).
        help=(
            "Task name to invoke. NOT validated here — a typo only "
            "surfaces at fire time on the worker."
        ),
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
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a "
            "pipe. Override with SWARMQ_DEFAULT_FORMAT=table|json. "
            "Use 'json' for agent-parseable output with an explicit "
            "'inserted' boolean."
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
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a "
            "pipe. Override with SWARMQ_DEFAULT_FORMAT=table|json. "
            "Use 'json' for agent-parseable output with an explicit "
            "'removed' boolean."
        ),
    )

    # ---- dlq subcommand group (R3.10) -----------------------------------

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
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )


def _add_dlq_parser(subparsers: Any) -> None:
    """``swarmq dlq list|inspect|retry|purge``."""
    dlq = subparsers.add_parser(
        "dlq",
        help="Inspect and recover messages from dead-letter queues.",
        description=(
            "Operate on {queue}.XQ — the broker-level dead-letter queue "
            "that receives tasks which exhausted their retries, hit "
            "NoRetry, or were routed via DLX by a permanent-failure "
            "stage (lock render-fail, conflict-cap). Requires "
            "SWARMQ_BROKER_URL (broker-only; no backend needed)."
        ),
    )
    dlq_sub = dlq.add_subparsers(
        dest="dlq_command", metavar="SUBCOMMAND", required=True,
    )

    # dlq list
    dlq_list = dlq_sub.add_parser(
        "list",
        help="Non-destructively enumerate DLQ messages.",
        description=(
            "Drain-and-republish scan of {queue}.XQ. Shows task_id, "
            "task_name, retry-count header, traceback preview, and "
            "body size. Repeated calls are safe; messages stay in "
            "the DLQ."
        ),
    )
    dlq_list.add_argument(
        "--queue", default="default",
        help="Main queue name (DLQ resolved as {queue}.XQ). Default: default.",
    )
    dlq_list.add_argument(
        "--limit", type=_positive_int, default=100,
        help="Max messages to list (default: 100).",
    )
    dlq_list.add_argument(
        "--format", choices=_FORMAT_CHOICES, default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )

    # dlq inspect
    dlq_inspect = dlq_sub.add_parser(
        "inspect",
        help="Show full payload + headers of one DLQ message.",
        description=(
            "Locate a DLQ message by task_id (from payload.task.id) "
            "and print its full headers + decoded body. "
            "Non-destructive."
        ),
    )
    dlq_inspect.add_argument("task_id", help="Task ID to locate.")
    dlq_inspect.add_argument(
        "--queue", default="default",
        help="Main queue name (default: default).",
    )
    dlq_inspect.add_argument(
        "--scan-limit", type=_positive_int, default=1000,
        help="Max DLQ messages to scan (default: 1000).",
    )
    dlq_inspect.add_argument(
        "--format", choices=_FORMAT_CHOICES, default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )

    # dlq retry
    dlq_retry = dlq_sub.add_parser(
        "retry",
        help="Republish DLQ message(s) into the main queue.",
        description=(
            "Republish either a single DLQ message by task_id or every "
            "message (--all). The x-swarmq-retry-count header is reset "
            "so the task gets the full retry budget on redelivery; all "
            "other headers (traceback, R3.5 dedup hash, etc.) survive."
        ),
    )
    # argparse cannot mix positional + mutually-exclusive-group cleanly —
    # model both via optional positional + --all, validate in the handler.
    dlq_retry.add_argument(
        "task_id", nargs="?", default=None,
        help="Task ID to retry (omit when using --all).",
    )
    dlq_retry.add_argument(
        "--all", action="store_true",
        help="Retry every message in the DLQ.",
    )
    dlq_retry.add_argument(
        "--queue", default="default",
        help="Main queue name (default: default).",
    )
    dlq_retry.add_argument(
        "--limit", type=_positive_int, default=10_000,
        help="Safety cap for --all (default: 10000).",
    )
    dlq_retry.add_argument(
        "--scan-limit", type=_positive_int, default=1000,
        help="Max DLQ messages to scan when looking up task_id (default: 1000).",
    )
    dlq_retry.add_argument(
        "--format", choices=_FORMAT_CHOICES, default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )

    # dlq purge
    dlq_purge = dlq_sub.add_parser(
        "purge",
        help="Delete every message from the DLQ.",
        description=(
            "Native AMQP queue.purge() on {queue}.XQ. Irreversible; "
            "messages are lost. Use with care."
        ),
    )
    dlq_purge.add_argument(
        "--queue", default="default",
        help="Main queue name (default: default).",
    )
    dlq_purge.add_argument(
        "--format", choices=_FORMAT_CHOICES, default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )

    # inspect — introspect registered TaskInfo (including R3.5 dedup)


def _add_inspect_parser(subparsers: Any) -> None:
    """``swarmq inspect`` — TaskInfo-Metadaten."""
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
        type=_dotted_module_name,
        metavar="DOTTED.PATH",
        help="Python module whose Task subclasses to introspect.",
    )
    inspect_parser.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )

