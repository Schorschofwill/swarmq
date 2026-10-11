"""argparse-Teilbaeume der Betriebsbefehle ``schedule``, ``dlq`` und
``inspect`` (Plan 0.12.0, MR 0; verbatim aus ``parser.py``).
``parser._build_parser`` haengt sie ein.
"""

from __future__ import annotations

from typing import Any

from swarmq.cli._shared import (
    _FORMAT_CHOICES,
    _dotted_module_name,
    _positive_int,
)


def _add_schedule_parser(subparsers: Any) -> None:
    """``swarmq schedule list|add|remove|status|prune``."""
    schedule = subparsers.add_parser(
        "schedule",
        help="Inspect or manage schedules without restarting workers.",
        description=(
            "Manage the embedded scheduler's schedule definitions in "
            "Valkey. Requires SWARMQ_BACKEND_URL (broker is not used "
            "by these subcommands). The leader removes static crons no "
            "worker has reported for scheduler_prune_after_s; 'prune "
            "--orphans' removes static crons no worker ever reported."
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
        help="List all schedules in [<namespace>:]sq:sched:defs.",
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
        help=("Task name to invoke. NOT validated here — a typo only surfaces at fire time on the worker."),
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

    # prune (Plan 2026-10-08-1541, U10, KTD8)
    sched_prune = schedule_sub.add_parser(
        "prune",
        help="Remove static crons no worker has ever reported.",
    )
    sched_prune.add_argument(
        "--orphans",
        action="store_true",
        required=True,
        help=(
            "Remove static crons without any report from a worker (legacy "
            "entries, apps before 0.10.0). The leader never removes them on "
            "its own. A disabled cron comes back disabled when a worker "
            "reports it again."
        ),
    )
    sched_prune.add_argument(
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
        dest="dlq_command",
        metavar="SUBCOMMAND",
        required=True,
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
        "--queue",
        default="default",
        help="Main queue name (DLQ resolved as {queue}.XQ). Default: default.",
    )
    dlq_list.add_argument(
        "--limit",
        type=_positive_int,
        default=100,
        help="Max messages to list (default: 100).",
    )
    dlq_list.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default=None,
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
        "--queue",
        default="default",
        help="Main queue name (default: default).",
    )
    dlq_inspect.add_argument(
        "--scan-limit",
        type=_positive_int,
        default=1000,
        help="Max DLQ messages to scan (default: 1000).",
    )
    dlq_inspect.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default=None,
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
        "task_id",
        nargs="?",
        default=None,
        help="Task ID to retry (omit when using --all).",
    )
    dlq_retry.add_argument(
        "--all",
        action="store_true",
        help="Retry every message in the DLQ.",
    )
    dlq_retry.add_argument(
        "--queue",
        default="default",
        help="Main queue name (default: default).",
    )
    dlq_retry.add_argument(
        "--limit",
        type=_positive_int,
        default=10_000,
        help="Safety cap for --all (default: 10000).",
    )
    dlq_retry.add_argument(
        "--scan-limit",
        type=_positive_int,
        default=1000,
        help="Max DLQ messages to scan when looking up task_id (default: 1000).",
    )
    dlq_retry.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )

    # dlq purge
    dlq_purge = dlq_sub.add_parser(
        "purge",
        help="Delete every message from the DLQ.",
        description=("Native AMQP queue.purge() on {queue}.XQ. Irreversible; messages are lost. Use with care."),
    )
    dlq_purge.add_argument(
        "--queue",
        default="default",
        help="Main queue name (default: default).",
    )
    dlq_purge.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default=None,
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


def _add_check_parser(subparsers: Any) -> None:
    """``swarmq check`` -- Betriebsumgebung pruefen (Plan 0.12.0, MR 14)."""
    check = subparsers.add_parser(
        "check",
        help="Check the operating environment (Valkey, shutdown) before an upgrade.",
        description=(
            "Read-only check of the environment swarmq runs in: Valkey eviction "
            "policy, maxmemory, memory, AOF, version and cluster mode, and the "
            "grace period the shutdown timeout needs. With SWARMQ_BROKER_URL also "
            "the queues workers report: orphaned queues (messages, no consumer, no "
            "live worker) and missing .XQ dead-letter queues. Run it with the new swarmq "
            "version before an upgrade. Exit 0 when nothing blocks, 78 when a "
            "worker would refuse to start (evicting maxmemory-policy under "
            "SWARMQ_VALKEY_EVICTION_CHECK=strict), 1 on errors. Not the same as "
            "'swarmq worker --check'."
        ),
    )
    check.add_argument(
        "--format",
        choices=_FORMAT_CHOICES,
        default=None,
        help=(
            "Output format. Default: 'table' on a TTY, 'json' on a pipe. "
            "Override the default with SWARMQ_DEFAULT_FORMAT=table|json."
        ),
    )
