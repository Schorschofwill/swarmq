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
    _APP_DIR_HELP,
    _VALID_LOG_FORMATS,
    _VALID_LOG_LEVELS,
    _app_dir_path,
    _autoscale_range,
    _configure_hook,
    _dotted_module_name,
    _positive_float,
    _positive_int,
    _queue_list,
    _watch_dir_path,
)
from swarmq.cli.parser_ops import _add_check_parser, _add_dlq_parser, _add_inspect_parser, _add_schedule_parser


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
            "SwarmQ — Python task queue on RabbitMQ + Valkey. Run a worker against tasks defined in a Python module."
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
    _add_check_parser(subparsers)
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
            "e.g. 'myapp.tasks'. Imported with --app-dir (default: the "
            "current working directory) first on the Python path."
        ),
    )
    worker.add_argument(
        "--app-dir",
        type=_app_dir_path,
        default=None,
        metavar="DIR",
        help=_APP_DIR_HELP + " Also used for --configure; supervisor and CPU compute children inherit it.",
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
        help=("Do not set up logging in any swarmq process. Overrides SWARMQ_CONFIGURE_LOGGING."),
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
        help=("Capacity of the in-process log queue (default: 10000). Overrides SWARMQ_LOG_QUEUE_SIZE."),
    )
    worker.add_argument(
        "--log-drain-timeout",
        default=None,
        metavar="SECONDS",
        help=(
            "Upper bound for flushing the log queues at exit, before a "
            "reload and before each compute result (default: 5). Records "
            "not written in time are counted as dropped. "
            "Overrides SWARMQ_LOG_DRAIN_TIMEOUT."
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
        "--no-scheduler",
        dest="scheduler",
        action="store_const",
        const="off",
        default=None,  # aufgeloest in cli.worker_options: Flag > SWARMQ_SCHEDULER > on
        help=(
            "Keep this worker out of the scheduler leader election "
            "(SWARMQ_SCHEDULER=off). It still reports its crons. Crons, "
            "delay/eta tasks and oneshots fire only while another worker "
            "on the same Valkey and namespace runs the scheduler."
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
            "this flag, publishing priority-tagged tasks raises "
            "UnroutableMessage (no {queue}.high/.low exists)."
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
            "and reloads on SIGHUP via os.execvpe() with the startup "
            "sys.orig_argv snapshot."
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
            "setpoint stored in the backend ([<namespace>:]sq:control:setpoint:<group>) "
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
            "existing SIGHUP reload pipeline (supervisor re-exec). "
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
