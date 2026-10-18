# Logging

SwarmQ logs structured events through Python's standard `logging`
module. As a library it never configures logging: an app that embeds
`SwarmQ(...)` keeps its root handlers, root level and structlog
configuration untouched. Only the processes SwarmQ starts itself — the
`swarmq worker` supervisor, its worker subprocesses and the compute
children of the CPU pool — set up logging, and every part of that setup
can be configured or switched off.

| Where SwarmQ runs | Who configures logging | Where SwarmQ's events go |
|---|---|---|
| Embedded in your app (`SwarmQ(...)`, `await app.start_worker()`) | Your app | Your root handlers; without any, WARNING and above reach Python's `lastResort` handler |
| Embedded, with `log_queue=True` | Your app | Your root handlers, called from a SwarmQ background thread ([opt-in log queue](#opt-in-log-queue)) |
| `swarmq worker` (supervisor, workers, compute children) | SwarmQ, unless you pass `--log-config` or `--no-configure-logging` | stderr, `human` or `json` ([worker processes](#worker-processes)) |
| Compute children of an embedded CPU pool | SwarmQ, per the `SwarmQConfig` logging fields | stderr, not your app's handlers ([embedded CPU pool](#embedded-cpu-pool)) |

## Library loggers and fields

Every module logs through a named logger `swarmq.<module>`, for example
`swarmq.worker.pipeline`, `swarmq.worker.retry`,
`swarmq.process_manager.supervisor` or `swarmq.scheduler.loop`. The
task lifecycle events have their own logger, `swarmq.task`. All of them
sit below `swarmq`, so `logging.getLogger("swarmq").setLevel(...)`
steers the whole library and `logging.getLogger("swarmq.task")` only the
per-task events.

A SwarmQ event is an ordinary `logging.LogRecord`:

- the **event name** is the record message (`record.getMessage()`
  returns `task_succeeded`);
- every **field** is an attribute of the record (`record.task_id`,
  `record.duration_ms`);
- a field whose name collides with a `LogRecord` attribute is renamed
  with the prefix `swarmq_` (`name` becomes `swarmq_name`, `module`
  becomes `swarmq_module`), so logging never fails with a `KeyError`.

SwarmQ installs no `NullHandler` on purpose. Without any logging
configuration, events at WARNING and above still appear on stderr via
Python's `lastResort` handler; INFO and DEBUG stay silent until your app
configures a handler and a level.

`logging.disable(level)` applies to SwarmQ's loggers like to any other.

## Integrating with your app

SwarmQ's events reach whatever handlers your app has. What you see
depends on the formatter: the event name is always the message, the
fields are only visible if the formatter reads record attributes.

### Plain stdlib logging

`logging.basicConfig(level=logging.INFO)` is enough to see SwarmQ's
INFO events, but its default format shows only the event name
(`INFO:swarmq.task:task_succeeded`). To see the fields, use a formatter
that reads the record attributes. A minimal JSON formatter:

```python
import json
import logging

_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message", "asctime",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "event": record.getMessage(),
            "level": record.levelname.lower(),
            "logger": record.name,
        }
        payload.update(
            {k: v for k, v in vars(record).items() if k not in _STANDARD}
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
```

```text
{"event": "task_succeeded", "level": "info", "logger": "swarmq.task", "duration_ms": 12.5, "task_id": "…", "task_name": "send_email", …}
```

Alternatively use SwarmQ's own formatter, which renders your records
and SwarmQ's the same way:

```python
from swarmq.logging import build_formatter

handler = logging.StreamHandler()
handler.setFormatter(build_formatter(json=True, colors=False))
logging.basicConfig(level=logging.INFO, handlers=[handler])
```

### structlog apps

An app that routes stdlib records through structlog's
`ProcessorFormatter` must add `structlog.stdlib.ExtraAdder()` to the
`foreign_pre_chain`. Without it, structlog renders only the event name
and **silently drops every field** of SwarmQ's events:

```python
import logging

import structlog

formatter = structlog.stdlib.ProcessorFormatter(
    foreign_pre_chain=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.ExtraAdder(),             # required for SwarmQ's fields
        structlog.processors.format_exc_info,
    ],
    processors=[
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
        structlog.processors.JSONRenderer(),
    ],
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
root = logging.getLogger()
root.addHandler(handler)
root.setLevel(logging.INFO)
```

SwarmQ's loggers do not depend on `structlog.configure`: your structlog
configuration neither changes SwarmQ's events nor is changed by them.

### `build_formatter`

`swarmq.logging.build_formatter(json: bool, colors: bool)` returns the
formatter SwarmQ uses for its own processes: a structlog
`ProcessorFormatter` that renders every record (SwarmQ's, yours, third
parties') with the keys `event`, `level`, `logger` and `timestamp`
(ISO 8601, UTC, taken from the record's creation time) plus all fields.
`json=True` selects the JSON renderer, otherwise the console renderer,
with ANSI colours only when `colors=True`. A record that cannot be
formatted is replaced by a `log_record_unformattable` event instead of
a traceback in the log stream. In a dictConfig file reference it with
`"()"` (see [the example](#own-dictconfig-file-log-config)).

## Opt-in log queue

By default SwarmQ's events run through your handlers synchronously, in
the thread that logs — inside the event loop for the worker. A slow
handler (network, file on a busy disk) then blocks the loop. Set
`log_queue=True` (env `SWARMQ_LOG_QUEUE=true`) to move this work off the
loop:

```python
app = SwarmQ(broker, backend, log_queue=True, log_level="INFO")
```

- SwarmQ attaches its own queue handler to the `swarmq` logger. A
  background thread named `swarmq-log-lsnr` hands every record to
  your **root** handlers at output time. Handler levels apply, handlers
  added later (Sentry, OpenTelemetry, uvicorn) receive the records, and
  without handlers `lastResort` still applies.
- The caller's context variables are available to your handlers'
  filters, as if they ran in the caller.
- `log_level` sets the level of the `swarmq` logger. Your handlers
  format every record, including the
  [`log_records_dropped`](#non-blocking-output-and-dropped-records)
  notice; `log_json` only selects the format of the
  [`log_drain_timeout_exceeded`](#bounded-flushing) line.
  `log_queue_size` (default `10000`) bounds the queue.
- Handlers you attach directly to the `swarmq` logger stay synchronous.
  Sentry's `LoggingIntegration` still runs in the caller.
- The thread starts with the first record, not in `SwarmQ(...)`. In the
  forkserver of a CPU pool it never starts: records logged there at
  import time go synchronously to your root handlers (without any, WARNING
  and above reach `lastResort`); each compute child starts its own thread
  with its first record.
- The last `SwarmQ` instance to leave `async with` restores the `swarmq`
  logger (handler, `propagate`, level) at once. It then waits for the
  thread to write the remaining records without blocking the event loop,
  at most [`log_drain_timeout`](#bounded-flushing) seconds (default 5);
  what is not written by then counts as dropped. If `__aexit__` is
  cancelled while it waits, the cancellation propagates at once; the
  thread keeps writing in the background until the deadline, and only
  what is left then counts as dropped and is reported. A new
  `SwarmQ(log_queue=True)` created meanwhile gets a fresh queue. The
  app's `log_drain_timeout` also bounds the flush at process exit. This
  also works when the event loop's default executor is already shut
  down; `__aexit__` never raises because of logging.
- The option is off by default and has no effect in processes that
  `swarmq worker` configures (built-in or `--log-config` mode); under
  `--no-configure-logging` it applies.

Without `log_queue`, `log_level` and `log_json` have no effect on an
embedded app: set the levels on your own loggers.

## Worker processes

`swarmq worker` configures logging in the supervisor, in every worker
subprocess and in every compute child. The built-in setup writes to
**stderr** through a queue and a background thread:

- `--log-format human` (default) uses the console renderer; ANSI colours
  only when stderr is a TTY.
- `--log-format json` writes one JSON object per line.
- Both carry the keys `event`, `level`, `timestamp` and `logger` plus the
  event's fields. `logger` exists since 0.8.0.
- `structlog.get_logger()` in your task code renders through the same
  output, unless your app already configured structlog.

```text
{"event": "worker_started", "level": "info", "logger": "swarmq.worker.lifecycle", "timestamp": "2026-10-06T10:01:58.942714Z", "pid": 41872, …}
```

### Options

| Flag | Env var | Default | Effect |
|---|---|---|---|
| `--log-format {human,json}` | `SWARMQ_LOG_FORMAT` | `human` | Output format. `SWARMQ_LOG_JSON=true/false` remains as an alias; `SWARMQ_LOG_FORMAT` wins when both are set. |
| `--log-level LEVEL` | `SWARMQ_LOG_LEVEL` | `INFO` | Root level. `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `--log-config PATH` | `SWARMQ_LOG_CONFIG` | unset | Your own [dictConfig file](#own-dictconfig-file-log-config) instead of the built-in setup. |
| `--no-configure-logging` | `SWARMQ_CONFIGURE_LOGGING=false` | configure | [Leave logging to your app](#leaving-logging-to-your-app) in every SwarmQ process. |
| `--logger-level NAME=LEVEL` (repeatable) | `SWARMQ_LOGGER_LEVELS="aio_pika=WARNING,valkey=ERROR"` | none | Level for individual loggers, applied last (also after a dictConfig). |
| `--log-queue-size N` | `SWARMQ_LOG_QUEUE_SIZE` | `10000` | Capacity of the in-process log queue; a positive integer. |
| `--log-lifecycle-level LEVEL` | `SWARMQ_LOG_LIFECYCLE_LEVEL` | `DEBUG` | Level of `task_received` and `task_started` ([lifecycle events](#task-lifecycle-events)). |
| `--log-drain-timeout SECONDS` | `SWARMQ_LOG_DRAIN_TIMEOUT` | `5` | Upper bound for every flush of the log queues ([bounded flushing](#bounded-flushing)); a number greater than 0. |

`CRITICAL` is accepted wherever a level is.

### Precedence

CLI flag > environment variable > `SwarmQConfig` value > default. The
supervisor resolves the settings **once** and passes the resolved
result to its worker subprocesses and compute children; the children do
not read the environment again. A flag on the command line therefore
wins in every process: with `SWARMQ_LOG_FORMAT=json` and
`--log-format human`, the supervisor and all workers log in human
format.

### Configuration errors

Invalid logging settings end `swarmq worker` with exit code `78` and a
single `worker_config_error` line (fields `source`, `error_type`,
`error`, `hint`; for a broken config file also `path`). This covers:

- an invalid level, format, `NAME=LEVEL` entry or queue size (the
  message names the flag or variable it came from);
- a drain timeout that is not a finite number greater than 0 and at
  most `threading.TIMEOUT_MAX`;
- `--log-config` combined with an explicit format — `--log-format`,
  `SWARMQ_LOG_FORMAT` or `SWARMQ_LOG_JSON` — because the file defines
  the format;
- `--log-config` combined with `--no-configure-logging` or
  `SWARMQ_CONFIGURE_LOGGING=false`;
- a config file that cannot be read, parsed or applied
  ([`LoggingConfigError`](errors.md#loggingconfigerror)).

### Own dictConfig file (`--log-config`)

`--log-config PATH` applies a
[`logging.config.dictConfig`](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema)
file in every SwarmQ process instead of the built-in setup.

- JSON is always supported; `.yaml`/`.yml` only when PyYAML is
  installed (otherwise a configuration error with a hint).
- When the file does not set `disable_existing_loggers`, SwarmQ uses
  `false`; the stdlib default `true` would silence loggers that are
  already imported (SwarmQ's, `aio_pika`).
- SwarmQ starts the listeners of the `QueueHandler`s the file defines
  (`dictConfig` creates them but does not start them) and stops them,
  flushing the queue within [`log_drain_timeout`](#bounded-flushing),
  when the process exits. A handler, filter or formatter that raises
  does not stop such a listener, a listener thread that died is
  restarted by the next record, and a full bounded queue drops the
  record instead of printing a traceback in the caller — as in the
  built-in setup.
- Task code may fork (`os.fork()`, `multiprocessing` with `fork`): in
  the child these queue handlers keep writing
  ([forking from task code](#forking-from-task-code)).
- An explicit `--log-level` additionally sets the root level after the
  file is applied; `--logger-level` entries apply after it too.
- The supervisor resolves the path, loads and validates the file once,
  and passes the validated configuration to its children. The children
  never read the file, so neither a different working directory nor a
  later edit of the file affects them.
- On a reload (`SIGHUP`, `--reload`) the running supervisor validates
  the file again in a separate interpreter **before** it stops a
  worker; supervision of the workers continues meanwhile. If the file
  is invalid now, the reload is aborted with `log_config_reload_failed`;
  if the check does not finish within 30 seconds (a handler may block
  on its target), with `log_config_reload_probe_timeout`. In both cases
  workers and logging keep running unchanged. Details in the
  [hot reload guide](../guides/hot-reload.md#how-it-works).
- In this mode SwarmQ does not configure structlog for your task code;
  if your tasks use `structlog.get_logger()`, configure structlog
  yourself.

Example with SwarmQ's formatter and a queue handler, so the event loop
never writes to the stream itself:

```json title="logging.json"
{
  "version": 1,
  "formatters": {
    "json": {"()": "swarmq.logging.build_formatter", "json": true, "colors": false}
  },
  "handlers": {
    "stderr": {
      "class": "logging.StreamHandler",
      "stream": "ext://sys.stderr",
      "formatter": "json"
    },
    "queue": {
      "class": "logging.handlers.QueueHandler",
      "handlers": ["stderr"]
    }
  },
  "root": {"level": "INFO", "handlers": ["queue"]},
  "loggers": {
    "aio_pika": {"level": "WARNING"}
  }
}
```

```bash
swarmq worker --module myapp.tasks --log-config logging.json
```

The stdlib `QueueHandler` formats the message before queueing it, so a
traceback ends up inside the `event` text rather than in a separate key.

!!! warning "Stream handlers only in multi-process mode"
    The supervisor and every worker process apply the same file. Use
    stream handlers (stdout/stderr) only — several processes writing
    and rotating one file through `RotatingFileHandler` corrupt it.
    Rotate outside the process: journald, logrotate with `copytruncate`,
    or a log collector.

    `copytruncate` needs the output opened in append mode (`>>`, systemd
    `StandardOutput=append:`), otherwise the file fills with NUL bytes
    after each rotation; lines written between copy and truncate are
    lost. journald or a collector avoid both.

### Leaving logging to your app

`--no-configure-logging` (or `SWARMQ_CONFIGURE_LOGGING=false`) makes
SwarmQ set up nothing in any of its processes — no handlers, no levels,
no structlog configuration. Your task module or `--configure` hook is
then responsible; if it does nothing, only WARNING and above reach
`lastResort`. In this mode the [opt-in log queue](#opt-in-log-queue)
applies.

### When a task module configures logging

The IO and the CPU pool follow the same rule. A worker subprocess
configures logging **before** it imports your task module. In the CPU
pool the forkserver imports your task modules; before that it attaches
a placeholder handler to the root logger (it starts no thread and passes
WARNING and above on to `lastResort` when no other handler gets the
record), and every compute child compares against it when it sets up
logging. Every process forked from the forkserver starts without the
placeholder; processes that your app starts from the shared forkserver
itself (for example a `ProcessPoolExecutor` with the default start
method `forkserver` on Python 3.14) therefore get an empty root logger,
and `basicConfig` works there. If your module (or a `--configure`
hook) changes logging, your app wins:

- **It replaces the root handlers** (for example with its own
  `dictConfig`): SwarmQ steps back and keeps its own events off the
  event loop by routing them through its queue to your handlers. It
  logs `logging_reconfigured_by_app` once. SwarmQ loggers that your
  `dictConfig` disabled are re-enabled; the number is in the field
  `reenabled_loggers`.
- **It adds handlers** next to SwarmQ's: both write, SwarmQ removes
  nothing and logs `logging_handlers_added_by_app` as WARNING with the
  handler types. A `--log-config` applied in a compute child does not
  close them.
- **`logging.basicConfig(...)` in a task module has no effect** under
  `swarmq worker`, in both pools, because the root logger already has a
  handler. `--log-format`, `--log-level` and `--log-config` therefore
  apply in the compute children too.

In the CPU pool the compute children inherit what your module set up
in the forkserver. `logging_reconfigured_by_app` and
`logging_handlers_added_by_app` then appear once per pool, from the
logger `swarmq.worker.compute.pool`. If your handlers include a
`QueueHandler` whose `QueueListener` has not been started yet — the
logging cookbook pattern with `listener.start()` in the `--configure`
hook, which runs in the worker process but not in the forkserver —
SwarmQ starts that listener in every compute child, so their lines are
not lost. A listener started at import time makes the forkserver
multi-threaded: the worker refuses to start (exit `78`), and the
message names the thread and suggests starting it lazily or in the
`--configure` hook.

Under `--no-configure-logging` there is no placeholder, and
`basicConfig` in a task module takes effect in both pools.

!!! note "Main module started by path"
    A main module started by path (`python app.py`, for example an
    embedded app with a CPU pool) is imported by the forkserver before
    SwarmQ's preload. Logging that this module configures at import
    time wins in the compute children, and a log record from it can
    start a listener thread in the forkserver. The rule above is
    guaranteed with the `swarmq` command and `python -m`.

To own logging completely, use `--log-config` or
`--no-configure-logging` instead of configuring logging at import time.

### Embedded CPU pool

When an embedded app runs a CPU pool (`pool="cpu"`), SwarmQ starts the
compute children itself. They log to **stderr** according to the
`SwarmQConfig` fields `log_level`, `log_json` (default `true`),
`log_config`, `configure_logging`, `logger_levels`, `log_queue_size` and
`log_drain_timeout` — not through your app's handlers. Logging that your
task modules configure at import time follows the
[same rule as under `swarmq worker`](#when-a-task-module-configures-logging),
including the limit for a main module started by path.

## Non-blocking output and dropped records

The code that logs only builds the record and puts it into a bounded
queue. Rendering and I/O happen in the listener thread. When the queue
is full, SwarmQ drops records instead of blocking:

- From 90 % fill level on, only records below WARNING are dropped;
  WARNING and above use the remaining 10 % and are lost only when the
  queue is completely full.
- Drops are counted per process. The code that logs never writes the
  notice itself and never raises, not even when stderr is closed. The
  listener thread reports the drops at most once per second, and once
  more when it stops: as an ordinary record `log_records_dropped`
  (logger `swarmq.logging`, level WARNING, field `count`) through its
  own handlers. It therefore has the same format as every other line,
  for example
  `{"event": "log_records_dropped", "count": N, "level": "warning", "logger": "swarmq.logging", "timestamp": …}`
  in JSON mode.
- When a handler, filter or formatter raises, the listener keeps
  running. The errors are counted and reported the same way as
  `log_handler_errors` (field `count`): at most once per second — a
  held-back count arrives with the next record after that second, even
  an error-free one — and once more when the listener stops. If that
  report cannot be written either, it goes straight to stderr. A
  listener thread that has died anyway is restarted by the next record.
- The worker exports the counter as the Prometheus metric
  `swarmq_log_records_dropped_total`. It covers the worker process only;
  drops in compute children are visible as their notice line.

### Bounded flushing

The queues are flushed on every orderly exit path, and every flush is
bounded by `log_drain_timeout` (default 5 seconds; `--log-drain-timeout`,
`SWARMQ_LOG_DRAIN_TIMEOUT` or the `SwarmQConfig` field). This applies at
process exit (`atexit`), when a worker subprocess ends, in the
supervisor before it re-executes itself for a reload, in a compute child
before it sends each result, to the listeners of a `--log-config` file
and in `SwarmQ.__aexit__`. All listeners of a process share one deadline
and are stopped in parallel, so one blocked target does not hold up the
others. Records that are not written by then count as dropped (in
`dropped_log_records()` and the metric). For the listeners of a
dictConfig file the count is an upper bound: the given-up thread may
still write some of these records later. Records that sit in a queue
whose listener is not running (for example one logged at the very
moment the flush swapped the queue) are written within the same
deadline.

When the deadline passes:

- SwarmQ gives up the listener thread. Its handlers are removed from the
  list that `logging.shutdown()` flushes at exit, so the interpreter
  does not wait for them either. As long as the thread hangs, new
  records for these handlers are counted as dropped instead of queued.
- Until the flush returns, no record starts a new listener thread — the
  flush would give it up at once although it is healthy; such records,
  and records still in a queue without a running listener, count as
  dropped.
- One final line `log_drain_timeout_exceeded` (fields `count`,
  `timeout_s`) goes directly to file descriptor 2, but only if stderr
  accepts it without blocking; otherwise only the counter remains. It
  is JSON when the given-up queue belongs to a JSON setup (built-in
  `json` format, or `log_json` with the opt-in queue), otherwise the
  text line
  `swarmq: log_drain_timeout_exceeded — N log record(s) not written within T s, counted as dropped`.
- Processes that SwarmQ starts itself — the `swarmq` command with its
  supervisor, the worker subprocesses and the compute children — end
  hard with `os._exit` and the exit code they would have ended with, if
  a given-up log thread still hangs after the flush. Without that, a
  thread stuck while writing to stderr makes the interpreter abort at
  shutdown (`Fatal Python error`, exit `134`) or hang. Registered
  `atexit` functions that have not run yet do not run then. stdout and
  stderr are flushed first, for at most 0.2 seconds — stdout not at
  all when it is the same file or pipe as the stuck stream (for example
  with `2>&1` or under journald). If the `swarmq` command, a worker
  subprocess or a compute child ends with an exception, its traceback
  (or the message of a `SystemExit`) goes to file descriptor 2 first,
  under the same condition as `log_drain_timeout_exceeded` and
  shortened to its last 4096 bytes; Ctrl-C ends the `swarmq` command
  with `130`.
- An app that embeds SwarmQ is never ended hard. If its stderr is still
  blocked when the process ends, the interpreter can still hang or abort
  there; that risk remains.

The `swarmq` command (and `python -m swarmq.cli`) starts through
`swarmq.cli:console_main`. Code that calls `swarmq.cli.main()` directly
gets no hard exit.

A record logged while the interpreter shuts down, for example from a
`__del__` after the flush at exit, is written synchronously instead of
causing a logging error. A process killed with `SIGKILL` loses the
records still in its queue.

A `multiprocessing` queue in a `--log-config` file is flushed before
`multiprocessing` closes its pipe at process exit (the exit hook of
`multiprocessing` usually runs before SwarmQ's). If the queue is closed
anyway — a record logged later, for example from an `atexit` function
registered before `multiprocessing` was imported, or a manager that has
shut down — the listener ends without a traceback and starts no new
thread; such records count as dropped and appear in the
`log_drain_timeout_exceeded` line.

### Forking from task code

Task code may call `os.fork()` (or use `multiprocessing` with the `fork`
start method), in the built-in setup as well as with the queue handlers
of a `--log-config` file:

- In the child every such queue handler gets a fresh, empty queue of the
  same type and size. Records still queued at the fork are written by
  the parent, not a second time by the child, and the parent's drops are
  not reported again. The child's listener thread starts with its first
  record.
- A `multiprocessing` queue (also a `multiprocessing.managers` proxy)
  stays shared with the parent: the child starts no listener of its own
  and only puts records into it; the parent's listener writes them, as
  the only reader of the queue. Under a raw `os.fork()` SwarmQ restarts
  the queue's feeder thread in the child, and a proxy opens its own
  connection to the manager (and registers its own reference) instead
  of sharing the parent's socket. A child that ends with
  `os._exit()` must call `queue.close()` and `queue.join_thread()` first
  (a `multiprocessing` rule), otherwise its last records are lost.
- Queues of other classes (such as your own subclass, created with
  `"()"` in the file) stay unchanged, and the child's own listener
  starts with its first record. SwarmQ only renews `queue.Queue` and
  `queue.SimpleQueue`.
- Before the fork, a hook waits up to 50 ms in total for listener
  threads that are writing at that moment, so the child does not inherit
  a half-written record. A target that was already busy at the previous
  fork and still is gets no second wait, so a listener stuck on a full
  pipe does not cost every fork 50 ms. A target that is busy for longer,
  or code that writes to the same stream without the handler's lock, can
  still leave a half-written record in the child.

## Task context fields

Inside a task, every SwarmQ event carries the task context. So do your
own `structlog.get_logger()` events in `swarmq worker` processes, and in
any app whose structlog chain contains `merge_contextvars`:

| Field | Content |
|---|---|
| `task_id` | ID of the task |
| `task_name` | Registered task name |
| `queue` | Queue the message came from |
| `worker_id` | [Worker identity](configuration.md#worker-identity) |
| `workflow_id` | Workflow the task belongs to; only when the message carries a valid one |
| `attempt` | Current attempt, `retry_count + 1` |
| `compute_child` | PID of the compute child; only for events logged inside a compute child |

The context is bound for the duration of the message and restored
afterwards; context variables your app binds itself are preserved.

## Task lifecycle events

Logged on the `swarmq.task` logger, for both the IO and the CPU pool:

| Event | Level | Fields |
|---|---|---|
| `task_received` | lifecycle level (default `DEBUG`) | task context |
| `task_started` | lifecycle level (default `DEBUG`) | task context |
| `task_succeeded` | `INFO` | `duration_ms`; `compute_child` in the CPU pool |
| `task_failed` | `INFO` | `duration_ms`, `error_type`, `error`; `compute_child` in the CPU pool |
| `task_cancelled` | `INFO` | `duration_ms` |

`task_failed` reports the outcome of one execution. Whether SwarmQ
retries or dead-letters is reported separately by `task_retrying`
(INFO) and `task_sent_to_dlq` (ERROR).

Raise the visibility of `task_received`/`task_started` with
`--log-lifecycle-level INFO`; silence the per-task events with
`--logger-level swarmq.task=WARNING`. Progress warnings
(`update_progress_hset_failed`, `update_progress_publish_failed`) share
the `swarmq.task` logger and stay visible at WARNING.

## Error fields

Events that report an exception carry `error_type` (the exception class
name) and `error` (its message, or the class name when the message is
empty). A traceback is attached only for unexpected errors — failures
SwarmQ does not anticipate, such as a crashing request handler of the
health or metrics server.

## Event catalog

The events below form the logging contract for operators. They are a
selection, not every event SwarmQ emits.

| Event | Logger | Level | Notes |
|---|---|---|---|
| `worker_started` | `swarmq.worker.lifecycle` | INFO | |
| `prefetch_exceeds_consumer_timeout` | `swarmq.worker.prefetch` | WARNING | At start, once: the worker consumes several queues and a task's `hard_timeout` × queues exceeds `SWARMQ_CONSUMER_TIMEOUT_S`; prefetch applies per consumer, so a held message may wait that long for its ack ([prefetch per consumer](../explanation/delivery-semantics.md#prefetch-per-consumer)). Fields `tasks`, `consumed_queues`, `concurrency`, `max_unacked`, `consumer_timeout_s`, `hint`. |
| `worker_died` | `swarmq.process_manager.supervisor` | INFO / WARNING | INFO for a clean exit (`0`) or an expected shutdown, otherwise WARNING. Fields `worker_id`, `pid`, `exitcode`, `expected`, `slot_state`. |
| `worker_config_error` | `swarmq.cli`, `swarmq.process_manager.entry`, `swarmq.process_manager.supervisor` | ERROR | Configuration error, exit `78`. From `swarmq.cli` (the parent process) and `swarmq.process_manager.entry` (a worker child): fields `source` (`supervisor`/`worker`), `error_type`, `error`, `hint`, plus `path` for a broken config file and `worker_id` in the child. From `swarmq.process_manager.supervisor` when a child exited with `78`: fields `worker_id`, `pid`, `exitcode`, `hint`. Traceback on stderr only at level `DEBUG` (`--log-level` or `SWARMQ_LOG_LEVEL`). |
| `worker_connection_error` | `swarmq.cli` | ERROR | Broker or backend unreachable at start, exit `1`. Traceback only at level `DEBUG` (`--log-level` or `SWARMQ_LOG_LEVEL`). |
| `worker_recycle_ack_timeout` | `swarmq.process_manager.entry` | ERROR | A recycling worker got no acknowledgement and exits `75`. |
| `log_config_reload_failed` | `swarmq.process_manager.reload` | ERROR | Reload aborted, the `--log-config` file is invalid now; fields `path`, `error_type`, `error`, `hint`. |
| `log_config_reload_probe_timeout` | `swarmq.process_manager.reload` | ERROR | Reload aborted, checking the `--log-config` file took longer than 30 s; workers keep running. Fields `path`, `timeout_s`, `hint`. |
| `logging_reconfigured_by_app` | `swarmq.logging`, `swarmq.worker.compute.pool` | INFO | Your app replaced the root handlers; field `reenabled_loggers`, from the compute pool also `compute_slot` and `pid` of the reporting child. |
| `logging_handlers_added_by_app` | `swarmq.logging`, `swarmq.worker.compute.pool` | WARNING | Your app added root handlers next to SwarmQ's; field `handler_types`, from the compute pool also `compute_slot` and `pid` of the reporting child. |
| `log_records_dropped` | `swarmq.logging` | WARNING | Field `count`; from the listener thread, at most once per second and once more when it stops. |
| `log_handler_errors` | `swarmq.logging` | WARNING | A handler, filter or formatter raised; field `count`; at most once per second and once more when the listener stops. |
| `log_drain_timeout_exceeded` | `swarmq.logging` | WARNING | Not a log record: a line written directly to stderr when a flush exceeded `log_drain_timeout`, only if stderr does not block. Fields `count`, `timeout_s`. |
| `log_record_unformattable` | the record's logger | the record's level | Field `error`. |
| `task_retrying` | `swarmq.worker.retry` | INFO | Fields `retry_count`, `max_retries`, `delay_ms`. |
| `task_sent_to_dlq` | `swarmq.worker.retry` | ERROR | The task was dead-lettered. Fields `error_type` (exception class; `WorkerHungError` when the delivery limit or the execution guard's orphan limit was reached, `RedeliveryLimitError` when only `x-acquired-count` reached the delivery limit, `LockWaitExceeded` when the wait for a task lock ended) and `error`. Logged only when the copy lies in the dead-letter queue or the message was routed there via the broker DLX; after a failed publish with redelivery only `dlq_publish_failed` appears. |
| `task_rate_limited` | `swarmq.worker.gates.rate_limit` | INFO | Fields `retry_after_ms`, `remaining`, `rate_limit_count`. |
| `task_lock_conflict` | `swarmq.worker.gates.lock` | INFO | Lock held elsewhere; message requeued. Logged on the first conflict of a message and then about once a minute (`TASK_LOCKED` fires on every conflict). Fields `lock_name`, `lock_conflict_count`, `waited_ms`. |
| `lock_wait_exceeded` | `swarmq.worker.gates.lock` | ERROR | The wait for a task lock ended; the message is dead-lettered as `LockWaitExceeded`. Fields `lock_name`, `reason` (`stalled`: same holder longer than its TTL + 60 s; `max_wait`: `SWARMQ_LOCK_WAIT_MAX_S` since the first conflict), `lock_conflicts`, `waited_ms`. |
| `lock_observe_unsupported` | `swarmq.worker.gates.lock` | WARNING | The lock store cannot report the holders of a lock (third-party store, or `observe` returns no `LockObservation`, e.g. an `AsyncMock`); a stalled holder is not detected, only `SWARMQ_LOCK_WAIT_MAX_S` ends the wait. Once per worker. Fields `lock_name`, `hint`, and `error_type`, `error` or `result_type`. |
| `lock_observe_failed` | `swarmq.worker.gates.lock` | WARNING | Reading the holders of a lock failed (backend unreachable, timeout); the message is deferred as usual. Fields `lock_name`, `error_type`, `error`. |
| `valkey_eviction_policy_unsafe` | `swarmq.backend.valkey.environment` | WARNING | Valkey may evict swarmq keys (`maxmemory-policy` other than `noeviction` with a `maxmemory` limit or an unknown limit); tasks can run twice. On connect and on the leader's hourly check; a worker under `SWARMQ_VALKEY_EVICTION_CHECK=strict` then stops with exit `78`. Fields `policy`, `maxmemory`, `hint` ([Valkey requirements](configuration.md#valkey-requirements)). |
| `valkey_eviction_policy_unknown` | `swarmq.backend.valkey.environment` | WARNING | `maxmemory-policy` could not be read (`CONFIG GET` and `INFO memory` failed, were forbidden or took longer than 2 s); never blocks the start. Field `hint`. |
| `valkey_data_loss_detected` | `swarmq.scheduler.epoch` | WARNING | The epoch key `[<namespace>:]sq:{sched}:epoch` this process knew is gone or different: Valkey lost its data. Crons disabled with `swarmq schedule remove` are active again, CLI crons and delayed tasks are gone. Fields `previous_epoch`, `epoch`, `hint`. |
| `task_deferred_backend_unavailable` | `swarmq.worker.gates.backend_defer` | WARNING | A check before execution (schedule-fire dedup, cancel check, rate limit, lock) could not reach the backend (connection or timeout error); the message went back through the delay queue as a new message and returns after `delay_ms` (the doubled wait, varied by ±20 %). Not a retry: `max_retries` and the delivery limit stay untouched. The only line per deferral. Fields `gate` (`schedfire`, `cancel`, `rate_limit`, `lock`, `execution_guard`), `stage` (`None` for `execution_guard`) (`completed_check`, `inflight_claim`, `cancel_check`, `workflow_cancel_check`, `cancel_store_result`, `check`, `acquire`), `defer_count`, `delay_ms`, `error_type`, `error`, plus `schedfire_key`, `workflow_id` or `lock_name` where they apply. |
| `task_gate_backend_error` | `swarmq.worker.gates.backend_defer` | ERROR | A check before execution (schedule-fire dedup, cancel check) reached the backend, but it answered with an error (`BackendResponseError`, for example `WRONGTYPE`). Not an outage, so the message is not deferred: it goes back to the queue in place and reaches `{queue}.XQ` through the delivery limit if the error persists. Fields `gate`, `stage`, `error_type`, `error`, plus `schedfire_key` or `workflow_id` where they apply. The rate-limit and lock checks log `rate_limit_check_failed` and `lock_acquire_failed` for this case. |
| `task_defer_failed_redelivered` | `swarmq.worker.gates.backend_defer` | WARNING | The deferral could not be published either (broker unreachable too); the message is requeued in place instead. Fields `gate`, `stage` and the context fields as above, `backend_error_type` and `backend_error` (the backend error that caused the deferral), `error_type` and `error` (the publish error). |
| `task_requeued_as_copy` | `swarmq.worker.gates` | WARNING | A message that held a schedule-fire claim was to be requeued in place, but the claim could not be released. A requeued original cannot carry the claim token and would be acked as a duplicate, so a copy with the token went to the delay queue instead, with the backoff of a deferral. Fields `schedfire_key`, `defer_count`, `delay_ms`; the failed release logs `schedfire_finalize_failed` before it. |
| `task_copy_failed_redelivered` | `swarmq.worker.gates` | WARNING | The copy of `task_requeued_as_copy` could not be published either (broker unreachable too); the message is requeued in place. The redelivery finds the claim of its own fire and is deferred until the claim expires (since 0.12.1; on classic queues, which carry no delivery counter, it can still be acked as a duplicate). Fields `schedfire_key`, `error_type`, `error` (the publish error). |
| `schedfire_redelivery_deferred` | `swarmq.worker.gates.schedfire` | INFO | A redelivered schedule fire found an in-flight claim of another delivery (for example of a worker that crashed). It went back through the delay queue as a new message instead of being acked as a duplicate, and is checked again after `delay_ms`. Fields `task_id`, `task_name`, `schedfire_key`, `defer_count` (header `x-swarmq-schedfire-defer-count`), `delay_ms`. |
| `schedfire_defer_failed_redelivered` | `swarmq.worker.gates.schedfire` | WARNING | The deferral of `schedfire_redelivery_deferred` could not be published (broker unreachable); the message is requeued in place. Fields `task_id`, `task_name`, `schedfire_key`, `error_type`, `error`. |
| `schedfire_defer_stalled` | `swarmq.worker.gates.schedfire` | WARNING | A redelivered schedule fire kept finding a foreign claim for longer than the claim can live (`cap_s`): another delivery holds a fresh claim. The redelivery is acked without running. Fields `task_id`, `task_name`, `schedfire_key`, `defer_count`, `cap_s`, `hint`. |
| `schedfire_finalize_failed` | `swarmq.worker.pipeline` | WARNING | A schedule-fire claim could not be completed (`terminal` true) or released (`terminal` false): the backend did not answer within `SWARMQ_BACKEND_OP_TIMEOUT` or raised. The worker carries on; an in-flight claim expires with its TTL. Fields `schedfire_key`, `terminal`, `error_type`, `error`. |
| `rate_limit_check_failed` | `swarmq.worker.gates.rate_limit` | ERROR | The rate-limit check failed with an error other than an unreachable backend (an error reply such as `WRONGTYPE`, or any exception of a third-party store); the message is requeued in place and reaches `{queue}.XQ` through the delivery limit if the error persists. An unreachable backend logs `task_deferred_backend_unavailable` instead. Fields `error_type`, `error`. |
| `lock_acquire_failed` | `swarmq.worker.gates.lock` | ERROR | Same as `rate_limit_check_failed`, for the lock acquire. Fields `lock_name`, `error_type`, `error`. |
| `lock_release_failed` | `swarmq.worker.pipeline`, `swarmq.worker.gates.lock` | WARNING / DEBUG | Releasing a task lock failed (best effort); the lock expires after its TTL (`hard_timeout + 30 s`). Field `stage`: absent for the release after the task (WARNING, from `swarmq.worker.pipeline`); `acquire_timeout` for the owner-checked release in the background after an acquire without a reply, which may have written the lock (from `swarmq.worker.gates.lock`). There it is DEBUG when the backend is down, since `task_deferred_backend_unavailable` already reports the outage, and WARNING for any other error. Fields `lock_name`, `stage`, `error_type`, `error`. |
| `task_lock_lost` | `swarmq.worker.lock_keeper`, `swarmq.worker.pipeline`, `swarmq.worker.lifecycle` | WARNING | The task's lock was gone or held by another owner: found while extending it (`stage="running"`: evicted, data loss, a Valkey outage of about two ticks once the extensions have started, about 40 s with a tick of 20 s), at release (`stage="release"`; also when the Valkey client repeated the release after a lost reply: the first attempt released the lock, the repeat finds it gone), or no extension succeeded before its deadline (`stage="expired"`, logged without a round trip, e.g. while Valkey is down); a second holder may run in parallel. Fields `lock_name`, `stage`, `hint`, `task_id`, `task_name` (at release during shutdown only while the lock was still kept). |
| `task_lock_held_past_hard_timeout` | `swarmq.worker.lock_keeper` | WARNING | The handler outlived `hard_timeout + 30 s` and the worker keeps extending its lock (the watchdog could not cancel it). Once per held lock. Fields `lock_name`, `task_id`, `task_name`, `lock_ttl_ms`, `hint`. |
| `lock_keeper_tick_failed` | `swarmq.worker.lock_keeper` | WARNING | A tick of the lock keeper raised; the keeper keeps running and tries again at the next tick. Fields `leases`, `error_type`, `error`. |
| `lock_extend_failed` | `swarmq.worker.lock_keeper` | WARNING | Extending a held lock failed (backend unreachable, timeout); retried at the next tick, the lock keeps its remaining TTL. Fields `lock_name`, `task_id`, `task_name`, `error_type`, `error`. |
| `lock_extend_unsupported` | `swarmq.worker.lock_keeper` | WARNING | The lock store has no `extend` (a third-party store), or `extend` returned neither `True` nor `False` (e.g. `None` from a `LockStore` subclass, an `AsyncMock`); the lock is not extended and expires with its TTL as before 0.12. Once per held lock. Fields `lock_name`, `task_id`, `task_name`, and `error_type`, `error` or `result_type`. |
| `publish_notification_failed` | `swarmq.worker.results`, `swarmq.worker.retry`, `swarmq.worker.gates.cancel`, `swarmq.backend.valkey.results` | WARNING | The result notification for waiting clients could not be published (after a pre-start cancel also on a timeout after `SWARMQ_BACKEND_OP_TIMEOUT`); the result is stored, clients waiting on it find it by polling. Fields `task_id`, `error_type`, `error`. |
| `task_expired` | `swarmq.worker.gates.expiry` | WARNING | Older than `max_age`; sent to the DLQ. Fields `age_s`, `max_age_s`. |
| `delivery_limit_reached_dead_letter` | `swarmq.worker.gates.delivery_count` | ERROR | Delivered `delivery_limit` times without acknowledgement; dead-lettered as `WorkerHungError`, or as `RedeliveryLimitError` when only `x-acquired-count` (RabbitMQ 4.3, counts every hand-back) reached the limit. Fields `delivery_count` (the counter that reached the limit), `delivery_limit`. |
| `redeliver_pause_failed` | `swarmq.worker.gates` | WARNING | The wait before a hand-back (`nack(requeue=True)`) failed; the delivery is handed back without it. Fields `error_type`, `error`. |
| `message_body_too_large` | `swarmq.worker.decode` | ERROR | Body over `SWARMQ_MAX_MESSAGE_BODY_BYTES`; not decoded, rejected to `.XQ` (copy with retention, the DLX as fallback). Fields `body_bytes`, `cap_bytes`, `hint`. |
| `deserialize_failed` | `swarmq.worker.decode` | ERROR | Undecodable body; routed to the DLX. |
| `task_unknown_deferred` | `swarmq.worker.pipeline` | WARNING | Task name not registered in this worker; the message is deferred with backoff within `SWARMQ_UNKNOWN_TASK_GRACE_S` ([unregistered tasks](../guides/dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)). Fields `delay_ms`, `unknown_since`, `grace_s`, `hint`. |
| `task_not_found` | `swarmq.worker.pipeline` | ERROR | Task name not registered in this worker and the grace period is over (or `0`): dead-lettered with an error result, `TASK_DLQ` and `TASK_REJECTED`. Fields `unknown_since`, `grace_s`. |
| `unknown_task_defer_failed` | `swarmq.worker.pipeline` | ERROR | The deferral copy of an unknown task could not be published; the message is redelivered. Fields `error_type`, `error`. |
| `unknown_task_dlq_failed` | `swarmq.worker.pipeline` | ERROR | The dead-letter publish of an unknown task failed; the message is routed via the broker DLX without diagnostic headers (`TASK_REJECTED` with `dlq_publish_failed`). Fields `error_type`, `error`. |
| `retry_publish_failed` | `swarmq.worker.retry` | ERROR | Retry could not be published; the message is redelivered. |
| `retry_ack_failed_after_publish` | `swarmq.worker.retry` | WARNING | The retry copy was confirmed by the broker but acknowledging the original failed; the retry stands (`task_retrying` follows) and the redelivered original is skipped as superseded (since 0.13.0; before, ERROR `retry_publish_failed`). Fields `error_type`, `error`. |
| `dlq_publish_failed` | `swarmq.worker.retry` | ERROR | Dead-letter publish failed; the message is redelivered. No `task_sent_to_dlq` follows. |
| `dlq_ack_failed_after_publish` | `swarmq.worker.gates` | WARNING | The copy is in the DLQ, only the ack of the original failed (channel lost). Counts as dead-lettered (task failure, end of a lock wait, lock render error): no DLX fallback, the unique flag is released, and the execution guard skips the redelivery. |
| `result_serialization_failed` | `swarmq.worker.results` | ERROR | Task result cannot be serialized; stored as an error. |
| `workflow_state_missing` | `swarmq.workflow_orchestration` | WARNING | The workflow state no longer exists (expired or cleaned up) when a step finished or failed terminally; the workflow cannot advance or be marked failed. Field `workflow_id`; on a finished step also `step_path`, `task_id`. |
| `workflow_fail_skipped_step_done` | `swarmq.worker.retry` | INFO | A step ended terminally (dead-letter, expiry, DLX route), but a success result for its `task_id` is already stored: a late duplicate, or a success whose advance failed. The workflow is not marked failed; since 0.13.0 the worker advances it with the stored result instead. Fields `workflow_id`, `task_id`. |
| `workflow_advance_caught_up` | `swarmq.worker.retry` | INFO | The worker completed a workflow advance that an earlier delivery of an already successful step could not finish; the message was acked without a DLQ copy and without `TASK_DLQ` (since 0.13.0). Fields `workflow_id`, `task_id`. |
| `workflow_advance_failed` | `swarmq.worker.results` | ERROR | Unexpected error while advancing a workflow; the workflow is marked failed. Field `workflow_id`. |
| `workflow_advance_backend_error`, `workflow_advance_broker_error` | `swarmq.worker.results` | WARNING / ERROR | Backend/broker error (or state conflict) while advancing. WARNING for `AT_LEAST_ONCE`: the step is redelivered and the idempotent advance runs again. This includes a lasting error reply such as `WRONGTYPE` (`BackendResponseError`, for example from the chord script): the step is redelivered until the queue's delivery limit, like at the lock and rate-limit gates. ERROR when the step cannot be redelivered: for `FIRE_AND_FORGET` the workflow does not advance; for a successful step that reached the delivery limit (since 0.13.0) the message goes to the DLQ without an error result and the workflow stays `running` until its TTL. Fields `workflow_id`, `error_type`, `error`. |
| `workflow_advance_unroutable` | `swarmq.worker.results` | ERROR | The queue of the next workflow step does not exist (`UnroutableMessage`); the workflow is marked failed at once and the step acked instead of redelivered. Fields `workflow_id`, `error_type`, `error`. |
| `delay_tier_unavailable` | `swarmq.broker.rabbitmq.declarations` | WARNING | The broker refused to declare a delay tier at worker start (queue limit, permission, an existing queue with other arguments); its delays go to `{queue}.DQ` for the rest of the process ([delay tiers](../explanation/delivery-semantics.md#delay-tiers)). Once per tier and process. Fields `queue`, `tier`, `fallback`, `error_type`, `error`, `hint`. |
| `own_queue_unroutable_redeclaring` | `swarmq.broker.rabbitmq.redeclare` | WARNING | A retry or dead-letter copy to the worker's own `{queue}.DQ`/`.XQ` was returned; the queue family (for a delay tier `{queue}.DQ.<tier>`: only that tier) is declared again on a separate channel and the copy published once more. Fields `queue`, `error_type`, `error`, `hint`. |
| `queue_argument_adopted` | `swarmq.broker.rabbitmq.arg_conflict` | WARNING / INFO / DEBUG | An existing queue has another `x-delivery-limit`, `x-dead-letter-strategy` or `x-overflow` than the configuration, and the worker declared it with the existing value. WARNING for `x-delivery-limit` (it works differently from the configuration, field `hint`); for the other arguments the first adoption per process is INFO, later ones DEBUG ([existing queue arguments](../explanation/delivery-semantics.md#existing-queue-arguments)). Fields `queue`, `argument`, `declared`, `existing`. |
| `queue_dead_letter_at_most_once` | `swarmq.broker.rabbitmq.declarations` | WARNING | At start, at most once per process: existing quorum queues that dead-letter at-most-once (no `x-dead-letter-strategy=at-least-once` with `x-overflow=reject-publish`); retries and DLQ transfers can be lost on a leader change. A policy fixes the dead-lettering but not the warning; `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=legacy` silences it ([dead-lettering guarantee](../explanation/delivery-semantics.md#dead-lettering-guarantee)). Fields `queues`, `hint`. |
| `publish_connection_blocked` | `swarmq.broker.rabbitmq.delivery` | WARNING | The broker blocked the publish connection (resource alarm: memory or disk); the publish sends nothing and waits until the unblock or `SWARMQ_PUBLISH_TIMEOUT_S`, then raises `BrokerPublishBlocked`. Fields `routing_key`, `hint`. |
| `publish_channel_close_incomplete` | `swarmq.broker.rabbitmq.connection` | WARNING | After a failed publish (deadline, lost confirm) the publish channel is discarded and closed in the background within 5 s; the close did not complete. During a broker resource alarm this is expected (the broker does not read the connection): logged without traceback and with `blocked=True` (since 0.13.0), and no channel leaks. Otherwise logged with traceback. Fields `error_type`, `error`, `blocked`. |
| `Got frame for closed channel N: <Channel.CloseOk>` | `aiormq.connection` | ERROR | Not a SwarmQ event: aiormq's own message, without task context. It follows `publish_channel_close_incomplete` during a resource alarm: when RabbitMQ unblocks the connection, it answers the abandoned close of the discarded channel, which aiormq has already removed. No action needed; the channel is closed on both sides. It is preceded by aiormq's WARNING `Closing channel … because RPC call <Channel.Close> cancelled`. |
| `publish_blocked_state_unreadable` | `swarmq.broker.rabbitmq.delivery` | WARNING | Once per broker: the blocked state of the connection cannot be read from aiormq; publishes run unchecked (fail-open) and hang on the socket during a broker alarm as before 0.12. Field `hint`. |
| `dlq_read_ack_failed` | `swarmq.broker.rabbitmq.admin` | WARNING | `swarmq dlq list`/`inspect`/`retry` could not acknowledge a message it had read; the message returns to the DLQ, after a copy it is there twice ([DLQ guide](../guides/dead-letter-queues.md)). Fields `error_type`, `error`, `hint`. |
| `result_store_retrying` | `swarmq.worker.results` | WARNING / DEBUG | Storing a task result failed; the worker retries the store before redelivering the message (`AT_LEAST_ONCE` only, since 0.13.0): up to five attempts, waits of nominally 1, 2, 4 and 8 s, capped by `SWARMQ_CONSUMER_TIMEOUT_S` and the drain end. The task slot stays taken meanwhile. WARNING at most once per worker and 10 s, otherwise DEBUG. Fields `task_id`, `store_attempt`, `delay_s`, `error_type`, `error`. |
| `result_store_recovered` | `swarmq.worker.results` | INFO | A result store succeeded after retries; the message is acknowledged without a second run. Fields `task_id`, `store_attempts`. |
| `backend_connection_error` | `swarmq.worker.results` | WARNING / ERROR | The task result could not be stored. WARNING for `AT_LEAST_ONCE`, after the store retries (`result_store_retrying`) failed too: the message is redelivered and the task runs again. ERROR for `FIRE_AND_FORGET`: the result is lost. Fields `error_type`, `error`. |
| `delivery_settle_failed` | `swarmq.worker.settle` | WARNING | A delivery could not be settled because the broker channel or connection was lost (usually the final ack). RabbitMQ redelivers the unacknowledged message, so the task may run again. Fields `error_type`, `error`, `hint`. |
| `delivery_settle_deferred` | `swarmq.broker.rabbitmq.delivery` | INFO | RabbitMQ 4.3: an ack or nack for a delivery whose consumer the broker cancelled waited until the consumer watch had sent its own cancel, and was sent then ([consumer timeout](../explanation/delivery-semantics.md#consumer-timeout)). Fields `verb`, `consumer_tag`. |
| `delivery_settle_withheld` | `swarmq.broker.rabbitmq.delivery` | WARNING | Same situation, but the own cancel did not complete within 10 s: the settle was not sent, so it cannot wake the zombie consumer; the broker had already taken the message back. The call returns normally. Fields `verb`, `consumer_tag`, `wait_s`, `hint`. |
| `consumer_reregistered` | `swarmq.broker.rabbitmq.consumer_watch` | WARNING | The consumer watch registered a consumer again that the broker had ended; counts `swarmq_consumer_reregistrations_total{queue}`. Fields `queue`, `reason` (`broker_cancel`: RabbitMQ 4.3 `basic.cancel` on an open channel; `channel_replaced`: channel closed or replaced, up to 4.2), `old_consumer_tag`, `consumer_tag`. |
| `consumer_cancel_failed` | `swarmq.broker.rabbitmq.consumer_watch`, `swarmq.broker.rabbitmq.delivery` | WARNING | The own cancel of a consumer failed: in the watch the next tick tries again and only then registers anew (fields `queue`, `consumer_tag`, `error_type`, `error`); at shutdown (`cancel_consume`) the remaining consumers are cancelled anyway (fields `consumer_tag`, `error_type`, `error`). |
| `consumer_reregister_failed` | `swarmq.broker.rabbitmq.consumer_watch` | WARNING | Registering a consumer again failed; the next tick tries again, and `/ready` turns red after `SWARMQ_CONSUMER_READY_GRACE_S`. Fields `queue`, `reason`, `error_type`, `error`. |
| `consumer_watch_channel_timeout` | `swarmq.broker.rabbitmq.consumer_watch` | WARNING | The consume channel was not available within the RPC timeout (2 s), for example during a reconnect; the tick ends and the next one checks again. Fields `queue`, `timeout_s`. |
| `consumer_watch_tick_failed` | `swarmq.broker.rabbitmq.consumer_watch` | WARNING | A tick of the consumer watch failed unexpectedly; the watch keeps running. Fields `error_type`, `error`. |
| `execution_guard_peek_failed` | `swarmq.worker.gates.execution_guard` | WARNING | Reading the execution guard state at the delivery limit failed; the message is dead-lettered as before, without knowing whether it already completed (since 0.13.0). Fields `task_id`, `task_name`, `error_type`, `error`, `hint`. |
| `duplicate_delivery_skipped` | `swarmq.worker.gates.execution_guard` | INFO | [Execution guard](../explanation/delivery-semantics.md#execution-guard): the execution already finished (`reason=done`) or a retry superseded it (`reason=superseded`); the redelivery was acknowledged without running. Fields `task_id`, `task_name`, `reason`. |
| `execution_guard_deferred` | `swarmq.worker.gates.execution_guard` | INFO | The same execution runs elsewhere; the delivery went back through the delay queue as a new message. Not a retry. Fields `task_id`, `task_name`, `defer_count`, `delay_ms`. |
| `execution_guard_defer_failed_redelivered` | `swarmq.worker.gates.execution_guard` | WARNING | That deferral could not be published (broker unreachable); the message is requeued in place instead. Fields `task_id`, `task_name`, `error_type`, `error`. |
| `execution_guard_orphan_limit` | `swarmq.worker.gates.execution_guard` | ERROR | The same execution expired `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` times without release (the worker running it died or hung); the delivery is dead-lettered as `WorkerHungError`. Fields `task_id`, `task_name`, `orphans`, `max_orphans`. |
| `execution_guard_lease_lost` | `swarmq.worker.gates.execution_guard` | WARNING | The running marker of a task in flight was gone or taken over at renewal (event loop blocked, Valkey outage longer than the TTL); a redelivery may run in parallel. Once per delivery. Fields `task_id`, `identity`, `hint`. |
| `execution_guard_defer_stalled` | `swarmq.worker.gates.execution_guard` | WARNING | The deferrals of a redelivery add up to more than `consumer_timeout_s` (nominal backoff): the same execution still runs elsewhere and renews its marker, so it runs unusually long or hangs. The redelivery keeps being deferred, no DLQ. Once per message. Fields `task_id`, `task_name`, `defer_count`, `consumer_timeout_s`, `hint`. |
| `execution_guard_renew_failed` | `swarmq.worker.gates.execution_guard` | WARNING | A renewal of the running marker failed; the next tick tries again, the TTL covers three ticks. Fields `task_id`, `identity`, `error_type`, `error`. |
| `execution_guard_finish_failed` | `swarmq.worker.gates.execution_guard` | WARNING | Setting a marker after the broker verb failed (Valkey briefly unreachable); the verb is not held up, the worker retries the marker in the background (only its own marker, with the backoff of `backend_defer_base_delay`/`_max_delay`, for about `execution_guard_ttl_s`; on shutdown one last attempt right away). Fields `identity`, `marker` (`done`, `superseded`, `release`), `error_type`, `error`. |
| `execution_guard_finish_recovered` | `swarmq.worker.gates.execution_guard` | INFO | A marker that had failed was set in the background. Fields `identity`, `marker`, `attempts`. |
| `execution_guard_finish_gave_up` | `swarmq.worker.gates.execution_guard` | WARNING | The background retries of a marker failed for about `execution_guard_ttl_s` (or the last attempt on shutdown failed): the running marker expires by its TTL and the next delivery counts it as an orphan; without the done marker a redelivery runs the task again. Fields `identity`, `marker`, `attempts`, `hint`, `error_type`, `error`. |
| `delivery_handler_failed` | `swarmq.worker.settle` | ERROR | An unexpected error ended the processing of a delivery; with traceback. Before 0.10.0 such errors only showed up as asyncio's "Task exception was never retrieved" without task context. Fields `error_type`, `error`. |
| `middleware_hook_failed` | `swarmq.middleware` | ERROR | A middleware hook other than `pre_enqueue` raised; with traceback. Fields `hook`, `middleware`. |
| `signal_handler_error` | `swarmq.signals` | WARNING | A signal handler raised; with traceback. Fields `signal`, `handler`. |
| `signal_handler_timeout` | `swarmq.signals` | WARNING | A lifecycle signal handler exceeded its time budget. Fields `signal`, `handler`, `timeout_s`. |
| `worker_hung` | `swarmq.process_manager.supervisor` | WARNING | No heartbeat; the worker is replaced. Fields `worker_id`, `pid`, `stale_s`, `deadline_s`. |
| `worker_hung_during_drain` | `swarmq.process_manager.supervisor` | WARNING | A draining or recycling worker stopped beating; it is terminated (SIGTERM, then SIGKILL), not replaced. Fields `worker_id`, `pid`, `stale_s`, `deadline_s`. |
| `worker_hung_killed` | `swarmq.process_manager.supervisor` | ERROR | A replaced worker survived SIGTERM and was killed. Fields `worker_id`, `pid`. |
| `hang_replacement_budget_exhausted` | `swarmq.process_manager.supervisor` | ERROR | Too many hung workers in the window; the slot is locked for the cooldown (1 h, since 0.12; before, for good). Fields `worker_id`, `limit`, `window_s`, `cooldown_s`, `hint`. |
| `worker_restart_limit_exceeded` | `swarmq.process_manager.supervisor` | ERROR | A slot crashed 5 times within an hour and is locked for the cooldown (1 h, since 0.12; before, for good). Fields `worker_id`, `limit`, `window_s`, `cooldown_s`, `recent_restarts`, `exhausted_slots`, `initial_workers`. |
| `worker_slot_unlocked` | `swarmq.process_manager.supervisor` | INFO | The cooldown of a locked slot has passed; the supervisor fills the slot again with an empty restart history (since 0.12). Fields `worker_id`, `cooldown_s`, `locked_slots`. |
| `shutdown_sigterm_escalation` | `swarmq.process_manager.supervisor` | INFO | The supervisor sends SIGTERM to workers still running at the planned drain end (T − 7 s by default); this is a regular step of the [shutdown budget](../explanation/delivery-semantics.md#shutdown-budget), not an alarm (since 0.13.0; before, WARNING). Field `count` (workers still running). |
| `shutdown_sigkill_escalation` | `swarmq.process_manager.supervisor` | ERROR | Workers still alive at T − 2 s are killed; this indicates a hanging worker. Field `count`. |
| `shutdown_step_timeout` | `swarmq.worker.lifecycle` | WARNING | A cleanup step of the graceful shutdown exceeded its time limit (the rest of `SWARMQ_SHUTDOWN_TIMEOUT`, at least 1 s) and was abandoned; the shutdown continues with the next step (since 0.12). Fields `step` (`executor_close`, `execution_guard_drain`, `di_container_close`, `broker_close`, `backend_close`), `timeout_s`. A hanging lock release logs `shutdown_lock_release_failed`, a hanging scheduler stop `scheduler_stop_failed` (the scheduler is then aborted). |
| `all_worker_slots_exhausted` | `swarmq.process_manager.supervisor` | ERROR | Every slot hit the restart limit; the supervisor exits. |
| `process_manager_reload_execvp` | `swarmq.process_manager.reload` | INFO | The supervisor re-executes itself for a reload, after its workers stopped. |
| `reload_aborted_alive_children` | `swarmq.process_manager.reload` | WARNING | A worker child survived the stop for a reload (even `SIGKILL`, for example in an uninterruptible kernel wait); the supervisor does not re-execute itself, so the child is not orphaned, and exits `0`. Fields `alive_count`, `alive_pids`, `hint`. |
| `reload_execvp_failed` | `swarmq.process_manager.reload` | ERROR | The `SIGHUP` re-exec failed; the supervisor exits `1`. |
| `hot_reload_enabled` | `swarmq.process_manager.supervisor` | WARNING | `--reload` is active (development only). Field `watch_dirs`. |
| `compute_pool_unhealthy` | `swarmq.worker.compute.pool` | ERROR | No compute child is usable; `/ready` reports the pool down. |
| `forkserver_restarted_multithreaded` | `swarmq.worker.compute.forkserver` | ERROR | A silently restarted forkserver has more than one thread; the pool rejects it and stays unhealthy (`/ready`). Fields `pid`, `threads`, `thread_names`, `hint`. |
| `compute_fork_source_rejected` | `swarmq.worker.compute.wiring` | ERROR | After that rejection the worker stops consuming and shuts down; a worker subprocess exits `71`. Fields `reason`, `hint`. |
| `worker_fork_source_rejected` | `swarmq.process_manager.supervisor` | WARNING | A worker child exited `71`; it is restarted with a fresh forkserver (within the restart budget). Fields `worker_id`, `pid`, `exitcode`. |
| `compute_child_unreaped` | `swarmq.worker.compute.pool` | ERROR | A compute child survived SIGKILL; its slot stays blocked. Fields `slot`, `pid`, `reason`. |
| `setpoint_applied` | `swarmq.process_manager.setpoint` | INFO | Control channel: new bounds in effect. Fields `group`, `effective`. |
| `setpoint_rejected` | `swarmq.process_manager.setpoint` | WARNING | Control channel: setpoint rejected, previous bounds stay. Fields `group`, `reason`. |
| `setpoint_store_unreachable` | `swarmq.process_manager.setpoint` | WARNING | Control channel: backend not reachable. Fields `group`, `error`. |
| `scheduler_next_fire_failed` | `swarmq.scheduler.fire_plan` | ERROR | The next fire of a cron cannot be calculated (unknown zone, broken expression). The entry leaves the fire index, the definition stays, the rest of the batch fires; the next adoption tick plans it again once a live worker reports it and the fire can be calculated. Fields `schedule_name`, `cron`, `timezone`, `error_type`, `error`. |
| `schedule_def_malformed_parked` | `swarmq.scheduler.definition` | WARNING | A due definition cannot be decoded. The poll skips it and the leader takes it out of the fire index; the definition stays, the rest of the batch fires ([fire index](../guides/scheduling-and-retries.md#fire-index)). Fields `schedule_name`, `error_type`, `error`. |
| `scheduler_fire_unroutable` | `swarmq.scheduler.loop` | WARNING | A cron fire was returned because its queue does not exist; the fire is lost and committed, `SCHEDULER_FIRE_UNROUTABLE` counts it. At most once per cron and 10 minutes ([missing queues](../guides/scheduling-and-retries.md#missing-queues)). Fields `schedule_name`, `queue`, `task_name`, `error_type`, `error`, `hint`. |
| `scheduler_fire_unroutable_foreign` | `swarmq.scheduler.loop` | WARNING | Added to `scheduler_fire_unroutable` when the cron is reported by a worker whose task the leader does not know, usually an app on another RabbitMQ vhost sharing the Valkey; such apps need their own `SWARMQ_NAMESPACE` ([namespace](configuration.md#namespace-several-apps-on-one-valkey)). Fields `schedule_name`, `queue`, `task_name`, `hint`. |
| `scheduler_publish_failures_stepdown` | `swarmq.scheduler.loop` | WARNING | The scheduler leader releases the leader lock after repeated failed publishes (`SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S`) and does not apply again for `cooldown_s` (2 × lock TTL); a worker with a working broker takes over. Followed by `SCHEDULER_STANDBY`. Fields `error_type`, `error`, `cooldown_s`, `hint`. |
| `scheduler_cron_held_unowned` | `swarmq.scheduler.loop` | WARNING | The leader starts holding a static cron that no live worker reports and that it does not register itself; the fire moves to `now + SWARMQ_SCHEDULER_SEEN_INTERVAL_S` on every poll without publishing. Once per cron and hold ([held crons](../guides/scheduling-and-retries.md#crons-nobody-reports-are-held)). Fields `schedule_name`, `task_name`, `queue`, `hint`. |
| `scheduler_cron_unowned_released` | `swarmq.scheduler.loop` | INFO | A held cron is reported again (it fires on its next occurrence) or was removed. Fields `schedule_name`, `task_name`. |
| `scheduler_unowned_check_skipped` | `swarmq.scheduler.loop` | WARNING | The leader fires without the cron reports of its current term (none loaded within 5 s after the takeover, or reading them failed): removed crons are not held until the next adoption tick succeeds. Once per term. Field `hint`. |
| `scheduler_known_crons_unavailable` | `swarmq.scheduler.loop` | WARNING | The set of ever-reported cron names (`[<namespace>:]sq:{sched}:report:known`) could not be read on an adoption tick (store without the method, or an error); nothing is held until the next tick. Fields `error_type`, `error`. |
| `schedule_known_forget_failed` | `swarmq.scheduler.definition` | WARNING | `swarmq schedule remove --permanent` could not take the name out of `[<namespace>:]sq:{sched}:report:known` (best effort); a same-named static cron fires only after a worker reports it again. Fields `schedule_name`, `error_type`, `error`. |
| `scheduler_oneshot_unroutable` | `swarmq.scheduler.loop` | WARNING | A deferred task's queue does not exist; it is retried every 60 s. At most once per entry and 10 minutes. Fields `schedule_name`, `task_id`, `task_name`, `queue`, `unroutable_since`, `hint`. |
| `scheduler_oneshot_unroutable_dropped` | `swarmq.scheduler.loop` | ERROR | A deferred task stayed unroutable for `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` and was dropped with an error result. Fields `schedule_name`, `task_id`, `task_name`, `queue`, `unroutable_since`, `max_s`, `hint`. |
| `scheduler_unroutable_result_store_failed`, `scheduler_unroutable_unique_release_failed` | `swarmq.scheduler.loop` | WARNING | Storing the error result or releasing the `unique_for` flag of a dropped deferred task failed (best effort). Fields `task_id` (and `dedup_hash`), `error_type`, `error`. |
| `scheduler_unique_release_failed` | `swarmq.scheduler.loop` | WARNING | Releasing the `unique_for` flag the leader had claimed for a fire raised (best effort): after a failed publish, or after a claim that ran into a timeout and may have set the flag (only under a fresh `task_id`: crons, oneshots without a stable ID). The flag runs out after `unique_for`, and until then the fire's retry counts as a duplicate. If the backend is down, the Valkey backend logs `unique_release_backend_error` instead. Fields `schedule_name`, `task_id`, `dedup_hash`, `error_type`, `error` ([unique crons](../guides/locking-and-dedup.md#unique-crons)). |
| `scheduler_park_failed` | `swarmq.scheduler.fire_plan`, `swarmq.scheduler.definition` | WARNING | The leader could not take an entry without a calculable next fire, or with an undecodable definition, out of the fire index; only this entry is affected, the rest of the batch fires, and the next tick tries again. Fields `schedule_name`, `error_type`, `error`. |
| `scheduler_park_owner_mismatch` | `swarmq.scheduler.fire_plan` | WARNING | Taking such an entry out of the fire index was rejected because this worker no longer holds the leader lock; the new leader handles the entry. Field `schedule_name`. |
| `scheduler_cron_report_invalid` | `swarmq.scheduler.sync` | ERROR | The leader rejected the newest live report of a cron (outside the crontab(5) [positive list](configuration.md#scheduling-cron), unknown timezone, no next fire time, not a static definition); the stored definition stays. Logged once per schedule, report and leadership term (since 0.13.0; before, on every adoption tick). Fields `schedule_name`, `fingerprint`, `error_type`, `error`. |
| `scheduler_cron_conflict` | `swarmq.scheduler.sync` | WARNING | Several definitions of one cron have been live for more than 10 minutes; on every adoption tick while it lasts. Fields `schedule_name`, `fingerprints`, `chosen`, `hint`. See [conflicts](../guides/scheduling-and-retries.md#conflicts). |
| `scheduler_cron_report_failed` | `swarmq.scheduler.loop` | WARNING | A worker could not report its crons or its version; the next tick reports again. Fields `error_type`, `error`. |
| `scheduler_queue_report_failed` | `swarmq.scheduler.loop` | WARNING | A worker could not report the queues it consumes (`[<namespace>:]sq:{sched}:report:queues`); the next tick reports again. Fields `error_type`, `error`. |
| `scheduler_queue_orphaned` | `swarmq.scheduler.loop` | WARNING | The leader found a reported queue with messages and no consumer that no live worker has reported for three report ticks (`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`); typical cause: a task moved to another queue or `--queues` changed. Once per queue and transition ([moving a task to another queue](../guides/defining-tasks.md#moving-a-task-to-another-queue)). Fields `queue`, `message_count`, `hint`. |
| `scheduler_queue_orphan_resolved` | `swarmq.scheduler.loop` | INFO | An orphaned queue is no longer orphaned: drained, consumed, deleted or reported again. Field `queue`. |
| `scheduler_queue_check_failed` | `swarmq.scheduler.loop` | WARNING | The leader could not check the reported queues (Valkey or broker error, or the 30 s time limit); the next adoption tick checks again. Fields `error_type`, `error`. |
| `scheduler_queue_check_unavailable` | `swarmq.scheduler.loop` | DEBUG | The store or broker lacks the methods for the check (third-party implementation); nothing is checked. Fields `error_type`, `error`. |
| `scheduler_cron_reports_read_failed`, `scheduler_cron_reports_prune_failed` | `swarmq.scheduler.sync` | WARNING | The leader could not read the cron reports (this tick adopts nothing) or delete reports older than 7 days. Fields `error_type`, `error`. |
| `scheduler_cron_adopt_failed` | `swarmq.scheduler.loop` | WARNING | An adoption tick of the leader failed unexpectedly; the next tick tries again. Fields `error_type`, `error`. |
| `scheduler_cron_pruned` | `swarmq.scheduler.prune` | INFO | The leader removed a static cron that no worker has reported for `SWARMQ_SCHEDULER_PRUNE_AFTER_S` ([removing crons](../guides/scheduling-and-retries.md#removing-crons-no-app-reports-anymore)). Field `schedule_name`. |
| `scheduler_cron_revived_disabled` | `swarmq.scheduler.sync` | WARNING | A cron that was disabled when it was pruned is reported again and created disabled. If the name now belongs to a different cron, `swarmq schedule remove --permanent <name>` clears the mark. Fields `schedule_name`, `hint`. |
| `scheduler_cron_prune_candidate` | `swarmq.scheduler.prune` | INFO | A static cron without any report (old entry, app before 0.10.0); never removed automatically, `swarmq schedule prune --orphans` removes it. Once per name and leader run. Fields `schedule_name`, `hint`. |
| `scheduler_cron_prune_skipped` | `swarmq.scheduler.prune` | INFO | With `SWARMQ_SCHEDULER_PRUNE=log`: this cron would have been removed. Once per name and leader run. Fields `schedule_name`, `hint`. |
| `scheduler_cron_prune_failed` | `swarmq.scheduler.prune` | WARNING | The leader could not read the definitions or remove a cron; the next tick tries again. Fields `error_type`, `error`, plus `schedule_name` when one cron failed. |
| `scheduler_disabled` | `swarmq.worker.lifecycle` | WARNING | This worker runs with `SWARMQ_SCHEDULER=off` (or `--no-scheduler`) and does not run for scheduler leader. Once per worker process at start: N times with `--processes N` and again after every recycle. Field `hint` (crons, `delay`/`eta` tasks and oneshots fire only while another worker on the same Valkey and namespace is leader). See [turning the scheduler off](../guides/scheduling-and-retries.md#turning-the-scheduler-off-on-single-workers). |
| `scheduler_leader_outdated` | `swarmq.scheduler.leader_version` | WARNING | The scheduler leader runs a swarmq below the minimum leader version (currently 0.12) and lacks scheduler fixes this worker relies on; once per leader, after two election ticks. Fields `leader`, `leader_version` (`None` = before 0.10.0), `version` (this worker's), `hint`. See [leader version](../guides/scheduling-and-retries.md#leader-version). |
| `scheduler_leader_version_write_failed`, `scheduler_leader_version_check_failed` | `swarmq.scheduler.leader_version` | WARNING | The leader could not write its version next to the lock (it stays leader; the next renewal writes again), or a non-leader could not read it. Fields `error_type`, `error`. |
| `dedup_detected` | `swarmq.client.schedule`, `swarmq.scheduler.loop` | INFO | |
| `dedup_rollback_failed` | `swarmq.client.schedule` | WARNING | `schedule()`/`schedule_many()` could not release a `unique_for` flag it had claimed (best effort): after a failed publish, or after a claim that ran into a timeout and may have set the flag. The original error still reaches the caller; the flag runs out after `unique_for`, and until then a retry counts as a duplicate. If the backend is down, the Valkey backend logs `unique_release_backend_error` instead. Fields `dedup_hash`, `task_id`, `error_type`, `error`. |
| `unique_release_backend_error` | `swarmq.backend.valkey.results` | WARNING | Releasing a `unique_for` flag failed because Valkey was unreachable or timed out; the release reports `False` and the flag runs out after its TTL. Covers the worker's release, the leader's and the client's rollback. Fields `dedup_hash`, `task_id`, `error_type`, `error`. |
| `client_task_defaults_unresolved` | `swarmq.client.schedule_resolve` | WARNING | A client without a registry sends a task whose `TaskInfo` it does not know: `unique_for`, `priority` and the delivery mode are not applied. Once per task name and client instance, at most 1000 names. Fields `task_name`, `hint`. See [client-only mode](api.md#schedule). |
| `http_server_started`, `http_server_stopped` | `swarmq._http` | INFO | Field `server` (`health_server`/`metrics_server`); `host`, `port` on start. |
| `http_server_request_failed` | `swarmq._http` | ERROR | With traceback; field `server`. |
| `check_backend_failed`, `check_broker_failed` | `swarmq.health` | WARNING | Readiness check failed. The `consumers` check logs nothing of its own; its reason is in the `/ready` body and in the consumer watch events above. |

## Python API

All in `swarmq.logging`:

| Name | Purpose |
|---|---|
| `get_logger(name)` | A SwarmQ-style logger (structlog `BoundLogger`) that hands events to stdlib `logging` under `name`; unaffected by `structlog.configure`. |
| `build_formatter(json, colors)` | The formatter described [above](#build_formatter). |
| `configure_logging(settings)` | Sets up logging for a process the way `swarmq worker` does; `settings` is a `swarmq.logging_settings.LoggingSettings` (see below). Meant for entry points, never called by the library. A second call in the same process is a no-op. Unless `configure` is `False`, it lifts an active [opt-in log queue](#opt-in-log-queue); that flush is bounded by `settings.drain_timeout`. |
| `shutdown_logging(timeout=None)` | Flushes the log queues and stops the listener threads, at most `timeout` seconds (default: the [`log_drain_timeout`](#bounded-flushing) set by `configure_logging()` or an opt-in `SwarmQ`, otherwise 5); records not written by then count as dropped. A later `configure_logging()` rebuilds. Runs automatically at exit. |
| `load_log_config(path)` | Loads and checks a dictConfig file (JSON, or YAML with PyYAML) and returns it as a JSON string, with `disable_existing_loggers` set to `false` unless the file sets it. Does not apply anything; raises `LoggingConfigError`. |
| `dropped_log_records()` | Number of log records dropped in this process because the queue was full; the value behind `swarmq_log_records_dropped_total`. |
| `LoggingConfigError` | Invalid logging config file; see the [error catalog](errors.md#loggingconfigerror). |

`LoggingSettings` is a frozen dataclass; every field has a default:

| Field | Default | Meaning |
|---|---|---|
| `level` | `"INFO"` | Root level. |
| `level_explicit` | `False` | Set `level` on the root logger also after a dictConfig file (what an explicit `--log-level` does). |
| `format` | `"human"` | `"human"` or `"json"` for the built-in setup. |
| `log_config` | `None` | Path of a dictConfig file; replaces the built-in setup. |
| `log_config_json` | `None` | The file's content as returned by `load_log_config()`. When set, the file is not read again. |
| `configure` | `True` | `False` makes `configure_logging()` do nothing. |
| `logger_levels` | `()` | `(("aio_pika", "WARNING"), ...)` pairs, applied last. |
| `queue_size` | `10000` | Capacity of the log queue. |
| `lifecycle_level` | `"DEBUG"` | Level of `task_received` and `task_started`. |
| `drain_timeout` | `5.0` | Upper bound in seconds for flushing the log queues ([bounded flushing](#bounded-flushing)); also applies when `configure` is `False`. |

```python
from swarmq.logging import configure_logging, load_log_config
from swarmq.logging_settings import LoggingSettings

path = "/etc/myapp/logging.json"
configure_logging(LoggingSettings(
    log_config=path,
    log_config_json=load_log_config(path),  # validate once, fail early
    logger_levels=(("aio_pika", "WARNING"),),
))
```

Deprecated: `setup_logging(log_level, log_json)` (rebuilds the built-in
setup on every call) and `flush_logging()` (an alias of
`shutdown_logging()`).

## See also

- [CLI reference](cli.md#swarmq-worker) — every `swarmq worker` flag
- [Configuration](configuration.md#logging) — the logging env vars and
  `SwarmQConfig` fields
- [Error catalog](errors.md#loggingconfigerror) — `LoggingConfigError`
- [Changelog](changelog.md#080-2026-10-06) — migration from the previous
  behaviour
