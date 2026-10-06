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
  background thread named `swarmq-log-listener` hands every record to
  your **root** handlers at output time. Handler levels apply, handlers
  added later (Sentry, OpenTelemetry, uvicorn) receive the records, and
  without handlers `lastResort` still applies.
- The caller's context variables are available to your handlers'
  filters, as if they ran in the caller.
- `log_level` sets the level of the `swarmq` logger. `log_json` only
  selects the format of the [`log_records_dropped`](#non-blocking-output-and-dropped-records)
  notice. `log_queue_size` (default `10000`) bounds the queue.
- Handlers you attach directly to the `swarmq` logger stay synchronous.
  Sentry's `LoggingIntegration` still runs in the caller.
- The thread starts with the first record, not in `SwarmQ(...)`. The
  last `SwarmQ` instance to leave `async with` restores the `swarmq`
  logger (handler, `propagate`, level).
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
  event's fields. `logger` is new in this release.
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
  flushing the queue, when the process exits.
- An explicit `--log-level` additionally sets the root level after the
  file is applied; `--logger-level` entries apply after it too.
- The supervisor resolves the path, loads and validates the file once,
  and passes the validated configuration to its children. The children
  never read the file, so neither a different working directory nor a
  later edit of the file affects them.
- On a reload (`SIGHUP`, `--reload`) the running supervisor validates
  the file again **before** it stops a worker. If it is invalid now, the
  reload is aborted with `log_config_reload_failed`; workers and logging
  keep running unchanged.
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

### Leaving logging to your app

`--no-configure-logging` (or `SWARMQ_CONFIGURE_LOGGING=false`) makes
SwarmQ set up nothing in any of its processes — no handlers, no levels,
no structlog configuration. Your task module or `--configure` hook is
then responsible; if it does nothing, only WARNING and above reach
`lastResort`. In this mode the [opt-in log queue](#opt-in-log-queue)
applies.

### When a task module configures logging

A worker subprocess configures logging **before** it imports your task
module. If the module (or a `--configure` hook) then changes logging,
your app wins:

- **It replaces the root handlers** (for example with its own
  `dictConfig`): SwarmQ steps back and keeps its own events off the
  event loop by routing them through its queue to your handlers. It
  logs `logging_reconfigured_by_app` once. SwarmQ loggers that your
  `dictConfig` disabled are re-enabled; the number is in the field
  `reenabled_loggers`.
- **It adds handlers** next to SwarmQ's: both write, SwarmQ removes
  nothing and logs `logging_handlers_added_by_app` as WARNING with the
  handler types.
- **`logging.basicConfig(...)` in a task module has no effect** under
  `swarmq worker`, because the root logger already has a handler.

In the CPU pool the same rule applies to compute children: handlers
your module sets up at import time are inherited from the forkserver
and win; `logging_reconfigured_by_app` then appears once per pool.

To own logging completely, use `--log-config` or
`--no-configure-logging` instead of configuring logging at import time.

### Embedded CPU pool

When an embedded app runs a CPU pool (`pool="cpu"`), SwarmQ starts the
compute children itself. They log to **stderr** according to the
`SwarmQConfig` fields `log_level`, `log_json` (default `true`),
`log_config`, `configure_logging`, `logger_levels` and `log_queue_size`
— not through your app's handlers.

## Non-blocking output and dropped records

The code that logs only builds the record and puts it into a bounded
queue. Rendering and I/O happen in the listener thread. When the queue
is full, SwarmQ drops records instead of blocking:

- From 90 % fill level on, only records below WARNING are dropped;
  WARNING and above use the remaining 10 % and are lost only when the
  queue is completely full.
- Drops are counted per process. At most once per second a notice goes
  straight to stderr: in JSON mode a JSON line
  `{"event": "log_records_dropped", "count": N, "level": "warning", "logger": "swarmq.logging", "timestamp": …}`,
  in human mode a text line `swarmq: logging queue full — N record(s) dropped so far`.
- The worker exports the counter as the Prometheus metric
  `swarmq_log_records_dropped_total`. It covers the worker process only;
  drops in compute children are visible as their notice line.

The queue is flushed on every orderly exit path. A process killed with
`SIGKILL` loses the records still in its queue.

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
| `worker_died` | `swarmq.process_manager.supervisor` | INFO / WARNING | INFO for a clean exit (`0`) or an expected shutdown, otherwise WARNING. Fields `worker_id`, `pid`, `exitcode`, `expected`, `slot_state`. |
| `worker_config_error` | `swarmq.cli`, `swarmq.process_manager.entry` | ERROR | Configuration error, exit `78`. Field `source` (`supervisor`/`worker`). |
| `worker_connection_error` | `swarmq.cli` | ERROR | Broker or backend unreachable at start, exit `1`. Traceback only with `--log-level DEBUG`. |
| `worker_recycle_ack_timeout` | `swarmq.process_manager.entry` | ERROR | A recycling worker got no acknowledgement and exits `75`. |
| `log_config_reload_failed` | `swarmq.process_manager.run` | ERROR | Reload aborted; fields `path`, `error_type`, `error`, `hint`. |
| `logging_reconfigured_by_app` | `swarmq.logging`, `swarmq.worker.compute.pool` | INFO | Your app replaced the root handlers; field `reenabled_loggers`. |
| `logging_handlers_added_by_app` | `swarmq.logging` | WARNING | Field `handler_types`. |
| `log_records_dropped` | — (direct stderr line) | warning | Field `count`; at most once per second. |
| `log_record_unformattable` | the record's logger | the record's level | Field `error`. |
| `task_retrying` | `swarmq.worker.retry` | INFO | Fields `retry_count`, `max_retries`, `delay_ms`. |
| `task_sent_to_dlq` | `swarmq.worker.retry` | ERROR | |
| `dedup_detected` | `swarmq.client.schedule`, `swarmq.scheduler.loop` | INFO | |
| `dedup_rollback_failed` | `swarmq.client.schedule` | WARNING | |
| `http_server_started`, `http_server_stopped` | `swarmq._http` | INFO | Field `server` (`health_server`/`metrics_server`); `host`, `port` on start. |
| `http_server_request_failed` | `swarmq._http` | ERROR | With traceback; field `server`. |
| `check_backend_failed`, `check_broker_failed` | `swarmq.health` | WARNING | Readiness check failed. |

## Python API

All in `swarmq.logging`:

| Name | Purpose |
|---|---|
| `get_logger(name)` | A SwarmQ-style logger (structlog `BoundLogger`) that hands events to stdlib `logging` under `name`; unaffected by `structlog.configure`. |
| `build_formatter(json, colors)` | The formatter described [above](#build_formatter). |
| `configure_logging(settings)` | Sets up logging for a process the way `swarmq worker` does; `settings` is a `swarmq.logging_settings.LoggingSettings`. Meant for entry points, never called by the library. A second call in the same process is a no-op. |
| `shutdown_logging()` | Flushes the log queues and stops the listener threads; a later `configure_logging()` rebuilds. Runs automatically at exit. |
| `load_log_config(path)` | Loads and checks a dictConfig file (JSON, or YAML with PyYAML); raises `LoggingConfigError`. |
| `LoggingConfigError` | Invalid logging config file; see the [error catalog](errors.md#loggingconfigerror). |

Deprecated: `setup_logging(log_level, log_json)` (rebuilds the built-in
setup on every call) and `flush_logging()` (an alias of
`shutdown_logging()`).

## See also

- [CLI reference](cli.md#swarmq-worker) — every `swarmq worker` flag
- [Configuration](configuration.md#logging) — the logging env vars and
  `SwarmQConfig` fields
- [Error catalog](errors.md#loggingconfigerror) — `LoggingConfigError`
- [Changelog](changelog.md#unreleased) — migration from the previous
  behaviour
