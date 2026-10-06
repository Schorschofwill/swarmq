# Configuration

SwarmQ has two configuration surfaces: **environment variables** that
the worker reads at startup (and the CLI also honours), and
**`TaskInfo` fields** declared per task class.

Precedence depends on how SwarmQ runs:

- **`swarmq worker`**: CLI flag, then env var, then default, for every
  worker option. The supervisor resolves the values once and hands them
  to its worker subprocesses and compute children, which do not read
  the environment again; `swarmq worker --check` validates the same
  resolved values.
- **Embedded `SwarmQ(...)`**: `SwarmQConfig` keyword arguments first,
  then `SWARMQ_*` env vars override them; defaults fill the rest.

## Environment variables

### Required

| Variable | Purpose |
|---|---|
| `SWARMQ_BROKER_URL` | AMQP URL for RabbitMQ, e.g. `amqp://guest:guest@localhost:5672/`. |
| `SWARMQ_BACKEND_URL` | Valkey/Redis URL for results and shared state, e.g. `valkey://localhost:6379/0`. |

The CLI prints a descriptive error if either is missing; see the
[error catalog](errors.md#configurationerror).

### Worker behaviour

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_QUEUES` | `default` | Comma-separated queue names the worker consumes. |
| `SWARMQ_CONCURRENCY` | `10` (io) / assigned CPUs (cpu) | Maximum tasks in flight per worker process. With `SWARMQ_POOL=cpu` the number of compute slots; see [`--concurrency`](cli.md#options). |
| `SWARMQ_RESULT_TTL` | `86400` | Seconds that stored results live in Valkey. |
| `SWARMQ_RETRY_MAX_DELAY` | `3600.0` | Cap on retry backoff in seconds. |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` | Seconds to let in-flight tasks finish on SIGTERM before hard-killing. |
| `SWARMQ_PUBLISH_TIMEOUT_S` | `120.0` | Upper bound on a complete publish, including internal retries. A publish that exceeds it fails with a broker connection error instead of waiting indefinitely for a confirm that a silently dead transport will never deliver. During graceful shutdown the shorter of this value and the remaining shutdown grace period applies. |
| `SWARMQ_DELIVERY_LIMIT` | `20` | Delivery budget for quorum queues (`x-delivery-limit`); ignored for classic queues. Once a message has been delivered this many times, the broker itself dead-letters it — the safety net that stops a task which reliably kills the process (SIGSEGV/OOM) from being redelivered forever without ever reaching `max_retries`. The default covers three to four task-level retries per delivery plus headroom for broker hiccups. That headroom is narrower since the publish deadline: a publish that breaks its deadline inside the worker redelivers the in-flight message instead of acking it, and each round costs one delivery. Raise it if you must ride out longer broker outages; lower it to force a tighter delivery window. |
| `SWARMQ_QUEUE_TYPE` | `quorum` | `quorum` (recommended) or `classic` RabbitMQ queue type. Only quorum queues carry `x-delivery-limit`; without it a message that keeps hanging workers is redelivered without bound (the worker warns at startup). |
| `SWARMQ_POOL` | `io` | `io` or `cpu` — see the [CPU mode guide](../guides/cpu-mode.md). |
| `SWARMQ_RECYCLE_RSS_MB` | unset (`250` in CPU mode) | Recycle a process after a task once its RSS exceeds this many MB. IO worker: opt-in self-recycling; CPU mode: threshold for reused DI children. |
| `SWARMQ_RECYCLE_MAX_TASKS` | unset (`1000` in CPU mode) | Recycle a process after this many tasks (second line behind the RSS threshold). |
| `SWARMQ_HEARTBEAT_INTERVAL_S` | `5` | How often the worker writes its liveness timestamp for the supervisor. |
| `SWARMQ_HEARTBEAT_TIMEOUT_S` | `60` | After this long without a heartbeat the supervisor replaces the worker (SIGTERM, then SIGKILL). Must exceed the interval and every legitimate loop stall; keep it below RabbitMQ's heartbeat detection. |
| `SWARMQ_CONSUMER_TIMEOUT_S` | `1800` | The broker's `consumer_timeout`; the worker warns when a task's `hard_timeout` exceeds it. |
| `SWARMQ_MALLOC_TRIM_THRESHOLD` | `131072` | `MALLOC_TRIM_THRESHOLD_` for compute children (glibc growth). |
| `SWARMQ_MALLOC_ARENA_MAX` | `2` | `MALLOC_ARENA_MAX` for compute children. |
| `SWARMQ_COMPUTE_THREAD_LIMIT` | `1` | Threads per compute child (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`, `MAGICK_THREAD_LIMIT`). |
| `SWARMQ_CANCEL_POLL_INTERVAL` | `1.0` | Seconds between in-task cancel-flag checks. |
| `SWARMQ_BACKEND_OP_TIMEOUT` | `5.0` | Upper bound on individual Valkey ops in the cancel / dedup pipeline. |
| `SWARMQ_BURST_MODE` | `false` | If `true`, worker exits after `SWARMQ_BURST_IDLE_TIMEOUT` seconds of queue silence. |
| `SWARMQ_BURST_IDLE_TIMEOUT` | `5.0` | Seconds of idle needed before burst exits. |
| `SWARMQ_PRIORITIES` | `false` | If `true`, auto-declare and consume the `.high`/`.normal`/`.low` triplet for every queue. |
| `SWARMQ_MAX_MESSAGE_BODY_BYTES` | `16777216` | Upper bound for an incoming task message body (16 MiB). Must be `> 0`. A larger body is not decoded: the worker logs `message_body_too_large` and routes the message to the DLQ (`TASK_REJECTED` with reason `body_too_large`). |

### Logging

These variables steer the logging that `swarmq worker` sets up in its
own processes. An app that embeds `SwarmQ(...)` configures logging
itself; there the variables only affect the
[opt-in log queue](logging.md#opt-in-log-queue), the lifecycle level of
an embedded worker and the compute children of an embedded CPU pool.
Details, recipes and the event catalog:
[Logging reference](logging.md).

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root level of SwarmQ's processes; with `SWARMQ_LOG_QUEUE` the level of the `swarmq` logger. |
| `SWARMQ_LOG_FORMAT` | `human` (CLI) | `human` or `json`. Wins over `SWARMQ_LOG_JSON`. |
| `SWARMQ_LOG_JSON` | `true` | Alias for the format: `true`/`1`/`yes` means `json`, anything else `human`. The default `true` is the `SwarmQConfig.log_json` default; `swarmq worker` defaults to `human`. |
| `SWARMQ_LOG_CONFIG` | unset | Path of a `dictConfig` file (JSON; YAML with PyYAML) used instead of the built-in setup. Not combinable with an explicit format or `SWARMQ_CONFIGURE_LOGGING=false`. |
| `SWARMQ_CONFIGURE_LOGGING` | `true` | `false` leaves logging in every SwarmQ process to your app. |
| `SWARMQ_LOGGER_LEVELS` | unset | Comma-separated `NAME=LEVEL` pairs, e.g. `aio_pika=WARNING,valkey=ERROR`. |
| `SWARMQ_LOG_QUEUE_SIZE` | `10000` | Capacity of the in-process log queue. |
| `SWARMQ_LOG_LIFECYCLE_LEVEL` | `DEBUG` | Level of `task_received` and `task_started`. |
| `SWARMQ_LOG_DRAIN_TIMEOUT` | `5` | Upper bound in seconds for every flush of the log queues (at exit, before a reload, before each compute result, in `SwarmQ.__aexit__`); records not written in time count as dropped. Greater than 0 and at most `threading.TIMEOUT_MAX`. See [bounded flushing](logging.md#bounded-flushing). |
| `SWARMQ_LOG_QUEUE` | `false` | Embedded apps: route `swarmq.*` events through a SwarmQ background thread to your root handlers ([opt-in log queue](logging.md#opt-in-log-queue)). |

The supervisor of `swarmq worker` resolves these values once (CLI flag
before env var) and hands the result to its worker subprocesses and
compute children, which do not read the environment again. The same
CLI > env > default order applies to every other worker option with a
flag (see [CLI reference](cli.md)). Invalid values end `swarmq worker`
with exit `78`.

### Scheduler

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_SCHEDULER_POLL_INTERVAL` | `1.0` | Seconds between scheduler tick polls. |
| `SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL` | `10.0` | Seconds between leader heartbeat renewals. |
| `SWARMQ_SCHEDULER_LOCK_TTL` | `30.0` | Leader lock TTL; must be `≥ 2 × heartbeat_interval`. |
| `SWARMQ_SCHEDULER_DEDUP_TTL` | `max(60, retry_max_delay + 300)` | Dedup TTL for scheduler-originated dispatches. |
| `SWARMQ_SCHEDULER_INFLIGHT_TTL` | `300.0` | Pre-run marker TTL while a scheduled task is executing. |

### Observability

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_METRICS_PORT` | *unset* | **Base port** for `/metrics`. The worker in slot `s` serves on `SWARMQ_METRICS_PORT + s`; an embedded worker without a slot serves on the base port itself. Requires `pip install swarmq[metrics]`. Must be `1..65535`. |
| `SWARMQ_METRICS_HOST` | `127.0.0.1` | Bind address for the metrics server. Set `0.0.0.0` when Prometheus scrapes from another host or container. |
| `SWARMQ_HEALTH_PORT` | *unset* | **Base port** for `/health` and `/ready`, same slot offset. Must be `1..65535`. |
| `SWARMQ_HEALTH_HOST` | `127.0.0.1` | Bind address for the health server. |
| `SWARMQ_METRICS_SAMPLE_INTERVAL` | `10.0` | Seconds between samples of the `swarmq_queue_depth`, `swarmq_dlq_messages` and `swarmq_worker_uptime_seconds` gauges; the same tick dispatches `WORKER_HEARTBEAT`. Must be `> 0`. The sampler only runs when metrics are enabled. |
| `SWARMQ_WORKER_ID` | hostname | Base of the worker identity that appears as `worker_id` in signals, metric labels and the log context. See [worker identity](#worker-identity). |

#### Metric catalog

Every worker with metrics enabled (`--metrics-port`/`SWARMQ_METRICS_PORT`,
`pip install 'swarmq[metrics]'`) exposes these series on `/metrics`:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `swarmq_tasks_total` | counter | `task_name`, `queue`, `outcome` | Tasks by outcome: `completed`, `failed`, `cancelled`, `rate_limited`, `duplicate`, `expired`, `dlq`. `failed` counts every failed attempt, `dlq` the terminal dead-lettering. |
| `swarmq_task_duration_seconds` | histogram | `task_name`, `queue` | Duration of `run()` for completed tasks. Buckets from 5 ms to 300 s. |
| `swarmq_tasks_active` | gauge | `queue` | Tasks currently in flight. |
| `swarmq_retries_total` | counter | `task_name`, `queue` | Retries scheduled. |
| `swarmq_tasks_rejected_total` | counter | `queue`, `reason` | Messages handed to the broker DLX without diagnostic headers (`TASK_REJECTED`), e.g. `body_too_large`, `undecodable`, `unregistered_task`. |
| `swarmq_recyclings_total` | counter | `kind`, `reason` | Process recyclings; `kind` is `worker` or `compute_child`. |
| `swarmq_queue_depth` | gauge | `queue`, `kind` | Broker-reported message count (`kind="pending"`), sampled every `SWARMQ_METRICS_SAMPLE_INTERVAL`. |
| `swarmq_dlq_messages` | gauge | `queue` | Messages in `{queue}.XQ`, sampled. |
| `swarmq_worker_uptime_seconds` | gauge | `worker_id` | Seconds since the worker started, sampled. |
| `swarmq_scheduler_leader` | gauge | `worker_id` | `1` while this worker holds the scheduler leader lock, else `0`. |
| `swarmq_log_records_dropped_total` | counter | none | Log records dropped because the log queue was full (see [Logging](logging.md#non-blocking-output-and-dropped-records)). |

`task_name` and `queue` are taken verbatim from the message; keep task
names bounded, otherwise every distinct value creates its own series.

#### Base ports and slots

The supervisor runs its workers in slots `0..N-1`, where `N` is
`--processes` or the upper bound of `--autoscale`. Each slot binds its
own observability ports, derived from the base port:

| Slot | `/metrics` with `SWARMQ_METRICS_PORT=9000` | `/health` with `SWARMQ_HEALTH_PORT=9100` |
|---|---|---|
| `0` | `9000` | `9100` |
| `1` | `9001` | `9101` |
| `2` | `9002` | `9102` |

A slot keeps its ports for the life of the run: a worker that crashes,
hangs or recycles itself is replaced in the same slot, and the
replacement re-binds the same port (with retries while the predecessor
releases it). Scrape targets therefore stay stable across restarts.

Two configurations are refused at startup with a `ConfigurationError`
(exit `78`), and `swarmq worker --check` reports the same:

- a range `[base, base + N)` that runs past `65535` — lower the base
  port or run fewer processes;
- metrics and health ranges that overlap — move one base port at least
  `N` ports away.

The `--check` payload lists the exact ports as `metrics_ports` and
`health_ports`, so an operator does not have to compute them.

#### Prometheus example

One target per slot; the bind host must be reachable from Prometheus
(`SWARMQ_METRICS_HOST=0.0.0.0` when it scrapes over the network):

```yaml
scrape_configs:
  - job_name: swarmq
    static_configs:
      - targets:
          - worker-host:9000   # slot 0
          - worker-host:9001   # slot 1
          - worker-host:9002   # slot 2
```

With `--autoscale MIN,MAX` list `MAX` targets; slots that are not
running are scraped as `down`, which is expected while the pool is
scaled down.

### Worker identity

Every worker carries a stable identity in the `worker_id` field of
the `WORKER_*`, `SCHEDULER_*` and `WORKER_RECYCLING` signals, in the
labels of `swarmq_worker_uptime_seconds` and `swarmq_scheduler_leader`,
and in the structured log context:

| Runs as | `worker_id` | Example |
|---|---|---|
| Under the supervisor (`swarmq worker`) | `<base>:<slot>` | `web-3:0`, `web-3:1` |
| Embedded (`await app.start_worker()`) | `<base>:p<pid>` | `web-3:p41872` |

`<base>` is `SWARMQ_WORKER_ID` (or the `worker_id=` kwarg of
`SwarmQConfig`); the default is the hostname. Compute children in CPU
mode report the identity of the loop process that owns them, so
`WORKER_RECYCLING` with `kind="compute_child"` carries the slot as
well. Read the slot in dashboards as the stable dimension: a slot's
uptime restarts when its worker is replaced, its identity does not.

All ports are validated at config-time: a bad value raises
`ConfigurationError` before the worker tries to bind.

### CLI output

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_DEFAULT_FORMAT` | *TTY-detect* | Overrides the default `--format` for output-shaped CLI subcommands (`schedule list/add/remove/status`, `dlq list/inspect/retry/purge`, `inspect`). Accepts `table` or `json`; any other value (typo, empty string) is ignored silently and the TTY-aware default applies (`table` on a terminal, `json` on a pipe). An explicit `--format` on the command line always wins. |

## `SwarmQConfig` kwargs

Programmatic equivalent of the env vars — pass any of these as
keyword arguments to `SwarmQ(broker, backend, **kwargs)`:

```python
app = SwarmQ(
    broker, backend,
    queues=["emails", "reports"],
    concurrency=50,
    result_ttl=3600,
    shutdown_timeout=60,
    burst_mode=False,
    priorities=True,
)
```

Field names match the env vars without the `SWARMQ_` prefix and in
lowercase. In an embedded app env vars always win when both are set
(`swarmq worker` resolves CLI flags before env vars instead) — pass kwargs only
when you need a non-default that you do not want in the shell
environment. One exception to the naming rule: `SWARMQ_LOG_FORMAT` sets
the field `log_json`. `worker_id=` sets the base of the
[worker identity](#worker-identity); an embedded worker has no slot,
so `metrics_port` and `health_port` are bound as given.

`SwarmQ(...)` does not configure logging. The logging fields
(`log_level`, `log_json`, `log_config`, `configure_logging`,
`logger_levels`, `log_queue_size`, `log_lifecycle_level`, `log_queue`,
`log_drain_timeout`) steer only the
[opt-in log queue](logging.md#opt-in-log-queue), the lifecycle level of
an embedded worker and the compute children of an embedded CPU pool.
With `log_queue=True`, `log_drain_timeout` bounds the flush in
`__aexit__` and at process exit; an invalid value raises
`ConfigurationError`.

## `TaskInfo` fields

Declared on each `Task` subclass via `task_info = TaskInfo(...)`.
The `TaskInfo` dataclass is **frozen**: fields are set once at class
definition time and cannot be mutated afterwards.

### Identity and routing

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | *required* | Canonical task name used by the client. Max 255 chars. |
| `queues` | `list[str]` | *required, ≥1* | Queue names the task is published to. The client uses `queues[0]` unless an explicit `queue=` is passed. |
| `priority` | `"high" \| "normal" \| "low"` | `"normal"` | With `--priorities`, routes to `{queue}.high`/`.low`. |
| `delivery` | `DeliveryMode` | `AT_LEAST_ONCE` | Alternative: `FIRE_AND_FORGET`. |

### Timeouts

| Field | Type | Default | Description |
|---|---|---|---|
| `soft_timeout` | `int \| None` | `None` | Seconds before the worker raises `SoftTimeoutError` inside the task. |
| `hard_timeout` | `int \| None` | `None` | Seconds before the worker cancels the task coroutine outright. Must be `> soft_timeout` when both set. Required when `lock` is set. |

### Retries

| Field | Type | Default | Description |
|---|---|---|---|
| `max_retries` | `int` | `0` | Number of retry attempts before DLQ. `0` means no retries. |
| `retry_backoff` | `str` | `"exponential"` | One of `"exponential"`, `"linear"`, `"fixed"`. |
| `retry_base_delay` | `float` | `1.0` | Base delay (seconds) for the backoff formula. Must be `≥ 0`. |
| `retry_exceptions` | `tuple[type[BaseException], ...] \| None` | `None` | Whitelist — only these exceptions trigger a retry. `None` retries any exception. |

### Rate limiting

| Field | Type | Default | Description |
|---|---|---|---|
| `rate_limit` | `str \| None` | `None` | Celery-style string like `"100/m"`, `"10/s"`, `"1000/h"`. Validated at class definition. |

### Locking and concurrency

| Field | Type | Default | Description |
|---|---|---|---|
| `lock` | `str \| None` | `None` | Lock name; can be a format string like `"user-{user_id}"`. Requires `hard_timeout`. |
| `max_concurrency` | `int` | `1` | With `lock`: max concurrent executions. `1` is a mutex; `> 1` is a semaphore. Requires `lock`. |

### Scheduling (cron)

| Field | Type | Default | Description |
|---|---|---|---|
| `cron` | `str \| None` | `None` | Standard 5-field cron expression. Extended syntax (`L`, `W`, `#`) is rejected. |
| `catchup` | `bool` | `False` | Reserved for a future release. `True` raises `ValueError`. |

### Deduplication

| Field | Type | Default | Description |
|---|---|---|---|
| `unique_for` | `int \| None` | `None` | Seconds; enables dedup on `(task_name, args, kwargs)` within the TTL. `0` is rejected. |
| `unique_until` | `"start" \| "completion"` | `"completion"` | When to release the dedup flag. `"start"` releases at worker pickup; `"completion"` at terminal state. |

### Expiry

| Field | Type | Default | Description |
|---|---|---|---|
| `max_age` | `int \| None` | `None` | Seconds; worker routes the message to DLQ with `TASK_EXPIRED` if age at pickup exceeds this. Must be `> 0` when set. |

### Result storage

| Field | Type | Default | Description |
|---|---|---|---|
| `result_ttl` | `int \| None` | `None` | Per-task override for `SwarmQConfig.result_ttl`. |

## Validation behaviour

`TaskInfo.__post_init__` validates every field at class definition
time. A bad value raises `ValueError` on import of your task module,
not at first `schedule()` call. This is deliberate: a misconfigured
task should fail the worker's startup, not the user's request three
hours later.

Examples of errors surfaced at import:

- `queues` empty → `ValueError("queues must have at least 1 element")`.
- `rate_limit="100"` → `ValueError` from `parse_rate_limit` ("missing unit").
- `lock="user-{}"` with `hard_timeout=None` → `ValueError` naming the lock-TTL contract.
- `catchup=True` → `ValueError("catchup=True is reserved for a future release")`.
- `max_concurrency=2` without `lock` → `ValueError("max_concurrency requires lock to be set")`.
- `unique_for=0` → `ValueError("unique_for=0 is invalid; use None to disable dedup")`.

Fix the offending `TaskInfo` line and re-run — no worker reset
required because the error prevents the worker from starting in the
first place.

## See also

- [CLI reference](cli.md) — the flag equivalents for the worker env vars
- [Logging](logging.md) — logger names, fields, events and integration
  recipes
- [API reference](api.md) — `SwarmQ`, `TaskInfo`, and `Task` signatures
- [Defining tasks](../guides/defining-tasks.md) — worked examples of
  each `TaskInfo` field
