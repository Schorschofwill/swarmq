# Configuration

SwarmQ has two configuration surfaces: **environment variables** that
the worker reads at startup (and the CLI also honours), and
**`TaskInfo` fields** declared per task class. CLI flags take
precedence over env vars; env vars take precedence over
`SwarmQConfig` defaults.

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
| `SWARMQ_CONCURRENCY` | `10` | Maximum tasks in flight per worker process. |
| `SWARMQ_LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `SWARMQ_LOG_JSON` | `true` | `true`/`1`/`yes` for structured JSON logs, anything else for human-formatted output. |
| `SWARMQ_RESULT_TTL` | `86400` | Seconds that stored results live in Valkey. |
| `SWARMQ_RETRY_MAX_DELAY` | `3600.0` | Cap on retry backoff in seconds. |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` | Seconds to let in-flight tasks finish on SIGTERM before hard-killing. |
| `SWARMQ_PUBLISH_TIMEOUT_S` | `120.0` | Upper bound on a complete publish, including internal retries. A publish that exceeds it fails with a broker connection error instead of waiting indefinitely for a confirm that a silently dead transport will never deliver. During graceful shutdown the shorter of this value and the remaining shutdown grace period applies. |
| `SWARMQ_QUEUE_TYPE` | `quorum` | `quorum` (recommended) or `classic` RabbitMQ queue type. |
| `SWARMQ_CANCEL_POLL_INTERVAL` | `1.0` | Seconds between in-task cancel-flag checks. |
| `SWARMQ_BACKEND_OP_TIMEOUT` | `5.0` | Upper bound on individual Valkey ops in the cancel / dedup pipeline. |
| `SWARMQ_BURST_MODE` | `false` | If `true`, worker exits after `SWARMQ_BURST_IDLE_TIMEOUT` seconds of queue silence. |
| `SWARMQ_BURST_IDLE_TIMEOUT` | `5.0` | Seconds of idle needed before burst exits. |
| `SWARMQ_PRIORITIES` | `false` | If `true`, auto-declare and consume the `.high`/`.normal`/`.low` triplet for every queue. |

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
| `SWARMQ_METRICS_PORT` | *unset* | If set, serve `/metrics` on this port. Requires `pip install swarmq[metrics]`. Must be `1..65535`. |
| `SWARMQ_METRICS_HOST` | `127.0.0.1` | Bind address for the metrics server. |
| `SWARMQ_HEALTH_PORT` | *unset* | If set, serve `/health` and `/ready` on this port. Must be `1..65535`. |
| `SWARMQ_HEALTH_HOST` | `127.0.0.1` | Bind address for the health server. |

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
    log_level="DEBUG",
    log_json=True,
    result_ttl=3600,
    shutdown_timeout=60,
    burst_mode=False,
    priorities=True,
)
```

Field names match the env vars without the `SWARMQ_` prefix and in
lowercase. Env vars always win when both are set — pass kwargs only
when you need a non-default that you do not want in the shell
environment.

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
- [API reference](api.md) — `SwarmQ`, `TaskInfo`, and `Task` signatures
- [Defining tasks](../guides/defining-tasks.md) — worked examples of
  each `TaskInfo` field
