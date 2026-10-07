# Message formats

Canonical wire-format reference for every payload SwarmQ puts on
the broker, writes to Valkey, or dispatches as a signal. Types are
expressed as JSON/msgspec-compatible shapes; Python-specific
notation is avoided.

## Task message (RabbitMQ)

Encoded with msgspec JSON, published to the routing key that
matches the target queue.

```json
{
  "task": {
    "id": "01HV3W3YGBN4ZCZ45F2D7R9PB8",
    "queue": "emails",
    "name": "send_email"
  },
  "args": ["first-positional", 42],
  "kwargs": {"to": "alice@example.com", "subject": "Hi"}
}
```

| Field | Type | Notes |
|---|---|---|
| `task.id` | `string` | UUIDv7 by default; caller-provided IDs are also accepted by `Client.schedule`. |
| `task.queue` | `string` | Fully-resolved queue name including priority suffix (`.high`/`.low`) when applied. |
| `task.name` | `string` | Must match a registered `TaskInfo.name`. |
| `args` | `array<any>` | Positional arguments forwarded to `Task.run()`. JSON types only. |
| `kwargs` | `object<any>` | Keyword arguments forwarded to `Task.run()`. JSON types only. |

### AMQP headers

The following headers are stamped by SwarmQ at publish or consume
time. All are optional from a consumer's perspective — absence
means the feature is not active.

| Header | Type | Written by | Meaning |
|---|---|---|---|
| `x-swarmq-enqueued-at` | `int` (unix seconds) | Client / Scheduler | Used by the worker's `max_age` check. |
| `x-swarmq-retry-count` | `int` | Worker | Monotonic counter; reset by `swarmq dlq retry`. |
| `x-swarmq-traceback` | `string` | Worker (on failure) | Full Python traceback of the raising attempt. |
| `x-swarmq-unique-hash` | `string` (hex) | Client | SHA-256 of canonical `(name, args, kwargs)`. |
| `x-swarmq-unique-until` | `"start"` \| `"completion"` | Client | When the worker releases the dedup flag. |
| `x-swarmq-schedfire` | `int` (unix ms) | Scheduler | Present on scheduler-published messages; worker dedupes on it. |
| `x-swarmq-schedfire-claim` | `string` | Worker (copy of a schedule fire) | Owner token of the fire's in-flight claim held by the delivery this copy came from; the copy takes that claim over instead of treating it as a duplicate. |
| `x-swarmq-backend-defer-count` | `int` | Worker (backend deferral) | How often a check before execution deferred this message because the backend was unreachable; sets the next wait. Not a retry count. |
| `x-swarmq-lock-render-error` | `string` | Worker (lock render failure) | Why the lock template could not be rendered; the message is DLQ-routed. |
| `x-swarmq-lock-conflict-count` | `int` | Worker (lock conflict) | Conflicts so far; at 10 the message is DLQ-routed. |
| `x-swarmq-lock-name` | `string` | Worker (lock conflict) | Rendered lock name the conflicts refer to. |
| `x-swarmq-rate-limit-count` | `int` | Worker (rate limit) | How often the message bounced off its rate limit. |
| `x-swarmq-workflow-id` | `string` | Client / Worker | Workflow the message belongs to (workflow steps only). |
| `x-swarmq-workflow-step` | `string` | Client / Worker | Step path inside the workflow (workflow steps only). |
| `x-death` | `array` | RabbitMQ | Broker-native DLX audit trail. |

## Task result (Valkey)

Stored at `sq:result:{task_id}`, encoded with msgpack, TTL equal to
the resolved `result_ttl`.

```jsonc
{
  "task": {"id": "01HV...", "name": "send_email", "queue": "emails"},
  "is_error": false,
  "is_cancelled": false,
  "result": "sent to alice@example.com",
  "error_message": null,
  "error_class": null
}
```

| Field | Type | Notes |
|---|---|---|
| `task.id` | `string` | Echoes the task message's `task.id`. |
| `task.name` | `string` | Echoes the task message's `task.name`. |
| `task.queue` | `string` | Echoes the task message's `task.queue`. |
| `is_error` | `bool` | `true` when the task raised an uncaught exception on its final attempt. |
| `is_cancelled` | `bool` | `true` when the task was cancelled via `Client.cancel`. |
| `result` | `any` | Present when `is_error=false` and `is_cancelled=false`. msgpack-native types. |
| `error_message` | `string \| null` | `str(exc)` from the failing attempt; present when `is_error=true`. |
| `error_class` | `string \| null` | Fully-qualified class name of the exception. Advisory — not guaranteed to survive wire encoding on future versions. |

### Decoding priority

When consuming a result, evaluate in this order:

1. `is_cancelled` → raise `TaskCancelledError`.
2. `is_error` → raise `TaskError(error_message)`.
3. Otherwise → return `result`.

## Progress update (Valkey)

Stored at `sq:progress:{task_id}` as a Valkey HASH. Published on
the Pub/Sub channel `sq:progress:channel:{task_id}` as a single
JSON document with the same fields.

```jsonc
{
  "current": "42",
  "total": "100",
  "percent": "42.00",
  "message": "item 42 rendered",
  "updated_at": "2026-04-21T12:34:56.789012+00:00",
  "started_at": "2026-04-21T12:30:00.000000+00:00",
  "eta_seconds": "380"
}
```

All values are strings at the HASH layer. Clients decode as:

| Field | Target type | Note |
|---|---|---|
| `current` | `int` | |
| `total` | `int` | |
| `percent` | `float` | `0.0`–`100.0` |
| `message` | `string` | May be empty. |
| `updated_at` | ISO 8601 datetime, UTC | Always present. |
| `started_at` | ISO 8601 datetime, UTC \| empty | Empty until first update. |
| `eta_seconds` | `int` \| empty | Empty at `current=0` and on the final flush. |

A HASH missing `current`, `total`, or `updated_at` is treated as
malformed and returned as `None`.

## Scheduler entry

Stored in the Valkey ZSET `sq:sched:fire` (score = fire timestamp
in ms) with the definition at `sq:sched:defs:{name}`.

```jsonc
{
  "schedule_name": "nightly-billing",
  "task_name": "run_billing",
  "args": [],
  "kwargs": {"tenant": "acme"},
  "queue": "priority",
  "cron": "0 3 * * *",
  "fire_ts_ms": 1735272000000,
  "source": "cli",
  "dedup_hash": null,
  "unique_until": "completion",
  "disabled": false
}
```

| Field | Type | Notes |
|---|---|---|
| `schedule_name` | `string` | Primary key. |
| `task_name` | `string` | Must map to a registered task. |
| `args` | `array<any>` | Positional args at fire time. |
| `kwargs` | `object<any>` | Keyword args at fire time. |
| `queue` | `string` | Override for `TaskInfo.queues[0]`. |
| `cron` | `string \| null` | 5-field cron; `null` for one-shot schedules. |
| `fire_ts_ms` | `int` | Next fire time. |
| `source` | `string` | `"cli"`, `"cli_unique"`, `"python"`, etc. |
| `dedup_hash` | `string \| null` | SHA-256 hex when dedup is armed. |
| `unique_until` | `"start" \| "completion"` | Dedup release mode. |
| `disabled` | `bool` | `true` after `schedule remove` (without `--permanent`). |

## DLQ envelope

`swarmq dlq list --format json` and `swarmq dlq inspect --format
json` return envelopes of this shape:

### `dlq list`

```jsonc
{
  "entries": [
    {
      "task_id": "01HV...",
      "task_name": "send_email",
      "queue": "emails.XQ",
      "body_size": 312,
      "headers": {"x-swarmq-retry-count": 3, "x-swarmq-traceback": "..."},
      "body": {"task": {...}, "args": [], "kwargs": {...}}
    }
  ],
  "scanned": 1,
  "limit": 100,
  "truncated": false,
  "queue": "emails"
}
```

### `dlq inspect`

Found:

```jsonc
{
  "found": true,
  "scanned": 17,
  "task_id": "01HV...",
  "task_name": "send_email",
  "queue": "emails.XQ",
  "body_size": 312,
  "headers": { ... },
  "body": { ... }
}
```

Not found (exit code `3`):

```jsonc
{
  "found": false,
  "task_id": "01HV...",
  "queue": "emails",
  "scanned": 1000,
  "scan_exhausted": true
}
```

### `dlq retry`

Single task_id:

```jsonc
{
  "moved": 1,
  "queue": "emails",
  "task_id": "01HV...",
  "found": true,
  "scanned": 1,
  "scan_exhausted": false
}
```

`--all`:

```jsonc
{
  "moved": 42,
  "scanned": 45,
  "skipped_malformed": 3,
  "truncated": false,
  "queue": "emails",
  "all": true
}
```

### `dlq purge`

```jsonc
{"purged": 42, "queue": "emails"}
```

## Cancel flag

Stored at `sq:cancel:{task_id}` in Valkey with TTL
`result_ttl × 1000` ms. The value is a fixed marker string —
presence is the signal.

| Key shape | Type | Semantics |
|---|---|---|
| `sq:cancel:{task_id}` | `string` ("1") | Present → cancel requested; absent → no cancel. |

The worker's cancel monitor `EXISTS`-checks this key periodically
(`cancel_poll_interval`, default 1.0 s) and on every Valkey
operation boundary. When set while the task is running, the worker
raises `TaskCancelledError` inside the task coroutine.

## Dedup flag

Stored at `sq:unique:{sha256_hex}` in Valkey with TTL
`unique_for × 1000` ms (plus any fire offset for deferred paths).

| Key shape | Value | Semantics |
|---|---|---|
| `sq:unique:{hash}` | `{task_id}` | Owner task_id. Release is owner-checked via Lua. |

`compute_dedup_hash(task_name, args, kwargs)` produces the hash.
`args` is a tuple, `kwargs` is a dict sorted by key before
serialisation.

## Signal payloads

Signals fire in-process inside SwarmQ workers and clients.
Handlers receive the following kwargs per signal. Fields marked
*(optional)* may be absent in some sources — always use
`.get()`-style access.

### `TASK_ENQUEUED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails"}
```

### `TASK_STARTED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails"}
```

### `TASK_COMPLETED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "duration_ms": 312}
```

### `TASK_FAILED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "exception": "smtp connection refused"}
```

`exception` is `str(exc)`, not the exception object. `TASK_FAILED`
fires after every failed attempt, also when a retry follows;
`TASK_DLQ` marks the terminal failure.

### `TASK_RETRYING`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "retry_count": 1, "delay": 2.0}
```

### `TASK_CANCELLED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails"}
```

### `TASK_EXPIRED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "age_s": 120, "max_age_s": 60}
```

### `TASK_LOCKED`

```jsonc
{"task_id": "01HV...", "task_name": "sync_user", "lock_name": "user-42", "lock_conflict_count": 1}
```

### `TASK_RATE_LIMITED`

```jsonc
{
  "task_id": "01HV...",
  "task_name": "send_sms",
  "retry_after_ms": 1200,
  "remaining": 3,
  "rate_limit_count": 4
}
```

### `TASK_DUPLICATE_DETECTED`

```jsonc
{
  "existing_task_id": "01HV...",
  "task_name": "send_welcome",
  "dedup_hash": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3...",
  "source": "client.schedule",
  "batch_index": 2
}
```

- `source` is one of `"client.schedule"`, `"client.schedule_deferred"`,
  `"client.schedule_many"`, `"scheduler.fire"`.
- `batch_index` *(optional)* is present only when
  `source == "client.schedule_many"`.

### `TASK_DLQ`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "error": "smtp connection refused"}
```

Fires once, when the worker dead-letters a message terminally. An
error result exists for `task_id`.

### `TASK_REJECTED`

```jsonc
{
  "queue": "emails",
  "reason": "unregistered_task",
  "task_id": "01HV...",
  "task_name": "send_emial",
  "error": "task 'send_emial' is not registered"
}
```

- `reason` is one of `"body_too_large"`, `"undecodable"`,
  `"unregistered_task"`, `"middleware_chain_factory"`,
  `"dlq_publish_failed"`.
- `task_id` and `task_name` are `null` when the body never decoded
  (`body_too_large`, `undecodable`).
- No error result is stored; the message carries only the broker's
  `x-death` header in `.XQ`.

### `WORKFLOW_STEP_ENQUEUED`

```jsonc
{
  "task_id": "01HV...",
  "task_name": "render_pdf",
  "queue": "pdf",
  "workflow_id": "01HW...",
  "step_path": "/1/g0"
}
```

Follows the `TASK_ENQUEUED` of the same step. `step_path` is the
step's position in the workflow topology: `/`-separated segments,
where `N` is the N-th step of a chain, `gN` the N-th member of a group
and `h` the header of a chord (`""` for a workflow that is a single
signature, `/0` for the first chain step, `/1/g0` for the first member
of a group that is the second chain step).

### `WORKER_*`

```jsonc
{"worker_id": "web-3:0"}
```

Emitted for `WORKER_ONLINE`, `WORKER_SHUTDOWN`,
`WORKER_HEARTBEAT` (the latter with `uptime_seconds` in addition).
`worker_id` is `<base>:<slot>` under the supervisor and
`<base>:p<pid>` for an embedded worker; `<base>` is `SWARMQ_WORKER_ID`
or the hostname. `WORKER_RECYCLING` adds `kind`, `slot`, `reason`,
`value`.

### `SCHEDULER_*`

```jsonc
{"worker_id": "web-3:0"}
```

Emitted for `SCHEDULER_LEADER`, `SCHEDULER_STANDBY`,
`SCHEDULER_HEARTBEAT`, `SCHEDULER_LEADER_HANDOVER`,
`SCHEDULER_LEADER_FAILOVER`.

## `swarmq worker --check` payload

`swarmq worker --check ...` prints one JSON object on stdout and exits
`0` (`ok: true`) or `1` (`ok: false`). Nothing is started and no
connection is opened.

```jsonc
{
  "ok": true,
  "module": "myapp.tasks",
  "module_origin": "/srv/app/myapp/tasks.py",
  "reload": false,
  "watch_dirs": null,
  "warnings": [],
  "error": null,
  "error_type": null,
  "pool": "io",
  "slots": 10,
  "configure": "myapp.hooks:setup",
  "metrics_ports": [9000, 9001, 9002],
  "health_ports": [9100, 9101, 9102]
}
```

| Field | Type | Description |
|---|---|---|
| `ok` | `bool` | `true` when every check passed. |
| `module` | `str` | The `--module` argument. |
| `module_origin` | `str \| null` | Resolved file of the task module (`find_spec`). |
| `reload` | `bool` | Whether `--reload` was requested. |
| `watch_dirs` | `list[str] \| null` | Resolved watch directories; `null` without `--reload`. |
| `warnings` | `list[str]` | Non-fatal findings, e.g. a `hard_timeout` above the broker's consumer timeout. |
| `error`, `error_type` | `str \| null` | Set when `ok` is `false`; `error_type` is the exception class name (`ConfigurationError`, `ModuleNotFoundError`, `ArgparseError`, …). |
| `pool` | `"io" \| "cpu"` | Effective pool. |
| `slots` | `int` | Effective concurrency per process (compute slots in CPU mode). |
| `configure` | `str \| null` | The `--configure` spec after syntax and `find_spec` validation; the callable itself is resolved only inside the worker. |
| `metrics_ports`, `health_ports` | `list[int] \| null` | The exact ports the slots `0..N-1` will bind, `N` being `--processes` or the `--autoscale` upper bound; `null` when the base port is unset. |

On an argparse error (exit `2`) the payload has `error_type:
"ArgparseError"` and the other fields `null`.

## See also

- [Decision trees](decision-trees.md) — when to emit which shape
- [Error catalog (machine)](error-catalog-machine.md) — the
  exceptions your consumer has to classify
- [API reference](../api.md) — Python signatures for the types
  behind these payloads
