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
| `x-swarmq-expired` | `"1"` | Worker (on expiry) | Set before the message is DLQ-routed. |
| `x-swarmq-lock-error` | `"render_failed"` \| `"conflict_cap"` | Worker (on lock failure) | Surfaces lock-system failures on DLQ'd messages. |
| `x-swarmq-error` | `"no_retry"` | Worker | Present when `NoRetry` routed the message. |
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
{"task_id": "01HV...", "task_name": "send_email"}
```

### `TASK_COMPLETED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "duration_ms": 312}
```

### `TASK_FAILED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "exception": "<Exception>"}
```

### `TASK_RETRYING`

```jsonc
{"task_id": "01HV...", "retry_count": 1, "delay": 2.0}
```

### `TASK_CANCELLED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email"}
```

### `TASK_EXPIRED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "age_seconds": 120}
```

### `TASK_LOCKED`

```jsonc
{"task_id": "01HV...", "task_name": "sync_user", "lock_name": "user-42"}
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

### `WORKER_*`

```jsonc
{"worker_id": "worker-01HV..."}
```

Emitted for `WORKER_ONLINE`, `WORKER_SHUTDOWN`,
`WORKER_HEARTBEAT`.

### `SCHEDULER_*`

```jsonc
{"worker_id": "worker-01HV..."}
```

Emitted for `SCHEDULER_LEADER`, `SCHEDULER_STANDBY`,
`SCHEDULER_HEARTBEAT`, `SCHEDULER_LEADER_HANDOVER`,
`SCHEDULER_LEADER_FAILOVER`.

## See also

- [Decision trees](decision-trees.md) — when to emit which shape
- [Error catalog (machine)](error-catalog-machine.md) — the
  exceptions your consumer has to classify
- [API reference](../api.md) — Python signatures for the types
  behind these payloads
