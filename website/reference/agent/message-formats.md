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
| `x-swarmq-traceback` | `string` | Worker (on failure) | Full Python traceback of the raising attempt; on a message the worker rejects (`TASK_REJECTED`) `<reason>: <error>`. |
| `x-swarmq-unique-hash` | `string` (hex) | Client | SHA-256 of canonical `(name, args, kwargs)`. |
| `x-swarmq-unique-until` | `"start"` \| `"completion"` | Client | When the worker releases the dedup flag. |
| `x-swarmq-schedfire` | `int` (unix ms) | Scheduler | Present on scheduler-published messages; worker dedupes on it. |
| `x-swarmq-schedfire-claim` | `string` (comma-separated tokens) | Worker (copy of a schedule fire) | Owner tokens of the fire's in-flight claim, newest first, at most 16: the token of the delivery this copy came from, followed by the tokens that delivery was offered (a claim whose reply was lost may hold any of them). The copy takes a claim held by one of these tokens over instead of treating it as a duplicate. Set on deferral, retry, rate-limit and lock copies and on the copy that replaces a requeue when releasing the claim failed; a requeue in place (`nack`) never carries it. The worker reads at most 16 tokens. |
| `x-swarmq-backend-defer-count` | `int` | Worker (backend deferral) | How often a check before execution deferred this message because the backend was unreachable; sets the next wait. Not a retry count. Copies made after the backend answered (retry, rate-limit hit, lock conflict) do not carry it, and the count starts over when the delivery carries a broker counter (`x-delivery-count`/`x-acquired-count` above 0, i.e. the broker handed the message back). |
| `x-swarmq-guard-defer-count` | `int` | Worker (execution guard) | How often the [execution guard](../../explanation/delivery-semantics.md#execution-guard) deferred this message because the same execution ran elsewhere; sets the next wait. Not a retry count. |
| `x-swarmq-unknown-since` | `int` (Unix milliseconds) | Worker (unregistered task) | First contact with a task name the worker does not register; the grace period `SWARMQ_UNKNOWN_TASK_GRACE_S` is measured from it. Kept on every deferral copy the worker makes before execution; removed on a retry copy, at dead-lettering and by `dlq retry`. |
| `x-swarmq-replay-id` | `string` (hex, at most 64 characters) | Worker (dead-lettering), `swarmq dlq retry` | Part of the [execution identity](../../explanation/delivery-semantics.md#execution-guard), so a deliberate replay runs even while the done marker of the first run exists. The worker stamps the id of the next generation on the DLQ copy (`"g"` plus 39 hex characters, derived from the current identity, the same for two copies of one execution); `dlq retry` keeps it when `x-swarmq-dlq-gen` matches `x-swarmq-dead-lettered-at` and sets a fresh `uuid4` otherwise (0.11 worker, old stock). A longer or non-string value is ignored. |
| `x-swarmq-xdeath-base` | `int` | Worker (every copy), `swarmq dlq retry` | Number of broker dead-letterings outside the delay queues the copy inherited: the `count` values of `x-death` whose queue is neither `{queue}.DQ` nor a delay tier, plus the previous base. Set because the broker rebuilds `x-death` on the next dead-lettering; absent means 0. The execution identity is `task_id|<replay id>#<base plus current x-death>`, without the suffix when the number is 0. |
| `x-swarmq-dlq-gen` | `int` (Unix milliseconds) | Worker (dead-lettering) | The value of `x-swarmq-dead-lettered-at` of the stamp that set `x-swarmq-replay-id`. `dlq retry` keeps the replay id only while both are equal and removes this header. |
| `x-swarmq-dead-lettered-at` | `int` (Unix milliseconds) | Worker (dead-lettering), `swarmq dlq` (old stock on first read) | When the message was dead-lettered; the `dlq` commands compute the remaining retention from it. Milliseconds because AMQP tables encode floats only in single precision. Removed by `dlq retry`. Messages the worker dead-letters also carry the property `expiration` (the retention, `SWARMQ_DEAD_LETTER_TTL_S`), unless the retention is `none`. |
| `x-swarmq-dead-letter-ttl-ms` | `int` (milliseconds) or `"none"` | Worker (dead-lettering), `swarmq dlq` (on first read when it is missing) | The retention the message was dead-lettered with; `"none"` means unlimited. The `dlq` commands compute the remaining retention from it, not from their own `SWARMQ_DEAD_LETTER_TTL_S`. Removed by `dlq retry`. |
| `x-swarmq-lock-render-error` | `string` | Worker (lock render failure) | Why the lock template could not be rendered; the message is DLQ-routed. |
| `x-swarmq-lock-conflict-count` | `int` | 0.11 worker (lock conflict) | Legacy counter: a 0.11 worker raises it and dead-letters at 10. 0.12 neither raises nor reads it; copies carry it unchanged. |
| `x-swarmq-lock-conflicts` | `int` | Worker (lock conflict) | Lock conflicts of this message so far; the `lock_conflict_count` of `TASK_LOCKED`. |
| `x-swarmq-lock-wait-since` | `int` (Unix milliseconds) | Worker (lock conflict) | First lock conflict; `SWARMQ_LOCK_WAIT_MAX_S` is measured from it. |
| `x-swarmq-lock-holder` | `string` | Worker (lock conflict) | Fingerprint of the lock's holders (the set of owners) at the last change of holder. |
| `x-swarmq-lock-holder-since` | `int` (Unix milliseconds) | Worker (lock conflict) | When the worker last saw the holders change. |
| `x-swarmq-lock-holder-ttl-ms` | `int` (milliseconds) | Worker (lock conflict) | Remaining TTL of the lock key at that change; a holder that keeps the lock longer than this plus 60 s counts as stalled. |
| `x-swarmq-lock-wait-gen` | `str` (`"<retry count>:<replay ID>"`) | Worker (lock conflict) | Execution the wait belongs to. A message whose retry count or replay ID differs (a copy that a 0.11 worker made of a later execution) starts a new wait. The six lock headers stay on every deferral copy a worker makes before execution and are removed on a retry copy, at dead-lettering and by `dlq retry`; a copy of a message the broker redelivered drops them as well. |
| `x-swarmq-lock-name` | `string` | Worker (lock conflict) | Rendered lock name the conflicts refer to. |
| `x-swarmq-rate-limit-count` | `int` | Worker (rate limit) | How often the message bounced off its rate limit. |
| `x-swarmq-workflow-id` | `string` | Client / Worker | Workflow the message belongs to (workflow steps only). |
| `x-swarmq-workflow-step` | `string` | Client / Worker | Step path inside the workflow (workflow steps only). |
| `x-delivery-count`, `x-acquired-count` | `int` | RabbitMQ (quorum queues) | Broker delivery counters; the worker checks both against the [effective delivery limit](../../explanation/delivery-semantics.md#effective-delivery-limit) (the smaller of `SWARMQ_DELIVERY_LIMIT` and the queue's `x-delivery-limit`; RabbitMQ 4.3 counts a requeue only in `x-acquired-count`). Every copy SwarmQ publishes (retry, deferral, rate-limit, lock, DLQ reads by `swarmq dlq`) drops them, so the copy is a new message with its own delivery budget. |
| `x-death` | `array` | RabbitMQ | Broker-native DLX audit trail. Every copy SwarmQ publishes drops it after moving its count into `x-swarmq-xdeath-base`. |

## Task result (Valkey)

Keys below are written without the optional namespace prefix: with
`SWARMQ_NAMESPACE` set, every key and channel starts with
`<namespace>:sq:` instead of `sq:`.

Stored at `sq:result:{task_id}`, encoded with msgpack, TTL equal to
the resolved `result_ttl`.

```jsonc
{
  "task": {"id": "01HV...", "name": "send_email", "queue": "emails"},
  "is_error": false,
  "is_cancelled": false,
  "error_message": "",
  "result": "sent to alice@example.com"
}
```

An error result additionally carries `error_class`:

```jsonc
{
  "task": {"id": "01HV...", "name": "send_email", "queue": "emails"},
  "is_error": true,
  "is_cancelled": false,
  "error_message": "",
  "result": null,
  "error_class": "builtins.ValueError"
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
| `error_message` | `string` | Always present. `str(exc)` from the failing attempt when `is_error=true`, otherwise `""`. An exception without text gives `""` as well; `error_class` then still names the class. |
| `error_class` | `string` (optional) | Only on error results, and only when a class is known: the fully-qualified class of the exception as `module.qualname` (built-ins as `builtins.ValueError`). In CPU mode it is the class reported by the compute child, even if the parent could not load it. Paths that end in the worker without a task exception name the public swarmq class where one exists (`swarmq.exceptions.TaskNotFoundError`, `LockWaitExceeded`, `RedeliveryLimitError`, `UnroutableMessage`, `ResultSerializationError`). Otherwise the key is **missing** (never `null`): results of older workers, expiry by `max_age`, `WorkerHungError`, and failures the worker generates itself (hard timeout, drain, lost compute child, child without error detail). Advisory: use it to tell failures apart, not as a stable API. |

### Decoding priority

When consuming a result, evaluate in this order:

1. `is_cancelled` → raise `TaskCancelledError`.
2. `is_error` → raise `TaskError(error_message)`; `TaskError.error_class`
   carries `error_class` (`None` when the key is missing).
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
| `started_at` | ISO 8601 datetime, UTC \| empty | Wall-clock start of the run (before 0.12.3 it came from the monotonic clock and read as a date in 1970). Empty only when the worker recorded no start time. |
| `eta_seconds` | `int` \| empty | Empty at `current=0` and on the final flush. |

A HASH missing `current`, `total`, or `updated_at` is treated as
malformed and returned as `None`.

## Scheduler entry

Stored in the Valkey ZSET `sq:{sched}:zset` (member = schedule name,
score = fire timestamp in ms) with the definition in the HASH
`sq:{sched}:defs` (field = schedule name).

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
| `fire_ts_ms` | `int \| null` | Next fire time. |
| `source` | `string` | `"static"` (cron defined in code), `"cli"` or `"cli_unique"` (CLI schedules and one-shots). |
| `disabled` | `bool` | `true` after `schedule remove` (without `--permanent`). |
| `last_fire_at` | `int \| null` | Unix ms of the last fire. |
| `dedup_hash` | `string \| null` | SHA-256 hex when dedup is armed. |
| `unique_until` | `"start" \| "completion"` | Dedup release mode. |
| `timezone` | `string \| null` | IANA zone of `cron`; `null` means UTC. |
| `task_id` | `string \| null` | Since 0.12: the ID `schedule(delay=\|eta=)` returned; a one-shot fires under it. `null` for crons. |
| `unroutable_since` | `int \| null` | Since 0.12: Unix ms of the first fire of a one-shot whose queue did not exist. |
| `unique_for` | `int \| null` | Since 0.12: `TaskInfo.unique_for` of a cron, reported by the worker. |

Fields added later come at the end with a `null` default, so entries
written by older versions still decode.

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
      "queue": "emails",
      "headers": {"x-swarmq-retry-count": 3, "x-swarmq-traceback": "..."},
      "body_size": 312
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
  "queue": "emails",
  "headers": { ... },
  "body": {"task": {...}, "args": [], "kwargs": {...}},
  "body_raw": "{\"task\": ...}",
  "body_size": 312
}
```

`queue` is the main queue; the messages are read from `{queue}.XQ`.
`dlq list` carries no body (use `dlq inspect`). `body` is the decoded
task body, `null` when it is not a JSON object; `body_raw` is the body as
UTF-8 text (invalid bytes replaced).

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

Stored at `sq:cancel:{task_id}` in Valkey with a TTL of
`max(24 h, result_ttl)`, plus the wait until a deferred task fires and
its `max_age` + 300 s when the client's registry knows the task. Once
the worker has handled the cancelled delivery it shortens the TTL to
at most 1 h (`PEXPIRE … LT`) instead of deleting the key. The value is a fixed marker string —
presence is the signal.

| Key shape | Type | Semantics |
|---|---|---|
| `sq:cancel:{task_id}` | `string` ("1") | Present → cancel requested; absent → no cancel. |

The worker's cancel monitor `EXISTS`-checks this key periodically
(`cancel_poll_interval`, default 1.0 s) and on every Valkey
operation boundary. When set while the task is running, the worker
cancels the running coroutine; the task sees `asyncio.CancelledError`,
and the stored result has `is_cancelled=true`, which `get_result()`
raises as `TaskCancelledError`. `TaskCancelledError` never arrives
inside the task.

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
- No error result is stored. For `body_too_large`, `undecodable` and
  `middleware_chain_factory` the worker puts a copy with retention into
  `.XQ`, with `x-swarmq-traceback` = `<reason>: <error>`; only when that
  copy fails, and for `dlq_publish_failed`, the message carries just the
  broker's `x-death` header instead.

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
connection is opened. `ok: true` predicts the start: the check builds
the worker configuration from the `SWARMQ_*` variables like the start,
so every value the start rejects with exit `78` gives `ok: false`.
Missing `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL` are not checked.

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
  "metrics_ports": [9000, 9002],
  "health_ports": [9100, 9102]
}
```

| Field | Type | Description |
|---|---|---|
| `ok` | `bool` | `true` when every check passed. |
| `module` | `str` | The `--module` argument. |
| `module_origin` | `str \| null` | Resolved file of the task module (`find_spec`). |
| `reload` | `bool` | Whether `--reload` was requested. |
| `watch_dirs` | `list[str] \| null` | Resolved watch directories; `null` without `--reload`. |
| `warnings` | `list[str]` | Non-fatal findings, each a string led by its code, e.g. `"reload_burst_conflict: --reload and --burst are not designed to be combined; …"` when `--reload` and `--burst` are given together. |
| `error`, `error_type` | `str \| null` | Set when `ok` is `false`; `error_type` is the exception class name (`ConfigurationError`, `ModuleNotFoundError`, `ArgparseError`, …). |
| `pool` | `"io" \| "cpu"` | Effective pool. |
| `slots` | `int` | Effective concurrency per process (compute slots in CPU mode). |
| `configure` | `str \| null` | The `--configure` spec after syntax and `find_spec` validation; the callable itself is resolved only inside the worker. |
| `metrics_ports`, `health_ports` | `list[int] \| null` | The port range the slots `0..N-1` will bind, as `[first, last]` (both inclusive), not the list of every port: with the base port `9000` and `N = 3`, `[9000, 9002]` covers `9000`, `9001` and `9002`. `N` is the largest of `--processes`, the `--autoscale` upper bound and `--max-processes`. With `N = 2` the range looks like an exact list (`[9000, 9001]`), with `N = 1` both entries are equal (`[9000, 9000]`). `null` when the base port is unset. |

Every payload carries the full field set above, also on a failed
check; fields not known at that point are `null`. On an argparse error
(exit `1`, not argparse's usual `2`) the payload has `ok: false`,
`error_type: "ArgparseError"`, the message in `error`, `warnings: []`
and every other field `null`.

## See also

- [Decision trees](decision-trees.md) — when to emit which shape
- [Error catalog (machine)](error-catalog-machine.md) — the
  exceptions your consumer has to classify
- [API reference](../api.md) — Python signatures for the types
  behind these payloads
