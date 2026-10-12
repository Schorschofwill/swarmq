# Signals

You want a handler to run on a specific task lifecycle event —
record a metric when a task fails, emit an audit log when a task is
cancelled, notify Slack when a scheduler leader swaps. Signals are
the in-process pub/sub layer for those events.

## Prerequisites

- A `SwarmQ` instance.
- An async function you want to run on some event.

## Registering a handler

Use the `@app.on_signal(...)` decorator:

```python
from swarmq import SwarmQ, Signal
from swarmq.logging import get_logger

app = SwarmQ(broker, backend)
log = get_logger("myapp.signals")


@app.on_signal(Signal.TASK_FAILED)
async def on_task_failed(task_id, task_name, exception, **kwargs):
    log.error(
        "myapp_task_attempt_failed",
        task_id=task_id, task_name=task_name,
        error=exception,
    )
```

The handler must be async. It runs inline after the event is
dispatched — no separate event loop, no worker thread.

Always end the parameter list with `**kwargs`. The worker passes every
key from the table below — `TASK_FAILED` also carries `queue` — and a
handler with a fixed signature fails with `TypeError` on each dispatch
(caught and logged as `signal_handler_error`, so the handler silently
never does its job). New keys can appear in later releases.

`swarmq.logging.get_logger(name)` returns a logger that hands events to
stdlib `logging` under `name`, with the keyword arguments as record
fields — the same mechanism SwarmQ uses for its own events (see
[Logging](../reference/logging.md)). Give your own events names that do
not collide with SwarmQ's built-in ones (`task_failed`,
`task_succeeded`, …), otherwise you cannot tell them apart in the log.

!!! note "`exception` is a string"
    `TASK_FAILED` carries `exception=str(exc)`, the message of the
    exception, not the exception object. You cannot `isinstance()` it or
    hand it to an error tracker as an exception. When you need the
    object (class, traceback), use a [middleware](middleware.md)
    `on_error` hook, which receives the real exception — see
    [Sentry / error aggregator integration](#sentry-error-aggregator-integration).

## Available signals

| Signal | Fires | Kwargs |
|---|---|---|
| `TASK_ENQUEUED` | After a successful `schedule()` | `task_id`, `task_name`, `queue` |
| `TASK_STARTED` | Before `run()` is called | `task_id`, `task_name`, `queue` |
| `TASK_COMPLETED` | After `run()` succeeds | `task_id`, `task_name`, `queue`, `duration_ms` |
| `TASK_FAILED` | After **every** failed attempt (`run()` raised or timed out), before the retry-or-DLQ decision | `task_id`, `task_name`, `queue`, `exception` (string) |
| `TASK_RETRYING` | Before a retry is published (not when a message is deferred because the backend is unreachable before execution; that deferral fires no signal) | `task_id`, `task_name`, `queue`, `retry_count` (the upcoming attempt's retry number), `delay` (seconds) |
| `TASK_CANCELLED` | When a cancel flag is observed mid-run | `task_id`, `task_name`, `queue` |
| `TASK_EXPIRED` | When `max_age` is exceeded at pickup; the message goes to the DLQ | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` |
| `TASK_LOCKED` | When the task's lock is held by another execution at pickup (lock conflict); the message is requeued with a delay | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` |
| `TASK_RATE_LIMITED` | When the rate limiter defers a message | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` |
| `TASK_DUPLICATE_DETECTED` | When a unique-task SETNX loses the race | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] |
| `TASK_DLQ` | Once, when a message is terminally dead-lettered by the worker (retry exhaustion, `NoRetry`, delivery limit or execution guard orphan limit reached) | `task_id`, `task_name`, `queue`, `error` |
| `TASK_REJECTED` | When the worker rejects a message it cannot run — oversized body, undecodable body, broken middleware-chain factory (a copy with retention in `.XQ`, the broker's dead-letter exchange as fallback) — when a dead-letter publish failed and the message goes to the broker's dead-letter exchange without diagnostic headers, and when it dead-letters an unregistered task after `SWARMQ_UNKNOWN_TASK_GRACE_S` (after `TASK_DLQ`) | `queue`, `reason`, `task_id`, `task_name`, `error` |
| `TASK_UNKNOWN_DEFERRED` | Once per deferral of a message whose task this worker does not register, within `SWARMQ_UNKNOWN_TASK_GRACE_S` (rolling deploy) | `task_id`, `task_name`, `queue`, `delay` (seconds), `unknown_since` (Unix ms) |
| `WORKFLOW_STEP_ENQUEUED` | After each workflow step is published (by `app.apply()` or the advancer in the worker), in addition to `TASK_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | Worker lifecycle | `worker_id` |
| `WORKER_HEARTBEAT` | Periodic liveness tick of the worker's metrics sampler | `worker_id`, `uptime_seconds` |
| `WORKER_RECYCLING` | A compute child (`kind="compute_child"`) or the worker itself (`kind="worker"`) is recycled by RSS or task count | `worker_id`, `kind`, `slot`, `reason` (`rss`/`tasks`/`forced`), `value` |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | Scheduler leader transitions | `worker_id` |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | Leader swap sources | `worker_id` |
| `SCHEDULER_FIRE_SKIPPED` | Catch-up after downtime skipped cron occurrences | `worker_id`, `schedule_name`, `skipped_count` |
| `SCHEDULER_CRON_CONFLICT` | Several definitions of one static cron live for more than 10 minutes (leader, every adoption tick while it lasts) | `worker_id`, `schedule_name`, `fingerprints` |
| `SCHEDULER_LEADER_OUTDATED` | The scheduler leader runs a swarmq without time zone support (`outdated=True`, once per leader), or a current leader follows (`outdated=False`) | `worker_id`, `leader`, `leader_version` (`None` = before 0.10.0), `outdated` |
| `SCHEDULER_CRON_UNOWNED` | The leader starts holding a static cron that no live worker reports and it does not register itself (`held=True`), or releases it again (`held=False`); once per transition | `worker_id`, `schedule_name`, `task_name`, `held`, `held_count` |
| `SCHEDULER_VALKEY_CHECKED` | The leader checked Valkey's eviction policy and memory again, at most once per hour (since 0.12) | `worker_id`, `status`, `policy`, `maxmemory`, `used_memory` |
| `SCHEDULER_QUEUE_ORPHANED` | The leader found a reported queue with messages, no consumer and no live worker reporting it (`orphaned=True`), or it is no longer orphaned (`orphaned=False`); once per transition (since 0.12) | `worker_id`, `queue`, `orphaned`, `message_count`, `orphaned_count` |
| `SCHEDULER_FIRE_UNROUTABLE` | The leader's publish was returned because the queue does not exist: a lost cron fire (`kind="cron_lost"`) or a deferred task dropped after `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (`kind="oneshot_dropped"`) | `worker_id`, `schedule_name`, `queue`, `kind` |

`TASK_FAILED` is per attempt: a task with `max_retries=3` that always
fails fires it four times, interleaved with three `TASK_RETRYING`. To
react only when the work is given up, subscribe to `TASK_DLQ`, which
fires once at the terminal hand-off to the DLQ. With dead-lettering of
task failures switched off (`TaskInfo(dead_letter=False)` or
`SWARMQ_DEAD_LETTER_TERMINAL_FAILURES=false`) there is no hand-off and
no `TASK_DLQ` for these tasks; the last `TASK_FAILED` is then the
terminal one.

The `TASK_DUPLICATE_DETECTED` signal's `batch_index` key is present only
when the source is `client.schedule_many` — use
`kwargs.get("batch_index")` to avoid `KeyError`.

`TASK_REJECTED` carries `reason` from a fixed set: `body_too_large`,
`undecodable`, `unregistered_task`, `middleware_chain_factory`,
`dlq_publish_failed`. `task_id` and `task_name` are `None` when the
body never decoded. It is distinct from `TASK_DLQ`, which promises an
error result and a task identity; the
[dead letter guide](dead-letter-queues.md#three-ways-into-the-xq)
explains which path a message took. `unregistered_task` is the one
reason that follows `TASK_DLQ`: from 0.12 the worker defers an unknown
task for `SWARMQ_UNKNOWN_TASK_GRACE_S` (`TASK_UNKNOWN_DEFERRED` on
every deferral) and then dead-letters it itself, with an error result
([unregistered tasks](dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)).

Workflow steps fire `TASK_ENQUEUED` with the step's real task name
(there is no `<workflow>` placeholder any more) and then
`WORKFLOW_STEP_ENQUEUED` with the workflow identity. Handlers that
only know `TASK_ENQUEUED` keep working; subscribe to the second signal
when you need `workflow_id` or `step_path`.

### The `worker_id` field

Every `WORKER_*`, `SCHEDULER_*` and `WORKER_RECYCLING` payload carries
the worker's stable identity: `<base>:<slot>` under the supervisor
(`web-3:0`, `web-3:1`, …) and `<base>:p<pid>` for an embedded worker
(`web-3:p41872`). `<base>` is `SWARMQ_WORKER_ID` or the hostname. The
slot survives restarts and recycling, so it is the right key for
per-worker dashboards; see
[worker identity](../reference/configuration.md#worker-identity).

## Error semantics

Handler exceptions are **caught per-handler** and logged at WARNING.
The dispatch loop continues to the next handler; the signal does
not abort the task or the worker.

```python
@app.on_signal(Signal.TASK_COMPLETED)
async def broken(task_id, task_name, duration_ms, **kwargs):
    raise RuntimeError("this is fine")
```

The library logs a WARNING on the `swarmq.signals` logger, with the
traceback attached (`--log-format human`):

```text
2026-10-06T10:02:13.511204Z [warning  ] signal_handler_error           [swarmq.signals] error='this is fine' error_type=RuntimeError handler=broken signal=task_completed
```

and moves on. This is intentional — a buggy metrics handler must not
be able to poison task execution.

## Multiple handlers per signal

Register as many as you need. Handlers run in registration order:

```python
@app.on_signal(Signal.TASK_DLQ)
async def to_slack(task_id, task_name, error, **kwargs):
    await slack.post(channel="#ops", text=f"{task_name} dead-lettered: {error}")


@app.on_signal(Signal.TASK_DLQ)
async def to_pagerduty(task_id, task_name, error, **kwargs):
    if task_name in ("charge_card", "send_invoice"):
        await pagerduty.trigger(task_id)
```

## Middleware vs signals — which one?

The two mechanisms overlap. Use this heuristic:

- **Middleware** when the behaviour must wrap every task (timing,
  tracing, auth context). It has access to `pre_*` and `post_*`
  pairs with shared state.
- **Signal handler** when you care about one specific event class
  and do not need to wrap each task lifecycle. Handlers are simpler
  to reason about than full middleware.

When in doubt: if the code only looks at `task_id` and the error
message, it is a signal handler. If it needs the exception object or
`pre_execute` + `post_execute` symmetry, it is a middleware.

## Typical uses

### Sentry / error aggregator integration

An error tracker wants the exception object with its traceback. The
signal only carries the message, so use a middleware `on_error` hook
instead; it runs on every failed attempt with the real exception:

```python
import sentry_sdk

from swarmq import Middleware, MessageContext


class SentryMiddleware(Middleware):
    async def on_error(
        self, message: MessageContext, exception: BaseException,
    ) -> None:
        with sentry_sdk.new_scope() as scope:
            scope.set_tag("task", message["task_name"])
            scope.set_tag("queue", message["queue"])
            sentry_sdk.capture_exception(exception)


app.add_middleware(SentryMiddleware())
```

### Slack notification for DLQ drops

`TASK_DLQ` fires once per dead-lettered message, after the retries
are used up (or on `NoRetry`); `error` is the error message:

```python
@app.on_signal(Signal.TASK_DLQ)
async def alert(task_id, task_name, queue, error, **kwargs):
    if task_name in ("charge_card", "send_invoice"):
        await slack.post(
            channel="#ops",
            text=f":fire: {task_name} dead-lettered on {queue}: {task_id} ({error})",
        )
```

### Alert on rejected messages

`TASK_REJECTED` means the worker could not even start the task: the
body was too large, undecodable, or named a task this worker does not
know. None of these are retried, so they deserve a separate alert from
ordinary DLQ traffic:

```python
@app.on_signal(Signal.TASK_REJECTED)
async def on_rejected(queue, reason, task_name=None, **kwargs):
    if reason == "unregistered_task":
        await slack.post(
            channel="#ops",
            text=f":warning: {queue}: unknown task {task_name!r} — deploy mismatch?",
        )
```

With `swarmq[metrics]` the same events are counted in
`swarmq_tasks_rejected_total{queue,reason}`.

### Audit log for cancellations

```python
@app.on_signal(Signal.TASK_CANCELLED)
async def audit(task_id, task_name, **kwargs):
    await audit_log.insert(
        event="task_cancelled",
        task_id=task_id, task_name=task_name,
    )
```

### Prometheus counters without the exporter

If you are not using `swarmq[metrics]`, you can emit Prometheus
metrics from handlers directly. This counts failed attempts, not
failed tasks:

```python
from prometheus_client import Counter

FAILED_ATTEMPTS = Counter("task_failed_attempts", "Failed task attempts", ["task"])

@app.on_signal(Signal.TASK_FAILED)
async def count(task_id, task_name, exception, **kwargs):
    FAILED_ATTEMPTS.labels(task=task_name).inc()
```

## What not to do in handlers

- **Do not call `app.schedule()` for every task event.** It works,
  but it is a queueing feedback loop. Handler → schedule → worker
  runs → signal → handler → ...
- **Do not block the loop.** Handlers run inline. A handler that
  does synchronous I/O (`requests.post`, `time.sleep`) stalls the
  worker's event loop and backs up every concurrent task.
- **Do not mutate shared state without a lock.** Handler
  concurrency follows task concurrency — two tasks finishing at the
  same moment fire two `TASK_COMPLETED` handlers.

## Verify

Register one handler for `TASK_COMPLETED` that logs an event of its
own, schedule a task, watch the worker log:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-format human
```

Your handler's line appears right after the worker's `task_succeeded`
line for that task, in the same log stream. Signals themselves are not
log lines: `TASK_COMPLETED` is the hook, `task_succeeded` is the log
event the worker writes just before dispatching it.

## See also

- [`Signal` enum](../reference/api.md#enum-signal) — full list with
  payload schemas
- [Middleware](middleware.md) — when you need per-task wrappers
  instead
- [Rate limiting](rate-limiting.md) — example consumer of
  `TASK_RATE_LIMITED`
