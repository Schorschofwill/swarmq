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

app = SwarmQ(broker, backend)


@app.on_signal(Signal.TASK_FAILED)
async def on_task_failed(task_id, task_name, exception):
    log.error(
        "task_failed",
        task_id=task_id, task_name=task_name,
        error=repr(exception),
    )
```

The handler must be async. It runs inline after the event is
dispatched — no separate event loop, no worker thread.

## Available signals

| Signal | Fires | Kwargs |
|---|---|---|
| `TASK_ENQUEUED` | After a successful `schedule()` | `task_id`, `task_name`, `queue` |
| `TASK_STARTED` | Before `run()` is called | `task_id`, `task_name` |
| `TASK_COMPLETED` | After `run()` succeeds | `task_id`, `task_name`, `duration_ms` |
| `TASK_FAILED` | After `run()` raises, on the last attempt | `task_id`, `task_name`, `exception` |
| `TASK_RETRYING` | Before a retry is published | `task_id`, `retry_count`, `delay` |
| `TASK_CANCELLED` | When a cancel flag is observed mid-run | `task_id`, `task_name` |
| `TASK_EXPIRED` | When `max_age` is exceeded at pickup | `task_id`, `task_name`, `age_seconds` |
| `TASK_LOCKED` | When a task's lock acquisition is in progress | `task_id`, `task_name`, `lock_name` |
| `TASK_RATE_LIMITED` | When the rate limiter defers a message | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` |
| `TASK_DUPLICATE_DETECTED` | When a unique-task SETNX loses the race | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` / `WORKER_HEARTBEAT` | Worker lifecycle | `worker_id` |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | Scheduler leader transitions | `worker_id` |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | Leader swap sources | `worker_id` |

Handlers should accept `**kwargs` defensively. The
`TASK_DUPLICATE_DETECTED` signal's `batch_index` key is present only
when the source is `client.schedule_many` — use
`kwargs.get("batch_index")` to avoid `KeyError`.

## Error semantics

Handler exceptions are **caught per-handler** and logged at WARNING.
The dispatch loop continues to the next handler; the signal does
not abort the task or the worker.

```python
@app.on_signal(Signal.TASK_COMPLETED)
async def broken(task_id, task_name, duration_ms):
    raise RuntimeError("this is fine")
```

The library emits:

```text
signal_handler_error signal=task_completed handler=broken error=...
```

and moves on. This is intentional — a buggy metrics handler must not
be able to poison task execution.

## Multiple handlers per signal

Register as many as you need. Handlers run in registration order:

```python
@app.on_signal(Signal.TASK_FAILED)
async def to_sentry(task_id, task_name, exception):
    await sentry.capture(exception, tags={"task": task_name})


@app.on_signal(Signal.TASK_FAILED)
async def to_pagerduty(task_id, task_name, exception):
    if isinstance(exception, DatabaseDown):
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

When in doubt: if the code only looks at `task_id` and `exception`,
it is a signal handler. If it needs `pre_execute` + `post_execute`
symmetry, it is a middleware.

## Typical uses

### Sentry / error aggregator integration

```python
@app.on_signal(Signal.TASK_FAILED)
async def capture(task_id, task_name, exception):
    await sentry.capture(exception, tags={"task": task_name})
```

### Slack notification for DLQ drops

```python
@app.on_signal(Signal.TASK_FAILED)
async def alert(task_id, task_name, exception):
    if task_name in ("charge_card", "send_invoice"):
        await slack.post(
            channel="#ops",
            text=f":fire: {task_name} failed: {task_id}",
        )
```

### Audit log for cancellations

```python
@app.on_signal(Signal.TASK_CANCELLED)
async def audit(task_id, task_name):
    await audit_log.insert(
        event="task_cancelled",
        task_id=task_id, task_name=task_name,
    )
```

### Prometheus counters without the exporter

If you are not using `swarmq[metrics]`, you can emit Prometheus
metrics from handlers directly:

```python
FAILURES = Counter("task_failures", "Task failures", ["task"])

@app.on_signal(Signal.TASK_FAILED)
async def count(task_id, task_name, exception):
    FAILURES.labels(task=task_name).inc()
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

Register one handler, schedule a task, watch the worker log:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-format human
```

Your handler's `log.info("...")` line appears immediately after
`TASK_COMPLETED`, in the same log stream.

## See also

- [`Signal` enum](../reference/api.md#enum-signal) — full list with
  payload schemas
- [Middleware](middleware.md) — when you need per-task wrappers
  instead
- [Rate limiting](rate-limiting.md) — example consumer of
  `TASK_RATE_LIMITED`
