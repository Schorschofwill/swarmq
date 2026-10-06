# Locking and deduplication

You want to prevent the same logical work from running in parallel
(locking), or prevent the same call from being enqueued twice
(deduplication). SwarmQ offers both, each with its own decision
point.

## Which one do you need?

- **Lock** — gates **execution**. Two workers that both have the
  message in-flight will not both call `run()` concurrently.
- **Unique task** — gates **scheduling**. Two `schedule()` calls
  with the same args produce one task_id. The second call returns
  the existing owner's task_id silently.

They compose — a task can declare both. A common pattern is
`unique_for` to absorb client retries and `lock` to serialise the
actual execution.

## Locking

### Declare a lock

```python
class SyncUser(Task):
    task_info = TaskInfo(
        name="sync_user",
        queues=["users"],
        lock="user-{user_id}",
        hard_timeout=60,
    )

    async def run(self, user_id: int) -> None:
        ...
```

The lock `name` can be a format string that references kwargs or
positional argument names from `run()`. At pickup, the worker
renders the template against the actual args — `"user-{user_id}"`
becomes `"user-42"` when `schedule("sync_user", user_id=42)` is
dispatched.

`hard_timeout` is **required** when `lock` is set. The lock's TTL
is derived from it (`max(30 s, 0.25 × hard_timeout)`), so a task
with no hard timeout could hold the lock indefinitely after a
worker crash.

### Mutex vs semaphore

By default, `max_concurrency=1` — the lock is a mutex. Set it
higher to cap concurrent executions by key:

```python
task_info = TaskInfo(
    name="pdf_render",
    queues=["pdf"],
    lock="pdf",          # one bucket for all renders
    max_concurrency=4,   # up to 4 in flight
    hard_timeout=300,
)
```

`max_concurrency=4` lets four worker processes render PDFs at once
across the whole fleet, regardless of `--concurrency` or
`--processes`. `max_concurrency` requires `lock`; setting it without
raises `ValueError` at class definition.

### Lock acquisition behaviour

When the worker picks up a message whose lock is held:

- The message is **requeued with a delay**. It does not block the
  worker or fail the task.
- The worker logs `task_lock_conflict` (INFO, with `lock_name` and
  `lock_conflict_count`) and fires the `TASK_LOCKED` signal so you can
  observe it.
- If the task's `max_age` is set, it eventually expires to the DLQ
  rather than retrying forever.

The delay backs off — repeated misses extend the wait. Most
`lock`-using tasks want `max_age` as a safety net so a stuck
holder does not back up the queue forever.

### Lock-render failures

If the template references a kwarg the message does not carry
(typo, old message from a renamed task), the render fails and the
task is routed to the DLQ with an `x-swarmq-lock-render-error` header. The
failure is surfaced to the operator, not silently swallowed, because
a broken lock template is a bug — you want it loud.

### Empty lock names

A `lock` of `""` or whitespace would collapse to the bare Valkey
namespace key `sq:lock:` and collide with every other empty-locked
task. `TaskInfo` rejects this at class definition.

## Deduplication (unique tasks)

### Declare dedup

```python
class SendWelcomeEmail(Task):
    task_info = TaskInfo(
        name="send_welcome",
        queues=["default"],
        unique_for=3600,          # dedup within the hour
        unique_until="completion", # release flag at terminal state
    )

    async def run(self, user_id: int) -> None:
        ...
```

`unique_for=3600` means: within 3600 seconds of the first schedule,
a second call with the same `(name, args, kwargs)` returns the
first task's task_id without enqueueing.

### When to release the flag

`unique_until` picks the release point:

- `"completion"` (default) — the flag lives until the task reaches
  a terminal state (success, DLQ, cancel). This blocks repeat
  schedules for the entire execution window.
- `"start"` — the flag releases the instant a worker picks up the
  message. A follow-up `schedule()` while the task is running will
  enqueue a new one.

Pick `"completion"` for idempotent work ("send welcome email to
user 42") and `"start"` for work where overlapping executions are
fine but duplicated pending messages are not.

### Per-call override

`unique_for` and `unique_until` can be overridden at the
`schedule()` call:

```python
# Disable dedup for this call only.
await app.schedule("send_welcome", user_id=42, unique_for=None)

# Tighter TTL for this call.
await app.schedule("send_welcome", user_id=42, unique_for=60)
```

Pass `None` to disable dedup explicitly; pass an `int` to override.
Omit the kwarg to use the `TaskInfo` default.

### How dedup is computed

The dedup key is a SHA-256 hash of a canonical encoding of
`(task_name, args, kwargs)`. That means:

- Argument order matters for positional args (you are hashing a
  tuple).
- Dict order **does not** matter for kwargs (the encoding sorts by
  key).
- Argument types matter (`1` and `"1"` hash differently).

Two clients that call `schedule("sync_user", user_id=42)` from
different processes will collide on the same hash and dedupe
correctly.

### Observing dedup

Every dedup hit fires `TASK_DUPLICATE_DETECTED`:

```python
from swarmq import Signal
from swarmq.logging import get_logger

log = get_logger("myapp.dedup")


@app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
async def on_dup(
    existing_task_id, task_name, dedup_hash, source, **kwargs,
):
    batch_index = kwargs.get("batch_index")
    log.info(
        "myapp_dedup_hit",
        task=task_name,
        existing_task_id=existing_task_id,
        source=source,
        batch_index=batch_index,
    )
```

`source` distinguishes the origin:

- `"client.schedule"` — immediate path.
- `"client.schedule_deferred"` — `delay=` / `eta=` path.
- `"client.schedule_many"` — batch enqueue.
- `"scheduler.fire"` — the embedded scheduler fired a recurring
  task that deduped against a prior run.

## Combining locks and dedup

Dedup at the client layer, lock at the worker layer. Client retries
collapse to one task; concurrent workers serialise the execution.

```python
class PollWebhook(Task):
    task_info = TaskInfo(
        name="poll_webhook",
        queues=["default"],
        lock="poll-{endpoint}",
        hard_timeout=120,
        unique_for=30,
        unique_until="start",
    )

    async def run(self, endpoint: str) -> None:
        ...
```

A client that retries `schedule("poll_webhook", endpoint="x")` 5
times in 30 s gets one task_id back from all 5 calls. Two workers
that both see the message will sequentially acquire the lock.

## Verify

Declare a lock, schedule the task from two clients concurrently,
and inspect worker logs:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-lifecycle-level INFO
```

Worker 1 logs `task_started` for the first message (visible at INFO
because of `--log-lifecycle-level INFO`; the default level of this
event is DEBUG). Worker 2 logs `task_lock_conflict` with `lock_name`
and `lock_conflict_count` for the second message and requeues it with
a delay. After worker 1's `task_succeeded`, worker 2's redelivered
attempt runs and logs `task_succeeded` as well.

For dedup, schedule twice:

```python
tid1 = await app.schedule("sync_user", user_id=42)
tid2 = await app.schedule("sync_user", user_id=42)
assert tid1 == tid2
```

## See also

- [Delivery semantics](../explanation/delivery-semantics.md) —
  idempotency expectations that make this composition necessary
- [`TaskInfo.lock` reference](../reference/configuration.md#locking-and-concurrency)
- [`TaskInfo.unique_for` reference](../reference/configuration.md#deduplication)
- [Signals](signals.md) — subscribing to `TASK_LOCKED` and
  `TASK_DUPLICATE_DETECTED`
