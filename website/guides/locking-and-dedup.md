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
is derived from it (`hard_timeout + 30 s`): it is the crash-recovery
deadline, so a task with no hard timeout could hold the lock
indefinitely after a worker crash.

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

- The message is **requeued with a delay** (100 ms, doubling per
  conflict, at most 60 s). It does not block the worker or fail the
  task.
- The worker fires the `TASK_LOCKED` signal on every conflict
  (`lock_conflict_count` counts the conflicts of this message) and logs
  `task_lock_conflict` (INFO, with `lock_name`, `lock_conflict_count`
  and `waited_ms`) on the first conflict and then about once a minute.
- The message keeps waiting as long as the lock changes hands or its
  holder keeps it alive (from 0.12 there is no cap of 10 conflicts).
  It is dead-lettered with `LockWaitExceeded` (error result,
  `TASK_DLQ`, a workflow step marks its workflow failed) only when
  - the same holder has held the lock longer than the key's TTL seen
    at the last change of holder plus 60 s (a stalled holder whose
    lock is kept alive), or
  - `SWARMQ_LOCK_WAIT_MAX_S` (default 6 h) has passed since the first
    conflict.
- `max_age` stays the hard limit: a task with `max_age` expires to the
  DLQ when it is older, whatever the wait.

The wait is measured with the workers' wall clocks
(`x-swarmq-lock-wait-since`, `x-swarmq-lock-holder-since`); keep the
clocks in sync, a skew of up to about 60 s is tolerated. A lock store
that cannot report its holders (a third-party store) logs
`lock_observe_unsupported` once and waits until
`SWARMQ_LOCK_WAIT_MAX_S`; so does a store whose `observe` returns no
`LockObservation`. The gauges `swarmq_lock_waits` and
`swarmq_lock_wait_oldest_seconds` show per worker how many messages it
recently deferred and how long the oldest has waited; a message not
seen again within 3 minutes drops out, so with a backlog of more than
about 2 minutes in the queue the gauges undercount.

A new wait begins when the broker redelivers the message (the worker
died after it had acquired the lock, or returned the message at
shutdown) and on the copy of a later execution (a retry, `dlq retry`;
the header `x-swarmq-lock-wait-gen` records retry count and replay ID
of the waiting execution). An old wait therefore never ends a
redelivered message at once. Classic queues carry no delivery counter;
there a redelivery continues the old wait.

In mixed operation with 0.11 workers of the same app: 0.12 no longer
raises `x-swarmq-lock-conflict-count`, so a 0.11 worker never sees a
0.12 copy at its cap of 10. A 0.11 worker that defers a message itself
still raises that counter and dead-letters at 10 as before, and it
drops nothing of the 0.12 wait state.

If Valkey is unreachable during the acquire (or does not answer within
`SWARMQ_BACKEND_OP_TIMEOUT`), that is not a conflict: the message is
deferred through the delay queue with its own backoff, without
`TASK_LOCKED` and without raising `lock_conflict_count`, and the worker
logs `task_deferred_backend_unavailable` (`gate="lock"`, `lock_name`).
Any other error of the acquire logs `lock_acquire_failed` (ERROR) and
requeues the message in place. An acquire that times out after Valkey
executed it leaves the lock held until its TTL.

Set `max_age` on `lock`-using tasks whose work goes stale, so a
message does not wait for hours behind a long holder.

### Locks while the task runs

The worker keeps a lock it holds alive for as long as the handler
runs (from 0.12). It starts extending one window before the original
deadline `t0 + TTL` (TTL = `hard_timeout + 30 s`; the window is three
ticks, a tick is `min(max(TTL / 3, 50 ms), 20 s)`). Before that,
nothing happens. From a `hard_timeout` of 30 s on the window is 60 s,
so a task that finishes within `hard_timeout - 30 s` costs no extra
round trip. With a shorter `hard_timeout` the window covers the whole
TTL: a handler that runs longer than about a third of its TTL is
extended from the first tick, one round trip per tick. Each extension
adds one window, mutex and semaphore slot alike. A holder killed hard
(SIGKILL, OOM) therefore leaves the lock behind until `t0 + TTL` as
before, or at most one window longer when it was killed while
overrunning.

A handler that ignores its cancellation (task code that swallows
`CancelledError`) outlives `hard_timeout + 30 s` and keeps its lock;
the worker logs `task_lock_held_past_hard_timeout` (WARNING) once.
A blocked event loop does not keep the lock: it blocks the extension
as well, the lock can expire while the handler runs, and the worker
reports that as `task_lock_lost` as soon as the loop runs again. Move
blocking code into [CPU mode](cpu-mode.md), where `hard_timeout` kills
the child process.

If the extension finds the lock gone or taken over (evicted by
`maxmemory-policy`, see [Valkey requirements](../reference/configuration.md#valkey-requirements), Valkey data loss, or a Valkey outage of about two
ticks once the extensions have started, about 40 s with a tick of
20 s), the worker logs `task_lock_lost` (WARNING, `stage="running"`):
a second holder may now run in parallel. The same warning with
`stage="release"` comes when the lock is already gone at release. A
failed extension logs `lock_extend_failed` and is retried at the next
tick; when no extension succeeded before the lock's deadline, the
worker logs `task_lock_lost` with `stage="expired"` without asking
Valkey and stops extending it.

At shutdown the worker releases the locks of handlers that are still
running after the drain. A handler that ignores its cancellation and
keeps running loses its lock at that point.

Limits: an acquirer with a short TTL still sweeps semaphore slots
older than its own TTL, so mixed `hard_timeout` values under one
semaphore name are not supported; and a 0.11 worker that acquires a
slot still shortens the key's TTL to its own.

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
  a terminal state (success, DLQ, cancel, or expiry through `max_age`).
  This blocks repeat schedules for the entire execution window.
- `"start"` — the flag releases once, right before the task starts
  running: after the rate-limit and lock checks. A message that is
  requeued before that (rate limit, lock wait) keeps the flag. If the
  message ends before it starts (cancel, `max_age` expiry, end of a
  lock wait, delivery limit, task not registered), the flag is released
  as well. A follow-up `schedule()` while the task is running will
  enqueue a new one.

Pick `"completion"` for idempotent work ("send welcome email to
user 42") and `"start"` for work where overlapping executions are
fine but duplicated pending messages are not.

### Unique crons

On a static cron, `unique_for` keeps a long run from overlapping the
next fire: the scheduler leader claims the flag before it publishes a
fire, and a fire whose flag is still held is skipped
(`TASK_DUPLICATE_DETECTED`, `source="scheduler.fire"`) and counts as
done. From 0.12 this works on every leader, also one that does not
have the task in its registry: workers report `unique_for` and
`unique_until` with their crons
([cron reports](scheduling-and-retries.md#cron-reports-and-the-leader)).

If the publish of a fire fails after the leader claimed the flag, the
leader releases the flag again, so the retry on the next tick is not
taken for a duplicate. The flag stays when the publish timed out or
lost its connection while waiting for the broker's confirm
(`PublishIndeterminate`), because the message may have arrived. A flag that `schedule(delay=|eta=)` claimed
belongs to the producer and is never released on such a failure.

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

The `client.*` sources fire on the app that calls `schedule()`;
register the handler there. `"scheduler.fire"` fires on the scheduler
leader, a worker: under `swarmq worker` register it in a `--configure`
hook ([Signals](signals.md#registering-a-handler)).

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
