# Locking and deduplication

You want to keep the same logical work from running in parallel
(locking), or keep the same call from being enqueued twice
(deduplication). SwarmQ offers both, each at its own point.

## Which one do you need?

- **Lock** gates **execution**. Two deliveries whose lock renders to
  the same name never run `run()` at the same time (with a semaphore:
  never more than `max_concurrency` of them).
- **Unique task** gates **scheduling**. Two `schedule()` calls with the
  same arguments produce one task. The second call silently returns
  the task ID of the first.

They compose, and a task can declare both. A common pattern is
`unique_for` to absorb client retries and `lock` to serialise the
actual execution.

## Locking

### Declare a lock

```python
class SyncUser(Task):
    task_info = TaskInfo(
        name="sync_user",
        queues=["users"],
        lock="user:{user_id}",
        hard_timeout=60,
    )

    async def run(self, user_id: int) -> None:
        ...
```

The lock name can be a format string that names parameters of `run()`,
passed by keyword or by position. The worker renders it against the
message's arguments: `schedule("sync_user", user_id=42)` locks
`user:42`. The lock lives in Valkey under
`[<namespace>:]sq:lock:<name>`.

`hard_timeout` is **required** with `lock`; `TaskInfo` raises
`ValueError` at import without it. The lock's TTL is
`hard_timeout + 30 s`. It is the crash-recovery deadline: when a worker
dies hard while holding the lock, the next holder waits at most that
long.

### Mutex vs semaphore

With the default `max_concurrency=1` the lock is a mutex. Set it higher
to allow several executions per lock name:

```python
task_info = TaskInfo(
    name="pdf_render",
    queues=["pdf"],
    lock="pdf",          # one name for all renders
    max_concurrency=4,   # up to 4 at a time
    hard_timeout=300,
)
```

`max_concurrency=4` lets four renders run at once across the whole
fleet, regardless of `--concurrency` or `--processes`.
`max_concurrency` requires `lock`; without it `TaskInfo` raises
`ValueError`.

### Lock acquisition behaviour

When the worker picks up a message whose lock is held:

- The message is **requeued with a delay** (100 ms, doubling per
  conflict, at most 60 s). It does not block the worker and does not
  fail the task.
- The worker fires `TASK_LOCKED` on every conflict
  (`lock_conflict_count` counts the conflicts of this message) and logs
  `task_lock_conflict` (INFO, with `lock_name`, `lock_conflict_count`
  and `waited_ms`) on the first conflict and then about once a minute.
- The message keeps waiting as long as the lock changes hands or its
  holder keeps it alive. It is dead-lettered with `LockWaitExceeded`
  (error result, `TASK_DLQ`, a workflow step fails its workflow) only
  when
    - the same holder has held the lock longer than the key's TTL seen
      at the last change of holder plus 60 s (a stalled holder whose
      lock is kept alive), or
    - `SWARMQ_LOCK_WAIT_MAX_S` (default 6 h) has passed since the first
      conflict.
- `max_age` stays the hard limit: a task with `max_age` expires to the
  DLQ when it is older, however long it has waited.

The wait is measured with the workers' wall clocks
(`x-swarmq-lock-wait-since`, `x-swarmq-lock-holder-since`). Keep the
clocks in sync; a skew of up to about 60 s is tolerated. A lock store
that cannot report its holders (a store of your own whose `observe` is
missing or returns no `LockObservation`) logs `lock_observe_unsupported`
once and waits until `SWARMQ_LOCK_WAIT_MAX_S`. The gauges
`swarmq_lock_waits` and `swarmq_lock_wait_oldest_seconds` show per
worker how many messages it recently deferred and how long the oldest
has waited. A message not seen again within 3 minutes drops out, so
with more than about 2 minutes of backlog in the queue the gauges
undercount.

A new wait begins when the broker redelivers the message (the worker
died after acquiring the lock, or handed the message back at shutdown)
and on the copy of a later execution (a retry, `swarmq dlq retry`; the
header `x-swarmq-lock-wait-gen` records retry count and replay ID of
the waiting execution). An old wait therefore never ends a redelivered
message at once. Classic queues carry no delivery counter; there a
redelivery continues the old wait.

If Valkey is unreachable during the acquire (or does not answer within
`SWARMQ_BACKEND_OP_TIMEOUT`), that is not a conflict. The message is
deferred through the delay queue with its own backoff, without
`TASK_LOCKED` and without raising `lock_conflict_count`, and the worker
logs `task_deferred_backend_unavailable` (`gate="lock"`, `lock_name`).
Since the acquire may have run in Valkey anyway, the worker releases
the lock in the background, checked against its own owner token. Any
other error of the acquire, including an error reply from Valkey, logs
`lock_acquire_failed` (ERROR) and requeues the message in place.

Set `max_age` on tasks whose work goes stale, so a message does not
wait for hours behind a long holder.

### Recovering a stuck lock

A lock that a stalled holder keeps alive, or that outlived its holder,
ends the wait of the queued messages with `LockWaitExceeded`. The DLQ
copy carries the lock name in `x-swarmq-lock-name`, and the error text
names the key to delete. Remove the lock only when you are sure its
holder no longer runs:

```bash
valkey-cli DEL sq:lock:user:42   # with a namespace: <namespace>:sq:lock:user:42
```

From Python, `await app.backend.locks.force_release("user:42",
max_concurrency=1)` does the same with one `DEL`; it returns `True`
when a lock existed and `False` when there was none. Then replay the
DLQ messages with `swarmq dlq retry`.

### Locks while the task runs

The worker keeps a lock it holds alive for as long as the handler
runs. It starts extending one window before the original deadline
`t0 + TTL` (TTL = `hard_timeout + 30 s`; the window is three ticks, a
tick is `min(max(TTL / 3, 50 ms), 20 s)`). Before that, nothing
happens. From a `hard_timeout` of 30 s on the window is 60 s, so a task
that finishes within `hard_timeout - 30 s` costs no extra round trip.
With a shorter `hard_timeout` the window covers the whole TTL: a
handler that runs longer than about a third of its TTL is extended from
the first tick, one round trip per tick. Each extension adds one
window, for a mutex and a semaphore slot alike. A holder killed hard
(SIGKILL, OOM) therefore leaves the lock behind until `t0 + TTL`, or at
most one window longer when it was killed while overrunning.

A handler that ignores its cancellation (task code that swallows
`CancelledError`) outlives `hard_timeout + 30 s` and keeps its lock;
the worker logs `task_lock_held_past_hard_timeout` (WARNING) once. A
blocked event loop does not keep the lock: it blocks the extension as
well, the lock can expire while the handler runs, and the worker
reports `task_lock_lost` as soon as the loop runs again. Move blocking
code into [CPU mode](cpu-mode.md), where `hard_timeout` kills the child
process.

If the extension finds the lock gone or taken over (evicted by
`maxmemory-policy`, see
[Valkey requirements](../reference/configuration.md#valkey-requirements),
Valkey data loss, or a Valkey outage of about two ticks once the
extensions have started, about 40 s with a tick of 20 s), the worker
logs `task_lock_lost` (WARNING, `stage="running"`): a second holder may
now run in parallel. The same warning with `stage="release"` comes when
the lock is already gone at release. A failed extension logs
`lock_extend_failed` and is retried on the next tick; when no extension
succeeded before the lock's deadline, the worker logs `task_lock_lost`
with `stage="expired"` without asking Valkey and stops extending it.

At shutdown the worker releases the locks of handlers that still run
after the drain. A handler that ignores its cancellation and keeps
running loses its lock at that point.

Limit: an acquirer with a short TTL sweeps semaphore slots older than
its own TTL, so mixed `hard_timeout` values under one semaphore name
are not supported.

### Lock-render failures

If the template names a parameter the message does not carry (a typo,
an old message of a renamed task), rendering fails, and the message is
routed to the DLQ with an `x-swarmq-lock-render-error` header: error
result, `TASK_DLQ`, a workflow step fails its workflow, and the worker
logs `lock_name_render_failed` (ERROR). The message is not requeued. A
broken lock template is a bug, and you want it loud.

The same applies to every other render error (`"{items[0]}"` with an
empty list, `"{user.id}"` on a dict), to a template that renders to an
empty or blank name (`lock_name_rendered_empty`), and to a rendered
name that starts with `{sched}`, which is reserved for swarmq's
internal locks such as the scheduler leader. Start lock templates with
a literal (`"order:{order_id}"`, not `"{order_id}"`); then no payload
value can produce a reserved or empty name.

A literal `lock` of `""` or whitespace would collide with every other
such task under the bare key `sq:lock:`; `TaskInfo` rejects it at
import.

## Deduplication (unique tasks)

### Declare dedup

```python
class SendWelcomeEmail(Task):
    task_info = TaskInfo(
        name="send_welcome",
        queues=["default"],
        unique_for=3600,            # dedup within the hour
        unique_until="completion",  # release the flag at the terminal state
    )

    async def run(self, user_id: int) -> None:
        ...
```

`unique_for=3600` means: for 3600 seconds after the first call, a
second call with the same `(name, args, kwargs)` returns the first
task's ID without enqueueing anything. The producer claims a flag in
Valkey (`[<namespace>:]sq:unique:<hash>`) before it publishes.

`unique_for` must be greater than 0 and at most `315360000` (ten
years); a `float` is rounded up to whole seconds. `0`, negative values,
`nan` and `inf` raise `ValueError`; switch dedup off with `None`.
`unique_until` without `unique_for` has no effect.

With `delay=` or `eta=`, a producer that resolves `unique_for` (from
the call or its task registry) claims the flag at the call, for
`unique_for` plus the delay: an equal call is a
duplicate from the call until `unique_for` after the fire (see
[immediate vs deferred execution](scheduling-and-retries.md#immediate-vs-deferred-execution)).

### When to release the flag

`unique_until` picks the release point:

- `"completion"` (default): the flag lives until the task reaches a
  terminal state: success, final failure (DLQ, or a stored error result
  when dead-lettering is switched off), cancel, or expiry through
  `max_age`. This blocks repeat calls for the whole execution, retries
  included.
- `"start"`: the flag is released once, right before the task starts
  running, after the rate-limit and lock checks. A message that is
  requeued before that (rate limit, lock wait) keeps the flag. If the
  message ends before it starts (cancel, `max_age` expiry, end of a
  lock wait, delivery limit, task not registered), the flag is released
  as well. A follow-up `schedule()` while the task runs enqueues a new
  one, and during the backoff of a retry there is no dedup.

A hand-back that fails because the channel or connection is lost
counts as done: the broker redelivers the unacknowledged message, and
the flag stays held in both modes.

Pick `"completion"` for work that must happen once ("send the welcome
email to user 42") and `"start"` for work where overlapping runs are
fine but duplicate pending messages are not.

### Unique crons

On a static cron, `unique_for` keeps a long run from overlapping the
next fire: the scheduler leader claims the flag before it publishes a
fire, and a fire whose flag is still held is skipped
(`TASK_DUPLICATE_DETECTED`, `source="scheduler.fire"`) and counts as
done. This works on every leader, also one that does not have the task
in its registry: workers report `unique_for` and `unique_until` with
their crons
([cron reports](scheduling-and-retries.md#cron-reports-and-the-leader)).

If the publish of a fire fails after the leader claimed the flag, the
leader releases the flag again, so the retry on the next tick is not
taken for a duplicate. The flag stays when the publish timed out or
lost its connection while waiting for the broker's confirm
(`PublishIndeterminate`), because the message may have arrived. A flag
that `schedule(delay=|eta=)` claimed belongs to the producer and is
never released on such a failure.

### When `schedule()` fails

A claim that `schedule()` or `schedule_many()` took stays until it
expires when the outcome is unknown:

- after `PublishIndeterminate`,
- after a backend outage while storing a `delay`/`eta` entry,
- when the call is cancelled (a timeout around it, `task.cancel()`)
  during the publish or while a `delay`/`eta` entry is stored.

The message or entry may have arrived, so a retry in that window is
reported as a duplicate. Definite failures release the claim, and so
does a backend outage during the claim itself (`schedule()` then
raises `BackendConnectionError`). A cancel before the broker can have
sent the message releases it as well: while the publish waits for a
connection that the broker blocked in a resource alarm, opens the
publish channel, waits for the channel lock or backs off after an
attempt that sent nothing. The broker then raises
`PublishCancelledUnsent`, a subclass of `asyncio.CancelledError`
([error catalog](../reference/errors.md#outside-the-hierarchy-publishcancelledunsent)).

### Per-call override

`unique_for` and `unique_until` can be overridden per `schedule()`
call:

```python
# Disable dedup for this call only.
await app.schedule("send_welcome", user_id=42, unique_for=None)

# Shorter window for this call.
await app.schedule("send_welcome", user_id=42, unique_for=60)
```

Pass `None` to switch dedup off, a number to override. Omit the
keyword to use the `TaskInfo` default. A producer without a task
registry knows no default: there only the keyword applies, and the
client warns once per task name (`client_task_defaults_unresolved`).

### How dedup is computed

The dedup key is a SHA-256 hash of the task name and a canonical JSON
encoding of `args` and `kwargs`:

- The order of positional arguments matters.
- The order of keyword arguments and of dict keys does not (the
  encoding sorts them).
- Types matter where JSON keeps them apart (`1` and `"1"` differ).
  `UUID`, `datetime`, `date`, `time` and `Decimal` hash like their
  string form, a `tuple` like a `list`, a `set` like its sorted list.
- `bytes`, `nan`, `inf`, dict keys that are not `str` and objects of
  your own classes (dataclasses, `msgspec.Struct`, plain `Enum` members) raise
  `TypeError` at the call. Convert them first, or switch dedup off for
  the call.

Two clients that call `schedule("sync_user", user_id=42)` from
different processes hash to the same key and dedupe correctly.

### Observing dedup

Every dedup hit logs `dedup_detected` (INFO) and fires
`TASK_DUPLICATE_DETECTED`:

```python
from swarmq import Signal
from swarmq.logging import get_logger

log = get_logger("myapp.dedup")


@app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
async def on_dup(
    existing_task_id, task_name, dedup_hash, source, **kwargs,
):
    log.info(
        "myapp_dedup_hit",
        task=task_name,
        existing_task_id=existing_task_id,
        source=source,
        batch_index=kwargs.get("batch_index"),
    )
```

`source` tells the origin:

- `"client.schedule"`: immediate path.
- `"client.schedule_deferred"`: `delay=` / `eta=` path.
- `"client.schedule_many"`: batch enqueue (with `batch_index`).
- `"scheduler.fire"`: the scheduler leader skipped a cron fire whose
  previous run still holds the flag.

The `client.*` sources fire on the app that calls `schedule()`;
register the handler there. `"scheduler.fire"` fires on the scheduler
leader, a worker: under `swarmq worker` register it in a `--configure`
hook ([Signals](signals.md#registering-a-handler)).

## Combining locks and dedup

Dedup works at the producer, the lock at the worker. Client retries
collapse into one task; messages that render the same lock name run
one after another.

```python
class PollWebhook(Task):
    task_info = TaskInfo(
        name="poll_webhook",
        queues=["default"],
        lock="poll:{endpoint}",
        hard_timeout=120,
        unique_for=30,
        unique_until="start",
    )

    async def run(self, endpoint: str) -> None:
        ...
```

A client that calls `schedule("poll_webhook", endpoint="x")` five
times within 30 s gets the same task ID back from all five calls. A
poll for `x` scheduled after the first one started waits for the lock
until the first one is done.

## Verify

Declare a lock, run two workers, schedule two tasks with the same lock
name, and read the worker logs:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-lifecycle-level INFO
```

Worker 1 logs `task_started` for the first message (visible at INFO
because of `--log-lifecycle-level INFO`; the default level of this
event is DEBUG). Worker 2 logs `task_lock_conflict` with `lock_name`
and `lock_conflict_count` for the second message and defers it. After
worker 1's `task_succeeded`, the deferred message runs and logs
`task_succeeded` as well.

For dedup, schedule twice:

```python
tid1 = await app.schedule("sync_user", user_id=42)
tid2 = await app.schedule("sync_user", user_id=42)
assert tid1 == tid2   # with unique_for set on the task
```

## See also

- [Delivery semantics](../explanation/delivery-semantics.md): the
  idempotency expectations that make this combination necessary
- [`TaskInfo.lock` reference](../reference/configuration.md#locking-and-concurrency)
- [`TaskInfo.unique_for` reference](../reference/configuration.md#deduplication)
- [Signals](signals.md): subscribing to `TASK_LOCKED` and
  `TASK_DUPLICATE_DETECTED`
