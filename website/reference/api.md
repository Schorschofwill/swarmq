# API reference

The public API is what `from swarmq import ...` gives you. This page
documents every public class and the methods application code is
expected to call. The library is written in async; every method that
does I/O is a coroutine.

## Module layout

```python
from swarmq import (
    SwarmQ,                    # Application entry point
    RabbitMQBroker,            # Broker (same class as swarmq.broker.rabbitmq)
    ValkeyBackend,             # Backend (same class as swarmq.backend.valkey)
    Task, TaskInfo,            # Task definition
    DeliveryMode,              # at_least_once | fire_and_forget
    Middleware,                # Middleware base class
    MessageContext,            # TypedDict passed to middleware hooks
    Signal,                    # Lifecycle event enum
    SignalDispatcher,          # Signal registry
    ProgressInfo,              # Task progress snapshot
    chain, group, chord,       # Workflow primitives
    sig, Signature,            # Bound task call for workflows
    WorkflowHandle,            # Handle returned by app.apply() / app.workflow()
    setup_dishka, SwarmQProvider, TaskContext,   # Optional Dishka DI
    HealthChecker, HealthServer, serve_health,   # /health + /ready
    Metrics, MetricsServer, serve_metrics,       # Prometheus /metrics
    install_signal_collectors,
    Retry, NoRetry,            # Task-side control flow
    SwarmQError,               # Base exception
    # ...and the rest of the exception hierarchy
)
```

Everything above is in `swarmq.__all__`. The heavy names (`SwarmQ`,
`RabbitMQBroker`, `ValkeyBackend`, `Task`, `Signal`, the health and
metrics classes, the Dishka names, ...) are loaded lazily on first
access, so `import swarmq` stays cheap; they are the same objects as
in their home modules (`from swarmq.broker.rabbitmq import
RabbitMQBroker` keeps working).

## `class SwarmQ`

Main application entry point. Wraps a broker + a backend, owns the
task registry, the middleware stack, and the signal dispatcher.

```python
SwarmQ(
    broker: RabbitMQBroker,
    backend: ValkeyBackend,
    **config_kwargs: Any,
)
```

A `SwarmQ` is both a **client** (for scheduling) and the context a
**worker** reads its config from. Use it as an async context
manager — entering the context connects the broker and the backend;
exiting closes both.

```python
async with SwarmQ(broker, backend) as app:
    await app.run("send_email", to="a@example.com")
```

**Parameters**

| Name | Type | Description |
|---|---|---|
| `broker` | `RabbitMQBroker` | Connected at `__aenter__`. |
| `backend` | `ValkeyBackend` | Connected at `__aenter__`. |
| `**config_kwargs` | — | Forwarded to `SwarmQConfig`. See [Configuration](configuration.md). |

**Notes.** A single `SwarmQ` instance is safe to share across
coroutines on the same event loop. Do *not* share it across
processes — each process builds its own.

### `__aenter__` / `__aexit__`

```python
async def __aenter__(self) -> SwarmQ
async def __aexit__(self, exc_type, exc_val, exc_tb) -> None
```

Connects `broker` then `backend` on enter; closes both on exit. If
the backend fails to connect, the broker connection is rolled back
before the exception propagates.

With `log_queue=True`, the last instance to exit detaches the
[opt-in log queue](logging.md#opt-in-log-queue) at once and then waits
for its remaining records without blocking the event loop, at most
`log_drain_timeout` seconds (default 5). Records not written by then
count as dropped. The wait runs on a thread of its own, so it also works
after the loop's default executor was shut down. If `__aexit__` is
cancelled while waiting, the cancellation propagates at once and the
flush continues in the background up to the deadline.

### `schedule()`

```python
async def schedule(
    task_name: str,
    *args: Any,
    queue: str | None = None,
    delay: float | None = None,
    eta: datetime | None = None,
    unique_for: float | None = _MISSING,
    unique_until: Literal["start", "completion"] = _MISSING,
    **kwargs: Any,
) -> str
```

Enqueue a task and return its `task_id`.

**Parameters**

| Name | Type | Description |
|---|---|---|
| `task_name` | `str` | Name declared in `TaskInfo.name`. |
| `*args`, `**kwargs` | `Any` | Forwarded to `Task.run()`. Must be JSON-compatible. |
| `queue` | `str \| None` | Override the task's default queue. Required in client-only mode (no registry). |
| `delay` | `float \| None` | Seconds to wait before delivery. Mutually exclusive with `eta`. Must be `≥ 0`. |
| `eta` | `datetime \| None` | Absolute fire time. Mutually exclusive with `delay`. Must be in the future (1s lenience). |
| `unique_for` | `float \| None` | Override `TaskInfo.unique_for`, in seconds. Must be `> 0` and finite (`ValueError` otherwise, before any claim, publish or store; since 0.12.3); a `float` is rounded up to whole seconds. Pass `None` to disable dedup for this call. |
| `unique_until` | `"start" \| "completion"` | Override `TaskInfo.unique_until`. |

**Returns.** `str` — the `task_id` (UUIDv7). On a dedup hit, the
*existing* owner's `task_id` is returned silently; a
`TASK_DUPLICATE_DETECTED` signal fires for observability.

**`unique_for=None` with `delay`/`eta`.** The opt-out is stored in the
one-shot entry (`source="cli_unique"` without a dedup hash), and the
leader fires the task without a dedup claim, even if its own `TaskInfo`
sets `unique_for`. This needs both a producer and a leader after
0.12.1: a producer up to 0.12.1 does not store the opt-out, and a
leader up to 0.12.1 falls back to its own `TaskInfo`. In both cases the
leader may drop the task as a duplicate at fire time while another run
with the same arguments holds the flag. If you leave out `unique_for`,
a producer whose registry knows the task claims the flag at the call
with `TaskInfo.unique_for` plus the delay; only a producer without a
registry leaves it to the leader, which applies its own `TaskInfo` at
fire time
([Scheduling and retries](../guides/scheduling-and-retries.md#immediate-vs-deferred-execution)).

**Middleware and signals with `delay`/`eta`.** Since 0.12.3 a deferred
call runs through the enqueue middleware like an immediate one. The
checks of `delay`, `eta` and `unique_for` come first. Then the client
computes the dedup hash from the arguments as passed (before any
middleware), runs `pre_enqueue`, claims the `unique_for` flag, stores
the one-shot entry, runs `post_enqueue` and fires `TASK_ENQUEUED`. A
veto from `pre_enqueue` aborts the call before anything is claimed or
stored, also for a call that would be a duplicate (that call used to
return the existing `task_id`). A duplicate returns the existing
`task_id` after `pre_enqueue`, without `post_enqueue`. In-place changes
a middleware makes to `kwargs` are stored, and the leader publishes
them at fire time; the leader fires without the enqueue middleware
([middleware guide](../guides/middleware.md#deferred-tasks-delayeta)).

**Client-only mode (no registry).** Without a registry the client does
not know the task's `TaskInfo`: `unique_for`, `priority` and the
delivery mode of the worker's task are not applied. Dedup only happens
with an explicit `unique_for=`, the message goes to the plain `queue`
(no `.high`/`.low` suffix), and it is sent `at_least_once`. The client
logs a WARNING `client_task_defaults_unresolved` (with `task_name`) once
per task name and client instance, for at most 1000 names. To apply the
defaults, build the producer with the same tasks registered, e.g. via a
`SwarmQ` app that imports the task modules.

**Raises.**

- `TaskNotFoundError` — task_name not in the registry (registry mode).
- `ValueError` — `delay < 0`, both `delay` and `eta` set, `eta` in the past,
  `unique_for` `<= 0`, `inf` or `nan`.
- `BrokerConnectionError` — broker unreachable at publish.
- `BackendResponseError` — with `delay`/`eta`: the scheduler store holds
  a different entry under the one-shot's name, or refused the write. On
  a conflict, and when Valkey is unreachable while storing, the call's
  own `unique_for` claim stays until its TTL ends, because the entry may
  have been stored and fire; after an error reply it is released.
- Propagated from `pre_enqueue` middleware hooks (with `delay`/`eta`
  too).

**Example.**

```python
task_id = await app.schedule(
    "send_email",
    to="alice@example.com", subject="Welcome",
    delay=60.0,           # deliver in a minute
    unique_for=3600,      # dedup within the hour
)
```

### `schedule_many()`

```python
async def schedule_many(
    tasks: list[tuple[str, tuple, dict]],
    queue: str | None = None,
    *,
    unique_for: float | None = _MISSING,
    unique_until: Literal["start", "completion"] = _MISSING,
) -> list[str]
```

Enqueue many tasks in one batch. Each element is a `(task_name, args,
kwargs)` tuple. Published concurrently via `asyncio.gather`.

**Returns.** `list[str]` — one `task_id` per input, in input order.
On dedup hits, the existing owner's task_id is placed at that index
(list length always equals input length).

`unique_for` follows the same rules as in `schedule()`: `> 0` and
finite, a `float` rounded up to whole seconds.

**Raises.** Same shape as `schedule()`. On partial publish failure,
successfully published task_ids are still written to the result
list, the rest are rolled back, and the first exception is
re-raised. As with `schedule()`, an element whose publish is
indeterminate keeps its `unique_for` claim, since its message may have
arrived; only claims of elements where nothing was sent are released.
If any element is indeterminate, the batch raises its
`PublishIndeterminate`, even when a definite failure sits at a smaller
index.

### `run()`

```python
async def run(
    task_name: str,
    *args: Any,
    queue: str | None = None,
    timeout: float = 10,
    polling: float = 0.2,
    **kwargs: Any,
) -> Any
```

Schedule a task and wait for its result. Convenience wrapper around
`schedule()` + `get_result()`.

**Raises.** Everything `schedule()` raises, plus `ResultNotReadyError`
on timeout, `TaskError` on task failure, `TaskCancelledError` on
cancel.

**Example.**

```python
result = await app.run(
    "fetch_weather", city="Berlin", timeout=30,
)
```

### `get_result()`

```python
async def get_result(
    task_id: str,
    timeout: float = 10,
    polling: float = 0.2,
) -> Any
```

Block until the task result is available, or `timeout` elapses.

Internally subscribes to `[<namespace>:]sq:notify:{task_id}` on Valkey for a
push-based wait, with 5ms-interval polling as a fallback. When
`timeout=0`, performs a single `GET` without Pub/Sub. A negative
`timeout` or `nan` counts as already expired: one `GET`, then
`ResultNotReadyError` if there is no result yet (since 0.12.3; before,
`nan` could crash the event loop when the `GET` hung). `inf` waits
without limit.

**Raises.**

- `ResultNotReadyError` — no result within `timeout` seconds.
- `TaskError` — task failed; `str(exc)` carries the worker's error message
  (`""` for an exception without text). `exc.error_class` names the
  exception class as `module.qualname` (e.g. `builtins.ValueError`, in CPU
  mode the class reported by the child); it is advisory and `None` for
  results of older workers and for failures the worker generates itself
  (hard timeout, drain, lost compute child, expiry).
- `TaskCancelledError` — the task was cancelled with `cancel()`, before
  or during execution (the stored result carries the cancel flag). The
  task itself never sees this exception; inside `run()` a cancel
  arrives as `asyncio.CancelledError`.

### `get_results()`

```python
async def get_results(
    task_ids: list[str],
    timeout: float = 10,
    poll_interval: float = 0.005,
) -> list[Any]
```

Batch-poll version of `get_result`. Uses `MGET` to fetch many at
once, every `poll_interval` seconds, without Pub/Sub. It reads at least
once: `timeout=0` is a single `MGET`, and when the timeout expires while
waiting a last `MGET` runs before it gives up (since 0.12.3; before,
`timeout=0` read nothing and reported every task as not ready). A
negative `timeout` or `nan` behaves like `0`; `inf` waits without limit.
Raises `ResultNotReadyError` with the missing task_ids when any is not
ready in time, and `TaskError` or `TaskCancelledError` like
`get_result()`.

### `cancel()`

```python
async def cancel(task_id: str) -> bool
```

Request cancellation of a task. Fire-and-forget: sets a cancel flag
and returns immediately. The flag lives `max(24 h, result_ttl)`. For a
deferred task (`schedule(delay=|eta=)`) that has not fired yet, the
wait until its fire time is added, and its `max_age` + 300 s when the
client's registry knows the task. A cancelled task that waits longer
than `result_ttl` in the queue therefore no longer runs.

Once the worker has handled the cancelled delivery (cancelled before
the start or while running), it does not delete the flag but shortens
it to at most 1 h (`PEXPIRE … LT`). A duplicate of the same message
stays cancelled; a flag set with a shorter TTL is never extended.

**Returns.** `True` when the flag was newly set, `False` when the flag
already exists: a prior `cancel()` call is still in flight, or the
task was cancelled within the last hour.

**Raises.** `ValueError` on empty `task_id`;
`BackendConnectionError` when Valkey is unreachable.

!!! warning "Stale cancel flag"
    If the same `task_id` is enqueued again while the flag lives
    (deterministic IDs; at least for 1 h after a cancelled delivery
    was handled), the new task is cancelled too, and `cancel()` returns
    `False`. Mitigations: let SwarmQ generate UUIDv7 task IDs (the
    default), or pass a shorter `ttl_ms` directly through the
    underlying client: `await app.client.cancel(task_id, ttl_ms=...)`.
    The worker never extends a shorter flag.

### `get_progress()` / `watch_progress()`

```python
async def get_progress(task_id: str) -> ProgressInfo | None
def watch_progress(task_id: str) -> AsyncIterator[ProgressInfo]
```

Read task progress written by `Task.update_progress(current, total,
message)`.

- `get_progress` is a one-shot read; returns `None` if no progress
  has been recorded (never started, TTL expired, unknown task_id,
  or malformed HASH).
- `watch_progress` yields progress updates as an async iterator.
  It subscribes to the Pub/Sub channel first, then yields any HASH
  state already recorded, then streams subsequent updates. The
  iterator never completes on its own — `break` when satisfied.

**Example.**

```python
async for info in app.watch_progress(task_id):
    print(f"{info.percent:.1f}%  ETA: {info.eta_seconds}s")
    if info.percent >= 100.0:
        break
```

### `apply()`

```python
async def apply(
    workflow: Signature | Chain | Group | Chord,
) -> WorkflowHandle
```

Start a workflow built with `chain`, `group`, `chord` or a single
`Signature` and return a [`WorkflowHandle`](#class-workflowhandle).
Fire-and-forget: the topology and initial state are written to Valkey,
the first steps are enqueued as `at_least_once` messages, and the
workers advance the rest. The call returns immediately; the workflow
does not depend on the calling process. The workflow state lives for
`result_ttl` seconds.

`pre_enqueue` middleware runs for every initial step before any state
is written, so a rejecting middleware leaves nothing behind. Each step
fires `TASK_ENQUEUED` and `WORKFLOW_STEP_ENQUEUED`.

**Raises.** `WorkflowValidationError` (from the primitives, at
construction), `BrokerConnectionError` when the initial steps cannot
be published (the workflow is then marked failed), anything a
`pre_enqueue` hook raises.

### `workflow()`

```python
async def workflow(workflow_id: str) -> WorkflowHandle
```

Reattach to a running or finished workflow by id, e.g. from another
process. **Raises** `WorkflowNotFound` when no state exists for the id
(never started or expired). The cancel flag that the handle's
`cancel()` sets lives `max(24 h, result_ttl)`, as for a handle from
`apply()` (since 0.13.0; before, a reattached handle used 24 h and one
from `apply()` used `result_ttl`). The lower-level
`Client.workflow(workflow_id, *, cancel_ttl_ms=None)` takes that lifetime
in milliseconds; `None` means 24 h.

**Example.**

```python
from swarmq import chain, chord, group, sig

handle = await app.apply(
    chord(group(sig("fetch", url) for url in urls), sig("merge"))
)
print(handle.workflow_id)

same = await app.workflow(handle.workflow_id)
merged = await same.get(timeout=60)
```

### `add_middleware()`

```python
def add_middleware(self, middleware: Middleware) -> None
```

Register a middleware instance. Safe to call before or after
`start_worker()`; the middleware chain is resolved per operation, so
newly added middleware is visible from the next enqueue or execute.

### `on_signal()`

```python
def on_signal(signal: Signal) -> Callable[[AsyncHandler], AsyncHandler]
```

Return a decorator that registers an async handler for a lifecycle
signal. Handlers run sequentially in registration order; exceptions
are logged at WARNING and do not abort the dispatch loop.

**Example.**

```python
from swarmq.logging import get_logger

log = get_logger("myapp.signals")


@app.on_signal(Signal.TASK_FAILED)
async def on_failure(task_id, task_name, exception, **kwargs):
    # ``exception`` is the message string (str(exc)), not the exception
    # object. For the real exception use Middleware.on_error.
    log.warning("myapp_task_failed", task_id=task_id, task_name=task_name, reason=exception)
```

Accept `**kwargs` in every handler: payloads gain fields over time
(`queue` was added to the task signals), and a fixed signature then
raises `TypeError`. A raising handler is logged as
`signal_handler_error` (WARNING, logger `swarmq.signals`, with
`error_type` and `error`); a worker lifecycle handler that exceeds its
time budget is logged as `signal_handler_timeout`.

### `start_worker()`

```python
async def start_worker(self) -> None
```

Enter the consumer loop. Returns on shutdown (SIGTERM/SIGINT).
Called by the CLI (`swarmq worker`); rarely invoked directly from
application code. If the start is cancelled or fails after the embedded
scheduler started, its election and poll tasks are stopped before the
exception propagates. With `pool="cpu"` it raises
`swarmq.supervision.ForkSourceRejected` after a graceful shutdown when
the pool rejected a silently restarted forkserver; see the
[error catalog](errors.md#outside-the-hierarchy-forksourcerejected).

## `class TaskInfo`

Frozen declarative config for a task. Field validation runs at
class definition time — a bad value raises `ValueError` on import,
not at first schedule.

```python
TaskInfo(
    name: str,
    queues: list[str],
    *,
    soft_timeout: int | None = None,
    hard_timeout: int | None = None,
    delivery: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
    result_ttl: float | None = None,
    max_retries: int = 0,
    retry_backoff: str = "exponential",
    retry_exceptions: list[type[BaseException]] | None = None,
    retry_base_delay: float = 1.0,
    rate_limit: str | None = None,
    lock: str | None = None,
    max_concurrency: int = 1,
    cron: str | None = None,
    unique_for: float | None = None,
    unique_until: Literal["start", "completion"] = "completion",
    max_age: int | None = None,
    priority: Literal["high", "normal", "low"] = "normal",
    timezone: str | None = None,
    dead_letter: bool | None = None,
)
```

`result_ttl` and `unique_for` take a `float` such as
`timedelta(hours=1).total_seconds()`: a whole value becomes `int`, a
fraction is rounded up to the next second, `inf` and `nan` raise
`ValueError` (since 0.12.3). Field semantics and validation rules live
in [Configuration reference — TaskInfo fields](configuration.md#taskinfo-fields).

## `next_fire_times()`

```python
from swarmq.scheduler import next_fire_times

next_fire_times(
    cron: str,
    start: datetime,
    *,
    timezone: str | None = None,
    count: int = 1,
) -> list[datetime]
```

The next `count` fires of `cron` after `start` (timezone-aware),
calculated with the scheduler's own rule, including the
[rule for clock changes](../guides/scheduling-and-retries.md#clock-changes).
`timezone` is an IANA zone, `None` means UTC; the fires come back
aware in that zone. A naive `start` raises `ValueError` instead of
being read as the host's local time. An invalid expression raises croniter's
`CroniterBadCronError`. Use it to check that a cron converted to a
time zone keeps its fires
([recipe](../guides/scheduling-and-retries.md#migrating-hand-converted-utc-crons)).

## `class Task`

Base class for task definitions. Subclassing with a `task_info =
TaskInfo(...)` attribute registers the class automatically.

```python
class Task:
    task_info: TaskInfo

    async def pre_execution(self) -> None: ...
    async def run(self, *args, **kwargs) -> Any: ...
    async def post_execution(self) -> None: ...

    async def update_progress(
        self, current: int, total: int, message: str = "",
    ) -> None: ...
```

### Methods you override

- `run(*args, **kwargs)` — the task body. Called with the args and
  kwargs the client passed to `schedule()` / `run()`. **Required.**
- `pre_execution()` — called before `run()`. Default: no-op.
- `post_execution()` — called after `run()`, only when
  `pre_execution()` and `run()` succeeded (not on an exception, a
  timeout or a cancel). An exception from it fails the attempt.
  Cleanup for every outcome belongs in `try/finally` in `run()` or a
  Dishka `yield` finalizer. Default: no-op.

### Methods you call from inside `run()`

- `update_progress(current, total, message="")` — record progress.
  Throttled to one write per 500 ms per task instance; the final
  update (`current == total`) always flushes. Safe no-op outside a
  worker context (unit tests).

### Workflow signatures

```python
@classmethod
def s(cls, *args, **kwargs) -> Signature
@classmethod
def si(cls, *args, **kwargs) -> Signature
```

Build a [`Signature`](#workflow-primitives) of this task for `chain`,
`group` and `chord`. `s()` is mutable: inside a chain the previous
step's result is passed as the first positional argument. `si()` is
immutable: the bound arguments are used as they are. Both reject a
`FIRE_AND_FORGET` task at workflow construction.

### What you do not override

Everything else — the registry hook, the worker-wiring instance
attrs (`_task_id`, `_backend`, etc.), the progress lock — is
framework-internal. Subclasses that care only about the task body
only override `run`.

**Example.**

```python
from swarmq import Task, TaskInfo

class RenderReport(Task):
    task_info = TaskInfo(
        name="render_report", queues=["reports"],
        max_retries=3, hard_timeout=300, soft_timeout=240,
    )

    async def run(self, report_id: int) -> str:
        items = await fetch_items(report_id)
        total = len(items)
        for i, item in enumerate(items):
            await render_item(item)
            await self.update_progress(i + 1, total, f"item {item.id}")
        return f"reports/{report_id}.pdf"
```

## `class Middleware`

Base class for middleware — cross-cutting concerns that run around
every task lifecycle event.

```python
class Middleware:
    async def pre_enqueue(self, message: MessageContext) -> None: ...
    async def post_enqueue(self, message: MessageContext) -> None: ...
    async def pre_execute(self, message: MessageContext) -> None: ...
    async def post_execute(
        self, message: MessageContext, result: Any = None,
    ) -> None: ...
    async def on_error(
        self, message: MessageContext, exception: BaseException,
    ) -> None: ...
    async def on_retry(
        self,
        message: MessageContext,
        exception: BaseException,
        retry_count: int,
        delay: float,
    ) -> None: ...
```

All hooks default to an async no-op; override only the ones you care
about.

**Error semantics.**

- `pre_enqueue` exceptions **propagate** — a raising middleware
  aborts the enqueue, with `delay`/`eta` before anything is stored or
  a `unique_for` flag is claimed.
- All other hooks **swallow** exceptions: each error is logged as
  `middleware_hook_failed` (ERROR, logger `swarmq.middleware`, fields
  `hook`, `middleware`, `error_type`, `error` and the traceback) and
  the chain continues.
- A cancelled task gets no `on_error` and no `post_execute`, only
  `pre_execute` before it: a cancel is not an error. Middleware that
  keeps state per task also handles the `TASK_CANCELLED` signal
  (`task_id`, `task_name`, `queue`).

**Registration order.**

- Forward for `pre_*` hooks.
- Reverse for `post_*`, `on_error`, `on_retry` hooks (classic
  "around" semantics).

Register with `app.add_middleware(SomeMiddleware())`. See
[Middleware guide](../guides/middleware.md) for worked examples.

## `TypedDict MessageContext`

The canonical message envelope every middleware hook receives.

```python
class MessageContext(TypedDict):
    task_id: str
    task_name: str
    queue: str
    args: list[Any]
    kwargs: dict[str, Any]
    delivery_mode: str
    retry_count: NotRequired[int]  # worker-side hooks only
```

`retry_count` is present on worker-side hooks (`pre_execute`,
`post_execute`, `on_error`, `on_retry`); absent on client-side hooks
(`pre_enqueue`, `post_enqueue`). Treat it as optional.

Workflow steps pass through `pre_enqueue` and `post_enqueue` as well
(from `app.apply()` and from the advancer inside the worker). Their
context always has `delivery_mode="at_least_once"` and two extra keys,
`workflow_id` and `step_path`, that plain `schedule()` calls do not
carry — read them with `.get()`.

## `enum Signal`

Lifecycle events emitted by SwarmQ. Register handlers with
`@app.on_signal(Signal.FOO)`.

| Signal | Payload (kwargs) | Fires |
|---|---|---|
| `TASK_ENQUEUED` | `task_id`, `task_name`, `queue` | After a publish (`schedule`, `schedule_many`, each workflow step), or, for `delay`/`eta`, once the one-shot is stored (not at fire time; cron fires do not dispatch it). |
| `TASK_STARTED` | `task_id`, `task_name`, `queue` | Before `run()`. |
| `TASK_COMPLETED` | `task_id`, `task_name`, `queue`, `duration_ms` | After a successful `run()`. |
| `TASK_FAILED` | `task_id`, `task_name`, `queue`, `exception` | After **every** failed attempt, also when a retry follows. `exception` is `str(exc)`. |
| `TASK_RETRYING` | `task_id`, `task_name`, `queue`, `retry_count`, `delay` | A retry was scheduled; `retry_count` is the new count, `delay` in seconds. |
| `TASK_CANCELLED` | `task_id`, `task_name`, `queue` | Cancelled before or during execution. No `on_error`/`post_execute` follows. |
| `TASK_EXPIRED` | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` | Picked up older than `max_age`; routed to the DLQ. |
| `TASK_LOCKED` | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` | The task's lock was held by another execution (lock conflict); the message is requeued, or dead-lettered as `LockWaitExceeded` when the wait ends. |
| `TASK_RATE_LIMITED` | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` | Rate limit hit; message delayed. |
| `TASK_DUPLICATE_DETECTED` | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] | `unique_for` dedup hit. |
| `TASK_DLQ` | `task_id`, `task_name`, `queue`, `error` | Terminal: the task was dead-lettered (retries exhausted or not retryable). Fires once. |
| `TASK_REJECTED` | `queue`, `reason`, `task_id`, `task_name`, `error` — `task_id`/`task_name` may be `None` | The worker rejected a message it cannot run (`body_too_large`, `undecodable`, `middleware_chain_factory`: a copy with retention and `x-swarmq-traceback` in `.XQ`, the broker DLX as fallback), a dead-letter publish failed and the message went to the broker DLX without diagnostic headers (`dlq_publish_failed`), or an unregistered task was dead-lettered after `SWARMQ_UNKNOWN_TASK_GRACE_S` (`unregistered_task`, after `TASK_DLQ`). |
| `TASK_UNKNOWN_DEFERRED` | `task_id`, `task_name`, `queue`, `delay`, `unknown_since` | The worker does not register the task and deferred the message within `SWARMQ_UNKNOWN_TASK_GRACE_S`; `delay` in seconds, `unknown_since` the first contact in Unix milliseconds. Once per deferral; counted in `swarmq_tasks_unknown_deferred_total`. |
| `WORKFLOW_STEP_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` | After each workflow step publish, in addition to `TASK_ENQUEUED`. |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | `worker_id` | Worker start / graceful shutdown. |
| `WORKER_HEARTBEAT` | `worker_id`, `uptime_seconds` | Every `SWARMQ_METRICS_SAMPLE_INTERVAL` seconds; only while metrics are enabled. |
| `WORKER_RECYCLING` | `worker_id`, `kind`, `slot`, `reason`, `value` | A worker (`kind="worker"`) or compute child (`kind="compute_child"`) is recycled; `reason` is `rss`, `tasks` or `forced`. |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | `worker_id` | Leader election and renewal. |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | `worker_id` | Leader hands over on shutdown / takes over after an expired lock. |
| `SCHEDULER_FIRE_SKIPPED` | `worker_id`, `schedule_name`, `skipped_count` | Catch-up after downtime skipped cron occurrences. |
| `SCHEDULER_CRON_CONFLICT` | `worker_id`, `schedule_name`, `fingerprints` | Several definitions of one static cron have been live for more than 10 minutes; the leader emits it on every adoption tick while the conflict lasts. |
| `SCHEDULER_LEADER_OUTDATED` | `worker_id`, `leader`, `leader_version`, `outdated` | A non-leader saw a scheduler leader running a swarmq below the minimum leader version (currently 0.12; `leader_version` `None` means before 0.10.0; an unreadable version does not count); once per leader with `outdated=True`, again with `outdated=False` when a current leader follows. |
| `SCHEDULER_CRON_UNOWNED` | `worker_id`, `schedule_name`, `task_name`, `held`, `held_count` | The leader holds a static cron no live worker reports and it does not register itself (`held=True`), or releases it (`held=False`); once per transition, `held_count` is the number of held crons afterwards. Sets `swarmq_scheduler_crons_held`. |
| `SCHEDULER_VALKEY_CHECKED` | `worker_id`, `status`, `policy`, `maxmemory`, `used_memory` | The leader checked Valkey's eviction policy and memory again, at most once per hour; `status` is `safe`, `unsafe` or `unknown`, `maxmemory` `0` means no limit, `None` unknown. Sets `swarmq_valkey_memory_used_ratio` (only with a limit). |
| `SCHEDULER_QUEUE_ORPHANED` | `worker_id`, `queue`, `orphaned`, `message_count`, `orphaned_count` | The leader found a reported queue with messages, no consumer and no live worker reporting it (`orphaned=True`), or the queue is no longer orphaned (`orphaned=False`: drained, consumed, deleted or reported again); once per transition, `orphaned_count` is the number of orphaned queues afterwards. Sets `swarmq_scheduler_queues_orphaned`. |
| `SCHEDULER_FIRE_UNROUTABLE` | `worker_id`, `schedule_name`, `queue`, `kind` | The leader's publish was returned (no such queue, BZ-03): `kind="cron_lost"` for a lost cron fire, `"oneshot_dropped"` for a deferred task dropped after `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`. Counted in `swarmq_scheduler_fires_unroutable_total`. |

Handlers must accept `**kwargs`. Use `kwargs.get("batch_index")` for the
`TASK_DUPLICATE_DETECTED`-only field, since non-batch sources do
not include it. `worker_id` is `<base>:<slot>` under the supervisor
and `<base>:p<pid>` embedded; see
[worker identity](configuration.md#worker-identity).

## Workflow primitives

```python
from swarmq import Signature, chain, chord, group, sig

sig(task_name: str, *args, immutable: bool = False, **kwargs) -> Signature
chain(*steps) -> Chain              # also: a | b | c
group(*members) -> Group            # or group(iterable)
chord(header: Group, body) -> Chord # also: group(...) | body
```

| Primitive | Result |
|---|---|
| `Signature` | One bound task call. Build it with `Task.s()` / `Task.si()` (task class known) or `sig("task_name", ...)` (by name, no import needed). `immutable=True` / `si()` suppresses result injection. |
| `chain(a, b, c)` | Runs the steps one after another; each mutable step receives the previous result as its first argument. The workflow result is the last step's result. |
| `group(a, b, c)` | Runs the members in parallel. The result is the list of member results in definition order, not completion order. |
| `chord(group(...), body)` | Runs the header group, then `body` exactly once with the ordered list of header results. |

Steps may themselves be chains, groups or chords. Limits, checked at
construction and raising `WorkflowValidationError`:

- nesting depth at most `10` (`max_workflow_depth`);
- at most `1000` members per group (`max_group_size`);
- no empty chain or group; a chord header must be a `group(...)`;
- no `FIRE_AND_FORGET` task as a step (`Task.s()`/`si()` check this at
  construction, `sig()` by name at run time).

Start a workflow with [`app.apply()`](#apply).

## `class WorkflowHandle`

Returned by `app.apply()` and `app.workflow(workflow_id)`. Holds only
the `workflow_id`; every call reads the state from Valkey, so a handle
can be rebuilt in any process.

```python
class WorkflowHandle:
    workflow_id: str

    async def status(self) -> str: ...
    async def children(self) -> dict[str, Any]: ...
    async def cancel(self) -> bool: ...
    async def get(self, timeout: float = 30.0) -> Any: ...
```

- `status()` — `"running"`, `"completed"`, `"failed"` or `"cancelled"`.
- `children()` — map of every enqueued step's topology path to
  `{"task_id", "status", "result"?}` with `status` `running`,
  `completed` or `failed`. Empty before any step is enqueued.
- `cancel()` — sets the workflow cancel flag. `get()` raises promptly;
  steps not yet picked up finish as cancelled; a step that is already
  running completes, but no further steps are enqueued. Returns `True`
  when the flag was newly set. The flag lives `max(24 h, result_ttl)`
  for handles from `app.apply()` and `app.workflow()` alike (since
  0.13.0), so a step that waits in a queue longer than `result_ttl` is
  still stopped.
- `get(timeout=30.0)` — waits for a terminal status and returns the
  final result. A negative `timeout` or `nan` reads the state once and
  then raises `ResultNotReadyError` (since 0.12.3; before, it polled
  without end).

**Raises.** `WorkflowNotFound` when the state is gone (`status()`,
`children()`, `get()`),
`ResultNotReadyError` when `get()` times out, `TaskError` for a failed
workflow (with `error_class` of the failing step when known, see
[`get_result()`](#get_result)), `TaskCancelledError` for a cancelled one,
`WorkflowResultExpired` when the workflow completed but its final
result expired before retrieval.

## `class ProgressInfo`

Immutable snapshot of a task's progress, returned by
`app.get_progress()` and yielded by `app.watch_progress()`.

Key fields:

| Field | Type | Description |
|---|---|---|
| `current` | `int` | Current unit count. |
| `total` | `int` | Total unit count. |
| `percent` | `float` | `current / total * 100`, 0.0–100.0. |
| `message` | `str` | Optional status line. |
| `updated_at` | `str` | ISO 8601 timestamp of the update, UTC. |
| `started_at` | `str \| None` | ISO 8601 wall-clock start of the run, UTC (since 0.12.3; before, a monotonic clock value that read as a date in 1970). |
| `eta_seconds` | `int \| None` | Linear extrapolation from the monotonic run time; `None` until the first non-zero progress and on the final update. |

## `enum DeliveryMode`

```python
class DeliveryMode(enum.Enum):
    AT_LEAST_ONCE = "at_least_once"    # default
    FIRE_AND_FORGET = "fire_and_forget"
```

See [Delivery semantics](../explanation/delivery-semantics.md) for
the semantic difference and when each applies.

## `class RabbitMQBroker`

```python
from swarmq import RabbitMQBroker

RabbitMQBroker(
    url: str | None = None,
    queue_type: str = "quorum",
    *,
    connection: aio_pika.abc.AbstractConnection | None = None,
    connection_pool: aio_pika.pool.Pool | None = None,
)
```

Pass exactly one of `url`, `connection` or `connection_pool`; none or
more than one raises `ConfigurationError`. With `url` SwarmQ owns the
connection. A borrowed `connection` or pool is for the producer path
only: SwarmQ opens its own channel and never closes the borrowed
connection, and `start_worker()` rejects such a broker. Inside
`SwarmQ(...)` the queue type comes from `SwarmQConfig.queue_type`
(`SWARMQ_QUEUE_TYPE`).

Async AMQP broker wrapping aio-pika. Implements the `Broker` and
`BrokerAdmin` protocols from `swarmq.contracts` (connect/declare/
publish/consume, the four delivery verbs `ack`/`redeliver`/
`retry_later`/`dead_letter`, queue depths and DLQ operations). The DLQ
read commands of `BrokerAdmin` (`list_dead_letters`, `inspect_dead_letter`,
`retry_dead_letter`, `retry_all_dead_letters`) return a `DeadLetterScan`
(`swarmq.contracts.broker`, since 0.13.0): a tuple `(result, scanned)`
that unpacks as before, plus `hit_limit`, which says whether reading
stopped at the limit (expired entries count towards the limit but not
towards `scanned`). An own `BrokerAdmin` implementation may keep returning
a plain tuple; the CLI then falls back to `scanned >= limit`. Code that
checks `type(result) is tuple` needs adjusting. All
configuration (queue type, prefetch, reconnect policy) flows through
`SwarmQConfig` and `TaskInfo` — you do not call methods on this class
directly from application code.

## `class ValkeyBackend`

```python
from swarmq import ValkeyBackend

ValkeyBackend(
    url: str | None = None,
    *,
    client: valkey.asyncio.Valkey | None = None,
    pool: valkey.asyncio.ConnectionPool | None = None,
    keys: KeySpace | None = None,
    eviction_check: str | None = None,
    namespace: str | None = None,
    connect_timeout_s: float | None = None,
)
```

Pass exactly one of `url`, `client` or `pool`; none or more than one
raises `ConfigurationError`. A borrowed `client` is never closed by
SwarmQ, a `pool` keeps its own lifecycle; both are for the producer
path only (`start_worker()` rejects them).

- `namespace` — the [namespace](configuration.md#namespace-several-apps-on-one-valkey)
  of every key and channel; without it `SWARMQ_NAMESPACE` applies. If
  both are set and differ, `SwarmQ` raises `ConfigurationError`
  (exit `78`) ([where the namespace comes from](configuration.md#where-the-namespace-comes-from)).
- `keys` — a ready `KeySpace` (`swarmq.keyspace`) instead of
  `namespace`; passing both is a `ConfigurationError`.
- `eviction_check` — `strict`, `warn` or `off`, as
  `SWARMQ_VALKEY_EVICTION_CHECK`, which applies without it.
- `connect_timeout_s` — overall deadline for the `PING` in `connect()`,
  including retries; wins over `SwarmQConfig.backend_connect_timeout_s`
  and `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (default 30 s). Must be `> 0`
  and at most `threading.TIMEOUT_MAX`, otherwise `ConfigurationError` at
  construction. When it expires, `connect()` raises
  `BackendConnectionError`.

Async Valkey client backing results, locks, rate limits, cancel
flags, scheduler state, and leader election. Implements the
`ResultBackend` protocol from `swarmq.contracts` and exposes the
coordination stores as `backend.locks`, `.rate_limit`, `.chords`,
`.schedules` and `.execution_guard` (available after `connect()`). Wire through `SwarmQ`
as the second positional argument. Since 0.2.0 the constructor
signatures of `Worker` and `Client` name these contracts instead of
the concrete classes — existing code keeps working unchanged.

### Coordination store protocols

`swarmq.contracts` defines the protocols the worker and the scheduler
call on these stores. You only need them when you write a store of your
own (for tests or another backend). Excerpt of the signatures whose
contract changed in 0.9.0:

```python
from swarmq.contracts import RateLimitStore, ScheduleStore

class RateLimitStore(Protocol):
    async def check(self, name: str, *, max_requests: int,
                    window_ms: int) -> RateLimitDecision: ...

class ScheduleStore(Protocol):
    async def commit_fire(self, name: str, *, next_fire_ts_ms: int | None,
                          payload: bytes | None, owner: str,
                          fired_ts_ms: int | None = None) -> bool: ...
    async def claim_fire(self, fire_key: str, ttl_ms: int, *,
                         owner: str | None = None,
                         takeover: str | Sequence[str] | None = None) -> bool: ...
    async def release_fire_claim(self, fire_key: str, *,
                                 owner: str | None = None) -> None: ...
```

- `RateLimitStore.check` must raise `BackendConnectionError` when the
  backend is unreachable (connection error, timeout). Only then does the
  worker defer the message with backoff; any other exception, including
  `BackendResponseError`, requeues it in place.
- `ScheduleStore.commit_fire(fired_ts_ms=...)` applies the commit only
  while the schedule still has exactly this score in the fire index; a
  commit discarded because the schedule was disabled, removed or
  rescheduled since the poll still returns `True` (`False` means only
  that the leader check failed).
- `ScheduleStore.claim_fire(owner=..., takeover=...)` writes the
  in-flight claim with the owner token atomically. It returns `True`
  when the claim was free or held one of the `takeover` tokens (one token
  or a sequence); the claim then belongs to `owner` with a fresh TTL.
- `ScheduleStore.release_fire_claim(owner=...)` deletes the claim only
  while it still holds this token (compare-and-delete).

Worker and scheduler always pass the new keywords, so a third-party
store that does not accept them raises `TypeError` on every fire.

Contract changes in 0.10.0. A store of your own must implement these
methods; the scheduler and `swarmq schedule status` call them:

```python
from swarmq.contracts import CronReport, CronReports, LeaderInfo, ScheduleStore

class ScheduleStore(Protocol):
    async def put(self, name: str, payload: bytes,
                  fire_ts_ms: int | None) -> None: ...
    async def fire_ts(self, name: str) -> int | None: ...
    async def commit_fire(self, name: str, *, next_fire_ts_ms: int | None,
                          payload: bytes | None, owner: str,
                          fired_ts_ms: int | None = None,
                          fired_payload: bytes | None = None) -> bool: ...
    async def replan_if_unchanged(self, name: str, expected: bytes,
                                  payload: bytes, fire_ts_ms: int) -> bool: ...
    async def report_cron(self, name: str, fingerprint: str, payload: bytes,
                          *, live_window_ms: int) -> None: ...
    async def cron_reports(self) -> CronReports: ...
    async def prune_cron_reports(self, *, max_age_ms: int) -> int: ...
    async def report_worker_version(self, worker_id: str, version: str,
                                    *, ttl_ms: int) -> None: ...
    async def worker_versions(self) -> Mapping[str, str]: ...
    async def set_leader_version(self, owner: str, version: str,
                                 *, ttl_ms: int) -> None: ...
    async def leader_info(self) -> LeaderInfo: ...
```

- `put(..., fire_ts_ms=None)` writes the definition and leaves the
  fire index untouched; a due fire keeps its score.
- `fire_ts(name)` returns the score in the fire index, `None` without
  an entry (disabled or unknown).
- `commit_fire(fired_payload=...)` writes `payload` only while the
  stored definition is still exactly `fired_payload` (the polled one);
  otherwise only the score moves on, so a definition adopted between
  poll and commit is not written back.
- `replan_if_unchanged(name, expected, payload, fire_ts_ms)` re-plans an
  entry without score in one atomic step: it writes `payload` and the
  score only while the stored definition is still exactly `expected` and
  the entry has no score, and returns whether it wrote. A schedule
  disabled or removed in between stays as it is.
- `report_cron` sets `last_seen` and the payload of `name|fingerprint`
  atomically in backend time; `first_seen` only when the report is
  missing or its `last_seen` is older than `live_window_ms`.
  `cron_reports()` returns every report together with the backend time
  of the read (`CronReports.now_ms`); `prune_cron_reports` deletes
  reports whose `last_seen` is older than `max_age_ms` and returns their
  number.
- `report_worker_version` / `worker_versions` hold the swarmq version
  per worker for `ttl_ms`; `set_leader_version` writes the leader's
  version next to the leader lock with the lock's TTL; `leader_info()`
  returns the lock owner and that version (`LeaderInfo.version` is
  `None` for a leader before 0.10.0).

Contract changes in 0.11.0. A `ScheduleStore` of your own needs three
more methods for [cron pruning](../guides/scheduling-and-retries.md#removing-crons-no-app-reports-anymore);
a backend that offers the [execution guard](../explanation/delivery-semantics.md#execution-guard)
exposes an `ExecutionGuardStore` as `backend.execution_guard`:

```python
from swarmq.contracts import ExecutionGuardStore, GuardClaim, ScheduleStore

class ScheduleStore(Protocol):
    async def prune_cron(self, name: str, expected: bytes, *,
                         stale_after_ms: int | None,
                         remember_disabled: bool) -> bool: ...
    async def pruned_disabled(self, name: str) -> bool: ...
    async def forget_pruned_disabled(self, name: str) -> None: ...

class ExecutionGuardStore(Protocol):
    async def claim(self, identity: str, *, owner: str, retry_count: int,
                    ttl_ms: int, state_ttl_ms: int,
                    max_orphans: int) -> GuardClaim: ...
    async def renew(self, identity: str, *, owner: str, ttl_ms: int) -> bool: ...
    async def mark_done(self, identity: str, *, owner: str,
                        state_ttl_ms: int) -> None: ...
    async def mark_superseded(self, identity: str, *, owner: str,
                              retry_count: int, state_ttl_ms: int) -> None: ...
    async def release(self, identity: str, *, owner: str) -> None: ...
```

- `prune_cron` removes a static cron and its fire atomically while the
  stored definition is still exactly `expected` and, with
  `stale_after_ms`, every report of the name is older than that (backend
  time, at least one report); without it, while no report of the name
  exists. With `remember_disabled` the store remembers the name, so
  `pruned_disabled` returns `True` until `forget_pruned_disabled`.
  Returns whether it removed.
- `claim` returns a `GuardClaim(status, orphans)` with `status` one of
  `claimed`, `done`, `superseded`, `running`, `orphan_limit`. A running
  marker that expired without release counts as an orphan; at
  `max_orphans` the claim still belongs to the caller, so the following
  dead-lettering finishes it. `renew` is a compare-and-PEXPIRE on the
  caller's own marker; `mark_done`, `mark_superseded` (only rising
  counts) and `release` delete the running marker only while it belongs
  to `owner`.
- A backend without an `execution_guard` attribute runs without the
  guard.

Contract note for 0.12.3 (signatures unchanged). `ScheduleStore.add`
should write the definition and its fire-index entry in one atomic
step, as swarmq's Valkey store does (`sched_add.lua`), so that a
repeated write after a lost reply never leaves an entry without a fire
time. When `add` returns `False` for a one-shot of
`schedule(delay=/eta=)` (name `oneshot:<task_id>`), swarmq reads the
entry back with `get`; a byte-identical entry counts as its own insert,
and a missing fire time is added with `replan_if_unchanged`. Any other
existing entry is a conflict, and `swarmq schedule add` still reports
"already exists" for an existing name.

## Dependency injection (Dishka)

Optional; install with `pip install 'swarmq[dishka]'`.

```python
from swarmq import SwarmQProvider, TaskContext, setup_dishka

setup_dishka(container: dishka.AsyncContainer) -> None
```

- `setup_dishka(container)` registers your `AsyncContainer`. Call it
  at module level in the task module, so every spawned worker process
  builds its container on import. A second call replaces the first and
  logs `dishka_container_replaced`.
- Parameters of `run`, `pre_execution` and `post_execution` annotated
  with `FromDishka[...]` (from `dishka`) are injected from a REQUEST
  scope the worker opens per task. The same holds for
  `Annotated[X, FromComponent("c")]` (a named component) and for aliases
  such as `Depends = FromDishka`, with or without
  `from __future__ import annotations`.
- Every annotation of such a method must be resolvable when the task
  class is defined, including the parameters that are not injected:
  define or import the types before the class, not only under
  `TYPE_CHECKING` and not as a forward reference to a later class.
  Otherwise the method is not injected, and SwarmQ reports it:
    - With `from __future__ import annotations` (PEP 563) the worker
      start fails with `ConfigurationError` (exit 78). The message names
      the task, the injected parameter and, if one is still unresolvable,
      that parameter.
    - Without it (PEP 649, Python 3.14) the class definition raises
      `NameError` with a hint.
    - An annotation that names `FromDishka` in its text but does not
      resolve raises `NameError` with a hint at class definition in both
      modes.
    - An annotation that is unresolvable and shows no recognisable marker
      (for example a type imported only under `TYPE_CHECKING`, or under
      PEP 649 `Depends[Later] | None` with a later class) is not injected
      and not reported. The class definition of a task without DI never
      fails on an unresolvable annotation, and neither does building the
      app (`SwarmQ(...)`, also at `swarmq worker` start), which reads the
      annotations of `run()` tolerantly.
- `FromDishka[X] | None` and `Depends[X] | None` derive no dependency
  key; the worker start rejects them with `ConfigurationError`.
- The DI check runs at `swarmq worker` start, in an embedded worker
  (`SwarmQ.start_worker()`, before any connection) and in
  `swarmq worker --check`. `--check` imports `--module` for this (its
  output goes to stderr) and checks the tasks that the import
  registers.
- `TaskContext` is a frozen dataclass with `task_id`, `task_name`,
  `queue` and `retry_count` for the running task.
- `SwarmQProvider` is a Dishka provider that makes `TaskContext`
  available at REQUEST scope; add it to your container when a provider
  depends on `TaskContext`. Accessing it without `dishka` installed
  raises `ImportError`.

```python
from dishka import FromDishka, make_async_container

from swarmq import SwarmQProvider, Task, TaskContext, TaskInfo, setup_dishka

# AppProvider: your own dishka Provider that provides PaymentClient
container = make_async_container(SwarmQProvider(), AppProvider())
setup_dishka(container)


class Charge(Task):
    task_info = TaskInfo(name="charge", queues=["billing"])

    async def run(self, order_id: int, payments: FromDishka[PaymentClient],
                  ctx: FromDishka[TaskContext]) -> None:
        await payments.charge(order_id, idempotency_key=ctx.task_id)
```

## Health and metrics

`swarmq worker --health-port` / `--metrics-port` start these servers
for you. Use the classes directly only in an embedded worker or your
own process.

```python
from swarmq import (
    HealthChecker, HealthServer, serve_health,
    Metrics, MetricsServer, serve_metrics, install_signal_collectors,
)

HealthChecker(broker, backend, *, timeout_s: float = 2.0, checks=None)
HealthServer(checker, *, host="127.0.0.1", port=8080, liveness_started=None)
serve_health(checker, *, host="127.0.0.1", port=8080)       # async context manager

Metrics(registry=None, namespace="swarmq")
MetricsServer(metrics, host="127.0.0.1", port=9090)
serve_metrics(metrics, host="127.0.0.1", port=9090)         # async context manager
install_signal_collectors(dispatcher: SignalDispatcher, metrics: Metrics) -> None
```

- `HealthChecker.check_all()` pings broker and backend (each bounded
  by `timeout_s`) and returns `{"ready", "broker", "backend"}` plus one
  entry per named check. `checks` maps a name to an async callable
  returning `(ok, error)`; each runs bounded by `timeout_s`, and a
  failing one makes `ready` false. The worker passes `"consumers"`
  (every consumed queue has an active consumer, see
  [consumer timeout](../explanation/delivery-semantics.md#consumer-timeout))
  and, in CPU mode, `"compute"`. Until 0.10.x the only extra check was
  `compute_check=`; pass `checks={"compute": ...}` instead.
- `HealthServer` serves `/health` (always `200` with `uptime_s`) and
  `/ready` (`200` or `503` with the `check_all()` snapshot).
- `Metrics` holds the [metric catalog](configuration.md#metric-catalog)
  in its own `CollectorRegistry`; without `prometheus_client`
  (`pip install 'swarmq[metrics]'`) every metric is a no-op.
- `MetricsServer` serves `/metrics` from a `Metrics` instance.
- `install_signal_collectors()` registers the signal handlers that
  update the task counters, gauges and the duration histogram.

Both servers have no authentication and bind to `127.0.0.1` by
default.

## Exceptions

All library exceptions descend from `SwarmQError` and can be imported
from `swarmq` (`from swarmq import TaskError, WorkflowNotFound, ...`).
The two worker-internal classes `WorkflowStateConflict` and
`SettleFailedAfterPublish` are left out of the top-level package;
import them from `swarmq.exceptions`. `BrokerPublishBlocked` and
`PublishRejected` can be imported from `swarmq` since 0.12.3; code that
must also run on 0.12.0–0.12.2 imports them from `swarmq.exceptions`.
Detailed semantics and `When raised` / `Cause` / `Recovery` for each
class live in the [error catalog](errors.md). `ForkSourceRejected`,
raised by an embedded CPU worker, is a `RuntimeError` in
`swarmq.supervision`.

`Retry(delay=...)` clamps a negative delay to `0` (retry immediately,
counted against `max_retries`) and raises `ValueError` for `inf` and
`nan` when it is created (since 0.12.3).

## See also

- [Configuration](configuration.md) — env vars and `TaskInfo` fields
- [CLI reference](cli.md) — the `swarmq` binary
- [Getting Started](../getting-started.md) — the round-trip this
  API supports
