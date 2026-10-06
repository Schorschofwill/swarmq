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
count as dropped.

### `schedule()`

```python
async def schedule(
    task_name: str,
    *args: Any,
    queue: str | None = None,
    delay: float | None = None,
    eta: datetime | None = None,
    unique_for: int | None = _MISSING,
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
| `unique_for` | `int \| None` | Override `TaskInfo.unique_for`. Pass `None` to disable dedup for this call. |
| `unique_until` | `"start" \| "completion"` | Override `TaskInfo.unique_until`. |

**Returns.** `str` — the `task_id` (UUIDv7). On a dedup hit, the
*existing* owner's `task_id` is returned silently; a
`TASK_DUPLICATE_DETECTED` signal fires for observability.

**Raises.**

- `TaskNotFoundError` — task_name not in the registry (registry mode).
- `ValueError` — `delay < 0`, both `delay` and `eta` set, `eta` in the past.
- `BrokerConnectionError` — broker unreachable at publish.
- Propagated from `pre_enqueue` middleware hooks.

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
    unique_for: int | None = _MISSING,
    unique_until: Literal["start", "completion"] = _MISSING,
) -> list[str]
```

Enqueue many tasks in one batch. Each element is a `(task_name, args,
kwargs)` tuple. Published concurrently via `asyncio.gather`.

**Returns.** `list[str]` — one `task_id` per input, in input order.
On dedup hits, the existing owner's task_id is placed at that index
(list length always equals input length).

**Raises.** Same shape as `schedule()`. On partial publish failure,
successfully published task_ids are still written to the result
list, the rest are rolled back, and the first exception is
re-raised.

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

Internally subscribes to `sq:notify:{task_id}` on Valkey for a
push-based wait, with 5ms-interval polling as a fallback. When
`timeout=0`, performs a single `GET` without Pub/Sub.

**Raises.**

- `ResultNotReadyError` — no result within `timeout` seconds.
- `TaskError` — task failed; `str(exc)` carries the worker's error message.
- `TaskCancelledError` — cancel flag tripped during execution.

### `get_results()`

```python
async def get_results(
    task_ids: list[str],
    timeout: float = 10,
    poll_interval: float = 0.005,
) -> list[Any]
```

Batch-poll version of `get_result`. Uses `MGET` to fetch many at
once. Raises `ResultNotReadyError` with the missing task_ids when
any timeout.

### `cancel()`

```python
async def cancel(task_id: str) -> bool
```

Request cancellation of a task. Fire-and-forget: sets a cancel flag
with TTL `result_ttl × 1000` ms and returns immediately.

**Returns.** `True` when the flag was newly set, `False` when a
prior `cancel()` call is still in flight.

**Raises.** `ValueError` on empty `task_id`.

!!! warning "Stale cancel flag"
    The flag TTL is 24 h by default. If the same `task_id` is reused
    within that window (deterministic IDs, redelivery after crash
    before the worker cleared the flag), the stale flag cancels the
    next delivery too. Mitigations: let SwarmQ generate UUIDv7 task
    IDs (the default), or pass a shorter `ttl_ms` directly through
    the underlying client: `await app.client.cancel(task_id, ttl_ms=...)`.

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
(never started or expired).

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
application code.

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
    result_ttl: int | None = None,
    max_retries: int = 0,
    retry_backoff: str = "exponential",
    retry_exceptions: list[type[BaseException]] | None = None,
    retry_base_delay: float = 1.0,
    rate_limit: str | None = None,
    lock: str | None = None,
    max_concurrency: int = 1,
    cron: str | None = None,
    unique_for: int | None = None,
    unique_until: Literal["start", "completion"] = "completion",
    max_age: int | None = None,
    priority: Literal["high", "normal", "low"] = "normal",
)
```

Field semantics and validation rules live in
[Configuration reference — TaskInfo fields](configuration.md#taskinfo-fields).

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
- `post_execution()` — called after `run()` (success or failure).
  Default: no-op.

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
  aborts the enqueue.
- All other hooks **swallow** exceptions: each error is logged as
  `middleware_hook_failed` (ERROR, logger `swarmq.middleware`, fields
  `hook`, `middleware`, `error_type`, `error` and the traceback) and
  the chain continues.

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
| `TASK_ENQUEUED` | `task_id`, `task_name`, `queue` | After a publish (`schedule`, `schedule_many`, each workflow step). |
| `TASK_STARTED` | `task_id`, `task_name`, `queue` | Before `run()`. |
| `TASK_COMPLETED` | `task_id`, `task_name`, `queue`, `duration_ms` | After a successful `run()`. |
| `TASK_FAILED` | `task_id`, `task_name`, `queue`, `exception` | After **every** failed attempt, also when a retry follows. `exception` is `str(exc)`. |
| `TASK_RETRYING` | `task_id`, `task_name`, `queue`, `retry_count`, `delay` | A retry was scheduled; `retry_count` is the new count, `delay` in seconds. |
| `TASK_CANCELLED` | `task_id`, `task_name`, `queue` | Cancelled before or during execution. |
| `TASK_EXPIRED` | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` | Picked up older than `max_age`; routed to the DLQ. |
| `TASK_LOCKED` | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` | The task's lock was held by another execution (lock conflict); the message is requeued, or dead-lettered at the conflict cap. |
| `TASK_RATE_LIMITED` | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` | Rate limit hit; message delayed. |
| `TASK_DUPLICATE_DETECTED` | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] | `unique_for` dedup hit. |
| `TASK_DLQ` | `task_id`, `task_name`, `queue`, `error` | Terminal: the task was dead-lettered (retries exhausted or not retryable). Fires once. |
| `TASK_REJECTED` | `queue`, `reason`, `task_id`, `task_name`, `error` — `task_id`/`task_name` may be `None` | A message went to the broker DLX without diagnostic headers (`body_too_large`, `undecodable`, `unregistered_task`, `middleware_chain_factory`, `dlq_publish_failed`). |
| `WORKFLOW_STEP_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` | After each workflow step publish, in addition to `TASK_ENQUEUED`. |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | `worker_id` | Worker start / graceful shutdown. |
| `WORKER_HEARTBEAT` | `worker_id`, `uptime_seconds` | Every `SWARMQ_METRICS_SAMPLE_INTERVAL` seconds; only while metrics are enabled. |
| `WORKER_RECYCLING` | `worker_id`, `kind`, `slot`, `reason`, `value` | A worker (`kind="worker"`) or compute child (`kind="compute_child"`) is recycled; `reason` is `rss`, `tasks` or `forced`. |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | `worker_id` | Leader election and renewal. |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | `worker_id` | Leader hands over on shutdown / takes over after an expired lock. |
| `SCHEDULER_FIRE_SKIPPED` | `worker_id`, `schedule_name`, `skipped_count` | Catch-up after downtime skipped cron occurrences. |

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
  when the flag was newly set.
- `get(timeout=30.0)` — waits for a terminal status and returns the
  final result.

**Raises.** `WorkflowNotFound` when the state is gone (`status()`,
`children()`, `get()`),
`ResultNotReadyError` when `get()` times out, `TaskError` for a failed
workflow, `TaskCancelledError` for a cancelled one,
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
| `updated_at` | `datetime` | UTC timestamp of the update. |
| `started_at` | `datetime \| None` | UTC start time, if recorded. |
| `eta_seconds` | `int \| None` | Linear extrapolation; `None` until the first non-zero progress. |

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
`retry_later`/`dead_letter`, queue depths and DLQ operations). All
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
)
```

Pass exactly one of `url`, `client` or `pool`; none or more than one
raises `ConfigurationError`. A borrowed `client` is never closed by
SwarmQ, a `pool` keeps its own lifecycle; both are for the producer
path only (`start_worker()` rejects them).

Async Valkey client backing results, locks, rate limits, cancel
flags, scheduler state, and leader election. Implements the
`ResultBackend` protocol from `swarmq.contracts` and exposes the
coordination stores as `backend.locks`, `.rate_limit`, `.chords`, and
`.schedules` (available after `connect()`). Wire through `SwarmQ`
as the second positional argument. Since 0.2.0 the constructor
signatures of `Worker` and `Client` name these contracts instead of
the concrete classes — existing code keeps working unchanged.

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
  scope the worker opens per task.
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

HealthChecker(broker, backend, *, timeout_s: float = 2.0, compute_check=None)
HealthServer(checker, *, host="127.0.0.1", port=8080, liveness_started=None)
serve_health(checker, *, host="127.0.0.1", port=8080)       # async context manager

Metrics(registry=None, namespace="swarmq")
MetricsServer(metrics, host="127.0.0.1", port=9090)
serve_metrics(metrics, host="127.0.0.1", port=9090)         # async context manager
install_signal_collectors(dispatcher: SignalDispatcher, metrics: Metrics) -> None
```

- `HealthChecker.check_all()` pings broker and backend (each bounded
  by `timeout_s`) and returns `{"ready", "broker", "backend"}` (plus
  `"compute"` in CPU mode).
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
`WorkflowStateConflict` is the one exception left out of the
top-level package; import it from `swarmq.exceptions`. Detailed
semantics and `When raised` / `Cause` / `Recovery` for each class
live in the [error catalog](errors.md).

## See also

- [Configuration](configuration.md) — env vars and `TaskInfo` fields
- [CLI reference](cli.md) — the `swarmq` binary
- [Getting Started](../getting-started.md) — the round-trip this
  API supports
