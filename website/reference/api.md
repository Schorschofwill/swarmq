# API reference

The public API is what `from swarmq import ...` gives you. This page
documents every public class and the methods application code is
expected to call. The library is written in async; every method that
does I/O is a coroutine.

## Module layout

```python
from swarmq import (
    SwarmQ,              # Application entry point
    Task, TaskInfo,      # Task definition
    DeliveryMode,        # at_least_once | fire_and_forget
    Middleware,          # Middleware base class
    MessageContext,      # TypedDict passed to middleware hooks
    Signal,              # Lifecycle event enum
    SignalDispatcher,    # Signal registry
    ProgressInfo,        # Task progress snapshot
    Retry, NoRetry,      # Task-side control flow
    SwarmQError,         # Base exception
    # ...and the rest of the exception hierarchy
)

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
```

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
    `Client.cancel()`.

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
@app.on_signal(Signal.TASK_FAILED)
async def on_failure(task_id, task_name, exception):
    await sentry.capture(exception, tags={"task": task_name})
```

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
- All other hooks **swallow** exceptions: errors are logged at
  ERROR level via structlog and the chain continues.

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

## `enum Signal`

Lifecycle events emitted by SwarmQ. Register handlers with
`@app.on_signal(Signal.FOO)`.

| Signal | Payload (kwargs) |
|---|---|
| `TASK_ENQUEUED` | `task_id`, `task_name`, `queue` |
| `TASK_STARTED` | `task_id`, `task_name` |
| `TASK_COMPLETED` | `task_id`, `task_name`, `duration_ms` |
| `TASK_FAILED` | `task_id`, `task_name`, `exception` |
| `TASK_RETRYING` | `task_id`, `retry_count`, `delay` |
| `TASK_CANCELLED` | `task_id`, `task_name` |
| `TASK_EXPIRED` | `task_id`, `task_name`, `age_seconds` |
| `TASK_LOCKED` | `task_id`, `task_name`, `lock_name` |
| `TASK_RATE_LIMITED` | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` |
| `TASK_DUPLICATE_DETECTED` | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` / `WORKER_HEARTBEAT` | `worker_id` |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | `worker_id` |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | `worker_id` |
| `SCHEDULER_FIRE_SKIPPED` | `worker_id`, `schedule_name`, `skipped_count` |

Handlers must accept `**kwargs` (or name every field explicitly).
Use `kwargs.get("batch_index")` for the
`TASK_DUPLICATE_DETECTED`-only field, since non-batch sources do
not include it.

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
from swarmq.broker.rabbitmq import RabbitMQBroker

broker = RabbitMQBroker(url: str)
```

Async AMQP broker wrapping aio-pika. All configuration (queue type,
prefetch, reconnect policy) flows through `SwarmQConfig` and
`TaskInfo` — you do not call methods on this class directly from
application code.

## `class ValkeyBackend`

```python
from swarmq.backend.valkey import ValkeyBackend

backend = ValkeyBackend(url: str)
```

Async Valkey client backing results, locks, rate limits, cancel
flags, scheduler state, and leader election. Wire through `SwarmQ`
as the second positional argument.

## Exceptions

All library exceptions descend from `SwarmQError`. Detailed
semantics and `When raised` / `Cause` / `Recovery` for each class
live in the [error catalog](errors.md).

## See also

- [Configuration](configuration.md) — env vars and `TaskInfo` fields
- [CLI reference](cli.md) — the `swarmq` binary
- [Getting Started](../getting-started.md) — the round-trip this
  API supports
