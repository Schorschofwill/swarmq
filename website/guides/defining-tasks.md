# Defining tasks

You want to define a task that runs on a worker. A task in SwarmQ is a
Python class that subclasses `Task` and declares its identity with a
`TaskInfo` field.

This guide covers the shape of a task definition, where the pieces
live, and the anti-patterns that bite in production.

## Prerequisites

- A Python package with at least one module (the one you will pass
  to `--module`).
- `pip install swarmq`.

## The minimum

```python title="myapp/tasks.py"
from swarmq import Task, TaskInfo


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["default"],
    )

    async def run(self, to: str, subject: str) -> str:
        # Your email logic here
        return f"sent to {to}"
```

Three rules are enforced the moment the module is imported:

1. **Subclass `Task`.** This registers the class with the global
   registry. No decorator, no manual `register()` call.
2. **Declare `task_info`.** Without it, `Task.__init_subclass__`
   skips registration — your class becomes invisible to workers.
3. **Define `async def run(self, ...)`.** The signature names the
   keyword arguments your clients will pass to `schedule()`.

## Name vs class name

`TaskInfo.name` is the over-the-wire identifier. It is what clients
pass to `schedule("name", ...)` and what appears in broker queues,
worker logs, and `swarmq inspect` output. The Python class name is
irrelevant to the system — you can rename `SendEmail` to
`EmailSender` without breaking anything.

**Renaming the `name` is a breaking change** for any queued
messages. A worker running new code will not find a task named
`send_email_v1` when the clients are still sending `send_email`.
Use [deprecation via a second class](#deprecating-a-task) instead.

## Queue routing

`queues` is a non-empty list. The first element is the default
target for `schedule("name")`; others are what the task *can* be
routed to when the client overrides with `queue=`.

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports", "default"],
)
```

```python
# Publishes to "reports" — the first element.
await app.schedule("render_report", report_id=42)

# Publishes to "default" — explicit override.
await app.schedule("render_report", report_id=42, queue="default")
```

Worker-side, `swarmq worker --queues reports,default` consumes from
both. See [Scheduling and retries](scheduling-and-retries.md) for
how `delay=` / `eta=` interact with routing.

### Moving a task to another queue

When a task moves to another queue, or a worker's `--queues` changes,
messages already in the old queue (and retry copies still waiting in
its delay queues) stay there: no worker consumes them any more. Since
0.12 every worker reports the queues it consumes to Valkey
(`[<namespace>:]sq:{sched}:report:queues`, timestamps from Valkey's clock). The
scheduler leader checks the reported queues on every adoption tick
(`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`, default 60 s; not on the tick right
after a takeover) with a passive
declare, without the management API. A queue counts as orphaned when it
holds messages, has no consumer, and no live worker has reported it for
three ticks. The leader then logs `scheduler_queue_orphaned` (WARNING,
once per transition), emits `SCHEDULER_QUEUE_ORPHANED` and sets the gauge
`swarmq_scheduler_queues_orphaned`; alert on it. `swarmq check` with
`SWARMQ_BROKER_URL` lists orphaned queues and reported queues without
their `.XQ` ([CLI](../reference/cli.md#swarmq-check)).

To clear it, run a worker for the old queue until it is drained, move
the messages, or delete the queue. Limits: only queues a worker of 0.12
or later has reported are checked; a queue with a consumer (for example
a worker before 0.12) never counts as orphaned. Entries of queues that
are gone or empty and have not been reported for 7 days are removed;
the whole key expires 7 days after the last report of any worker.

## Arguments and return values

### What clients can pass

Arguments to `run()` travel as **JSON**, encoded with `msgspec`. Plain
`str`, `int`, `float`, `bool`, `None`, `list` and `dict` arrive
unchanged. Other types are converted on the way, and the task receives
the converted value, not the original type:

| Passed | Arrives in `run()` as |
|---|---|
| `datetime`, `date`, `time` | ISO 8601 `str` (`"2026-01-02T03:04:05"`) |
| `Decimal`, `UUID` | `str` |
| `bytes` | base64 `str` |
| `tuple`, `set`, `frozenset` | `list` |
| dataclass, `msgspec.Struct` | `dict` |
| `Enum` | its value |
| `dict` with `int` keys | `dict` with `str` keys (`{1: "a"}` → `{"1": "a"}`) |
| `float("nan")` | `None` |

Convert such values back in `run()` yourself (for example
`datetime.fromisoformat(...)`), or pass them as `str` in the first
place.

An object `msgspec` cannot encode (an ORM row, an arbitrary class) is
rejected at schedule time, before anything is published:

- A direct `schedule()` raises `ResultSerializationError` (the name is
  misleading; it is the class the JSON encoder raises).
- With `delay=` or `eta=`, the arguments go into the scheduler store
  first; there the same object raises a plain `TypeError`.
- With `unique_for=`, the dedup hash additionally rejects `bytes` and
  objects of your own classes with `TypeError`.

### What `run()` returns

The return value goes through **msgpack** and is stored at
`sq:result:{task_id}` in Valkey. msgpack is more permissive than
JSON: `datetime`, `date`, `time`, `Decimal`, `UUID`, `set` and
`frozenset` come back with the same type; a `tuple` comes back as a
`list`. Arbitrary Python objects are not supported. If your return
value is not serialisable, the worker raises
`ResultSerializationError` and the task is treated as failed.

When in doubt, return a `dict` of primitives:

```python
return {"status": "sent", "message_id": message_id, "duration_ms": elapsed}
```

## Reserved parameter names

Four parameter names on `run()` are reserved by `app.schedule()`:
`delay`, `eta`, `unique_for`, `unique_until`. A task that declares
one of them raises `ValueError` at class registration, with a
migration hint:

```python
# ❌ This raises at import time:
class Scheduled(Task):
    task_info = TaskInfo(name="scheduled", queues=["default"])
    async def run(self, delay: float) -> None:  # "delay" reserved
        ...

# ✅ Rename:
async def run(self, retry_delay: float) -> None:
    ...
```

A few more keyword names belong to the calls that start a task, but
swarmq does not reject them on `run()`:

- `queue` — `app.schedule()` and `app.run()` read it as the target
  queue and do not pass it on. `schedule("export", queue="eu")` routes
  the task to the queue `eu`: the publish fails with
  `UnroutableMessage` if no such queue exists, or the task runs
  without the argument (`TypeError` for a required parameter).
- `timeout`, `polling` — `app.run()` reads them to wait for the result
  and does not pass them on.

Rename such a parameter, or pass the value positionally
(`schedule("export", "eu")`).

## Lifecycle hooks

Override `pre_execution` and `post_execution` to run code on the
worker around the task body. `post_execution` runs only after
`pre_execution` and `run()` succeeded. It does not run when either
raises, on a soft or hard timeout, or on a cancel; an exception in it
turns the attempt into a failure. Use it for follow-up work on
success. Cleanup that must run on every outcome belongs in
`try/finally` inside `run()`:

```python
class WithSession(Task):
    task_info = TaskInfo(name="with_session", queues=["default"])

    async def run(self, user_id: int) -> dict:
        session = await open_db_session()
        try:
            return await session.fetch(user_id)
        finally:
            await session.close()
```

With [dependency injection](../reference/api.md#dependency-injection-dishka),
put it in the provider's `yield` finalizer instead; it also receives
the exception, for commit or rollback. For cross-cutting concerns that
should apply to **every** task, use [middleware](middleware.md)
instead; its `on_error` runs on every failure.

## Progress reporting

Call `self.update_progress(current, total, message="")` from inside
`run()` to stream progress to clients:

```python
class RenderReport(Task):
    task_info = TaskInfo(
        name="render_report", queues=["reports"],
        hard_timeout=300, soft_timeout=240,
    )

    async def run(self, report_id: int) -> str:
        items = await fetch_items(report_id)
        total = len(items)
        for i, item in enumerate(items):
            await render_item(item)
            await self.update_progress(i + 1, total, f"item {item.id}")
        return f"reports/{report_id}.pdf"
```

Progress writes are throttled to one per 500 ms per task instance;
the final update (`current == total`) always flushes. See
[`app.watch_progress()`](../reference/api.md#get_progress-watch_progress)
for the consumer side.

## Deprecating a task

To rename or retire a task, keep the old task registered as a thin
redirect for at least one release cycle:

```python
class SendEmail(Task):
    task_info = TaskInfo(name="send_email_v2", queues=["default"])

    async def run(self, to: str, subject: str) -> str:
        ...


class SendEmailLegacy(Task):
    """Deprecated — use send_email_v2. Kept alive for in-flight messages."""

    task_info = TaskInfo(name="send_email", queues=["default"])

    async def run(self, *args, **kwargs) -> str:
        # Delegate to the new implementation.
        return await SendEmail().run(*args, **kwargs)
```

Once the main queue is drained and the DLQ has no `send_email`
entries (`swarmq dlq list | grep send_email` is empty), delete the
legacy class.

## Anti-patterns

### Mutable class defaults

```python
class Bad(Task):
    task_info = TaskInfo(name="bad", queues=["default"])
    cache: dict = {}  # ❌ shared across every task run

    async def run(self, key: str) -> str:
        self.cache[key] = "value"  # leaks forever
        return self.cache[key]
```

Use module-level caches with explicit TTLs instead, or store state
in Valkey via your own connection.

### Threading or multiprocessing inside `run()`

Do not spawn threads or processes from a task. SwarmQ already runs N
tasks concurrently (`--concurrency`), and launches N worker
processes (`--processes`). Spawning from inside multiplies that
factor and exhausts connection pools.

If a task is CPU-bound, run the CPU work in a
`concurrent.futures.ProcessPoolExecutor` created at module load and
reused across runs — or promote the task to its own queue with
lower `--concurrency`.

### Leaking `asyncio.Task` handles

```python
# ❌ fire-and-forget tasks leak
asyncio.create_task(some_coro())
```

The parent `run()` returns before the child completes; the child
may be cancelled when the worker shuts down, or it may raise
unhandled and crash the worker's event loop. `await` the coroutine
directly, or `schedule()` it as a proper SwarmQ task.

### Globals that depend on process identity

```python
# ❌ `process_id` is captured at module import time
process_id = f"worker-{os.getpid()}"
```

With `--processes > 1` and `--reload`, worker PIDs change on every
re-exec. Capture the value lazily inside `run()` if you need it.

## Verify

```bash
swarmq inspect --module myapp.tasks
```

Every task you defined should appear in the table with its `queues`,
`delivery`, `rate_limit`, `lock`, `unique_for` / `unique_until`, and
`cron` columns populated as declared. A missing task means its
module was not imported — fix the `--module` path.

## See also

- [`TaskInfo` fields](../reference/configuration.md#taskinfo-fields)
  — every option with type and default
- [Scheduling and retries](scheduling-and-retries.md) — how retry
  policy is configured
- [Testing tasks](testing-tasks.md) — unit-test a `Task` without a
  broker
