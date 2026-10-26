# Defining tasks

You want to define a task that runs on a worker. A SwarmQ task is a
Python class that subclasses `Task` and declares its identity in a
`TaskInfo`.

This guide covers the shape of a task definition, routing, arguments
and results, and the anti-patterns that bite in production.

## Prerequisites

- A Python package with at least one module (the one you pass to
  `--module`).
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

Three rules apply when the module is imported:

1. **Subclass `Task`.** That registers the class; there is no
   decorator and no `register()` call.
2. **Declare `task_info`.** A class without it is not registered and
   stays invisible to workers. Use such a class as a shared base for
   helpers.
3. **Define `async def run(self, ...)`.** Its parameters are the
   arguments clients pass to `schedule()`, positionally or by keyword.

## Validation at import

`TaskInfo` checks every field when the class is defined. A bad value
raises `ValueError` on import of the task module, not at the first
`schedule()`: an empty `queues` list, a `lock` without `hard_timeout`,
`soft_timeout` or `hard_timeout` not greater than 0, a cron outside the
crontab(5) syntax or without a next fire time, a deadline above ten
years (`315360000` s) and more. `swarmq worker` then ends with exit `1`
and is restarted until the restart limit. Catch it before a deploy;
`--check` imports the module and reports the error as `"ok": false`:

```bash
swarmq worker --module myapp.tasks --check
```

Every field with its type and default:
[`TaskInfo` fields](../reference/configuration.md#taskinfo-fields);
the full list of import errors:
[validation behaviour](../reference/configuration.md#validation-behaviour).

## Name vs class name

`TaskInfo.name` (1 to 255 characters) is the identifier on the wire.
Clients pass it to `schedule("name", ...)`, and it appears in
messages, worker logs and `swarmq inspect`. The Python class name does
not matter: renaming `SendEmail` to `EmailSender` breaks nothing.

**Renaming `name` is a breaking change** for messages already queued:
after `send_email` becomes `send_email_v2`, no worker registers
`send_email` for the messages clients still send. Keep the old name
alive as described in [deprecating a task](#deprecating-a-task).

## Queue routing

`queues` is a non-empty list. Its first element is where
`schedule("name")` publishes; a client can send a single call
elsewhere with `queue=`:

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports", "default"],
)
```

```python
# Publishes to "reports", the first element.
await app.schedule("render_report", report_id=42)

# Publishes to "default", an explicit override.
await app.schedule("render_report", report_id=42, queue="default")
```

`queue=` is not checked against the list: any queue a worker has
declared works, a queue that does not exist raises
`UnroutableMessage`. List every queue the task may run on anyway;
`swarmq inspect` shows them. A task with `priority="high"` or `"low"`
publishes to `{queue}.high` or `{queue}.low`, also after an override.

On the worker side, `swarmq worker --queues reports,default` consumes
both (default: `default`). How `delay=` and `eta=` route is covered in
[scheduling and retries](scheduling-and-retries.md).

### Moving a task to another queue

When a task moves to another queue, or a worker's `--queues` changes,
the messages in the old queue (and retry copies still waiting in its
delay queues) stay there with no consumer. SwarmQ reports this:

- Every worker reports the queues it consumes to Valkey
  (`[<namespace>:]sq:{sched}:report:queues`, with Valkey's clock).
- The scheduler leader checks, on every adoption tick
  (`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`, default 60 s; not on the tick
  right after a takeover), the queues no live worker has reported for
  three ticks, with a passive declare (no management API). A queue
  with messages and no consumer counts as orphaned.
- The leader then logs `scheduler_queue_orphaned` (WARNING, once per
  transition), emits `SCHEDULER_QUEUE_ORPHANED` and sets the gauge
  `swarmq_scheduler_queues_orphaned`; alert on it.
  `scheduler_queue_orphan_resolved` (INFO) follows when the queue is
  consumed or empty again.
- `swarmq check` with `SWARMQ_BROKER_URL` lists orphaned queues and
  reported queues without their `.XQ`
  ([CLI](../reference/cli.md#swarmq-check)).

To clear it, run a worker for the old queue until it is drained, move
the messages, or delete the queue. Limits: only queues a worker from
0.12 on has reported are checked, and a queue with any consumer never
counts as orphaned. Entries of queues that are gone or empty and have
not been reported for 7 days are removed; the whole key expires 7 days
after the last report of any worker.

## Arguments and return values

### What clients can pass

Arguments of `run()` travel as **JSON**, encoded with `msgspec`.
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
- With `unique_for`, the dedup hash also rejects `bytes`, objects of
  your own classes (dataclass, `Struct`, `Enum`), `dict` keys that are
  not `str` and `nan`/`inf` with `TypeError`.

### What `run()` returns

The return value is encoded with **msgpack** and stored under
`[<namespace>:]sq:result:{task_id}` in Valkey. msgpack is more
permissive than JSON: `datetime`, `date`, `time`, `Decimal`, `UUID`,
`set` and `frozenset` come back with the same type; a `tuple` comes
back as a `list`. Arbitrary Python objects are not supported: a return
value that cannot be encoded raises `ResultSerializationError`, and the
attempt counts as failed.

When in doubt, return a `dict` of primitives:

```python
return {"status": "sent", "message_id": message_id, "duration_ms": elapsed}
```

## Reserved parameter names

`delay`, `eta`, `unique_for` and `unique_until` are keyword arguments
of `app.schedule()`. A task whose `run()` declares one of them raises
`ValueError` with a rename hint when the class is defined:

```python
# ❌ Raises at import time:
class Scheduled(Task):
    task_info = TaskInfo(name="scheduled", queues=["default"])

    async def run(self, delay: float) -> None:  # "delay" is reserved
        ...

# ✅ Rename:
class Scheduled(Task):
    task_info = TaskInfo(name="scheduled", queues=["default"])

    async def run(self, retry_delay: float) -> None:
        ...
```

Three more keyword names belong to the calls that start a task, but
swarmq does not reject them on `run()`:

- `queue`: `app.schedule()` and `app.run()` read it as the target
  queue and do not pass it on. `schedule("export", queue="eu")` routes
  the task to the queue `eu`: the publish fails with
  `UnroutableMessage` if no such queue exists, or the task runs
  without the argument (`TypeError` for a required parameter).
- `timeout`, `polling`: `app.run()` reads them to wait for the result
  and does not pass them on.

Rename such a parameter, or pass the value positionally
(`schedule("export", "eu")`).

## Lifecycle hooks

Override `pre_execution` and `post_execution` to run code on the
worker around `run()`. `post_execution` runs only after
`pre_execution` and `run()` succeeded. It does not run when either
raises, on a soft or hard timeout, or on a cancel; an exception in it
turns the attempt into a failure. Use it for follow-up work on
success.

Cleanup that must run on every outcome belongs in `try/finally` inside
`run()`:

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
put it into the provider's `yield` finalizer instead; it also receives
the exception, for commit or rollback. For concerns that apply to
**every** task, use [middleware](middleware.md); its `on_error` runs on
every failure, but not on a cancel (handle the `TASK_CANCELLED` signal
for that).

## Progress reporting

Call `self.update_progress(current, total, message="")` inside `run()`
to stream progress to clients:

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

Progress is best effort: writes are throttled to one per 500 ms per
task instance, the final update (`current == total`) always goes
through, and a Valkey error is logged (`update_progress_hset_failed`)
instead of failing the task. The entry lives as long as the result
(`result_ttl`). See
[`app.watch_progress()`](../reference/api.md#get_progress-watch_progress)
for the client side.

## Deprecating a task

To rename or retire a task, keep the old name registered as a thin
redirect for at least one release cycle:

```python
class SendEmail(Task):
    task_info = TaskInfo(name="send_email_v2", queues=["default"])

    async def run(self, to: str, subject: str) -> str:
        ...


class SendEmailLegacy(Task):
    """Deprecated, use send_email_v2. Kept for in-flight messages."""

    task_info = TaskInfo(name="send_email", queues=["default"])

    async def run(self, *args, **kwargs) -> str:
        # Delegate to the new implementation.
        return await SendEmail().run(*args, **kwargs)
```

Deploy the workers with the new name before the producers that send
it. Delete the legacy class once the queue is drained and the DLQ holds
no `send_email` entries (`swarmq dlq list --queue default | grep
send_email` is empty; raise `--limit` for a large DLQ). A message that
arrives later for a name no worker registers is deferred for
`SWARMQ_UNKNOWN_TASK_GRACE_S` (default 15 minutes) and then
dead-lettered
([unregistered tasks](dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)).

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

The worker creates a new instance per run, but class attributes are
shared. Use a module-level cache with an explicit size or TTL, or keep
state in Valkey through your own connection.

### Threads or processes inside `run()`

Do not start threads or processes from a task. SwarmQ already runs
`--concurrency` tasks per worker process and can run several worker
processes (`--processes`); spawning from inside multiplies that and
exhausts connection pools.

For CPU-bound work, run the task on a worker with
[`--pool cpu`](cpu-mode.md), which runs task bodies in compute
children outside the event loop, or give the task its own queue with a lower `--concurrency`.

### Leaking `asyncio.Task` handles

```python
# ❌ fire-and-forget tasks leak
asyncio.create_task(some_coro())
```

`run()` returns before the child completes; the worker does not wait
for it, a shutdown can cut it off, and its exception is lost. `await`
the coroutine directly, or `schedule()` it as a SwarmQ task of its own.

### Globals that depend on process identity

```python
# ❌ `process_id` is captured at module import time
process_id = f"worker-{os.getpid()}"
```

With `--processes > 1` and `--reload`, worker PIDs change on every
restart. Read the value inside `run()` if you need it.

## Verify

```bash
swarmq inspect --module myapp.tasks
```

Every task should appear with its `QUEUES`, `DELIVERY`, `RETRIES`,
`RATE_LIMIT`, `LOCK`, `UNIQUE_FOR`/`UNIQUE_UNTIL` and `CRON` as
declared (`--format json` shows every field). A missing task means its
module was not imported; fix the `--module` path.

## See also

- [`TaskInfo` fields](../reference/configuration.md#taskinfo-fields):
  every option with type and default
- [Scheduling and retries](scheduling-and-retries.md): retry policy,
  delays and crons
- [Testing tasks](testing-tasks.md): unit-test a `Task` without a
  broker
