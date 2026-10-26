# Testing tasks

You want to test a task without RabbitMQ or Valkey on the host. A
SwarmQ task is a plain Python class: unit tests instantiate it and
call `run()` directly. Integration tests then cover the parts that
need the infrastructure.

## Prerequisites

- A task defined in your project (`myapp/tasks.py`).
- `pytest` and `pytest-asyncio` (`pip install pytest pytest-asyncio`).

## Unit-testing `run()` directly

Call `run()` as a coroutine:

```python title="tests/test_tasks.py"
import pytest

from myapp.tasks import SendEmail


@pytest.mark.asyncio
async def test_send_email_returns_message_id():
    task = SendEmail()

    result = await task.run(to="a@example.com", subject="Hi")

    assert result == "sent to a@example.com"
```

No broker, no backend, no `SwarmQ` application: the framework only
comes in when a worker consumes the task's messages.

Two things a direct call does not do:

- **Serialization.** The worker hands `run()` arguments decoded from
  JSON, so a `datetime` arrives as `str` and a `tuple` as `list`
  ([arguments and return values](defining-tasks.md#arguments-and-return-values)).
  Pass the values in the shape the worker delivers them.
- **Lifecycle.** `pre_execution`, `post_execution`, timeouts, retries
  and middleware run only in the worker.

Define task classes in modules, not inside test functions. Every
subclass with a `task_info` lands in a process-wide list, and
`SwarmQ(...)` raises `ConfigurationError` (`Task '…' is already
registered`) when two classes share a name.

## Testing with dependencies

If your task uses shared dependencies (SMTP client, HTTP session, DB
pool), take them as constructor arguments and pass test doubles. The
worker builds the task with `SendEmail()`, so every constructor
parameter needs a default:

```python title="myapp/tasks.py"
class SendEmail(Task):
    task_info = TaskInfo(name="send_email", queues=["default"])

    def __init__(self, smtp=None) -> None:
        self._smtp = smtp or default_smtp

    async def run(self, to: str, subject: str) -> str:
        return await self._smtp.send(to, subject)
```

```python title="tests/test_tasks.py"
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_send_email_calls_smtp():
    smtp = AsyncMock()
    smtp.send.return_value = "ok"

    task = SendEmail(smtp=smtp)
    result = await task.run(to="a@example.com", subject="Hi")

    assert result == "ok"
    smtp.send.assert_awaited_once_with("a@example.com", "Hi")
```

!!! note "Tasks with `FromDishka` parameters"
    A task whose `run()` takes parameters annotated with
    [`FromDishka[...]`](../reference/api.md#dependency-injection-dishka)
    cannot be called directly: `run()` raises `RuntimeError` ("No
    active SwarmQ request scope"), even if you pass the parameter
    yourself. Keep the logic in a plain class or function that you test
    directly, and cover the injected task with an integration test.

## Testing progress updates

`update_progress()` does nothing when no worker has wired the task
(`_backend` and `_task_id` are `None`), so unit tests can call it
without Valkey.

To assert that progress was written, set a mock backend:

```python
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_report_emits_progress():
    task = RenderReport()
    task._task_id = "test-1"
    task._backend = AsyncMock()
    task._result_ttl_ms = 60_000

    await task.run(report_id=42)

    assert task._backend.set_progress.await_count >= 1
```

The attributes `_backend`, `_task_id` and `_result_ttl_ms` are
assignable on purpose, so tests need no monkeypatching. Writes are
throttled to one per 500 ms, the final update (`current == total`)
always goes through; count with `>=`, not `==`.

## Testing middleware

A middleware is a plain class. Construct it and call its hooks with a
`MessageContext`, which is a `TypedDict` and therefore a plain `dict`
at runtime:

```python
import logging

import pytest

from myapp.middleware import TimingMiddleware


@pytest.mark.asyncio
async def test_timing_middleware_logs_duration(caplog):
    caplog.set_level(logging.INFO, logger="myapp.timing")
    mw = TimingMiddleware()
    message = {
        "task_id": "test-1",
        "task_name": "send_email",
        "queue": "default",
        "args": [],
        "kwargs": {"to": "a@example.com"},
        "delivery_mode": "at_least_once",
    }

    await mw.pre_execute(message)
    await mw.post_execute(message, result="ok")

    [record] = [r for r in caplog.records if r.getMessage() == "myapp_task_finished"]
    assert record.task_id == "test-1"
    assert record.duration_ms >= 0
```

`caplog` sees the event because the
[example middleware](middleware.md#writing-a-middleware) logs through
`swarmq.logging.get_logger`, which produces ordinary `logging` records:
the event name is the record message, every field a record attribute.
A middleware that logs with an unconfigured `structlog.get_logger()`
prints to stdout instead and never reaches `caplog`.

## Testing signal handlers

Create a `SignalDispatcher`, register the handler and dispatch the
signal yourself:

```python
import pytest

from swarmq import Signal
from swarmq.signals import SignalDispatcher


@pytest.mark.asyncio
async def test_on_task_failed_records_the_attempt():
    dispatcher = SignalDispatcher()
    captured = []

    @dispatcher.on_signal(Signal.TASK_FAILED)
    async def on_failed(task_id, task_name, exception, **kwargs):
        captured.append((task_id, task_name, exception))

    # The payload the worker sends: every key, the exception as its
    # message string.
    await dispatcher.dispatch(
        Signal.TASK_FAILED,
        task_id="t-1",
        task_name="send_email",
        queue="default",
        exception="boom",
    )

    assert captured == [("t-1", "send_email", "boom")]
```

- Dispatch the full payload from the
  [signal table](signals.md#available-signals). A test that leaves out
  `queue` passes even for a handler with a fixed signature, which then
  fails with `TypeError` in the real worker.
- `dispatch()` catches and logs every exception a handler raises, so
  an `assert` inside the handler never fails the test. Record what the
  handler sees and assert afterwards, as above.

## Integration tests with real infrastructure

The round trip (schedule, worker runs, result available) needs a real
RabbitMQ and Valkey, for example the containers from
[Getting started](../getting-started.md#start-the-broker). Use a
dedicated vhost or Valkey database for tests; never point them at
production.

```python title="tests/conftest.py"
import pytest_asyncio

import myapp.tasks  # noqa: F401 (registers the tasks before SwarmQ is built)
from swarmq import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker


@pytest_asyncio.fixture
async def app():
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")
    async with SwarmQ(broker, backend) as application:
        yield application
```

```python title="tests/test_roundtrip.py"
import asyncio

import pytest

from swarmq import Signal


@pytest.mark.asyncio
async def test_roundtrip(app):
    online = asyncio.Event()

    @app.on_signal(Signal.WORKER_ONLINE)
    async def on_online(**kwargs):
        online.set()

    worker_task = asyncio.create_task(app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=30)
        result = await app.run(
            "send_email", to="a@example.com", subject="Hi", timeout=30
        )
        assert result == "sent to a@example.com"
    finally:
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
```

Wait for `WORKER_ONLINE` before you schedule: the worker declares its
queues at start, and a publish to a queue that does not exist yet
raises `UnroutableMessage`.

!!! tip "Burst mode in CI"
    `--burst` drains the queues and exits instead of running a
    long-lived consumer:

    ```bash
    swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
    ```

    The worker exits with `0` once its queues have been idle for
    `--burst-idle-timeout` seconds (default 5). On a fresh broker the
    queues exist only after a worker has declared them: start the
    worker first and schedule within the idle window, or the publish
    raises `UnroutableMessage`.

## Testing deferred and cron scheduling

`app.schedule(delay=N)` returns at once; the task runs only after the
scheduler leader fires it. `delay=0` publishes right away like a plain
`schedule()`. To cover the deferred path in an integration test,
schedule with a short `delay` (for example `1`) and wait for the result
with a timeout of several seconds; the embedded worker takes the
scheduler role when no other worker on the same Valkey holds it.

Test a cron expression without waiting for the clock: compute its fire
times with
[`next_fire_times`](../reference/api.md#next_fire_times), which uses
the scheduler's rule, time zones and clock changes included:

```python
from datetime import UTC, datetime

from swarmq.scheduler import next_fire_times

from myapp.tasks import NightlyReport


def test_nightly_report_fires_at_three_berlin_time():
    info = NightlyReport.task_info
    start = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)

    fires = next_fire_times(info.cron, start, timezone=info.timezone, count=2)

    assert [f.hour for f in fires] == [3, 3]
```

`start` must carry a time zone; a naive `datetime` raises
`ValueError`. The fire times come back in the cron's zone. Whether the
expression is accepted at all is checked when the task module is
imported (`TaskInfo` raises `ValueError`); `swarmq worker --module
myapp.tasks --check` reports it before a deploy.

## Fast vs full suites

- **Unit tests** (no infrastructure): milliseconds per test, run on
  every change.
- **Integration tests** (real RabbitMQ and Valkey): seconds per test.
  Run them in CI before merging and before a release.

In CI, keep the unit tier the default (`pytest tests/unit`) and run the
integration tier in a separate job with the services.

## See also

- [Getting started](../getting-started.md): run the infrastructure
  the integration tests use
- [Defining tasks](defining-tasks.md): the shape of `Task.run()` under
  test
- [`swarmq inspect`](../reference/cli.md#swarmq-inspect): check
  registration without running a worker
