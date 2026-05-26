# Testing tasks

You want to unit-test a task without a RabbitMQ or Valkey running
on the host. SwarmQ tasks are plain Python classes — the fast path
is to instantiate and call `run()` directly. Integration tests then
cover the parts that actually need the infrastructure.

## Prerequisites

- A task defined in your project (`myapp/tasks.py`).
- `pytest` installed (`pip install pytest pytest-asyncio`).

## Unit-testing `run()` directly

The simplest test: call `run()` as a coroutine.

```python title="tests/test_tasks.py"
import pytest

from myapp.tasks import SendEmail


@pytest.mark.asyncio
async def test_send_email_returns_message_id():
    task = SendEmail()

    result = await task.run(to="a@example.com", subject="Hi")

    assert result == "sent to a@example.com"
```

No broker, no backend, no `SwarmQ` application. `Task` subclasses
are regular Python classes — the framework only kicks in when a
worker consumes their messages.

## Testing with dependencies

If your task uses shared dependencies (SMTP client, HTTP session,
DB pool), inject them in the constructor and override in tests:

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

This pattern also keeps the task itself clean — the dependency is
an explicit constructor argument, not a module-level global.

## Testing progress updates

`update_progress()` is a no-op when no worker has wired the task
(`_backend` and `_task_id` are `None`). That means unit tests call
it safely without needing a Valkey.

If you need to assert progress was emitted, wire a mock backend:

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

Framework-internal attrs (`_backend`, `_task_id`, `_result_ttl_ms`)
are intentionally assignable so tests can install mocks without
monkey-patching the class.

## Testing middleware

Middleware is a plain class; construct one and call its hooks
with a synthesised `MessageContext`:

```python
import pytest
from myapp.middleware import TimingMiddleware


@pytest.mark.asyncio
async def test_timing_middleware_logs_duration(caplog):
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

    assert "task_finished" in caplog.text
```

The `message` dict can be constructed literally — `MessageContext`
is a `TypedDict`, i.e. a plain `dict` at runtime.

## Testing signal handlers

Instantiate `SignalDispatcher` directly, register the handler, and
dispatch manually:

```python
import pytest
from swarmq import Signal
from swarmq.signals import SignalDispatcher


@pytest.mark.asyncio
async def test_on_task_failed_captures_to_sentry():
    dispatcher = SignalDispatcher()
    captured = []

    @dispatcher.on_signal(Signal.TASK_FAILED)
    async def on_failed(task_id, task_name, exception):
        captured.append((task_id, task_name, exception))

    await dispatcher.dispatch(
        Signal.TASK_FAILED,
        task_id="t-1",
        task_name="send_email",
        exception=RuntimeError("boom"),
    )

    assert captured == [("t-1", "send_email", captured[0][2])]
```

## Integration tests with real infrastructure

For the round-trip (schedule → worker runs → result available),
you need a real RabbitMQ and Valkey. The repo's
`docker-compose.yml` starts both; tests can rely on it as a
fixture:

```python title="tests/conftest.py"
import pytest
import pytest_asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend


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

from myapp.tasks import SendEmail  # noqa — registers the task


@pytest.mark.asyncio
async def test_roundtrip(app):
    worker_task = asyncio.create_task(app.start_worker())
    try:
        result = await app.run("send_email",
                               to="a@example.com", subject="Hi",
                               timeout=30)
        assert result == "sent to a@example.com"
    finally:
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
```

!!! tip
    Use `--burst` in CI to drain and exit instead of running a
    long-lived consumer:

    ```bash
    swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
    ```

    Schedule tasks first, then run the worker. It exits 0 once the
    queue is empty and idle.

## Testing deferred and cron scheduling

`app.schedule(delay=N)` does not block — tests need to advance time
or wait. The simplest shape: schedule with `delay=0`, assert the
result came back, then add a second test with `delay=1` to cover
the scheduler path.

For cron tests, inject the current time via a fixture that patches
`datetime.now`:

```python
@pytest.fixture
def freeze_time(monkeypatch):
    import datetime
    fixed = datetime.datetime(2026, 4, 21, 3, 0, tzinfo=datetime.UTC)

    class _Frozen(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed if tz is None else fixed.astimezone(tz)

    monkeypatch.setattr(datetime, "datetime", _Frozen)
```

Use `monkeypatch` sparingly — most scheduler behaviour is already
covered by SwarmQ's own test suite.

## Fast vs full suites

- **Unit tests** (no infra): subsecond per test, run on every
  change.
- **Integration tests** (real RabbitMQ + Valkey): seconds to tens
  of seconds. Run on push to main, and before release.

In CI, keep unit-only the default (`pytest tests/unit`) and gate
the integration tier behind a separate job with Docker.

## See also

- [Getting Started](../getting-started.md) — run the infrastructure
  the integration tests use
- [Defining tasks](defining-tasks.md) — the shape of `Task.run()`
  under test
- [`swarmq inspect`](../reference/cli.md#swarmq-inspect) — sanity
  check registration without running a worker
