# Tasks testen

Du willst einen Task unit-testen, ohne RabbitMQ oder Valkey auf dem
Host laufen zu lassen. SwarmQ-Tasks sind einfache Python-Klassen —
der schnellste Weg ist, sie zu instanziieren und `run()` direkt
aufzurufen. Integration-Tests decken dann die Teile ab, die
wirklich die Infrastruktur brauchen.

## Voraussetzungen

- Ein Task, definiert in deinem Projekt (`myapp/tasks.py`).
- `pytest` installiert (`pip install pytest pytest-asyncio`).

## `run()` direkt unit-testen

Der einfachste Test: `run()` als Coroutine aufrufen.

```python title="tests/test_tasks.py"
import pytest

from myapp.tasks import SendEmail


@pytest.mark.asyncio
async def test_send_email_returns_message_id():
    task = SendEmail()

    result = await task.run(to="a@example.com", subject="Hi")

    assert result == "sent to a@example.com"
```

Kein Broker, kein Backend, keine `SwarmQ`-Anwendung. `Task`-
Subklassen sind reguläre Python-Klassen — das Framework greift
erst, wenn ein Worker ihre Nachrichten konsumiert.

## Testen mit Dependencies

Nutzt dein Task geteilte Dependencies (SMTP-Client, HTTP-Session,
DB-Pool), injiziere sie im Konstruktor und override in Tests:

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

Dieses Pattern hält auch den Task selbst sauber — die Dependency
ist ein expliziter Konstruktor-Arg, kein Modul-Level-Global.

## Progress-Updates testen

`update_progress()` ist ein No-Op, wenn kein Worker den Task
gewired hat (`_backend` und `_task_id` sind `None`). Das bedeutet:
Unit-Tests rufen ihn safe auf, ohne Valkey zu brauchen.

Wenn du asserten musst, dass Progress emittiert wurde, wire ein
Mock-Backend:

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

Framework-interne Attrs (`_backend`, `_task_id`,
`_result_ttl_ms`) sind bewusst zuweisbar, damit Tests Mocks
installieren können, ohne die Klasse zu monkey-patchen.

## Middleware testen

Middleware ist eine einfache Klasse; konstruiere eine und rufe
ihre Hooks mit einem synthetisierten `MessageContext` auf:

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

Das `message`-Dict kann literal konstruiert werden —
`MessageContext` ist ein `TypedDict`, also zur Laufzeit ein
einfaches `dict`.

`caplog` sieht das Event, weil die
[Beispiel-Middleware](middleware.md#eine-middleware-schreiben) über
`swarmq.logging.get_logger` loggt und damit gewöhnliche
`logging`-Records erzeugt: Der Event-Name ist die Record-Nachricht,
jedes Feld ein Record-Attribut. Eine Middleware, die mit einem
unkonfigurierten `structlog.get_logger()` loggt, schreibt stattdessen
auf stdout und erreicht `caplog` nie.

## Signal-Handler testen

Instanziiere `SignalDispatcher` direkt, registriere den Handler
und dispatche manuell:

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

    # Dispatch the payload the worker sends: every key, and the
    # exception as its message string.
    await dispatcher.dispatch(
        Signal.TASK_FAILED,
        task_id="t-1",
        task_name="send_email",
        queue="default",
        exception="boom",
    )

    assert captured == [("t-1", "send_email", "boom")]
```

Dispatche das vollständige Payload aus der
[Signal-Tabelle](signals.md#verfugbare-signals). Ein Test, der `queue`
weglässt, besteht auch für einen Handler mit fester Signatur, der im
echten Worker dann mit `TypeError` scheitert.

## Integration-Tests mit echter Infrastruktur

Für den Roundtrip (schedule → Worker läuft → Ergebnis da)
brauchst du ein echtes RabbitMQ und Valkey. Die
`docker-compose.yml` des Repos startet beide; Tests können sich
auf sie als Fixture verlassen:

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

from myapp.tasks import SendEmail  # noqa — registriert den Task


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
    Nutze `--burst` in CI, um zu drainen und zu exiten, statt einen
    langlebigen Consumer laufen zu lassen:

    ```bash
    swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
    ```

    Plane erst Tasks, dann starte den Worker. Er exited mit 0,
    sobald die Queue leer und idle ist.

## Deferred- und Cron-Scheduling testen

`app.schedule(delay=N)` blockiert nicht — Tests müssen Zeit
vorrücken oder warten. Einfachste Form: mit `delay=0` planen,
asserten, dass das Ergebnis zurückkam, dann einen zweiten Test mit
`delay=1` hinzufügen, um den Scheduler-Pfad abzudecken.

Für Cron-Tests injiziere die aktuelle Zeit via Fixture, die
`datetime.now` patcht:

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

Nutze `monkeypatch` sparsam — das meiste Scheduler-Verhalten wird
bereits von SwarmQs eigener Testsuite abgedeckt.

## Fast vs Full Suites

- **Unit-Tests** (ohne Infra): Sub-Sekunde pro Test, läuft bei
  jeder Änderung.
- **Integration-Tests** (echtes RabbitMQ + Valkey): Sekunden bis
  zehn Sekunden. Läuft bei Push zu main und vor Release.

Halte in CI Unit-only als Default (`pytest tests/unit`) und gate
die Integration-Tier hinter einem separaten Job mit Docker.

## Siehe auch

- [Schnellstart](../getting-started.md) — starte die
  Infrastruktur, die die Integration-Tests nutzen
- [Tasks definieren](defining-tasks.md) — die Form von
  `Task.run()` unter Test
- [`swarmq inspect`](../reference/cli.md#swarmq-inspect) —
  Registrierung ohne Worker-Start sanity-checken
