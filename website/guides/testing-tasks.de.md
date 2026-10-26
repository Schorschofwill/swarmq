# Tasks testen

Du willst einen Task testen, ohne dass RabbitMQ oder Valkey auf dem
Host laufen. Ein SwarmQ-Task ist eine gewöhnliche Python-Klasse:
Unit-Tests instanziieren sie und rufen `run()` direkt auf.
Integrationstests decken danach die Teile ab, die die Infrastruktur
brauchen.

## Voraussetzungen

- Ein Task in deinem Projekt (`myapp/tasks.py`).
- `pytest` und `pytest-asyncio` (`pip install pytest pytest-asyncio`).

## `run()` direkt unit-testen

Rufe `run()` als Coroutine auf:

```python title="tests/test_tasks.py"
import pytest

from myapp.tasks import SendEmail


@pytest.mark.asyncio
async def test_send_email_returns_message_id():
    task = SendEmail()

    result = await task.run(to="a@example.com", subject="Hi")

    assert result == "sent to a@example.com"
```

Kein Broker, kein Backend, keine `SwarmQ`-Anwendung: Das Framework
kommt erst ins Spiel, wenn ein Worker die Nachrichten des Tasks
konsumiert.

Zwei Dinge leistet ein direkter Aufruf nicht:

- **Serialisierung.** Der Worker übergibt `run()` die aus JSON
  dekodierten Argumente, ein `datetime` kommt also als `str` an, ein
  `tuple` als `list`
  ([Argumente und Rückgabewerte](defining-tasks.md#argumente-und-ruckgabewerte)).
  Übergib die Werte in der Form, in der der Worker sie liefert.
- **Lebenszyklus.** `pre_execution`, `post_execution`, Timeouts,
  Retries und Middleware laufen nur im Worker.

Definiere Task-Klassen in Modulen, nicht innerhalb von Testfunktionen.
Jede Unterklasse mit `task_info` landet in einer prozessweiten Liste,
und `SwarmQ(...)` wirft `ConfigurationError` (`Task '…' is already
registered`), wenn zwei Klassen denselben Namen tragen.

## Testen mit Abhängigkeiten

Nutzt dein Task gemeinsame Abhängigkeiten (SMTP-Client, HTTP-Session,
DB-Pool), nimm sie als Konstruktor-Argumente entgegen und übergib im
Test Attrappen. Der Worker baut den Task mit `SendEmail()`, darum
braucht jeder Konstruktor-Parameter einen Standardwert:

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

!!! note "Tasks mit `FromDishka`-Parametern"
    Ein Task, dessen `run()` mit
    [`FromDishka[...]`](../reference/api.md#dependency-injection-dishka)
    annotierte Parameter nimmt, lässt sich nicht direkt aufrufen:
    `run()` wirft `RuntimeError` („No active SwarmQ request scope“),
    auch wenn du den Parameter selbst übergibst. Halte die Logik in
    einer gewöhnlichen Klasse oder Funktion, die du direkt testest, und
    decke den injizierten Task mit einem Integrationstest ab.

## Fortschrittsmeldungen testen

`update_progress()` tut nichts, solange kein Worker den Task
verdrahtet hat (`_backend` und `_task_id` sind `None`). Unit-Tests
können es also ohne Valkey aufrufen.

Um zu prüfen, dass Fortschritt geschrieben wurde, setze ein
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

Die Attribute `_backend`, `_task_id` und `_result_ttl_ms` sind
absichtlich zuweisbar, Tests brauchen also kein Monkeypatching.
Schreibvorgänge sind auf einen je 500 ms gedrosselt, die letzte
Meldung (`current == total`) geht immer durch; zähle mit `>=`, nicht
mit `==`.

## Middleware testen

Eine Middleware ist eine gewöhnliche Klasse. Erzeuge sie und rufe ihre
Hooks mit einem `MessageContext` auf; der ist ein `TypedDict` und zur
Laufzeit ein gewöhnliches `dict`:

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

`caplog` sieht das Ereignis, weil die
[Beispiel-Middleware](middleware.md#eine-middleware-schreiben) über
`swarmq.logging.get_logger` loggt. Das erzeugt gewöhnliche
`logging`-Records: Der Ereignisname ist die Record-Nachricht, jedes
Feld ein Record-Attribut. Eine Middleware, die mit einem
unkonfigurierten `structlog.get_logger()` loggt, schreibt stattdessen
nach stdout und erreicht `caplog` nie.

## Signal-Handler testen

Erzeuge einen `SignalDispatcher`, registriere den Handler und löse das
Signal selbst aus:

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

    # Die Nutzlast, die der Worker sendet: jeder Schlüssel, die
    # Ausnahme als Nachrichtentext.
    await dispatcher.dispatch(
        Signal.TASK_FAILED,
        task_id="t-1",
        task_name="send_email",
        queue="default",
        exception="boom",
    )

    assert captured == [("t-1", "send_email", "boom")]
```

- Löse das Signal mit der vollständigen Nutzlast aus der
  [Signal-Tabelle](signals.md#verfugbare-signals) aus. Ein Test, der
  `queue` weglässt, besteht auch für einen Handler mit fester Signatur,
  der dann im echten Worker mit `TypeError` scheitert.
- `dispatch()` fängt und loggt jede Ausnahme eines Handlers, ein
  `assert` im Handler lässt den Test also nie scheitern. Halte fest,
  was der Handler sieht, und prüfe danach, wie oben.

## Integrationstests mit echter Infrastruktur

Der Rundlauf (einplanen, Worker führt aus, Ergebnis liegt vor) braucht
ein echtes RabbitMQ und Valkey, etwa die Container aus dem
[Schnellstart](../getting-started.md#broker-starten). Nutze für Tests
einen eigenen vhost oder eine eigene Valkey-Datenbank, nie die
Produktion.

```python title="tests/conftest.py"
import pytest_asyncio

import myapp.tasks  # noqa: F401 (registriert die Tasks, bevor SwarmQ gebaut wird)
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

Warte auf `WORKER_ONLINE`, bevor du einplanst: Der Worker deklariert
seine Queues beim Start, und ein Publish in eine Queue, die es noch
nicht gibt, wirft `UnroutableMessage`.

!!! tip "Burst-Modus in der CI"
    `--burst` leert die Queues und beendet sich, statt als
    langlebiger Consumer weiterzulaufen:

    ```bash
    swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
    ```

    Der Worker endet mit `0`, sobald seine Queues
    `--burst-idle-timeout` Sekunden (Standard 5) still waren. Auf einem
    frischen Broker gibt es die Queues erst, wenn ein Worker sie
    deklariert hat: Starte den Worker zuerst und plane innerhalb des
    Leerlauffensters ein, sonst wirft der Publish `UnroutableMessage`.

## Verzögertes und Cron-Scheduling testen

`app.schedule(delay=N)` kehrt sofort zurück; der Task läuft erst, wenn
der Scheduler-Leader ihn auslöst. `delay=0` veröffentlicht sofort wie
ein gewöhnliches `schedule()`. Um den verzögerten Pfad in einem
Integrationstest abzudecken, plane mit kurzem `delay` (etwa `1`) ein
und warte mit einem Timeout von einigen Sekunden auf das Ergebnis; der
eingebettete Worker übernimmt die Scheduler-Rolle, wenn kein anderer
Worker auf demselben Valkey sie hält.

Einen Cron-Ausdruck testest du, ohne auf die Uhr zu warten: Berechne
seine Termine mit
[`next_fire_times`](../reference/api.md#next_fire_times). Die Funktion
nutzt die Regel des Schedulers, Zeitzonen und Zeitumstellungen
eingeschlossen:

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

`start` muss eine Zeitzone tragen; ein naives `datetime` wirft
`ValueError`. Die Termine kommen in der Zone des Crons zurück. Ob der
Ausdruck überhaupt angenommen wird, prüft schon der Import des
Task-Moduls (`TaskInfo` wirft `ValueError`); `swarmq worker --module
myapp.tasks --check` meldet es vor einem Deploy.

## Schnelle und vollständige Suiten

- **Unit-Tests** (ohne Infrastruktur): Millisekunden je Test, bei
  jeder Änderung ausführen.
- **Integrationstests** (echtes RabbitMQ und Valkey): Sekunden je
  Test. In der CI vor dem Merge und vor einem Release ausführen.

Halte in der CI die Unit-Stufe als Standard (`pytest tests/unit`) und
führe die Integrationsstufe in einem eigenen Job mit den Diensten aus.

## Siehe auch

- [Schnellstart](../getting-started.md): die Infrastruktur starten,
  die die Integrationstests nutzen
- [Tasks definieren](defining-tasks.md): die Form von `Task.run()` im
  Test
- [`swarmq inspect`](../reference/cli.md#swarmq-inspect): die
  Registrierung prüfen, ohne einen Worker zu starten
