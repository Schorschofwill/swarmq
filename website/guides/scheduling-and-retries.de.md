# Scheduling und Retries

Du willst steuern, **wann** ein Task läuft und **wie oft** er bei
Fehler retry't wird. Beides lebt auf `TaskInfo` (Defaults für jede
Ausführung) und auf `Client.schedule()` (Per-Call-Overrides).

## Voraussetzungen

- Eine Task-Klasse mit `task_info = TaskInfo(name=..., queues=...)`.
- Ein laufender Broker und Backend — siehe
  [Schnellstart](../getting-started.md).

## Immediate vs Deferred

Default-`schedule()` ist immediate — die Nachricht landet in der
Queue, der erste bereite Worker holt sie.

```python
task_id = await app.schedule("send_email", to="a@example.com")
```

Zwei kwargs verzögern den Publish: `delay=` und `eta=`.

```python
# In 30 Sekunden laufen.
await app.schedule("send_email", to="a@example.com", delay=30)

# Zu einem spezifischen Zeitpunkt (muss in Zukunft; 1s Skew ok).
from datetime import datetime, timedelta, UTC
eta = datetime.now(UTC) + timedelta(minutes=15)
await app.schedule("send_email", to="a@example.com", eta=eta)
```

Deferred Tasks sitzen nicht in der RabbitMQ-Queue — sie sitzen im
Valkey-Scheduler-Store. Ein Worker (der gewählte Leader) pollt den
Store und publisht zum Fire-Zeitpunkt an RabbitMQ. Siehe
[Architektur → Message Lifecycle](../explanation/architecture.md#nachrichten-lifecycle).

`delay=0` ist ein einfacher Immediate-Publish; nur positives
`delay` oder beliebiges `eta` nimmt den Scheduler-Pfad.

## Cron-Scheduling

Für wiederkehrende Arbeit setze `cron` auf `TaskInfo`:

```python
class DailyReport(Task):
    task_info = TaskInfo(
        name="daily_report",
        queues=["reports"],
        cron="0 3 * * *",  # jeden Tag um 03:00 UTC
    )

    async def run(self) -> None:
        ...
```

Nur Standard-5-Feld-Cron; Extended Syntax (`L`, `W`, `#`) wird zur
Klassendefinitionszeit abgelehnt. Der eingebettete Scheduler des
Workers nimmt die Deklaration automatisch auf — kein separater
Cron-Daemon.

### CLI-definierte Schedules

Schedules hinzufügen ohne Code-Deploy:

```bash
swarmq schedule add weekly-billing \
  --cron '0 4 * * 1' \
  --task run_billing \
  --kwargs '{"tenant": "acme"}'

swarmq schedule list
swarmq schedule status --limit 5
```

Siehe [CLI → `swarmq schedule`](../reference/cli.md#swarmq-schedule)
für die volle Grammatik.

## Retry-Policy

Retries sind per Default aus. Aktiviere sie auf `TaskInfo`:

```python
class ChargeCard(Task):
    task_info = TaskInfo(
        name="charge",
        queues=["payments"],
        max_retries=5,
        retry_backoff="exponential",  # "linear", "fixed" auch verfügbar
        retry_base_delay=1.0,
    )

    async def run(self, card_id: str, amount: int) -> dict:
        return await stripe.charge(card_id, amount)
```

### Backoff-Formeln

| Strategie | Delay bei Versuch *n* (ab `n=1`) |
|---|---|
| `exponential` (Default) | `retry_base_delay × 2^(n-1)` |
| `linear` | `retry_base_delay × n` |
| `fixed` | `retry_base_delay` |

Globales Cap: `retry_max_delay` (Default 3600 s) kürzt jeden
berechneten Delay. Setze `SWARMQ_RETRY_MAX_DELAY=...` oder
übergib `retry_max_delay=...` als `SwarmQ`-kwarg.

### Selektive Retries

Per Default verbrennt jede Exception aus `run()` einen
Retry-Versuch. Um nur spezifische Klassen zu retry'n, übergib
`retry_exceptions`:

```python
task_info = TaskInfo(
    name="charge",
    queues=["payments"],
    max_retries=5,
    retry_exceptions=[stripe.RateLimitError, stripe.APIConnectionError],
)
```

Jede andere Exception — `ValueError`, `KeyError`, der
Card-Declined-Fehler — umgeht das Retry-Budget und geht direkt in
die DLQ.

### Task-Body-Kontrolle mit `Retry` und `NoRetry`

Innerhalb `run()` kannst du die Policy überschreiben:

```python
from swarmq import Retry, NoRetry

class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email", queues=["default"], max_retries=3,
    )

    async def run(self, to: str) -> None:
        response = await smtp.send(to)
        if response.status == 429:
            raise Retry(delay=60.0)  # Retry in 60 s
        if response.status in (400, 403):
            raise NoRetry(f"unrecoverable: {response.status}")
```

- `Retry(delay=...)` — retry mit spezifischem Delay, ignoriert den
  konfigurierten Backoff. Nutze, wenn du die korrekte Wartezeit
  kennst (z. B. HTTP `Retry-After: 60`).
- `Retry()` ohne Delay — nutzt die Backoff-Formel.
- `NoRetry(reason)` — überspringt das Retry-Budget, routet sofort
  in die DLQ.

## Per-Call-Overrides

Alles auf `TaskInfo` ist ein Default. `schedule()` nimmt Overrides
für Scheduling-bezogene kwargs:

```python
# Queue zur Schedule-Zeit überschreiben.
await app.schedule("send_email", to="...", queue="priority")

# Dedup-Settings überschreiben.
await app.schedule("send_email", to="...", unique_for=300)
```

Retry-Policy kann nicht per Call überschrieben werden — sie ist
eine Eigenschaft des Tasks, nicht des Aufrufs.

## Timeouts

`TaskInfo.soft_timeout` und `TaskInfo.hard_timeout` (beide in
Sekunden) steuern, wie lange der Worker `run()` laufen lässt.

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports"],
    soft_timeout=240,
    hard_timeout=300,
)
```

- **Soft Timeout.** Der Worker raist `SoftTimeoutError` innerhalb
  `run()`, damit dein Task checkpointen und aufräumen kann.
- **Hard Timeout.** Der Worker bricht die Coroutine ab.

`hard_timeout` muss strikt größer als `soft_timeout` sein, wenn
beides gesetzt ist. `hard_timeout` ist auch pflicht, wenn ein Task
`lock=...` deklariert.

## Task-Expiry

Für zeitkritische Arbeit deklariere `max_age`, damit abgestandene
Tasks nie laufen:

```python
task_info = TaskInfo(
    name="send_otp",
    queues=["auth"],
    max_age=60,  # Abandon, wenn älter als 60 s bei Pickup
)
```

Bei Pickup, wenn `now - enqueued_at > max_age`, routet der Worker
die Nachricht mit `TASK_EXPIRED`-Signal in die DLQ und ruft
`run()` nicht auf.

## Cancellation

Um einen bereits in-flight (oder noch in der Queue liegenden) Task
abzubrechen:

```python
cancelled = await app.cancel(task_id)
```

Der Aufruf ist Fire-and-forget — er setzt ein Flag in Valkey und
kehrt zurück. Der Cancel-Monitor des Workers picks das Flag auf
und raist `TaskCancelledError` innerhalb der Task-Coroutine. Siehe
[`app.cancel()`](../reference/api.md#cancel) für den
Stale-Flag-Caveat.

## Verifizieren

Plane etwas mit Retries und beobachte die Logs:

```python
import asyncio
from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
import myapp.tasks  # noqa — registriert den Task

async def main() -> None:
    async with SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    ) as app:
        tid = await app.schedule("flaky", delay=2.0)
        print("scheduled", tid)

asyncio.run(main())
```

Der Worker-Log zeigt sofort ein `TASK_ENQUEUED` und eine
`TASK_STARTED` / `TASK_RETRYING` / `TASK_COMPLETED`-Sequenz mit
den konfigurierten Delays zwischen den Versuchen.

## Siehe auch

- [Zustellungssemantik](../explanation/delivery-semantics.md) — was
  at-least-once bei Retries bedeutet
- [Dead-Letter-Queues](dead-letter-queues.md) — was nach
  `max_retries`-Erschöpfung passiert
- [`TaskInfo.retry_*` Referenz](../reference/configuration.md#retries)
- [`Retry` / `NoRetry` API](../reference/api.md#exceptions)
