# API-Referenz

Die öffentliche API ist das, was `from swarmq import ...` dir
liefert. Diese Seite dokumentiert jede öffentliche Klasse und die
Methoden, die Anwendungscode aufrufen soll. Die Library ist async
geschrieben; jede Methode mit I/O ist eine Coroutine.

## Modul-Layout

```python
from swarmq import (
    SwarmQ,              # Anwendungs-Entrypoint
    Task, TaskInfo,      # Task-Definition
    DeliveryMode,        # at_least_once | fire_and_forget
    Middleware,          # Middleware-Basisklasse
    MessageContext,      # TypedDict, das an Middleware-Hooks geht
    Signal,              # Lifecycle-Event-Enum
    SignalDispatcher,    # Signal-Registry
    ProgressInfo,        # Task-Progress-Snapshot
    Retry, NoRetry,      # Task-seitiger Control Flow
    SwarmQError,         # Basis-Exception
    # ...und der Rest der Exception-Hierarchie
)

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
```

## `class SwarmQ`

Haupt-Anwendungs-Entrypoint. Wrappt einen Broker + ein Backend,
besitzt die Task-Registry, den Middleware-Stack und den
Signal-Dispatcher.

```python
SwarmQ(
    broker: RabbitMQBroker,
    backend: ValkeyBackend,
    **config_kwargs: Any,
)
```

Ein `SwarmQ` ist zugleich **Client** (für Scheduling) und der
Context, aus dem ein **Worker** seine Config liest. Nutze ihn als
async Context Manager — das Betreten verbindet Broker und Backend,
das Verlassen schließt beides.

```python
async with SwarmQ(broker, backend) as app:
    await app.run("send_email", to="a@example.com")
```

**Parameter**

| Name | Typ | Beschreibung |
|---|---|---|
| `broker` | `RabbitMQBroker` | Wird bei `__aenter__` verbunden. |
| `backend` | `ValkeyBackend` | Wird bei `__aenter__` verbunden. |
| `**config_kwargs` | — | An `SwarmQConfig` weitergereicht. Siehe [Konfiguration](configuration.md). |

**Hinweise.** Eine einzelne `SwarmQ`-Instanz ist safe, sie über
Coroutines auf derselben Event-Loop zu teilen. Teile sie *nicht*
über Prozesse hinweg — jeder Prozess baut seine eigene.

### `__aenter__` / `__aexit__`

```python
async def __aenter__(self) -> SwarmQ
async def __aexit__(self, exc_type, exc_val, exc_tb) -> None
```

Verbindet zuerst `broker`, dann `backend` beim Enter; schließt
beides beim Exit. Scheitert die Backend-Verbindung, wird die
Broker-Verbindung zurückgerollt, bevor die Exception propagiert.

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

Enqueuet einen Task und gibt seine `task_id` zurück.

**Parameter**

| Name | Typ | Beschreibung |
|---|---|---|
| `task_name` | `str` | Name, den `TaskInfo.name` deklariert. |
| `*args`, `**kwargs` | `Any` | An `Task.run()` weitergegeben. Müssen JSON-kompatibel sein. |
| `queue` | `str \| None` | Überschreibt die Default-Queue des Tasks. Im Client-Only-Modus (ohne Registry) pflicht. |
| `delay` | `float \| None` | Sekunden Verzögerung vor Zustellung. Gegenseitig ausschließend mit `eta`. Muss `≥ 0` sein. |
| `eta` | `datetime \| None` | Absoluter Fire-Zeitpunkt. Gegenseitig ausschließend mit `delay`. Muss in der Zukunft liegen (1s Lenienz). |
| `unique_for` | `int \| None` | Überschreibt `TaskInfo.unique_for`. `None` deaktiviert Dedup für diesen Aufruf. |
| `unique_until` | `"start" \| "completion"` | Überschreibt `TaskInfo.unique_until`. |

**Returns.** `str` — die `task_id` (UUIDv7). Bei Dedup-Hit wird
die *bestehende* Owner-`task_id` still zurückgegeben; ein
`TASK_DUPLICATE_DETECTED`-Signal feuert zur Observability.

**Raises.**

- `TaskNotFoundError` — task_name nicht in der Registry (Registry-Modus).
- `ValueError` — `delay < 0`, `delay` und `eta` gleichzeitig, `eta` in Vergangenheit.
- `BrokerConnectionError` — Broker beim Publish unerreichbar.
- Propagiert aus `pre_enqueue`-Middleware-Hooks.

**Beispiel.**

```python
task_id = await app.schedule(
    "send_email",
    to="alice@example.com", subject="Welcome",
    delay=60.0,           # in einer Minute zustellen
    unique_for=3600,      # Dedup innerhalb der Stunde
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

Enqueuet viele Tasks in einem Batch. Jedes Element ist ein
`(task_name, args, kwargs)`-Tupel. Nebenläufig via `asyncio.gather`
published.

**Returns.** `list[str]` — eine `task_id` pro Input, in
Input-Reihenfolge. Bei Dedup-Hits wird die Owner-task_id an diesem
Index platziert (Listenlänge ist immer gleich Input-Länge).

**Raises.** Dieselbe Form wie `schedule()`. Bei partiellem
Publish-Fehlschlag werden erfolgreich publishte task_ids trotzdem
in die Result-Liste geschrieben, der Rest wird rollbacked, und
die erste Exception wird erneut geraist.

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

Enqueuet einen Task und wartet auf das Ergebnis. Komfort-Wrapper
um `schedule()` + `get_result()`.

**Raises.** Alles, was `schedule()` raist, plus
`ResultNotReadyError` bei Timeout, `TaskError` bei Task-Fehlschlag,
`TaskCancelledError` bei Cancel.

**Beispiel.**

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

Blockiert, bis das Task-Ergebnis verfügbar ist oder `timeout`
abläuft.

Intern wird `sq:notify:{task_id}` auf Valkey abonniert für ein
Push-basiertes Warten, mit 5ms-Polling als Fallback. Bei
`timeout=0` ein einzelnes `GET` ohne Pub/Sub.

**Raises.**

- `ResultNotReadyError` — kein Ergebnis innerhalb `timeout`.
- `TaskError` — Task schlug fehl; `str(exc)` trägt die Worker-
  Fehlermeldung.
- `TaskCancelledError` — Cancel-Flag während Ausführung ausgelöst.

### `get_results()`

```python
async def get_results(
    task_ids: list[str],
    timeout: float = 10,
    poll_interval: float = 0.005,
) -> list[Any]
```

Batch-Poll-Variante von `get_result`. Nutzt `MGET`, um viele auf
einmal zu holen. Raist `ResultNotReadyError` mit den fehlenden
task_ids bei jedem Timeout.

### `cancel()`

```python
async def cancel(task_id: str) -> bool
```

Fordert den Abbruch eines Tasks an. Fire-and-forget: setzt ein
Cancel-Flag mit TTL `result_ttl × 1000` ms und kehrt sofort zurück.

**Returns.** `True`, wenn das Flag neu gesetzt wurde, `False`, wenn
ein vorheriger `cancel()`-Aufruf noch in Flight ist.

**Raises.** `ValueError` bei leerer `task_id`.

!!! warning "Stale Cancel-Flag"
    Die Flag-TTL ist per Default 24 h. Wird dieselbe `task_id`
    innerhalb dieses Fensters wiederverwendet (deterministische
    IDs, Redelivery nach Crash, bevor der Worker das Flag gelöscht
    hat), wird das stale Flag auch die nächste Zustellung abbrechen.
    Mitigation: lass SwarmQ UUIDv7 task_ids erzeugen (Default)
    oder übergib eine kürzere `ttl_ms` direkt via
    `Client.cancel()`.

### `get_progress()` / `watch_progress()`

```python
async def get_progress(task_id: str) -> ProgressInfo | None
def watch_progress(task_id: str) -> AsyncIterator[ProgressInfo]
```

Liest Task-Progress, den `Task.update_progress(current, total,
message)` geschrieben hat.

- `get_progress` ist ein One-Shot-Read; gibt `None` zurück, wenn
  kein Progress aufgezeichnet ist (nie gestartet, TTL abgelaufen,
  unbekannte task_id oder malformed HASH).
- `watch_progress` yields Progress-Updates als async Iterator.
  Abonniert zuerst den Pub/Sub-Channel, yields dann jeden bereits
  gespeicherten HASH-State, streamt danach nachfolgende Updates.
  Der Iterator endet nie von allein — `break` wenn zufrieden.

**Beispiel.**

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

Registriert eine Middleware-Instanz. Safe, jederzeit aufzurufen —
vor oder nach `start_worker()`, vor oder nach `schedule()`. Die
Kette wird pro Operation aufgelöst, also ist neu hinzugefügte
Middleware ab dem nächsten Aufruf sichtbar.

### `on_signal()`

```python
def on_signal(signal: Signal) -> Callable[[AsyncHandler], AsyncHandler]
```

Gibt einen Decorator zurück, der einen async-Handler für ein
Lifecycle-Signal registriert. Handler laufen sequentiell in
Registrierungs-Reihenfolge; Exceptions werden auf WARNING geloggt
und brechen die Dispatch-Schleife nicht ab.

**Beispiel.**

```python
@app.on_signal(Signal.TASK_FAILED)
async def on_failure(task_id, task_name, exception):
    await sentry.capture(exception, tags={"task": task_name})
```

### `start_worker()`

```python
async def start_worker(self) -> None
```

Betritt die Consumer-Schleife. Kehrt bei Shutdown (SIGTERM/SIGINT)
zurück. Wird von der CLI (`swarmq worker`) aufgerufen; aus
Anwendungscode selten direkt.

## `class TaskInfo`

Frozen deklarative Config für einen Task. Feld-Validierung läuft
bei der Klassendefinitionszeit — ein Bad Value raist `ValueError`
beim Import, nicht beim ersten Schedule.

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

Feld-Semantik und Validierungsregeln leben in
[Konfigurations-Referenz — TaskInfo-Felder](configuration.md#taskinfo-felder).

## `class Task`

Basisklasse für Task-Definitionen. Subclassing mit einem
`task_info = TaskInfo(...)`-Attribut registriert die Klasse
automatisch.

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

### Methoden, die du überschreibst

- `run(*args, **kwargs)` — der Task-Body. Aufgerufen mit den
  Args/Kwargs, die der Client an `schedule()` / `run()` übergeben
  hat. **Pflicht.**
- `pre_execution()` — vor `run()` aufgerufen. Default: No-Op.
- `post_execution()` — nach `run()` aufgerufen (Success oder
  Fehler). Default: No-Op.

### Methoden, die du aus `run()` aufrufst

- `update_progress(current, total, message="")` — Progress
  aufzeichnen. Gedrosselt auf einen Write pro 500 ms pro
  Task-Instanz; das finale Update (`current == total`) flusht
  immer. Safe No-Op außerhalb eines Worker-Contexts (Unit-Tests).

### Was du nicht überschreibst

Alles Weitere — der Registry-Hook, die Worker-Wiring-Instanz-
Attrs (`_task_id`, `_backend` usw.), der Progress-Lock — ist
Framework-intern. Subklassen, die sich nur um den Task-Body
kümmern, überschreiben nur `run`.

**Beispiel.**

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

Basisklasse für Middleware — Querschnittsthemen, die um jedes
Task-Lifecycle-Event herum laufen.

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

Alle Hooks defaulten auf Async-No-Ops; überschreibe nur die, die du
brauchst.

**Fehlersemantik.**

- `pre_enqueue`-Exceptions **propagieren** — eine raisende
  Middleware bricht das Enqueue ab.
- Alle anderen Hooks **schlucken** Exceptions: Fehler werden auf
  ERROR via structlog geloggt, die Kette läuft weiter.

**Registrierungs-Reihenfolge.**

- Vorwärts für `pre_*`-Hooks.
- Rückwärts für `post_*`, `on_error`, `on_retry`-Hooks (klassische
  „around"-Semantik).

Registriere mit `app.add_middleware(SomeMiddleware())`. Siehe
[Middleware-Guide](../guides/middleware.md) für Beispiele.

## `TypedDict MessageContext`

Der kanonische Nachrichten-Envelope, den jeder Middleware-Hook
bekommt.

```python
class MessageContext(TypedDict):
    task_id: str
    task_name: str
    queue: str
    args: list[Any]
    kwargs: dict[str, Any]
    delivery_mode: str
    retry_count: NotRequired[int]  # nur Worker-seitige Hooks
```

`retry_count` ist auf Worker-seitigen Hooks vorhanden
(`pre_execute`, `post_execute`, `on_error`, `on_retry`); auf
Client-seitigen Hooks (`pre_enqueue`, `post_enqueue`) abwesend.
Behandle es als optional.

## `enum Signal`

Lifecycle-Events, die SwarmQ emittiert. Registriere Handler mit
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

Handler müssen `**kwargs` akzeptieren (oder jedes Feld explizit
nennen). Nutze `kwargs.get("batch_index")` für das nur in
`TASK_DUPLICATE_DETECTED` vorhandene Feld, da Nicht-Batch-Quellen
es nicht enthalten.

## `class ProgressInfo`

Immutabler Snapshot des Task-Progress, von `app.get_progress()`
zurückgegeben und von `app.watch_progress()` geyielded.

Kernfelder:

| Feld | Typ | Beschreibung |
|---|---|---|
| `current` | `int` | Aktuelle Einheiten-Anzahl. |
| `total` | `int` | Gesamt-Einheiten-Anzahl. |
| `percent` | `float` | `current / total * 100`, 0.0–100.0. |
| `message` | `str` | Optionale Statuszeile. |
| `updated_at` | `datetime` | UTC-Zeitstempel des Updates. |
| `started_at` | `datetime \| None` | UTC-Startzeit, falls aufgezeichnet. |
| `eta_seconds` | `int \| None` | Lineare Extrapolation; `None` bis zum ersten nicht-null Progress. |

## `enum DeliveryMode`

```python
class DeliveryMode(enum.Enum):
    AT_LEAST_ONCE = "at_least_once"    # Default
    FIRE_AND_FORGET = "fire_and_forget"
```

Siehe [Zustellungssemantik](../explanation/delivery-semantics.md)
für den semantischen Unterschied und wann welcher greift.

## `class RabbitMQBroker`

```python
from swarmq.broker.rabbitmq import RabbitMQBroker

broker = RabbitMQBroker(url: str)
```

Async-AMQP-Broker-Wrapper um aio-pika. Alle Konfigurationen
(Queue-Typ, Prefetch, Reconnect-Policy) laufen durch `SwarmQConfig`
und `TaskInfo` — Anwendungscode ruft Methoden an dieser Klasse
nicht direkt auf.

## `class ValkeyBackend`

```python
from swarmq.backend.valkey import ValkeyBackend

backend = ValkeyBackend(url: str)
```

Async-Valkey-Client hinter Results, Locks, Rate-Limits,
Cancel-Flags, Scheduler-State und Leader-Election. Wird als
zweites Positional-Argument an `SwarmQ` gereicht.

## Exceptions

Alle Library-Exceptions erben von `SwarmQError`. Detaillierte
Semantik und `Wann geraist` / `Ursache` / `Recovery` pro Klasse
leben im [Fehlerkatalog](errors.md).

## Siehe auch

- [Konfiguration](configuration.md) — Env-Vars und `TaskInfo`-Felder
- [CLI-Referenz](cli.md) — das `swarmq`-Binary
- [Schnellstart](../getting-started.md) — der Roundtrip, den diese
  API unterstützt
