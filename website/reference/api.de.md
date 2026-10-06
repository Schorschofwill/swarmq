# API-Referenz

Die öffentliche API ist das, was `from swarmq import ...` dir
liefert. Diese Seite dokumentiert jede öffentliche Klasse und die
Methoden, die Anwendungscode aufrufen soll. Die Library ist async
geschrieben; jede Methode mit I/O ist eine Coroutine.

## Modul-Layout

```python
from swarmq import (
    SwarmQ,                    # Anwendungs-Entrypoint
    RabbitMQBroker,            # Broker (dieselbe Klasse wie swarmq.broker.rabbitmq)
    ValkeyBackend,             # Backend (dieselbe Klasse wie swarmq.backend.valkey)
    Task, TaskInfo,            # Task-Definition
    DeliveryMode,              # at_least_once | fire_and_forget
    Middleware,                # Middleware-Basisklasse
    MessageContext,            # TypedDict, das an Middleware-Hooks geht
    Signal,                    # Lifecycle-Event-Enum
    SignalDispatcher,          # Signal-Registry
    ProgressInfo,              # Task-Progress-Snapshot
    chain, group, chord,       # Workflow-Primitive
    sig, Signature,            # Gebundener Task-Aufruf für Workflows
    WorkflowHandle,            # Handle aus app.apply() / app.workflow()
    setup_dishka, SwarmQProvider, TaskContext,   # Optionale Dishka-DI
    HealthChecker, HealthServer, serve_health,   # /health + /ready
    Metrics, MetricsServer, serve_metrics,       # Prometheus /metrics
    install_signal_collectors,
    Retry, NoRetry,            # Task-seitiger Control Flow
    SwarmQError,               # Basis-Exception
    # ...und der Rest der Exception-Hierarchie
)
```

Alles oben steht in `swarmq.__all__`. Die schweren Namen (`SwarmQ`,
`RabbitMQBroker`, `ValkeyBackend`, `Task`, `Signal`, die Health- und
Metrics-Klassen, die Dishka-Namen, ...) werden erst beim ersten Zugriff
geladen, `import swarmq` bleibt also billig; es sind dieselben Objekte
wie in ihren Heimatmodulen (`from swarmq.broker.rabbitmq import
RabbitMQBroker` funktioniert weiter).

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

Mit `log_queue=True` hängt die letzte Instanz beim Exit die
[Opt-in-Log-Queue](logging.md#opt-in-log-queue) sofort ab und wartet
dann, ohne den Event-Loop anzuhalten, auf deren restliche Einträge,
höchstens `log_drain_timeout` Sekunden (Vorgabe 5). Was bis dahin nicht
geschrieben ist, gilt als verworfen.

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
    oder übergib eine kürzere `ttl_ms` direkt über den
    darunterliegenden Client: `await app.client.cancel(task_id, ttl_ms=...)`.

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

### `apply()`

```python
async def apply(
    workflow: Signature | Chain | Group | Chord,
) -> WorkflowHandle
```

Startet einen mit `chain`, `group`, `chord` oder als einzelne
`Signature` gebauten Workflow und gibt ein
[`WorkflowHandle`](#class-workflowhandle) zurück. Fire-and-forget:
Topologie und Anfangszustand landen in Valkey, die ersten Schritte
werden als `at_least_once`-Nachrichten eingereiht, den Rest treiben die
Worker voran. Der Aufruf kehrt sofort zurück; der Workflow hängt nicht
vom aufrufenden Prozess ab. Der Workflow-Zustand lebt `result_ttl`
Sekunden.

`pre_enqueue`-Middleware läuft für jeden Anfangsschritt, bevor
Zustand geschrieben wird; eine ablehnende Middleware hinterlässt also
nichts. Jeder Schritt löst `TASK_ENQUEUED` und
`WORKFLOW_STEP_ENQUEUED` aus.

**Raises.** `WorkflowValidationError` (von den Primitiven, beim
Bauen), `BrokerConnectionError`, wenn die Anfangsschritte nicht
publiziert werden können (der Workflow wird dann als gescheitert
markiert), alles, was ein `pre_enqueue`-Hook wirft.

### `workflow()`

```python
async def workflow(workflow_id: str) -> WorkflowHandle
```

Hängt sich per ID an einen laufenden oder fertigen Workflow, etwa aus
einem anderen Prozess. **Raises** `WorkflowNotFound`, wenn es zur ID
keinen Zustand gibt (nie gestartet oder abgelaufen).

**Beispiel.**

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
from swarmq.logging import get_logger

log = get_logger("myapp.signals")


@app.on_signal(Signal.TASK_FAILED)
async def on_failure(task_id, task_name, exception, **kwargs):
    # ``exception`` ist der Meldungstext (str(exc)), nicht das
    # Exception-Objekt. Die echte Exception bekommt Middleware.on_error.
    log.warning("myapp_task_failed", task_id=task_id, task_name=task_name, reason=exception)
```

Akzeptiere in jedem Handler `**kwargs`: Payloads bekommen mit der Zeit
neue Felder (`queue` kam bei den Task-Signalen hinzu), und eine feste
Signatur wirft dann `TypeError`. Ein werfender Handler wird als
`signal_handler_error` geloggt (WARNING, Logger `swarmq.signals`, mit
`error_type` und `error`); ein Worker-Lebenszyklus-Handler, der sein
Zeitbudget überschreitet, als `signal_handler_timeout`.

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

### Workflow-Signaturen

```python
@classmethod
def s(cls, *args, **kwargs) -> Signature
@classmethod
def si(cls, *args, **kwargs) -> Signature
```

Bauen eine [`Signature`](#workflow-primitive) dieses Tasks für
`chain`, `group` und `chord`. `s()` ist veränderlich: In einer Chain
bekommt der Schritt das Ergebnis des Vorgängers als erstes
Positionsargument. `si()` ist unveränderlich: Die gebundenen Argumente
gelten so, wie sie sind. Beide weisen einen `FIRE_AND_FORGET`-Task
beim Bauen des Workflows ab.

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
- Alle anderen Hooks **schlucken** Exceptions: Jeder Fehler wird als
  `middleware_hook_failed` geloggt (ERROR, Logger `swarmq.middleware`,
  Felder `hook`, `middleware`, `error_type`, `error` und der
  Traceback), die Kette läuft weiter.

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

Workflow-Schritte durchlaufen ebenfalls `pre_enqueue` und
`post_enqueue` (aus `app.apply()` und aus dem Advancer im Worker).
Ihr Kontext hat immer `delivery_mode="at_least_once"` und zwei
zusätzliche Schlüssel, `workflow_id` und `step_path`, die gewöhnliche
`schedule()`-Aufrufe nicht tragen — mit `.get()` lesen.

## `enum Signal`

Lifecycle-Events, die SwarmQ emittiert. Registriere Handler mit
`@app.on_signal(Signal.FOO)`.

| Signal | Payload (kwargs) | Feuert |
|---|---|---|
| `TASK_ENQUEUED` | `task_id`, `task_name`, `queue` | Nach einem Publish (`schedule`, `schedule_many`, jeder Workflow-Schritt). |
| `TASK_STARTED` | `task_id`, `task_name`, `queue` | Vor `run()`. |
| `TASK_COMPLETED` | `task_id`, `task_name`, `queue`, `duration_ms` | Nach erfolgreichem `run()`. |
| `TASK_FAILED` | `task_id`, `task_name`, `queue`, `exception` | Nach **jedem** gescheiterten Versuch, auch wenn ein Retry folgt. `exception` ist `str(exc)`. |
| `TASK_RETRYING` | `task_id`, `task_name`, `queue`, `retry_count`, `delay` | Ein Retry wurde eingeplant; `retry_count` ist der neue Zähler, `delay` in Sekunden. |
| `TASK_CANCELLED` | `task_id`, `task_name`, `queue` | Vor oder während der Ausführung abgebrochen. |
| `TASK_EXPIRED` | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` | Älter als `max_age` abgeholt; in die DLQ geleitet. |
| `TASK_LOCKED` | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` | Der Lock des Tasks war von einer anderen Ausführung belegt (Lock-Konflikt); die Nachricht wird neu eingereiht, an der Konfliktgrenze in die DLQ geleitet. |
| `TASK_RATE_LIMITED` | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` | Rate-Limit erreicht; Nachricht verzögert. |
| `TASK_DUPLICATE_DETECTED` | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] | Dedup-Treffer durch `unique_for`. |
| `TASK_DLQ` | `task_id`, `task_name`, `queue`, `error` | Endgültig: Der Task ging in die DLQ (Retries erschöpft oder nicht wiederholbar). Feuert einmal. |
| `TASK_REJECTED` | `queue`, `reason`, `task_id`, `task_name`, `error` — `task_id`/`task_name` können `None` sein | Eine Nachricht ging ohne Diagnose-Header an den Broker-DLX (`body_too_large`, `undecodable`, `unregistered_task`, `middleware_chain_factory`, `dlq_publish_failed`). |
| `WORKFLOW_STEP_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` | Nach jedem Publish eines Workflow-Schritts, zusätzlich zu `TASK_ENQUEUED`. |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | `worker_id` | Worker-Start / Graceful Shutdown. |
| `WORKER_HEARTBEAT` | `worker_id`, `uptime_seconds` | Alle `SWARMQ_METRICS_SAMPLE_INTERVAL` Sekunden; nur bei eingeschalteten Metriken. |
| `WORKER_RECYCLING` | `worker_id`, `kind`, `slot`, `reason`, `value` | Ein Worker (`kind="worker"`) oder Rechenkind (`kind="compute_child"`) wird recycelt; `reason` ist `rss`, `tasks` oder `forced`. |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | `worker_id` | Leader-Wahl und -Erneuerung. |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | `worker_id` | Leader gibt beim Shutdown ab / übernimmt nach abgelaufenem Lock. |
| `SCHEDULER_FIRE_SKIPPED` | `worker_id`, `schedule_name`, `skipped_count` | Nachholen nach Ausfallzeit hat Cron-Termine übersprungen. |

Handler müssen `**kwargs` akzeptieren. Nutze `kwargs.get("batch_index")` für das nur in
`TASK_DUPLICATE_DETECTED` vorhandene Feld, da Nicht-Batch-Quellen
es nicht enthalten. `worker_id` ist `<basis>:<slot>` unter dem
Supervisor und `<basis>:p<pid>` eingebettet; siehe
[Worker-Identität](configuration.de.md#worker-identitat).

## Workflow-Primitive

```python
from swarmq import Signature, chain, chord, group, sig

sig(task_name: str, *args, immutable: bool = False, **kwargs) -> Signature
chain(*steps) -> Chain              # auch: a | b | c
group(*members) -> Group            # oder group(iterable)
chord(header: Group, body) -> Chord # auch: group(...) | body
```

| Primitiv | Ergebnis |
|---|---|
| `Signature` | Ein gebundener Task-Aufruf. Gebaut mit `Task.s()` / `Task.si()` (Task-Klasse bekannt) oder `sig("task_name", ...)` (per Name, ohne Import). `immutable=True` / `si()` unterdrückt die Ergebnis-Injektion. |
| `chain(a, b, c)` | Führt die Schritte nacheinander aus; jeder veränderliche Schritt bekommt das vorige Ergebnis als erstes Argument. Ergebnis des Workflows ist das des letzten Schritts. |
| `group(a, b, c)` | Führt die Mitglieder parallel aus. Ergebnis ist die Liste der Mitglieds-Ergebnisse in Definitions-, nicht in Fertigstellungsreihenfolge. |
| `chord(group(...), body)` | Führt die Header-Gruppe aus, dann `body` genau einmal mit der geordneten Liste der Header-Ergebnisse. |

Schritte können selbst Chains, Groups oder Chords sein. Grenzen, beim
Bauen geprüft, mit `WorkflowValidationError` bei Verstoß:

- Verschachtelungstiefe höchstens `10` (`max_workflow_depth`);
- höchstens `1000` Mitglieder je Group (`max_group_size`);
- keine leere Chain oder Group; der Header eines Chords muss ein
  `group(...)` sein;
- kein `FIRE_AND_FORGET`-Task als Schritt (`Task.s()`/`si()` prüfen
  das beim Bauen, `sig()` per Name zur Laufzeit).

Gestartet wird ein Workflow mit [`app.apply()`](#apply).

## `class WorkflowHandle`

Kommt aus `app.apply()` und `app.workflow(workflow_id)`. Hält nur die
`workflow_id`; jeder Aufruf liest den Zustand aus Valkey, ein Handle
lässt sich also in jedem Prozess neu bauen.

```python
class WorkflowHandle:
    workflow_id: str

    async def status(self) -> str: ...
    async def children(self) -> dict[str, Any]: ...
    async def cancel(self) -> bool: ...
    async def get(self, timeout: float = 30.0) -> Any: ...
```

- `status()` — `"running"`, `"completed"`, `"failed"` oder
  `"cancelled"`.
- `children()` — Abbildung jedes eingereihten Schritts (Topologie-Pfad)
  auf `{"task_id", "status", "result"?}` mit `status` `running`,
  `completed` oder `failed`. Leer, bevor ein Schritt eingereiht ist.
- `cancel()` — setzt das Cancel-Flag des Workflows. `get()` wirft
  sofort; noch nicht abgeholte Schritte enden als abgebrochen; ein
  bereits laufender Schritt läuft zu Ende, danach werden keine weiteren
  Schritte eingereiht. Gibt `True` zurück, wenn das Flag neu gesetzt
  wurde.
- `get(timeout=30.0)` — wartet auf einen Endzustand und gibt das
  Endergebnis zurück.

**Raises.** `WorkflowNotFound`, wenn der Zustand weg ist (`status()`,
`children()`, `get()`), `ResultNotReadyError`, wenn `get()` in den Timeout läuft,
`TaskError` für einen gescheiterten Workflow, `TaskCancelledError` für
einen abgebrochenen, `WorkflowResultExpired`, wenn der Workflow fertig
ist, sein Endergebnis aber vor dem Abholen abgelaufen ist.

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
from swarmq import RabbitMQBroker

RabbitMQBroker(
    url: str | None = None,
    queue_type: str = "quorum",
    *,
    connection: aio_pika.abc.AbstractConnection | None = None,
    connection_pool: aio_pika.pool.Pool | None = None,
)
```

Genau eines von `url`, `connection` oder `connection_pool` übergeben;
keines oder mehrere werfen `ConfigurationError`. Mit `url` gehört die
Verbindung SwarmQ. Eine geliehene `connection` oder ein Pool ist nur für
den Producer-Pfad: SwarmQ öffnet einen eigenen Channel und schließt die
geliehene Verbindung nie, und `start_worker()` weist einen solchen
Broker ab. Innerhalb von `SwarmQ(...)` kommt der Queue-Typ aus
`SwarmQConfig.queue_type` (`SWARMQ_QUEUE_TYPE`).

Asynchroner AMQP-Broker über aio-pika. Implementiert die Protokolle
`Broker` und `BrokerAdmin` aus `swarmq.contracts` (connect/declare/
publish/consume, die vier Zustellungs-Verben `ack`/`redeliver`/
`retry_later`/`dead_letter`, Queue-Tiefen und DLQ-Operationen). Die
gesamte Konfiguration (Queue-Typ, Prefetch, Reconnect) läuft über
`SwarmQConfig` und `TaskInfo` — aus Anwendungscode rufst du diese
Klasse nicht direkt.

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

Genau eines von `url`, `client` oder `pool` übergeben; keines oder
mehrere werfen `ConfigurationError`. Einen geliehenen `client` schließt
SwarmQ nie, ein `pool` behält seinen eigenen Lebenszyklus; beide sind
nur für den Producer-Pfad (`start_worker()` weist sie ab).

Asynchroner Valkey-Client für Ergebnisse, Locks, Rate-Limits,
Cancel-Flags, Scheduler-Zustand und Leader-Election. Implementiert das
`ResultBackend`-Protokoll aus `swarmq.contracts` und reicht die
Koordinations-Stores als `backend.locks`, `.rate_limit`, `.chords` und
`.schedules` heraus (verfügbar nach `connect()`). Wird als zweites
Positionsargument durch `SwarmQ` gereicht. Seit 0.2.0 nennen die
Konstruktor-Signaturen von `Worker` und `Client` diese Verträge statt
der konkreten Klassen — bestehender Code läuft unverändert.

## Dependency Injection (Dishka)

Optional; Installation mit `pip install 'swarmq[dishka]'`.

```python
from swarmq import SwarmQProvider, TaskContext, setup_dishka

setup_dishka(container: dishka.AsyncContainer) -> None
```

- `setup_dishka(container)` registriert deinen `AsyncContainer`. Auf
  Modulebene im Task-Modul aufrufen, damit jeder gespawnte
  Worker-Prozess seinen Container beim Import baut. Ein zweiter Aufruf
  ersetzt den ersten und loggt `dishka_container_replaced`.
- Mit `FromDishka[...]` (aus `dishka`) annotierte Parameter von `run`,
  `pre_execution` und `post_execution` werden aus einem REQUEST-Scope
  injiziert, den der Worker je Task öffnet.
- `TaskContext` ist eine eingefrorene Dataclass mit `task_id`,
  `task_name`, `queue` und `retry_count` des laufenden Tasks.
- `SwarmQProvider` ist ein Dishka-Provider, der `TaskContext` im
  REQUEST-Scope bereitstellt; nimm ihn in deinen Container auf, wenn ein
  Provider von `TaskContext` abhängt. Ohne installiertes `dishka` wirft
  der Zugriff `ImportError`.

```python
from dishka import FromDishka, make_async_container

from swarmq import SwarmQProvider, Task, TaskContext, TaskInfo, setup_dishka

# AppProvider: dein eigener Dishka-Provider, der PaymentClient liefert
container = make_async_container(SwarmQProvider(), AppProvider())
setup_dishka(container)


class Charge(Task):
    task_info = TaskInfo(name="charge", queues=["billing"])

    async def run(self, order_id: int, payments: FromDishka[PaymentClient],
                  ctx: FromDishka[TaskContext]) -> None:
        await payments.charge(order_id, idempotency_key=ctx.task_id)
```

## Health und Metriken

`swarmq worker --health-port` / `--metrics-port` starten diese Server
für dich. Die Klassen direkt brauchst du nur in einem eingebetteten
Worker oder einem eigenen Prozess.

```python
from swarmq import (
    HealthChecker, HealthServer, serve_health,
    Metrics, MetricsServer, serve_metrics, install_signal_collectors,
)

HealthChecker(broker, backend, *, timeout_s: float = 2.0, compute_check=None)
HealthServer(checker, *, host="127.0.0.1", port=8080, liveness_started=None)
serve_health(checker, *, host="127.0.0.1", port=8080)       # async Context-Manager

Metrics(registry=None, namespace="swarmq")
MetricsServer(metrics, host="127.0.0.1", port=9090)
serve_metrics(metrics, host="127.0.0.1", port=9090)         # async Context-Manager
install_signal_collectors(dispatcher: SignalDispatcher, metrics: Metrics) -> None
```

- `HealthChecker.check_all()` pingt Broker und Backend (je begrenzt
  durch `timeout_s`) und liefert `{"ready", "broker", "backend"}` (im
  CPU-Modus zusätzlich `"compute"`).
- `HealthServer` bedient `/health` (immer `200` mit `uptime_s`) und
  `/ready` (`200` oder `503` mit dem `check_all()`-Snapshot).
- `Metrics` hält den [Metrik-Katalog](configuration.md#metrik-katalog)
  in einer eigenen `CollectorRegistry`; ohne `prometheus_client`
  (`pip install 'swarmq[metrics]'`) ist jede Metrik ein No-op.
- `MetricsServer` bedient `/metrics` aus einer `Metrics`-Instanz.
- `install_signal_collectors()` registriert die Signal-Handler, die
  Task-Zähler, Gauges und das Dauer-Histogramm aktualisieren.

Beide Server haben keine Authentifizierung und binden per Default an
`127.0.0.1`.

## Exceptions

Alle Library-Exceptions erben von `SwarmQError` und lassen sich aus
`swarmq` importieren (`from swarmq import TaskError, WorkflowNotFound,
...`). `WorkflowStateConflict` ist die eine Exception, die nicht im
Top-Level-Paket steht; importiere sie aus `swarmq.exceptions`.
Detaillierte Semantik und `Wann geraist` / `Ursache` / `Recovery` pro
Klasse leben im [Fehlerkatalog](errors.md).

## Siehe auch

- [Konfiguration](configuration.md) — Env-Vars und `TaskInfo`-Felder
- [CLI-Referenz](cli.md) — das `swarmq`-Binary
- [Schnellstart](../getting-started.md) — der Roundtrip, den diese
  API unterstützt
