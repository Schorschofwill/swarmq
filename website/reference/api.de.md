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
geschrieben ist, gilt als verworfen. Das Warten läuft in einem eigenen
Thread und funktioniert daher auch, wenn der Default-Executor des Loops
schon heruntergefahren ist. Wird `__aexit__` während des Wartens
abgebrochen, propagiert der Abbruch sofort, und das Leeren läuft im
Hintergrund bis zur Frist weiter.

### `schedule()`

```python
async def schedule(
    task_name: str,
    *args: Any,
    queue: str | None = None,
    delay: float | None = None,
    eta: datetime | None = None,
    unique_for: float | None = _MISSING,
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
| `delay` | `float \| None` | Sekunden Verzögerung vor Zustellung. Gegenseitig ausschließend mit `eta`. Muss `≥ 0`, endlich und höchstens `315360000` sein (zehn Jahre, seit 0.13.1). |
| `eta` | `datetime \| None` | Absoluter Fire-Zeitpunkt. Gegenseitig ausschließend mit `delay`. Muss in der Zukunft liegen (1s Lenienz) und höchstens `315360000` Sekunden voraus (seit 0.13.1). Ein naives `datetime` gilt als Ortszeit. |
| `unique_for` | `float \| None` | Überschreibt `TaskInfo.unique_for`, in Sekunden. Muss `> 0`, endlich und höchstens `315360000` sein (sonst `ValueError`, vor jedem Claim, Publish oder Speichern; seit 0.12.3, die Obergrenze seit 0.13.0); ein `float` wird auf ganze Sekunden aufgerundet. `None` deaktiviert Dedup für diesen Aufruf. |
| `unique_until` | `"start" \| "completion"` | Überschreibt `TaskInfo.unique_until`. |

**Returns.** `str` — die `task_id` (UUIDv7). Bei Dedup-Hit wird
die *bestehende* Owner-`task_id` still zurückgegeben; ein
`TASK_DUPLICATE_DETECTED`-Signal feuert zur Observability.

**`unique_for=None` mit `delay`/`eta`.** Der Opt-out wird im
Oneshot-Eintrag gespeichert (`source="cli_unique"` ohne Dedup-Hash), und
der Leader feuert den Task ohne Dedup-Claim, auch wenn seine eigene
`TaskInfo` `unique_for` setzt. Dafür müssen Producer und Leader neuer
als 0.12.1 sein: Ein Producer bis 0.12.1 speichert den Opt-out nicht,
und ein Leader bis 0.12.1 fällt auf seine eigene `TaskInfo` zurück. In
beiden Fällen kann der Leader den Task beim Fire als Duplikat verwerfen,
solange ein anderer Lauf mit denselben Argumenten das Flag hält. Fehlt
`unique_for`, setzt ein Producer, dessen Registry den Task kennt, das
Flag beim Aufruf mit `TaskInfo.unique_for` plus Verzögerung; nur ein
Producer ohne Registry überlässt es dem Leader, der beim Fire seine
eigene `TaskInfo` anwendet
([Scheduling und Retries](../guides/scheduling-and-retries.de.md#immediate-vs-deferred)).

**Middleware und Signale bei `delay`/`eta`.** Seit 0.12.3 läuft ein
verzögerter Aufruf wie ein sofortiger durch die Enqueue-Middleware.
Zuerst laufen die Prüfungen von `delay`, `eta` und `unique_for`. Dann
berechnet der Client den Dedup-Hash aus den übergebenen Argumenten (vor
jeder Middleware), ruft `pre_enqueue`, setzt das `unique_for`-Flag,
speichert den Oneshot-Eintrag, ruft `post_enqueue` und löst
`TASK_ENQUEUED` aus. Ein Veto aus `pre_enqueue` bricht den Aufruf ab,
bevor etwas beansprucht oder gespeichert ist, auch bei einem Aufruf,
der ein Duplikat wäre (bisher kam dort die bestehende `task_id`
zurück). Ein Duplikat gibt die bestehende `task_id` nach `pre_enqueue`
zurück, ohne `post_enqueue`. In-place-Änderungen einer Middleware an
`kwargs` werden gespeichert, und der Leader publiziert sie beim Fire;
der Leader feuert ohne Enqueue-Middleware
([Middleware-Guide](../guides/middleware.md#verzogerte-tasks-delayeta)).

**Client-Only-Modus (ohne Registry).** Ohne Registry kennt der Client
die `TaskInfo` des Tasks nicht: `unique_for`, `priority` und der
Liefermodus des Worker-Tasks werden nicht angewendet. Dedup gibt es nur
mit ausdrücklichem `unique_for=`, die Nachricht geht an die einfache
`queue` (ohne `.high`/`.low`-Suffix) und wird `at_least_once` gesendet.
Der Client loggt einmal je Task-Name und Client-Instanz, für höchstens
1000 Namen, eine WARNING `client_task_defaults_unresolved` (mit
`task_name`). Damit die Defaults greifen, den Producer mit denselben
registrierten Tasks bauen, etwa über eine `SwarmQ`-App, die die
Task-Module importiert.

**Raises.**

- `TaskNotFoundError` — task_name nicht in der Registry (Registry-Modus).
- `ValueError` — `delay < 0`, `delay` und `eta` gleichzeitig, `eta` in Vergangenheit,
  `delay` `nan`, `inf` oder über `315360000`, `eta` mehr als `315360000`
  Sekunden voraus (seit 0.13.1, vor `pre_enqueue`, jedem Claim und
  jedem Schreiben), `unique_for` `<= 0`, `inf`, `nan` oder über `315360000` (zehn Jahre).
- `BrokerConnectionError` — Broker beim Publish unerreichbar.
- `BackendResponseError` — bei `delay`/`eta`: Der Scheduler-Store hat
  unter dem Namen des Oneshots einen anderen Eintrag, oder er hat das
  Schreiben abgelehnt. Bei einem Konflikt und wenn Valkey beim Speichern
  nicht erreichbar ist, bleibt die eigene `unique_for`-Sperre des
  Aufrufs bis zu ihrem TTL stehen, weil der Eintrag gespeichert sein und
  feuern kann; nach einer Fehlerantwort wird sie freigegeben.
- `asyncio.CancelledError` — der Aufruf wurde abgebrochen (ein Timeout
  um ihn herum, `task.cancel()`). Seit 0.13.1 gilt ein Abbruch während
  des Publish oder beim Speichern eines `delay`/`eta`-Eintrags wie ein
  Ausfall als unbestimmt: Die `unique_for`-Sperre bleibt bis zu ihrem
  Ablauf (`unique_for` plus Verzögerung) stehen, und der
  `CancelledError` geht unverändert weiter. Ein Abbruch in
  `pre_enqueue`, bevor etwas gesendet wurde, gibt sie frei.
- Propagiert aus `pre_enqueue`-Middleware-Hooks (auch bei
  `delay`/`eta`).

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
    unique_for: float | None = _MISSING,
    unique_until: Literal["start", "completion"] = _MISSING,
) -> list[str]
```

Enqueuet viele Tasks in einem Batch. Jedes Element ist ein
`(task_name, args, kwargs)`-Tupel. Nebenläufig via `asyncio.gather`
published.

**Returns.** `list[str]` — eine `task_id` pro Input, in
Input-Reihenfolge. Bei Dedup-Hits wird die Owner-task_id an diesem
Index platziert (Listenlänge ist immer gleich Input-Länge).

`unique_for` folgt denselben Regeln wie bei `schedule()`: `> 0`,
endlich und höchstens `315360000`, ein `float` wird auf ganze Sekunden aufgerundet.

**Raises.** Dieselbe Form wie `schedule()`. Bei partiellem
Publish-Fehlschlag werden erfolgreich publishte task_ids trotzdem
in die Result-Liste geschrieben, der Rest wird rollbacked, und
die erste Exception wird erneut geraist. Wie bei `schedule()` behält
ein Element, dessen Publish unbestimmt ist, seine `unique_for`-Sperre,
denn seine Nachricht kann angekommen sein; freigegeben werden nur die
Sperren der Elemente, bei denen nichts gesendet wurde. Ist ein Element
unbestimmt, wirft der Batch dessen `PublishIndeterminate`, auch wenn ein
bestimmter Fehler an kleinerem Index steht. Seit 0.13.1 gilt auch ein
Abbruch während des Publish als unbestimmt: Die Sperren bleiben, und der
`CancelledError` geht weiter.

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

Intern wird `[<namensraum>:]sq:notify:{task_id}` auf Valkey abonniert für ein
Push-basiertes Warten, mit 5ms-Polling als Fallback. Bei
`timeout=0` ein einzelnes `GET` ohne Pub/Sub. Ein negatives `timeout`
oder `nan` gilt als schon abgelaufen: ein `GET`, dann
`ResultNotReadyError`, wenn noch kein Ergebnis da ist (seit 0.12.3;
vorher konnte `nan` die Event-Loop zum Absturz bringen, wenn das `GET`
hing). `inf` wartet unbegrenzt.

**Raises.**

- `ResultNotReadyError` — kein Ergebnis innerhalb `timeout`.
- `TaskError` — Task schlug fehl; `str(exc)` trägt die Worker-
  Fehlermeldung (`""` bei einer Exception ohne Text). `exc.error_class`
  nennt die Exception-Klasse als `modul.qualname` (z. B.
  `builtins.ValueError`, im CPU-Modus die Klasse, die das Kind meldet);
  beratend und `None` bei Ergebnissen älterer Worker und bei Fehlern, die
  der Worker selbst erzeugt (harter Timeout, Drain, verlorenes Rechenkind,
  Ablauf).
- `TaskCancelledError` — der Task wurde mit `cancel()` abgebrochen, vor
  oder während der Ausführung (das gespeicherte Ergebnis trägt das
  Cancel-Flag). Der Task selbst sieht diese Exception nie; in `run()`
  kommt ein Cancel als `asyncio.CancelledError` an.

### `get_results()`

```python
async def get_results(
    task_ids: list[str],
    timeout: float = 10,
    poll_interval: float = 0.005,
) -> list[Any]
```

Batch-Poll-Variante von `get_result`. Nutzt `MGET`, um viele auf
einmal zu holen, alle `poll_interval` Sekunden, ohne Pub/Sub. Liest
mindestens einmal: `timeout=0` ist ein einzelnes `MGET`, und läuft die
Frist während des Wartens ab, folgt vor dem Aufgeben ein letztes `MGET`
(seit 0.12.3; vorher las `timeout=0` nichts und meldete jeden Task als
nicht fertig). Ein negatives `timeout` oder `nan` wirkt wie `0`; `inf`
wartet unbegrenzt. Raist `ResultNotReadyError` mit den fehlenden
task_ids, wenn einer nicht rechtzeitig fertig ist, sowie `TaskError`
oder `TaskCancelledError` wie `get_result()`.

### `cancel()`

```python
async def cancel(task_id: str) -> bool
```

Fordert den Abbruch eines Tasks an. Fire-and-forget: setzt ein
Cancel-Flag und kehrt sofort zurück. Das Flag lebt
`max(24 h, result_ttl)`. Bei einem verzögerten Task
(`schedule(delay=|eta=)`), der noch nicht gefeuert hat, kommt die
Wartezeit bis zum Fire dazu, und `max_age` + 300 s, wenn die Registry
des Clients den Task kennt. Ein stornierter Task, der länger als
`result_ttl` in der Queue wartet, läuft daher nicht mehr.

Hat der Worker die stornierte Zustellung abgeschlossen (Abbruch vor
dem Start oder während des Laufs), löscht er das Flag nicht, sondern
verkürzt es auf höchstens 1 h (`PEXPIRE … LT`). Ein Duplikat derselben
Nachricht bleibt storniert; ein Flag mit kürzerer TTL wird nie
verlängert.

**Returns.** `True`, wenn das Flag neu gesetzt wurde, `False`, wenn
es schon existiert: Ein vorheriger `cancel()`-Aufruf ist noch in
Flight, oder der Task wurde innerhalb der letzten Stunde storniert.

**Raises.** `ValueError` bei leerer `task_id`;
`BackendConnectionError`, wenn Valkey nicht erreichbar ist.

!!! warning "Stale Cancel-Flag"
    Wird dieselbe `task_id` neu eingereiht, solange das Flag lebt
    (deterministische IDs; mindestens 1 h nach einer abgeschlossenen
    stornierten Zustellung), wird auch der neue Task storniert, und
    `cancel()` liefert `False`. Mitigation: lass SwarmQ UUIDv7
    task_ids erzeugen (Default) oder übergib eine kürzere `ttl_ms`
    direkt über den darunterliegenden Client:
    `await app.client.cancel(task_id, ttl_ms=...)`. Der Worker
    verlängert ein kürzeres Flag nie.

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
keinen Zustand gibt (nie gestartet oder abgelaufen). Das Cancel-Flag,
das `cancel()` des Handles setzt, lebt `max(24 h, result_ttl)`, wie bei
einem Handle aus `apply()` (seit 0.13.0; vorher nahm ein neu gebautes
Handle 24 h und eines aus `apply()` `result_ttl`). Das tiefer liegende
`Client.workflow(workflow_id, *, cancel_ttl_ms=None)` nimmt diese
Lebensdauer in Millisekunden; `None` heißt 24 h.

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
Anwendungscode selten direkt. Wird der Start abgebrochen oder scheitert
er, nachdem der eingebettete Scheduler gestartet ist, werden dessen
Election- und Poll-Tasks gestoppt, bevor die Exception propagiert. Mit
`pool="cpu"` wirft es nach einem geordneten Herunterfahren
`swarmq.supervision.ForkSourceRejected`, wenn der Pool einen still neu
gestarteten Forkserver verworfen hat; siehe
[Fehlerkatalog](errors.md#auerhalb-der-hierarchie-forksourcerejected).

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
    result_ttl: float | None = None,
    max_retries: int = 0,
    retry_backoff: str = "exponential",
    retry_exceptions: list[type[BaseException]] | None = None,
    retry_base_delay: float = 1.0,
    rate_limit: str | None = None,
    lock: str | None = None,
    max_concurrency: int = 1,
    cron: str | None = None,
    unique_for: float | None = None,
    unique_until: Literal["start", "completion"] = "completion",
    max_age: int | None = None,
    priority: Literal["high", "normal", "low"] = "normal",
    timezone: str | None = None,
    dead_letter: bool | None = None,
)
```

`result_ttl` und `unique_for` nehmen einen `float` wie
`timedelta(hours=1).total_seconds()` an: Ein ganzzahliger Wert wird
`int`, ein Bruch wird auf die nächste Sekunde aufgerundet, `inf` und
`nan` werfen `ValueError` (seit 0.12.3). `result_ttl`, `unique_for`,
`max_age` und `hard_timeout` sind höchstens `315360000` Sekunden (zehn
Jahre, die Grenze von `SwarmQConfig`); ein größerer Wert wirft
`ValueError` (seit 0.13.0). `soft_timeout` und `hard_timeout` müssen
größer als 0 sein: `nan`, `-inf`, `0`, negative Werte und `False`
werfen `ValueError`, eine Nicht-Zahl wie eine `timedelta` wirft
`TypeError`, jeweils mit dem Feldnamen (seit 0.13.1). `soft_timeout=inf`
bleibt erlaubt. Feld-Semantik und
Validierungsregeln leben in
[Konfigurations-Referenz — TaskInfo-Felder](configuration.md#taskinfo-felder).

## `next_fire_times()`

```python
from swarmq.scheduler import next_fire_times

next_fire_times(
    cron: str,
    start: datetime,
    *,
    timezone: str | None = None,
    count: int = 1,
) -> list[datetime]
```

Die nächsten `count` Termine von `cron` nach `start`
(zeitzonenbehaftet), berechnet mit der Regel des Schedulers,
einschließlich der
[Regel für die Zeitumstellung](../guides/scheduling-and-retries.md#zeitumstellung).
`timezone` ist eine IANA-Zone, `None` heißt UTC; die Termine kommen
zeitzonenbehaftet in dieser Zone zurück. Ein naiver `start` wirft
`ValueError`, statt als Ortszeit des Hosts gelesen zu werden. Ein ungültiger Ausdruck wirft
croniters `CroniterBadCronError`. Damit lässt sich prüfen, dass ein auf
eine Zeitzone umgestellter Cron seine Termine behält
([Rezept](../guides/scheduling-and-retries.md#von-hand-umgerechnete-utc-crons-umstellen)).

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
- `post_execution()` — nach `run()` aufgerufen, nur wenn
  `pre_execution()` und `run()` erfolgreich waren (nicht bei Ausnahme,
  Timeout oder Cancel). Eine Ausnahme darin lässt den Versuch
  scheitern. Aufräumen für jeden Ausgang gehört in `try/finally` in
  `run()` oder in einen Dishka-`yield`-Finalizer. Default: No-Op.

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
  Middleware bricht das Enqueue ab, bei `delay`/`eta` bevor etwas
  gespeichert oder ein `unique_for`-Flag gesetzt ist.
- Alle anderen Hooks **schlucken** Exceptions: Jeder Fehler wird als
  `middleware_hook_failed` geloggt (ERROR, Logger `swarmq.middleware`,
  Felder `hook`, `middleware`, `error_type`, `error` und der
  Traceback), die Kette läuft weiter.
- Ein abgebrochener Task bekommt kein `on_error` und kein
  `post_execute`, nur vorher `pre_execute`: Ein Cancel ist kein Fehler.
  Middleware mit Zustand je Task behandelt zusätzlich das Signal
  `TASK_CANCELLED` (`task_id`, `task_name`, `queue`).

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
| `TASK_ENQUEUED` | `task_id`, `task_name`, `queue` | Nach einem Publish (`schedule`, `schedule_many`, jeder Workflow-Schritt) oder, bei `delay`/`eta`, sobald der Oneshot gespeichert ist (nicht beim Fire; Cron-Fires lösen es nicht aus). |
| `TASK_STARTED` | `task_id`, `task_name`, `queue` | Vor `run()`. |
| `TASK_COMPLETED` | `task_id`, `task_name`, `queue`, `duration_ms` | Nach erfolgreichem `run()`. |
| `TASK_FAILED` | `task_id`, `task_name`, `queue`, `exception` | Nach **jedem** gescheiterten Versuch, auch wenn ein Retry folgt. `exception` ist `str(exc)`. |
| `TASK_RETRYING` | `task_id`, `task_name`, `queue`, `retry_count`, `delay` | Ein Retry wurde eingeplant; `retry_count` ist der neue Zähler, `delay` in Sekunden. |
| `TASK_CANCELLED` | `task_id`, `task_name`, `queue` | Vor oder während der Ausführung abgebrochen. Es folgt kein `on_error`/`post_execute`. |
| `TASK_EXPIRED` | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` | Älter als `max_age` abgeholt; in die DLQ geleitet. |
| `TASK_LOCKED` | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` | Der Lock des Tasks war von einer anderen Ausführung belegt (Lock-Konflikt); die Nachricht wird neu eingereiht, am Ende der Wartezeit als `LockWaitExceeded` in die DLQ geleitet. |
| `TASK_RATE_LIMITED` | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` | Rate-Limit erreicht; Nachricht verzögert. |
| `TASK_DUPLICATE_DETECTED` | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] | Dedup-Treffer durch `unique_for`. |
| `TASK_DLQ` | `task_id`, `task_name`, `queue`, `error` | Endgültig: Der Task ging in die DLQ (Retries erschöpft oder nicht wiederholbar). Feuert einmal. |
| `TASK_REJECTED` | `queue`, `reason`, `task_id`, `task_name`, `error` — `task_id`/`task_name` können `None` sein | Der Worker wies eine Nachricht ab, die er nicht ausführen kann (`body_too_large`, `undecodable`, `middleware_chain_factory`: Kopie mit Frist und `x-swarmq-traceback` in `.XQ`, der Broker-DLX als Rückfall), ein Dead-Letter-Publish scheiterte und die Nachricht ging ohne Diagnose-Header an den Broker-DLX (`dlq_publish_failed`), oder ein unregistrierter Task wurde nach `SWARMQ_UNKNOWN_TASK_GRACE_S` dead-gelettert (`unregistered_task`, nach `TASK_DLQ`). |
| `TASK_UNKNOWN_DEFERRED` | `task_id`, `task_name`, `queue`, `delay`, `unknown_since` | Der Worker registriert den Task nicht und hat die Nachricht innerhalb von `SWARMQ_UNKNOWN_TASK_GRACE_S` zurückgestellt; `delay` in Sekunden, `unknown_since` der erste Kontakt in Unix-Millisekunden. Einmal je Rückstellung; gezählt in `swarmq_tasks_unknown_deferred_total`. |
| `WORKFLOW_STEP_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` | Nach jedem Publish eines Workflow-Schritts, zusätzlich zu `TASK_ENQUEUED`. |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | `worker_id` | Worker-Start / Graceful Shutdown. |
| `WORKER_HEARTBEAT` | `worker_id`, `uptime_seconds` | Alle `SWARMQ_METRICS_SAMPLE_INTERVAL` Sekunden; nur bei eingeschalteten Metriken. |
| `WORKER_RECYCLING` | `worker_id`, `kind`, `slot`, `reason`, `value` | Ein Worker (`kind="worker"`) oder Rechenkind (`kind="compute_child"`) wird recycelt; `reason` ist `rss`, `tasks` oder `forced`. |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | `worker_id` | Leader-Wahl und -Erneuerung. |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | `worker_id` | Leader gibt beim Shutdown ab / übernimmt nach abgelaufenem Lock. |
| `SCHEDULER_FIRE_SKIPPED` | `worker_id`, `schedule_name`, `skipped_count` | Nachholen nach Ausfallzeit hat Cron-Termine übersprungen. |
| `SCHEDULER_CRON_CONFLICT` | `worker_id`, `schedule_name`, `fingerprints` | Mehrere Definitionen eines statischen Crons leben seit mehr als 10 Minuten; der Leader sendet es in jedem Übernahme-Takt, solange der Konflikt besteht. |
| `SCHEDULER_LEADER_OUTDATED` | `worker_id`, `leader`, `leader_version`, `outdated` | Ein Nicht-Leader hat einen Scheduler-Leader gesehen, der ein swarmq unter der Mindestversion für Leader ausführt (derzeit 0.12; `leader_version` `None` heißt vor 0.10.0; eine unlesbare Version zählt nicht); einmal pro Leader mit `outdated=True`, erneut mit `outdated=False`, wenn ein aktueller Leader folgt. |
| `SCHEDULER_CRON_UNOWNED` | `worker_id`, `schedule_name`, `task_name`, `held`, `held_count` | Der Leader hält einen statischen Cron zurück, den kein lebender Worker meldet und den er selbst nicht registriert (`held=True`), oder gibt ihn frei (`held=False`); einmal je Übergang, `held_count` ist die Zahl der danach zurückgehaltenen Crons. Setzt `swarmq_scheduler_crons_held`. |
| `SCHEDULER_VALKEY_CHECKED` | `worker_id`, `status`, `policy`, `maxmemory`, `used_memory` | Der Leader hat Eviction-Policy und Speicher von Valkey erneut geprüft, höchstens einmal je Stunde; `status` ist `safe`, `unsafe` oder `unknown`, `maxmemory` `0` heißt ohne Grenze, `None` unbekannt. Setzt `swarmq_valkey_memory_used_ratio` (nur mit Grenze). |
| `SCHEDULER_QUEUE_ORPHANED` | `worker_id`, `queue`, `orphaned`, `message_count`, `orphaned_count` | Der Leader hat eine gemeldete Queue mit Nachrichten, ohne Consumer und ohne lebenden Worker, der sie meldet, gefunden (`orphaned=True`), oder die Queue ist nicht mehr verwaist (`orphaned=False`: abgearbeitet, konsumiert, gelöscht oder wieder gemeldet); einmal je Übergang, `orphaned_count` ist die Zahl der danach verwaisten Queues. Setzt `swarmq_scheduler_queues_orphaned`. |
| `SCHEDULER_FIRE_UNROUTABLE` | `worker_id`, `schedule_name`, `queue`, `kind` | Der Broker hat einen Publish des Leaders zurückgegeben (Queue fehlt, BZ-03): `kind="cron_lost"` für einen verlorenen Cron-Fire, `"oneshot_dropped"` für einen nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` verworfenen verzögerten Task. Gezählt in `swarmq_scheduler_fires_unroutable_total`. |

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
  wurde. Das Flag lebt `max(24 h, result_ttl)`, für Handles aus
  `app.apply()` und `app.workflow()` gleichermaßen (seit 0.13.0); ein
  Schritt, der länger als `result_ttl` in einer Queue wartet, wird also
  trotzdem gestoppt.
- `get(timeout=30.0)` — wartet auf einen Endzustand und gibt das
  Endergebnis zurück. Ein negatives `timeout` oder `nan` liest den
  Zustand einmal und wirft dann `ResultNotReadyError` (seit 0.12.3;
  vorher pollte es ohne Ende).

**Raises.** `WorkflowNotFound`, wenn der Zustand weg ist (`status()`,
`children()`, `get()`), `ResultNotReadyError`, wenn `get()` in den Timeout läuft,
`TaskError` für einen gescheiterten Workflow (mit `error_class` des
gescheiterten Schritts, wenn bekannt, siehe [`get_result()`](#get_result)),
`TaskCancelledError` für einen abgebrochenen, `WorkflowResultExpired`, wenn der Workflow fertig
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
| `updated_at` | `str` | ISO-8601-Zeitstempel des Updates, UTC. |
| `started_at` | `str \| None` | ISO-8601-Wanduhr-Start des Laufs, UTC (seit 0.12.3; vorher ein Wert der monotonen Uhr, der als Datum von 1970 erschien). |
| `eta_seconds` | `int \| None` | Lineare Extrapolation aus der monotonen Laufzeit; `None` bis zum ersten nicht-null Progress und beim letzten Update. |

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
DLQ-Lesebefehle von `BrokerAdmin` (`list_dead_letters`,
`inspect_dead_letter`, `retry_dead_letter`, `retry_all_dead_letters`)
liefern ein `DeadLetterScan` (`swarmq.contracts.broker`, seit 0.13.0):
ein Tupel `(ergebnis, scanned)`, das sich wie bisher entpacken lässt,
dazu `hit_limit`, das sagt, ob das Lesen am Limit endete (abgelaufene
Einträge zählen beim Limit mit, in `scanned` nicht). Eine eigene
`BrokerAdmin`-Implementierung darf weiter ein schlichtes Tupel liefern;
die CLI fällt dann auf `scanned >= limit` zurück. Code, der
`type(result) is tuple` prüft, muss angepasst werden. Die
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
    keys: KeySpace | None = None,
    eviction_check: str | None = None,
    namespace: str | None = None,
    connect_timeout_s: float | None = None,
)
```

Genau eines von `url`, `client` oder `pool` übergeben; keines oder
mehrere werfen `ConfigurationError`. Einen geliehenen `client` schließt
SwarmQ nie, ein `pool` behält seinen eigenen Lebenszyklus; beide sind
nur für den Producer-Pfad (`start_worker()` weist sie ab).

- `namespace` — der [Namensraum](configuration.md#namensraum-mehrere-apps-auf-einem-valkey)
  aller Keys und Kanäle; ohne Angabe gilt `SWARMQ_NAMESPACE`. Sind beide
  gesetzt und verschieden, wirft `SwarmQ` `ConfigurationError` (Exit
  `78`) ([Herkunft des Namensraums](configuration.md#herkunft-des-namensraums)).
- `keys` — ein fertiger `KeySpace` (`swarmq.keyspace`) statt
  `namespace`; beides zusammen ist ein `ConfigurationError`.
- `eviction_check` — `strict`, `warn` oder `off`, wie
  `SWARMQ_VALKEY_EVICTION_CHECK`, das ohne Angabe gilt.
- `connect_timeout_s` — Gesamtfrist für das `PING` in `connect()` samt
  Wiederholungen; geht vor `SwarmQConfig.backend_connect_timeout_s` und
  `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (Default 30 s). Muss `> 0` und
  höchstens `threading.TIMEOUT_MAX` sein, sonst `ConfigurationError` beim
  Konstruieren. Läuft sie ab, wirft `connect()`
  `BackendConnectionError`. Sie ist eine Obergrenze: Die eigenen
  Verbindungsversuche des Treibers geben nach höchstens rund 33 s auf,
  ein größerer Wert wartet nicht länger.

Asynchroner Valkey-Client für Ergebnisse, Locks, Rate-Limits,
Cancel-Flags, Scheduler-Zustand und Leader-Election. Implementiert das
`ResultBackend`-Protokoll aus `swarmq.contracts` und reicht die
Koordinations-Stores als `backend.locks`, `.rate_limit`, `.chords`,
`.schedules` und `.execution_guard` heraus (verfügbar nach `connect()`). Wird als zweites
Positionsargument durch `SwarmQ` gereicht. Seit 0.2.0 nennen die
Konstruktor-Signaturen von `Worker` und `Client` diese Verträge statt
der konkreten Klassen — bestehender Code läuft unverändert.

### Protokolle der Koordinations-Stores

`swarmq.contracts` definiert die Protokolle, die Worker und Scheduler auf
diesen Stores aufrufen. Du brauchst sie nur, wenn du einen eigenen Store
schreibst (für Tests oder ein anderes Backend). Auszug der Signaturen,
deren Vertrag sich in 0.9.0 geändert hat:

```python
from swarmq.contracts import RateLimitStore, ScheduleStore

class RateLimitStore(Protocol):
    async def check(self, name: str, *, max_requests: int,
                    window_ms: int) -> RateLimitDecision: ...

class ScheduleStore(Protocol):
    async def commit_fire(self, name: str, *, next_fire_ts_ms: int | None,
                          payload: bytes | None, owner: str,
                          fired_ts_ms: int | None = None) -> bool: ...
    async def claim_fire(self, fire_key: str, ttl_ms: int, *,
                         owner: str | None = None,
                         takeover: str | Sequence[str] | None = None) -> bool: ...
    async def release_fire_claim(self, fire_key: str, *,
                                 owner: str | None = None) -> None: ...
```

- `RateLimitStore.check` muss `BackendConnectionError` werfen, wenn das
  Backend nicht erreichbar ist (Verbindungsfehler, Timeout). Nur dann
  stellt der Worker die Nachricht mit Backoff zurück; jede andere
  Exception, auch `BackendResponseError`, führt zum Requeue an derselben
  Stelle.
- `ScheduleStore.commit_fire(fired_ts_ms=...)` greift nur, solange der
  Zeitplan noch genau diesen Score im Fire-Index hat; ein Commit, der
  verworfen wird, weil der Zeitplan seit dem Poll deaktiviert, entfernt
  oder umgeplant wurde, liefert trotzdem `True` (`False` heißt nur, dass
  die Leader-Prüfung scheiterte).
- `ScheduleStore.claim_fire(owner=..., takeover=...)` schreibt den
  In-Flight-Claim mit dem Owner-Token atomar. Es liefert `True`, wenn der
  Claim frei war oder eines der `takeover`-Tokens trug (ein Token oder
  eine Folge); der Claim gehört danach `owner` mit neuer TTL.
- `ScheduleStore.release_fire_claim(owner=...)` löscht den Claim nur,
  solange er noch dieses Token trägt (Compare-and-Delete).

Worker und Scheduler übergeben die neuen Schlüsselwörter immer; ein Store
von Dritten, der sie nicht annimmt, wirft daher bei jedem Fire
`TypeError`.

Vertragsänderungen in 0.10.0. Ein eigener Store muss diese Methoden
umsetzen; der Scheduler und `swarmq schedule status` rufen sie auf:

```python
from swarmq.contracts import CronReport, CronReports, LeaderInfo, ScheduleStore

class ScheduleStore(Protocol):
    async def put(self, name: str, payload: bytes,
                  fire_ts_ms: int | None) -> None: ...
    async def fire_ts(self, name: str) -> int | None: ...
    async def commit_fire(self, name: str, *, next_fire_ts_ms: int | None,
                          payload: bytes | None, owner: str,
                          fired_ts_ms: int | None = None,
                          fired_payload: bytes | None = None) -> bool: ...
    async def replan_if_unchanged(self, name: str, expected: bytes,
                                  payload: bytes, fire_ts_ms: int) -> bool: ...
    async def report_cron(self, name: str, fingerprint: str, payload: bytes,
                          *, live_window_ms: int) -> None: ...
    async def cron_reports(self) -> CronReports: ...
    async def prune_cron_reports(self, *, max_age_ms: int) -> int: ...
    async def report_worker_version(self, worker_id: str, version: str,
                                    *, ttl_ms: int) -> None: ...
    async def worker_versions(self) -> Mapping[str, str]: ...
    async def set_leader_version(self, owner: str, version: str,
                                 *, ttl_ms: int) -> None: ...
    async def leader_info(self) -> LeaderInfo: ...
```

- `put(..., fire_ts_ms=None)` schreibt die Definition und lässt den
  Fire-Index unberührt; ein fälliger Termin behält seinen Score.
- `fire_ts(name)` liefert den Score im Fire-Index, `None` ohne Eintrag
  (deaktiviert oder unbekannt).
- `commit_fire(fired_payload=...)` schreibt `payload` nur, solange die
  gespeicherte Definition noch genau `fired_payload` (die gepollte) ist;
  sonst rückt nur der Score vor, und eine zwischen Poll und Commit
  übernommene Definition wird nicht zurückgeschrieben.
- `replan_if_unchanged(name, expected, payload, fire_ts_ms)` plant einen
  Eintrag ohne Score in einem atomaren Schritt neu ein: Es schreibt
  `payload` und den Score nur, solange die gespeicherte Definition noch
  genau `expected` ist und der Eintrag keinen Score hat, und meldet, ob
  es geschrieben hat. Ein zwischendurch deaktivierter oder entfernter
  Schedule bleibt, wie er ist.
- `report_cron` setzt `last_seen` und Payload von `name|fingerprint`
  atomar in Backend-Zeit; `first_seen` nur, wenn die Meldung fehlt oder
  ihr `last_seen` älter als `live_window_ms` ist. `cron_reports()`
  liefert alle Meldungen samt Backend-Zeit des Lesens
  (`CronReports.now_ms`); `prune_cron_reports` löscht Meldungen, deren
  `last_seen` älter als `max_age_ms` ist, und liefert ihre Anzahl.
- `report_worker_version` / `worker_versions` halten die swarmq-Version
  pro Worker für `ttl_ms`; `set_leader_version` schreibt die Version
  des Leaders mit der TTL des Locks neben das Leader-Lock;
  `leader_info()` liefert den Owner des Locks und diese Version
  (`LeaderInfo.version` ist `None` bei einem Leader vor 0.10.0).

Vertragsänderungen in 0.11.0. Ein eigener `ScheduleStore` braucht drei
weitere Methoden für das [Aufräumen von Crons](../guides/scheduling-and-retries.md#crons-entfernen-die-keine-app-mehr-meldet);
ein Backend, das den [Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
anbietet, reicht einen `ExecutionGuardStore` als `backend.execution_guard`
heraus:

```python
from swarmq.contracts import ExecutionGuardStore, GuardClaim, ScheduleStore

class ScheduleStore(Protocol):
    async def prune_cron(self, name: str, expected: bytes, *,
                         stale_after_ms: int | None,
                         remember_disabled: bool) -> bool: ...
    async def pruned_disabled(self, name: str) -> bool: ...
    async def forget_pruned_disabled(self, name: str) -> None: ...

class ExecutionGuardStore(Protocol):
    async def claim(self, identity: str, *, owner: str, retry_count: int,
                    ttl_ms: int, state_ttl_ms: int,
                    max_orphans: int) -> GuardClaim: ...
    async def renew(self, identity: str, *, owner: str, ttl_ms: int) -> bool: ...
    async def mark_done(self, identity: str, *, owner: str,
                        state_ttl_ms: int) -> None: ...
    async def mark_superseded(self, identity: str, *, owner: str,
                              retry_count: int, state_ttl_ms: int) -> None: ...
    async def release(self, identity: str, *, owner: str) -> None: ...
```

- `prune_cron` entfernt einen statischen Cron samt Termin atomar,
  solange die gespeicherte Definition noch genau `expected` ist und, mit
  `stale_after_ms`, jede Meldung des Namens älter als diese Frist ist
  (Backend-Zeit, mindestens eine Meldung); ohne Frist, solange keine
  Meldung des Namens existiert. Mit `remember_disabled` merkt sich der
  Store den Namen, sodass `pruned_disabled` bis
  `forget_pruned_disabled` `True` liefert. Liefert, ob entfernt wurde.
- `claim` liefert einen `GuardClaim(status, orphans)` mit `status` aus
  `claimed`, `done`, `superseded`, `running`, `orphan_limit`. Ein ohne
  Freigabe abgelaufener Running-Marker zählt als Waise; bei
  `max_orphans` gehört der Claim trotzdem dem Aufrufer, damit das
  folgende Dead-Lettering ihn abschließt. `renew` ist ein
  Compare-and-PEXPIRE auf den eigenen Marker; `mark_done`,
  `mark_superseded` (nur steigende Zähler) und `release` löschen den
  Running-Marker nur, solange er `owner` gehört.
- Ein Backend ohne Attribut `execution_guard` läuft ohne den Schutz.

Vertragshinweis für 0.12.3 (Signaturen unverändert).
`ScheduleStore.add` sollte die Definition und ihren Eintrag im
Fire-Index in einem atomaren Schritt schreiben, wie es der Valkey-Store
von swarmq tut (`sched_add.lua`), damit ein nach verlorener Antwort
wiederholtes Schreiben nie einen Eintrag ohne Fire-Zeitpunkt
hinterlässt. Liefert `add` für einen Oneshot aus
`schedule(delay=/eta=)` (Name `oneshot:<task_id>`) `False`, liest
swarmq den Eintrag mit `get` zurück; ein byte-gleicher Eintrag gilt als
eigener Insert, und ein fehlender Fire-Zeitpunkt wird mit
`replan_if_unchanged` ergänzt. Jeder andere bestehende Eintrag ist ein
Konflikt, und `swarmq schedule add` meldet für einen bestehenden Namen
weiter „already exists“.

Vertragsänderung in 0.13.0. `ExecutionGuardStore` hat eine neue
Pflichtmethode:

```python
async def peek(self, identity: str, *, retry_count: int) -> GuardSettled | None: ...
```

Sie liest nur, ohne Claim und ohne Marker (F4-02): `"done"` nach
`mark_done`, `"superseded"`, wenn der Superseded-Zähler größer als
`retry_count` ist, sonst `None` (auch bei abgelaufenem Zustand).
`GuardSettled` ist `Literal["done", "superseded"]` aus
`swarmq.contracts.coordination`. Der Worker ruft sie am Zustelllimit
auf; eine eigene Implementierung ohne `peek` loggt dort ERROR
`delivery_handler_failed` (`AttributeError`) und lässt die Nachricht
unbestätigt liegen, statt sie in die DLQ zu leiten.

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
  injiziert, den der Worker je Task öffnet. Das gilt ebenso für
  `Annotated[X, FromComponent("c")]` (eine benannte Komponente) und für
  Aliase wie `Depends = FromDishka`, mit und ohne
  `from __future__ import annotations`.
- Jede Annotation einer solchen Methode muss bei der Klassendefinition
  auflösbar sein, auch die der nicht injizierten Parameter: Typen vor der
  Klasse definieren oder importieren, nicht nur unter `TYPE_CHECKING` und
  nicht als Vorwärtsverweis auf eine spätere Klasse. Sonst wird die
  Methode nicht injiziert, und SwarmQ meldet das:
    - Mit `from __future__ import annotations` (PEP 563) scheitert der
      Worker-Start mit `ConfigurationError` (Exit 78). Die Meldung nennt
      den Task, den injizierten Parameter und, falls einer weiter
      unauflösbar ist, diesen Parameter.
    - Ohne (PEP 649, Python 3.14) wirft die Klassendefinition `NameError`
      mit Hinweis.
    - Eine Annotation, die `FromDishka` im Text nennt, aber nicht
      auflösbar ist, wirft in beiden Fällen bei der Klassendefinition
      `NameError` mit Hinweis.
    - Eine unauflösbare Annotation ohne erkennbaren Marker (etwa ein Typ,
      der nur unter `TYPE_CHECKING` importiert wird, oder unter PEP 649
      `Depends[Later] | None` mit einer späteren Klasse) wird nicht
      injiziert und nicht gemeldet. Die Klassendefinition eines Tasks ohne
      DI scheitert nie an einer unauflösbaren Annotation, ebenso wenig der
      Bau der App (`SwarmQ(...)`, auch beim Start von `swarmq worker`), der
      die Annotationen von `run()` tolerant liest.
- `FromDishka[X] | None` und `Depends[X] | None` ergeben keinen
  Dependency-Key; der Worker-Start lehnt sie mit `ConfigurationError` ab.
- Die DI-Prüfung läuft beim Start von `swarmq worker`, im eingebetteten
  Worker (`SwarmQ.start_worker()`, vor jeder Verbindung) und bei
  `swarmq worker --check`. `--check` importiert dafür `--module` (dessen
  Ausgaben gehen nach stderr) und prüft die Tasks, die der Import
  registriert.
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

HealthChecker(broker, backend, *, timeout_s: float = 2.0, checks=None)
HealthServer(checker, *, host="127.0.0.1", port=8080, liveness_started=None)
serve_health(checker, *, host="127.0.0.1", port=8080)       # async Context-Manager

Metrics(registry=None, namespace="swarmq")
MetricsServer(metrics, host="127.0.0.1", port=9090)
serve_metrics(metrics, host="127.0.0.1", port=9090)         # async Context-Manager
install_signal_collectors(dispatcher: SignalDispatcher, metrics: Metrics) -> None
```

- `HealthChecker.check_all()` pingt Broker und Backend (je begrenzt
  durch `timeout_s`) und liefert `{"ready", "broker", "backend"}` plus
  einen Eintrag je benannter Prüfung. `checks` bildet einen Namen auf
  eine async Funktion ab, die `(ok, error)` liefert; jede läuft begrenzt
  durch `timeout_s`, und eine fehlschlagende macht `ready` falsch. Der
  Worker übergibt `"consumers"` (jede konsumierte Queue hat einen
  aktiven Consumer, siehe
  [Consumer-Timeout](../explanation/delivery-semantics.md#consumer-timeout))
  und im CPU-Modus `"compute"`. Bis 0.10.x war `compute_check=` die
  einzige Zusatzprüfung; stattdessen `checks={"compute": ...}`
  übergeben.
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
...`). Die beiden Worker-internen Klassen `WorkflowStateConflict` und
`SettleFailedAfterPublish` stehen nicht im Top-Level-Paket; importiere
sie aus `swarmq.exceptions`. `BrokerPublishBlocked` und
`PublishRejected` lassen sich seit 0.12.3 aus `swarmq` importieren;
Code, der auch auf 0.12.0–0.12.2 laufen muss, importiert sie aus
`swarmq.exceptions`.
Detaillierte Semantik und `Wann geraist` / `Ursache` / `Recovery` pro
Klasse leben im [Fehlerkatalog](errors.md). `ForkSourceRejected`, das
ein eingebetteter CPU-Worker wirft, ist ein `RuntimeError` in
`swarmq.supervision`.

`Retry(delay=...)` klemmt eine negative Wartezeit auf `0` (sofort
wiederholen, zählt gegen `max_retries`) und wirft für `inf` und `nan`
beim Erzeugen einen `ValueError` (seit 0.12.3).

## Siehe auch

- [Konfiguration](configuration.md) — Env-Vars und `TaskInfo`-Felder
- [CLI-Referenz](cli.md) — das `swarmq`-Binary
- [Schnellstart](../getting-started.md) — der Roundtrip, den diese
  API unterstützt
