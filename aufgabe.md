# SwarmQ — Async Task Manager für Python

## 1. Projektziel

Ein high-performance, vollständig asynchroner Task Manager für Python 3.14+, vergleichbar mit Celery, TaskIQ, arq, AioTasks und streaQ. **Ausfallsicherheit hat höchste Priorität**, gefolgt von Performance.

---

## 2. Architektur-Übersicht

### 2.1 Kernprinzipien
- Vollständig async (nahtlose Integration mit async DB-/Business-Logik-Libraries)
- Minimale externe Abhängigkeiten
- Type-safe (mypy-kompatibel)
- Sprachübergreifendes Message-Format (JSON-basiert)
- Austauschbare Broker und Backends (Abstraktion wie bei TaskIQ)
- **Startup-Caching:** Alles was nur einmal evaluiert werden muss, wird beim Worker-Start berechnet und gecacht. Ein längerer Worker-Start ist akzeptabel wenn dadurch die Task-Ausführung schneller wird.
- **Kein Free-Threaded Python (no-GIL):** SwarmQ nutzt reguläres Python 3.14 mit GIL. Die multiprocessing-Architektur (ein Event-Loop pro Prozess) umgeht den GIL bereits vollständig — free-threaded Mode bringt keinen messbaren Vorteil, verursacht aber 5-10% Single-Thread-Overhead und birgt Kompatibilitätsrisiken bei C-Extensions (msgspec, msgpack, uvloop).

#### Caching-Strategie

Beim Worker-Start werden folgende Daten einmalig evaluiert und im Speicher gecacht:

| Was | Wann gecacht | Zweck |
|---|---|---|
| Task-Klassen (Name → Klasse) | Auto-Discovery beim Start | Kein dynamischer Lookup pro Task |
| Return-Type Annotations | Beim Registrieren | Deserialisierung ohne Reflection zur Laufzeit |
| TaskInfo (Timeouts, Delivery-Mode, TTL) | Beim Registrieren | Kein Attribut-Zugriff pro Task-Ausführung |
| Serialisierte TaskInfo-Defaults | Beim Registrieren | Vorgefertigte Defaults für `schedule()`/`get_result()` |
| Lua-Scripts (SHA-Hashes) | Bei Verbindungsaufbau zum Backend | `EVALSHA` statt `EVAL` — kein Script-Transfer pro Aufruf |
| Cron-Expressions (vorberechnete nächste Ausführung) | Beim Scheduler-Start | Kein Cron-Parsing pro Tick |
| Queue-Deklarationen | Bei Broker-Verbindung | Queues werden einmal deklariert, nicht pro Message geprüft |

**Prinzip:** Der Hot-Path (Task empfangen → ausführen → Ergebnis speichern) soll keine Berechnungen enthalten die beim Start hätten erledigt werden können.

### 2.2 Erlaubte Abhängigkeiten

| Kategorie | Library | Zweck |
|---|---|---|
| Serialisierung (JSON) | `msgspec` | JSON-Encoding/Decoding mit Type-Hooks für datetime, bytes, Decimal etc. |
| Serialisierung (Results) | `msgpack` | Serialisierung beliebiger Datentypen zum Backend |
| Serialisierung (optional) | `orjson` | Optionale schnelle JSON-Alternative. Automatisch verwendet wenn installiert. Installierbar via `pip install swarmq[fast]` |
| Logging | `structlog` | Strukturiertes, performantes Logging mit contextvars (keine transitiven Deps) |
| DI (optional, später) | `dishka` | Dependency Injection mit korrekten Scopes |
| Dev/Test | `mypy`, `ruff`, `pytest` | Qualitätssicherung |

**Pickle-Serialisierung wird NICHT unterstützt.** Nur JSON (msgspec) für Messages und msgpack für Results. Beide sind sicher und deterministisch. Kein `accept_content=['pickle']` oder äquivalent.

### 2.3 Package-Struktur

- **PyPI-Paket:** `swarmq`
- **Import:** `import swarmq` / `from swarmq import Task, TaskInfo, SwarmQ`
- **Extras:** `pip install swarmq[fast]` (orjson), `pip install swarmq[metrics]` (prometheus_client)
- **CLI:** `swarmq` als Entry-Point (via `pyproject.toml [project.scripts]`)

### 2.4 Broker & Backend

| Rolle | Phase 1 | Später |
|---|---|---|
| **Broker** | RabbitMQ | Valkey, Redis |
| **Backend** | Valkey | Redis |

**Anforderung an Broker:** Muss fehlgeschlagene Nachrichten für spätere Wiederverarbeitung unterstützen (Dead Letter / Retry-Mechanismus).

### 2.5 Verbindungs-Recovery

SwarmQ muss Ausfälle von Broker und Backend zur Laufzeit graceful behandeln:

| Szenario | Verhalten |
|---|---|
| **Broker-Verbindung verloren** | Automatischer Reconnect via `connect_robust()` (aio-pika). Laufende Tasks werden weiter verarbeitet. Neue Tasks können nicht empfangen werden bis Reconnect. |
| **Backend-Verbindung verloren** | Automatischer Reconnect (valkey-py Retry). Results werden gepuffert und nach Reconnect geschrieben. Task-Ausführung wird NICHT abgebrochen. |
| **Broker beim Start nicht erreichbar** | Worker wartet konfigurierbare Zeit (`connection_timeout`, Default: 30s), dann Exit mit Fehler. |
| **Backend beim Start nicht erreichbar** | Wie Broker — warten, dann Fehler. |
| **Broker-Restart während Betrieb** | Reconnect + Re-Subscribe auf Queues. Unacked Messages werden vom Broker redelivered. |
| **Backend-Restart während Betrieb** | Reconnect. Schedule-State (last_run) und Locks gehen verloren — Scheduler handelt automatisch (neue Leader Election, ggf. einmalige Doppelausführung). |
| **Result-Write fehlgeschlagen** | Task als FAILURE markieren, Error loggen (WARNING), Message nack mit requeue=True. Idempotency Key verhindert Doppelausführung. **Kein stiller Verlust.** |

#### 2.5.1 Circuit Breaker

Broker- und Backend-Connections verwenden ein Circuit-Breaker-Pattern um bei Ausfällen CPU-Burn und Log-Spam zu vermeiden:

| Zustand | Verhalten | Übergang |
|---|---|---|
| **CLOSED** (normal) | Alle Operationen werden ausgeführt. Fehler werden gezählt. | → OPEN nach `failure_threshold` aufeinanderfolgenden Fehlern (Default: 5) |
| **OPEN** (Ausfall) | Alle Operationen werden sofort mit `BrokerConnectionError`/`BackendConnectionError` abgelehnt. Kein Reconnect-Versuch. | → HALF_OPEN nach `recovery_timeout` (Default: 30s) |
| **HALF_OPEN** (Test) | Ein einzelner Test-Request wird durchgelassen. | → CLOSED bei Erfolg, → OPEN bei Fehler |

- Circuit Breaker ist pro Connection (Broker und Backend getrennt)
- Im OPEN-Zustand: Worker stoppt Task-Consumption, loggt einmalig ERROR
- Konfigurierbar: `circuit_breaker_threshold`, `circuit_breaker_timeout`

### 2.6 Client-Modus

SwarmQ kann als **reiner Client** ohne Worker genutzt werden (z.B. in Web-Apps):

```python
from swarmq import SwarmQ

# Nur Client — kein Worker, kein Scheduler
client = SwarmQ(
    broker=RabbitMQBroker("amqp://..."),
    backend=ValkeyBackend("valkey://..."),
)

# Tasks einreihen und Ergebnisse abrufen
task_id = await client.schedule("send_email", to="user@example.com")
result = await client.get_result(task_id, timeout=30)
```

- Kein Worker-Prozess nötig
- Keine Task-Discovery/Registry (nur Task-Namen als Strings)
- Leichtgewichtige Abhängigkeit für Producer-Seite

### 2.7 Result-TTL (Ablaufzeit der Ergebnisse)

Ergebnisse werden im Backend (Valkey) mit einer konfigurierbaren Ablaufzeit (TTL) gespeichert. Nach Ablauf werden sie automatisch gelöscht.

| Ebene | Konfiguration | Beschreibung |
|---|---|---|
| **Global** | `result_ttl` in der SwarmQ-Konfiguration | Default: `86400` (24 Stunden). Gilt für alle Tasks wenn kein Task-spezifischer Wert gesetzt ist. |
| **Pro Task** | `result_ttl` in `TaskInfo` | Überschreibt den globalen Default. `None` = globaler Default, `0` = Ergebnis wird nicht gespeichert. |

```python
# Globaler Default in der SwarmQ-Konfiguration
swarmq = SwarmQ(
    broker=RabbitMQBroker("amqp://..."),
    backend=ValkeyBackend("valkey://..."),
    result_ttl=86400,  # 24 Stunden (Default)
)

# Task mit eigenem TTL
class ImportantTask(Task):
    task_info = TaskInfo(
        name="important_task",
        queues=["default"],
        result_ttl=604800,  # 7 Tage
    )

# Task ohne Ergebnis-Speicherung
class FireAndForgetTask(Task):
    task_info = TaskInfo(
        name="log_event",
        queues=["logging"],
        delivery=DeliveryMode.FIRE_AND_FORGET,
        result_ttl=0,  # Ergebnis wird nicht gespeichert
    )
```

**Verhalten:**
- TTL wird beim Schreiben des Ergebnisses gesetzt (`SET ... PX <ttl_ms>`)
- Auflösungsreihenfolge: Task-`result_ttl` → Globaler `result_ttl` → Fallback `86400`
- `result_ttl=0` bedeutet: Ergebnis wird nach Ausführung **nicht** im Backend gespeichert

---

## 3. Message-Formate

### 3.1 Task Message (Broker → Worker)
```json
{
    "task": {
        "id": "UUID7",
        "queue": "STRING(255)",
        "name": "STRING(255)"
    },
    "args": [],
    "kwargs": {}
}
```

### 3.2 Result Message (Worker → Backend)
```json
{
    "task": {
        "id": "UUID7",
        "queue": "STRING(255)",
        "name": "STRING(255)"
    },
    "is_error": false,
    "error_message": "STRING(255)",
    "result": "<serialisierte Daten>"
}
```

---

## 4. Task-Definition (Klassen-basiert)

Tasks werden **nicht** über Decorators definiert, sondern als Klassen mit Basisklasse `Task`.

```python
class ExampleTask(Task):

    task_info = TaskInfo(
        soft_timeout=30,
        hard_timeout=60,
        queues=["a", "b"],
        name="example_task",
        delivery=DeliveryMode.AT_LEAST_ONCE,
        result_ttl=7200,
        rate_limit="100/m",
        max_retries=3,
        retry_backoff="exponential",
        retry_exceptions=[ConnectionError, TimeoutError],
        unique_for=300,
        lock="example-lock",
        max_age=3600,
        priority=5,
    )

    async def pre_execution(self) -> None:
        """Wird vor run() aufgerufen — Setup, Validierung, etc."""

    async def post_execution(self) -> None:
        """Wird nach run() aufgerufen — Cleanup, Logging, etc."""

    async def run(self, *args, **kwargs) -> MyReturnType:
        """Muss einen Return-Type haben. Kein `-> None` erlaubt."""
        # Fortschritt melden (optional, für lang laufende Tasks)
        await self.update_progress(current=50, total=100, message="Halbzeit")
```

### 4.1 TaskInfo
| Feld | Typ | Beschreibung |
|---|---|---|
| `name` | `str` (max 255) | Eindeutiger Task-Name |
| `queues` | `list[str]` | Zugeordnete Queues |
| `soft_timeout` | `int` | Weiches Timeout (Sekunden) |
| `hard_timeout` | `int` | Hartes Timeout (Sekunden) |
| `delivery` | `DeliveryMode` | `AT_LEAST_ONCE` (Default) oder `FIRE_AND_FORGET` |
| `result_ttl` | `int \| None` | Ablaufzeit des Ergebnisses in Sekunden. `None` = globaler Default |
| `rate_limit` | `str \| None` | Rate Limiting pro Task. Format: `"N/s"`, `"N/m"`, `"N/h"`. `None` = kein Limit |
| `max_retries` | `int` | Maximale Anzahl Retries. Default: `0` (kein Retry) |
| `retry_backoff` | `str` | Backoff-Strategie: `"linear"`, `"exponential"`, `"fixed"`. Default: `"exponential"` |
| `retry_exceptions` | `list[type] \| None` | Nur diese Exceptions lösen Retries aus. `None` = alle Exceptions |
| `unique_for` | `int \| None` | Deduplication-Fenster in Sekunden. Verhindert doppeltes Scheduling. `None` = deaktiviert |
| `lock` | `str \| None` | Task-Lock-Name. Nur eine Instanz mit diesem Lock läuft gleichzeitig. `None` = kein Lock |
| `max_age` | `int \| None` | Maximales Alter in Sekunden. Tasks die älter sind werden verworfen. `None` = deaktiviert (Default) |
| `priority` | `int` | Priorität. Niedrigere Werte = höhere Priorität. Default: `10` |
| `idempotency_key` | `str \| Callable \| None` | Idempotency-Schutz gegen doppelte Ausführung. String-Template (`"process-order-{order_id}"`) oder Callable das den Key aus args/kwargs berechnet. `None` = deaktiviert. |

### 4.5 Idempotency (Schutz gegen doppelte Ausführung)

Bei `AT_LEAST_ONCE` Delivery kann ein Task nach Worker-Crash erneut zugestellt werden. `idempotency_key` verhindert doppelte Ausführung:

```python
class ProcessOrderTask(Task):
    task_info = TaskInfo(
        name="process_order",
        idempotency_key="process-order-{order_id}",  # Template mit kwargs
    )
    async def run(self, order_id: int) -> dict: ...
```

**Verhalten:**
- Vor `run()`: Worker prüft ob `sq:idempotent:{key}` in Valkey existiert
- Key existiert mit Status COMPLETED → gecachtes Result zurückgeben, Task nicht ausführen
- Key existiert nicht → `SET NX` mit TTL, Task ausführen
- TTL des Idempotency-Keys = `result_ttl` des Tasks
- Idempotency-Prüfung passiert NACH `pre_execution()` aber VOR `run()`

### 4.2 Delivery-Modi

| Modus | Verhalten | Ack | Einsatz |
|---|---|---|---|
| `AT_LEAST_ONCE` (Default) | Message wird nach erfolgreicher Verarbeitung geackt. Bei Worker-Crash wird sie erneut zugestellt. Task muss idempotent sein. | Nach `post_execution()` | Kritische Tasks (Zahlungen, E-Mails, Daten-Mutationen) |
| `FIRE_AND_FORGET` | Message wird sofort beim Empfang geackt, **vor** der Verarbeitung. Spart einen Roundtrip pro Task. Bei Worker-Crash geht der Task verloren. | Sofort bei Empfang | Unkritische Tasks (Logging, Analytics, Cache-Warming) |

### 4.3 Lifecycle
1. `pre_execution()` — Vorbereitung
2. `run()` — Ausführung, Return-Wert wird automatisch via `msgpack` serialisiert
3. `post_execution()` — Nachbereitung

### 4.4 Dependency Injection (spätere Phase)
- Neue Basisklasse (z.B. `InjectableTask`) die Dependencies über `dishka` automatisch auflöst
- Fallback: `@inject` Decorator
- Korrekte Scope-Auflösung ist Pflicht

---

## 5. Task Registry & Auto-Discovery

### 5.1 Auto-Discovery

Tasks werden beim Worker-Start **automatisch entdeckt**. Zwei Modi:

```bash
# Automatisch: Scannt das aktuelle Package und alle Sub-Packages nach Task-Subklassen
swarmq worker

# Expliziter Pfad: Nur angegebene Module/Packages scannen
swarmq worker myapp.tasks
swarmq worker myapp.tasks myapp.workers
```

**Mechanismus:**
- Jede Klasse die von `Task` erbt, registriert sich automatisch via Metaklasse beim Import
- Im automatischen Modus: SwarmQ scannt das Working Directory rekursiv nach Python-Modulen und importiert sie
- Im expliziten Modus: Nur die angegebenen Module werden importiert
- Doppelte Task-Namen → Fehler beim Start (nicht stillschweigend überschreiben)

### 5.2 Registry

- Die Registry cached so viele Daten wie möglich (Name, Timeouts, Return-Type, etc.)
- Registrierte Informationen pro Task:
  - `name` — Eindeutiger Identifier
  - `timeout` / `polling` — Defaults für `get_result`
  - **Return-Type Annotation** — wird für Deserialisierung via `msgpack` verwendet
  - **TaskInfo** — alle konfigurierten Parameter (gecacht)
- `schedule()` und `run()` validieren, ob der Task-Name registriert ist → `TaskNotFoundError` wenn nicht

---

## 6. Client API

### 6.1 Grundoperationen

```python
# Task einplanen — gibt Task-ID zurück
task_id = await broker.schedule("example_task", [1, 2, 3], test=123, bla="blub")

# Ergebnis abrufen — mit konfigurierbarem Timeout und Polling-Intervall
result = await broker.get_result(task_id="...", timeout=10, polling=0.2)

# Kombination: Einplanen + Ergebnis abwarten
result = await broker.run("example_task", [1, 2, 3], test=123, bla="blub", timeout=10, polling=0.2)
```

- `timeout` und `polling` fallen auf Registry-Defaults zurück, wenn nicht explizit angegeben
- Return-Type aus der Registry wird zur Deserialisierung verwendet (`msgpack`)
- **Type-Validation bei schedule():** Task-Argumente werden gegen die Type-Annotations der `run()`-Methode validiert. Ungültige Argumente → `TaskArgumentError` sofort beim Scheduling, nicht erst bei Ausführung.

### 6.2 Verzögerte Ausführung (ETA / Countdown)

```python
# Relativ: In 60 Sekunden ausführen
task_id = await broker.schedule("send_reminder", delay=60)

# Absolut: Zu einem bestimmten Zeitpunkt ausführen
from datetime import datetime
task_id = await broker.schedule("send_report", eta=datetime(2026, 4, 16, 8, 30))
```

### 6.3 Task Cancellation

```python
# Task in Queue abbrechen (noch nicht gestartet)
await broker.cancel(task_id="...")

# Laufenden Task abbrechen (Worker erhält Cancel-Signal)
await broker.cancel(task_id="...", terminate=True)
```

### 6.4 Bulk Enqueue

```python
# Viele Tasks in einem Roundtrip einreihen
task_ids = await broker.schedule_many([
    ("process_image", [img_id], {}) for img_id in image_ids
])
```

### 6.5 Progress Tracking

```python
# Fortschritt eines laufenden Tasks abfragen
progress = await broker.get_progress(task_id="...")
# → {"current": 50, "total": 100, "message": "Halbzeit", "percent": 50}
```

### 6.6 Workflow-Primitiven

Zwei Wege um Signatures zu erstellen:

```python
from swarmq import chain, group, chord, sig

# Variante 1: Klassenmethode (type-safe, IDE-Autocompletion)
FetchTask.s(url)

# Variante 2: Name-basiert (entkoppelt, kein Import nötig)
sig("fetch", url)
```

**Workflows:**

```python
# Chain — sequenziell: Ergebnis fließt zum nächsten Task
result = await chain(
    FetchTask.s(url),
    ParseTask.s(),        # erhält Ergebnis von FetchTask
    StoreTask.s(),        # erhält Ergebnis von ParseTask
).apply()

# Group — parallel: Alle Tasks gleichzeitig ausführen
results = await group(
    DownloadTask.s(url) for url in urls
).apply()

# Chord — parallel + Callback: Group-Ergebnisse an Callback
result = await chord(
    group(DownloadTask.s(url) for url in urls),
    MergeResultsTask.s(),    # erhält Liste aller Download-Ergebnisse
).apply()

# Name-basiert (z.B. für entkoppelte Services)
result = await chain(
    sig("fetch", url),
    sig("parse"),
    sig("store"),
).apply()
```

**Sicherheitslimits (gegen Celery-Canvas-Probleme):**
- `max_workflow_depth`: Maximale Verschachtelungstiefe (Default: 10). Verhindert rekursive Blowups.
- `max_group_size`: Maximale Tasks in einer Group (Default: 1000). Verhindert Memory-Explosion.
- Workflows sind DAG-basiert (nicht rekursiv verschachtelt wie Celery Signatures).

---

## 7. Worker

### 7.1 Worker-Typen

| Typ | Verhalten | Concurrency |
|---|---|---|
| **Async Worker** | Alle Tasks rein async im Event-Loop | Konfigurierbar (max gleichzeitige Tasks) |
| **CPU Worker** | Async-Signatur, aber sequenzielle Abarbeitung | 1 Task gleichzeitig |

### 7.2 Task-Heartbeat (Zombie-Erkennung)

Worker sendet periodische Heartbeats für jeden laufenden Task an Valkey. Wenn Heartbeats ausbleiben (Worker-Crash), wird der Task als abandoned erkannt.

| Parameter | Default | Beschreibung |
|---|---|---|
| `heartbeat_interval` | 10s | Heartbeat-Frequenz pro laufendem Task |
| `heartbeat_timeout` | 50s | Nach dieser Zeit ohne Heartbeat gilt Task als abandoned |

**Implementierung:**
- Separater asyncio-Task im Worker der nie von Task-Ausführung blockiert wird
- Heartbeat = `PEXPIRE sq:heartbeat:{task_id} <timeout_ms>` in Valkey
- Abandoned Tasks werden vom nächsten Worker erkannt und requeued (nack mit requeue=True)
- Heartbeat ist orthogonal zu Soft/Hard Timeouts — beide laufen parallel

### 7.3 Skalierung
- Mehrere Worker startbar → jeweils eigener Prozess mit eigenem Event-Loop
- Mehrere Queues pro Worker definierbar

### 7.4 Graceful Shutdown
- **Plattform:** Linux
- OS-Signale (SIGTERM, SIGINT) korrekt verarbeiten
- Laufende Tasks sauber abschließen
- Bereits geschedulte Tasks dürfen **nicht verloren gehen** (zurück an Broker / nack)

### 7.4 Autoscaling

```bash
swarmq worker --autoscale=10,3    # min 3, max 10 Prozesse
```

- Skaliert Worker-Prozesse automatisch basierend auf Queue-Last
- Skaliert hoch wenn Queue-Tiefe steigt, runter wenn Queues leer
- Min/Max-Grenzen konfigurierbar

### 7.5 Burst Mode

```bash
swarmq worker --burst    # Alle Tasks verarbeiten, dann beenden
```

- Verarbeitet alle anstehenden Tasks in den konfigurierten Queues
- Beendet sich mit Exit-Code 0 wenn alle Queues leer sind
- Nützlich für CI/CD-Pipelines und Batch-Verarbeitung

### 7.6 Health Check

```bash
swarmq worker --check    # CLI Health Check (Exit-Code 0 = OK)
```

- **HTTP Health Endpoint:** Optionaler HTTP-Server am Worker für Kubernetes Readiness/Liveness Probes
- **CLI Check:** `swarmq worker --check` prüft ob der Worker läuft und gesund ist

**Response-Format (GET /health):**
```json
{
    "status": "healthy",
    "state": "consuming",
    "broker": "connected",
    "backend": "connected",
    "tasks_running": 3,
    "tasks_completed": 147,
    "uptime_seconds": 3600,
    "queue_depth": {"default": 10, "high-priority": 2}
}
```

Mögliche `state`-Werte: `starting`, `consuming`, `draining`, `shutting_down`, `stopped`

### 7.7 CLI
- Einfache, intuitive CLI zum Steuern der Worker
- Start, Stop, Status

### 7.8 Optional: Hot Reload
- Erkennt Code-Änderungen an Task-Klassen
- Startet Worker automatisch neu

---

## 8. Embedded Scheduler (Cron-basierte Aufgabenplanung)

Ein in jeden Worker eingebetteter Scheduler, der Cron-basierte Tasks automatisch ausführt. **Kein separater Prozess nötig** — der Benutzer startet Worker und der Scheduler läuft automatisch mit.

### 8.1 Kernprinzip: Embedded mit Leader Election

Der Scheduler läuft als asyncio-Task **innerhalb jedes Worker-Prozesses**. Über einen Distributed Lock (Valkey) wird sichergestellt, dass nur ein Worker der aktive Scheduler-Leader ist. Alle anderen sind Standby-Kandidaten.

```
Worker 1 [Scheduler: LEADER]   → dispatcht fällige Cron-Tasks
Worker 2 [Scheduler: STANDBY]  → wartet auf Lock
Worker 3 [Scheduler: STANDBY]  → wartet auf Lock
```

**Vorteile gegenüber separatem Prozess (Celery Beat, TaskIQ Scheduler):**
- Kein separates Deployment, kein zusätzlicher Prozess
- Automatischer Failover: Wenn der Leader-Worker stirbt, übernimmt ein anderer
- Skaliert mit den Workern — es gibt immer einen potenziellen Scheduler
- Der Benutzer muss sich nicht darum kümmern

### 8.2 Leader Election via Valkey

**Lock-Mechanismus:**
- Atomarer Lock-Erwerb: `SET swarmq:scheduler:lock <worker_id> NX PX 30000`
- Leader verlängert den Lock per Heartbeat (alle 10s) mit Lua-Script inkl. Ownership-Check
- Kandidaten prüfen alle 5s ob der Lock frei ist
- Bei Graceful Shutdown: Lock wird sofort freigegeben → Failover in Millisekunden
- Bei Crash: Lock läuft nach max. 30s ab → anderer Worker übernimmt

**Lua-Scripts (atomar, auf Valkey-Seite):**

Lock verlängern (nur wenn man noch Besitzer ist):
```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
```

Lock freigeben (nur eigenen Lock löschen):
```lua
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
```

**Kritische Regeln (aus RedBeat-Bug-Analyse):**
1. Lock-Verlängerung MUSS Ownership prüfen — sonst kann ein alter Leader den Lock eines neuen verlängern
2. Fehler beim Lock-Erwerb dürfen NICHT ignoriert werden — Scheduler darf nie ohne Lock laufen
3. VOR jedem Task-Dispatch nochmal prüfen ob man noch Leader ist

**Konfigurierbare Parameter:**

| Parameter | Default | Beschreibung |
|---|---|---|
| `lock_ttl` | 30s | TTL des Scheduler-Locks |
| `heartbeat_interval` | 10s | Intervall für Lock-Verlängerung |
| `candidate_check_interval` | 5s | Intervall für Lock-Erwerb-Versuche |

### 8.3 Schedule-Definition

Zwei Wege um Schedules zu definieren:

**Variante 1: Am Task selbst (bevorzugt für statische Schedules)**

```python
class CleanupTask(Task):
    task_info = TaskInfo(
        name="cleanup_expired_sessions",
        queues=["maintenance"],
        cron="0 */6 * * *",           # Alle 6 Stunden
    )
```

**Variante 2: Zentral in der SwarmQ-Konfiguration (für dynamische/externe Schedules)**

```python
from swarmq import CronSchedule

swarmq = SwarmQ(
    broker=...,
    backend=...,
    schedules=[
        CronSchedule(
            task="send_daily_report",
            cron="30 8 * * 1-5",          # Werktags um 08:30
            kwargs={"format": "pdf"},
        ),
    ],
)
```

**Variante 3: Dynamisch zur Laufzeit (CLI oder API)**

```bash
swarmq schedule add cleanup_expired_sessions "0 */6 * * *"
```

Task-Definition hat Vorrang vor zentraler Konfiguration. Dynamische Schedules (CLI/API) werden in Valkey persistiert und überleben Neustarts.

### 8.4 Anforderungen

| Anforderung | Beschreibung |
|---|---|
| **Cron-Syntax** | Standard 5-Feld Cron-Ausdrücke (Minute, Stunde, Tag, Monat, Wochentag). Zusätzlich: Interval-Syntax für Sub-Minute-Schedules (`every=30` für alle 30 Sekunden). |
| **Einzelinstanz-Garantie** | Leader Election via Valkey — nur ein Scheduler dispatcht, mit automatischem Failover |
| **Persistenz** | Schedules und letzte Ausführungszeitpunkte in Valkey gespeichert — überleben Neustarts und Leader-Wechsel |
| **Dynamische Verwaltung** | Schedules zur Laufzeit hinzufügen, ändern und entfernen (API + CLI) |
| **Missed-Run-Handling** | Konfigurierbar: übersprungene Ausführungen nachholen oder ignorieren |
| **Timezone-Support** | Cron-Ausdrücke beziehen sich auf eine konfigurierbare Zeitzone |

### 8.5 Architektur

- Jeder Worker-Prozess startet einen `EmbeddedScheduler` als asyncio-Task
- Der Scheduler versucht den Valkey-Lock zu erwerben (Leader Election)
- Nur der Leader prüft periodisch, welche Schedules fällig sind
- Fällige Tasks werden über den normalen Broker-Weg eingereiht (`schedule()`)
- Letzter Ausführungszeitpunkt pro Schedule wird in Valkey persistiert (`swarmq:schedule:{name}:last_run`)
- Bei Leader-Wechsel liest der neue Leader die letzten Ausführungszeitpunkte aus Valkey

### 8.6 CLI

```bash
swarmq schedule list                       # Alle Schedules anzeigen
swarmq schedule add <task> "<cron>"        # Schedule hinzufügen
swarmq schedule remove <schedule_id>       # Schedule entfernen
swarmq schedule status                     # Welcher Worker ist aktueller Scheduler-Leader?
```

---

## 9. Middleware-System

Ein erweiterbares Middleware-System für Cross-Cutting Concerns (Metriken, Error-Tracking, Tracing) ohne Task-Code zu verändern.

### 9.1 Middleware-Hooks

| Hook | Seite | Beschreibung |
|---|---|---|
| `pre_enqueue(message)` | Client | Vor dem Senden an Broker. Kann Message modifizieren. |
| `post_enqueue(message)` | Client | Nach dem Senden an Broker. |
| `pre_execute(message)` | Worker | Vor Task-Ausführung. Kann Message modifizieren. |
| `post_execute(message, result)` | Worker | Nach erfolgreicher Ausführung. |
| `on_error(message, result, exception)` | Worker | Bei Task-Fehler. |
| `on_retry(message, exception, retry_count)` | Worker | Bei Retry-Auslösung. |

### 9.2 Beispiel

```python
from swarmq import Middleware

class SentryMiddleware(Middleware):
    async def on_error(self, message, result, exception):
        sentry_sdk.capture_exception(exception)

class MetricsMiddleware(Middleware):
    async def post_execute(self, message, result):
        metrics.increment("tasks.completed", tags={"task": message.task.name})

    async def on_error(self, message, result, exception):
        metrics.increment("tasks.failed", tags={"task": message.task.name})

swarmq.add_middleware(SentryMiddleware())
swarmq.add_middleware(MetricsMiddleware())
```

### 9.3 Ausführungsreihenfolge

- `pre_*`-Hooks: In Reihenfolge der Registrierung (Forward)
- `post_*`- und `on_error`-Hooks: In umgekehrter Reihenfolge (Stack-Semantik)

---

## 10. Signal/Event-System

Ein Dekorator-basiertes Observer-Pattern für Task-Lifecycle-Events.

### 10.1 Verfügbare Signale

| Signal | Wann ausgelöst |
|---|---|
| `TASK_ENQUEUED` | Task wurde in Queue eingereiht |
| `TASK_STARTED` | Worker hat Task-Ausführung begonnen |
| `TASK_COMPLETED` | Task erfolgreich abgeschlossen |
| `TASK_FAILED` | Task mit Exception fehlgeschlagen |
| `TASK_RETRYING` | Task wird erneut versucht |
| `TASK_REVOKED` | Task wurde abgebrochen |
| `TASK_EXPIRED` | Task wurde wegen max_age verworfen |
| `TASK_LOCKED` | Task-Ausführung wegen Lock übersprungen |
| `TASK_RATE_LIMITED` | Task wegen Rate Limit verzögert |
| `WORKER_ONLINE` | Worker ist bereit |
| `WORKER_SHUTDOWN` | Worker fährt herunter |
| `WORKER_HEARTBEAT` | Periodischer Worker-Heartbeat |
| `SCHEDULER_LEADER` | Worker wurde Scheduler-Leader |
| `SCHEDULER_STANDBY` | Worker ist Scheduler-Standby |

### 10.2 Verwendung

```python
from swarmq import Signal

@swarmq.on_signal(Signal.TASK_FAILED)
def on_failure(task_name, task_id, exception):
    alert_ops_team(task_name, exception)

@swarmq.on_signal(Signal.TASK_COMPLETED)
def on_success(task_name, task_id, duration_ms):
    statsd.timing(f"tasks.{task_name}.duration", duration_ms)
```

---

## 11. Dead Letter Queue (DLQ)

### 11.1 Verhalten

- Tasks die alle Retries erschöpft haben, werden automatisch in die DLQ verschoben
- Tasks die wegen `max_age` verworfen werden, landen ebenfalls in der DLQ
- DLQ-Messages werden mit Kontext gespeichert (Original-Task, Exception, Retry-Historie)

### 11.2 CLI

```bash
swarmq dlq list                    # Alle DLQ-Messages anzeigen
swarmq dlq list --queue default    # Nur bestimmte Queue
swarmq dlq inspect <message_id>    # Details einer Message (inkl. Traceback)
swarmq dlq retry <message_id>      # Einzelne Message erneut einstellen
swarmq dlq retry --all             # Alle DLQ-Messages erneut einstellen
swarmq dlq purge                   # Alle DLQ-Messages löschen
swarmq dlq purge --queue default   # Nur bestimmte Queue purgen
```

---

## 12. Prometheus Metrics

Optionaler Metrics-Endpoint für Production-Monitoring.

### 12.1 Aktivierung

```bash
swarmq worker --metrics-port 9090    # HTTP /metrics Endpoint auf Port 9090
```

### 12.2 Exportierte Metriken

| Metrik | Typ | Beschreibung |
|---|---|---|
| `swarmq_tasks_total` | Counter | Gesamtzahl verarbeiteter Tasks (Labels: task, queue, status) |
| `swarmq_task_duration_seconds` | Histogram | Task-Ausführungsdauer (Labels: task, queue) |
| `swarmq_tasks_active` | Gauge | Aktuell laufende Tasks |
| `swarmq_queue_depth` | Gauge | Anzahl Tasks in Queue (Labels: queue) |
| `swarmq_worker_uptime_seconds` | Gauge | Worker-Uptime |
| `swarmq_retries_total` | Counter | Anzahl Retries (Labels: task) |
| `swarmq_dlq_messages` | Gauge | Anzahl Messages in DLQ (Labels: queue) |
| `swarmq_worker_memory_bytes` | Gauge | Worker-Speicherverbrauch (RSS) |
| `swarmq_broker_circuit_state` | Gauge | Circuit Breaker Zustand (0=closed, 1=open, 2=half_open) |

---

## 13. Logging & Observability

Performantes, strukturiertes Logging das dem Benutzer jederzeit einen detaillierten Überblick über den Systemzustand gibt.

### 13.1 Anforderungen

- **Performant:** Logging darf den Hot-Path nicht spürbar verlangsamen. Keine synchronen I/O-Operationen im Task-Ausführungspfad.
- **Strukturiert:** Alle Log-Einträge enthalten maschinenlesbare Kontextdaten (Task-ID, Queue, Worker-ID, Dauer, etc.)
- **Konfigurierbar:** Log-Level pro Komponente einstellbar (Worker, Scheduler, Broker, Backend)
- **Standardkonform:** Nutzt Python's `logging`-Modul als Backend — kompatibel mit bestehenden Logging-Setups des Benutzers

### 13.1.1 Technische Umsetzung: structlog + QueueHandler

**Architektur (Event-Loop wird nie blockiert):**
```
structlog → stdlib Logger → QueueHandler (~1μs) → [Bounded Queue]
    → QueueListener (eigener Thread) → Handler (Stream/File/Network)
```

- **structlog** als Frontend: Processor-Pipeline, contextvars, JSON-Rendering
- **stdlib logging** als Backend: Handler-Kompatibilität (aio-pika, valkey-py Logs werden ebenfalls strukturiert)
- **QueueHandler:** `queue.put()` kostet ~1μs — kein I/O im Event-Loop
- **QueueListener:** Eigener Thread für die tatsächliche Log-Ausgabe
- **Bounded Queue** (maxsize=10.000): Backpressure statt unbegrenztem Speicherwachstum
- **`make_filtering_bound_logger`:** Deaktivierte Log-Levels sind `return None` — buchstäblich kein Overhead
- **`cache_logger_on_first_use=True`:** Logger wird einmalig assembliert und gecacht (Startup-Caching)
- **contextvars:** Task-ID, Worker-ID etc. werden einmal gebunden und propagieren automatisch über alle `await`-Aufrufe — kein manuelles Mitschleifen
- **`ProcessorFormatter`:** Alle Logs (structlog UND stdlib von Drittbibliotheken) werden einheitlich als JSON gerendert

### 13.2 Log-Level-Strategie

| Level | Was geloggt wird |
|---|---|
| `DEBUG` | Jeder Schritt im Task-Lifecycle, Broker-Messages, Lock-Erwerb/Verlust, Heartbeats, Prefetch-Details |
| `INFO` | Task gestartet/abgeschlossen (mit Dauer), Worker gestartet/gestoppt, Scheduler-Leadership, Schedule-Dispatch, Verbindungsaufbau |
| `WARNING` | Soft-Timeout erreicht, Retry ausgelöst, Verbindung verloren (mit Reconnect), Lock-Verlust, fehlgeschlagener Heartbeat |
| `ERROR` | Task fehlgeschlagen (mit Exception), Hard-Timeout, DLQ-Zustellung, Ergebnis konnte nicht gespeichert werden |
| `CRITICAL` | Worker kann nicht starten, Broker/Backend dauerhaft nicht erreichbar, alle Restarts erschöpft |

### 13.3 Strukturierte Kontextdaten

Jeder Log-Eintrag im Task-Kontext enthält automatisch:

```python
{
    "worker_id": "worker-3",
    "worker_pid": 12345,
    "task_id": "01926f3a-87c4-7def-8abc-123456789012",
    "task_name": "send_daily_report",
    "queue": "default",
    "delivery": "AT_LEAST_ONCE",
    "attempt": 1,            # Retry-Nummer
    "duration_ms": 142.7,    # Nur bei Abschluss/Fehler
}
```

### 13.4 Periodische Status-Ausgabe

Worker geben in konfigurierbarem Intervall (Default: 60s) einen Status-Report aus:

```
[INFO] Worker-3 Status: 47 Tasks abgeschlossen | 3 aktiv | 0 fehlgeschlagen | Uptime 12m34s | Memory 48MB | Queue: default (12 pending)
```

**Inhalt:**
- Abgeschlossene / aktive / fehlgeschlagene Tasks seit Start
- Durchschnittliche Task-Dauer (gleitend)
- Worker-Uptime und Memory-Verbrauch
- Queue-Tiefe (wenn vom Broker abfragbar)
- Scheduler-Status: Leader/Standby, nächster Schedule-Dispatch

### 13.5 Startup-Report

Beim Worker-Start wird ein detaillierter Report ausgegeben:

```
[INFO] SwarmQ Worker gestartet
  PID:            12345
  Worker-ID:      worker-3
  Python:         3.14.0
  Event-Loop:     uvloop 0.21.0
  Broker:         RabbitMQ (amqp://localhost:5672)
  Backend:        Valkey (valkey://localhost:6379)
  Queues:         default, high-priority
  Tasks:          12 registriert
  Concurrency:    10 (async)
  Result-TTL:     86400s (global)
  Scheduler:      Embedded (Standby)
```

### 13.6 Performance-Optimierungen

- **Lazy Formatting:** Log-Messages werden nur formatiert wenn das Level aktiv ist (`log.debug("Task %s", task_id)` statt f-Strings)
- **Kontextdaten via `logging.LoggerAdapter` oder `contextvars`:** Kein manuelles Mitschleifen von Task-ID/Worker-ID durch alle Funktionsaufrufe
- **Asynchrone Handler (optional):** Für Produktion kann ein async-fähiger Handler konfiguriert werden der Log-I/O nicht den Event-Loop blockiert

### 13.7 Credential-Sanitierung

- Alle Broker/Backend-URLs werden vor dem Loggen sanitiert: Passwörter → `***`
- Gilt auch für Exception-Tracebacks, DLQ-Messages und Startup-Report
- Auch Task-Argumente die als sensibel markiert sind (`sensitive=True` in TaskInfo) werden nicht geloggt

---

## 14. Konfiguration

### 14.1 Konfigurationsmodell

SwarmQ wird primär **programmatisch** konfiguriert. Umgebungsvariablen dienen als Override für Deployment-Szenarien.

**Alle Konfiguration wird beim Start validiert.** Widersprüchliche Settings (z.B. `hard_timeout < soft_timeout`) → `ConfigurationError` mit klarer Fehlermeldung. Maximal ~30 Konfigurationsoptionen um Celery's Konfigurationsexplosion (100+) zu vermeiden.

```python
from swarmq import SwarmQ, RabbitMQBroker, ValkeyBackend

app = SwarmQ(
    # Broker & Backend
    broker=RabbitMQBroker("amqp://localhost:5672"),
    backend=ValkeyBackend("valkey://localhost:6379"),

    # Globale Defaults
    result_ttl=86400,              # 24h
    task_timeout=300,              # 5 Min Default-Timeout

    # Worker
    concurrency=10,                # Max gleichzeitige Tasks
    queues=["default"],            # Queues die konsumiert werden

    # Scheduler
    scheduler_enabled=True,        # Embedded Scheduler aktiv (Default)

    # Logging
    log_level="INFO",
    log_json=True,                 # JSON-Output (Default in Produktion)

    # Metrics
    metrics_port=None,             # None = deaktiviert, 9090 = aktiviert
)
```

### 14.2 Umgebungsvariablen

Jede programmatische Option kann über Umgebungsvariablen überschrieben werden:

| Variable | Entspricht | Beispiel |
|---|---|---|
| `SWARMQ_BROKER_URL` | `broker=` | `amqp://user:pass@host:5672` |
| `SWARMQ_BACKEND_URL` | `backend=` | `valkey://host:6379/0` |
| `SWARMQ_RESULT_TTL` | `result_ttl=` | `86400` |
| `SWARMQ_CONCURRENCY` | `concurrency=` | `10` |
| `SWARMQ_QUEUES` | `queues=` | `default,high-priority` |
| `SWARMQ_LOG_LEVEL` | `log_level=` | `DEBUG` |
| `SWARMQ_LOG_JSON` | `log_json=` | `true` |
| `SWARMQ_METRICS_PORT` | `metrics_port=` | `9090` |

**Auflösungsreihenfolge:** Umgebungsvariable → Programmatisch → Default

### 14.3 CLI-Flags

CLI-Flags überschreiben sowohl Umgebungsvariablen als auch programmatische Konfiguration:

```bash
swarmq worker                                    # Standard: Alle Queues, auto-discovered Tasks
swarmq worker myapp.tasks                        # Expliziter Task-Pfad
swarmq worker --queues default,high              # Bestimmte Queues
swarmq worker --concurrency 20                   # Concurrency override
swarmq worker --log-level DEBUG                  # Log-Level override
swarmq worker --burst                            # Burst Mode
swarmq worker --autoscale 10,3                   # Autoscaling
swarmq worker --metrics-port 9090                # Prometheus Metrics
swarmq worker --health-port 8080                 # Health Check HTTP
swarmq worker --check                            # Health Check CLI
swarmq worker --reload                           # Hot Reload
```

**Auflösungsreihenfolge:** CLI-Flag → Umgebungsvariable → Programmatisch → Default

---

## 15. Exception-Hierarchie

```
SwarmQError (Basis)
  |
  |-- ConfigurationError           # Ungültige Konfiguration beim Start
  |
  |-- ConnectionError
  |     |-- BrokerConnectionError  # Broker nicht erreichbar
  |     |-- BackendConnectionError # Backend nicht erreichbar
  |
  |-- TaskError
  |     |-- TaskNotFoundError      # Task-Name nicht in Registry
  |     |-- TaskTimeoutError       # Soft- oder Hard-Timeout überschritten
  |     |-- TaskCancelledError     # Task wurde abgebrochen
  |     |-- TaskExpiredError       # Task wegen max_age verworfen
  |     |-- TaskLockedError        # Task-Lock konnte nicht erworben werden
  |     |-- TaskRateLimitedError   # Rate Limit überschritten
  |
  |-- ResultError
  |     |-- ResultNotReadyError    # Ergebnis noch nicht verfügbar
  |     |-- ResultExpiredError     # Ergebnis-TTL abgelaufen
  |     |-- ResultSerializationError # Ergebnis konnte nicht (de)serialisiert werden
  |
  |-- SchedulingError
  |     |-- DuplicateTaskError     # Unique-Task bereits in Queue (unique_for)
  |
  |-- WorkflowError
        |-- ChainError             # Fehler in einer Chain
        |-- ChordError             # Chord-Callback konnte nicht ausgelöst werden
```

**Task-interne Steuerungs-Exceptions:**

```python
from swarmq import Retry, NoRetry

# Retry mit benutzerdefiniertem Delay
raise Retry(delay=60, reason="API temporarily unavailable")

# Explizit KEINEN Retry (auch wenn retry_exceptions matcht)
raise NoRetry(reason="Invalid input, retry won't help")
```

---

## 16. Testing & Qualitätssicherung

| Tool | Zweck |
|---|---|
| `pytest` | Komplette Test-Suite, alle Funktionen, Edge Cases |
| `mypy` | Statische Typ-Prüfung |
| `ruff` | Linting & Formatting |
| `docker-compose` | End-to-End Tests mit echtem RabbitMQ + Valkey |

---

## 17. Dokumentation

### 17.1 Entwickler-Dokumentation (HTML)

Ausführliche Dokumentation für Programmierer, generiert als statische HTML-Seiten.

**Inhalt:**
- **Getting Started** — Installation, minimales Beispiel, erster Worker in unter 5 Minuten
- **Core Concepts** — Tasks, Queues, Broker, Backend, Worker, Lifecycle erklärt mit einfachen Beispielen
- **Task-Definition** — Klassen-basierte Tasks, TaskInfo, Delivery-Modi, Return-Types, Timeouts
- **Client API** — `schedule()`, `get_result()`, `run()` mit Beispielen für jeden Anwendungsfall
- **Worker** — Konfiguration, Async vs. CPU Worker, Concurrency, Graceful Shutdown
- **Middleware** — Eigene Middleware schreiben, Hooks, Reihenfolge
- **Serialisierung** — Message-Format (JSON/msgspec), Result-Serialisierung (msgpack)
- **Error Handling & Retries** — Delivery-Modi, DLQ, Retry-Strategien, Idempotenz
- **Dependency Injection** — dishka-Integration, Scopes, Beispiele
- **Testing** — InMemoryBroker, Burst-Modus, Test-Fixtures, Best Practices
- **CLI Reference** — Alle Befehle und Optionen
- **Weiterführende Themen** — Horizontale Skalierung, Monitoring, Queue-Routing, Performance-Tuning
- **API Reference** — Auto-generiert aus Docstrings/Type-Annotations

**Anforderungen:**
- Jedes Konzept startet mit einem simplen, kopierbaren Beispiel
- Weiterführende Themen bauen darauf auf (progressive Komplexität)
- Alle Code-Beispiele müssen lauffähig sein

### 17.2 KI-Agenten-Dokumentation

Separate Dokumentation die es KI-Agenten (LLMs, Coding-Assistenten) ermöglicht, SwarmQ zu nutzen **ohne den Quellcode zu lesen**.

**Inhalt:**
- **Vollständige API-Spezifikation** — Alle öffentlichen Klassen, Methoden, Parameter, Return-Types mit Beschreibung
- **Message-Formate** — Exakte JSON-Schemas für Task-Messages und Result-Messages
- **CLI-Spezifikation** — Alle Befehle, Flags, erwartete Ausgaben, Exit-Codes
- **Entscheidungsbäume** — Wann welchen Delivery-Modus nutzen, wann welchen Worker-Typ, wann Retry konfigurieren
- **Komplette Beispiele** — Für jeden Anwendungsfall ein Copy-Paste-fertiges Beispiel
- **Fehlerkatalog** — Alle Exceptions, ihre Bedeutung, und wie man sie behebt
- **Konfigurationsreferenz** — Alle Optionen mit Defaults, erlaubten Werten und Auswirkungen

**Anforderungen:**
- Maschinenlesbar strukturiert (klare Überschriften, konsistente Formate)
- Keine Annahme dass der Agent den Quellcode kennt — alles explizit dokumentiert
- Jede API-Methode mit Signatur, Parametern, Return-Type, Exceptions und Beispiel
