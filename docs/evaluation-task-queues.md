# Evaluation bestehender Task-Queue-Systeme

**Erstellt:** 2026-04-14
**Zweck:** Referenzdokument fuer die Entwicklung von SwarmQ. Enthaelt alle Erkenntnisse aus der systematischen Analyse von Celery, TaskIQ, Dramatiq und arq.

---

## Inhaltsverzeichnis

1. [Celery](#1-celery)
2. [TaskIQ](#2-taskiq)
3. [Dramatiq](#3-dramatiq)
4. [arq](#4-arq)
5. [Vergleichsmatrix](#5-vergleichsmatrix)
6. [Best-Practice-Synthese fuer SwarmQ](#6-best-practice-synthese-fuer-swarmq)

---

# 1. Celery

**Version:** 5.6.x | **Stars:** 25.000+ | **Lizenz:** BSD | **Python:** 3.8+
**Abhaengigkeiten:** kombu (Messaging), billiard (Multiprocessing), vine (Promises)

## 1.1 Ausfallsicherheit & Reliability

### Message-Garantien

Celery bietet **at-least-once** Delivery als primaere Garantie. Es gibt kein eingebautes exactly-once — das muss auf Anwendungsebene durch idempotente Tasks realisiert werden.

Die Garantie-Stufe haengt direkt von der Konfiguration ab:

| Konfiguration | Garantie | Risiko |
|---|---|---|
| `acks_late=False` (Default) | At-most-once | Task geht bei Worker-Crash verloren |
| `acks_late=True` | At-least-once | Task kann doppelt ausgefuehrt werden |
| `acks_late=True` + Idempotenz | Pseudo-exactly-once | Komplexitaet liegt beim Entwickler |

**Kritische Erkenntnis:** Celerys Default (`acks_late=False`) ist **unsicher** — die Nachricht wird geackt *bevor* der Task ausgefuehrt wird. Tasks verschwinden bei Worker-Crashes spurlos, was schwerer zu debuggen ist als doppelte Ausfuehrungen.

### Task Acknowledgment

```python
# Default: ACK vor Ausfuehrung (unsicher!)
@app.task
def my_task():
    do_work()

# Empfohlen: ACK nach Ausfuehrung
@app.task(acks_late=True)
def my_task():
    do_work()

# Global aendern:
app.conf.task_acks_late = True

# Fein-granular: auch bei Failure/Timeout acken?
@app.task(acks_late=True, acks_on_failure_or_timeout=True)
def my_task():
    do_work()
```

Wenn `acks_on_failure_or_timeout=True` (Default bei `acks_late=True`), wird die Nachricht auch bei Exceptions geackt — der Task wird *nicht* redelivered bei Fehler, sondern nur bei Worker-Crash *waehrend* der Ausfuehrung.

### Retry-Mechanismen

**Manueller Retry (via `self.retry()`):**
```python
@app.task(bind=True, max_retries=3)
def my_task(self, url):
    try:
        response = requests.get(url)
    except ConnectionError as exc:
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)
```

**Automatischer Retry (via `autoretry_for`):**
```python
@app.task(
    autoretry_for=(ConnectionError, TimeoutError),
    retry_backoff=True,          # Exponential Backoff aktivieren
    retry_backoff_max=600,       # Max 10 Minuten zwischen Retries
    retry_jitter=True,           # Zufaellige Varianz (verhindert Thundering Herd)
    max_retries=5,
    retry_kwargs={'countdown': 5}  # Initiales Delay
)
def fetch_data(url):
    return requests.get(url).json()
```

Backoff-Formel: `countdown = retry_backoff * (2 ** (retries - 1))`, begrenzt durch `retry_backoff_max`.

**Schwaeche:** Manueller Retry erfordert `bind=True` und explizites Exception-Handling. `autoretry_for` wurde spaet hinzugefuegt und fuehlt sich wie ein Afterthought an.

### Dead Letter Queue Handling

**Celery hat keine erstklassige DLQ-Unterstuetzung.** Dies ist einer der am haeufigsten kritisierten Punkte. Die offizielle Dokumentation verweist lediglich auf die DLQ-Features des jeweiligen Brokers.

Fuer RabbitMQ muss man DLQs manuell ueber `task_queues` und broker-spezifische Argumente konfigurieren:

```python
from kombu import Queue, Exchange

dead_letter_exchange = Exchange('dead_letter')

app.conf.task_queues = [
    Queue(
        'default',
        exchange=Exchange('default'),
        routing_key='default',
        queue_arguments={
            'x-dead-letter-exchange': 'dead_letter',
            'x-dead-letter-routing-key': 'dead_letter',
        }
    ),
    Queue('dead_letter', dead_letter_exchange, routing_key='dead_letter'),
]
```

**Erkenntnis:** DLQ-Support sollte erstklassig und broker-agnostisch implementiert werden. Bei Celery bricht die Broker-Abstraktion genau an diesem Punkt zusammen.

### Graceful Shutdown

Celery implementiert ein **dreistufiges Shutdown-Modell**:

| Stufe | Trigger | Verhalten |
|---|---|---|
| **Warm Shutdown** | SIGTERM (oder erster SIGINT) | Stoppt neue Message-Consumption, wartet *unbegrenzt* auf laufende Tasks |
| **Soft Shutdown** | `worker_soft_shutdown_timeout` Sekunden nach Warm | Zeitlimitierter Warm-Shutdown; eskaliert zu Cold nach Timeout |
| **Cold Shutdown** | SIGQUIT (oder zweiter SIGINT) | Bricht laufende Tasks sofort ab |

Shutdown-Flow:
```
Signal empfangen
  -> Signal Handler setzt should_stop/should_terminate Flag
  -> Consumer stoppt Message-Consumption
  -> [Warm: Pool drainen, Tasks abschliessen]
  -> [Cold: Pool sofort terminieren]
  -> Connections und Ressourcen aufraeumen
  -> Exit (EX_OK=0, EX_FAILURE=1, EX_UNAVAILABLE=69)
```

Seit Celery 5.6 werden Heartbeats waehrend des Warm Shutdown weiter gesendet, damit die Broker-Connection nicht vorzeitig geschlossen wird.

**SIGHUP** fuehrt zu einem Prozess-Restart via `os.execv()` — der Worker ersetzt sich selbst.

**Falle:** Die Environment-Variable `REMAP_SIGTERM=SIGQUIT` kann SIGTERM auf Cold Shutdown umleiten — in Kubernetes-Umgebungen ist das ein haeufiger Fehler, weil Container per SIGTERM gestoppt werden und dann Tasks verloren gehen.

### Worker-Crash waehrend Task-Ausfuehrung

| Konfiguration | Verhalten bei Crash |
|---|---|
| `acks_late=False` (Default) | **Task ist verloren** — Nachricht wurde bereits geackt |
| `acks_late=True` + RabbitMQ | Broker liefert Nachricht erneut an anderen Worker (nach Erkennung der verlorenen Connection) |
| `acks_late=True` + Redis | Nachricht wird erst nach Ablauf des Visibility Timeout (Default: 3600s = 1 Stunde) redelivered |

### Visibility Timeout (Redis-spezifisch)

Da Redis kein natives AMQP-Acknowledgment besitzt, simuliert Kombu dies ueber einen Visibility-Timeout-Mechanismus:

```python
app.conf.broker_transport_options = {
    'visibility_timeout': 3600  # Sekunden, Default
}
```

Unacknowledged Messages werden nach Ablauf automatisch redelivered. Bei Worker-Crash mit Redis als Broker kann es **bis zu einer Stunde** dauern, bis ein Task erneut zugestellt wird.

**Falle:** Wenn ein Task laenger als der Visibility Timeout laeuft, wird er redelivered *waehrend* er noch laeuft — doppelte Ausfuehrung.

### Idempotenz-Unterstuetzung

Celery bietet **keine eingebaute Idempotenz-Unterstuetzung**. Community-Strategien:
1. Idempotency Keys: Externe Locks (Redis SETNX) basierend auf Task-Name + Argumenten
2. Datenbank-Constraints: Unique-Constraints auf Ergebnis-Tabellen
3. celery-singleton: Drittanbieter-Library fuer Redis-Locks
4. Task-ID als Idempotenz-Schluessel fuer externe API-Aufrufe

---

## 1.2 Architektur & Erweiterbarkeit

### Gesamtarchitektur

Celery folgt dem Producer/Consumer-Pattern mit vier Hauptkomponenten:

```
Producer (App/Web)
    |
    | task.delay() / apply_async()
    v
Broker (RabbitMQ / Redis / SQS)
    |
    | Message Delivery
    v
Worker (Consumer) ---> Result Backend (Redis / DB / RPC)
    |                        ^
    | Ausfuehrung            | Ergebnis speichern
    v                        |
    Task-Execution ----------+
```

Die zentrale Abstraktion liegt in **Kombu**, Celerys Messaging-Library, die Broker-Kommunikation abstrahiert.

### Broker-Abstraktion (Kombu)

Kombu implementiert eine zweistufige Transport-Hierarchie:

```
Transport (Basis-Klasse)
  |
  +-- Native Transports (AMQP-nativ)
  |     +-- pyamqp (Default, AMQP 0-9-1)
  |     +-- librabbitmq (C-Bindings, hohe Performance)
  |     +-- qpid (AMQP 0-10)
  |
  +-- Virtual Transports (AMQP-Emulation)
        +-- redis
        +-- sqs
        +-- mongodb
        +-- memory (fuer Tests)
        +-- sqlalchemy
        +-- zookeeper
```

**Native Transports** sprechen direkt AMQP und unterstuetzen alle Features (Exchanges, Routing, Acknowledgments, Transactions).

**Virtual Transports** emulieren AMQP-Semantik ueber non-AMQP-Backends. Sie erben von `kombu.transport.virtual.Transport` und implementieren Backend-spezifische Methoden (`_put()`, `_get()`, `_purge()`). Die virtuelle Basisklasse uebernimmt Exchange-Routing und Acknowledgment-Logik.

Transport-Auswahl ueber URL-Schema:
```python
app = Celery('myapp', broker='amqp://user:pass@host:5672/vhost')   # RabbitMQ
app = Celery('myapp', broker='redis://localhost:6379/0')            # Redis
app = Celery('myapp', broker='sqs://access_key:secret_key@')        # SQS
```

**Kritische Schwaeche:** Die Abstraktion ist "leaky" — Features wie DLQ, Priority Queues und TTL sind broker-spezifisch. Redis unterstuetzt z.B. kein TTL fuer Messages, und Priority Queues verwenden Sorted Sets statt nativer Mechanismen.

### Result Backend Abstraktion

```
Backend (celery/backends/base.py)
  |
  +-- BaseBackend (+ SyncBackendMixin)
  |     +-- MongoBackend
  |     +-- CassandraBackend
  |
  +-- KeyValueStoreBackend (+ SyncBackendMixin)
  |     +-- MemcachedBackend
  |     +-- AzureBlobBackend
  |
  +-- RedisBackend (+ AsyncBackendMixin)
  |     (Nutzt Redis Pub/Sub fuer Echtzeit-Delivery)
  |
  +-- DatabaseBackend (SQLAlchemy)
  |
  +-- RPCBackend (AMQP Reply-Queues)
        (Transient, einmalig abrufbar)
```

`RedisBackend` nutzt Redis Pub/Sub statt Polling fuer Echtzeit-Result-Delivery. Alle anderen Backends verwenden Polling.

`RPCBackend` sendet Ergebnisse als AMQP-Messages zurueck an den Caller. Ergebnisse sind transient und nur einmal abrufbar.

### Signal-System

Celery bietet ein umfassendes Signal-System:

**Task-Signale:**
- `task_prerun` / `task_postrun` — vor/nach Ausfuehrung
- `task_success` / `task_failure` / `task_retry`
- `task_received` / `task_revoked` / `task_rejected`
- `task_internal_error` / `task_unknown`

**Worker-Signale:**
- `celeryd_init` / `celeryd_after_setup`
- `worker_init` / `worker_ready` / `worker_shutdown`
- `worker_shutting_down`
- `worker_process_init` / `worker_process_shutdown`
- `heartbeat_sent`

**Beat/Logging/Command-Signale:**
- `beat_init` / `beat_embedded_init`
- `setup_logging` / `after_setup_logger`

```python
from celery.signals import task_prerun, task_failure

@task_prerun.connect
def task_starting(sender=None, task_id=None, **kwargs):
    print(f"Task {sender.name} [{task_id}] starting")

@task_failure.connect
def task_failed(sender=None, task_id=None, exception=None, **kwargs):
    alert_ops_team(sender.name, task_id, exception)
```

### Bootstep/Extension System

Celery bietet ein maechtiges, aber komplexes Erweiterungssystem ueber **Bootsteps** — Komponenten, die in den Worker- oder Consumer-Lifecycle eingehaengt werden:

```python
from celery import bootsteps

class MyCustomStep(bootsteps.StartStopStep):
    requires = ('celery.worker.components:Pool',)

    def start(self, worker):
        print("Custom step started")

    def stop(self, worker):
        print("Custom step stopped")

app.steps['worker'].add(MyCustomStep)
```

Die Reihenfolge wird durch einen Dependency-Graph (`requires`-Attribut) bestimmt. Es gibt Worker-Steps (Hauptprozess) und Consumer-Steps (Broker-Verbindung).

### Serialisierung

| Format | Vorteile | Nachteile |
|---|---|---|
| **JSON** (Default seit 4.0) | Sicher, human-readable, breit kompatibel | Keine komplexen Typen |
| **Pickle** | Kann fast alles serialisieren | **Sicherheitsrisiko** — arbitrary code execution |
| **YAML** | Mehr Datentypen als JSON | Langsam |
| **MsgPack** | Kompakt, schnell, binaer | Weniger human-readable |

```python
app.conf.task_serializer = 'json'
app.conf.result_serializer = 'json'
app.conf.accept_content = ['json']  # Whitelist!

# Pro Task
@app.task(serializer='msgpack')
def my_task(data):
    pass

# Custom Serializer registrieren:
from kombu.serialization import register
register('custom', encode_fn, decode_fn, content_type='application/x-custom')
```

Seit Celery 3.2 wird Pickle per Default abgelehnt. Die `accept_content`-Whitelist kontrolliert akzeptierte Formate.

### Task-Definition-Patterns

**Decorator-Pattern (primaer):**
```python
@app.task
def add(x, y):
    return x + y

@app.task(bind=True, max_retries=3, rate_limit='10/m')
def fetch(self, url):
    pass
```

**Klassen-basiert (Custom Base Class):**
```python
class DatabaseTask(celery.Task):
    _db = None

    @property
    def db(self):
        if self._db is None:
            self._db = Database.connect()
        return self._db

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        log_to_sentry(exc)

    def on_success(self, retval, task_id, args, kwargs):
        audit_log(task_id, retval)

@app.task(base=DatabaseTask)
def process(record_id):
    pass
```

Task-Naming automatisch als `{module}.{function_name}` oder explizit via `name`-Parameter. Seit Celery 5.x gibt es Pydantic-Integration fuer automatische Argument-Validierung.

### Canvas/Workflow-System

Celerys Canvas bietet komposierbare Workflow-Primitives:

**Signature** — serialisierbarer Task-Aufruf:
```python
sig = add.s(2, 2)          # Mutable (empfaengt Ergebnis des Vorgaengers)
sig = add.si(2, 2)         # Immutable (ignoriert vorheriges Ergebnis)
```

**Chain** — sequentielle Ausfuehrung:
```python
from celery import chain
result = chain(add.s(2, 2), add.s(4), add.s(8))()
# 2+2=4 -> 4+4=8 -> 8+8=16
```

**Group** — parallele Ausfuehrung:
```python
from celery import group
result = group(add.s(i, i) for i in range(10))()
results = result.get()  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
```

**Chord** — parallele Ausfuehrung mit Callback:
```python
from celery import chord
result = chord(
    [add.s(i, i) for i in range(10)],
    xsum.s()  # Callback: Summe aller Ergebnisse
)()
```

**Chord-Implementierung intern:** Fuer Redis/Memcached/DynamoDB wird ein atomarer Counter inkrementiert; wenn er die Anzahl der Header-Tasks erreicht, wird der Callback ausgeloest. Fuer andere Backends pollt ein periodischer Task jede Sekunde.

**Starmap und Chunks** — Batch-Verarbeitung:
```python
add.starmap([(2, 2), (4, 4), (8, 8)])
add.chunks(zip(range(100), range(100)), 10)  # 10 Chunks a 10 Tasks
```

---

## 1.3 Performance & Skalierung

### Concurrency-Modelle

| Pool | Mechanismus | Best fuer | Concurrency | Memory |
|---|---|---|---|---|
| **Prefork** (Default) | multiprocessing, umgeht GIL | CPU-bound | = Anzahl CPUs | Hoch (Prozess-Kopien) |
| **Threads** | ThreadPoolExecutor | I/O-bound (moderate Last) | Begrenzt durch GIL | Mittel |
| **Gevent** | Greenlets (kooperativ) | I/O-bound (hohe Last) | 100-1000+ | Sehr niedrig |
| **Eventlet** | Greenlets (kooperativ) | I/O-bound (hohe Last) | 100-1000+ | Sehr niedrig |
| **Solo** | Inline, keine Parallelitaet | K8s Microservices | 1 | Minimal |

```bash
celery -A myapp worker --concurrency=4                    # Prefork (Default)
celery -A myapp worker --pool=gevent --concurrency=500    # Gevent
celery -A myapp worker --pool=solo                        # Solo
```

**Wichtig:** Default-Concurrency (CPU-Anzahl) ist fuer Gevent/Eventlet sinnlos — dort an I/O-Parallelitaet orientieren.

### Benchmark-Daten (Gevent vs. Prefork, 1000 I/O-bound Tasks je 500ms)

| Concurrency | Gevent | Prefork |
|---|---|---|
| 5 | 103s | 101s |
| 10 | 51s | 50s |
| 50 | 11s | 10s |
| 100 | 6.4s | 5.3s |
| 250 | **3.7s** | 4.1s |
| 500 | **3.8s** | 5.8s |

Gevent ist nicht schneller bei gleicher Concurrency, aber skaliert effizienter im Memory. Prefork bei Concurrency 500 = 500 Prozesse = Gigabytes RAM; Gevent braucht einen Bruchteil.

### Prefetch-Multiplier und Backpressure

```python
app.conf.worker_prefetch_multiplier = 4   # Default: 4 pro Worker-Slot
app.conf.worker_prefetch_multiplier = 1   # Faire Lastverteilung
app.conf.worker_prefetch_multiplier = 0   # Deaktiviert (nur mit acks_late=True)
```

**Problem mit Default (4):** Bei Tasks mit stark variierender Ausfuehrungszeit koennen Worker bis zu `4 * concurrency` Tasks horten. Ein Worker mit langem Task blockiert alle vorgeladenen kurzen Tasks, waehrend andere Worker idle sind.

### Horizontale Skalierung

- Mehrere Worker auf verschiedenen Maschinen, gleiche Queue
- Queue-basiertes Routing:
```python
app.conf.task_routes = {
    'app.tasks.cpu_heavy': {'queue': 'cpu'},
    'app.tasks.io_heavy': {'queue': 'io'},
}
```
- Separate Worker mit verschiedenen Pools:
```bash
celery -A myapp worker -Q cpu --pool=prefork --concurrency=4
celery -A myapp worker -Q io --pool=gevent --concurrency=500
```

### Bekannte Performance-Bottlenecks

1. **ETA/Countdown Tasks im Worker-Memory:** Geplante Tasks werden im Memory gehalten bis zur Ausfuehrung. Viele ETAs = Memory-Bloat.
2. **Gossip-Protokoll skaliert O(n^2):** Worker subscriben gegenseitig auf Events. Bei N Workern = quadratischer Netzwerk-Overhead. Empfehlung: deaktivieren.
3. **Chord-Polling bei non-Redis Backends:** Periodischer Task pollt jede Sekunde.
4. **Prefork Memory-Vervielfachung:** Jeder Child-Prozess ist ein Klon.
5. **Result Backend Polling:** Ausser Redis (Pub/Sub) nutzen alle Backends Polling fuer `result.get()`.

---

## 1.4 Developer Experience

### API-Design

**Staerken:**
- `task.delay(args)` als einfacher Shortcut
- `task.apply_async()` fuer volle Kontrolle
- `app.send_task('name', args)` fuer entkoppelte Aufrufe
- Canvas-DSL fuer Workflows

**Schwaechen:**
- Zwei APIs (`delay` vs `apply_async`) mit unterschiedlichen Signaturen = Verwirrung
- `bind=True` fuer `self.retry()` fuehlt sich wie Workaround an
- Konfiguration verteilt auf App-Instanz, Decorator, Task-Klasse, Umgebungsvariablen

### Typ-Sicherheit

Celery hat **keine native mypy-Unterstuetzung**. Der `@app.task`-Decorator veraendert die Funktionssignatur — aus einer Funktion wird ein `Task`-Objekt. Das Community-Paket `celery-types` bietet Type-Stubs mit Einschraenkungen. Return-Types von `delay()` sind generisch `AsyncResult`, nicht auf den Task-Return-Type typisiert.

### Testbarkeit

**Eager Mode (NICHT EMPFOHLEN):**
```python
app.conf.task_always_eager = True  # Synchron, umgeht Broker
```
Die offizielle Dokumentation raet selbst davon ab. Umgeht Serialisierung, Broker und Worker-Logik.

**Empfohlener Ansatz — Mocking:**
```python
def test_my_task():
    result = my_task("input")  # Direkt aufrufen
    assert result == "expected"
```

**Integration Tests mit pytest-celery:**
```python
@pytest.fixture
def celery_config():
    return {'broker_url': 'memory://', 'result_backend': 'cache+memory://'}

def test_task_integration(celery_worker):
    result = my_task.delay("input")
    assert result.get(timeout=10) == "expected"
```

### CLI und Monitoring

```bash
celery -A myapp worker --loglevel=info
celery -A myapp beat --scheduler django_celery_beat.schedulers:DatabaseScheduler
celery -A myapp inspect active       # Laufende Tasks
celery -A myapp inspect reserved     # Reservierte Tasks
celery -A myapp status               # Worker-Status
```

**Flower (Web-Monitoring):** Real-time via WebSocket, aber liest nicht aus dem Result Backend, verliert Daten bei Restart, keine aggregierten Views, kein Alerting.

### Konfigurationskomplexitaet

Ueber 100 Konfigurationsoptionen. Zwei Namenskonventionen (alt: `CELERY_BROKER_URL`, neu: `broker_url`).

### Haeufige Stolperfallen

1. Import-Reihenfolge: Vergessene Imports = "Task not registered"
2. Pickle-Sicherheit: Neulinge aktivieren Pickle ohne Bedenken
3. Visibility Timeout mit Redis: Langlaufende Tasks werden redelivered
4. `task_always_eager` versehentlich in Production
5. Fehlende Timeouts: Kein Default-Timeout = haengende Tasks
6. Chord-Deadlocks: Chords innerhalb von Chords erschoepfen Worker-Pool

---

## 1.5 Bekannte Schwaechen

1. **Kein nativer Async/Await-Support:** Workaround via `asyncio.run()` in jedem Task. Kein Connection-Pooling ueber Tasks.
2. **Keine Task-Level Concurrency Limits:** Nur pro Worker, nicht pro Task-Typ.
3. **Keine globalen Rate Limits:** `rate_limit='10/m'` gilt pro Worker. Bei 5 Workern = 50/min global.
4. **ETA Tasks Memory-Problem:** Im Worker-Memory bis zur Ausfuehrung.
5. **Gossip O(n^2):** Muss in Produktion deaktiviert werden.
6. **Fragmentierte Observability:** Flower verliert Daten, kein natives Alerting.
7. **Kombu als separate Library:** Versionsinkompatibilitaeten. Zwei Projekte zu maintainen.
8. **Prefork als Default-Pool:** Suboptimal fuer I/O-lastige Workloads.
9. **100+ Settings:** Konfigurationsexplosion.
10. **Ueber 700 offene Issues:** Langsame Release-Zyklen.

---

# 2. TaskIQ

**Version:** aktuell | **Stars:** ~2.000 | **Lizenz:** MIT | **Python:** 3.9+
**Kernprinzip:** Async-first, Broker-agnostisch, FastAPI-inspiriert

## 2.1 Ausfallsicherheit & Reliability

### Message-Garantien

TaskIQ implementiert **keine eigene Message-Garantie-Schicht**, sondern delegiert vollstaendig an den jeweiligen Broker:

- **RedisStreamBroker** (At-least-once): Redis Streams mit Consumer Groups. `xack` nach Verarbeitung. Unbearbeitete Nachrichten via `xautoclaim` nach konfigurierbarem `idle_timeout` (Standard: 10 Min) automatisch einem anderen Consumer zugewiesen.
- **ListQueueBroker** (At-most-once): `lpush`/`brpop` — Nachricht bei Lesen aus Liste entfernt, bei Crash verloren.
- **PubSubBroker** (Fire-and-forget): Keine Persistenz, nur Broadcasting.
- **AioPikaBroker** (RabbitMQ): AMQP-native Ack via `aio-pika`, Persistent Delivery.

### Dreistufiges Acknowledgment-System

Einer der durchdachtesten Teile von TaskIQ. Drei konfigurierbare Ack-Zeitpunkte:

```python
class AcknowledgeType(str, Enum):
    WHEN_RECEIVED = "when_received"   # Sofort nach Empfang, vor Ausfuehrung
    WHEN_EXECUTED = "when_executed"    # Nach Ausfuehrung, vor Result-Speicherung
    WHEN_SAVED = "when_saved"         # Nach Result-Speicherung (Standard)
```

Implementation im Receiver:
```python
# Checkpoint 1: Sofort nach Empfang
if self.ack_time == AcknowledgeType.WHEN_RECEIVED and isinstance(message, AckableMessage):
    await maybe_awaitable(message.ack())

result = await self.run_task(target=task.original_func, message=taskiq_msg)

# Checkpoint 2: Nach Ausfuehrung
if self.ack_time == AcknowledgeType.WHEN_EXECUTED and isinstance(message, AckableMessage):
    await maybe_awaitable(message.ack())

# ... post_execute middleware, result speichern ...

# Checkpoint 3: Nach Speicherung
if self.ack_time == AcknowledgeType.WHEN_SAVED and isinstance(message, AckableMessage):
    await maybe_awaitable(message.ack())
```

**Kritische Schwaeche:** Kein `nack`/`reject` im Core-Interface. `AckableMessage` hat nur `ack`, kein `nack`. Fehlgeschlagene Nachrichten werden trotzdem geackt. Kein nativer Weg, eine Nachricht zurueck in die Queue zu stellen (GitHub Issue #578).

### Retry-Mechanismen

**SimpleRetryMiddleware:** Sofortiger Retry durch Re-Kick. Counter als Label `_retries`:
```python
@broker.task(retry_on_error=True, max_retries=5)
async def my_task():
    ...
```

**SmartRetryMiddleware:** Delay (Standard 5s), Exponential Backoff (`delay * retries`, gedeckelt), Jitter, optionale ScheduleSource-Integration.

Beide nutzen denselben Mechanismus: Bei Fehler wird ein **neuer Task** gesendet (re-kick), nicht die Original-Nachricht zurueckgestellt.

### Dead Letter Queue

**Kein natives DLQ-Handling.** AioPikaBroker unterstuetzt Queue-Level DLQ-Parameter, aber der Core ackt Nachrichten bedingungslos — fehlgeschlagene Nachrichten werden nie in die DLQ geroutet (Issue #578).

### Graceful Shutdown

Zweistufig:
- **Soft Kill:** SIGINT/SIGTERM/SIGHUP setzen `shutdown_event`. Prefetcher stoppt, Runner wartet mit `asyncio.wait(tasks, timeout=wait_tasks_timeout)`.
- **Hard Kill:** Wiederholte Signale (konfigurierbar via `--hardkill-count`) werfen `KeyboardInterrupt`.

ProcessManager erkennt tote Worker-Prozesse (jede Sekunde geprueft) und startet automatisch neu. `--max-fails` begrenzt Neustarts.

---

## 2.2 Architektur & Erweiterbarkeit

### Gesamtarchitektur — Komponentenmodell

```
Client-Seite:                      Worker-Seite:

@broker.task() -> Decorator        CLI: taskiq worker broker:app
      |                                  |
AsyncTaskiqDecoratedTask           ProcessManager
      |                                  |
  .kiq() -> AsyncKicker            N x Process (start_listen)
      |                                  |
  pre_send Middleware              Receiver
      |                                  |
  Formatter.dumps()                Broker.listen()
      |                                  |
  Broker.kick()  ----[Queue]---->  Formatter.loads()
      |                                  |
  post_send Middleware             pre_execute Middleware
                                         |
                                   DependencyGraph.resolve()
                                         |
                                   run_task() (async/sync)
                                         |
                                   post_execute Middleware
                                         |
                                   ResultBackend.set_result()
                                         |
                                   post_save Middleware
```

### Broker-Abstraktion (vorbildlich minimalistisch)

Nur zwei abstrakte Methoden:

```python
class AsyncBroker(ABC):
    @abstractmethod
    async def kick(self, message: BrokerMessage) -> None:
        """Sendet eine Nachricht an Worker."""

    @abstractmethod
    def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """Liefert eingehende Nachrichten als async Generator."""
```

`listen()` kann `bytes` (kein Ack-Support) oder `AckableMessage` (mit Ack) yielden. Diese Union-Type-Entscheidung ist elegant.

Fluent-Configuration:
```python
broker = RedisStreamBroker("redis://localhost") \
    .with_result_backend(RedisAsyncResultBackend("redis://localhost")) \
    .with_middlewares(SimpleRetryMiddleware(default_retry_count=3)) \
    .with_serializer(ORJSONSerializer()) \
    .with_formatter(JSONFormatter())
```

### Result-Backend-Abstraktion

```python
class AsyncResultBackend(ABC, Generic[_ReturnType]):
    @abstractmethod
    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None: ...

    @abstractmethod
    async def is_result_ready(self, task_id: str) -> bool: ...

    @abstractmethod
    async def get_result(self, task_id: str, with_logs: bool = False) -> TaskiqResult[_ReturnType]: ...

    # Optional:
    async def set_progress(self, task_id: str, progress: TaskProgress[Any]) -> None: ...
    async def get_progress(self, task_id: str) -> TaskProgress[Any] | None: ...
```

Default ist `DummyResultBackend` (verwirft alles) — bewusster Design-Entscheid.

### Middleware-System (8 Hooks)

| Hook | Seite | Richtung | Return |
|------|-------|----------|--------|
| `startup()` | Beide | — | None |
| `shutdown()` | Beide | — | None |
| `pre_send(msg)` | Client | Forward | TaskiqMessage (modifizierbar) |
| `post_send(msg)` | Client | Forward | None |
| `pre_execute(msg)` | Worker | Forward | TaskiqMessage (modifizierbar) |
| `post_execute(msg, result)` | Worker | **Reversed** | None |
| `post_save(msg, result)` | Worker | **Reversed** | None |
| `on_error(msg, result, exc)` | Worker | **Reversed** | None |

`post_execute`, `post_save` und `on_error` werden in **umgekehrter Reihenfolge** aufgerufen (Stack-Semantik).

Clevere Optimierung: Hooks werden nur aufgerufen wenn die Subklasse sie tatsaechlich ueberschreibt:
```python
if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
    taskiq_msg = await maybe_awaitable(middleware.pre_execute(taskiq_msg))
```

Alle Hooks unterstuetzen sync und async via `maybe_awaitable()`.

### Zweischichtige Serialisierung

**Schicht 1 — Serializer** (`TaskiqSerializer`): Python-Objekte zu `bytes`.
- `JSONSerializer` (Standard), `ORJSONSerializer`, `MSGPackSerializer`, `CBORSerializer`, `PickleSerializer`

**Schicht 2 — Formatter** (`TaskiqFormatter`): `TaskiqMessage` zu `BrokerMessage`.
```python
class JSONFormatter(TaskiqFormatter):
    def dumps(self, message: TaskiqMessage) -> BrokerMessage:
        return BrokerMessage(
            task_id=message.task_id, task_name=message.task_name,
            message=model_dump_json(message).encode(), labels=message.labels,
        )
    def loads(self, message: bytes) -> TaskiqMessage:
        return model_validate_json(TaskiqMessage, message)
```

### Task-Definition mit voller Type-Preservation

Decorator-basiert mit `ParamSpec` und `TypeVar`:
```python
class AsyncTaskiqDecoratedTask(Generic[_FuncParams, _ReturnType]):
    ...
```

Drei `@overload`-Varianten fuer `kiq()` stellen korrekten Return-Typ sicher — auch bei Coroutines.

### Dependency Injection (taskiq-dependencies)

FastAPI-inspiriert, unabhaengig nutzbar:
- `TaskiqDepends()` als Marker
- Generator-Dependencies fuer Setup/Teardown
- Caching innerhalb einer Task-Ausfuehrung
- `Annotated`-Syntax fuer Python 3.10+
- Dependency-Graph beim Startup einmalig berechnet
- Eingebaute Dependencies: `Context` (Task-Metadaten), `TaskiqState` (globaler Zustand)

### Plugin-Oekosystem

Offiziell gepflegte Plugins:
- **taskiq-redis**: Redis-Broker (PubSub, List, Stream) und Result-Backend
- **taskiq-aio-pika**: RabbitMQ/AMQP-Broker
- **taskiq-fastapi**: FastAPI-Integration
- **taskiq-faststream**: FastStream-Integration
- **taskiq-pipelines**: Task-Verkettung
- **taskiq-admin**: Dashboard/Admin-UI
- **taskiq-psqlpy**: PostgreSQL-Backend

---

## 2.3 Performance & Skalierung

### Async-First-Architektur

Konsequent auf `asyncio` aufgebaut, nutzt `anyio` als Abstraktionsschicht. `uvloop` wird automatisch verwendet wenn installiert:
```python
if uvloop is not None:
    loop = uvloop.new_event_loop()
else:
    loop = asyncio.new_event_loop()
```

Synchrone Tasks via `loop.run_in_executor()` mit `contextvars.copy_context()`.

### Prefetcher/Runner-Pattern mit Semaphoren

1. **Prefetcher-Coroutine:** Liest vom Broker in `asyncio.Queue`, begrenzt durch `sem_prefetch`
2. **Runner-Coroutine:** Spawnt `asyncio.Task`-Instanzen, begrenzt durch `sem` (max_async_tasks)

Beide in `anyio.create_task_group()`. Done-Callback-Pattern verhindert GC-bedingte Task-Cancellation:
```python
task = asyncio.create_task(self.callback(message=message, raise_err=False))
tasks.add(task)
task.add_done_callback(task_cb)
```

### Worker-Architektur

```
Main Process (ProcessManager)
    |-- Worker-0 (Process) -> asyncio Event-Loop -> Receiver (prefetcher + runner)
    |-- Worker-1 (Process) -> asyncio Event-Loop -> Receiver (prefetcher + runner)
    |-- Worker-N (Process) -> asyncio Event-Loop -> Receiver (prefetcher + runner)
```

Konfiguration: `--workers N`, `--max-async-tasks N`, `--max-prefetch N`, `--max-tasks-per-child N`, `--use-process-pool`

### Benchmark (20.000 No-Op Jobs, 10 Worker, Redis)

| Framework | Zeit (Sekunden) | Faktor vs. TaskIQ |
|-----------|-----------------|-------------------|
| **TaskIQ** | **2,03** | **1,0x (Referenz)** |
| Huey (Processes) | 3,62 | 1,78x |
| Dramatiq (Threads) | 4,12 | 2,03x |
| Celery (Threads) | 11,68 | 5,75x |
| Celery (Processes) | 17,60 | 8,67x |
| arq | 35,37 | 17,42x |

TaskIQ ist der schnellste — fast 6x schneller als Celery, 2x schneller als Dramatiq.

---

## 2.4 Developer Experience

### API-Design

```python
from taskiq_redis import RedisStreamBroker

broker = RedisStreamBroker("redis://localhost")

@broker.task
async def add(a: int, b: int) -> int:
    return a + b

task = await add.kiq(1, 2)
result = await task.wait_result(timeout=5.0)
print(result.return_value)  # 3
```

Fluent Kicker-API:
```python
result = await add.kicker() \
    .with_labels(priority=5, queue_name="high") \
    .with_task_id("custom-id") \
    .with_broker(other_broker) \
    .kiq(1, 2)
```

### Typ-Sicherheit (hervorragend)

- `ParamSpec`/`TypeVar`-Generics auf `AsyncTaskiqDecoratedTask`
- `@overload`-Dekorationen fuer `kiq()` mit Coroutine-Type-Extraction
- `Generic[_ReturnType]` auf `AsyncTaskiqTask`, `TaskiqResult`, `AsyncResultBackend`
- Pydantic-Modelle fuer alle Nachrichten-Typen

### Testbarkeit (InMemoryBroker)

```python
from taskiq import InMemoryBroker

broker = InMemoryBroker()

@broker.task
async def my_task(x: int) -> int:
    return x * 2

async def test_my_task():
    task = await my_task.kiq(5)
    await broker.wait_all()
    result = await task.wait_result()
    assert result.return_value == 10
```

### CLI

`taskiq worker` und `taskiq scheduler` mit 20+ Flags: `--ack-type`, `--reload`, `-fsd` (Auto-Discovery), `--max-tasks-per-child`, `--receiver`

---

## 2.5 Bekannte Schwaechen

1. **Fehlendes nack/reject:** Groesstes architektonisches Defizit. Kein DLQ-Routing moeglich.
2. **Synchrones Senden nicht moeglich:** `kiq()` ist immer `async`. Fuer Django/sync-Code braucht man `asyncio.run()`.
3. **Kein nativer Task-Prioritaets-Mechanismus:** Labels nur weitergereicht, keine Prioritaets-Queue.
4. **Polling-basiertes Result-Waiting:** `wait_result()` nutzt `asyncio.sleep(check_interval)` statt Push.
5. **Kleine Community:** ~2.000 Stars, <10 Core-Contributors, <$900 Sponsors.
6. **Timeout-Problem bei Sync Tasks:** Nur Warning im Log.
7. **Kein Canvas/Workflow-System:** `taskiq-pipelines` existiert, aber unreif.
8. **Kein Rate-Limiter, keine Task-Revocation, kein Multi-Tenant-Support.**
9. **Kein Config-File-Support:** Nur programmatisch.

---

# 3. Dramatiq

**Version:** 2.1.0 (Maerz 2026) | **Stars:** 4.650 | **Lizenz:** LGPL-3.0 | **Python:** 3.10+
**Kernprinzip:** Simplicity-first, "not Celery", hohe Zuverlaessigkeit

## 3.1 Ausfallsicherheit & Reliability

### Message-Garantien: At-Least-Once (hardcoded)

Dramatiq implementiert konsequent **at-least-once Delivery**. Acknowledge **ausschliesslich nach erfolgreicher Verarbeitung**. Nicht konfigurierbar — bewusste Designentscheidung.

Konsequenz: Alle Actors muessen **idempotent** sein.

### Retry-Mechanismen

Ueber `Retries`-Middleware mit Defaults:

| Parameter | Default | Beschreibung |
|---|---|---|
| `max_retries` | 20 | Maximale Versuche |
| `min_backoff` | 15.000 ms (15s) | Minimale Wartezeit |
| `max_backoff` | 604.800.000 ms (7 Tage) | Maximale Wartezeit |
| `retry_when` | None | Praedikat-Funktion |
| `throws` | None | Exceptions die nicht retried werden |

Backoff-Formel: `delay = min(max_backoff, min_backoff * (2 ** retry_number))`

```python
@dramatiq.actor(max_retries=3)
def fragile_task(url):
    response = requests.get(url)
    response.raise_for_status()

# Bedingte Retries
def should_retry(retries_so_far, exception):
    return retries_so_far < 3 and isinstance(exception, HttpTimeout)

@dramatiq.actor(retry_when=should_retry)
def conditional_task(url):
    ...

# Bestimmte Exceptions ausschliessen
@dramatiq.actor(throws=(ValueError, ValidationError))
def validated_task(data):
    ...
```

Ab v1.17.1: `on_retries_exhausted` Callback-Actor wenn alle Retries erschoepft.

### Dead Letter Queue (erstklassig)

Wenn alle Retries oder Alters-Limit ueberschritten: automatisch in DLQ verschoben.

- **RabbitMQ:** Jede Queue `foo` erzeugt automatisch `foo.XQ` als DLQ
- **Redis:** DLQ-Nachrichten mit konfigurierbarer TTL (Default 7 Tage)

Dreischritt der Message-Verarbeitung:
1. **ack** — erfolgreich verarbeitet, aus Queue entfernt
2. **nack** — fehlgeschlagen, in Delayed Queue mit Backoff verschoben
3. **skip** — Middleware hat `SkipMessage` geworfen, geackt ohne Ausfuehrung

### Graceful Shutdown

| Signal | Verhalten |
|---|---|
| `SIGTERM` / `SIGINT` | Graceful: Consumer stoppen, Worker beenden laufende Tasks |
| Zweites `SIGTERM` | Erzwungener Abbruch |
| `SIGHUP` | Graceful Shutdown + Worker-Reload |

**ShutdownNotifications-Middleware:** Wirft `Shutdown`-Exception in laufende Tasks:
```python
@dramatiq.actor(max_retries=3, notify_shutdown=True)
def long_running_task():
    try:
        setup_missiles()
        do_work()
    except Shutdown:
        cleanup_missiles()
        raise  # Re-raise fuer Retry
```

Exit-Codes: 0 (Graceful), 1 (Killed), 2 (Import-Fehler), 3 (Broker-Fehler), 4 (PID-Konflikt)

### Worker-Crash Recovery

- Nicht-geackte Nachrichten werden vom Broker nach Visibility Timeout redelivered
- RabbitMQ: Automatisches Requeue bei Connection-Verlust
- Redis: `heartbeat_timeout=60000` (60s) — nach 60s ohne Heartbeat gelten Nachrichten als verwaist
- Bei Graceful Shutdown: ungefetchte Nachrichten werden zurueck in Queue geschoben (requeue)

### Message Persistence

Nachrichten sind persistent auf Disk und ueberleben Broker-Neustarts (RabbitMQ: persistent Messages; Redis: abhaengig von AOF/RDB).

---

## 3.2 Architektur & Erweiterbarkeit

### Gesamtarchitektur

```
@dramatiq.actor --> Actor-Registrierung beim Broker
    |
actor.send() --> Message-Objekt --> Broker.enqueue()
    |                                  |
Message (queue_name, actor_name,   Middleware Pipeline
         args, kwargs, options,    (before_enqueue, after_enqueue)
         message_id, timestamp)         |
                                   Message Broker Backend
                                   (RabbitMQ / Redis / Stub)
                                        |
                                   Consumer Thread (pull)
                                        |
                                   Worker Thread Pool
                                   (before_process_message)
                                        |
                                   Actor-Funktion ausfuehren
                                        |
                                   (after_process_message)
                                        |
                                   ack() / nack() / skip()
```

**Queue-Struktur bei RabbitMQ (3 Queues pro logische Queue):**
- `foo` — Haupt-Queue fuer sofortige Verarbeitung
- `foo.DQ` — Delayed Queue fuer Retries
- `foo.XQ` — Dead Letter Queue

### Broker-Abstraktion

```python
class Broker:
    def declare_actor(self, actor: Actor) -> None: ...
    def declare_queue(self, queue_name: str) -> None: ...
    def enqueue(self, message: Message, *, delay=None) -> Message: ...
    def consume(self, queue_name: str, prefetch=1, timeout=30000) -> Consumer: ...
    def get_actor(self, actor_name: str) -> Actor: ...
    def flush(self, queue_name: str) -> None: ...
    def join(self, queue_name: str, *, timeout=None) -> None: ...
    def add_middleware(self, middleware, *, before=None, after=None) -> None: ...
    def close(self) -> None: ...
```

Drei Implementierungen:

**RabbitmqBroker:** `confirm_delivery`, `max_priority`, HA-Cluster-Support, VHosts
```python
broker = RabbitmqBroker(url=['amqp://node1.foo.net', 'amqp://node2.foo.net'])
```

**RedisBroker:** `namespace`, `maintenance_chance`, `heartbeat_timeout`, `dead_message_ttl`
```
HSET dramatiq:default.msgs $ID $PAYLOAD
RPUSH dramatiq:default $ID
```

**StubBroker:** Test-Broker. `fail_fast_default=True` (v2.0.0+), `dead_letters` Property.

### Middleware-System (Kernstueck — 20+ Hooks)

```python
class Middleware:
    # Properties
    @property
    def actor_options(self) -> set[str]: ...
    @property
    def forks(self) -> list[Callable]: ...

    # Message-Lifecycle
    def before_enqueue(self, broker, message, delay): ...
    def after_enqueue(self, broker, message, delay): ...
    def before_delay_message(self, broker, message): ...
    def before_process_message(self, broker, message): ...   # kann SkipMessage werfen
    def after_process_message(self, broker, message, *, result=None, exception=None): ...
    def after_skip_message(self, broker, message): ...

    # Acknowledgment-Lifecycle
    def before_ack(self, broker, message): ...
    def after_ack(self, broker, message): ...
    def before_nack(self, broker, message): ...
    def after_nack(self, broker, message): ...

    # Declaration-Lifecycle
    def before_declare_actor(self, broker, actor): ...
    def after_declare_actor(self, broker, actor): ...
    def before_declare_queue(self, broker, queue_name): ...
    def after_declare_queue(self, broker, queue_name): ...
    def after_declare_delay_queue(self, broker, queue_name): ...

    # Worker-Lifecycle
    def after_process_boot(self, broker): ...
    def before_worker_boot(self, broker, worker): ...
    def after_worker_boot(self, broker, worker): ...
    def before_worker_shutdown(self, broker, worker): ...
    def after_worker_shutdown(self, broker, worker): ...

    # Thread-Lifecycle
    def after_consumer_thread_boot(self, broker, thread): ...
    def before_consumer_thread_shutdown(self, broker, thread): ...
    def after_worker_thread_boot(self, broker, thread): ...
    def before_worker_thread_shutdown(self, broker, thread): ...
```

**Built-in Middlewares (Default-Stack):**

| Middleware | Funktion |
|---|---|
| `AgeLimit` | Verwirft zu alte Nachrichten |
| `TimeLimit` | Ausfuehrungszeitlimit (Default: 10 Min), wirft `TimeLimitExceeded` |
| `ShutdownNotifications` | Benachrichtigt Tasks ueber Worker-Shutdown |
| `Retries` | Automatische Retries mit Backoff |
| `Callbacks` | Success/Failure Callbacks |
| `Pipelines` | Message-Chaining |
| `CurrentMessage` | Zugriff auf aktuelle Message im Actor |

**Manuell hinzuzufuegen:**
- `Results` — Result-Backend
- `AsyncIO` — Async-Actor-Support
- `Prometheus` — Metriken
- `GroupCallbacks` — Completion-Callbacks

**Middleware-Reihenfolge steuern:**
```python
broker.add_middleware(MyMiddleware(), before=Retries)
broker.add_middleware(MyMiddleware(), after=TimeLimit)
```

Jede Middleware kann eigene `actor_options` definieren — so erweitert z.B. `Retries` den Decorator um `max_retries`, `retry_when`, etc.

### Serialisierung

```python
# Default: JSON
from dramatiq import JSONEncoder
dramatiq.set_encoder(JSONEncoder())

# Custom Encoder
class MsgPackEncoder(dramatiq.Encoder):
    def encode(self, data: dict) -> bytes: ...
    def decode(self, data: bytes) -> dict: ...
```

### Actor-Definition-Patterns

**Funktions-basiert:**
```python
@dramatiq.actor(queue_name="high-priority", priority=0, max_retries=5)
def process_order(order_id):
    ...
```

**Klassen-basiert (GenericActor):**
```python
class ProcessOrder(dramatiq.GenericActor):
    class Meta:
        queue_name = "orders"
        max_retries = 3

    def perform(self, order_id):
        ...
```

### Message Pipeline und Composition

**Pipeline (sequentiell):**
```python
pipe = get_uri_contents.message(uri) | count_words.message(uri)
result = pipe.get_result(block=True, timeout=5000)
```

**Group (parallel):**
```python
g = group([frobnicate.message(1, 2), frobnicate.message(2, 3)]).run()
g.wait(timeout=10_000)
for res in g.get_results(block=True, timeout=5_000):
    print(res)
```

**Completion-Callbacks:**
```python
g = group([...])
g.add_completion_callback(notify_complete.message())
g.run()
```

### Rate Limiting (distributed)

```python
from dramatiq.rate_limits import ConcurrentRateLimiter
from dramatiq.rate_limits.backends import RedisBackend

backend = RedisBackend()
MUTEX = ConcurrentRateLimiter(backend, "distributed-mutex", limit=1)

@dramatiq.actor
def one_at_a_time():
    with MUTEX.acquire():
        do_exclusive_work()
```

### Priority Queues

```python
@dramatiq.actor(priority=0)   # Hoechste Prioritaet
def critical_task(): ...

@dramatiq.actor(priority=100)  # Niedrigste
def background_task(): ...
```

Prioritaet wirkt nur innerhalb des Worker-Thread-Pools, nicht auf Broker-Ebene.

---

## 3.3 Performance & Skalierung

### Threading-basierte Architektur (kein async)

- Master-Prozess spawnt N Worker-Prozesse (Default: CPU-Kerne)
- Jeder Worker hat zwei Thread-Pools:
  - **Consumer-Threads:** Ziehen Nachrichten vom Broker
  - **Worker-Threads:** Fuehren Actors aus (Default: 8 pro Prozess)
- Dazwischen: **PriorityQueue** als Backpressure-Puffer

```bash
dramatiq app --processes 4 --threads 8
```

### Benchmark (20.000 No-Op Jobs, 10 Worker, Redis)

| Library | Konfiguration | Zeit |
|---|---|---|
| TaskIQ | — | 2,03s |
| **Dramatiq** | **Threads** | **4,12s** |
| **Dramatiq** | **Processes** | **4,35s** |
| Celery | Threads | 11,68s |
| arq | — | 35,37s |

Dramatiq ist ~2,8x schneller als Celery (Threads), ~4x schneller als Celery (Processes).

### Prometheus-Metriken

- `dramatiq_messages_total`, `dramatiq_message_errors_total`, `dramatiq_message_retries_total`
- `dramatiq_message_rejects_total`, `dramatiq_messages_inprogress`
- `dramatiq_message_duration_milliseconds` (Histogram)
- Alle mit `queue_name` und `actor_name` Labels. Grafana Dashboard ID 3692.

### Gevent-Integration

```bash
dramatiq-gevent app -p 8 -t 250  # = 8 Prozesse x 250 Greenlets = 2000 "Threads"
```

Warnung ab v2.1.0: Bei free-threaded Python (3.13+) reaktiviert gevent den GIL.

---

## 3.4 Developer Experience

### API-Design ("Not Celery")

Bewusst minimalistisch:
```python
import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker(host="rabbitmq")
dramatiq.set_broker(broker)

@dramatiq.actor
def send_email(to, subject, body):
    ...

send_email.send("user@example.com", "Hello", "World")
send_email.send_with_options(
    args=("user@example.com", "Hello", "World"),
    delay=10_000,
)
```

Kein `app`-Objekt, kein `bind`, kein `task_always_eager`.

### Testbarkeit (StubBroker — exzellent)

```python
@pytest.fixture
def broker():
    broker = StubBroker()
    broker.emit_after("process_boot")
    dramatiq.set_broker(broker)
    yield broker
    broker.flush_all()

@pytest.fixture
def worker(broker):
    worker = dramatiq.Worker(broker, worker_timeout=100)
    worker.start()
    yield worker
    worker.stop()

def test_send_email(broker, worker):
    send_email.send("user@example.com", "Test", "Body")
    broker.join(send_email.queue_name)
    worker.join()
```

`fail_fast=True` (v2.0.0 Default): Fehlgeschlagene Nachrichten werfen automatisch Exceptions im Test.

### Cross-Language Interop

Nachrichten-Format:
```json
{
    "queue_name": "default",
    "actor_name": "add",
    "args": [1, 2],
    "kwargs": {},
    "options": {},
    "message_id": "unique-uuid",
    "message_timestamp": 1713100000000
}
```

---

## 3.5 Bekannte Schwaechen

1. **Kein nativer Async-Support:** `AsyncIO`-Middleware laeuft async Actors in einem Event-Loop-Thread, aber Concurrency bleibt = Worker-Thread-Anzahl. Kein Gewinn an Parallelitaet.
2. **Fehlendes Canvas-System:** Nur `pipeline` und `group`, keine verschachtelten Workflows, kein `chord`.
3. **Result-Backend beschraenkt:** Nur Redis/Memcached, ephemer (TTL), kein Progress-Tracking.
4. **Kleinere Community:** 62.763 Downloads/Woche vs. Celery Millionen.
5. **Ein-Maintainer-Projekt:** Bus-Faktor-Risiko (Bogdan Popa).
6. **Kein nativer Scheduler:** Braucht APScheduler oder `periodiq`.
7. **dramatiq-dashboard:** Nicht mehr aktiv gepflegt.

---

# 4. arq

**Version:** 0.27.0 (Januar 2026) | **Stars:** ~2.000 | **Lizenz:** MIT | **Python:** 3.8+
**Status:** Maintenance-only (Issue #510, keine neuen Features)
**Kernprinzip:** Async-native, minimalistisch, Redis-only

## 4.1 Ausfallsicherheit & Reliability

### Message-Garantien: At-Most-Once (mit best-effort Recovery)

Enqueue mit WATCH/MULTI-Transaction:
```python
async with self.pipeline(transaction=True) as pipe:
    await pipe.watch(job_key)
    if await pipe.exists(job_key, result_key_prefix + job_id):
        await pipe.reset()
        return None  # Job existiert bereits

    pipe.multi()
    pipe.psetex(job_key, expires_ms, job)
    pipe.zadd(_queue_name, {job_id: score})
    await pipe.execute()
```

Job in zwei Redis-Strukturen:
- `arq:job:<job_id>` — String-Key mit TTL (Job-Daten)
- `arq:queue` — Sorted Set (job_id als Member, Timestamp als Score)

**Kein Recovery-Mechanismus** fuer verwaiste Jobs bei Worker-Crash nach Dequeue aber vor Ergebnis-Speicherung.

### Retry-Mechanismus

`Retry`-Exception-Klasse:
```python
class Retry(RuntimeError):
    def __init__(self, defer: Optional['SecondsTimedelta'] = None):
        self.defer_score: Optional[int] = to_ms(defer)
```

Default: `max_tries=5`. **Kein exponentielles Backoff eingebaut** — Defer-Wert muss manuell gesetzt werden.

Retry-Counter via separatem Redis-Key `arq:retry:<job_id>` (TTL 24.5h).

### Job-Timeout-Handling

Via `asyncio.wait_for`:
```python
result = await asyncio.wait_for(task, timeout_s)
```
Default: 300s. Bei Timeout: `asyncio.TimeoutError`, automatischer Retry bei `retry_jobs=True`.

### Graceful Shutdown

Zwei Strategien:
1. **Sofortiges Abbrechen:** Alle Tasks werden `cancel()`-t
2. **Warten auf Completion:** `allow_pick_jobs=False`, wartet `job_completion_wait` Sekunden, dann Cancel. Fuer Kubernetes konzipiert.

### Worker-Crash-Verhalten

1. `in_progress`-Key (`arq:in-progress:<job_id>`) hat TTL `max_timeout + 10s`
2. Job bleibt im Sorted Set
3. **Kein automatischer Recovery:** Kein anderer Worker nimmt den Job auf. Erst sichtbar wenn in_progress-Key ablaeuft.
4. **Kritisch:** Wenn `arq:job:<job_id>` vor `arq:in-progress:<job_id>` ablaeuft, ist der Job endgueltig verloren.

### Health Checks

```python
info = f'{datetime.now():%b-%d %H:%M:%S} j_complete={self.jobs_complete} ...'
await self.pool.psetex(self.health_check_key, int((self.health_check_interval + 1) * 1000), info.encode())
```

Periodisch (Default: 3600s) Key mit TTL in Redis. `check_health` prueft Existenz — Exit-Code 0/1. Kubernetes-Liveness-Probe-ready.

### Job-Deduplication

Eingebaut. Bei expliziter `_job_id`: kein zweiter Job mit gleicher ID solange Job-Key ODER Result-Key existiert. Ohne explizite ID: UUID generiert, keine automatische Dedup.

### Heartbeat

**Kein echtes Heartbeat.** `in_progress`-Key hat feste TTL, wird waehrend Ausfuehrung **nicht erneuert**. Langlaufende Jobs koennen parallel doppelt gestartet werden.

---

## 4.2 Architektur & Erweiterbarkeit

### Redis-Datenstrukturen

| Key-Pattern | Typ | Zweck | TTL |
|---|---|---|---|
| `arq:queue` | Sorted Set | Job-Queue, Score = Ausfuehrungszeitpunkt (ms) | Kein TTL |
| `arq:job:<id>` | String | Serialisierte Job-Definition | `expires_ms` (24h + defer) |
| `arq:in-progress:<id>` | String | Lock-Key "1" | `max_timeout + 10s` |
| `arq:result:<id>` | String | Serialisiertes Ergebnis | `keep_result` (3600s) |
| `arq:retry:<id>` | String | Retry-Counter | 88400s (~24.5h) |
| `arq:abort` | Sorted Set | Job-IDs zum Abbrechen | Auto-Bereinigung |
| `arq:queue:health-check` | String | Health-Check-Daten | `health_check_interval + 1s` |

**Architektur der Redis-Interaktionen:**
```
ENQUEUE:                          DEQUEUE (Worker-Poll):
  WATCH arq:job:<id>              ZRANGEBYSCORE arq:queue -inf <now_ms>
  EXISTS arq:job:<id>             Fuer jeden Job:
  EXISTS arq:result:<id>            WATCH arq:in-progress:<id>
  MULTI                             EXISTS arq:in-progress:<id>
    PSETEX arq:job:<id>             MULTI
    ZADD arq:queue                    PSETEX arq:in-progress:<id>
  EXEC                              EXEC -> asyncio.create_task(run_job)

JOB FINISH:                       JOB FAIL:
  PIPELINE:                         PIPELINE:
    DEL arq:in-progress:<id>          DEL arq:retry:<id>
    SET arq:result:<id>               DEL arq:in-progress:<id>
    DEL arq:retry:<id>                DEL arq:job:<id>
    DEL arq:job:<id>                  ZREM arq:abort <id>
    ZREM arq:abort <id>               ZREM arq:queue <id>
    ZREM arq:queue <id>               SET arq:result:<id>
```

### Redis als einziger Broker+Backend

**Vorteile:** Einfaches Setup, Sorted Set liefert zeitbasierte Priorisierung kostenlos, WATCH/MULTI fuer optimistisches Locking, Sentinel-Support eingebaut.

**Nachteile:** Keine nativen Message-Garantien, Memory-limitiert, kein Fan-Out, kein Multi-Broker, Redis-Ausfall = alles steht.

### WorkerSettings-Klasse

```python
class WorkerSettings:
    functions = [func(foobar, name='foobar')]
    redis_settings = RedisSettings(host='redis')
    max_jobs = 20
    cron_jobs = [cron(cleanup, hour=3, minute=0)]
    on_startup = startup_handler
    on_shutdown = shutdown_handler
```

Settings werden via `get_kwargs` aus Attributen extrahiert. **Keine Validierung** — falsche Attribute werden stillschweigend ignoriert.

### Job-Definition

Einfache async-Funktionen mit `ctx`-Dict:
```python
async def send_email(ctx, email: str, subject: str) -> bool:
    redis = ctx['redis']
    job_id = ctx['job_id']
    job_try = ctx['job_try']
    return True
```

### Serialisierung (Pickle Default — Sicherheitsrisiko)

```python
def serialize_job(..., serializer=None):
    data = {'t': job_try, 'f': function_name, 'a': args, 'k': kwargs, 'et': enqueue_time_ms}
    if serializer is None:
        serializer = pickle.dumps
    return serializer(data)
```

Custom Serializer moeglich:
```python
redis_ = ArqRedis(
    job_serializer=msgpack.packb,
    job_deserializer=functools.partial(msgpack.unpackb, raw=False),
)
```

### Cron-Job-Support

Worker reiht Cron-Jobs selbst ein. `unique=True` (Default) generiert deterministische Job-ID: `{name}:{unix_ms_of_next_run}`. Verhindert Doppelausfuehrung bei mehreren Workern. **Problem:** Verpasste Cron-Jobs werden nicht nachgeholt.

### Startup/Shutdown Hooks

1. `on_startup` — Worker-Start (DB-Connections etc.)
2. `on_shutdown` — Worker-Stop
3. `on_job_start` — vor jeder Job-Ausfuehrung
4. `on_job_end` — nach Job, vor Result-Speicherung
5. `after_job_end` — nach Job UND Result-Speicherung

### Erweiterbarkeit

**Stark limitiert.** Monolithische Worker-Klasse (928 Zeilen). Kein Broker-Interface, kein Result-Backend-Interface, kein Middleware-System, kein Plugin-System.

---

## 4.3 Performance & Skalierung

### Concurrency-Modell

`asyncio.BoundedSemaphore(max_jobs + 1)`. Alle Jobs im selben Thread/Prozess. CPU-intensive Jobs blockieren den Worker.

### Polling-basiert

```python
async for _ in poll(self.poll_delay_s):  # Default: 0.5s
    await self._poll_iteration()
```

Minimale Latenz = `poll_delay`. Jeder Poll = Redis-Roundtrip auch wenn Queue leer. Vorteil: Sorted Set erlaubt zeitbasiertes Scheduling, Worker bleibt responsive.

### Horizontale Skalierung — Probleme

- Alle Worker lesen gleiche Job-IDs aus Sorted Set -> Contention steigt linear
- Optimistisches Locking fuehrt zu WatchErrors unter Last
- Kein Work-Stealing, kein partitioniertes Queueing

### Memory-Footprint

Minimal. 20-40 MB RAM pro Worker-Prozess. Nur `redis[hiredis]` und `click` als Abhaengigkeiten.

---

## 4.4 Developer Experience

### API (minimalistisch)

Gesamte oeffentliche API = 8 Exports:
```python
__all__ = ('ArqRedis', 'create_pool', 'cron', 'VERSION', 'Retry', 'Worker', 'check_health', 'func', 'run_worker')
```

```python
# tasks.py
async def say_hello(ctx, name: str) -> str:
    return f'Hello {name}'

class WorkerSettings:
    functions = [say_hello]
    redis_settings = RedisSettings()

# enqueue.py
redis = await create_pool(RedisSettings())
job = await redis.enqueue_job('say_hello', 'World')
result = await job.result(poll_delay=0.5)
```

### Testbarkeit (Burst-Modus — exzellent)

```python
async def test_job(arq_redis, worker):
    async def my_task(ctx):
        return 42

    await arq_redis.enqueue_job('my_task')
    w = worker(functions=[func(my_task, name='my_task')], burst=True, poll_delay=0)
    await w.main()  # Laeuft bis Queue leer
    assert w.jobs_complete == 1
```

`run_check` wirft `FailedJobs` bei Fehlern.

### CLI

```bash
arq path.to.WorkerSettings           # Worker starten
arq path.to.WorkerSettings --burst    # Burst-Modus
arq path.to.WorkerSettings --check    # Health-Check
arq path.to.WorkerSettings --watch .  # Auto-Reload
```

---

## 4.5 Bekannte Schwaechen

1. **Redis-Lock-In:** Kein Abstraktions-Layer. Wechsel unmoeglich.
2. **Fehlende Features:** Keine Workflows, keine Prioritaeten, keine DLQ, kein Rate-Limiting, keine Middleware, keine Dependencies.
3. **Pickle als Default:** Sicherheitsrisiko.
4. **Skalierungsgrenzen:** Contention bei >10 Workern, kein Partitioning.
5. **Maintenance-only:** Keine neuen Features, abnehmendes Momentum.
6. **String-basiertes Enqueue:** `enqueue_job('name')` ohne Typ-Sicherheit.

---

# 5. Vergleichsmatrix

| Dimension | Celery | TaskIQ | Dramatiq | arq |
|---|---|---|---|---|
| **Async-Native** | Nein | Ja (asyncio + anyio) | Nein (Threading) | Ja (asyncio) |
| **Message-Garantie Default** | At-most-once (unsicher!) | Broker-abhaengig, 3 Modi | At-least-once (hardcoded) | At-most-once |
| **DLQ-Support** | Keiner (broker-manuell) | Keiner (Bug #578) | Erstklassig (auto 3-Queue) | Keiner |
| **Retry-System** | Manuell + autoretry_for | Simple + Smart Middleware | 20 Retries, exp. Backoff | `raise Retry(defer=N)` |
| **Broker-Abstraktion** | Kombu (leaky, komplex) | Minimal (2 Methoden) | Sauber (enqueue+consume) | Keine (Redis fix) |
| **Middleware** | Signals + Bootsteps | 8 Hooks, Fwd/Reversed | 20+ Hooks (vorbildlich) | Keine |
| **Typ-Sicherheit** | Keine native | Hervorragend (ParamSpec) | Minimal | Partial (py.typed) |
| **Testbarkeit** | Eager-Mode (schlecht) | InMemoryBroker (exzellent) | StubBroker (exzellent) | Burst-Modus (exzellent) |
| **Workflow/Canvas** | Umfangreich | Minimal | Basisch (Pipe + Group) | Keiner |
| **DI-Support** | Keiner | FastAPI-artig | Keiner | ctx-Dict |
| **Performance (20k Jobs)** | 11.7s | **2.0s** | 4.1s | 35.4s |
| **Concurrency** | Prefork/Gevent/Thread | Async + Semaphore | Thread-Pools | asyncio.Semaphore |
| **Graceful Shutdown** | 3-stufig | 2-stufig + Auto-Restart | 2-stufig + Shutdown-Exc | 2-stufig |
| **Rate Limiting** | Per-Worker nur | Keiner | Distributed (Redis) | Keiner |
| **Scheduler/Cron** | Celery Beat | taskiq-scheduler | Keiner (extern) | Eingebaut |
| **Community** | 25k+ Stars | ~2k Stars | ~4.6k Stars | ~2k Stars |
| **Wartung** | 700+ offene Issues | Aktiv, klein | Aktiv, 1 Maintainer | Maintenance-only |
| **Serialisierung Default** | JSON (seit 4.0) | JSON | JSON | Pickle (unsicher!) |
| **Result-Backend** | Redis/DB/RPC/... | Redis/Memcached/PG | Redis/Memcached | Redis only |
| **Config-Komplexitaet** | 100+ Optionen | Programmatisch | Minimal | Minimal |

---

# 6. Best-Practice-Synthese fuer SwarmQ

## 6.1 Von Dramatiq uebernehmen (Reliability-Champion)

| Pattern | Beschreibung | Warum |
|---|---|---|
| Ack nach Verarbeitung als Default | Nicht konfigurierbar wie bei Celery | Unsicherer Default ist die #1 Fehlerquelle bei Celery |
| Automatisches 3-Queue-Pattern | Main/Delayed/DLQ pro logischer Queue | Kein anderes System hat erstklassige DLQ |
| Middleware-System mit 20+ Hooks | Message-, Ack-, Declaration-, Worker-, Thread-Lifecycle | Beste Architektur-Entscheidung aller vier Systeme |
| ShutdownNotifications | Tasks per Exception ueber Shutdown informieren | Ermoeglicht sauberes Cleanup in long-running Tasks |
| StubBroker mit fail_fast | Test-Broker wirft Exceptions bei Fehlern | Erzwingt korrektes Error-Handling in Tests |
| Convention over Configuration | Wenige, korrekte Defaults | Celery's 100+ Settings sind ein Anti-Pattern |
| nack/reject im Core | Nachrichten zurueck in Queue oder DLQ | TaskIQ fehlt das komplett (Bug #578) |
| Distributed Rate Limiting | ConcurrentRateLimiter mit Redis-Backend | Einziges System mit globalem Rate-Limiting |

## 6.2 Von TaskIQ uebernehmen (Architecture-Champion)

| Pattern | Beschreibung | Warum |
|---|---|---|
| Minimale Broker-Abstraktion | `kick` + `listen` (+ `nack`/`reject` ergaenzen!) | Elegantestes Interface, <50 Zeilen fuer neuen Broker |
| Dreistufiges Ack-System | WHEN_RECEIVED / WHEN_EXECUTED / WHEN_SAVED | Praezise Steuerung Reliability vs. Performance |
| ParamSpec/TypeVar Type-Safety | Return-Types durchgaengig getrackt | Am besten typisiertes Task-Queue-Framework |
| Fluent-API-Pattern | `.with_result_backend().with_middlewares()` | Konsistent, lesbar, discoverable |
| Prefetcher/Runner mit Semaphoren | Zwei Coroutines mit asyncio.Queue dazwischen | Natuerlicher Backpressure-Mechanismus |
| Zweischichtige Serialisierung | Serializer (Payload) + Formatter (Message-Struktur) | Unabhaengig anpassbar |
| FastAPI-artiges DI-System | taskiq-dependencies als Vorbild | Eliminiert Boilerplate fuer DB, Redis etc. |
| InMemoryBroker fuer Tests | Fuehrt Tasks sofort aus, volles Interface | Kein externer Service noetig |
| maybe_awaitable() Utility | Sync und async Hooks transparent | Vereinfacht Middleware-Implementierung |

## 6.3 Von arq uebernehmen (DX-Champion)

| Pattern | Beschreibung | Warum |
|---|---|---|
| Async-native von Grund auf | Kein Threading-Workaround | Wichtigste Differenzierung vs. Celery/Dramatiq |
| `raise Retry(defer=60)` Pattern | Exception-basierte Retry-Steuerung | Intuitiv, job-spezifisch, kein Framework-Code |
| Burst-Modus fuer Tests | Worker laeuft bis Queue leer, dann Exit | Simpelstes Test-Pattern |
| Health-Check via TTL-Key | Periodischer Redis-Key | Kubernetes-ready, minimal |
| Sorted Set fuer zeitbasierte Queues | Score = Ausfuehrungszeitpunkt | Scheduling kostenlos, atomares Dequeue |
| Minimaler Kern | ~1500 Zeilen fuer vollen Task Manager | Beweis, dass es schlank geht |
| WATCH/MULTI Optimistisches Locking | Verhindert Doppelausfuehrung ohne verteilte Locks | Effizient, keine Deadlocks |
| on_startup/on_shutdown Hooks | Async Setup/Teardown im Worker | Perfekt fuer Connection-Pools |

## 6.4 Von Celery uebernehmen (Feature-Champion)

| Pattern | Beschreibung | Warum |
|---|---|---|
| Canvas-Workflow-System | Chain/Group/Chord (vereinfacht, async-nativ) | Celery's Canvas ist die Referenz |
| Queue-basiertes Routing | Task-Typ-Isolation ueber Queues | Essentiell fuer gemischte Workloads |
| Separate Worker-Pools | Async vs. CPU Worker auf verschiedenen Queues | CPU-Tasks brauchen andere Concurrency |
| `prefetch_multiplier=1` als Default | Statt Celery's 4 | Default 4 fuehrt zu Lastverteilungsproblemen |
| Serializer-Whitelist | `accept_content` kontrolliert akzeptierte Formate | Sicherheit gegen Pickle-Injection |
| Task-Naming-Convention | `{module}.{class_name}` automatisch | Eindeutig ohne manuelle Pflege |
| Pydantic-Argument-Validierung | Automatische Validierung bei annotierten Args | Type-Safety ueber die Wire-Grenze |

## 6.5 Was KEINES der Systeme gut macht (SwarmQ-Differenzierung)

| Luecke | Problem | SwarmQ-Loesung |
|---|---|---|
| Push-basiertes Result-Waiting | Alle nutzen Polling | Valkey Pub/Sub oder Streams fuer Echtzeit-Results |
| Globale Rate-Limits | Celery nur per-Worker, Rest gar nicht | Verteiltes Rate-Limiting ueber Valkey |
| Task-Level Concurrency Limits | Keines begrenzt pro Task-Typ | Semaphore pro Task-Name im Worker |
| Klassenbasierte Tasks + Typing | Kein System kombiniert beides | Task-Klasse mit `run()` + Return-Type-Annotation |
| Heartbeat-Renewal | arq fixer TTL, Celery Gossip O(n^2) | Periodisches Heartbeat-Update des in-progress-Keys |
| Safe-by-Default Serialisierung | arq Pickle, Celery hatte Pickle als Default | msgpack + JSON Whitelist, Pickle nie erlaubt |
| Unified Observability | Celery: Flower verliert Daten | Metriken ueber Result-Backend persistent |

## 6.6 Architektur-Empfehlung fuer SwarmQ

### Kern-Entscheidungen

1. **Async-first:** Jeder Task ist `async def run()`, Worker ist `asyncio.run()` Event-Loop. Kein sync Workaround.
2. **Ack nach Verarbeitung:** Default und nicht abschaltbar. At-least-once als einzig sichere Semantik.
3. **3-Queue-Pattern:** Jede logische Queue hat Main/Delayed/DLQ automatisch.
4. **Minimale Broker-Abstraktion:** `kick` + `listen` + `nack` (3 Methoden). Phase 1: RabbitMQ.
5. **Klassenbasierte Tasks:** Lifecycle-Hooks (`pre_execution`, `run`, `post_execution`) + volle mypy-Kompatibilitaet.
6. **Middleware-Pipeline:** Dramatiq's 20+ Hooks als Vorbild, aber async-nativ.
7. **msgspec fuer Wire-Format (JSON):** Schnell, typ-sicher, sprachuebergreifend.
8. **msgpack fuer Result-Serialisierung:** Kompakt, schnell, typ-sicher.
9. **Valkey als Result-Backend:** Push-basierte Results via Pub/Sub.
10. **Prefetcher/Runner-Pattern:** TaskIQ's Semaphore-basierter Backpressure.
11. **InMemoryBroker + Burst-Modus:** Erstklassige Testbarkeit von Tag 1.
12. **DI via dishka:** Korrekte Scopes, Generator-Dependencies.

### Nicht implementieren (YAGNI / Anti-Patterns)

- Gossip-Protokoll (skaliert nicht)
- Pickle-Serialisierung (Sicherheitsrisiko)
- ETA/Countdown im Worker-Memory (Broker soll das halten)
- `task_always_eager` Mode (InMemoryBroker ist besser)
- 100+ Konfigurationsoptionen (minimal, korrekte Defaults)
- Eventlet/Gevent Pools (async-native stattdessen)

---

## Quellen

### Celery
- Celery GitHub Repository: github.com/celery/celery
- Celery 5.6.3 Dokumentation: docs.celeryq.dev
- Kombu Transport System (DeepWiki)
- The Problems with Celery (Hatchet Blog)
- Gevent vs Prefork Performance (Celery School)
- celery-types (PyPI)

### TaskIQ
- TaskIQ GitHub Repository: github.com/taskiq-python/taskiq
- TaskIQ Dokumentation: taskiq-python.github.io
- TaskIQ DLQ Issue #578
- Python Task Queue Benchmark (Steven Yue)

### Dramatiq
- Dramatiq Offizielle Dokumentation: dramatiq.io
- Dramatiq GitHub Repository: github.com/Bogdanp/dramatiq
- Async/Await Issue #238
- DeepWiki Architektur-Analyse

### arq
- arq GitHub Repository: github.com/samuelcolvin/arq
- arq Dokumentation: arq-docs.helpmanual.io
- Issue #510 (Maintenance-only Status)
