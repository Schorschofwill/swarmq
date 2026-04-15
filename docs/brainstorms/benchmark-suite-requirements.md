# Benchmark Suite: SwarmQ vs. Celery, TaskIQ, Dramatiq, arq

**Datum:** 2026-04-15
**Ziel:** Internes Entwicklungs-Feedback — ehrliche Standortbestimmung von SwarmQ im Vergleich zu etablierten Python Task Queues. Keine Schönfärberei, keine Bevorzugung.

---

## 1. Motivation

SwarmQ v2 ist eine async-native Task Queue für Python 3.14+. Um fundierte Architektur- und Optimierungsentscheidungen treffen zu können, braucht es einen realistischen, fairen Vergleich mit den etablierten Alternativen. Der Benchmark soll Stärken und Schwächen aller Systeme — inklusive SwarmQ — klar offenlegen.

### Konkrete Fragen, die der Benchmark beantworten soll

1. **Framework-Overhead:** Wie hoch ist SwarmQs Per-Task-Overhead im Vergleich? (Serialisierung, Routing, Ack)
2. **Concurrency-Effizienz:** Ist SwarmQs Semaphore-basierte Concurrency kompetitiv mit TaskIQs asyncio-Ansatz?
3. **Quorum-Queue-Kosten:** Wie viel Throughput kostet SwarmQs Entscheidung für RabbitMQ Quorum Queues?
4. **Latenz-Profil:** Wo liegt SwarmQ bei P50/P95/P99 — insbesondere im Vergleich zu Libraries mit Push-basiertem Result-Backend?
5. **Retry-Overhead:** Wie effizient ist SwarmQs DLX-basiertes Retry vs. in-process Retry anderer Libraries?

### Entscheidungsregeln

**Phase 1 (SwarmQ vs. TaskIQ, Cross-Broker — nur richtungsweisend):**

| Ergebnis | Aktion |
|----------|--------|
| SwarmQ-Throughput < 50% von TaskIQ (trotz Broker-Unterschied) | Profiling-Session — bei so großem Abstand ist der Broker nicht allein schuld |
| SwarmQ P99-Latenz > 5× TaskIQ | Result-Polling-Architektur überdenken (Pub/Sub?) |
| SwarmQ Memory/Task > 2× TaskIQ | Objekt-Allokation und Serialisierung profilen |
| Semaphore-Skalierung (3.9.2) flacht vor Concurrency 50 ab | Prefetcher-Architektur reviewen |

**Phase 2+ (gleicher Broker, falls Ebene A verfügbar):**

| Ergebnis | Aktion |
|----------|--------|
| SwarmQ-Throughput < 50% des besten async-Konkurrenten (gleicher Broker) | Profiling, Bottleneck identifizieren |
| Quorum-Queue-Overhead > 30% vs. Classic Queues (3.9.1) | Konfigurierbare Queue-Typen als Feature evaluieren |
| CPU-bound: SwarmQ < 20% von Celery | ProcessPoolExecutor-Support priorisieren |

## 2. Teilnehmer und Konfiguration

### Phasenansatz

**Phase 1 (Kern):** SwarmQ vs. TaskIQ — beide async-native, direkter Architektur-Vergleich. Liefert sofort actionable Daten.

**Phase 2 (Erweiterung):** + Celery + Dramatiq — sync/multi-process Libraries für breitere Einordnung.

**Phase 3 (Optional):** + arq — nur wenn Phase 1+2 Fragen aufwerfen, die arq beantworten kann. arq ist in Maintenance-Only-Modus und hat kein Multi-Worker-Support; es wird daher nicht standardmäßig einbezogen.

### Broker-Konfiguration: Zwei Ebenen

**Ebene B — Preferred Broker (primär in Phase 1):** Jede Library nutzt ihren bevorzugten Broker. Misst die "ökologische" Performance — was ein Nutzer in der Praxis erleben würde. Da SwarmQ aktuell nur RabbitMQ unterstützt, ist dies die einzige Ebene, in der SwarmQ teilnehmen kann.

> **Cross-Broker-Vergleichbarkeit:** Ergebnisse zwischen RabbitMQ-basierten Libraries (SwarmQ, Celery, Dramatiq) und Redis-basierten Libraries (TaskIQ) sind wegen unterschiedlicher Broker-Overhead-Profile **nicht direkt vergleichbar**. Der Benchmark liefert trotzdem Wert: (1) Vergleiche innerhalb derselben Broker-Gruppe sind aussagekräftig, (2) große Unterschiede (>2×) deuten auf Library-Probleme hin, nicht auf Broker-Differenzen, (3) die SwarmQ-spezifischen Szenarien (3.9) brauchen keinen Cross-Library-Vergleich.

**Ebene A — Controlled Broker (Zukunft):** Alle Libraries auf Redis/Valkey. Isoliert Framework-Overhead vom Broker. Wird möglich, sobald SwarmQ einen Redis-Broker-Adapter hat. Nicht Teil des initialen Benchmark-Scope.
- Celery: `redis://` Broker (gut unterstützt)
- TaskIQ: `ListQueueBroker` (Standard)
- Dramatiq: `RedisBroker` (gut unterstützt)
- SwarmQ: Redis-Broker-Adapter noch nicht vorhanden — Ebene A ist ein **Folgeprojekt**, kein Blocker

| Library | Broker (Ebene B) | Result-Backend | Concurrency-Modell | Worker-Setup |
|---------|-------------------|----------------|---------------------|--------------|
| **SwarmQ** | RabbitMQ (Quorum) | Valkey | asyncio (Semaphore) | Single-Process, configurable concurrency |
| **Celery** | RabbitMQ | Redis | prefork (Multiprocessing) | Multi-Process, prefork pool |
| **TaskIQ** | Redis (ListQueueBroker) | Redis | asyncio | Single-Process, async workers |
| **Dramatiq** | RabbitMQ | Redis | Threads + Prozesse | Multi-Process, Thread-Pool pro Prozess |

### Docker-Services und Ressourcen

**Services:**
- **RabbitMQ** (rabbitmq:4-management-alpine) — Port 5672
- **Valkey** (valkey/valkey:8-alpine) — Port 6379 (dient als Redis-kompatibles Backend für alle Libraries)

**Hinweis:** Valkey ist Redis-wire-kompatibel. Celery/TaskIQ/Dramatiq nutzen ihre Redis-Clients gegen den Valkey-Server. Es wird kein separater Redis-Server benötigt.

**Redis-Datenbank-Isolation:** Jede Library nutzt eine eigene Redis-Datenbank (TaskIQ: db=1, Celery-Backend: db=2, Dramatiq-Backend: db=3). Zwischen den Benchmark-Läufen verschiedener Systeme wird ein FLUSHDB pro genutzter Datenbank ausgeführt.

**Docker-Ressourcenlimits:**
- **Alle Libraries:** `cpus: 4, mem_limit: 2g` — identisches Budget. Jede Library entscheidet selbst, wie sie die Ressourcen nutzt (Single-Process mit 4 verfügbaren Cores vs. 4 Worker-Prozesse mit je 1 Core). Das spiegelt die Realität wider: ein Nutzer stellt eine Maschine bereit, und die Library nutzt sie nach ihrem Modell.
- Die Limits werden in der docker-compose.yml dokumentiert und begründet
- **Hinweis:** Single-Process async-Libraries können die zusätzlichen Cores nur über `ProcessPoolExecutor` o.ä. nutzen — das ist ein fairer Nachteil ihres Modells, kein Benchmark-Bias

## 3. Benchmark-Szenarien

### 3.1 No-op Throughput

**Was:** N Tasks ohne Nutzlast/Arbeit schedulen und ausführen.
**Misst:** Reiner Framework-Overhead (Serialisierung, Routing, Ack).
**Parameter:** N = 1.000, 5.000, 10.000 Tasks.
**Broker:** Ebene B (preferred) — da kein Controlled-Broker-Vergleich möglich ohne SwarmQ Redis-Broker.
**Hinweis:** Ergebnisse für RabbitMQ-basierte Libraries (SwarmQ, Celery, Dramatiq) und Redis-basierte Libraries (TaskIQ) sind wegen unterschiedlicher Broker-Overhead-Profile **nicht direkt vergleichbar**. Vergleiche innerhalb derselben Broker-Gruppe sind aussagekräftig.

### 3.2 I/O-bound Tasks

**Was:** Tasks führen eine simulierte I/O-Operation aus.
**Simulation:** Alle Libraries nutzen `asyncio.sleep()` (async) bzw. `time.sleep()` (sync). Da `time.sleep()` einen OS-Thread blockiert und `asyncio.sleep()` nicht, ist dies **kein äquivalenter I/O-Proxy** — es misst die Concurrency-Architektur, nicht reale I/O-Performance.
**Misst:** Concurrency-Effizienz des jeweiligen Modells unter idealisierter I/O-Last. Async-Libraries haben hier einen konstruktionsbedingten Vorteil.
**Parameter:**
- Work-Duration: 10ms, 50ms, 100ms
- Concurrency: 10, 50, 100 parallele Tasks
- Gesamtzahl: 1.000 Tasks pro Kombination
**Interpretation:** Vergleiche primär SwarmQ vs. TaskIQ (async vs. async) und Celery vs. Dramatiq (sync vs. sync). Cross-Modell-Vergleiche zeigen den Architektur-Unterschied, nicht Library-Qualität.

### 3.3 CPU-bound Tasks

**Was:** Tasks berechnen SHA-256 Hash N-mal iterativ.
**Misst:** Parallelismus-Fähigkeit unter CPU-Last.
**Parameter:**
- Iterationen: 10.000 (leicht), 100.000 (mittel), 1.000.000 (schwer)
- Gesamtzahl: 500 Tasks pro Kombination
**Regeln:**
- Celery: prefork Pool (Standard, nutzt echten Multiprocessing-Parallelismus)
- Dramatiq: Standard Threads + Prozesse
- TaskIQ: **Kein** `run_in_executor()` — die CPU-Arbeit läuft im Event-Loop-Thread, wie es der Default ist
- SwarmQ: **Kein** `ProcessPoolExecutor` — SwarmQ hat dieses Feature nicht, und der Benchmark misst den Ist-Zustand
**Erwartung:** Celery und Dramatiq dominieren hier. Das ist ein bekannter und dokumentierter SwarmQ-Nachteil. Der Benchmark quantifiziert den Abstand.

### 3.4 Latenz unter Last (Sustained Load)

**Was:** Konstante Enqueue-Rate aufrechterhalten und End-to-End-Latenz messen.
**Misst:** Tail-Latency unter realistischer Last.
**Parameter:**
- Enqueue-Rate: 100 Tasks/s, 500 Tasks/s, 1.000 Tasks/s
- Task-Arbeit: 10ms Sleep-Simulation
- Dauer: 30 Sekunden pro Rate
**Messmethode:**
- "Enqueue" = Zeitpunkt unmittelbar vor dem `enqueue()`-Aufruf im Orchestrator-Prozess
- "Result verfügbar" = Zeitpunkt, an dem der Orchestrator das Ergebnis erstmals aus dem Backend lesen kann
- Polling-Intervall: **10ms** für alle Libraries (Kompromiss: minimiert Polling-Artefakte ohne exzessiven CPU-Overhead — bei 1ms und 1000 Tasks/s entstünden ~1M Backend-Reads/s, was selbst zum Bottleneck wird)
- SwarmQs Default-Polling von 200ms wird für dieses Szenario explizit auf 10ms überschrieben
**Metriken:** P50, P95, P99 Latenz (Enqueue → Result verfügbar)

### 3.5 Payload-Größen

**Was:** Tasks mit unterschiedlich großen Payloads (JSON-Daten).
**Misst:** Serialisierungs-Overhead und Broker-Verhalten bei großen Nachrichten.
**Parameter:** 100 Bytes, 1 KB, 10 KB, 100 KB Payload.
**Gesamtzahl:** 1.000 Tasks pro Größe.

### 3.6 Concurrency-Skalierung

**Was:** Gleiche Gesamtarbeit, aber steigende Concurrency.
**Misst:** Wie gut skaliert die Library mit steigender Parallelität?
**Parameter:**
- Concurrency: 1, 5, 10, 25, 50, 100
- Task-Arbeit: 10ms Sleep-Simulation
- Gesamtzahl: 1.000 Tasks
**Hinweis:** Für Multi-Process-Libraries bedeutet "Concurrency 50" ggf. Anpassung der Worker-Anzahl — die Konfiguration wird pro Library und pro Concurrency-Stufe dokumentiert.

### 3.7 Retry/Failure-Handling

**Was:** 30% der Tasks werfen eine Exception, die Retry auslöst.
**Misst:** Retry-Mechanismus-Overhead (nicht Backoff-Wartezeit).
**Parameter:**
- max_retries: 3
- Backoff: **50ms Basis** (bewusst kurz, um Retry-Mechanismus-Overhead sichtbar zu machen statt Backoff-Timer zu messen)
- 1.000 Tasks, davon 300 scheitern beim ersten Versuch, 100 beim zweiten, 30 beim dritten (deterministisch)
**Metriken:**
- **Retry-Latenz:** Zeit von Exception bis Redelivery an den Worker (exkl. Backoff-Wartezeit). Misst den reinen Mechanismus-Overhead (DLX-Routing vs. in-process Retry).
- **Gesamtdauer:** Wall-Clock-Zeit bis alle Tasks abgeschlossen (inkl. Backoff). Bei DLX-basiertem Retry (SwarmQ) dominiert hier die Broker-Round-Trip-Zeit.
**Hinweis:** SwarmQ nutzt DLX-basiertes Retry über RabbitMQ (out-of-process mit Delay-Queue), andere Libraries retrien in-process. Bei SwarmQ entsteht pro Retry ein zusätzlicher Broker-Round-Trip (Publish → DLQ → TTL → Redeliver), der bei in-process Retry nicht existiert.

### 3.8 Memory-Profiling

**Was:** Peak-Memory-Verbrauch während der Szenarien 3.1 und 3.2.
**Misst:** Ressourcen-Effizienz.
**Methode:** `/proc/[pid]/status` VmRSS-Tracking über die gesamte Szenario-Dauer.
**Normalisierung:**
- **Single-Process** (SwarmQ, TaskIQ): RSS des Worker-Prozesses
- **Multi-Process** (Celery, Dramatiq): Summe RSS aller Worker-Prozesse. Zusätzlich: RSS/Worker-Prozess als Vergleichswert
- Beide Werte werden berichtet — die Summe für "was kostet das System insgesamt?", RSS/Worker für "wie effizient ist der einzelne Prozess?"

### 3.9 SwarmQ-spezifische Szenarien (nur Phase 1)

Diese Szenarien laufen **nur für SwarmQ** und dienen der Architektur-Analyse:

**3.9.1 Quorum vs. Classic Queue Overhead:**
- Gleicher SwarmQ-Code, einmal mit Quorum Queues, einmal mit Classic Queues
- **Voraussetzung:** Erfordert eine minimale Code-Änderung: `x-queue-type` in `RabbitMQBroker.declare_queue()` konfigurierbar machen (aktuell hardcoded als `"quorum"`). Dies ist eine gezielte Konfigurationsänderung, keine Feature-Entwicklung — Ausnahme vom Nicht-Ziel "keine Optimierung VOR dem Benchmark"
- Misst den Preis der Quorum-Queue-Entscheidung
- Parameter: 10.000 No-op Tasks, 3 Durchläufe

**3.9.2 Semaphore-Concurrency Skalierung:**
- SwarmQ mit Concurrency-Werten 1, 5, 10, 25, 50, 100, 200
- Misst: Ab welcher Concurrency flacht der Throughput ab oder steigt die Latenz?
- Parameter: 5.000 Tasks mit 10ms Sleep, pro Concurrency-Stufe

**3.9.3 Result-Backend Polling-Intervall-Analyse:**
- Vergleich: Polling mit 1ms, 10ms, 50ms, 200ms Intervall
- Misst: Wie viel Latenz kostet der aktuelle Polling-Ansatz bei verschiedenen Intervallen? Ab wann wird Polling-CPU-Overhead selbst zum Bottleneck?
- Parameter: 1.000 Tasks mit 10ms Sleep, pro Polling-Intervall

## 4. Metriken und Aggregation

### Metriken pro Szenario

| Metrik | Einheit | Beschreibung |
|--------|---------|--------------|
| **Throughput** | Tasks/s | Abgeschlossene Tasks pro Sekunde |
| **Latenz P50** | ms | Median Enqueue-to-Result |
| **Latenz P95** | ms | 95. Perzentil |
| **Latenz P99** | ms | 99. Perzentil |
| **Total Duration** | s | Gesamtzeit für alle Tasks |
| **Memory Peak** | MB | Maximaler RSS-Verbrauch (siehe 3.8 für Normalisierung) |

### Aggregation über Durchläufe

- Mindestens **3 Durchläufe** pro Szenario
- Pro Durchlauf werden alle Metriken berechnet (Throughput, P50/P95/P99 über alle Tasks des Laufs)
- Über die Durchläufe wird der **Median** jeder Metrik berichtet
- Zusätzlich: **Min/Max** über die Durchläufe als Varianz-Indikator

### Warm-up

Vor jedem Szenario werden **100 zusätzliche Tasks** als Warm-up ausgeführt und verworfen. Diese sind nicht Teil des gemessenen N. Die Warm-up-Tasks nutzen dasselbe Task-Profil wie das Szenario (bei Retry-Szenarien ohne Failure-Injection).

## 5. Fairness-Prinzipien

1. **Zwei Vergleichsebenen** — Controlled Broker (isoliert Framework-Overhead) und Preferred Broker (ökologische Validität). Ergebnisse werden getrennt dargestellt, nicht vermischt.
2. **Jede Library nutzt ihr optimales Concurrency-Modell** — Celery bekommt prefork, TaskIQ bekommt asyncio.
3. **Proportionales Hardware-Budget** — Docker-Container mit dokumentierten CPU/Memory-Limits, proportional zur Worker-Anzahl (siehe Sektion 2).
4. **Kein async-Bias im Harness** — Sync-Libraries (Celery, Dramatiq) werden über Subprozesse gesteuert, nicht über async-Wrapper. Der Orchestrator kommuniziert mit sync-Workern über CLI/Subprocess-Steuerung.
5. **Mehrere Durchläufe** — Mindestens 3 Durchläufe pro Szenario, Median mit Min/Max.
6. **Kein Cherry-Picking** — ALLE Ergebnisse werden berichtet, auch wenn SwarmQ schlecht abschneidet.
7. **Dokumentierte Konfiguration** — Jede Library-Konfiguration wird im Benchmark-Code dokumentiert und begründet.
8. **Bekannte Limitierungen benennen** — z.B. SwarmQ hat keinen ProcessPoolExecutor; Cross-Broker-Vergleiche sind nicht direkt aussagekräftig.
9. **Interpretationshinweise** — Jedes Szenario dokumentiert, welche Vergleiche aussagekräftig sind und welche nicht.

## 6. Erwartete SwarmQ-Schwächen (ehrliche Einschätzung)

- **CPU-bound Tasks:** Ohne ProcessPoolExecutor ist SwarmQ bei reiner CPU-Arbeit gegenüber Celery/Dramatiq im Nachteil. Alle Arbeit läuft im selben Event-Loop-Thread.
- **Ökosystem-Reife:** SwarmQ ist neu und hat keine Battle-Tested-Historie. Celery läuft in Produktion seit über 10 Jahren.
- **Feature-Umfang:** Viele geplante Features (Priorities, Workflows, Rate-Limiting) sind noch nicht implementiert. Der Benchmark vergleicht nur tatsächlich implementierte Features.
- **Multi-Worker:** SwarmQ hat aktuell keinen eingebauten Process-Manager für Multi-Worker-Deployment.
- **Broker-Lock-in:** SwarmQ unterstützt aktuell nur RabbitMQ Quorum Queues. Das macht direkten Throughput-Vergleich mit Redis-basierten Libraries schwierig.
- **Result-Polling:** SwarmQs 200ms Default-Polling-Intervall erhöht die messbare E2E-Latenz gegenüber Libraries mit Push-basiertem Result-Backend.

## 7. Technische Umsetzung

### Verzeichnisstruktur

```
benchmarks/
├── comparison/
│   ├── docker-compose.yml     # RabbitMQ + Valkey mit Ressourcenlimits
│   ├── requirements.txt       # Alle Library-Dependencies
│   ├── runner.py              # Haupt-Benchmark-Orchestrator
│   ├── results/               # JSON-Ergebnisse pro Lauf
│   ├── scenarios/
│   │   ├── base.py            # Gemeinsame Szenario-Logik
│   │   ├── noop.py            # 3.1 No-op Throughput
│   │   ├── io_bound.py        # 3.2 I/O-bound
│   │   ├── cpu_bound.py       # 3.3 CPU-bound
│   │   ├── latency.py         # 3.4 Latenz unter Last
│   │   ├── payload.py         # 3.5 Payload-Größen
│   │   ├── scaling.py         # 3.6 Concurrency-Skalierung
│   │   ├── retry.py           # 3.7 Retry/Failure
│   │   ├── memory.py          # 3.8 Memory-Profiling
│   │   ├── quorum_classic.py  # 3.9.1 Quorum vs. Classic Queue
│   │   ├── semaphore_scale.py # 3.9.2 Semaphore-Concurrency
│   │   └── polling_interval.py # 3.9.3 Polling-Intervall-Analyse
│   ├── systems/
│   │   ├── base.py            # System-Schnittstelle
│   │   ├── swarmq_sys.py      # SwarmQ-Adapter
│   │   ├── celery_sys.py      # Celery-Adapter
│   │   ├── taskiq_sys.py      # TaskIQ-Adapter
│   │   └── dramatiq_sys.py    # Dramatiq-Adapter
│   ├── workers/               # Standalone Worker-Definitionen pro Library
│   │   ├── swarmq_worker.py   # SwarmQ Task-Klassen + Worker-Start
│   │   ├── celery_worker.py   # Celery App + Tasks (via `celery worker`)
│   │   ├── taskiq_worker.py   # TaskIQ Broker + Tasks (via `taskiq worker`)
│   │   └── dramatiq_worker.py # Dramatiq Actors (via `dramatiq` CLI)
│   └── report.py              # Ergebnis-Aufbereitung (Tabellen, JSON)
```

### Worker-Modell (kein all-async ABC)

Statt eines einheitlichen async-ABC nutzt jede Library **ihr natives Worker-Modell:**

**Async-Libraries** (SwarmQ, TaskIQ): Worker laufen als `asyncio.Task` im selben Prozess wie der Orchestrator. Der Orchestrator managed den Lifecycle direkt. **Wichtig:** Der SwarmQ-Adapter muss eigene Broker/Backend-Instanzen für den Worker erstellen (nicht die Orchestrator-Connections teilen), da `Worker._execute_shutdown()` die Connections schließt.

**Sync-Libraries** (Celery, Dramatiq): Worker laufen als **separate Subprozesse**, gestartet über ihre nativen CLI-Befehle (`celery -A worker`, `dramatiq module`). Der Orchestrator:
- Startet den Worker-Subprozess
- Enqueued Tasks über die Library-eigene Client-API (synchron, im eigenen Thread via `asyncio.to_thread`)
- Pollt Ergebnisse aus dem Result-Backend
- Stoppt den Worker-Subprozess via Signal

### System-Schnittstelle

```python
class TaskQueueSystem(ABC):
    """Minimale Schnittstelle — implementierungsspezifisch pro Library."""
    name: str

    async def setup(self) -> None:
        """Broker/Backend-Verbindung, Queue-Deklaration."""

    async def teardown(self) -> None:
        """Cleanup: Queues leeren, Verbindungen schließen."""

    async def start_worker(self) -> None:
        """Async: asyncio.Task starten. Sync: Subprozess starten."""

    async def stop_worker(self) -> None:
        """Async: Task canceln. Sync: SIGTERM an Subprozess."""

    async def enqueue(self, task_name: str, payload: dict) -> str:
        """Task schedulen, task_id zurückgeben.
        Sync-Libraries: via asyncio.to_thread()."""

    async def get_result(self, task_id: str, timeout: float, poll_interval: float = 0.01) -> Any:
        """Result pollen. poll_interval wird vom Szenario vorgegeben (Default: 10ms)."""
```

**Task-Registrierung:** Jede Library definiert ihre Tasks in der jeweiligen Worker-Datei unter `workers/`, in der idiomatischen Art der Library:
- SwarmQ: `Task`-Subklassen mit `task_info` (registriert sich via `__init_subclass__`)
- Celery: `@app.task`-Dekorator
- TaskIQ: `@broker.task`-Dekorator
- Dramatiq: `@dramatiq.actor`-Dekorator

### Ausführung

```bash
# Phase 1: SwarmQ vs TaskIQ (Kern-Szenarien + SwarmQ-spezifisch)
cd benchmarks/comparison
docker compose up -d
python runner.py --scenario noop,io_bound,latency,scaling --systems swarmq,taskiq --runs 3
python runner.py --scenario quorum_classic,semaphore_scale,polling_interval --systems swarmq --runs 3
python report.py results/

# Phase 2: + Celery, Dramatiq (alle Szenarien)
python runner.py --scenario all --systems all --runs 3
python report.py results/
```

## 8. Nicht-Ziele

- Kein visuelles Dashboard oder Web-UI für Ergebnisse.
- Kein Vergleich von Features, die SwarmQ noch nicht implementiert hat (z.B. Workflows, Priorities).
- Keine Optimierung des SwarmQ-Codes VOR dem Benchmark — erst messen, dann optimieren.
- Keine Veröffentlichung der Ergebnisse (internes Tool).
- Kein reusable Benchmarking-Framework — der Code dient einem konkreten Vergleich, nicht der Wiederverwendung.

## 9. Python-Kompatibilität

Vor Implementierung muss verifiziert werden, dass alle Libraries auf **Python 3.14+** lauffähig sind:
- Celery 5.6.x: offiziell bis 3.13, 3.14-Kompatibilität prüfen
- Dramatiq 2.1.x: offiziell bis 3.14
- TaskIQ 0.11.x: 3.10+, 3.14 prüfen

Falls eine Library nicht auf 3.14 läuft, wird sie entweder mit der neuesten kompatiblen Python-Version getestet (dokumentiert) oder aus dem Benchmark ausgeschlossen.
