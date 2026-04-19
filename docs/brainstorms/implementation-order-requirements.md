---
date: 2026-04-15
topic: implementation-order
---

# SwarmQ — Implementierungsreihenfolge

## Problem Frame

SwarmQ ist vollstaendig spezifiziert (17 Abschnitte, ~970 Zeilen in `aufgabe.md`) und durch ~23.000 Zeilen Recherche-Dokumentation untermauert. Die Implementierung wird ausschliesslich von Claude (AI Coding Agent) durchgefuehrt, strikt nach TDD (definiert in `AGENTS.md`). Die Aufgaben muessen so definiert sein, dass Claude sie ohne Rueckfragen selbststaendig umsetzen kann.

**Ziel:** Produktionsfaehiger Kern als erster Meilenstein — Task-Roundtrip mit Retries, DLQ, Graceful Shutdown und strukturiertem Logging.

## Implementierungsstrategie

**Horizontale Schichten:** Erst den duennsten Roundtrip der funktioniert (Skeleton), dann Schicht fuer Schicht Features drauflegen. Jede Phase baut auf der vorherigen auf und ist am Ende eigenstaendig lauffaehig.

**Claude-Optimierung:** Jede Aufgabe ist:
- Selbststaendig ausfuehrbar (alle Eingaben definiert, kein Kontext aus vorherigen Gespraechen noetig)
- TDD-konform (Test zuerst, dann Implementierung)
- An eine konkrete Datei/Modul gebunden
- Mit klarem "Fertig"-Kriterium

## Phasen-Ueberblick

```
Phase 0: Projektstruktur          [kein Produktionscode]
Phase 1: Minimaler Roundtrip      [Task -> Queue -> Worker -> Ergebnis]
Phase 2: Produktionsfaehiger Kern [Retries, DLQ, Shutdown, Logging]
Phase 3: Komfort-Features         [Middleware, Scheduler, CLI, Rate Limiting]
Phase 4: Erweiterte Features      [Priorities, Expiry, Burst, ProcessManager, Autoscaling — Workflows + Bulk-Optim. verschoben]
Phase 5: Operations               [Metrics, Health Checks, Docs]
```

## Requirements

**Phase 0 — Projektstruktur**

- R0.1. `pyproject.toml` mit Projekt-Metadaten, Dependencies (`msgspec`, `msgpack`, `structlog`, `aio-pika`, `valkey[libvalkey]`), optionalen Extras (`[fast]` fuer orjson, `[metrics]` fuer prometheus_client, `[test]` fuer pytest-Stack), Entry-Point `swarmq`, und Tool-Konfiguration (pytest, mypy, ruff). Referenz: `aufgabe.md` Abschnitt 2.2, 2.3.
- R0.2. Verzeichnisstruktur: `src/swarmq/` (Package), `tests/unit/`, `tests/integration/`, `tests/e2e/`. Alle `__init__.py` Dateien. Referenz: `docs/recherche-tdd.md` Abschnitt 20.
- R0.3. `docker-compose.yml` mit RabbitMQ 4.x + Valkey 8.x, Health Checks, persistierte Volumes. Referenz: `docs/recherche-rabbitmq-server.md` Abschnitt 9, `docs/recherche-valkey-server.md` Abschnitt 8.
- R0.4. `tests/conftest.py` mit gemeinsamen Fixtures, `tests/unit/conftest.py` mit Unit-Fixtures (Mocks, Factories), `tests/integration/conftest.py` mit Integration-Fixtures (echte Broker/Backend Connections). Referenz: `docs/recherche-tdd.md` Abschnitt 20.2.
- R0.5. pytest-Konfiguration in `pyproject.toml`: asyncio_mode="auto", Marker (unit, integration, e2e, slow), Timeouts, Plugin-Liste. Referenz: `docs/recherche-tdd.md` Abschnitt 19.

**Phase 1 — Minimaler Roundtrip**

*Foundation:*
- R1.1. `src/swarmq/exceptions.py` — Phase-1-Exceptions: `SwarmQError` (Basis), `ConfigurationError`, `BrokerConnectionError`, `BackendConnectionError`, `TaskNotFoundError`, `ResultNotReadyError`, `ResultExpiredError`, `ResultSerializationError`, `Retry(delay=)`, `NoRetry(reason=)`. Weitere Exceptions werden in der Phase eingefuehrt wo sie gebraucht werden. Referenz: `aufgabe.md` Abschnitt 15, `docs/recherche-deepening-internals.md` Abschnitt 4.
- R1.2. `src/swarmq/config.py` — Konfigurationsklasse als dataclass. Phase 1: `broker_url`, `backend_url`, `result_ttl`, `queues`, `concurrency`, `log_level`. 2-Tier Aufloesung (Env -> Programmatic -> Default). CLI-Tier wird in R3.10 ergaenzt. Env-Vars: `SWARMQ_BROKER_URL`, `SWARMQ_BACKEND_URL`, `SWARMQ_RESULT_TTL`, `SWARMQ_CONCURRENCY`, `SWARMQ_QUEUES` (kommasepariert), `SWARMQ_LOG_LEVEL`. Referenz: `aufgabe.md` Abschnitt 14, `docs/recherche-deepening-internals.md` Abschnitt 3.
- R1.3. `src/swarmq/serialization.py` — JSON-Serialisierung mit msgspec und Type-Hooks (datetime, bytes, Decimal, UUID, Enum). Kein orjson in Phase 1 (wird in Phase 4 ergaenzt). msgpack-Serialisierung fuer Results mit Ext-Types. None-Results werden als msgpack nil serialisiert. Referenz: `aufgabe.md` Abschnitt 3, `docs/recherche-deepening-internals.md` Abschnitt 1.
- R1.4. `src/swarmq/task.py` — `Task` Basisklasse mit `task_info: TaskInfo`, `pre_execution()`, `run(*args, **kwargs)` (abstrakt, async), `post_execution()`. `TaskInfo` als frozen dataclass mit Phase-1-Feldern (name, queues, soft_timeout, hard_timeout, delivery, result_ttl). Weitere TaskInfo-Felder in spaeteren Phasen. Auto-Registration via `__init_subclass__`. Referenz: `aufgabe.md` Abschnitt 4, `docs/recherche-deepening-internals.md` Abschnitt 2.
- R1.5. `src/swarmq/registry.py` — `TaskRegistry` als Klasse (kein Singleton, instanziiert durch SwarmQ App). `register()` und `get()` in Phase 1. `discover()` (Auto-Discovery via os.walk) in R3.10 (CLI). Import-Fehler: loggen und ueberspringen. Doppelte Task-Namen: `ConfigurationError`. Referenz: `aufgabe.md` Abschnitt 5, `docs/recherche-deepening-internals.md` Abschnitt 2.

*Broker:*
- R1.6. `src/swarmq/broker/rabbitmq.py` — `RabbitMQBroker` als konkrete Klasse (keine abstrakte Basisklasse in Phase 1). `connect_robust()`, Publisher Confirms. Queue-Deklaration: volle 3-Queue-Topologie ({queue}, {queue}.DQ, {queue}.XQ) mit DLX-Bindings sofort deklarieren — Phase 1 nutzt nur Main-Queue, DQ+XQ stehen aber bereit fuer Phase 2. Referenz: `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 1, `docs/recherche-aio-pika-rabbitmq.md`.

*Backend:*
- R1.7. `src/swarmq/backend/valkey.py` — `ValkeyBackend` als konkrete Klasse (keine abstrakte Basisklasse in Phase 1). `valkey.asyncio.BlockingConnectionPool(max_connections=20, timeout=10)`. `store_result(task_id, result, ttl_ms)` mit SET PX. `get_result(task_id)` mit GET. Kein Lua-Script-Loading in Phase 1. Referenz: `docs/recherche-valkey-py.md`.

*Application:*
- R1.8. `src/swarmq/worker.py` — Einzelner Worker-Prozess: Consumer-Loop empfaengt Tasks, Registry-Lookup, Task-Instanz erzeugt, `pre_execution()` -> `run(*args, **kwargs)` -> `post_execution()` ausfuehrt, Result in Backend speichert, Message ackt. **Fehlerbehandlung Phase 1:** Bei Task-Exception: Error-Result speichern (is_error=true, error_message), Message acken, Error loggen. Bei Registry-Miss (TaskNotFoundError): nack ohne requeue, Error loggen. Bei Backend-Ausfall: nack mit requeue. Kein ProcessManager in Phase 1. Referenz: `aufgabe.md` Abschnitt 7, `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 2.
- R1.9. `src/swarmq/client.py` — Client-API: `schedule(task_name, *args, **kwargs)` publiziert Message an Broker, gibt task_id zurueck. `get_result(task_id, timeout, polling)` pollt Backend. `run()` als Kombination. Referenz: `aufgabe.md` Abschnitt 6.1.
- R1.10. `src/swarmq/app.py` — `SwarmQ` Hauptklasse die Broker, Backend, Config, Registry zusammenbringt. Implementiert `__aenter__`/`__aexit__` fuer Connection-Lifecycle. Sowohl als Client (nur publish/get_result) als auch als Worker nutzbar. Referenz: `aufgabe.md` Abschnitt 2.6.

**Phase 2 — Produktionsfaehiger Kern**

- R2.1. Delivery Modes: AT_LEAST_ONCE (Ack nach post_execution + Result-Write) und FIRE_AND_FORGET (Ack sofort bei Empfang). Beide Modi im Worker-Consumer implementieren. Referenz: `aufgabe.md` Abschnitt 4.2, `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 2.
- R2.2. Retry-System: Bei Task-Fehler pruefen: (a) Exception matcht retry_exceptions UND max_retries nicht ueberschritten → neue Message in `{queue}.DQ` mit TTL (Backoff). (b) Exception matcht retry_exceptions NICHT → direkt in DLQ (`{queue}.XQ`). (c) max_retries ueberschritten → in DLQ. Backoff-Strategien: exponential (default), linear, fixed. Full Jitter. `Retry(delay=)` umgeht retry_exceptions-Check aber respektiert max_retries. `NoRetry()` geht direkt in DLQ. Referenz: `docs/recherche-retry-dlq.md`, `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 4.
- R2.3. DLQ (3-Queue-Topologie): Fuer jede logische Queue drei RabbitMQ-Queues deklarieren: `{queue}`, `{queue}.DQ`, `{queue}.XQ`. DLX-Bindings. Messages die alle Retries erschoepft haben → XQ. Referenz: `docs/recherche-retry-dlq.md`, `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 1.
- R2.4. Soft/Hard Timeouts: Soft-Timeout via Watchdog-asyncio.Task → SoftTimeoutError (fangbar, loest Retry aus). Hard-Timeout via zweiten Watchdog → asyncio.Task.cancel() → nack(requeue=True). Referenz: `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 5.
- R2.5. Graceful Shutdown: 4-Zustandsmaschine (RUNNING → DRAINING → SHUTTING_DOWN → STOPPED). Signal-Handler (SIGTERM/SIGINT). 7-Schritte Sequenz: Consumer stoppen → Scheduler-Lock freigeben → Soft-Timeout → Hard-Cancel → Nack prefetched → Broker close → Backend close. Referenz: `docs/recherche-deepening-broker-lifecycle.md` Abschnitt 3, `prototypes/worker_prototype.py`.
- R2.6. structlog Logging: Setup-Funktion mit QueueHandler + QueueListener (non-blocking). ProcessorFormatter fuer einheitliches JSON. contextvars fuer Task-Kontext. Startup-Report. Periodische Status-Ausgabe. Referenz: `aufgabe.md` Abschnitt 13, `docs/recherche-logging.md`.
- R2.7. Result-TTL: Globaler Default (config.result_ttl) + Task-spezifisch (TaskInfo.result_ttl). Aufloesung: Task → Global → 86400. result_ttl=0 → kein Speichern. Referenz: `aufgabe.md` Abschnitt 2.7.

**Phase 3 — Komfort-Features**

- R3.1. Middleware-System: `Middleware` Basisklasse mit 6 Hooks (pre_enqueue, post_enqueue, pre_execute, post_execute, on_error, on_retry). Stack-Semantik fuer post-Hooks. MiddlewareChain gecacht beim Start. Referenz: `aufgabe.md` Abschnitt 9, `docs/recherche-middleware-signals.md`.
- R3.2. Signal/Event-System: `Signal` Enum (14 Signale). `SignalDispatcher` mit Dekorator-API `@swarmq.on_signal()`. Async-Handler. Fehler in Handlern werden geschluckt und geloggt. Referenz: `aufgabe.md` Abschnitt 10, `docs/recherche-middleware-signals.md`.
- R3.3. Rate Limiting: Sliding Window Counter via Lua-Script in Valkey. String-Syntax ("100/m"). Globales Rate Limiting ueber alle Worker. Rate-limitierte Tasks werden verzoegert (requeue mit Delay). Referenz: `aufgabe.md` Abschnitt 4.1 (rate_limit), `docs/recherche-rate-limiting-locking-dedup.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 3.
- R3.4. Task Locking: Valkey SET NX PX mit Owner-Tracking. Mutex (lock="name") und Semaphore (max_concurrency=N). Konfigurierbares Verhalten bei Konflikt: requeue/skip/raise. Referenz: `aufgabe.md` Abschnitt 4.1 (lock), `docs/recherche-rate-limiting-locking-dedup.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 4.
- R3.5. Unique Tasks / Deduplication: Valkey SET NX PX mit unique_for TTL. unique_until Strategie ("start" vs "success"). DuplicateTaskError bei Duplikat. Stabiler Args-Hash (sha256 + json.dumps sort_keys). Referenz: `aufgabe.md` Abschnitt 4.1 (unique_for), `docs/recherche-rate-limiting-locking-dedup.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 5.
- R3.6. Task Cancellation: Valkey SET fuer Cancel-Flag. Zwei-Phasen: Flag-Check vor Start + Cancel-Monitor-Coroutine waehrend Ausfuehrung. asyncio.Task.cancel() fuer laufende Tasks. Eigener Status CANCELLED (nicht DLQ). Referenz: `aufgabe.md` Abschnitt 6.3, `docs/recherche-workflows-cancel-progress.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 6.
- R3.7. Progress Tracking: `self.update_progress(current, total, message)` API im Task. Valkey HASH + Pub/Sub Notification. Throttling max 1 Update/500ms. Client: `get_progress()` (Polling) + `watch_progress()` (AsyncIterator). Referenz: `aufgabe.md` Abschnitt 6.5, `docs/recherche-workflows-cancel-progress.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 1.
- R3.8. Verzoegerte Ausfuehrung: `schedule(task, delay=60)` und `schedule(task, eta=datetime(...))`. Valkey Sorted Set mit next_run als Score. Embedded Scheduler prueft faellige delayed Tasks. Referenz: `aufgabe.md` Abschnitt 6.2, `docs/recherche-operational-features.md` Abschnitt 1.
- R3.9. Embedded Scheduler: In jeden Worker eingebettet. Leader Election via Valkey Lock (eingebaute Lock-Klasse, thread_local=False). Heartbeat alle 10s. Cron-Parsing. Schedule-Definition am Task (cron= in TaskInfo) und zentral. Dynamische Schedules via CLI/API in Valkey persistiert. Referenz: `aufgabe.md` Abschnitt 8, `docs/recherche-embedded-scheduler.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 8.
- R3.10. CLI: `swarmq worker [module_path] [--queues] [--concurrency] [--log-level] [--burst] [--autoscale] [--metrics-port] [--health-port] [--reload] [--check]`. `swarmq schedule list|add|remove|status`. `swarmq dlq list|inspect|retry|purge`. Referenz: `aufgabe.md` Abschnitt 7.7, 8.6, 11.2, 14.3.

**Phase 4 — Erweiterte Features**

Reihenfolge geändert (2026-04-19): R4.1 Workflows + R4.2 Bulk-Enqueue
nach hinten verschoben. Operator-Features (Priorities, Expiry, Burst,
ProcessManager, Autoscaling) liefern unmittelbaren Production-Wert;
die Workflow-Primitiven sind eine Ergänzung, die einen aktiven
Use-Case braucht, um sinnvoll dimensioniert zu werden.

- R4.3. Task-Prioritaeten: Separate Queues (high/default/low) statt RabbitMQ Priority Queues (Quorum-kompatibel). Gewichtete Consumption (3:2:1). priority-Feld in TaskInfo mapped auf Queue-Suffix. Referenz: `aufgabe.md` Abschnitt 4.1 (priority), `docs/recherche-operational-features.md` Abschnitt 7.
- R4.4. Task Expiry (max_age): Application-Level-Check bei Consumption (Timestamp in Message-Header + max_age → verwerfen wenn abgelaufen). RabbitMQ per-message TTL als Sicherheitsnetz (1.5x max_age). Signal TASK_EXPIRED. DLQ-Eintrag. Referenz: `aufgabe.md` Abschnitt 4.1 (max_age), `docs/recherche-operational-features.md` Abschnitt 8.
- R4.5. Burst Mode: `swarmq worker --burst`. Consumer-Loop mit Idle-Timeout. Exit 0 wenn alle Queues leer. Referenz: `aufgabe.md` Abschnitt 7.5, `docs/recherche-operational-features.md` Abschnitt 2.
- R4.6. ProcessManager: Multiprocessing mit spawn-Kontext. Worker-Prozesse starten/ueberwachen. Auto-Restart bei Crash (max_restarts=5 pro Worker pro Stunde, danach ERROR-Log und Worker als failed markiert). SIGHUP-Reload via os.execvp(). Referenz: `prototypes/worker_prototype.py`, `aufgabe.md` Abschnitt 7.
- R4.7. Worker Autoscaling: `--autoscale=max,min`. ProcessManager passt Worker-Anzahl basierend auf Queue-Tiefe an. Scale-Up bei voller Auslastung, Scale-Down bei Idle. Cooldown zwischen Skalierungen. Referenz: `aufgabe.md` Abschnitt 7.4, `docs/recherche-operational-features.md` Abschnitt 4.

*Verschoben — Bearbeitung nach Phase 5 (oder später, sobald ein
realer Use-Case die API-Form klärt):*

- R4.1. Workflow-Primitiven: `Signature` (via `Task.s()` Klassenmethode und `sig("name", ...)` Funktion). `chain()`, `group()`, `chord()`. Chord-Completion via atomarem Lua INCR+Check in Valkey. Resultat-Weitergabe inline (<64KB) oder via Valkey-Referenz. Referenz: `aufgabe.md` Abschnitt 6.6, `docs/recherche-workflows-cancel-progress.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 7.
- R4.2. Bulk Enqueue (Erweiterung): `schedule_many` mit Batch-Publish + Publisher-Confirms-Pipeline + Valkey-Pipeline fuer Result-Keys. Anmerkung: die Basis-Form von `schedule_many` ist bereits in R3.5 (Unique-Tasks) implementiert; R4.2 hebt sie auf den Throughput-optimierten Modus. Referenz: `aufgabe.md` Abschnitt 6.4, `docs/recherche-operational-features.md` Abschnitt 6.

**Phase 5 — Operations**

- R5.1. Prometheus Metrics: Optionale Dependency. `/metrics` Endpoint auf konfigurierbarem Port. 7 Metriken (tasks_total, task_duration, tasks_active, queue_depth, worker_uptime, retries_total, dlq_messages). Referenz: `aufgabe.md` Abschnitt 12, `docs/recherche-operational-features.md` Abschnitt 5.
- R5.2. Health Check Endpoints: HTTP-Server (minimaler asyncio Server). GET /health (Liveness), GET /ready (Readiness: Broker + Backend verbunden). CLI: `swarmq worker --check`. Referenz: `aufgabe.md` Abschnitt 7.6, `docs/recherche-operational-features.md` Abschnitt 3.
- R5.3. Hot Reload: File-Watching auf Task-Module. Worker-Restart bei Aenderung. SIGHUP als manueller Trigger. Referenz: `aufgabe.md` Abschnitt 7.8. **Verschoben (2026-04-19):** sinnvoll erst nach R4.6 ProcessManager — ohne diesen bleibt Hot Reload eine halbe Loesung (Single-Worker self-execvp statt orchestrierter Subprocess-Restart).
- R5.4. Entwickler-Dokumentation: HTML-Seiten mit Getting Started, Core Concepts, Task-Definition, Client API, Worker, Middleware, Serialisierung, Error Handling, Testing, CLI Reference. Referenz: `aufgabe.md` Abschnitt 17.1.
- R5.5. KI-Agenten-Dokumentation: Maschinenlesbare API-Spec, Message-Formate, CLI-Spec, Entscheidungsbaeume, Beispiele, Fehlerkatalog. Referenz: `aufgabe.md` Abschnitt 17.2. **Teilweise erfuellt (2026-04-19):** mkdocs-Site unter `website/` enthaelt vollstaendige Reference (API + CLI + Configuration + Errors + Changelog) plus Tutorials/Guides/Explanation. Eine separate maschinenlesbare JSON/YAML-Spec ist deferred bis ein konkreter Konsument sie braucht.

## Abhaengigkeitsgraph

```
Phase 0 (Struktur)
  |
Phase 1 (Roundtrip)
  Foundation:
    R1.1 Exceptions ────────────────────────────┐
    R1.2 Config ────────────────────────────────┤
    R1.3 Serialization ────────────────────────┤
    R1.4 Task + TaskInfo ────┐                  │
    R1.5 Registry ───────────┤ (haengt von 1.4)│
  Broker:                                       ├── Alles abhaengig von R1.1
    R1.6 RabbitMQ Broker ───────────────────────┤
  Backend:                                      │
    R1.7 Valkey Backend ────────────────────────┤
  Application:                                  │
    R1.8 Worker ────────────┤ (haengt von 1.3-1.7)
    R1.9 Client ────────────┤ (haengt von 1.3, 1.6-1.7)
    R1.10 App ──────────────┘ (haengt von allem)
  |
Phase 2 (Kern) — alles haengt von Phase 1 ab
  Empfohlene Reihenfolge (R2.6 zuerst fuer Log-Infrastruktur):
  R2.6 Logging ────────── haengt von R1.2, R1.8 (ZUERST implementieren)
  R2.1 Delivery Modes ──── haengt von R1.8
  R2.2 Retry ──────────── haengt von R1.8, R1.6 (fuehrt TaskError, TaskTimeoutError ein)
  R2.3 DLQ ────────────── haengt von R2.2, R1.6 (Routing zu {queue}.XQ, Topologie aus R1.6)
  R2.4 Timeouts ───────── haengt von R1.8 (fuehrt SoftTimeoutError ein)
  R2.5 Graceful Shutdown ─ haengt von R1.8
  R2.7 Result-TTL ─────── haengt von R1.7
  |
Phase 3 (Komfort) — haengt von Phase 2 ab
  |
Phase 4 (Erweitert) — haengt von Phase 3 ab
  R4.6 ProcessManager ──── haengt von R2.6 (Graceful Shutdown)
  R4.7 Autoscaling ─────── haengt von R4.6
  |
Phase 5 (Operations) — haengt von Phase 4 ab
```

## Success Criteria

- **Phase 0:** `pytest` laeuft, docker-compose startet RabbitMQ + Valkey, `pip install -e .` funktioniert
- **Phase 1:** Ein Task kann definiert, gescheduled, ausgefuehrt und das Ergebnis abgerufen werden (End-to-End mit echtem RabbitMQ + Valkey)
- **Phase 2:** Tasks werden bei Fehler automatisch retried, nach max_retries in DLQ verschoben, FIRE_AND_FORGET Delivery Mode funktioniert, Soft-Timeout loest Retry aus, Hard-Timeout cancelt Task, Worker faehrt bei SIGTERM sauber herunter, Result-TTL wird korrekt angewendet, alle Aktionen sind strukturiert geloggt
- **Phase 3:** Middleware/Signals erweiterbar, Scheduler fuehrt Cron-Tasks aus, CLI bedienbar
- **Phase 4:** Workflows (chain/group/chord) funktionieren, Bulk-Scheduling performant
- **Phase 5:** Prometheus Metrics exportiert, Health Checks fuer K8s nutzbar, Docs generiert

## Scope Boundaries

- **Nicht in Scope:** Valkey/Redis als Broker (nur RabbitMQ in Phase 1), Web-Dashboard/Admin-UI, Multi-Tenancy, Django/FastAPI-Integration
- **Spaetere Phase:** Dependency Injection (dishka), Cluster-Support (Valkey Cluster), Monitoring-Dashboard

## Key Decisions

- **Horizontale Schichten:** Erst duennster Roundtrip, dann Features drauflegen — weil jede Phase testbar und lauffaehig sein muss
- **TDD durchgehend:** Jedes Feature wird test-driven entwickelt (AGENTS.md Regeln)
- **Phase 1+2 = Produktionsfaehiger Kern:** Genuegt um echte Tasks zuverlaessig auszufuehren
- **CLI in Phase 3 (nicht Phase 1):** Erst programmatische API, CLI als Convenience danach

## Dependencies / Assumptions

- RabbitMQ 4.x und Valkey 8.x laufen via docker-compose
- Python 3.14+ ist installiert
- Alle Recherche-Dokumente in `docs/recherche-*.md` sind aktuell und korrekt
- AGENTS.md TDD-Regeln werden bei jeder Implementierung befolgt

## Outstanding Questions

### Deferred to Planning
- [Affects R1.6][Technical] aio-pika connect_robust() fuer Reconnect nutzen. Consumer-Recovery in Integration-Tests verifizieren.
- [Affects R3.9][Decision] Cron-Parsing: `croniter` verwenden (ausgereift, 1.2K Stars, aktiv gepflegt, keine C-Extension).
- [Affects R5.4][Decision] HTML-Dokumentation: `MkDocs` mit `mkdocstrings` Plugin (Python-native, Markdown-basiert, einfach).
- [Affects R1.6][Technical] aio-pika Python 3.14 Kompatibilitaet in Integration-Test verifizieren (get_event_loop()-Aufrufe). Smoke-Test in R0.4 Fixtures aufnehmen.

## Next Steps

-> /ce:plan fuer strukturierte Implementierungsplanung, beginnend mit Phase 0 + Phase 1
