---
title: "feat: Phase 3a — Middleware-System und Signal/Event-System"
type: feat
status: active
date: 2026-04-16
origin: docs/brainstorms/implementation-order-requirements.md
deepened: 2026-04-16
---

# feat: Phase 3a — Middleware-System und Signal/Event-System

## Overview

SwarmQ bekommt ein erweiterbares Middleware-System für Cross-Cutting Concerns (Metriken, Error-Tracking, Tracing) und ein Signal/Event-System für Task-Lifecycle-Events. Middleware hat 6 Hooks mit Stack-Semantik. Signals haben 14 Events mit Dekorator-API. Beide Systeme sind rein additiv — kein bestehender Code ändert sein Verhalten.

## Problem Frame

SwarmQ hat aktuell keine Möglichkeit für Benutzer, sich in den Task-Lifecycle einzuklinken ohne Task-Code zu verändern. Monitoring, Tracing, Error-Reporting und Custom-Logic erfordern direkten Eingriff in Worker/Client-Code. Middleware und Signals lösen das durch definierte Extension-Points.

(see origin: `docs/brainstorms/implementation-order-requirements.md`)

## Requirements Trace

- R3.1a. `Middleware` Basisklasse mit 6 async Hooks: `pre_enqueue`, `post_enqueue`, `pre_execute`, `post_execute`, `on_error`, `on_retry`
- R3.1b. Stack-Semantik: `pre_*` Hooks in Registrierungsreihenfolge (forward), `post_*` und `on_error` in umgekehrter Reihenfolge (reverse)
- R3.1c. `MiddlewareChain` gecacht beim Start — kein dynamisches Lookup pro Task
- R3.1d. Fehler in Middleware-Hooks: loggen + fortfahren (nicht den Task abbrechen)
- R3.2a. `Signal` Enum mit 14 Signalen (TASK_ENQUEUED bis SCHEDULER_STANDBY)
- R3.2b. `SignalDispatcher` mit Dekorator-API `@app.on_signal(Signal.TASK_COMPLETED)`
- R3.2c. Async-Handler, Fehler in Handlern werden geloggt aber nicht propagiert
- R3.2d. Signals im Worker und Client an den richtigen Stellen feuern

## Scope Boundaries

- Keine Middleware-Ordering-API (`before=`/`after=`) — einfache Registrierungsreihenfolge reicht für Phase 3a
- Kein `SkipTask`-Mechanismus in Middleware (Dramatiq-Feature) — kommt bei Bedarf
- Keine dynamische Middleware-Registrierung zur Laufzeit (nur beim App-Start)
- Signals SCHEDULER_LEADER und SCHEDULER_STANDBY werden definiert aber erst in Phase 3c (Scheduler) gefeuert

### Deferred to Separate Tasks

- Middleware `before=/after=` Ordering: Phase 3b oder spätere Iteration
- Prometheus MetricsMiddleware: Phase 5 (R5.1), nutzt das hier erstellte Middleware-System

## Context & Research

### Relevant Code and Patterns

- `src/swarmq/worker.py` — `_handle_message()` mit klar definierten Lifecycle-Schritten: pre_execution → run → post_execution → retry/error. Middleware-Hooks werden an diesen Stellen eingefügt.
- `src/swarmq/client.py` — `schedule()` und `schedule_many()` mit Publish-Calls. pre_enqueue vor publish, post_enqueue danach.
- `src/swarmq/app.py` — Zentrale App-Klasse. Wird um `add_middleware()` und `on_signal()` erweitert.
- `src/swarmq/task.py` — TaskInfo mit allen Feldern. Middleware erhält TaskInfo als Kontext.

### Institutional Learnings

- `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md` — Async-Callbacks müssen sofort returnen. Relevant: Middleware-Hooks sind await-bar aber sollten kurz sein.

### External References

- `docs/recherche-middleware-signals.md` — Dramatiq Middleware-Pattern, Stack-Semantik, Hook-Design, Signal-Pattern.

## Key Technical Decisions

- **Async Middleware-Hooks:** Alle Hooks sind `async def`. Erlaubt I/O in Middleware (z.B. Metrics an Remote-Service senden) ohne den Event-Loop zu blockieren.
- **Stack-Semantik für post/error Hooks:** `pre_*` forward (1→2→3), `post_*`/`on_error` reverse (3→2→1). Standard-Pattern (Dramatiq, Django). Garantiert symmetrisches Wrap-Verhalten.
- **Middleware-Fehler werden geschluckt:** Exception in einem Middleware-Hook wird geloggt (WARNING) aber nicht propagiert. Der Task-Lifecycle darf nie durch kaputte Middleware abbrechen. Einzige Ausnahme: `pre_enqueue` darf den Enqueue verhindern (raise verhindert Publish).
- **pre_enqueue kann Enqueue verhindern:** Wenn `pre_enqueue` eine Exception wirft, wird der Task NICHT enqueued. Use-Case: Validierung, Rate-Limiting auf Client-Seite.
- **Signal-Handler sind fire-and-forget:** Async Handler, Fehler geloggt, nie propagiert. Ähnlich wie Middleware-Fehler.
- **MiddlewareChain gecacht:** `_build_chains()` beim App-Start. Pro Hook-Typ eine Liste von Callables. Kein dynamisches Lookup pro Task.
- **Signal Enum jetzt, Handler-Verkabelung schrittweise:** Alle 14 Signale werden als Enum definiert. Die tatsächlichen `dispatch()`-Aufrufe werden nur für die aktuell implementierten Features eingefügt (TASK_ENQUEUED, TASK_STARTED, TASK_COMPLETED, TASK_FAILED, TASK_RETRYING, WORKER_ONLINE, WORKER_SHUTDOWN). Scheduler-Signale werden in Phase 3c verdrahtet.
- **on_error + on_retry Semantik (explizit):** `on_error` feuert bei JEDEM Task-Fehler (vor der Retry-Entscheidung). `on_retry` feuert ZUSÄTZLICH wenn der Retry-Pfad gewählt wird. Bei einem retried Fehler feuern BEIDE in Reihenfolge: `on_error` → `on_retry`. Bei DLQ ohne Retry: nur `on_error`. Ein MetricsMiddleware das `tasks.failed` in `on_error` zählt, sollte Retry-Fälle über `on_retry` separat tracken, um Doppelzählung bewusst zu handhaben.
- **Message-Dict Schema (kanonisch):** Plain `dict` (nicht Objekt) mit folgenden Feldern:
  - `task_id: str` (UUID7 String)
  - `task_name: str`
  - `queue: str`
  - `args: list`
  - `kwargs: dict`
  - `delivery_mode: str` (nur bei Worker-Hooks, z.B. "at_least_once")
  - `retry_count: int` (nur bei Worker-Hooks, 0 für ersten Versuch)
  Konsistent zwischen Client-Hooks (pre_enqueue, post_enqueue) und Worker-Hooks (pre_execute, post_execute, on_error, on_retry). Zusätzliche Felder sind hook-spezifische kwargs (z.B. `result`, `exception`, `delay_seconds`).

## Open Questions

### Resolved During Planning

- **Middleware-Message-Objekt:** Middleware-Hooks erhalten ein `dict` mit task_id, task_name, queue, args, kwargs — kein aio-pika Message-Objekt (Broker-Abstraktion). Konsistent für Client- und Worker-seitige Hooks.
- **Signal-Handler Signatur:** Handler empfangen `**kwargs` — jedes Signal hat eigene Payload-Felder (task_name, task_id, duration_ms, exception, etc.). Flexibel und erweiterbar.

### Deferred to Implementation

- Exakte Performance-Auswirkung von Middleware-Chain-Aufrufen pro Task: Wird bei Bedarf mit Benchmarks verifiziert.
- Ob `on_retry` den Retry-Delay als Parameter erhält: Trivial zu ergänzen.

## Implementation Units

- [ ] **Unit 1: Middleware Basisklasse und MiddlewareChain**

**Goal:** `Middleware` Basisklasse mit 6 Hooks und `MiddlewareChain` die die Stack-Semantik implementiert.

**Requirements:** R3.1a, R3.1b, R3.1c

**Dependencies:** Keine

**Files:**
- Create: `src/swarmq/middleware.py`
- Test: `tests/unit/test_middleware.py`

**Execution note:** Test-first.

**Approach:**
- `Middleware` Basisklasse: 6 async no-op Methoden (pre_enqueue, post_enqueue, pre_execute, post_execute, on_error, on_retry). Alle empfangen ein `message: dict` als erstes Argument, plus hook-spezifische kwargs (result, exception, retry_count).
- `MiddlewareChain`: Nimmt `list[Middleware]` im Konstruktor. Methoden `run_pre_enqueue(message)`, `run_post_enqueue(message)`, etc. Pre-Hooks: forward-Order. Post/Error-Hooks: reverse-Order. Jeder Hook in try/except — Fehler geloggt, nicht propagiert.
- **Ausnahme pre_enqueue:** Exceptions propagieren (können Enqueue verhindern).
- Chain wird gecacht — keine dynamische Middleware-Liste.

**Patterns to follow:**
- `docs/recherche-middleware-signals.md` (Dramatiq Middleware Pattern)

**Test scenarios:**
- Happy path: pre_execute wird in Registrierungsreihenfolge aufgerufen (A→B→C)
- Happy path: post_execute wird in umgekehrter Reihenfolge aufgerufen (C→B→A)
- Happy path: on_error wird in umgekehrter Reihenfolge aufgerufen
- Happy path: on_retry empfängt retry_count und delay
- Happy path: Middleware ohne Override → no-op (Default-Methoden)
- Error path: Exception in post_execute-Hook → geloggt, nächster Hook läuft weiter
- Error path: Exception in pre_enqueue → propagiert (verhindert Enqueue)
- Edge case: Leere Middleware-Liste → Chain funktioniert (no-op)
- Edge case: Ein Middleware-Hook ist sync statt async → funktioniert trotzdem (duck-typing)

**Verification:**
- Stack-Semantik korrekt für alle Hook-Typen
- Fehler in Hooks werden nicht propagiert (außer pre_enqueue)

---

- [ ] **Unit 2: Signal Enum und SignalDispatcher**

**Goal:** `Signal` Enum mit 14 Signalen und `SignalDispatcher` mit Dekorator-API.

**Requirements:** R3.2a, R3.2b, R3.2c

**Dependencies:** Keine

**Files:**
- Create: `src/swarmq/signals.py`
- Test: `tests/unit/test_signals.py`

**Execution note:** Test-first.

**Approach:**
- `Signal` Enum: TASK_ENQUEUED, TASK_STARTED, TASK_COMPLETED, TASK_FAILED, TASK_RETRYING, TASK_REVOKED, TASK_EXPIRED, TASK_LOCKED, TASK_RATE_LIMITED, WORKER_ONLINE, WORKER_SHUTDOWN, WORKER_HEARTBEAT, SCHEDULER_LEADER, SCHEDULER_STANDBY
- `SignalDispatcher`: `connect(signal, handler)` registriert async Handler. `dispatch(signal, **kwargs)` feuert alle Handler für das Signal. `on_signal(signal)` als Dekorator.
- Handler sind async. Fehler in Handlern: loggen (WARNING), nie propagieren.
- `dispatch()` ist async und feuert Handler sequenziell (nicht parallel) — deterministische Reihenfolge.

**Patterns to follow:**
- `docs/recherche-middleware-signals.md` (Signal-Pattern)

**Test scenarios:**
- Happy path: Signal.TASK_COMPLETED existiert mit korrektem Wert
- Happy path: Handler registriert via connect() → empfängt dispatch()
- Happy path: Handler registriert via @on_signal Dekorator → empfängt dispatch()
- Happy path: Mehrere Handler pro Signal → alle aufgerufen in Registrierungsreihenfolge
- Happy path: dispatch() mit kwargs → Handler empfängt kwargs
- Error path: Handler wirft Exception → geloggt, nächster Handler läuft
- Edge case: dispatch() ohne registrierte Handler → no-op
- Edge case: Alle 14 Signale sind im Enum definiert

**Verification:**
- Signale können registriert und dispatcht werden
- Handler-Fehler werden geschluckt

---

- [ ] **Unit 3: App-Integration (add_middleware + on_signal)**

**Goal:** SwarmQ App bekommt `add_middleware()` und `on_signal()` API. MiddlewareChain und SignalDispatcher werden an Worker/Client durchgereicht.

**Requirements:** R3.1c, R3.2b

**Dependencies:** Unit 1, Unit 2

**Files:**
- Modify: `src/swarmq/app.py`
- Modify: `src/swarmq/__init__.py` (Exports: Middleware, Signal, SignalDispatcher)
- Test: `tests/unit/test_app.py`

**Approach:**
- `SwarmQ.__init__`: `self._middlewares: list[Middleware] = []`, `self._signal_dispatcher = SignalDispatcher()`
- `add_middleware(middleware: Middleware)` → appends to list
- `on_signal(signal: Signal)` → Dekorator der `_signal_dispatcher.connect()` aufruft
- Bei `__aenter__` oder `start_worker()`: `MiddlewareChain(self._middlewares)` bauen und an Worker/Client übergeben
- `__init__.py`: Export `Middleware`, `Signal` für die Public API

**Test scenarios:**
- Happy path: add_middleware() registriert Middleware
- Happy path: on_signal() registriert Handler
- Happy path: Worker/Client erhalten MiddlewareChain und SignalDispatcher

**Verification:**
- Middleware und Signals sind über die SwarmQ Public API registrierbar

---

- [ ] **Unit 4: Worker Middleware + Signal Integration**

**Goal:** Worker ruft Middleware-Hooks und feuert Signals an den richtigen Stellen im Task-Lifecycle.

**Requirements:** R3.1a, R3.1d, R3.2d

**Dependencies:** Unit 1, Unit 2, Unit 3

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py`

**Approach:**
- Worker bekommt `middleware_chain: MiddlewareChain` und `signal_dispatcher: SignalDispatcher` als Konstruktor-Parameter
- **Exakte Hook-Placements in `_handle_message()`:**
  - `run_pre_execute`: NACH Registry-Lookup (TaskInfo bekannt), VOR `task_instance.pre_execution()`. Innerhalb der Semaphore-Region. message_dict enthält dann task_id, task_name, queue, args, kwargs, delivery_mode, retry_count.
  - `run_post_execute`: NACH erfolgreichem `task_instance.post_execution()`, VOR dem Retry/DLQ-Branch. Wichtig: NICHT direkt nach `run()` — post_execution kann selbst is_error=True setzen.
  - `run_on_error`: Bei JEDEM Task-Fehler (pre_execution, run, post_execution Exception). Nach dem Exception-Catch, VOR `_evaluate_retry`.
  - `run_on_retry`: NACH `_evaluate_retry` entschieden hat dass retried wird, VOR `publish_to_delay_queue`. Empfängt retry_count und delay_seconds.
  - **Reihenfolge bei retried Fehler:** on_error → on_retry (beide feuern)
  - **Reihenfolge bei DLQ-ohne-Retry:** nur on_error (kein on_retry)
- Signals:
  - `start()` nach Connect: dispatch(WORKER_ONLINE, worker_id=...)
  - `_handle_message()` nach Registry-Lookup: dispatch(TASK_STARTED, task_id=..., task_name=...)
  - Bei Erfolg (vor ack): dispatch(TASK_COMPLETED, task_id=..., duration_ms=...)
  - Bei Fehler: dispatch(TASK_FAILED, task_id=..., task_name=..., exception=...)
  - Bei Retry: dispatch(TASK_RETRYING, task_id=..., retry_count=..., delay=...)
  - `_execute_shutdown()` Start: dispatch(WORKER_SHUTDOWN, worker_id=...)

**Test scenarios:**
- Happy path: pre_execute wird vor task.run() aufgerufen
- Happy path: post_execute wird nach erfolgreichem task.run() aufgerufen mit Result
- Happy path: on_error wird bei Task-Exception aufgerufen mit Exception
- Happy path: on_retry wird bei Retry aufgerufen mit retry_count und delay_seconds
- Happy path: Bei retried Fehler feuern BEIDE (on_error dann on_retry)
- Happy path: Bei DLQ-ohne-Retry feuert NUR on_error (kein on_retry)
- Happy path: Bei post_execution-Fehler feuert on_error aber NICHT post_execute
- Happy path: message_dict enthält alle kanonischen Felder (task_id, task_name, queue, args, kwargs, delivery_mode, retry_count)
- Happy path: TASK_STARTED Signal wird bei Message-Empfang dispatcht
- Happy path: TASK_COMPLETED Signal wird bei Erfolg dispatcht mit duration_ms
- Happy path: TASK_FAILED Signal wird bei Fehler dispatcht
- Happy path: WORKER_ONLINE und WORKER_SHUTDOWN Signale werden dispatcht
- Error path: Middleware-Fehler in pre_execute → Task läuft trotzdem
- Edge case: Keine Middleware registriert → Worker funktioniert wie bisher

**Verification:**
- Middleware-Hooks werden im korrekten Lifecycle-Moment aufgerufen
- Signals werden korrekt dispatcht
- Bestehende Worker-Tests bestehen weiterhin (Rückwärtskompatibilität)

---

- [ ] **Unit 5: Client Middleware + Signal Integration**

**Goal:** Client ruft Middleware-Hooks und feuert Signals bei Enqueue-Operationen.

**Requirements:** R3.1a, R3.2d

**Dependencies:** Unit 1, Unit 2, Unit 3

**Files:**
- Modify: `src/swarmq/client.py`
- Test: `tests/unit/test_client.py`

**Approach:**
- Client bekommt `middleware_chain: MiddlewareChain` und `signal_dispatcher: SignalDispatcher` als optionale Konstruktor-Parameter (optional für Client-Modus Rückwärtskompatibilität)
- In `schedule()`:
  - Vor publish: `await middleware_chain.run_pre_enqueue(message_dict)` — Exception verhindert Enqueue
  - Nach publish: `await middleware_chain.run_post_enqueue(message_dict)`
  - Signal: dispatch(TASK_ENQUEUED, task_id=..., task_name=..., queue=...)
- In `schedule_many()`: Hooks pro Task, Signals pro Task

**Test scenarios:**
- Happy path: pre_enqueue wird vor publish aufgerufen
- Happy path: post_enqueue wird nach publish aufgerufen
- Happy path: TASK_ENQUEUED Signal wird dispatcht
- Error path: pre_enqueue Exception → Task wird NICHT enqueued
- Edge case: Client ohne Middleware/Signals → funktioniert wie bisher

**Verification:**
- Client-seitige Middleware-Hooks werden korrekt aufgerufen
- TASK_ENQUEUED Signal wird gefeuert
- Bestehende Client-Tests bestehen weiterhin

---

- [ ] **Unit 6: E2E Middleware + Signals Tests**

**Goal:** Verifiziere dass Middleware und Signals End-to-End mit echtem RabbitMQ + Valkey funktionieren.

**Requirements:** Alle R3.1 + R3.2

**Dependencies:** Alle vorherigen Units

**Files:**
- Create: `tests/e2e/test_middleware_signals.py`

**Test scenarios:**
- Integration: Custom Middleware empfängt pre_execute + post_execute im echten Task-Roundtrip
- Integration: Custom Middleware on_error bei fehlendem Task
- Integration: Signal TASK_COMPLETED Handler empfängt task_id und duration_ms
- Integration: pre_enqueue Exception verhindert Enqueue

**Verification:**
- Middleware und Signals funktionieren End-to-End

## System-Wide Impact

- **Interaction graph:** Worker._handle_message → MiddlewareChain.run_pre_execute → task.run → MiddlewareChain.run_post_execute. Client.schedule → MiddlewareChain.run_pre_enqueue → publish → MiddlewareChain.run_post_enqueue. SignalDispatcher.dispatch an jedem Lifecycle-Punkt.
- **Error propagation:** Middleware-Fehler werden geschluckt (außer pre_enqueue). Signal-Handler-Fehler werden geschluckt. Keine neue Fehlerquelle im Happy Path.
- **API surface parity:** Neue Public API: `SwarmQ.add_middleware()`, `SwarmQ.on_signal()`, `Middleware` Basisklasse, `Signal` Enum. Bestehende APIs bleiben unverändert.
- **Unchanged invariants:** Task-Lifecycle (Retry, DLQ, Timeouts, Shutdown) wird nicht verändert. Middleware und Signals sind rein additiv.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Middleware-Hooks verlangsamen den Hot-Path | Hooks sind async no-op wenn keine Middleware registriert. Chain-Aufruf kostet <1μs wenn leer. |
| pre_enqueue Exception-Propagation ist ungewöhnlich | Explizit dokumentiert. Use-Case: Client-seitige Validierung/Rate-Limiting. |
| Signal-Handler-Reihenfolge nicht deterministic bei concurrent dispatch | dispatch() ist sequenziell (nicht parallel), Reihenfolge = Registrierungsreihenfolge. |

## Sources & References

- **Origin document:** [docs/brainstorms/implementation-order-requirements.md](docs/brainstorms/implementation-order-requirements.md)
- **Middleware/Signals Recherche:** [docs/recherche-middleware-signals.md](docs/recherche-middleware-signals.md)
- **Spezifikation:** [aufgabe.md](aufgabe.md) — Abschnitte 9, 10
- **Existing Code:** `src/swarmq/worker.py`, `src/swarmq/client.py`, `src/swarmq/app.py`
