---
title: "feat: Phase 3b.3 — Task Cancellation (R3.6)"
type: feat
status: active
date: 2026-04-16
origin: docs/brainstorms/2026-04-16-phase-3b3-task-cancellation-requirements.md
---

# feat: Phase 3b.3 — Task Cancellation (R3.6)

## Overview

SwarmQ bekommt eine Cancel-API nach dem Celery-`revoke` / arq-`abort`-Muster.
`TaskInfo` wird nicht erweitert; stattdessen erhält die Client-API eine neue
Methode `cancel(task_id)`, die ein flüchtiges Valkey-Flag `sq:cancel:{task_id}`
setzt. Worker prüfen das Flag in zwei Phasen: einmal pre-start (kurz nach
Message-Deserialize, vor Semaphore-Acquire) und periodisch während der
Ausführung über eine Monitor-Coroutine. Läuft ein Cancel rein, wird der
laufende Task via `asyncio.Task.cancel()` abgebrochen, ein
`TaskCancelledError` an den Client geliefert, und Retry/DLQ komplett
umgangen. Poll statt Pub/Sub bewusst gewählt, um Connection-Lifecycle-
und Race-Komplexität zu vermeiden — der Brainstorm-Review hat die Pub/Sub-
Entscheidung zugunsten von Poll überstimmt.

## Problem Frame

Aktuell gibt es keinen Weg, einen Task vor oder während der Ausführung
kontrolliert zu stoppen. Stuck-Tasks laufen bis zum `hard_timeout`;
queued Tasks müssen durchlaufen oder per Queue-Purge grob weggeräumt
werden. Das blockiert Use-Cases wie User-Cancel-Buttons, Workflow-Abort,
und Operator-Drain. `asyncio.Task.cancel()`-Plumbing ist teilweise schon
da (Hard-Timeout-Watchdog), aber nicht für externe Ziel-IDs.

(see origin: `docs/brainstorms/2026-04-16-phase-3b3-task-cancellation-requirements.md`)

## Requirements Trace

- **R3.6** (origin: `docs/brainstorms/implementation-order-requirements.md`):
  Valkey-basiertes Cancel-Flag; Zwei-Phasen-Check (pre-start + running);
  `asyncio.Task.cancel()` für laufende Tasks; eigener Status `CANCELLED`
  (nicht DLQ).
- **Closed Decisions aus Brainstorm** (origin §Closed Decisions):
  1. Terminologie `cancel` durchgängig. `Signal.TASK_REVOKED` → `TASK_CANCELLED`.
  2. Poll statt Pub/Sub, Intervall konfigurierbar, Default 1.0 s.
  3. `is_cancelled: bool` im `result_data` + `TaskCancelledError(TaskError)`.
  4. Lock-Release über normalen `finally`-Pfad; zusätzlich `asyncio.shield`.
  5. Cancel bypasst Retry/DLQ vollständig.
  6. TTL = `config.result_ttl`; Worker DEL nach Result-Write und vor Ack.
  7. v1 nur `cancel(task_id) -> bool`; **kein** `cancel_many`.
  8. **Keine** `self.is_cancelled`-Property im Task.
  9. Pre-Start-Check vor Semaphore, Rate-Limit, Lock.
  10. Hard-Timeout vs. Cancel Discriminator: lokale `cancel_reason`-Variable.
  11. Sequencing maintainer-convenience: Cancel jetzt, Scheduler danach.
  12. **Kein** Lua-Script — Standard-Commands reichen.

## Scope Boundaries

**In-Scope:**
- `Client.cancel(task_id)` + `SwarmQ.cancel(task_id)` Delegation
- Neue Exception `TaskCancelledError(TaskError)`
- Rename `Signal.TASK_REVOKED` → `Signal.TASK_CANCELLED`
- Result-Schema-Erweiterung um `is_cancelled` mit backward-compat
- Worker pre-start Cancel-Check + Monitor-Coroutine + cancel_reason-Discriminator
- `asyncio.shield` um `_release_acquired_locks`
- Neue Config-Option `cancel_poll_interval` (Default 1.0 s)
- Backend-Methoden `set_cancel_flag` / `is_cancelled` / `clear_cancel_flag`
- Unit + Integration + E2E Tests

**Out-of-Scope (siehe origin §Non-Goals, Out-of-Scope):**
- `cancel_many` Bulk-API (v1 deferred; `asyncio.gather` reicht)
- Pub/Sub-Push-Architektur (bewusst gegen Poll abgewogen)
- `self.is_cancelled`-Property im Task (YAGNI, kein cooperative-cancel für CPU-Loops)
- Wait-for-effect-Semantik in `cancel()` selbst
- CLI-Subcommand `swarmq cancel` (Phase 3c)
- Workflow-Cancel für Chain/Group/Chord (Phase 4)
- Progress-Tracking-Integration (Phase 3 R3.7)
- Cross-Cluster-Cancel

### Deferred to Separate Tasks

- `cancel_many` + `set_cancel_flags`: nach Workflow-Cancel (Phase 4, R4.1)
- CLI `swarmq cancel <task_id>`: Phase 3c zusammen mit `swarmq dlq` / `swarmq schedule`
- `Signal.TASK_CANCELLED` → Emission beim FAF-Cancel: offen, Empfehlung ja (siehe Open Questions)

## Context & Research

### Relevant Code and Patterns

- **Pattern-Source — R3.4 Locking**:
  - `src/swarmq/locking.py` — Manager-Klasse mit Valkey-AsyncScript-Wrapping
    (hier NICHT Script, aber dieselbe `BackendConnectionError`-Boundary).
  - `src/swarmq/worker.py:_try_apply_lock` (Zeilen 253-502) — Pipeline-Stage-
    Template für den pre-start Cancel-Check (derselbe "early-return-bool"-
    Vertrag, aber ohne Backend-Roundtrip für `sq:cancel:{id}` nötig).
  - `src/swarmq/worker.py:_acquire_and_register` + `asyncio.shield`-Nutzung
    (Zeilen 380-390) — Pattern für `shield` um Release.
- **Pattern-Source — R3.3 Rate-Limit**:
  - `src/swarmq/worker.py:_try_apply_rate_limit` (Zeilen 124-251) — kanonisches
    Pipeline-Stage-Pattern.
- **Pattern-Source — Result-Notification (Pub/Sub kommt NICHT in R3.6)**:
  - `src/swarmq/client.py:get_result` Zeilen 172-218 — subscribe-then-check-
    Lifecycle. In R3.6 wird daraus die Inspiration für robustes
    try/finally um die Monitor-Coroutine, nicht Pub/Sub selbst.
- **Pattern-Source — Config**:
  - `src/swarmq/config.py` Zeilen 11-64 — Env-Var + Programmatic-Override-Muster.
- **Worker-Pipeline**:
  - `src/swarmq/worker.py:_handle_message` Zeilen 675-987 — der pre-start
    Cancel-Check muss VOR der `await self._semaphore.acquire()` (Zeile 701)
    eingebaut werden, also **außerhalb** des großen try/finally-Blocks, das
    die Semaphore released.
  - `src/swarmq/worker.py:_hard_timeout_watchdog` Zeilen 989-994 — der
    Watchdog ruft schon heute `run_task.cancel()`. Der Cancel-Monitor muss
    dasselbe tun, aber vorher `cancel_reason = "cancel"` setzen (bzw. der
    Watchdog muss vorher `cancel_reason = "timeout"` setzen).
  - `src/swarmq/worker.py` Zeilen 827-837 — der bestehende
    `except asyncio.CancelledError` in der Run-Exception-Leiter ist die
    Stelle, an der `cancel_reason` gelesen und verzweigt wird.
  - `src/swarmq/worker.py` Zeilen 978-987 — `finally` mit Lock-Release und
    Semaphore-Release; `_release_acquired_locks` muss in `asyncio.shield`
    gewickelt werden.
- **Result-Serialization**:
  - `src/swarmq/client.py:_unpack` Zeilen 386-392 — nutzt bereits `.get()`,
    also backward-compat für neues Feld `is_cancelled` trivial.
  - `src/swarmq/serialization.py` — msgpack-Encoder benötigt keine
    Änderung, dict-Keys passieren durch.
- **Exception-Inventar**:
  - `src/swarmq/exceptions.py` — `TaskError` schon vorhanden als Basisklasse.
- **Signal-Inventar**:
  - `src/swarmq/signals.py` Zeile 52 — `TASK_REVOKED` existiert bereits,
    wird **nirgends in `src/` dispatched**. Sicherer Rename.

### Institutional Learnings

- **`docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`**
  — vier Mandatory Rules. Für R3.6 relevant: Pre-Start-Check muss VOR
  FIRE_AND_FORGET-Ack laufen; Result-Write + Flag-DEL + Ack in einem
  try/except; Ack-Ordering-Disziplin.
- **`docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`**
  — alle Valkey-I/O an der Boundary in `BackendConnectionError` wrappen,
  damit der strukturierte Error-Path in `_handle_message` greift.
- **`docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`**
  — bei Shutdown-Pfad keine zusätzliche try/except um `close()`;
  `RobustConnection.is_closed` lügt. Relevant für Monitor-Coroutine-
  Cleanup unter Shutdown (siehe Unit 7).
- **`docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`**
  — `nack(requeue=False)` routet via broker-seitiges DLX nach `{q}.XQ`,
  kein Datenverlust. Cancel verlangt stattdessen `ack()`, kein DLQ-Pfad.

### External References

- `docs/recherche-workflows-cancel-progress.md` §8 (arq: Job Abort) und §9
  (SwarmQ-Architektur-Vorschlag) — Poll-basiertes Muster ist dort als
  "einfaches Design" dokumentiert.
- Keine zusätzliche externe Recherche nötig — lokale Patterns (R3.3, R3.4)
  decken alle benötigten Mechaniken ab.

## Key Technical Decisions

- **Poll statt Pub/Sub**: Monitor-Coroutine pollt `backend.is_cancelled(task_id)`
  mit Default-Intervall 1.0 s (konfigurierbar). Rationale: einfache Architektur,
  kein per-Task Valkey-Connection, kein Race-Window zwischen Pre-Start-Check
  und Subscribe, keine Connection-Loss-Edge-Cases. Latenz-Budget 1-2 s für
  User-Cancel-Button akzeptabel. Entspricht arq-Pattern.
- **`cancel_reason`-lokale Variable** als Discriminator zwischen Hard-Timeout-
  Cancel und User-Cancel. Beide Writes laufen auf demselben Event-Loop vor
  dem `.cancel()`-Aufruf → serialisiert. Alternative "Exception-Subclass"
  abgelehnt, weil sie eine dritte Fehlerquelle-Hierarchie einführt und die
  bestehende Hard-Timeout-Branch sprachlich bricht.
- **`SET NX PX` statt Lua**: Ein einziges Valkey-Command reicht für Idempotenz
  + TTL. Returnt `"OK"` wenn neu, `None` wenn bereits gesetzt → direkter
  Boolean-Mapping. Kein Script-Loading, kein NOSCRIPT-Recovery-Pfad, kein
  neuer Lua-File.
- **`asyncio.shield` um Lock-Release + Detached-Release-Set für Shutdown**:
  Der Release-Aufruf wird in `asyncio.shield` gewickelt, um inner-cancel
  (`run_task`) zu überleben. Für outer-cancel (graceful-shutdown via
  `_execute_shutdown`) wird zusätzlich ein Worker-weites
  `self._detached_releases: set[asyncio.Task]` eingeführt: die Shield-
  Coroutine wird als `asyncio.Task` gestartet und in dieses Set
  eingetragen; bei Fertigstellung entfernt sie sich selbst. `_execute_shutdown`
  awaitet nach dem Task-Drain (Zeile 620-630) und VOR `backend.close()`
  (Zeile 657) alle noch laufenden Detached-Releases mit kleinem Timeout
  (1 Valkey-Roundtrip pro Lock). Verhindert Orphan-Releases gegen einen
  geschlossenen Valkey-Client.
- **Result-Schema `is_cancelled: bool`**: neues Feld, via
  `data.get("is_cancelled", False)` gelesen — backward-compat für alle
  Pre-R3.6 Result-Blobs. Keine Schema-Migration nötig.
- **Pipeline-Position pre-start vor Semaphore**: Spart Semaphore-Slot für
  gecancelte Messages. Struktureller Refactor: der bestehende große
  try/finally-Block in `_handle_message` wird nicht geöffnet; Cancel-Check
  läuft in einem eigenen kleinen try/except davor.
- **Phase-Payload im Signal**: Vorerst droppen (kein Subscriber braucht's).
  Signal bekommt nur `task_id` + `task_name`. Wiederaufnahme möglich, wenn
  konkreter Consumer auftaucht. Empfehlung aus Review integriert.
- **FAF + Cancel**: `Signal.TASK_CANCELLED` wird auch für FAF-Tasks
  emittiert (einziger Beobachtungspunkt), aber kein Result gespeichert.
- **`cancel()` für unbekannte `task_id`**: Harmlos zulassen. Flag wird
  gesetzt, verfällt per TTL, keine Validierung. Registry hat sowieso keinen
  Lookup per `task_id`.
- **Flag-DEL-Failure**: log + ack (orphan self-heals via TTL). Neues
  Log-Event `cancel_flag_delete_failed` mit `task_id`.

## Open Questions

### Resolved During Planning

- **Signal-Payload**: droppe `phase`-Feld. Nur `task_id` + `task_name`.
- **FAF + Cancel Signal**: ja, Signal wird emittiert.
- **`cancel()` für unbekannte task_id**: harmlos zulassen.
- **Flag-DEL-Failure**: log + ack.
- **API-Stabilität-Marker**: Release-Doc-Flag „cancel API is experimental
  until v1" in CHANGELOG-Note beim Release dieser Phase; kein Code-Marker.
- **Shutdown-Ordering**: Monitor-Coroutine wird im `finally` des
  Execute-Blocks gecancelt (vor Semaphore-Release und Lock-Release). Bei
  Graceful-Shutdown wird Monitor durch `task.cancel()` des
  `_handle_message`-Tasks mit-gecancelt; Detached-Release-Set sorgt
  dafür, dass noch laufende Lock-Releases vor `backend.close()`
  abgeschlossen werden.
- **Shield-Orphan-Mitigation**: `_detached_releases`-Set im Worker,
  im `_execute_shutdown` vor `backend.close()` awaited (siehe
  Key Technical Decisions und Unit 7 Approach).
- **Client.cancel TTL-Wiring**: `SwarmQ.cancel` übergibt ttl_ms
  per-call an `Client.cancel(task_id, ttl_ms=...)`. Keine Änderung
  an `Client.__init__`. Fallback-Default 86_400_000 ms in
  `Client.cancel`, wenn ttl_ms nicht übergeben wurde.

### Deferred to Implementation

- **Genaue Method-Namen in Worker**: ob die Monitor-Coroutine als
  `_cancel_monitor` oder `_cancel_watcher` benannt wird, entsteht beim
  Schreiben.
- **Test-Timing-Toleranzen**: exakte Wartezeiten in E2E-Tests (z.B.
  `asyncio.sleep(1.5)` für 1s-Poll-Intervall + Puffer) werden beim
  Schreiben kalibriert.
- **Logging-Feld-Schema**: genaue strukturierte Felder in
  `task_cancelled`-Log-Events (z.B. `duration_until_cancel_ms`) nach
  Bedarf ergänzen, wenn Observability-Value erkennbar.
- **Cancel-während-`post_execution`**: akzeptierte Limitation — Cancel-
  Window schließt mit `run_task.done()`, nicht mit `post_execution`-
  Completion. In User-Doku explizit erwähnen.

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification.*

```
Client                     Valkey                    Worker
  │                          │                          │
  │ cancel(task_id)          │                          │
  │─────────────────────────▶│                          │
  │   SET NX PX sq:cancel:.. │                          │
  │                          │                          │
  │                          │      (task in queue)     │
  │                          │          ......          │
  │                          │                          │
  │                          │    Deserialize message   │
  │                          │          ◀───────────────│
  │                          │◀─ EXISTS sq:cancel:..? ──│  Pre-Start-Check
  │                          │── True ─────────────────▶│  (before Semaphore)
  │                          │                          │  ── Store is_cancelled=True
  │                          │                          │  ── DEL sq:cancel
  │                          │                          │  ── Ack message
  │                          │                          │
  │                          │           OR             │
  │                          │                          │
  │                          │                          │  Pre-Start: not cancelled
  │                          │                          │  → Semaphore/RateLimit/Lock
  │                          │                          │  → start run_task
  │                          │                          │  → spawn Monitor-Coroutine
  │                          │                          │
  │                          │◀── EXISTS (every 1s) ────│  Monitor-Loop
  │                          │── False ────────────────▶│
  │                          │          ......          │
  │                          │◀── EXISTS ───────────────│
  │                          │── True ─────────────────▶│
  │                          │                          │  cancel_reason = "cancel"
  │                          │                          │  run_task.cancel()
  │                          │                          │  (CancelledError bubbles)
  │                          │                          │  → finally: shield-release-lock
  │                          │                          │  → store is_cancelled=True
  │                          │                          │  → DEL sq:cancel
  │                          │                          │  → Ack
  │                          │                          │
  │                          │                          │
  │   get_result(task_id)    │                          │
  │─────────────────────────▶│                          │
  │◀── is_cancelled=True ────│                          │
  │  raise TaskCancelledError│                          │
```

**Pipeline-Position im Worker** (bestehend + neu):

```
_handle_message(message):
  1. Deserialize body                          [existing]
  2. bind structlog context                    [existing]
  ──── NEU: Cancel Pre-Start-Check ────
  3. if backend.is_cancelled(task_id):
       store_result(is_cancelled=True)
       clear_cancel_flag(task_id)
       ack(message)
       return
  ────────────────────────────────────
  4. semaphore.acquire()                       [existing, jetzt nach Cancel-Check]
  5. try:
  6.   registry.get(task_name)                 [existing]
  7.   _try_apply_rate_limit()                 [existing]
  8.   _try_apply_lock()                       [existing]
  9.   FAF-Ack                                 [existing]
  10.  ──── NEU: spawn Monitor-Coroutine ────
       cancel_reason: list[str | None] = [None]
       def set_reason_if_unset(r):
           if cancel_reason[0] is None:
               cancel_reason[0] = r
       monitor = asyncio.create_task(
         _cancel_monitor(task_id, run_task, set_reason_if_unset)
       )
       ────────────────────────────────────
  11.  run_task = await run(...)               [existing]
  12.  except CancelledError:
         if cancel_reason[0] == "cancel":      [NEW branch]
           is_cancelled = True
         else:  # timeout or None
           ...existing hard_timeout path...
  13.  finally:
  14.    monitor.cancel(); await monitor       [NEW]
  15.    ──── shield release_locks ────
         await asyncio.shield(_release_acquired_locks(...))
         ─────────────────────────────
  16.    semaphore.release()                   [existing]
```

## Implementation Units

Die Units sind strikt dependency-geordnet. Jede kann atomar committet werden
und die Test-Suite muss am Ende jeder Unit grün sein (siehe AGENTS.md TDD-
Pflicht).

---

- [ ] **Unit 1: Exception + Signal Rename (Foundation)**

**Goal:** `TaskCancelledError` als neue `TaskError`-Subklasse einführen und
`Signal.TASK_REVOKED` → `Signal.TASK_CANCELLED` umbenennen. Alles, was
spätere Units importieren, existiert nach dieser Unit.

**Requirements:** Closed Decisions 1, 3.

**Dependencies:** keine.

**Files:**
- Modify: `src/swarmq/exceptions.py`
- Modify: `src/swarmq/signals.py`
- Modify: `src/swarmq/__init__.py` (Export)
- Test: `tests/unit/test_exceptions.py`
- Test: `tests/unit/test_signals.py`

**Approach:**
- `TaskCancelledError(TaskError)` neu anlegen. Konstruktor explizit
  (kein "analog" — `TaskNotFoundError` hat keinen Parameter):
  ```
  class TaskCancelledError(TaskError):
      def __init__(self, task_id: str | None = None) -> None:
          self.task_id = task_id
          msg = f"task {task_id} was cancelled" if task_id else "task was cancelled"
          super().__init__(msg)
  ```
- In `signals.py`: Enum-Value `"task_revoked"` → `"task_cancelled"`,
  Enum-Name `TASK_REVOKED` → `TASK_CANCELLED`. Docstring-Payload-Sektion
  erweitern um `TASK_CANCELLED: task_id, task_name`.
- **Test-Migration**: `tests/unit/test_signals.py` enthält aktuell eine
  Assertion auf `Signal.TASK_REVOKED.value == "task_revoked"`. Diese muss
  auf `TASK_CANCELLED`/`"task_cancelled"` umgeschrieben werden — sonst
  scheitert die Verification-Regel `grep -rn "TASK_REVOKED" tests/`.
- `__init__.py` Re-Export: `TaskCancelledError`.

**Execution note:** Test-first. RED zuerst für `isinstance(TaskCancelledError, TaskError) is True`.

**Patterns to follow:**
- `TaskError` / `TaskNotFoundError` in `src/swarmq/exceptions.py` (Shape).
- `Signal.TASK_LOCKED` in `src/swarmq/signals.py` (Enum-Value-Format).

**Test scenarios:**
- Happy path — `TaskCancelledError("abc")` ist catchable als `TaskError`
  via `except TaskError`.
- Happy path — `TaskCancelledError` hat `str(exc)` mit `task_id` in der
  Message.
- Happy path — `Signal.TASK_CANCELLED.value == "task_cancelled"`.
- Edge — `Signal.TASK_REVOKED` ist NICHT mehr als Attribut vorhanden
  (AttributeError beim Zugriff).
- Happy path — `from swarmq import TaskCancelledError` funktioniert.

**Verification:**
- `pytest tests/unit/test_exceptions.py tests/unit/test_signals.py -x`
  grün.
- `grep -rn "TASK_REVOKED" src/ tests/` liefert 0 Treffer.
- `grep -rn "TaskCancelledError" src/` liefert mindestens zwei Treffer
  (definition + export).

---

- [ ] **Unit 2: Config-Feld `cancel_poll_interval`**

**Goal:** Neue Config-Option, die das Monitor-Poll-Intervall steuert.
Default 1.0 s. Env-Var `SWARMQ_CANCEL_POLL_INTERVAL`, Validation `> 0`.

**Requirements:** Closed Decision 2 (Poll-Intervall konfigurierbar).

**Dependencies:** Unit 1.

**Files:**
- Modify: `src/swarmq/config.py`
- Test: `tests/unit/test_config.py`

**Approach:**
- Feld `cancel_poll_interval: float = 1.0` in `SwarmQConfig`.
- Env-Override `SWARMQ_CANCEL_POLL_INTERVAL` in `__post_init__`, parsed
  als float.
- Validation: `cancel_poll_interval <= 0` → `ConfigurationError`.

**Execution note:** Test-first, minimales Feld, Triangulation für
Validation.

**Patterns to follow:**
- `result_ttl` / `retry_max_delay` in `src/swarmq/config.py` (Env + Validation).

**Test scenarios:**
- Happy path — Default ist 1.0.
- Happy path — `SWARMQ_CANCEL_POLL_INTERVAL=0.5` setzt Feld auf 0.5.
- Error — `cancel_poll_interval = 0` raised `ConfigurationError`.
- Error — `cancel_poll_interval = -1.0` raised `ConfigurationError`.
- Edge — Env-Var mit Whitespace (`" 2.0 "`) wird korrekt geparst (falls
  strip()-Pattern existiert) ODER error (je nach bestehender Konvention).

**Verification:**
- `pytest tests/unit/test_config.py -x` grün.

---

- [ ] **Unit 3: Backend-Methoden `set_cancel_flag` / `is_cancelled` / `clear_cancel_flag`**

**Goal:** Drei async Methoden auf `ValkeyBackend`, die das Cancel-Flag-
Lifecycle abdecken. Alle wrappen Valkey-Errors in `BackendConnectionError`.

**Requirements:** Closed Decisions 6, 12.

**Dependencies:** Unit 1, 2.

**Files:**
- Modify: `src/swarmq/backend/valkey.py`
- Test: `tests/unit/backend/test_valkey.py`
- Test: `tests/integration/backend/test_valkey_cancel.py` (neu)

**Approach:**
- Key-Helper `_cancel_key(task_id) -> str` analog `_key`:
  `f"sq:cancel:{task_id}"`.
- `async def set_cancel_flag(task_id: str, ttl_ms: int) -> bool`:
  `SET _cancel_key(task_id) "1" NX PX ttl_ms`. valkey-py liefert mit
  `nx=True` einen truthy Wert (True) wenn neu gesetzt, `None` wenn
  bereits existent. Implementierung: `return bool(result)`.
- `async def is_cancelled(task_id: str) -> bool`: `EXISTS` == 1.
- `async def clear_cancel_flag(task_id: str) -> None`: `DEL`. Errors werden
  geloggt aber nicht re-raised (das spätere `log + ack`-Muster in
  Worker-Unit handhabt das Business-Verhalten).
- Alle drei wrappen `ValkeyError` und `OSError` in `BackendConnectionError`
  — `clear_cancel_flag` jedoch fängt `BackendConnectionError` intern,
  loggt, gibt None zurück (siehe Business-Regel: Orphan-Flag heilt via TTL).

**Patterns to follow:**
- `ValkeyBackend.store_result` / `get_result` / `delete_result`
  (`src/swarmq/backend/valkey.py:37-53`) — Error-Wrapping-Shape.

**Test scenarios:**
- Happy path (unit, mocked Valkey) — `set_cancel_flag(id, 1000)` ruft
  `client.set` mit `"1"`, `nx=True`, `px=1000` auf.
- Happy path (unit) — `set_cancel_flag` returnt `True` wenn Mock
  `client.set` truthy liefert (z.B. `True`), `False` wenn Mock `None`
  liefert.
- Happy path (unit) — `is_cancelled` returnt `True`/`False` basierend auf
  `client.exists`-Rückgabe.
- Happy path (unit) — `clear_cancel_flag` ruft `client.delete` auf.
- Error (unit) — `ValkeyError` beim `set_cancel_flag` → `BackendConnectionError`.
- Error (unit) — `ValkeyError` beim `is_cancelled` → `BackendConnectionError`.
- Error (unit) — `ValkeyError` beim `clear_cancel_flag` → wird geloggt,
  nicht re-raised (log.warning mit `task_id`).
- Integration (echter Valkey) — `set_cancel_flag + is_cancelled + clear_cancel_flag`
  round-trip funktioniert.
- Integration — `set_cancel_flag` zweimal mit selbem `task_id`: erstes
  `True`, zweites `False` (NX-Semantik).
- Integration — `set_cancel_flag` mit kurzer TTL (z.B. 100 ms), danach
  `asyncio.sleep(0.2)`, dann `is_cancelled → False` (TTL lief ab).

**Verification:**
- `pytest tests/unit/backend/test_valkey.py -x -k "cancel"` grün.
- `pytest tests/integration/backend/test_valkey_cancel.py -x` grün (mit
  laufendem Valkey).

---

- [ ] **Unit 4: Result-Schema + Client `_unpack`**

**Goal:** Client-seitig `is_cancelled` im Result-Dict erkennen und
`TaskCancelledError` werfen. Worker-seitige Result-Erstellung kommt in
Unit 7.

**Requirements:** Closed Decision 3; Behavior Constraint 5
(Wire-Format backward-compat via `.get()`).

**Dependencies:** Unit 1.

**Files:**
- Modify: `src/swarmq/client.py` (Method `_unpack`)
- Test: `tests/unit/test_client.py`

**Approach:**
- `_unpack` prüft `data.get("is_cancelled", False)` **vor**
  `data.get("is_error")` und wirft `TaskCancelledError(task_id)` mit
  ID aus `data["task"]["id"]`.
- Die Reihenfolge ist wichtig: `is_cancelled` ist-`True` → nicht retryen,
  nicht als Error melden. `is_error`-True danach → `TaskError`.

**Execution note:** Test-first. RED-Test ist ein Result-Dict mit
`is_cancelled=True` → `TaskCancelledError`.

**Patterns to follow:**
- `Client._unpack` bestehende Implementierung in `src/swarmq/client.py:386-392`.

**Test scenarios:**
- Happy path — `_unpack({"task": {"id": "x", ...}, "is_cancelled": True, "is_error": False, "result": None})`
  → raises `TaskCancelledError`, Exception-Message enthält `"x"`.
- Happy path — `_unpack({..., "is_cancelled": False, "is_error": False, "result": 42})`
  returnt `42` (bestehendes Verhalten).
- Happy path — `_unpack({..., "is_cancelled": False, "is_error": True, "error_message": "boom", "result": None})`
  raises `TaskError("boom")` (bestehendes Verhalten).
- Edge (backward-compat) — `_unpack({..., "is_error": False, "result": 99})` OHNE
  `is_cancelled`-Key → returnt `99` (kein KeyError).
- Edge — `_unpack` mit beiden `is_cancelled=True` UND `is_error=True` →
  `TaskCancelledError` gewinnt (priority semantik).

**Verification:**
- `pytest tests/unit/test_client.py -x -k "_unpack or cancel"` grün.

---

- [ ] **Unit 5: Client + App `cancel()`-API**

**Goal:** `Client.cancel(task_id) -> bool` und `SwarmQ.cancel(task_id) -> bool`
Delegation. Fire-and-forget, wartet nicht auf Worker-Reaktion.

**Requirements:** Closed Decision 7.

**Dependencies:** Unit 3 (backend.set_cancel_flag).

**Files:**
- Modify: `src/swarmq/client.py` (neue Methode)
- Modify: `src/swarmq/app.py` (Delegation-Methode)
- Test: `tests/unit/test_client.py`
- Test: `tests/unit/test_app.py`

**Approach:**
- `async def cancel(self, task_id: str, ttl_ms: int = 86_400_000) -> bool`
  in `Client`: akzeptiert TTL als per-call-Parameter mit 24h-Default-
  Fallback. Keine Änderung an `Client.__init__`-Signatur.
- Ruft `await self._backend.set_cancel_flag(task_id, ttl_ms)` auf und
  returnt dessen Bool.
- `async def cancel(self, task_id: str) -> bool` in `SwarmQ`: berechnet
  `ttl_ms = self.config.result_ttl * 1000` und delegiert:
  `return await self.client.cancel(task_id, ttl_ms=ttl_ms)`.
- Validierung `task_id`: non-empty string — sonst `ValueError`.
  Format-Validation (UUID etc.) bewusst nicht erzwungen — Caller ist
  trusted, TTL begrenzt worst-case Memory-Accumulation.

**Execution note:** Test-first. Mock Backend im Client-Unit-Test.

**Patterns to follow:**
- `SwarmQ.schedule` / `SwarmQ.get_result` in `src/swarmq/app.py:106-113`
  (Delegation-Shape).
- `Client.schedule` in `src/swarmq/client.py:88-145` (Method-Structure).

**Test scenarios:**
- Happy path (client) — `await client.cancel("abc", ttl_ms=86400000)`
  ruft `backend.set_cancel_flag("abc", 86400000)` auf und returnt
  dessen Bool-Wert.
- Happy path (client) — `await client.cancel("abc")` (ohne ttl_ms)
  nutzt Default 86_400_000.
- Happy path (client) — Zweiter Cancel-Call auf selben `task_id` returnt
  `False` (weil Mock-Backend False liefert).
- Error — `client.cancel("")` raised `ValueError`.
- Error — Backend wirft `BackendConnectionError` → propagiert zum Caller
  (nicht schlucken).
- Integration (echtes Backend) — `client.cancel(id)` → Valkey-Flag ist
  gesetzt mit TTL.
- Happy path (app) — `await app.cancel("abc")` berechnet
  `config.result_ttl * 1000` und ruft `client.cancel("abc", ttl_ms=...)`
  auf.
- Integration (app) — `SwarmQ.cancel` setzt Flag mit TTL entsprechend
  `config.result_ttl`.

**Verification:**
- `pytest tests/unit/test_client.py tests/unit/test_app.py -x -k "cancel"` grün.
- `pytest tests/integration/backend/test_valkey_cancel.py -x` grün.

---

- [ ] **Unit 6: Worker — Pre-Start-Check**

**Goal:** Direkt nach Deserialize und VOR `semaphore.acquire()` prüft der
Worker `backend.is_cancelled(task_id)`. Falls `True`: Cancel-Result
schreiben, Flag löschen, Message ack'en, Signal emittieren, ohne
Semaphore/Rate-Limit/Lock zu acquirieren.

**Requirements:** Closed Decision 9; Pipeline-Stage-Discipline
(`worker-pipeline-stage-ack-ordering-2026-04-16.md` Rule 1).

**Dependencies:** Units 1, 3, 4.

**Files:**
- Modify: `src/swarmq/worker.py` (`_handle_message`)
- Modify: `src/swarmq/worker.py` (neuer Helper `_handle_cancelled_at_pre_start` oder inline)
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_cancellation.py` (neu)

**Approach:**
- Nach dem Deserialize-Block (Zeilen 677-689) und dem structlog-Bind
  (Zeilen 692-698), ABER VOR dem `await self._semaphore.acquire()`
  (Zeile 701) einen neuen Block einfügen:
  ```
  if await self._backend.is_cancelled(task_id):
      try:
          await self._handle_cancelled_at_pre_start(
              message, task_info_or_None, task_id, task_name, queue
          )
      finally:
          structlog.contextvars.clear_contextvars()
      return
  ```
  Der `finally` mit `clear_contextvars()` ist Pflicht: die Contextvars
  wurden in Zeile 692-698 gebunden und der bestehende finally-Pfad in
  Zeile 987 wird nicht erreicht, weil der Cancel-Check den Semaphore-
  Block nicht betritt.
- Neuer Helper `_handle_cancelled_at_pre_start(message, task_info, task_id, task_name, queue)`:
  - **DeliveryMode-Unterscheidung** (wichtig, fixt Widerspruch zwischen
    Pre-Start und Running-Cancel-Pfad): Ruft `self._registry.get(task_name)`
    auf, um den `DeliveryMode` zu lesen. Bei `TaskNotFoundError`:
    behandelt als `AT_LEAST_ONCE` (Fallback für unbekannte Tasks —
    Result wird gespeichert, damit `get_result()` nicht ewig hängt).
  - Wenn `delivery == FIRE_AND_FORGET` **und** `result_ttl == 0`
    (effektiv „kein Result"): Skip `store_result` und `publish_notification`
    — konsistent mit bestehendem FAF-Pfad in Zeile 950-953.
    Emittiert nur `Signal.TASK_CANCELLED` und ack'd + DEL.
  - Sonst: `encode_result` + `backend.store_result` mit
    `config.result_ttl * 1000`, `backend.publish_notification(task_id)`,
    dann `backend.clear_cancel_flag(task_id)`, dann `broker.ack(message)`,
    dann `Signal.TASK_CANCELLED` dispatchen.
- Die Reihenfolge Result→Flag-DEL→Ack (Brainstorm Behavior Constraint 1)
  wird strikt eingehalten; store_result + clear_cancel_flag + ack in
  einem gemeinsamen try/except.
- **Fehlerpfad**:
  - `is_cancelled`-Call wirft `BackendConnectionError` → `nack(requeue=True)`
    (gleicher Pfad wie bestehender `backend_connection_error` in Zeile 962).
  - `store_result` wirft → `nack(requeue=True)`, **keine** weiteren
    Ops. Flag bleibt, Redelivery sieht Flag erneut.
  - `clear_cancel_flag` kann nicht raisen (Unit 3 spezifiziert log+swallow
    intern); Unit 6 Test-Scenario "clear_cancel_flag wirft" ist eigentlich
    ein Unit-3-Contract-Test — streichen oder nach Unit 3 verschieben.
  - `publish_notification`-Failure wird geloggt+geschluckt (analog
    Happy-Path in Zeile 969-973).
- **Pipeline-Position-Refactor**: Cancel-Check befindet sich **außerhalb**
  des großen try/finally-Blocks, der die Semaphore released (Zeile
  700-987). Das vermeidet, dass die Semaphore acquired+released wird für
  Messages, die ohnehin gecancelt sind.

**Execution note:** Test-first mit Mock-Backend; zuerst RED-Test für den
Pre-Start-Cancel-Pfad, dann minimale GREEN-Implementation, dann
Refactoring wenn nötig.

**Patterns to follow:**
- `_try_apply_lock` in `src/swarmq/worker.py:253-502` — return-early-on-handled-Pattern
  und Ack-Ordering.
- Result-Serialisierung im Happy-Path `_handle_message` Zeilen 920-976.

**Test-Layering-Policy**: Pro Verhalten genau **ein** Real-Infra-Level
wählen. Unit-Tests (mocked Backend) decken **nur** reine Control-Flow-
und Branching-Logik ab (ohne Backend-Roundtrip-Semantik). Integration-
Tests mit echtem Valkey+Broker decken die tatsächliche Backend-
Interaktion ab. Keine Duplikation derselben End-to-End-Flow-Assertion.

**Test scenarios (Unit-Level, mocked — nur Control-Flow):**
- Control-Flow — Cancel-Flag gesetzt: Reihenfolge ist `store_result` →
  `clear_cancel_flag` → `ack` → `Signal.TASK_CANCELLED` dispatched. `run()`
  wird NICHT aufgerufen. `semaphore.acquire` wird NICHT aufgerufen.
  (Mock-Backend liefert `is_cancelled=True`, alle anderen Calls success.)
- Control-Flow — Cancel-Flag nicht gesetzt: normaler Flow, Semaphore
  wird acquired, `run()` wird aufgerufen (Regression-Test).
- Control-Flow — `is_cancelled`-Call wirft `BackendConnectionError`:
  `nack(requeue=True)`, kein store_result, kein ack, kein Signal.
- Control-Flow — `store_result` wirft während Cancel-Pfad:
  `nack(requeue=True)`, kein `clear_cancel_flag`-Call, kein ack.
- Control-Flow — Task-Name nicht in Registry, Cancel-Flag gesetzt:
  `store_result` wird gerufen (AT_LEAST_ONCE-Fallback); Client sieht
  später `TaskCancelledError` statt `TaskNotFoundError`.
- Control-Flow — FAF-Task mit `result_ttl=0` und Cancel-Flag:
  **kein** `store_result`, nur Signal + Flag-DEL + ack.
- Control-Flow — FAF-Task mit `result_ttl>0` und Cancel-Flag:
  `store_result` wird gerufen.

**Test scenarios (Integration, echter Broker + Backend):**
- Integration — Schedule Task, dann `client.cancel(id)`, dann
  Worker-Start: Task läuft nie, Client sieht `TaskCancelledError`.
  Flag ist nach Worker-Verarbeitung in Valkey weg.
- Integration (Cancel im Retry-Delay) — Task mit `max_retries=3`,
  wirft beim ersten Run, landet via `.DQ` mit Delay 2 s. Während des
  Delays `client.cancel(id)`. Nach Delay kommt Message zurück in
  Main-Queue, Pre-Start-Check greift, Client sieht `TaskCancelledError`.
- Integration (Doppel-Cancel) — `client.cancel(id)` → `True`, direkt
  danach `client.cancel(id)` → `False`.
- Integration (Cancel nach Completion) — Task läuft erfolgreich durch
  und schreibt Result. Danach `client.cancel(id)` → `True` (Flag gesetzt,
  niemand konsumiert). `client.get_result(id)` liefert weiterhin das
  echte Result (Cancel hat keinen retroaktiven Effekt).

**Verification:**
- `pytest tests/unit/test_worker.py -x -k "cancel"` grün.
- `pytest tests/integration/test_worker_cancellation.py -x -k "pre_start"` grün.
- Keine Regression in bestehenden `tests/unit/test_worker.py` Tests.

---

- [ ] **Unit 7: Worker — Running-Cancel via Monitor-Coroutine**

**Goal:** Während ein Task läuft, pollt eine Monitor-Coroutine das
Cancel-Flag. Bei `True` wird `cancel_reason="cancel"` gesetzt und
`run_task.cancel()` aufgerufen. `CancelledError` wird im
Exception-Handler über `cancel_reason` als User-Cancel erkannt.
`asyncio.shield` wickelt den Lock-Release ein.

**Requirements:** Closed Decisions 2, 4, 5, 10; Behavior Constraints 4, 6, 7.

**Dependencies:** Units 1, 2, 3, 6.

**Files:**
- Modify: `src/swarmq/worker.py` (`_handle_message`, neuer
  `_cancel_monitor` Helper)
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_cancellation.py` (erweitern)
- Test: `tests/unit/test_worker_timeouts.py` (Regression: Hard-Timeout-Pfad)

**Approach:**
- **`cancel_reason` als nonlocal-Container (first-writer-wins)**: Im
  `_handle_message`-Scope wird ein 1-Element-List `cancel_reason = [None]`
  als mutable Box definiert. (Ein bare `nonlocal`-Variable ginge in einer
  reinen `def`-Umschließung auch, aber eine Liste ist idiomatischer für
  eine Box und übersichtlicher als ein Dict mit Magic-Key.) Beide Setter
  — Monitor und Watchdog — verwenden **compare-and-set**, nicht
  unconditional write: `if cancel_reason[0] is None: cancel_reason[0] = r`.
  Damit gewinnt der **erste** Writer; wenn später das Gegenpart auch noch
  feuert, überschreibt es den ersten Grund nicht. Dies ist die Fix für
  den race condition, der sonst den User-facing Ausgang umdreht.
- Neue Method `async def _cancel_monitor(self, task_id, run_task, set_reason_if_unset)`:
  ```
  while not run_task.done():
      try:
          done, _ = await asyncio.wait(
              {run_task}, timeout=self._config.cancel_poll_interval
          )
          if run_task in done:
              return   # Task beendet — kein Valkey-Poll mehr
          if await self._backend.is_cancelled(task_id):
              set_reason_if_unset("cancel")
              run_task.cancel()
              return
      except Exception as exc:
          logger.warning("cancel_monitor_error", error=str(exc))
          # kein re-raise, Loop läuft weiter
  ```
  Der `asyncio.wait(…, timeout=…)`-Pattern vermeidet den Fall, dass nach
  Task-Completion noch ein Valkey-Poll pro Handler bis zu einem vollen
  `cancel_poll_interval` später feuert — Monitor exitet sofort, wenn
  run_task fertig wird.
- In `_handle_message`: nach `run_task = asyncio.create_task(run_coro)`
  (Zeile 803), Monitor-Coroutine starten:
  ```
  cancel_reason: list[str | None] = [None]
  def set_reason_if_unset(r: str) -> None:
      if cancel_reason[0] is None:
          cancel_reason[0] = r
  monitor = asyncio.create_task(
      self._cancel_monitor(task_id, run_task, set_reason_if_unset)
  )
  ```
- Der Hard-Timeout-Watchdog (`_hard_timeout_watchdog`, Zeile 989-994)
  wird erweitert um denselben `set_reason_if_unset`-Parameter:
  ```
  async def _hard_timeout_watchdog(self, run_task, timeout, set_reason_if_unset):
      await asyncio.sleep(timeout)
      set_reason_if_unset("timeout")
      run_task.cancel()
  ```
  Call-Site in Zeile 808-812 wird entsprechend angepasst. Backward-compat
  NICHT gewünscht — bestehende Regression-Tests für Hard-Timeout passen
  die Call-Site mit an.
- `_handle_message`-Block, der `run_task` awaitet (Zeilen 806-869):
  - Der bestehende `except asyncio.CancelledError` Branch (Zeile 827-837)
    prüft neu `cancel_reason[0]`:
    - `== "cancel"`: `is_cancelled = True`; `task_exception = None`;
      `error_message = ""`.
    - Sonst (`"timeout"` oder `None`): bestehender Hard-Timeout-Pfad
      (RuntimeError mit `hard_err_msg`).
- Im `finally` des `run_task`-Awaits (Zeilen 858-869): Monitor-Coroutine
  canceln und awaiten:
  ```
  if not monitor.done():
      monitor.cancel()
      try:
          await monitor
      except asyncio.CancelledError:
          pass
  ```
  Monitor-Start und -Cancel sind im selben try/finally — eine Exception
  zwischen Monitor-Start und finally-Eintritt kann nicht zu einer
  Leak-Situation führen, da `finally` auch auf Exception läuft.
- **`asyncio.shield` + Detached-Release-Set** (Zeilen 983-984):
  ```
  if acquired_locks and self._lock_manager is not None:
      release_task = asyncio.ensure_future(
          asyncio.shield(
              self._release_acquired_locks(handler_id, acquired_locks)
          )
      )
      self._detached_releases.add(release_task)
      release_task.add_done_callback(self._detached_releases.discard)
      try:
          await release_task
      except asyncio.CancelledError:
          # outer-cancel (graceful-shutdown): lass Release detached
          # weiterlaufen — _execute_shutdown awaitet _detached_releases
          pass
  ```
- Worker bekommt ein neues Feld
  `self._detached_releases: set[asyncio.Task] = set()` im Konstruktor.
- `_execute_shutdown` wird erweitert: nach dem Task-Drain (Zeile 620-630)
  und VOR `backend.close()` (Zeile 657):
  ```
  if self._detached_releases:
      await asyncio.wait(self._detached_releases, timeout=5)
      # übriggebliebene Releases loggen aber nicht blocken
  ```
- **Result-Branch im Exception-Handling**: wenn `is_cancelled` gesetzt
  wurde, bypassed `_evaluate_retry` komplett (analog zu
  `_handle_message`-Zeile 899: neuer Guard `and not is_cancelled`
  ergänzen). Signal-Emission: Im bestehenden Signal-Dispatch-Block
  (Zeilen 873-892) **zusätzlich** die Cancel-Variante einbauen — wenn
  `is_cancelled` → `Signal.TASK_CANCELLED` statt `TASK_COMPLETED` /
  `TASK_FAILED`. Dispatch `TASK_CANCELLED` mit `task_id` + `task_name`.
- **Result-Write**: wenn `is_cancelled` → `result_data` mit
  `is_cancelled=True` ergänzen (Dict-Key hinzufügen bei Build in
  Zeilen 920-925). FAF-Tasks mit `result_ttl=0` nutzen bestehenden
  Early-Return in Zeile 950-953 — **kein** Result-Write, aber Signal
  wurde bereits dispatched. Das ist konsistent zum Pre-Start-Pfad.
- **Flag-DEL**: nach erfolgreichem `store_result` (bzw. bei FAF nach
  Signal-Dispatch, aber vor Ack) wird `backend.clear_cancel_flag(task_id)`
  gerufen — log+swallow bei Failure (Unit-3-Contract).

**Execution note:** Test-first für jeden Pfad. Zuerst Monitor-Coroutine
isoliert testen, dann Integration in `_handle_message`.

**Patterns to follow:**
- `_acquire_and_register` + `asyncio.shield` in `src/swarmq/worker.py:380-390`.
- `_hard_timeout_watchdog` in `src/swarmq/worker.py:989-994`.
- Existierendes `except asyncio.CancelledError` in
  `src/swarmq/worker.py:827-837`.

**Test-Layering-Policy**: Monitor-Coroutine isoliert im Unit-Level
(mocked Backend, reine Control-Flow-Logik). Worker-Pipeline-Interaktion
inklusive Race- und Regression-Szenarien im Integration-Level (echter
Broker+Backend). Keine doppelte Abdeckung derselben Flow-Assertion.

**Test scenarios (Unit-Level, Monitor isoliert — nur Control-Flow):**
- Control-Flow — Monitor: `is_cancelled` returnt `True` → Callback wird
  mit `"cancel"` gerufen, `run_task.cancel()` wird gerufen, Loop exitet.
- Control-Flow — Monitor: `run_task.done() == True` vor erstem Check →
  Loop exitet ohne Valkey-Call.
- Control-Flow — Monitor: `is_cancelled` wirft Exception → Warning
  geloggt, Loop läuft weiter bis zum nächsten Poll.
- Control-Flow — `set_reason_if_unset`: erster Call setzt Value,
  zweiter Call (mit anderem String) lässt Value unverändert.
- Control-Flow — `_handle_message` CancelledError-Branch: wenn
  `cancel_reason[0] == "cancel"` → `is_cancelled=True`; wenn `"timeout"`
  oder `None` → bestehender Hard-Timeout-Pfad.
- Control-Flow — Result-Build: wenn `is_cancelled=True`, dict enthält
  `"is_cancelled": True`; wenn `False`, dict enthält
  `"is_cancelled": False` (backward-compat mit `.get()`).
- Control-Flow — Signal-Dispatch: `is_cancelled=True` →
  `TASK_CANCELLED` dispatched, **nicht** `TASK_COMPLETED` oder
  `TASK_FAILED`.

**Test scenarios (Integration, echter Broker + Backend):**
- Integration — Running-Cancel: Task awaitet `asyncio.sleep(5)`,
  `client.cancel(id)` nach 500 ms, Client sieht `TaskCancelledError`
  via `get_result` innerhalb ~2 s (Poll-Intervall + Puffer).
- Integration (Regression) — Hard-Timeout ohne Cancel: Task mit
  `hard_timeout=1` und `asyncio.sleep(5)`-Body. Kein Cancel gerufen.
  Task wird nach 1 s via Watchdog gecancelt, `cancel_reason = "timeout"`,
  bestehender Hard-Timeout-Pfad greift (Retry oder DLQ wie vorher), kein
  `TaskCancelledError`.
- Integration (Race Watchdog+Monitor, first-writer-wins) — Task mit
  `hard_timeout=2`; `client.cancel(id)` nach 1.9 s gerufen, Flag gesetzt;
  2 s später feuert Watchdog. Monitor-Poll bei ~2.0 s sieht Flag →
  `cancel_reason[0] = "cancel"`; Watchdog ruft danach
  `set_reason_if_unset("timeout")` — Guard verhindert Überschreiben.
  Client sieht `TaskCancelledError`, nicht Hard-Timeout-RuntimeError.
- Integration (Race umgekehrt) — wie oben, aber Watchdog feuert zuerst:
  `cancel_reason[0] = "timeout"`, Monitor kommt danach, Overwrite
  verhindert. Client sieht Hard-Timeout (Cancel kam zu spät, akzeptierte
  Semantik).
- Integration (FAF + running cancel) — FAF-Task mit `result_ttl=0`,
  `client.cancel(id)` während Lauf. `Signal.TASK_CANCELLED` wird
  dispatched, **kein** `store_result` (`valkey-cli EXISTS sq:result:{id}`
  liefert 0).
- Integration (Cancel während `post_execution`) — Task mit schnellem
  `run()` (100 ms) und langsamem `post_execution` (2 s). `cancel` wird
  nach 500 ms gerufen. `run_task` ist bereits fertig → Monitor ist
  schon exited → Cancel hat keinen Effekt, Task endet erfolgreich.
- Integration (Monitor-Leak-Safety) — Exception wird in `run()` nach
  100 ms geworfen (nicht Cancel). Finally-Block canceled Monitor,
  Monitor-Task ist nach `_handle_message`-Ende `done`. Kein aktiver
  `_detached_releases`-Eintrag übrig.
- **Integration (Lock-Successor)** — Task mit
  `TaskInfo(lock="X", max_concurrency=1, hard_timeout=30)` hält Lock,
  Cancel greift, `CancelledError` bubbles, Shield-Release feuert,
  Detached-Release-Task awaited, Lock-Key in Valkey ist weg; ein
  nachgelagerter Schedule mit selbem Lock kann sofort acquirieren.
  Landet in `tests/e2e/test_locking.py` (dort ist die
  Lock-E2E-Infrastructure bereits vorhanden).
- **Integration (Shutdown-with-Monitor)** — Worker bekommt SIGTERM
  während Task läuft + Monitor aktiv; `_execute_shutdown` wartet auf
  Detached-Release-Set; Backend wird erst danach geschlossen; keine
  Orphan-Einträge in Valkey nach Shutdown. Landet in der bestehenden
  Shutdown-E2E (`tests/e2e/test_phase2.py` oder analog).

**Verification:**
- `pytest tests/unit/test_worker.py -x -k "cancel or monitor"` grün.
- `pytest tests/unit/test_worker_timeouts.py -x` grün (keine Regression
  im Hard-Timeout-Pfad).
- `pytest tests/integration/test_worker_cancellation.py -x` grün.
- `pytest tests/e2e/test_locking.py -x -k "cancel"` grün (Lock-Successor-Test).
- `pytest tests/e2e/ -x` grün (Shutdown-with-Monitor-Test).
- Alle 476+ bestehenden Tests weiterhin grün:
  `pytest tests/ -x`.

---

## System-Wide Impact

- **Interaktionsgraph**:
  - `Client.cancel` (neu) ↔ `ValkeyBackend.set_cancel_flag` (neu).
  - `Worker._handle_message` (modifiziert) ↔ `ValkeyBackend.is_cancelled`
    (neu) + `ValkeyBackend.clear_cancel_flag` (neu).
  - Worker-Hard-Timeout-Watchdog bekommt neuen `set_reason_cb`-Parameter
    (Signatur-Change, aber interne API).
- **Error-Propagation**: `TaskCancelledError` propagiert aus
  `Client.get_result` / `SwarmQ.run` ganz normal wie `TaskError`.
  Middleware-Consumer sehen keinen `on_error` für Cancel (brainstorm-
  Closed-Decision: Cancel ist kein Error). Wenn ein Middleware-User
  Cancel beobachten will, muss er `@on_signal(TASK_CANCELLED)` nutzen.
- **State-Lifecycle-Risiken**:
  - Orphan-Cancel-Flag bei DEL-Failure: dokumentierte Limitation,
    TTL heilt.
  - Cancel-Flag für nie-konsumierte task_id: TTL heilt.
  - Monitor-Coroutine-Leak bei Exception im Setup: abgefangen via
    `try/finally` in Unit 7.
- **API-Surface-Parity**: `Client.cancel` + `SwarmQ.cancel` — beides
  verfügbar. `Agent-Native-Parity`: wenn Worker später eine
  Agent-Tool-Schnittstelle bekommt, muss `cancel` dort auch
  verfügbar sein (nicht in diesem Plan-Scope).
- **Integration-Coverage**: Primärer Integrationsscope liegt in den
  Integration-Test-Szenarien von Units 3, 5, 6 und 7 (keine separate
  E2E-Unit). Zwei Full-Stack-E2E-Szenarien docken an die bestehenden
  E2E-Suites an: Lock-Successor in `tests/e2e/test_locking.py`,
  Shutdown-with-Monitor in der Shutdown-E2E.
- **Unchanged invariants**:
  - `TaskInfo` bleibt unverändert (kein neues Feld für Cancel).
  - Broker-Queue-Topologie unverändert (`{q}`, `{q}.DQ`, `{q}.XQ`).
  - Serialisierung: msgspec.json für Messages, msgpack für Results —
    unverändert. Neues `is_cancelled`-Feld ist additiv, backward-compat
    via `.get()`.
  - Rate-Limit- und Lock-Stages bleiben unverändert; Cancel läuft davor.
  - Hard-Timeout-Pfad funktioniert weiter (Regression-Tests erforderlich).

## Risks & Dependencies

| Risiko | Mitigation |
|---|---|
| `cancel_reason`-Discriminator-Race: Watchdog und Monitor feuern gleichzeitig | **First-writer-wins via compare-and-set**: beide Setter nutzen `if cancel_reason[0] is None: cancel_reason[0] = r`, der späte Writer wird ignoriert. Test mit explizitem Interleaving (Unit 7, Edge-Case "Race Watchdog+Monitor"). User-Cancel dominiert Hard-Timeout, wenn Cancel zuerst kommt — das ist die gewünschte Semantik. |
| Monitor-Coroutine-Leak unter Exception | `try/finally` um Monitor-Start und -Cancel in `_handle_message`. Test erzwingt Exception im Setup (Unit 7). |
| Shield-Orphan bei outer-cancel (graceful-shutdown) — orphanierte Release-Coroutine nutzt geschlossenen Valkey-Client | **Mitigation (entschieden): Detached-Release-Set.** Shielded Release-Aufrufe werden als `asyncio.Task` in `self._detached_releases` eingetragen. `_execute_shutdown` awaited sie nach Task-Drain und vor `backend.close()` mit kleinem Timeout. Unit 7-Verification enthält einen simultaneous-Cancel-+-SIGTERM-Test. Shield allein schützt gegen inner-cancel; das Set schützt gegen outer-cancel. |
| Config-Feld `cancel_poll_interval` zu klein → Valkey-Last | Default 1.0 s → 1 `EXISTS` pro aktivem Task pro Sekunde. Bei `concurrency=10` = 10 EXISTS/s per Worker — vernachlässigbar für Valkey. Dokumentiert in Config-Docstring. |
| Worker-Regression: Pre-Start-Refactor bricht bestehende Tests | Hard-Timeout-, Retry-, Lock-, Rate-Limit-Regression-Tests laufen nach jedem Unit. `pytest tests/` vor jedem Commit. |
| Signal-Rename bricht User-Code | Aktuell 0 Treffer für `TASK_REVOKED` in `src/`. Rename ist sicher. CHANGELOG-Note beim Release. |
| FAF-Cancel-Signal-Emission: Dokumentations-Lücke | Im Release-Doc / CHANGELOG explizit erklären: „FAF-Tasks können via `@on_signal(TASK_CANCELLED)` beobachtet werden, Result ist nicht verfügbar." |

## Documentation / Operational Notes

> **Post-Merge Housekeeping** — nicht Teil des Plan-Done-Kriteriums. Nach
> dem Merge separat als kleinen Folge-Commit erledigen (3 Zeilen
> Dokumentations-Edit, kein eigenes Test-Scope). Entspricht dem bisherigen
> Vorgehen bei Phase 3b.1 und 3b.2.

- README-Abschnitt „What works today" um „Task Cancellation (v1: single-id)"
  erweitern.
- CHANGELOG-Note: `Signal.TASK_REVOKED` → `TASK_CANCELLED` (breaking-renamed,
  aber kein User-Code im Repo betroffen). `cancel` API ist experimental,
  Bulk-Variante folgt mit Workflow-Support in Phase 4.
- Neue Config-Env-Var `SWARMQ_CANCEL_POLL_INTERVAL` in README-Config-Tabelle
  aufnehmen.
- Neues Solution-Doc nicht primär nötig; wenn beim Implementieren subtile
  Race-Condition auftaucht, dann `docs/solutions/logic-errors/` analog zu
  `worker-pipeline-stage-ack-ordering` erweitern.

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-16-phase-3b3-task-cancellation-requirements.md](../brainstorms/2026-04-16-phase-3b3-task-cancellation-requirements.md)
- **Vorgänger-Plan (Pattern-Source):** [docs/plans/2026-04-16-008-feat-phase3b2-task-locking-plan.md](2026-04-16-008-feat-phase3b2-task-locking-plan.md)
- **Rate-Limit-Plan (Pattern-Source):** [docs/plans/2026-04-16-005-feat-phase3b-rate-limiting-plan.md](2026-04-16-005-feat-phase3b-rate-limiting-plan.md)
- **Solution — Pipeline-Stage-Ordering:** [docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md](../solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md)
- **Solution — Semaphore/Error-Wrapping:** [docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md](../solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md)
- **Recherche — Cancellation-Muster:** [docs/recherche-workflows-cancel-progress.md](../recherche-workflows-cancel-progress.md) §8-9
- **Roadmap:** [docs/brainstorms/implementation-order-requirements.md](../brainstorms/implementation-order-requirements.md) R3.6
