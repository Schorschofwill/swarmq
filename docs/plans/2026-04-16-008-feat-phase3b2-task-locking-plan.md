---
title: "feat: Phase 3b.2 — Task Locking (R3.4)"
type: feat
status: active
date: 2026-04-16
origin: docs/brainstorms/2026-04-16-phase-3b2-task-locking-requirements.md
---

# feat: Phase 3b.2 — Task Locking (R3.4)

## Overview

SwarmQ bekommt deklaratives Task-Locking analog Celery / Dramatiq. User definiert
`TaskInfo(lock="resource_name")` (Mutex) oder `TaskInfo(lock="resource_name",
max_concurrency=N)` (Semaphore); der Worker stellt sicher, dass die Lock-
Constraints global über alle Worker hinweg gelten. Pattern und Infrastruktur
sind eine direkte Erweiterung von R3.3 Rate-Limiting (Lua-Scripts + Valkey
SET NX PX, Pipeline-Stage-Disziplin aus dem Solution-Doc).

## Problem Frame

Heute kann SwarmQ nicht garantieren, dass eine Task gegenüber sich selbst
serialisiert ausgeführt wird. Bei geteilten Resources (DB-Row, externes API
mit Quota, exklusive Datei) muss der User das selbst lösen — was Production-
Workloads (Payment-Processing, Email-Versand mit Provider-Limits, externe API-
Aufrufe) effektiv ausschliesst. Locking ist Table-Stakes im Wettbewerb (siehe
origin doc).

## Requirements Trace

- **R3.4** (origin: `docs/brainstorms/implementation-order-requirements.md`):
  Valkey SET NX PX mit Owner-Tracking; Mutex (`lock="name"`) und Semaphore
  (`max_concurrency=N`); Konflikt-Verhalten requeue/skip/raise.
- **Brief-Decision-1**: Konflikt-Verhalten v1 = nur `requeue` mit exponential
  Backoff. skip/raise später.
- **Brief-Decision-2**: Owner-Token = random UUID4 pro Acquisition.
- **Brief-Decision-3**: Lock-TTL = `max(30s, 0.25 * hard_timeout)`.
- **Brief-Decision-4 (verschärft im Plan)**: `hard_timeout=None` + Lock → fail-fast
  als `ValueError` im `TaskInfo.__post_init__` (Class-Definition-Import-Zeit).
  Brief sagte "ConfigurationError beim Schedule", aber Class-Definition-Zeit
  ist strenger — Bug erscheint beim Modul-Import statt beim ersten Schedule-
  Aufruf. Plan-Update gegenüber Brief; CHANGELOG-Note bei Release.
- **Brief-Decision-5**: Heartbeat-Renewal NICHT in v1 — akzeptierte Limitation.
- **Brief-Decision-6**: Pipeline-Position = nach Rate-Limit, vor Execute.
- **Brief-Decision-7**: Backoff-Cap-Header = `x-swarmq-lock-conflict-count`,
  Cap-Wert default 10 (analog Rate-Limit).

## Scope Boundaries

- **R3.5 Unique Tasks** — separater Brief, nach Scheduler. NICHT hier.
- **R3.6 Task Cancellation** — Phase 3b.3, separater Plan.
- **Heartbeat-Renewal** — explizit out-of-scope; akzeptierte Limitation
  (max `max(30s, 0.25*hard_timeout)` Sekunden Block nach Worker-Crash).
- **Konflikt-Verhalten skip/raise** — nicht v1, später wenn jemand braucht.
- **Lock-Wait-Queues** (Lock-auf-Lock blockend warten) — nicht im Scope;
  Konflikt = requeue mit Backoff.
- **Lock-Inspection-API** (CLI `swarmq locks list`, `force_release`) — Phase 3c.
- **Cross-Cluster-Lock** — nur ein Valkey-Backend.

### Deferred to Separate Tasks

- **R3.5 Unique Tasks**: eigener Brainstorm + Plan nach R3.8/R3.9 (Scheduler).

## Context & Research

### Relevant Code and Patterns

- **Pattern-Source — R3.3 Rate-Limit**:
  - `src/swarmq/limits.py` — `RateLimiter` Klasse (Konstruktor mit
    `register_script`, `check()` Methode, NamedTuple Config). Direkter
    Vorlage für `LockManager`.
  - `src/swarmq/lua/sliding_window_counter.lua` — Lua-Script-Datei-Pattern.
  - `src/swarmq/lua/__init__.py` — `load_script(name)` Loader.
  - `src/swarmq/worker.py:_try_apply_rate_limit` (lines 90-186) — kanonisches
    Pipeline-Stage-Pattern; `_try_apply_lock` muss exakt dieselbe Shape haben.
  - `src/swarmq/worker.py:_handle_message` lines 282-453 — Pipeline-Position;
    Lock-Stage geht direkt nach Rate-Limit (line 330) und vor FAF-Ack (line 336).
- **TaskInfo-Erweiterung**:
  - `src/swarmq/task.py` lines 19-99 — frozen dataclass mit explicit `__init__`
    + `__post_init__` Validation. Neue Felder müssen alle drei Stellen
    ergänzen (signature, `object.__setattr__`, validation).
- **Worker-Lifecycle / Shutdown**:
  - `src/swarmq/worker.py:_execute_shutdown` lines 237-267 — 5-Schritte
    Shutdown-Sequenz. Lock-Release muss VOR Step 4 (`self._backend.close()`)
    erfolgen, sonst `BackendConnectionError` beim Release.
- **Exception-Inventar**:
  - `src/swarmq/exceptions.py` — `ConfigurationError`, `BackendConnectionError`
    schon vorhanden. Eventuell neue Klasse: `LockUnavailableError` (für skip/raise
    in einer späteren Phase — v1 nicht nötig, weil nur requeue).
- **Signal**:
  - `src/swarmq/signals.py:54` — `Signal.TASK_LOCKED` schon definiert.

### Institutional Learnings

- **`docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`**
  — DAS load-bearing Dokument. Vier Mandatory Rules + 4-Test-Template; nennt
  explizit `_try_apply_lock` und `x-swarmq-lock-conflict-count`. Alle Rules
  übernehmen, ohne Abkürzung.
- **`docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`**
  — Bei Lock-Release-Pfaden unter Shutdown: `RobustConnection.is_closed` lügt;
  nicht try/except um `close()` wickeln (zerstört aio-pika State-Cleanup).
- **`docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`**
  — Alle Valkey I/O (Lock SET NX PX, Lua eval, NOSCRIPT recovery) in
  `BackendConnectionError` an der Boundary wrappen, damit Worker's strukturierter
  Error-Path feuert.
- **`docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md`**
  — Library-spezifische Lifecycle-Contracts schlagen generische "wrap in
  try/except"-Reviewer-Hinweise. Owner-Token-Release Lua hat eine spezifische
  Atomarität — nicht aufweichen.

### External References

- Keine — lokales Pattern (R3.3) ist mehrfach etabliert; externe Recherche
  nicht nötig.

## Key Technical Decisions

- **Lock-State-Repräsentation in Valkey**:
  - **Mutex** (`max_concurrency=1`): einfacher String-Key
    `sq:lock:<lock_name>` mit Owner-Token als Value, TTL via PX.
  - **Semaphore** (`max_concurrency>1`): SortedSet-Key `sq:lock:<lock_name>`,
    Members = Owner-Tokens, Score = Acquisition-Timestamp-ms (für Ablauf-
    Check innerhalb des Lua-Scripts; expired Members werden via ZREMRANGEBYSCORE
    bereinigt). Counter via ZCARD.
  - **Begründung**: Mutex via String ist die etablierte Redlock-Single-Node
    Form (siehe Owner-Token-Release-Pattern). SortedSet für Semaphore erlaubt
    atomare Expiry-Bereinigung im selben Lua-Roundtrip wie der Acquire-Check —
    keine separate Cleanup-Coroutine nötig.

- **Owner-Token-Source**: `uuid.uuid4().hex` pro Acquire-Call.

- **Lock-Tracking-Strategie** (P1-Finding aus Adversarial-Review):
  - **Per-Handler local var**: Jede `_handle_message`-Invocation hat ihre
    eigene `acquired_locks: list[(lock_name, owner_token, max_concurrency)]`
    Variable. Kein zentrales `_active_locks` Dict — task_id kann unter
    AT_LEAST_ONCE-Redelivery oder FAF-Retry zwei concurrent Handler in
    demselben Worker treffen, was bei Dict-Key=task_id den ersten Owner-
    Token überschreiben würde. Per-Handler local ist kollisionsfrei.
  - **Zusätzlich** Worker-zentrale `_active_handler_locks: dict[uuid.UUID,
    list[(lock_name, owner_token, max_concurrency)]]` keyed by per-handler
    `handler_id = uuid.uuid4()`, **nur** für Graceful-Shutdown-Cleanup
    (Unit 5). Per-Handler local ist die canonical source of truth für
    Release; das zentrale Dict ist Backup für SIGTERM während Task läuft.
  - Eintrag in zentrales Dict wird nur als Backup geführt; cleanup beim
    finally entfernt den Eintrag.

- **Lock-TTL-Berechnung** (`_compute_lock_ttl_ms(task_info)`):
  - `hard_timeout=None` → `ConfigurationError` beim ersten Schedule-Versuch
    (Validation in TaskInfo `__post_init__` wenn `lock` gesetzt).
  - `hard_timeout` gesetzt → `max(30, 0.25 * hard_timeout) * 1000` ms.
  - 0.25-Faktor: gibt Lock-TTL einen klaren Sicherheitsabstand zum Hard-Timeout,
    ohne Lock-Recovery-Latenz nach Crash zu sehr in die Höhe zu treiben.

- **Pipeline-Position**: `_try_apply_lock` direkt nach `_try_apply_rate_limit`,
  vor FIRE_AND_FORGET-Ack. Reihenfolge in `_handle_message`:
  1. semaphore acquire
  2. shutdown check
  3. registry lookup
  4. **rate-limit stage** (existing)
  5. **lock stage** (NEW) — `_try_apply_lock` returns bool
  6. FIRE_AND_FORGET ack
  7. middleware + execute
  8. **lock release** (NEW) — im `finally` des execute-Blocks

- **Backoff bei Konflikt**: gleiche Mechanik wie Rate-Limit — Republish in
  Delay-Queue mit `delay = 100ms * 2^conflict_count` (cap 60s); Header
  `x-swarmq-lock-conflict-count` inkrementiert. Bei Header-Wert >= 10 → DLQ-
  Routing (analog Rate-Limit-Cap).

- **Format-String-Locks** (P1-Finding-Update): `lock="user-{user_id}"` wird zur
  Schedule-Zeit NICHT gerendert, sondern erst im Worker beim Lock-Acquire. Render-
  Strategie: `inspect.signature(task_class.run).bind(*args, **kwargs).arguments`
  → liefert OrderedDict mit positional + keyword Args nach Parameter-Namen
  gebunden. Dann `task_info.lock.format_map(bound)`. Vorteil: User schreibt
  `lock="user-{user_id}"` und Task `def charge(user_id, amount)` — funktioniert
  sowohl bei `charge(42, 100)` als auch `charge(user_id=42, amount=100)`.
  Validation beim TaskInfo-Konstruktor: best-effort parse Format-Placeholders +
  Warning bei unbekannten; kein hard error (für `*args`/`**kwargs`-Tasks). Hard
  fail erst im Worker bei Render-Failure → DLQ-Routing mit klarer Error-Meldung.

- **Lock-Release-Discipline** (P0-Finding-Update aus Adversarial-Review):
  Drei-Schicht-Verteidigung gegen Lock-Leaks:
  1. **`asyncio.shield`-protected acquire+register**: Acquire-Lua-Call und das
     anschliessende Register im per-handler `acquired_locks` + zentralem
     `_active_handler_locks` passieren in einem `asyncio.shield()`-geschützten
     Block. Verhindert das Race "acquired but cancelled before register" —
     wenn Cancellation während der shield kommt, wird sie aufgeschoben bis
     register fertig ist.
  2. **try/finally um execute**: nach erfolgreichem Acquire wird der gesamte
     Execute-Pfad (middleware, run, post-execute, retry-evaluate) in einen
     try gewrappt; finally ruft `_release_acquired_locks(acquired_locks)`.
     Release-Helper iteriert die Liste, ruft `release()` für jeden Lock,
     entfernt entsprechenden Eintrag aus `_active_handler_locks`. Dabei:
     **erst release, dann central-dict cleanup** — wenn release wirft, ist
     der zentrale Eintrag noch da und Shutdown-Cleanup wird es erneut
     versuchen. Release-Exceptions im finally NICHT propagieren (log + weiter).
  3. **Shutdown-Cleanup**: `_execute_shutdown` releast alle Einträge in
     `_active_handler_locks` NACH `asyncio.wait(pending, timeout=1)`
     (Step 3 in Worker-Shutdown), VOR `self._backend.close()` (Step 4).
     Diese Reihenfolge ist kritisch: erst sicherstellen, dass alle in-flight
     Tasks fertig/cancelled sind (sonst released man Lock einer noch
     laufenden Task), dann cleanup.

## Open Questions

### Resolved During Planning

- **Semaphore-Implementierung** (Brief-OQ-1): SortedSet (siehe Key Decision
  oben). ZCARD/ZADD/ZREM atomar via Lua, ZREMRANGEBYSCORE für Expiry-Cleanup
  im selben Roundtrip.
- **Lua-Script-Granularität** (Brief-OQ-2): zwei separate Files
  (`lock_acquire.lua`, `lock_release.lua`). Konsistent mit Single-Responsibility-
  Pattern; R3.3 hat ein File, weil dort acquire+check eine Operation ist.
  Hier sind acquire und release verschiedene Operationen mit verschiedenem
  Owner-Check-Logic.
- **Format-String-Validation** (Brief-OQ-3): best-effort beim TaskInfo-
  Konstruktor (parse Format-String, warne bei unbekannten Placeholders, aber
  kein Fail — `*args`/`**kwargs` Tasks zulassen). Hard fail erst zur Lock-
  Acquire-Zeit wenn Render-Args fehlen.
- **Default Backoff-Cap** (Brief-OQ-4): 10 Conflicts, dann DLQ — exakt analog
  Rate-Limit-Cap. Konsistenz wichtiger als Tuning ohne Use-Case-Daten.

### Deferred to Implementation

- **Lua-Script-Test-Setup**: Inline-`SCRIPT LOAD` in Tests vs. eigene Test-
  Lua-Wrapper — entscheiden wenn der erste Lock-Lua-Test geschrieben wird.
- **Concrete `inspect.signature.bind` Edge-Cases**: bei `*args`/`**kwargs`-
  Tasks oder Default-Args konkretisieren, wenn Tests zeigen, dass Render
  ungewöhnlich versagt.
- **Backoff-Cap-Reset bei TTL-Recovery**: aktuell incrementiert Counter
  über alle Retries hinweg; ob ein "TTL ist abgelaufen, Counter resetten"
  Pattern lohnt → entscheiden wenn der erste DLQ-Wedge im Test auftaucht.
- **Comparison-Suite-Reaktivierung** (Brief-OQ-5): NICHT in diesem Plan.
  Reaktivierung kann separat in einem eigenen Setup-Schritt passieren, wenn
  jemand Vorher/Nachher-Throughput messen will.

## Implementation Units

- [x] **Unit 1: Lua Scripts — `lock_acquire.lua` + `lock_release.lua`**

**Goal:** Atomare Lock-Acquire- und Release-Operationen, die sowohl Mutex
(String-Key) als auch Semaphore (SortedSet-Key) abdecken.

**Requirements:** R3.4 (atomarität), Brief-Decisions 2 (owner-token), 3
(TTL-aware).

**Dependencies:** Keine (steht am Anfang der Pipeline).

**Files:**
- Create: `src/swarmq/lua/lock_acquire.lua`
- Create: `src/swarmq/lua/lock_release.lua`
- Test: `tests/integration/test_lock_lua_scripts.py` (echtes Valkey nötig)

**Approach:**
- `lock_acquire.lua`:
  - KEYS[1] = lock_key
  - ARGV[1] = owner_token, ARGV[2] = ttl_ms, ARGV[3] = max_concurrency,
    ARGV[4] = now_ms
  - Wenn `max_concurrency == 1`: `SET KEYS[1] ARGV[1] NX PX ARGV[2]` →
    return 1 (acquired) oder 0 (conflict)
  - Wenn `max_concurrency > 1`:
    - `ZREMRANGEBYSCORE KEYS[1] 0 (now_ms - ttl_ms)` (expired bereinigen)
    - `if ZCARD KEYS[1] >= max_concurrency`: return 0
    - `ZADD KEYS[1] now_ms owner_token`; `PEXPIRE KEYS[1] ttl_ms`; return 1
- `lock_release.lua`:
  - KEYS[1] = lock_key
  - ARGV[1] = owner_token, ARGV[2] = max_concurrency
  - **Admin-Bypass-Reservierung** (Adversarial-F6): wenn `ARGV[1] == "__force__"`,
    DEL bzw. ZREMRANGEBYSCORE der ganzen Key — return 2 (force-released).
    Reserviert die Convention für Phase 3b.3 Cancellation und Phase 3c
    `force_release` CLI-Befehl, ohne dass die Lua-API später inkompatibel
    erweitert werden muss.
  - Wenn `max_concurrency == 1`: `if GET == owner: DEL` → return 1 (released)
    oder 0 (foreign owner)
  - Wenn `max_concurrency > 1`: `ZREM KEYS[1] owner_token` → return 1 oder 0

**Patterns to follow:**
- `src/swarmq/lua/sliding_window_counter.lua` — Header-Kommentar, ARGV/KEYS-
  Doku oben im File.
- `docs/recherche-deepening-lua-scripts.md` Abschnitt 4 — Lock-Pattern-Details.

**Test scenarios:**
- Happy path: Mutex acquire → release → re-acquire möglich; return-Codes korrekt.
- Happy path: Semaphore mit max_concurrency=3 — 3 acquires erlaubt, 4. blockt.
- Edge case: TTL läuft ab → Mutex automatisch frei (re-acquire möglich nach Wait).
- Edge case: Semaphore Expiry-Cleanup — Members mit altem Timestamp werden bei
  acquire automatisch entfernt.
- Error path: Foreign-Owner Release → return 0, Lock bleibt held.
- Error path: Release auf nicht-existenten Key → return 0, kein Crash.

**Verification:**
- Beide Scripts laden via `load_script()` ohne Fehler.
- Integration-Tests gegen echtes Valkey grün.

---

- [x] **Unit 2: `LockManager` Klasse + Lock-TTL-Berechnung**

**Goal:** Python-API um die Lua-Scripts. Hält registered AsyncScripts (NOSCRIPT-
Recovery via Valkey-py); bietet `acquire()` und `release()`.

**Requirements:** R3.4 (Owner-tracking, TTL-Berechnung), Brief-Decisions 2 + 3.

**Dependencies:** Unit 1.

**Files:**
- Create: `src/swarmq/locking.py`
- Test: `tests/unit/test_locking.py`
- Test: `tests/integration/test_locking_manager.py`

**Approach:**
- Klasse `LockManager`:
  - `__init__(client: valkey_asyncio.Valkey)` — registriert beide Lua-Scripts
    via `client.register_script(...)`.
  - `async acquire(lock_name: str, owner_token: str, ttl_ms: int, max_concurrency: int) -> bool`
    — wrappt Lua, nutzt server-side `TIME` für `now_ms` (analog `RateLimiter`).
  - `async release(lock_name: str, owner_token: str, max_concurrency: int) -> bool`
    — wrappt Lua, kein TIME nötig.
  - Beide Methoden wrappen Valkey-Exceptions als `BackendConnectionError`
    (Pattern aus `solutions/.../async-semaphore-deadlock-...md`).
- Helper-Funktion `compute_lock_ttl_ms(hard_timeout: int | None) -> int`:
  - `None` → `ValueError("hard_timeout required when lock is set")`
  - sonst → `max(30, 0.25 * hard_timeout) * 1000`
  - Standalone-Funktion (nicht Methoden), damit Tests sie ohne LockManager
    aufrufen können.

**Patterns to follow:**
- `src/swarmq/limits.py` — `RateLimiter` Klasse als direkte Vorlage (gleiches
  `register_script`-Pattern, gleiche Wrapper-Form).

**Test scenarios:**
- Happy path: `acquire(...)` returnt True wenn Lock frei; False wenn held.
- Happy path: `release(...)` returnt True wenn Owner stimmt; False bei foreign.
- Edge case: `compute_lock_ttl_ms(None)` raises ValueError.
- Edge case: `compute_lock_ttl_ms(10)` returnt `max(30, 2.5) * 1000` = 30000.
- Edge case: `compute_lock_ttl_ms(300)` returnt `max(30, 75) * 1000` = 75000.
- Error path: Valkey-Disconnect → `BackendConnectionError` (nicht raw Valkey-
  Exception).
- Integration: 10 parallele `acquire()` Tasks auf gleichem Lock-Name → genau
  1 erfolgreich (Mutex), genau 3 erfolgreich (Semaphore mit max_concurrency=3).

**Verification:**
- Alle Unit-Tests grün ohne Valkey.
- Integration-Tests grün mit echtem Valkey.

---

- [x] **Unit 3: `TaskInfo` — neue Felder `lock`, `max_concurrency`**

**Goal:** Public-API-Erweiterung. Validation-Errors früh (beim Class-Definition
import), nicht im Worker.

**Requirements:** R3.4 (deklarative API), Brief-Decision 4 (`hard_timeout=None`
→ ConfigurationError beim Schedule).

**Dependencies:** Keine.

**Files:**
- Modify: `src/swarmq/task.py`
- Test: `tests/unit/test_task.py` (vorhandene Datei erweitern)

**Approach:**
- Neue Felder im `dataclass`:
  - `lock: str | None = None`
  - `max_concurrency: int = 1`
- `__init__` analog erweitern: zwei neue keyword-Args, zwei neue
  `object.__setattr__`-Aufrufe.
- `__post_init__` Validation (alle als `ValueError` beim TaskInfo-Konstruktor —
  fail-fast bei Modul-Import, siehe Brief-Decision-4 verschärft):
  - Wenn `lock` gesetzt UND `hard_timeout is None` → `ValueError("lock=...
    requires hard_timeout to be set; see Brief-Decision-4")`.
  - Wenn `max_concurrency < 1` → `ValueError`.
  - Wenn `max_concurrency != 1` UND `lock is None` → `ValueError("max_concurrency
    requires lock to be set")`.
- Format-String-Best-Effort-Check: wenn `lock` Format-Placeholders enthält
  (`{...}`), kein hard error — Render-Validation passiert zur Lock-Acquire-Zeit
  (Unit 4) via `inspect.signature.bind`. (Best-effort Warning gestrichen — der
  bind-Approach handhabt positional+keyword sauber, sodass die Warning meist
  nur Noise wäre.)

**Patterns to follow:**
- `src/swarmq/task.py` lines 19-99 — bestehende `TaskInfo` Validation als
  Vorlage (z. B. `rate_limit` parse-eager-Check).

**Test scenarios:**
- Happy path: `TaskInfo(lock="x", hard_timeout=60)` instanziiert ohne Fehler.
- Happy path: `TaskInfo(lock="x", max_concurrency=3, hard_timeout=60)` ditto.
- Edge case: `TaskInfo(lock="x")` ohne hard_timeout → ValueError mit klarer
  Message.
- Edge case: `TaskInfo(max_concurrency=3)` ohne lock → ValueError.
- Edge case: `TaskInfo(lock="x", max_concurrency=0, hard_timeout=60)` → ValueError.
- Edge case: `TaskInfo(lock="x", max_concurrency=-1, hard_timeout=60)` → ValueError.
- Edge case: `TaskInfo(lock="user-{user_id}", hard_timeout=60)` → kein Fehler
  (kein Warning — Render geschieht zur Acquire-Zeit, siehe Unit 4).

**Verification:**
- Alle bestehenden TaskInfo-Tests bleiben grün.
- Neue Tests grün.

---

- [ ] **Unit 4: Worker `_try_apply_lock` Pipeline-Integration**

**Goal:** Lock-Stage im Worker-Pipeline. Folgt strikt den 4 Mandatory Rules
aus `docs/solutions/.../worker-pipeline-stage-ack-ordering-...md`.

**Requirements:** R3.4 (Pipeline-Integration), Brief-Decisions 6 (Pipeline-
Position) und 7 (Backoff-Header).

**Dependencies:** Units 2, 3.

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py` (erweitern um Lock-Klassen)

**Approach:**
- Im `Worker.__init__`:
  - `self._lock_manager: LockManager | None = None`
  - `self._active_handler_locks: dict[uuid.UUID, list[tuple[str, str, int]]] = {}`
    — Backup für Shutdown-Cleanup, **nicht** primary release source. Key ist
    eine pro-`_handle_message`-Aufruf erzeugte UUID, nicht task_id (vermeidet
    Kollisionen unter AT_LEAST_ONCE-Redelivery).
- Im `Worker.start()`: `self._lock_manager = LockManager(self._backend._client)`
  (analog `RateLimiter` line 196).
- Neue Methode `_try_apply_lock(message, task_info, queue, task_id, task_name,
  args, kwargs, task_class, handler_id, acquired_locks) -> bool`:
  - `handler_id` = `uuid.uuid4()` vom Caller (`_handle_message`) erzeugt
  - `acquired_locks` = leere Liste vom Caller bereitgestellt; bei erfolgreichem
    Acquire wird `(lock_name, owner_token, max_concurrency)` an die Liste
    angehängt und parallel in `self._active_handler_locks[handler_id]` gespiegelt
  - Rule 1: läuft VOR FIRE_AND_FORGET-Ack (siehe `_handle_message`-Reihenfolge)
  - Rule 2: try/except mirror um beide kritische Operationen
  - Rule 3: `x-swarmq-lock-conflict-count` Header, Cap = 10 → DLQ. **DLQ-
    Message muss `lock_name` carry** (Adversarial-F3 Operator-Recovery): in
    `lock_diagnostic_headers = {"x-swarmq-lock-name": rendered_name,
    "x-swarmq-lock-conflict-count": count}` — Operator kann manuell
    `valkey-cli DEL sq:lock:<name>` zur Recovery.
  - Rule 4: Single helper, return bool
  - **Format-String-Render**: wenn `task_info.lock` `{...}` enthält → 
    `bound = inspect.signature(task_class.run).bind(*args, **kwargs).arguments`,
    dann `lock_name = task_info.lock.format_map(bound)`. Bei TypeError oder
    KeyError → log + DLQ (User-Konfigurationsfehler), DLQ-Message mit Header
    `x-swarmq-lock-render-error: <message>`.
  - TTL berechnen via `compute_lock_ttl_ms(task_info.hard_timeout)`.
  - Owner-Token = `uuid.uuid4().hex`; **acquire+register asyncio.shield-protected**
    (P0-Finding aus Adversarial-F2):
    ```
    async with asyncio.shield(... innerer try mit acquire+register ...):
    ```
    Verhindert die Race "acquired aber CancelledError vor Register". Wenn
    Cancellation während shield kommt, wird sie aufgeschoben.
  - Bei Konflikt (acquire returnt False): Counter-Header inkrementieren,
    `Signal.TASK_LOCKED` dispatchen, Republish in Delay-Queue mit
    Backoff `100ms * 2^count` (cap 60s).
  - Bei Counter >= 10: nicht mehr requeuen, sondern direkt in DLQ.
- In `_handle_message`:
  - Vor dem Rate-Limit-Check: `handler_id = uuid.uuid4()`, `acquired_locks: list = []`
  - Nach `_try_apply_rate_limit` (line 330) neue Zeile:
    ```
    if await self._try_apply_lock(message, task_info, queue, task_id, task_name,
                                  args, kwargs, task_class, handler_id, acquired_locks):
        return
    ```
  - Lock-Release passiert in Unit 5 (try/finally um execute-block).

**Execution note:** Test-first für die 4 Mandatory-Regression-Tests aus dem
Solution-Doc — Pattern wurde bei R3.3 etabliert und ist die Lehre, dass
diese Tests VOR der Implementation geschrieben werden müssen, damit die
FIRE_AND_FORGET-Reihenfolge nicht erneut zertrennt wird.

**Patterns to follow:**
- `src/swarmq/worker.py:_try_apply_rate_limit` lines 90-186 — exakte Vorlage.
- `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`
  — Rules 1-4 streng befolgen.
- `tests/unit/test_worker.py` — `TestWorkerRateLimiting`, `TestFireAndForgetRateLimit`,
  `TestRateLimitRequeueCounter` als Test-Templates.

**Test scenarios:**
- Happy path: `lock="x"` Task acquired, läuft, released — Lock danach frei.
- Happy path: `lock="x", max_concurrency=3` — 3 parallele Tasks laufen, 4. wird
  requeue-delayed.
- Mandatory regression: `test_lock_set_but_lock_manager_none_falls_through_to_execution`
  — Defensive Guard.
- Mandatory regression: `test_lock_check_connection_error_nacks_with_requeue`
  — Valkey-Fault-Pfad.
- Mandatory regression: `TestFireAndForgetLock::test_fire_and_forget_lock_failure_does_not_drop_message`
  — kritisch.
- Mandatory regression: `TestLockRequeueCounter::test_*` — Counter-Header.
- Edge case: `lock="user-{user_id}"` mit `kwargs={"user_id": 42}` → Lock-Name
  wird zu `"user-42"`.
- Edge case: `lock="user-{user_id}"` ohne `user_id` in args/kwargs → DLQ.
- Edge case: Counter erreicht 10 → DLQ statt Requeue.

**Verification:**
- Alle 4 Mandatory-Regression-Tests existieren und sind grün.
- `_handle_message` enthält die neue Zeile nach Rate-Limit, vor FIRE_AND_FORGET-
  Ack.
- `tests/unit/test_worker.py` Test-Suite bleibt grün insgesamt.

---

- [ ] **Unit 5: Lock-Release Discipline (try/finally + Shutdown-Cleanup)**

**Goal:** Lock wird IMMER freigegeben — bei success, exception, soft-timeout,
hard-cancel, AND graceful shutdown.

**Requirements:** R3.4 (kein Lock-Leak), Success-Criterion "Lock-Release
garantiert auch bei Exception".

**Dependencies:** Unit 4.

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/integration/test_worker_lock_release.py`

**Approach:**
- Im `_handle_message`: das **bestehende** äussere `try` (lines 309-571 in
  worker.py heute, das die Semaphore-Acquire/Release wrappt) wird zum Träger
  des Lock-Release-Finally. Konkret:
  - Direkt nach `await self._semaphore.acquire()` (line 308) → `acquired_locks: list = []`
    und `handler_id = uuid.uuid4()` deklarieren.
  - Im **bestehenden** `finally` (line ~568, vor `self._semaphore.release()`):
    `await self._release_acquired_locks(handler_id, acquired_locks)` aufrufen.
  - Neue Methode `async _release_acquired_locks(handler_id, acquired_locks) -> None`:
    iteriert die Liste, ruft `self._lock_manager.release(name, token, max_conc)`
    pro Eintrag, **fängt Exceptions** (log error, weiter), entfernt
    `self._active_handler_locks.pop(handler_id, None)` am Ende. Ordnung:
    erst alle releases versuchen, dann central-dict cleanup — wenn ein release
    wirft, bleibt das Backup intakt für Shutdown.
  - **WARUM bestehender finally statt neuer dedicated finally**: Vermeidet
    neue try/finally-Schachtelung im 250-Zeilen-`_handle_message`. Lock-Hold
    bleibt während Result-Store + Retry-Republish (~1-2 Valkey-Roundtrips
    extra) — akzeptabel, weil diese Operationen schnell sind. Alternative
    (Release vor Result-Store) bricht Atomicity: wenn Result-Store fehlschlägt
    und retry geschedulet wird, der neue Versuch könnte Lock noch gehalten
    sehen.
- Im `_execute_shutdown` (lines 237-267): nach Step 3 (`asyncio.wait(pending,
  timeout=1)`, line 259) und VOR Step 4 (`self._broker.close()`, line 263)
  einen neuen Step einfügen:
  - "3b. Release any leftover handler locks": für alle Einträge in
    `self._active_handler_locks`, `release()` für jeden Lock aufrufen, Errors
    loggen aber nicht failen, dann Dict leeren.
  - **Reihenfolge ist kritisch** (P2-Finding aus Feasibility-Review): MUSS nach
    `asyncio.wait(pending, ...)` kommen, NICHT davor. Sonst Race: cancelled
    Task läuft noch kurz, Lock bereits released, anderer Worker acquired,
    erste Task crasht in shared Resource.
  - Begründung der neuen Position: Wenn der Worker mit Locks im finally
    landet UND der finally aus irgendeinem Grund nicht durchläuft (Task wurde
    in Step 3 cancelled bevor finally), läuft 3b als letzter Sicherheitsnetz.
    Für die normale Drain (Step 2) erfolgt Release schon im handler-finally.

**Patterns to follow:**
- `src/swarmq/worker.py:_execute_shutdown` lines 237-267 — bestehende Shutdown-
  Sequenz; der neue Step kommt zwischen "wait for in-flight" und "close
  connections".
- `docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md`
  — `close()` nicht in try/except wrappen (zerstört State-Cleanup).

**Test scenarios:**
- Happy path: Task läuft, success → Lock freigegeben → re-acquire möglich.
- Edge case: Task wirft Exception → Lock freigegeben (kein Leak).
- Edge case: Task wird durch soft-timeout → SoftTimeoutError → Lock
  freigegeben (regression-relevant).
- Edge case: Task wird durch hard-cancel cancelled → Lock freigegeben.
- Edge case: Worker SIGTERM während Task läuft → graceful shutdown, Lock
  freigegeben vor Backend-Close.
- Error path: Release wirft (Valkey down): Error geloggt, Worker macht weiter.

**Verification:**
- 10 parallele Schedule + Task-Crash-Szenarien: Lock ist nach jedem Crash
  innerhalb max(30s, 0.25*hard_timeout) wieder frei (Test mit Wartezeit + neuem
  Acquire).
- Graceful-Shutdown-Test: Worker hat 5 active Locks, SIGTERM gesendet, neuer
  Worker kann sofort acquired (vor TTL-Wartezeit).

---

- [ ] **Unit 6: End-to-End Integration Tests + Comparison-Update**

**Goal:** Multi-Worker-Szenarien gegen echte Docker-Stack; Brief Success-Criteria
verifizieren.

**Requirements:** Alle Brief-Success-Criteria.

**Dependencies:** Units 1-5.

**Files:**
- Create: `tests/integration/test_locking_e2e.py`

**Approach:**
- Test-Setup: 2-3 separate `Worker`-Instanzen gegen denselben Broker + Valkey
  (analog zu vorhandenen Multi-Worker-E2E-Patterns).
- Szenarien (jedes als eigene `async def test_*`):

**Test scenarios:**
- Happy path: Mutex über 2 Worker — `lock="x"`, 10 Tasks geschedulet, Tasks
  loggen Start+End-Timestamps in eine geteilte Liste; Verifikation: keine zwei
  Tasks haben überlappende Intervalle.
- Happy path: Semaphore über 2 Worker — `max_concurrency=3`, 10 Tasks; max 3
  überlappen sich.
- Edge case: Format-String-Lock `lock="user-{user_id}"` — gleicher user_id
  wird sequenziert; verschiedene user_ids laufen parallel.
- Edge case: Worker A acquired Lock, dann SIGKILL des Workers — Worker B kann
  nach max(30s, 0.25*hard_timeout) Sekunden Lock acquire (TTL-Recovery).
- Edge case: Worker mit `lock="x"`, hard_timeout=60s → effektive Lock-TTL = 30s
  (max(30, 15)).
- Error path: Counter-Cap-Test — bewusst held lock + 11 schedules, der 11. wird
  in DLQ statt requeue. DLQ-Message muss `x-swarmq-lock-name` Header carry.
- **Chaos-Test 1** (Adversarial-F4): SIGKILL des Workers MIT held lock — neuer
  Worker muss innerhalb der per-Test-konfigurierten TTL Lock acquired (nicht
  via graceful shutdown, sondern via Process-Kill `os.kill(pid, signal.SIGKILL)`
  in einem subprocess-test).
- **Chaos-Test 2** (Adversarial-F4): SIGKILL während `asyncio.shield`-Block —
  ist Lock acquired oder nicht? Test mit acquire-instrumentation, dass entweder
  beide (Lock + central-dict-Eintrag) gesetzt sind oder keines.
- **Chaos-Test 3** (Adversarial-F2): Cancellation während `_handle_message`
  zwischen Lock-Acquire und Execute-Begin — verifiziert dass acquired_locks
  konsistent ist und finally-Release greift.

**Verification:**
- Alle E2E-Tests grün gegen `docker compose up -d` Stack.
- Test-Count gesamt: ~510-525 (von 478 vorher).
- `swarmq` Pakettests vollständig grün.

## System-Wide Impact

- **Interaction graph:**
  - Worker-Pipeline-Stages (Rate-Limit, Lock, Future-Unique, Future-Cancel) sind
    eine geordnete Kette — alle folgen den 4 Mandatory Rules.
  - `LockManager` ist ein neuer Worker-Subkomponent analog `RateLimiter`.
  - `Signal.TASK_LOCKED` wird hier zum ersten Mal emittiert.
  - Graceful-Shutdown bekommt einen neuen Step (Lock-Release).
- **Error propagation:**
  - Lock-Acquire-Failure (Valkey down) → `BackendConnectionError` →
    `nack(requeue=True)` → Broker re-delivered eventuell zu anderem Worker
    (gleiche Disziplin wie Rate-Limit).
  - Lock-Release-Failure (Valkey down beim release) → log error, weiter.
    Lock läuft via TTL ab — kein dauerhaftes Leak.
- **State lifecycle risks:**
  - `_active_locks` Dict im Worker-Memory: bei Worker-Crash verloren — TTL
    löst auf.
  - Format-String-Render-Failure → DLQ (User-Konfigurationsfehler).
- **API surface parity:**
  - `TaskInfo` bekommt zwei neue optionale Felder (`lock`, `max_concurrency`).
    Public-API-Erweiterung, keine Breaking Change.
  - Kein neuer CLI-Befehl in dieser Phase (Lock-Inspection-API ist Phase 3c).
- **Integration coverage:**
  - Multi-Worker-Mutex (siehe Unit 6) — kann nicht durch Mocks bewiesen werden.
  - Worker-Crash-TTL-Recovery — braucht echtes Valkey.
- **Unchanged invariants:**
  - Rate-Limit-Pipeline-Stage-Verhalten (R3.3) bleibt bit-genau identisch —
    Lock kommt strikt DANACH.
  - FIRE_AND_FORGET-Ack-Position bleibt nach Lock-Stage; Solution-Doc-Rule 1
    wird strikt eingehalten.
  - Bestehende 478 Tests bleiben grün.
  - `Worker._handle_message`-Signatur und -Vertrag unverändert (interner Refactor).

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Lock-TTL läuft ab unter GC-Pause / NTP-Skew bei kurzem hard_timeout | TTL-Formel `max(30s, 0.25*hard_timeout)` gibt mindestens 30s Puffer; Test mit hard_timeout=1s + simuliertem Stall (asyncio.sleep im Task vor Release) verifiziert untere Grenze |
| Worker-SIGKILL mit gehaltenem Lock blockt nächsten Worker für TTL-Dauer (akzeptierte Limitation) | Im Brief dokumentiert; CHANGELOG-Note bei Release; Heartbeat als Phase-3c-Erweiterung wenn Use-Case auftaucht |
| FIRE_AND_FORGET-Ack-Race wie bei R3.3 — Lock-Stage muss vor Ack laufen | Solution-Doc-Pattern strikt; 4 Mandatory-Regression-Tests test-first geschrieben (Execution Note in Unit 4) |
| Format-String mit fehlendem Arg → silent KeyError zur Laufzeit | DLQ-Routing bei Render-Fehler (klarer Pfad); `tests/integration/test_locking_e2e.py` Edge-Case |
| Owner-Token-Mismatch (anderer Worker released foreign Lock) | Lua-Script prüft Owner atomar — Foreign-Release returnt 0 ohne Wirkung |
| Pipeline-Reordering-Bug (Lock vs Rate-Limit Reihenfolge falsch) | Integration-Test mit BEIDEN Stages aktiv prüft Reihenfolge: Rate-Limit-Block muss VOR Lock-Acquire-Versuch greifen |
| `_active_handler_locks` Memory-Leak bei nie-released Locks (z. B. wenn finally ausgelassen wird) | Test: 1000 Tasks mit Exception → `_active_handler_locks` ist nach Drain leer |
| Lua-NOSCRIPT-Recovery-Fehler unter Last | Pattern aus R3.3 (`AsyncScript.register_script` macht es selbst) — keine eigene Implementation |
| Lock-Release in Worker-Shutdown wirft, weil Backend bereits halb-closed | Lock-Release nach asyncio.wait(pending), vor `self._backend.close()` (siehe Unit 5 Step 3b) |
| **Wedged Lock** (Holder hängt, kein Heartbeat, kein force_release in v1): Tasks landen wiederholt in DLQ ohne Recovery (Adversarial-F3) | DLQ-Messages tragen `x-swarmq-lock-name` Header → Operator kann manuell `valkey-cli DEL sq:lock:<name>` zur Recovery; Operations-Notes-Update + Phase 3c liefert `swarmq locks force-release` |
| **`asyncio.shield`-Failure**: shield kann selbst KeyboardInterrupt nicht aufhalten (Adversarial-F2) | Akzeptierte Limitation — KeyboardInterrupt ist eh signal-handler-driven; SIGTERM-Path nutzt graceful shutdown statt Cancellation |
| **Cancellation während Render** (zwischen `inspect.signature.bind` und `acquire`): Lock noch nicht acquired, kein Leak | Pre-acquire ist OK — wenn Cancel vor acquire kommt, kein Lock geholt, kein Leak; Test bestätigt |
| **TaskInfo-Validation-Verschärfung** vs Brief-Spec: User erwartet ConfigurationError beim Schedule, bekommt ValueError beim Modul-Import (Coherence-P0) | Strenger als Brief; CHANGELOG-Note bei Release; klar dokumentiert in TaskInfo-Docstring |

## Documentation / Operational Notes

- **Docstring-Update** in `src/swarmq/task.py` — `TaskInfo`-Klassen-Docstring
  muss `lock` und `max_concurrency` analog `rate_limit` erklären, mit
  Beispielen.
- **Solution-Doc Update**: nach Implementation den Pipeline-Stage-Solution-Doc
  ergänzen — neuer "implemented stages" Abschnitt: `_try_apply_rate_limit`
  (R3.3) und `_try_apply_lock` (R3.4) folgen alle 4 Rules.
- **Roadmap-Update**: nach Merge `docs/brainstorms/implementation-order-
  requirements.md` — R3.4 als done markieren (analog R3.3).
- **README-Update**: "What works today" Abschnitt um Lock-Beschreibung erweitern
  (Mutex + Semaphore mit kurzem Code-Beispiel).
- **Solution-Doc-NEU** (post-Implementation, falls Review-Findings auftauchen):
  Pattern dokumentieren wenn neue Lessons gelernt werden.
- **Operational**: Bei Production-Use mit `lock=...` müssen Operatoren wissen:
  - Nach Worker-SIGKILL wird bis zu `max(30s, 0.25*hard_timeout)` Sekunden
    gewartet, bis nächster Worker den Lock acquired (TTL-Recovery).
  - **Wedged-Lock-Recovery** (in v1, vor Phase 3c CLI): wenn ein Task wiederholt
    in DLQ landet wegen Lock-Conflict-Counter-Cap, lesen Operatoren den
    `x-swarmq-lock-name` Header der DLQ-Message und führen
    `valkey-cli DEL sq:lock:<lock_name>` aus, um den Lock zwangsweise
    freizugeben. Anschliessend kann die DLQ-Message reprocessed werden.
  - Diese Notiz gehört in eine zukünftige Operations-Doc (Phase 5).

## Sources & References

- **Origin document**: [docs/brainstorms/2026-04-16-phase-3b2-task-locking-requirements.md](../brainstorms/2026-04-16-phase-3b2-task-locking-requirements.md)
- **Pipeline-Pattern**: [docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md](../solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md)
- **Rate-Limit-Plan (Vorgänger-Pattern)**: [docs/plans/2026-04-16-005-feat-phase3b-rate-limiting-plan.md](2026-04-16-005-feat-phase3b-rate-limiting-plan.md)
- **Recherche**: `docs/recherche-rate-limiting-locking-dedup.md`, `docs/recherche-deepening-lua-scripts.md` Abschnitt 4
- **Roadmap-Spec**: `docs/brainstorms/implementation-order-requirements.md` (R3.4)
- **Bestehende Code-Referenzen**:
  - `src/swarmq/limits.py` (RateLimiter — Klassen-Vorlage)
  - `src/swarmq/lua/sliding_window_counter.lua` (Lua-File-Pattern)
  - `src/swarmq/worker.py:_try_apply_rate_limit` (Pipeline-Helper-Vorlage)
  - `src/swarmq/worker.py:_handle_message` (Pipeline-Position)
  - `src/swarmq/worker.py:_execute_shutdown` (Shutdown-Sequenz)
  - `src/swarmq/task.py:TaskInfo` (Public-API-Erweiterungs-Stelle)
  - `src/swarmq/signals.py` (`TASK_LOCKED` bereits definiert)
