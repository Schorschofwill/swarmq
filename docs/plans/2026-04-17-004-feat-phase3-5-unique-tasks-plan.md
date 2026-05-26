---
title: "feat: Phase 3.5 — Unique Tasks / Deduplication (R3.5)"
type: feat
status: implemented-and-reviewed
date: 2026-04-17
origin: docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md
---

# feat: Phase 3.5 — Unique Tasks / Deduplication (R3.5)

## Overview

Library-Level-Deduplizierung via Producer-Side SETNX auf Valkey.
Silent-by-default mit Observability via neuem `TASK_DUPLICATE_DETECTED`-
Signal. Zwei Modi: `completion` (Default — Flag überlebt bis Terminal-
State) und `start` (Opt-in — Flag bei Worker-Pickup gelöscht). Auto-
Hash via `sha256(task_name + canonical_encode(args, kwargs))`; kein
User-Custom-Key in v1.

Producer-Side SETNX greift an vier Stellen:
- `Client.schedule()` (immediate publish)
- `Client._schedule_deferred()` (delay/eta via ScheduleStore)
- `Client.schedule_many()` (per Batch-Element)
- `Scheduler._fire_one()` (Cron/Delay-ETA-Fires vor broker.publish)

Consumer-Side-Release im Worker bei allen Terminal-Pfaden
(`completion`) bzw. bei Pickup (`start`). Vollständige Terminal-Pfad-
Enumeration folgt dem Pattern der `clear_cancel_flag`-Aufrufer
(R3.6 — bereits etabliert).

## Problem Frame

Siehe origin: `docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md`.
Kurzfassung: SwarmQ hat heute keine eingebaute Dedup-Schicht. Nutzer
müssen Idempotenz selbst bauen (ad-hoc SETNX, DB-Constraints). Peer-
Konkurrenz (Celery, Dramatiq, arq) liefert das out-of-the-box.
Zwei primäre Use-Cases: Submit-Button-Doppelklick, Webhook-Idempotenz.
Parallel-Run-Schutz ist **kein** R3.5-Thema — R3.4 Locking deckt das.

## Requirements Trace

- **R1. `TaskInfo.unique_for` + `unique_until`-Felder** (origin §In-Scope
  TaskInfo-Felder) — Units 2, 12.
- **R2. Per-call-Override** (origin §Per-Call-Override) — Units 6, 7, 8.
- **R3. Dedup-Key via sha256 + canonical_encode** (origin §Dedup-Key,
  CD2) — Unit 1.
- **R4. Silent-by-default + `TASK_DUPLICATE_DETECTED`-Signal** (origin
  §Observability, CD4) — Units 3, 6, 7, 8, 9.
- **R5. Scheduler-Integration** (origin §Scheduler-Integration, CD7) —
  Unit 9.
- **R6. Cancel-Integration via Worker-Terminal-Release** (origin
  §Cancel-Integration, CD8) — Unit 11.
- **R7. Retry-Semantik pro Mode** (origin CD10) — Units 10, 11.
- **R8. Fail-fast-Validierung** (origin CD6) — Unit 2.
- **R9. Reserved-kwarg-Erweiterung** (origin §Reserved-kwarg) — Unit 2.
- **R10. Deferred-Pfad-SETNX-Timing** (origin CD11) — Unit 7.
- **R11. Chaos-Safety-Net via TTL-only** (origin §Chaos-Safety-Net) —
  alle Units (kein spezielles Handling).
- **R12. E2E-User-Outcome-Tests** (origin §Success Criteria) — Unit 12.

## Scope Boundaries

### In-Scope

Alle In-Scope-Punkte aus origin (§In-Scope). Keine Erweiterungen.

### Non-Goals

Alle Non-Goals aus origin (§Non-Goals) bleiben:
- Custom-`unique_key`-Override (String/Callable)
- `DuplicateTaskError`-Exception
- Parallel-Run-Schutz als primärer Use-Case
- Cross-Queue-Dedup-API
- Dedup-Metrics/Stats
- `release_on_failure=False`-Flag
- Dedup-Aware-Cancel

### Deferred to Separate Tasks

- Custom-`unique_key` (String + Callable) — post-v1 Plan wenn User-Bedarf
- `raise_on_duplicate`-Opt-in — via User-Middleware heute bereits machbar
- Dedup-Metrics — Teil von R3.7
- Scheduler-Leader-Failover-Signal-Noise-Dedifferenzierung — v2

## Context & Research

### Relevant Code and Patterns

- **Producer-Side SETNX-Vorlage:** `src/swarmq/scheduler.py` Scheduler-
  Commit (`sched_commit.lua` mit Owner-Check). Selber Pattern für Unique.
- **Owner-Check-Lua-Vorlage:** `src/swarmq/lua/lock_release.lua` (Zeilen
  1-62). `unique_release.lua` kopiert GET+compare+DEL-Block.
- **Lua-Loader:** `src/swarmq/lua/__init__.py` (`load_script(name)`),
  Registration-Pattern in `src/swarmq/locking.py:86-88`.
- **Signal-Dispatcher-Pattern:** `src/swarmq/signals.py` Enum (Zeilen
  26-80) + `SignalDispatcher.dispatch` (83-133). Bestehende
  Emission-Sites in `src/swarmq/client.py:203, 380, 392`.
- **TaskInfo-Validierung-Template:** `src/swarmq/task.py:115-188`
  (`__post_init__`, `ValueError` fail-fast).
- **Reserved-kwarg-Mechanik:** `src/swarmq/task.py:25`
  (`_RESERVED_RUN_PARAM_NAMES`), Check in `__init_subclass__:215-243`.
- **msgspec-Encoder:** `src/swarmq/serialization.py:16-27` (`encode_json`).
  `canonical_encode` neu nach Zeile 28.
- **Worker-Terminal-Pfade (R3.6-Pattern):** `src/swarmq/worker.py`
  `clear_cancel_flag`-Aufrufer bei 943, 1275, 1375, 1402. Plus DLQ-
  Pfade bei 350, 386, 475, 1646.
- **Scheduler-Fire-Flow:** `src/swarmq/scheduler.py:668-738`
  (`_fire_one`). SETNX-Insertion zwischen Zeile 693 (body built) und
  696 (broker.publish). Bei NX-Fail: `commit_fire` **trotzdem**
  aufrufen (Zeile 724) sonst re-pollt zset ewig.

### Institutional Learnings

- **`docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`**
  nennt R3.5 explizit als Nachfolger-Stage. Wichtigste Regeln für
  Release-Pfade (Unit 11): (1) Transient error → `nack(requeue=True)`;
  permanent error → `nack(requeue=False)` (DLX routet). (2) Two-Level-
  DLQ-Pattern wenn Release-DEL fehlschlägt. (3) Alle neuen Valkey-Calls
  in `BackendConnectionError` wrappen.
- **`docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`**:
  Log-Wording für DLX-Pfade ("routed via broker DLX to {queue}.XQ"),
  nie "dropped" oder "lost". Relevant für Unit 11 Release-Failure-
  Handling.
- **`docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`**:
  Jeder neue Valkey-Call wrapped `(ValkeyError, OSError)` →
  `BackendConnectionError(str(exc)) from exc`. NOSCRIPT-Recovery für
  Lua-Scripts (analog `lock_release.lua`).

### External References

Keine — Muster vollständig im Repo etabliert (SETNX pattern ×3,
Owner-Check-Lua ×1, Signal-System, msgspec-Layer).

## Key Technical Decisions

- **Architektur: Producer-Side SETNX** (nicht Consumer-Side Worker-
  Stage). Rationale: Duplikate werden am Enqueue gestoppt, kein
  Broker-Traffic, kein Worker-`_try_apply_unique`-Stage nötig.
  Ausnahme: Consumer-Side-Release (für `start`-Mode Pickup und
  `completion`-Mode Terminal-Pfade) passiert im Worker.
- **Dedup-Key-Serialisierung:** `canonical_encode` sortiert Dict-Keys
  rekursiv vor `msgspec.json`-Encoding; lehnt Sets/Custom-Types
  (datetime, UUID, Decimal, dataclass) ab — `TypeError` → Aufrufer
  wrapped zu `ResultSerializationError`. Tupel/Listen behalten Order.
  Rationale: msgspec.json hat kein `sort_keys`-Flag; eigene
  Kanonisierung nötig. Sets-Ordering-Hell vermeiden durch Ablehnen.
- **SETNX-Value:** `task_id` (für Owner-Check beim completion-Release).
- **Message-Header:** Bei `completion`-Mode trägt jede publishte
  Message einen neuen Header `x-swarmq-unique-hash` (dedup-key).
  Worker liest diesen beim Terminal-Release, damit er ohne
  Args-Rehashing den richtigen Valkey-Key löschen kann.
- **Scheduler-SETNX in Python (nicht Lua):** Im `_fire_one` via
  ValkeyBackend-Call vor broker.publish. Rationale: kein sched_commit.
  lua-Update nötig; Race zwischen SETNX-check und broker.publish ist
  akzeptabel (TTL-Backstop wirkt bei Crash dazwischen). Bei NX-Fail:
  `commit_fire` **trotzdem** aufrufen, sonst re-pollt das zset ewig.
- **Scheduler-Leader-Failover-Signal-Noise:** Akzeptierte v1-
  Einschränkung. Bei Leader-Crash zwischen SETNX und commit_fire kann
  der zweite Leader ein `TASK_DUPLICATE_DETECTED` mit
  `source="scheduler.fire"` emittieren obwohl kein User-Duplikat.
  Operator-Filter-Empfehlung: source-Feld prüfen. Dedifferenzierung
  via separates `SCHEDULER_FIRE_DEDUP`-Signal ist Deferred (v2).
- **TaskInfo-Validierung:** `unique_for=0` → `ValueError`; `unique_for=None`
  + `unique_until` gesetzt → `unique_until` wird ignoriert (kein
  ValueError, um Sentinel-Diskriminierung in frozen-dataclass zu
  vermeiden). Rationale: origin CD6 Compromise.
- **No Per-Schedule-Sentinel für `schedule_many` Override:** `_MISSING`
  ist modul-privater `object()`-Instance. Nicht öffentlicher Typ.
- **`canonical_encode` lehnt Non-JSON-Native-Types ab** (inkl. Sets,
  datetime, UUID). Rationale: bereits heute scheitert `encode_json`
  bei diesen Types — kein Regression. User die solche Args brauchen,
  konvertieren sie vorher (ISO-String etc.) oder deaktivieren Dedup.

## Open Questions

### Resolved During Planning

- **SETNX-Value:** task_id (ermöglicht Owner-Check).
- **Scheduler-SETNX-Ort:** Python in `_fire_one` (nicht Lua).
- **Signal-Naming:** `Signal.TASK_DUPLICATE_DETECTED` (konsistent mit
  `TASK_ENQUEUED`/`TASK_COMPLETED`).
- **Lua-Script-Count:** ein einziges `unique_release.lua` (owner-
  checked DEL). `start`-Mode kann dasselbe Script mit `task_id` als
  Owner-Token nutzen — owner-check ist harmless wenn Value ohnehin
  task_id ist.
- **`client.run()` sync-API:** propagiert existing_task_id transparent
  via `schedule()` + `get_result()`. Keine Spezialbehandlung.
- **Deferred-Pfad-TTL-Formel:** `unique_for + fire_offset_seconds`
  wobei `fire_offset_seconds = delay || (eta - now).total_seconds()`.
- **`unique_for=0`-Verhalten:** `ValueError` in `__post_init__`
  (Fail-fast, konsistent).
- **Reserved-kwargs:** `unique_for` und `unique_until` in
  `_RESERVED_RUN_PARAM_NAMES` + Migration-Hinweis in Changelog.

### Deferred to Implementation

- **Genauer DEL-Pfad bei Message-Consume (`start`-Mode):** vor oder
  nach Deserialisierung? Vor Middleware-Chain? Exakter Ort wird
  bei Unit 10 bestimmt — muss aber VOR FIRE_AND_FORGET-Ack passieren
  (sonst: Valkey-Blip silent drops message, siehe ack-ordering-doc
  Rule 1).
- **`schedule_many`-Batch-Signal-Emission:** ein Signal pro Duplikat
  (sync sequential per SignalDispatcher-Contract). Performance bei
  Batch-100 mit Dups akzeptabel? Plan-Hypothese: ja; Unit 12 Perf-
  Smoke-Test verifiziert.
- **Vollständige Worker-Terminal-Pfad-Enumeration:** grep-Output aus
  Unit 11 — `_send_to_dlq`-Aufrufer + `clear_cancel_flag`-Aufrufer
  + alle fallback-ack-Pfade (nach DLQ-Publish-Failure).
- **`TaskInfo.__post_init__`-Reihenfolge:** neue unique-Validierungen
  vor oder nach bestehenden lock-Validierungen? Stilistische
  Entscheidung bei Unit 2.

## High-Level Technical Design

> *Directional guidance, not implementation specification.*

**Producer-Side Flow (Client.schedule):**

```
client.schedule(task_name, *args, unique_for=60, **kwargs)
  │
  ├── resolve unique_for/unique_until (per-call or TaskInfo default)
  ├── if unique_for is None: (legacy path, no dedup)
  │     └── publish → ack → return task_id
  │
  ├── compute dedup_hash = sha256(task_name + canonical_encode(args, kwargs))
  ├── task_id = uuid7()
  │
  ├── SETNX(sq:unique:{hash}, task_id, PX=unique_for_ms)
  │     │
  │     ├── SUCCESS (NX granted):
  │     │     ├── build message with x-swarmq-unique-hash header (completion-mode)
  │     │     ├── broker.publish(...)
  │     │     ├── emit TASK_ENQUEUED
  │     │     └── return task_id
  │     │
  │     └── FAILURE (NX rejected — flag already held):
  │           ├── GET(sq:unique:{hash}) → existing_task_id
  │           ├── emit TASK_DUPLICATE_DETECTED(existing_task_id, ...,
  │           │                                 source="client.schedule")
  │           └── return existing_task_id       # silent
```

**Consumer-Side Flow (Worker — completion-Mode):**

```
Worker receives Message
  │
  ├── (existing pipeline: rate-limit, lock, middleware, execute ...)
  │
  ├── Terminal Exit (any of: success-ack, DLQ-publish,
  │                          retry-exhausted, cancel, timeout)
  │     │
  │     ├── if header x-swarmq-unique-hash present:
  │     │     ├── call unique_release.lua(hash, task_id)
  │     │     ├── DEL if Value == task_id (owner-check)
  │     │     └── log at DEBUG (no signal — release is silent)
  │     │
  │     └── proceed with existing terminal action
```

**Consumer-Side Flow (Worker — start-Mode):**

```
Worker.handle_message
  │
  ├── deserialize
  ├── if header x-swarmq-unique-hash present AND start-mode:
  │     └── call unique_release.lua(hash, task_id)  # release BEFORE execution
  │
  ├── proceed with existing pipeline
  └── terminal paths: NO additional release (already done at pickup)
```

**SETNX-Key-Owner-State:**

```
Valkey Key      | Type   | Value     | TTL            | Semantics
----------------|--------|-----------|----------------|-------------------
sq:unique:<hex> | string | task_id   | unique_for_ms  | producer holds
                |        |           | (+ delay_ms    | until worker
                |        |           |  for deferred) | releases (owner-
                |        |           |                | checked DEL)
```

## Implementation Units

- [ ] **Unit 1: `canonical_encode`-Helper + Tests**

**Goal:** Stabile, deterministische Bytes-Repräsentation von
`(args, kwargs)` für Dedup-Hashing; Grundlage für alle SETNX-Pfade.

**Requirements:** R3.

**Dependencies:** Keine.

**Files:**
- Modify: `src/swarmq/serialization.py`
- Test: `tests/unit/test_serialization.py`

**Approach:**
- Neue Funktion `canonical_encode(args: tuple, kwargs: dict) -> bytes`
  direkt nach `encode_json` (nach Zeile 28).
- **Pre-Normalisierung + rekursive Kanonisierung** vor msgspec-Encode
  (weil msgspec.json kein `sort_keys` hat und UUID/datetime
  ordnungsunempfindlich native encoded würden):
  - `UUID` → `str(uuid)` (ISO-Form stabil)
  - `datetime`/`date`/`time` → `.isoformat()`
  - `Decimal` → `str(decimal)` (verhindert float-Präzisions-Drift)
  - `set`/`frozenset` → `sorted(list(s))` (TypeError wenn Elemente
    nicht sortierbar — mixed types)
  - `dict` → Keys rekursiv sortiert (alle Keys müssen `str` sein,
    sonst TypeError)
  - `tuple` → Liste (JSON-encoding konvertiert ohnehin; Plan-
    Invariante siehe Test unten)
- **Rejected Types** (TypeError): `bytes`, dataclass, `msgspec.Struct`,
  Custom-Klassen. Message: "canonical_encode does not support
  {type.__name__}; dedup requires JSON-native or pre-normalizable
  args. Convert to str/dict before scheduling, or disable dedup for
  this task."
- Output: `bytes` (UTF-8 JSON).

**Rationale für Permissiveness:** `encode_json` (heute) akzeptiert
UUID/datetime/set nativ via msgspec.json. Strikte Ablehnung in
canonical_encode wäre Silent-Regression für Bestandsnutzer. Pre-
Normalisierung bewahrt Kompatibilität UND garantiert Hash-Stabilität.

**Patterns to follow:**
- `encode_json` (serialization.py:20-27) für Error-Wrapping-Stil.
- `parse_rate_limit` (task.py) für klare Error-Messages mit
  Inline-Beispielen.

**Test scenarios:**
- Happy path: `canonical_encode((), {"a": 1, "b": 2})` ==
  `canonical_encode((), {"b": 2, "a": 1})` (Key-Sort wirkt).
- Happy path: `canonical_encode((), {"x": {"b": 1, "a": 2}})` ==
  `canonical_encode((), {"x": {"a": 2, "b": 1}})` (rekursiv).
- Happy path: `canonical_encode(("foo",), {})` !=
  `canonical_encode(("bar",), {})` (positional args unterscheiden).
- Happy path: `canonical_encode(([1, 2, 3],), {})` !=
  `canonical_encode(([3, 2, 1],), {})` (list order signifikant).
- **Invariant (tuple/list-Äquivalenz für Scheduler-Reconstruction):**
  `canonical_encode((1, 2, 3), {})` == `canonical_encode([1, 2, 3], {})`
  (JSON-encode macht sie indistinguishable; Plan-Hypothese wird
  hier festgenagelt).
- **Invariant (msgpack-Roundtrip-Stabilität, für ScheduleStore):**
  Args via msgpack-Encode+Decode → Hash unverändert. Getestet
  mit dict{"a": 1} → msgpack → dict → canonical_encode.
- Normalization: `canonical_encode((), {"id": uuid.UUID('...')})` ==
  `canonical_encode((), {"id": "..."})` (UUID stringified).
- Normalization: `canonical_encode((), {"ts": datetime(2026,1,1)})` ==
  `canonical_encode((), {"ts": "2026-01-01T00:00:00"})` (isoformat).
- Normalization: `canonical_encode((), {"d": Decimal("1.10")})` ==
  `canonical_encode((), {"d": "1.10"})` (stringified).
- Normalization: `canonical_encode((), {"s": {3, 1, 2}})` ==
  `canonical_encode((), {"s": [1, 2, 3]})` (sorted-list).
- Edge case: leere args + leere kwargs → deterministisches Output.
- Edge case: nested dict mit mixed primitive types → konsistent.
- Edge case: int vs float: `canonical_encode((), {"n": 1})` !=
  `canonical_encode((), {"n": 1.0})` (dokumentierte Einschränkung —
  User die das konvergieren wollen, normalisieren selbst).
- Error path: `canonical_encode((), {"s": {1, "a"}})` → `TypeError`
  (mixed-type set, nicht sortierbar).
- Error path: `canonical_encode((), {"d": MyDataclass()})` →
  `TypeError` (dataclass rejected).
- Error path: `canonical_encode((), {"k": b"bytes"})` → `TypeError`
  (bytes rejected).
- Error path: `canonical_encode((), {1: "a"})` → `TypeError`
  (non-str dict-key).

**Verification:** `uv run pytest tests/unit/test_serialization.py -k canonical`
grün. Hash-Output über 100 random-permuted-kwargs-Calls identisch.

---

- [ ] **Unit 2: `TaskInfo.unique_for` + `unique_until`-Felder + Validation + Reserved-kwargs**

**Goal:** TaskInfo-API-Erweiterung mit Fail-fast-Validierung und
Reserved-kwarg-Mechanik.

**Requirements:** R1, R8, R9.

**Dependencies:** Keine (Unit 1 parallel).

**Files:**
- Modify: `src/swarmq/task.py`
- Test: `tests/unit/test_task.py`

**Approach:**
- Neue Felder auf `TaskInfo` (frozen dataclass):
  - `unique_for: int | None = None`
  - `unique_until: Literal["start", "completion"] = "completion"`
- Validierung in `__post_init__` (nach Zeile 188, Ende der Methode):
  - `unique_for == 0` → `ValueError("unique_for=0 is invalid; use None to disable dedup")`
  - `unique_for is not None and unique_for < 0` → `ValueError`
  - `unique_until` Literal wird durch Type-Annotation bereits geprüft
    (msgspec/dataclass), aber Runtime-Check für Plain-Python-Instanzen
    redundant einbauen.
  - **KEIN** ValueError bei `unique_for=None + unique_until="start"`
    explizit gesetzt (würde Sentinel-Diskriminierung brauchen).
- `_RESERVED_RUN_PARAM_NAMES` (Zeile 25) erweitern:
  `frozenset({"delay", "eta", "unique_for", "unique_until"})`.
- Migration-Hinweis in `_check_run_signature_reserved_params`
  (Zeile 236) Fehlermeldung ergänzen:
  `delay → retry_delay, eta → run_at, unique_for → dedup_ttl,
   unique_until → dedup_mode`.
- CHANGELOG.md-Entry (oder docs/migration-notes) für die Reserved-
  kwarg-Erweiterung.

**Patterns to follow:**
- `TaskInfo.lock`/`max_concurrency`-Validierung (task.py:~160) als
  Validation-Template.
- `parse_rate_limit`-Error-Style.

**Test scenarios:**
- Happy path: `TaskInfo(name="t", queues=("q",), unique_for=60)` →
  `unique_until == "completion"` (Default).
- Happy path: `TaskInfo(..., unique_for=60, unique_until="start")` →
  Instance created.
- Happy path: `TaskInfo(..., unique_for=None)` → defaults OK, kein
  ValueError auch wenn `unique_until` explizit gesetzt.
- Error path: `TaskInfo(..., unique_for=0)` → `ValueError` mit
  "unique_for=0" Message.
- Error path: `TaskInfo(..., unique_for=-5)` → `ValueError`.
- Integration: `class FooTask(Task): task_info = TaskInfo(..., unique_for=60); def run(self, unique_for): ...` → `ValueError` at
  `__init_subclass__` time (Reserved-kwarg-Check).
- Integration: Bestehender TaskInfo-Use ohne unique_* → keine
  Regression.

**Verification:** `uv run pytest tests/unit/test_task.py` grün.
Alle bestehenden TaskInfo-Tests laufen ohne Änderung.

---

- [ ] **Unit 3: `Signal.TASK_DUPLICATE_DETECTED`-Enum-Member**

**Goal:** Signal-System-Integration für Dedup-Observability.

**Requirements:** R4.

**Dependencies:** Keine.

**Files:**
- Modify: `src/swarmq/signals.py`
- Test: `tests/unit/test_signals.py`

**Approach:**
- Neuer Enum-Member zwischen `TASK_RATE_LIMITED` und
  `WORKER_ONLINE`: `TASK_DUPLICATE_DETECTED = "task_duplicate_detected"`.
- Docstring-Update-Block in `signals.py` erklärt Payload:
  `existing_task_id`, `task_name`, `dedup_hash`, `source`.
- `source` ∈ `{"client.schedule", "client.schedule_many",
  "scheduler.fire"}`.
- Kein Emission-Site in dieser Unit — nur das Enum.

**Patterns to follow:**
- Bestehender Enum-Block (signals.py:26-80).
- Existing Docstring-Style.

**Test scenarios:**
- Happy path: Signal-Dispatcher mit `@on_signal(TASK_DUPLICATE_DETECTED)`-
  Handler registriert → `await dispatcher.dispatch(TASK_DUPLICATE_DETECTED,
  existing_task_id="t1", task_name="foo", dedup_hash="abc", source="client.schedule")`
  ruft Handler mit kwargs.
- Edge case: Handler-Exception wird geschluckt (per bestehendem
  SignalDispatcher-Contract).

**Verification:** `uv run pytest tests/unit/test_signals.py` grün.
`Signal.TASK_DUPLICATE_DETECTED` ist importierbar.

---

- [ ] **Unit 4: `unique_release.lua` + Loader-Integration**

**Goal:** Owner-checked DEL-Lua-Script für Flag-Release.

**Requirements:** R6, R7 (completion-Mode-Release).

**Dependencies:** Keine.

**Files:**
- Create: `src/swarmq/lua/unique_release.lua`
- Test: `tests/unit/test_lua_loader.py` (bestehende Test-Datei
  erweitern, falls sinnvoll)
- Test: `tests/integration/test_valkey_unique.py` (neue Datei
  für Lua-Integration-Tests gegen echte Valkey)

**Approach:**
- Kopiere `src/swarmq/lua/lock_release.lua` als Template.
- Vereinfache: kein Semaphore-Branch (nur Mutex-Branch).
- Signatur:
  ```
  KEYS[1] = sq:unique:<hex>
  ARGV[1] = task_id (owner-token)
  ```
- Logik: GET KEYS[1]; if value == ARGV[1]: DEL; return 1 else return 0.
  Wenn key fehlt (TTL-expired): return 0.
- Kein `__force__`-Branch in v1 (YAGNI).

**Patterns to follow:**
- `src/swarmq/lua/lock_release.lua` Zeilen 14-40 (Mutex-Branch).
- `src/swarmq/locking.py:86-88` Registration-Pattern (kommt in Unit 5).

**Test scenarios:**
- Happy path: SET key=v1, release(key, v1) → returns 1, key weg.
- Edge case: SET key=v1, release(key, v2) → returns 0, key bleibt.
- Edge case: key TTL-expired bevor release → returns 0.
- Edge case: key fehlt komplett → returns 0.

**Verification:** `uv run pytest tests/integration/test_valkey_unique.py -k release`
grün. Script lädt ohne NOSCRIPT-Fehler.

---

- [ ] **Unit 5: `ValkeyBackend`-Methoden für Unique-Dedup**

**Goal:** Thin-Layer Valkey-API für Dedup-Operationen mit
`BackendConnectionError`-Wrapping.

**Requirements:** R1, R4, R6.

**Dependencies:** Unit 4 (Lua-Script existiert).

**Files:**
- Modify: `src/swarmq/backend/valkey.py`
- Test: `tests/integration/test_valkey_unique.py` (fortsetzen)
- Test: `tests/unit/backend/test_valkey_unique.py` (neue Unit-Tests
  mit AsyncMock)

**Approach:**
- Registration im `ValkeyBackend.__init__` (oder wo locking-Scripts
  registriert werden, z. B. `_ensure_unique_release_script`):
  ```
  self._unique_release_script = client.register_script(
      load_script("unique_release"))
  ```
- Zwei Methoden (get_unique_owner gestrichen — atomare Alternative
  via `SET NX GET`):
  - `setnx_unique(hash: str, task_id: str, ttl_ms: int) -> str | None`
    → `SET sq:unique:{hash} task_id NX GET PX ttl_ms`; returnt `None`
    bei NX-Success (Flag gesetzt), sonst existing owner-value
    (existing_task_id) atomar in einem Round-Trip. Valkey/Redis 7.0+
    unterstützt `SET NX GET` — verifiziert via valkey-py-docs. Wenn
    die valkey-py-Version das Flag nicht unterstützt: Fallback via
    kleinem Lua-Script `SET NX` + return-existing.
    Wrapping: `(ValkeyError, OSError) → BackendConnectionError`
    (wie `set_cancel_flag` pattern).
  - `release_unique(hash: str, task_id: str) -> bool` →
    script-eval mit `KEYS=[sq:unique:hash], ARGV=[task_id]`; returnt
    `True` wenn released (1), `False` wenn nicht owner oder missing.
    **Wrapping: swallow-on-failure** (wie `clear_cancel_flag` —
    `log.warning("unique_release_backend_error")` und return `False`).
    Rationale: Release ist non-critical (TTL-Backstop wirkt); raising
    BackendConnectionError würde Terminal-Pfade im Worker abbrechen.
- Kein Lua für SETNX — atomares `SET NX GET PX` genügt.

**Patterns to follow:**
- `src/swarmq/locking.py:86-88` Registration.
- `src/swarmq/backend/valkey.py` bestehende `set_cancel_flag`/
  `is_cancelled`/`clear_cancel_flag`-Methoden (R3.6) als
  Wrapping-Template.

**Test scenarios:**
- Happy path: `setnx_unique("abc", "t1", 60000)` → `None` returned
  (NX success — flag set).
- Happy path: `setnx_unique("abc", "t2", 60000)` (zweiter call) →
  `"t1"` returned atomar (existing owner).
- Happy path: `release_unique("abc", "t1")` → `True`; `get_unique_owner("abc")` → `None`.
- Edge case: `release_unique("abc", "t2")` (wrong owner) → `False`;
  Key bleibt.
- Edge case: `release_unique("nonexistent", "t1")` → `False`.
- Error path: Valkey offline → `BackendConnectionError` bei setnx.
- Error path: NOSCRIPT → Script wird automatisch reloaded via
  `valkey.AsyncScript`-Contract (integration test).

**Verification:** `uv run pytest tests/integration/test_valkey_unique.py`
grün. Alle neuen Methoden wrappen korrekt.

---

- [ ] **Unit 6: `Client.schedule()` — immediate SETNX-Pfad**

**Goal:** Producer-Side-Dedup für Immediate-Publish-Pfad.

**Requirements:** R2, R4.

**Dependencies:** Units 1, 2, 3, 5.

**Files:**
- Modify: `src/swarmq/client.py`
- Test: `tests/unit/test_client.py`
- Test: `tests/integration/test_client_unique.py` (neu)

**Approach:**
- Neue Reserved-Keyword-Args in `schedule`-Signatur:
  `unique_for: int | None | _MISSING = _MISSING, unique_until: Literal[...] | _MISSING = _MISSING`.
- `_MISSING` ist modul-private `object()`-Instance.
- Resolution-Logic:
  1. Wenn `unique_for is _MISSING` → TaskInfo-Default.
  2. Wenn `unique_for is None` (explizit) → Dedup aus.
  3. Wenn `unique_for is int` → dieser Wert.
- Gleiches für `unique_until` (Default: `completion`).
- **Headers-Plumbing (Pre-Requisite):** `Client.schedule` leitet heute
  keine Headers an `broker.publish` weiter (verifiziert:
  `client.py:199` ruft nur `(queue, encode_json(message))`). Unit 6
  muss zuerst die Headers-Plumbing einführen: `publish`-Signature
  um `headers: dict[str, Any] | None = None` erweitern (falls nicht
  schon vorhanden — `broker/rabbitmq.py:57` akzeptiert via
  `aio_pika.Message(headers=...)`). Per-Call `headers: dict[str, Any]`
  dict parallel zum Message-Body aufbauen.
- **Resolution-Logic** (Registry-aware):
  1. `unique_for is _MISSING` + `self._registry is not None` →
     TaskInfo-Lookup (`registry.get(task_name).task_info.unique_for`);
     fallback None wenn Task unbekannt.
  2. `unique_for is _MISSING` + `self._registry is None` (Client-
     Modus) → Dedup aus (keine TaskInfo-Default-Resolution möglich;
     siehe Risks).
  3. `unique_for is None` (explizit) → Dedup aus.
  4. `unique_for is int` → dieser Wert.
  Gleiches für `unique_until`.
- Call-Flow-Einfügung:
  - Nach Zeile 171 (`task_id = str(uuid.uuid7())`), vor Zeile 199
    (`await self._broker.publish(...)`):
    ```
    headers: dict[str, Any] = {}  # new plumbing
    if unique_for is not None:
        dedup_hash = _compute_dedup_hash(task_name, args, kwargs)
        existing = await self._backend.setnx_unique(
            dedup_hash, task_id, unique_for * 1000)
        if existing is not None:  # NX rejected, existing holds flag
            await self._signals.dispatch(
                Signal.TASK_DUPLICATE_DETECTED,
                existing_task_id=existing,
                task_name=task_name,
                dedup_hash=dedup_hash,
                source="client.schedule")
            logger.info("swarmq.dedup.detected",
                        existing_task_id=existing, ...)
            return existing  # silent, original API shape preserved
        # Header IMMER setzen wenn Dedup aktiv — sowohl start-Mode
        # (Worker releaset bei Pickup) als auch completion-Mode
        # (Worker releaset bei Terminal).
        headers["x-swarmq-unique-hash"] = dedup_hash
    # publish with headers
    await self._broker.publish(resolved_queue, encode_json(message),
                               headers=headers if headers else None)
    ```
- Helper-Funktion `_compute_dedup_hash(task_name, args, kwargs) -> str`
  in `client.py` (oder `serialization.py`). Nutzt `canonical_encode`.
- `TASK_ENQUEUED`-Emission nur bei NX-Success (nicht bei Dup).
- **`setnx_unique` atomar mit Value-Return:** nutzt `SET NX GET PX`
  (verfügbar ab Valkey 7.0 / Redis 6.2) → returnt existing Value
  bei NX-Fail in einem Round-Trip. Siehe Unit 5 Anpassung.

**Patterns to follow:**
- Bestehende `Client.schedule`-Struktur (~Zeile 126-230).
- `delay`/`eta`-Validation-Pattern für Reserved-kwargs.

**Test scenarios:**
- Happy path: erster `schedule()` mit `unique_for=60` → SETNX-
  success, message published, `TASK_ENQUEUED` emitted, task_id
  returned.
- Happy path: zweiter `schedule()` binnen 60s → silent dedup, dieselbe
  task_id returned, `TASK_DUPLICATE_DETECTED` emitted mit
  `source="client.schedule"`, kein `TASK_ENQUEUED`.
- Happy path: nach 60s TTL-Ablauf → neuer `schedule()` erfolgreich.
- Happy path: TaskInfo-Default `unique_for=30`, per-call `unique_for=120`
  override → TTL=120s im SETNX.
- Happy path: TaskInfo-Default `unique_for=60`, per-call `unique_for=None`
  → Dedup für diesen Call aus.
- Edge case: `schedule(..., unique_for=60, unique_until="start")` →
  `x-swarmq-unique-hash`-Header WIRD gesetzt (Worker braucht ihn für
  Pickup-Release). Einheitliche Regel: Header ist aktiv sobald Dedup
  aktiv, unabhängig vom Mode.
- Edge case: Hash-Kollision-Race: zwei Clients schedulen gleichzeitig
  → ein SETNX gewinnt, andere sehen existing_task_id.
- Error path: `ResultSerializationError` bei nicht-serialisierbaren
  Args → propagiert up (existiert heute schon via encode_json).
- Error path: Valkey offline bei SETNX → `BackendConnectionError`
  propagiert; keine Message published (Client wird abgelehnt, User
  sieht Exception).
- Integration: `TASK_DUPLICATE_DETECTED`-Handler sieht existing_task_id
  und kann `client.get_result(existing_task_id)` aufrufen.

**Verification:** `uv run pytest tests/unit/test_client.py tests/integration/test_client_unique.py`
grün. Bestehende Client-Tests laufen ohne Regression.

**Execution note:** Test-first für die Resolution-Logic (TaskInfo-
Default + per-call Override + `_MISSING`-Sentinel) — das Edge-Case-
Matrix ist komplex genug dass Tests erst die API pinnen sollten.

---

- [ ] **Unit 7: `Client._schedule_deferred()` — Deferred-Pfad-SETNX**

**Goal:** Dedup für delay/eta-Tasks **vor** ScheduleStore-Insert.

**Requirements:** R2, R10.

**Dependencies:** Unit 6 (Resolution-Logic + Hash-Helper existieren).

**Files:**
- Modify: `src/swarmq/client.py`
- Test: `tests/unit/test_client.py`
- Test: `tests/integration/test_client_unique.py`

**Approach:**
- **Schema-Change `ScheduleDefinition`:** zwei neue optionale Felder:
  - `dedup_hash: str | None = None` (persistiert Hash byte-exakt)
  - `source` erweitert um `"cli_unique"` (neben bestehenden
    `"cli"`/`"static"`).
  Migration: ScheduleStore-msgpack-Roundtrip kompatibel mit
  optionalen Default-None-Feldern (bestehende Schedules bleiben
  valid).
- Im `_schedule_deferred` (client.py:485-529), zwischen Zeile 514
  (task_id generation) und Zeile 527 (store resolution):
  ```
  dedup_hash = None
  if unique_for is not None:
      dedup_hash = _compute_dedup_hash(task_name, args, kwargs)
      fire_offset_s = max(0, int(delay if delay else
                                 (eta - now_utc()).total_seconds()))
      ttl_ms = (unique_for + fire_offset_s) * 1000
      existing = await self._backend.setnx_unique(
          dedup_hash, task_id, ttl_ms)
      if existing is not None:  # NX rejected
          await self._signals.dispatch(
              Signal.TASK_DUPLICATE_DETECTED,
              existing_task_id=existing,
              task_name=task_name,
              dedup_hash=dedup_hash,
              source="client.schedule")
          return existing  # silent
  # Build ScheduleDefinition with dedup_hash + source
  definition = ScheduleDefinition(
      schedule_name=f"oneshot:{task_id}",
      ...
      source="cli_unique" if dedup_hash else "cli",
      dedup_hash=dedup_hash,
  )
  ```
- ScheduleStore-Insert läuft danach; Scheduler-Fire (Unit 9)
  erkennt `source="cli_unique"` + `dedup_hash` und skippt seinen
  eigenen SETNX (siehe Unit 9).
- **Partition der SETNX-Verantwortung** zwischen Unit 7 und Unit 9
  (kritisch, sonst Double-SETNX-Race):
  - Unit 7 (`_schedule_deferred`) ist **authoritativ** für
    Client-originated deferred Tasks: setzt SETNX mit langer TTL
    (unique_for + fire_offset), markiert ScheduleDefinition mit
    `source="cli_unique"` (neues Source-Value).
  - Unit 9 (`Scheduler._fire_one`) **skippt** SETNX wenn
    `definition.source == "cli_unique"` (Flag schon gesetzt,
    nur publish + commit_fire ausführen).
  - Für `source == "static"` (Cron-Schedules aus TaskInfo.cron)
    übernimmt Unit 9 die SETNX-Logik normal.
- **Dedup-Hash-Flow für Deferred-Tasks:** Client rechnet Hash,
  setzt SETNX, persistiert Hash in ScheduleDefinition **als
  separates Feld `dedup_hash: str | None`** (neues Feld — Schema-
  Change). Rationale: Reconstruction am Fire-Zeitpunkt wäre fragil
  (tuple/list/int/float-Drift durch ScheduleStore-Roundtrip).
  Stabiler: Hash byte-exakt speichern und nur `x-swarmq-unique-hash`-
  Header beim Publish daraus ableiten. Plan-Risk bei Schema-Change
  akzeptiert.

**Patterns to follow:**
- Unit 6 Resolution-Logic und Emission.
- Bestehende `_schedule_deferred`-Struktur.

**Test scenarios:**
- Happy path: `schedule(..., unique_for=300, delay=60)` → SETNX mit
  TTL=360s; ScheduleStore-Insert erfolgreich.
- Happy path: `schedule(..., unique_for=300, eta=now+120)` → SETNX mit
  TTL=(300+120)s; ScheduleStore-Insert erfolgreich.
- Happy path: zweiter Deferred-`schedule()` mit gleichen Args vor
  Fire → silent dedup, dieselbe task_id returned.
- Edge case: `eta=past_datetime` → TTL = unique_for (fire_offset<0
  clamped auf 0 oder 1).
- Edge case: User-Outcome-Test: "delayed task + immediate gleiche Args"
  → immediate wird deduppt weil Deferred-Flag lebt.
- Error path: ScheduleStore.add_schedule fehlschlägt → Flag bleibt
  (TTL-Backstop); dokumentierte Einschränkung.

**Verification:** `uv run pytest tests/integration/test_client_unique.py -k deferred`
grün.

---

- [ ] **Unit 8: `Client.schedule_many()` — per-Element SETNX**

**Goal:** Batch-API Dedup mit `list[str]`-Return (Duplikat-Positionen
bekommen existing_task_id).

**Requirements:** R2, R4.

**Dependencies:** Units 5, 6.

**Files:**
- Modify: `src/swarmq/client.py`
- Test: `tests/unit/test_client.py`
- Test: `tests/integration/test_client_unique.py`

**Approach:**
- `schedule_many`-Signatur erweitern:
  ```
  async def schedule_many(
      self, tasks, queue=None, *,
      unique_for: int | None = _MISSING,
      unique_until: Literal[...] = _MISSING,
  ) -> list[str]:
  ```
- Consistent `_MISSING`-Sentinel-Naming (konsistent mit Unit 6).
- **Client-Modus-Handling:** bei `self._registry is None` → TaskInfo-
  Default-Resolution skippen, `_MISSING` → None (no dedup). Identisch
  zu Unit 6 Resolution-Logic.
- `queue` bleibt positional-or-keyword (kein Break für heutige
  Caller).
- Pass 1 (client.py ~322): per-Element task_id generieren, bei
  `unique_for is not None` per-Element SETNX versuchen.
- Pro Duplikat: `TASK_DUPLICATE_DETECTED` emittieren mit
  `source="client.schedule_many"` und zusätzlichem `batch_index: int`
  in kwargs.
- Duplikate werden NICHT in `encoded_messages` gepackt; an ihrer
  Position wird `existing_task_id` direkt in die Result-Liste
  geschrieben.
- `_MISSING` ist derselbe modul-private Sentinel wie Unit 6.

**Patterns to follow:**
- Bestehende `schedule_many` Pass-1+Pass-2-Struktur
  (client.py:284-399).
- Unit 6 Resolution-Logic.

**Test scenarios:**
- Happy path: `schedule_many([(t, (), {}), (t, (), {})])` mit
  `unique_for=60` → `[task_id_1, task_id_1]`; ein
  `TASK_DUPLICATE_DETECTED`-Signal mit `batch_index=1`.
- Happy path: Gemischte Liste (1 dup + 2 frische) → `list[str]` mit
  drei IDs, davon eine wiederholt; ein Signal für den Dup.
- Happy path: ohne `unique_for` → keine SETNX, keine Signals,
  identisches Verhalten wie heute.
- Edge case: Batch-weiter Override `unique_for=None` überschreibt
  TaskInfo-Default → Batch komplett ohne Dedup.
- Edge case: Batch-weiter Override `unique_for=120` überschreibt
  TaskInfo-Default → alle Elemente nutzen 120s.
- Error path: Valkey offline → erstes SETNX raist
  `BackendConnectionError`; Batch abgebrochen vor publish
  (analog zu bestehender Partial-Failure-Semantik).
- Integration: Perf-Smoke-Test — Batch-50 mit 10 Dups, Measure Signal-
  Dispatch-Overhead (nicht > 200ms total).

**Verification:** `uv run pytest tests/unit/test_client.py -k schedule_many`
grün. Bestehende positional-`queue`-Caller laufen ohne Änderung.

---

- [ ] **Unit 9: `Scheduler._fire_one` — Pre-Publish SETNX + commit-on-Fail**

**Goal:** Scheduler-seitige Dedup für Cron/Delay/ETA-Fires.

**Requirements:** R5.

**Dependencies:** Units 4, 5.

**Files:**
- Modify: `src/swarmq/scheduler.py`
- Test: `tests/unit/test_scheduler_loop.py`
- Test: `tests/integration/test_worker_scheduler_integration.py`

**Approach:**
- **TaskInfo-Lookup** (Scheduler hat kein eigenes Registry-Feld —
  verifiziert: `Scheduler.__init__` akzeptiert `backend, broker,
  config, lock_manager, store, signal_dispatcher, owner_token,
  worker_id` ohne registry). Folge dem bestehenden Pattern aus
  `sync_taskinfo_crons` (scheduler.py:772): iteriere
  `Task._registered_tasks` für Lookup. Optional: Helper-Funktion
  `_resolve_task_info(task_name) -> TaskInfo | None` als O(n)-
  Scan. Für typische n (<100 Tasks) vernachlässigbar.
- **Partitioned-SETNX-Flow:** wenn `definition.source == "cli_unique"`
  (Client-originated deferred) → Flag schon gesetzt, skippe
  SETNX, publishe + commit_fire normal. Header aus
  `definition.dedup_hash` direkt übernommen.
- Im `_fire_one` (scheduler.py:668-738), zwischen Zeile 693 (body
  built) und Zeile 695-700 (broker.publish try-block):
  ```
  headers = {"x-swarmq-schedfire": schedfire_key}  # existing
  dedup_hash = None
  if definition.source == "cli_unique" and definition.dedup_hash:
      # Unit 7 hat schon SETNX gemacht; nur Header propagieren
      dedup_hash = definition.dedup_hash
      headers["x-swarmq-unique-hash"] = dedup_hash
  else:
      # Static cron: resolve TaskInfo and SETNX
      task_info = self._resolve_task_info(definition.task_name)
      if task_info and task_info.unique_for:
          dedup_hash = _compute_dedup_hash(
              definition.task_name,
              tuple(definition.args),
              dict(definition.kwargs))
          existing = await self._backend.setnx_unique(
              dedup_hash, task_id,
              task_info.unique_for * 1000)
          if existing is not None:  # NX rejected
              await self._signals.dispatch(
                  Signal.TASK_DUPLICATE_DETECTED,
                  existing_task_id=existing,
                  task_name=definition.task_name,
                  dedup_hash=dedup_hash,
                  source="scheduler.fire")
              logger.info("swarmq.dedup.detected", ...)
              # MUST commit_fire even on dup, otherwise infinite re-poll
              try:
                  await self._store.commit_fire(...)
              except BackendConnectionError as e:
                  logger.warning("scheduler_commit_failed_on_dedup",
                                 error=str(e))
                  # Next poll will re-emit dup signal; self-heals
              return
          # NX success: always set header (both start and completion)
          headers["x-swarmq-unique-hash"] = dedup_hash
  # Continue with broker.publish(..., headers=headers) and commit_fire
  ```
- **Kritisch:** bei Dup-Pfad MUSS `commit_fire` versucht werden
  (sonst re-pollt das zset den selben Fire ewig). Wenn commit_fire
  selbst mit `BackendConnectionError` fehlschlägt: log warning und
  return; nächster Poll-Tick emittiert ein weiteres Dup-Signal
  (akzeptierter Trade-off — transient Valkey-Outage = Signal-
  Repetitions, self-heals wenn Valkey wiederkommt).
- **Header-Policy-Uniform:** `x-swarmq-unique-hash` wird in BEIDEN
  Modi (start + completion) gesetzt — konsistent mit Client (Unit 6).
- `x-swarmq-schedfire`-Header bleibt unverändert (R3.8 Consumer-
  Dedup koexistiert).

**Patterns to follow:**
- Bestehende `_fire_one`-Struktur mit `commit_fire`-Flow
  (scheduler.py:695-730).
- Unit 6 Emission-Pattern.

**Test scenarios:**
- Happy path: Cron `*/1 * * * *` + Task `unique_for=120, unique_until=completion`
  dauert 90s → zweite Fire bei t+60 wird geskipped mit Signal,
  zset advanced, dritte Fire bei t+180 normal publiziert.
- Happy path: zwei verschiedene Schedules (different args) fire
  gleichzeitig → beide publishen, keine False-Positives.
- Edge case: Task **ohne** `unique_for` → alte Flow
  (keine SETNX, kein Signal).
- Edge case: Task mit `unique_for` aber Task-Registry löscht-den-Task
  → Handling wie existierender `task_info is None`-Pfad (log + skip).
- Edge case: SETNX success aber `broker.publish` fehlschlägt → Flag
  bleibt (TTL-Backstop); bestehender publish-Fail-Pfad regelt rest.
- Error path: `BackendConnectionError` bei SETNX → existing publish-
  fail-Pfad (Zeile 736).
- Integration: Cron-Dedup + `x-swarmq-schedfire` Layer-Koexistenz:
  Leader-Failover zwischen SETNX und commit → zweiter Leader's
  re-publish sieht schedfire-Header-Dup → kein Dopp-Worker-Run.
- Integration: Signal-Source-Differenzierung: Cron-Skip sendet
  `source="scheduler.fire"` (nicht `"client.schedule"`).

**Verification:** `uv run pytest tests/unit/test_scheduler_loop.py -k dedup`
grün. Bestehende Scheduler-Tests ohne Regression.

---

- [ ] **Unit 10: Worker `start`-Mode — Release bei Message-Consume**

**Goal:** Im `start`-Mode wird der Flag freigegeben **bevor** die Task
ausgeführt wird.

**Requirements:** R7 (start-Semantik).

**Dependencies:** Units 4, 5.

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_unique.py` (neu)

**Approach:**
- **Insertion-Point (pinned):** direkt nach `task_info = task_class.task_info`-
  Resolution (worker.py ~1049) und VOR dem ersten potentiell-
  nack(requeue=True)-ausführenden Stage (rate-limit check bei
  worker.py:1056). Begründung: (a) Release muss VOR FIRE_AND_FORGET-
  ack passieren (ack-ordering-doc Rule 1); (b) nach task_info
  resolved damit `unique_until` lesbar. Wenn Header
  `x-swarmq-unique-hash` präsent UND `task_info.unique_until == "start"`:
  ```
  if unique_hash and task_info.unique_until == "start":
      try:
          await self._backend.release_unique(unique_hash, task.id)
      except BackendConnectionError as e:
          logger.warning("unique_release_backend_error",
                         hash=unique_hash, error=str(e))
          # Continue — TTL-Backstop wirkt
  ```
- **KEIN** Release-Retry; TTL ist Safety-Net.
- `BackendConnectionError` wird geschluckt (log only) — würde sonst
  den Task fälschlich aborten, obwohl die eigentliche Task-Execution
  noch nicht begonnen hat.
- `mode != "start"`-Pfad: keine Änderung (completion-Pfad in Unit 11).

**Patterns to follow:**
- `clear_cancel_flag`-Emission-Style (worker.py).
- Ack-ordering-doc Rule "release before FIRE_AND_FORGET ack".

**Test scenarios:**
- Happy path: Worker consumt Message mit Header + TaskInfo
  unique_until=start → `release_unique` wird aufgerufen bevor
  Task ausgeführt wird; Flag in Valkey weg.
- Happy path: Nach Pickup kann zweiter Client `schedule()` mit
  gleichen Args sofort erfolgreich enqueuen (kein stale-Flag).
- Edge case: Header fehlt → kein Release (no-op).
- Edge case: TaskInfo.unique_until == completion → kein Pre-Execution-
  Release.
- Error path: `BackendConnectionError` → Log-Warning, Task läuft
  trotzdem weiter.
- Error path: `release_unique` returnt `False` (wrong owner —
  sollte bei start-Pickup nie passieren, aber defensiv) → Log-Debug,
  Task läuft trotzdem.

**Verification:** `uv run pytest tests/integration/test_worker_unique.py -k start_mode`
grün.

---

- [ ] **Unit 11: Worker `completion`-Mode — Release bei allen Terminal-Pfaden**

**Goal:** Im `completion`-Mode (Default) wird der Flag erst bei
Terminal-State freigegeben. Release an allen Pfaden aus R3.6 +
R3.4 Pattern.

**Requirements:** R6, R7 (completion-Semantik).

**Dependencies:** Units 4, 5, 10 (helper-Funktion wiederverwenden).

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_unique.py`

**Approach:**
- Helper-Funktion `Worker._maybe_release_unique(task_id, headers) -> None`
  in `worker.py`: checkt `x-swarmq-unique-hash`-Header + TaskInfo-
  Config, ruft `release_unique` mit owner-check, wrapped
  `BackendConnectionError` in Log-Warning.
- Call-Sites (vollständige Enumeration aus Repo-Research-Phase;
  Plan verifiziert via grep bei Implementation — Zeilennummern
  sind Snapshot, können drift):
  1. Nach erfolgreichem `ack` (~worker.py:1370, 1405)
  2. Nach `publish_to_dead_letter_queue` vor ack (~worker.py:350,
     386, 475, 1646)
  3. Nach `nack(requeue=False)` (~worker.py:364, 399, 501, 1046,
     1097) — **AUSSER worker.py:994** (deserialize-fail: task_id
     ist noch unbekannt, kein Release möglich; siehe Risks)
  4. In allen `clear_cancel_flag`-Aufrufern (~worker.py:943, 1275,
     1375, 1402)
  5. Fallback-Ack-Pfade nach DLQ-publish-Failure (~worker.py:353,
     389, 478)
- **Retry-Republish-Header-Preservation** (kritisch — sonst zombie-
  Flag): worker.py:1587 `publish_to_delay_queue(headers=...)` muss
  Header-Dict als **merge** der Original-Message-Headers + neuer
  retry-Header bauen, nicht als fresh-dict:
  ```
  headers = {**dict(message.headers or {}),
             "x-swarmq-retry-count": retry_count + 1,
             "x-swarmq-traceback": error_message}
  ```
  Pattern bereits in rate-limit (worker.py:229/233) und lock-conflict
  (510/514) etabliert — einheitlich machen.
- **KEIN Release bei `nack(requeue=True)`** (non-terminal; Message
  re-delivered mit preserved Header → completion-Release beim
  nächsten Terminal-Exit).
- **Log-Wording** bei release-failure: "unique_release_owner_mismatch"
  (kein "dropped" oder "lost" — folgt ack-ordering-doc).
- Idempotent: mehrere Release-Calls für dasselbe hash sind harmless
  (owner-check).

**Patterns to follow:**
- `clear_cancel_flag`-Verteilung in worker.py (R3.6).
- `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-...md`
  Log-Wording.
- `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-...md`
  Rule 1-4.

**Execution note:** Characterization-first. **Bevor** Release-Hooks
eingefügt werden: für jeden Terminal-Pfad einen Test schreiben der
verifiziert dass der bestehende Pfad wie heute funktioniert (kein
Regression). Dann Release einfügen und erweitern.

**Test scenarios:**
- Happy path: Successful execution → ack → `release_unique` aufgerufen
  → Flag weg.
- Happy path: Task failed + retries exhausted → DLQ publish → release
  → Flag weg (bereit für neuen Task mit gleichen Args).
- Happy path: Task cancelled via `client.cancel()` (mid-execution)
  → clear_cancel_flag + release_unique beide.
- Happy path: Pre-start-cancel → release vor short-circuit-ack.
- Happy path: Hard-timeout-cancel → release bei timeout-terminal.
- Happy path: Lock-conflict-cap-DLQ → release bei DLQ-publish.
- Happy path: Lock-render-failure-DLQ → release bei DLQ-publish.
- Edge case: DLQ-publish fails → fallback-ack path → release trotzdem
  (verhindert Stuck-State).
- Edge case: Header fehlt (alte Message ohne x-swarmq-unique-hash)
  → kein Release-Attempt.
- Edge case: TaskInfo unique_until=start → kein Completion-Release
  (ist schon bei Pickup gelöscht).
- Edge case: TTL-Expiry-Race: `unique_for=60` + 120s-Task → bei t+60
  TTL-expiry; t+70 zweiter Task SETNX-success; t+120 erster Task
  completion-release → owner-check schlägt fehl, DEL wird skipped,
  Flag des zweiten Tasks bleibt intakt.
- Error path: Valkey offline bei release → Log-Warning, bestehender
  Terminal-Pfad läuft weiter (kein stuck).
- Integration: Combined `TaskInfo(unique_for=60, lock="report:{user_id}",
  max_concurrency=1)` — beide Features funktionieren orthogonal;
  Lock-Acquire + Unique-Release sind unabhängige Release-Pfade.
- **Retry-Header-Preservation-Test:** Task mit `unique_for=3600,
  unique_until=completion` failed, retries via delay-queue
  republish → Header `x-swarmq-unique-hash` muss in
  redelivered Message präsent sein; Worker bei finalem Success
  releaset korrekt. Regression-Guard gegen Header-Drop.
- **AT_LEAST_ONCE Redelivery nach Release:** Task completed,
  release_unique success, dann RabbitMQ redelivered dieselbe
  Message (Worker hat pre-ack crashed) → zweiter Worker findet
  Flag weg, SETNX-success bei neuem Client-Call möglich, owner-
  check beim zweiten Release-Attempt schlägt fehl, kein-Op.

**Verification:** `uv run pytest tests/integration/test_worker_unique.py
tests/unit/test_worker.py -k unique_release` grün. Alle bestehenden
Worker-Terminal-Pfad-Tests ohne Regression.

---

- [ ] **Unit 12: E2E-Tests — User-Outcome-Szenarien**

**Goal:** End-to-End-Validierung der beiden primären Use-Cases und des
Integrations-Szenarios (Locking + Unique).

**Requirements:** R12.

**Dependencies:** Units 1-11.

**Files:**
- Create: `tests/e2e/test_unique_tasks.py`

**Approach:**
- Drei E2E-Szenarien:
  1. **Submit-Button-Doppelklick:** HTTP-Handler simulierter Doppel-
     Enqueue innerhalb 5s mit `unique_for=30`; beide Calls bekommen
     dieselbe task_id; ein `TASK_DUPLICATE_DETECTED` observed; Task
     läuft genau einmal.
  2. **Webhook-Idempotenz:** `unique_for=600, unique_until=completion`
     (Default); simuliere zweite Webhook-Delivery DURCH Retry-Backoff-
     Fenster (Task failed, retry scheduled, zweite Delivery kommt an
     während retry-backoff); zweiter Enqueue wird deduppt; nach
     completion ist Flag weg, dritter Enqueue akzeptiert.
  3. **Locking + Unique kombiniert:** `TaskInfo(unique_for=30, lock="report:{user_id}", max_concurrency=1)`;
     zwei gleichzeitige schedule()-Calls mit gleichen Args binnen 30s
     → einer wird deduppt (Signal), erster läuft, zweiter bekommt
     existing_id; erster hält Lock während Execution; dritter
     schedule() OUTSIDE dedup-Fenster aber WÄHREND Lock → wartet
     auf Semaphore (Lock-Mechanik), läuft nach erstem Finish.

**Patterns to follow:**
- `tests/e2e/test_cancellation.py` (R3.6) als E2E-Template mit
  pytest.mark.timeout und pytestmark-Pattern.
- `tests/e2e/test_locking.py` für kombinierten Pattern.

**Test scenarios:**
- Happy path: Jedes der drei Szenarien oben läuft grün gegen echte
  RabbitMQ + Valkey (docker-compose.swarmq-dev.yml).
- Signal-Assertion: `TASK_DUPLICATE_DETECTED`-Payload hat
  `existing_task_id`, `task_name`, `dedup_hash`, `source` korrekt
  gesetzt.
- Smoke-Perf: Submit-Test mit 100 sequentiellen Enqueues (alle
  dupliziert) < 3s Wall-Clock (Signal-Dispatch-Overhead).

**Verification:** `uv run pytest tests/e2e/test_unique_tasks.py`
grün gegen Dev-Stack (Ports 5673/6380).

---

## System-Wide Impact

- **Interaction graph:**
  - `Client.schedule` → neuer SETNX-Call via ValkeyBackend;
    neues Signal-Emission.
  - `Client.schedule_many` → per-Element-SETNX + per-Duplikat-Signal.
  - `Client._schedule_deferred` → SETNX vor ScheduleStore.add_schedule.
  - `Scheduler._fire_one` → SETNX vor broker.publish; commit_fire
    MUSS auch bei Dup laufen.
  - `Worker._handle_message` → neuer start-Mode-Pickup-Release.
  - `Worker` Terminal-Pfade (~10 Call-Sites) → neuer completion-Mode-
    Release.
- **Error propagation:**
  - SETNX-Failures (transient Valkey) → `BackendConnectionError`
    propagiert im Client (User sieht Exception); im Worker geschluckt
    (Log-Warning, TTL-Backstop).
  - Release-Failures im Worker → Log-Warning, Terminal-Pfad läuft
    weiter; kein stuck-state.
  - `canonical_encode` TypeError bei Non-JSON-Native-Args → bubble-up
    als `ResultSerializationError` (analog `encode_json` heute).
- **State lifecycle risks:**
  - Stale-Flag-Zombie bei Worker-Crash zwischen SETNX und Ack:
    TTL-Backstop wirkt. Dokumentierte Einschränkung, dieselbe Klasse
    wie Cancel-Flag-Zombie.
  - TTL-Expiry-Race: bei `completion`-Mode + `unique_for < task_runtime`
    kann ein zweiter Task parallel laufen. Owner-Check im Release
    verhindert dass der erste Task den Flag des zweiten löscht.
    Behavior ist korrekt-aber-überraschend; Edge-Case-Test deckt ab.
- **API surface parity:**
  - `Client.schedule`, `Client.schedule_many`, `Client.run` (via
    schedule) — alle neuen kwargs verfügbar.
  - `client.run()` propagiert existing_task_id transparent
    via `schedule()` + `get_result()`. Keine Spezialbehandlung.
  - Scheduler honoriert `TaskInfo.unique_for` gleichermaßen —
    uniformes Verhalten unabhängig vom Entry-Point.
- **Integration coverage:**
  - Signal-Dispatch bei `TASK_DUPLICATE_DETECTED` mit Real-SignalDispatcher
    (nicht AsyncMock) — SignalDispatcher.dispatch semantics (exceptions
    swallow) muss per Integration-Test verifiziert werden.
  - Unit 11 Release-Pfade in Kombination mit Rate-Limit, Lock, Cancel,
    Middleware — alle R3.x-Features koexistieren.
  - Unit 12 Cookbook-Szenario.
- **Unchanged invariants:**
  - Bestehende `Client.schedule()`/`schedule_many()`/`schedule()`
    Return-Types UNVERÄNDERT (`str` bzw `list[str]`).
  - `Client.schedule_many(tasks, queue=...)` positional-Caller
    funktionieren weiter (queue bleibt pos-or-kw).
  - `x-swarmq-schedfire`-Header-Dedup (R3.8) bleibt unverändert.
  - `clear_cancel_flag`-Semantik (R3.6) unverändert — Unique-Release
    ist **zusätzlicher** Hook neben Cancel-Release.
  - `TaskInfo`-Felder ohne `unique_for=` verhalten sich wie heute.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Terminal-Pfad-Enumeration in Unit 11 unvollständig → stuck-Flag im DLQ-Pfad | grep-based verification at implementation time (+ characterization tests per Pfad). Doku-Update bei jedem neuen DLQ-Pfad in Zukunft. |
| Retry-Republish in worker.py:1587 droppt `x-swarmq-unique-hash`-Header → zombie-Flag in completion-Mode bis TTL | Unit 11 preserviert Header via `{**dict(message.headers or {}), ...}`-Merge (Pattern aus rate-limit/lock-conflict). Dedizierter Test-Scenario. |
| `canonical_encode` Pre-Normalisierung weicht von msgspec.json-Default ab → User-Args mit UUID/datetime werden jetzt stringified statt ISO-encoded | Dokumentierte User-Outcome-Invariante: Dedup-Hash ist deterministisch über UUID/datetime/Decimal-Args hinweg. Regression-Test in Unit 1 zeigt canonical_encode(UUID) == canonical_encode(str(UUID)). |
| Deserialize-fail-Pfad (worker.py:994) kann Flag nicht releasen (task_id unbekannt) — bleibt bis TTL | Dokumentierte Einschränkung. Für typische `unique_for=60` vernachlässigbar; für `unique_for=3600` Webhook kann stuck-Fenster bis 1h entstehen. User die das nicht akzeptieren, wählen kürzere TTL oder nutzen `unique_until=start`. |
| Client-Modus (registry=None) kann keine TaskInfo-Defaults auflösen — per-call `unique_for=` erforderlich | Dokumentiert im Client-Modus-Docstring + Runtime-Log bei Client-Construction wenn `_MISSING` als `unique_for` weitergeleitet wird. |
| Double-SETNX-Race zwischen Unit 7 (Client-Deferred) und Unit 9 (Scheduler-Fire) | Partitioniert via `source="cli_unique"` + `dedup_hash` in ScheduleDefinition. Unit 9 skippt SETNX für cli_unique-Entries. |
| Scheduler-`commit_fire` bei Dup-Pfad schlägt fehl → Signal-Storm bei transient Valkey-Outage | Log + return; next poll emitiert weiteres Dup-Signal. Self-healing wenn Valkey wiederkommt. Operator-Filter via `source="scheduler.fire"`-Feld. Akzeptierter v1-Trade-off. |
| ScheduleDefinition Schema-Change (neue `dedup_hash`-Feld + Source-Value) bricht ScheduleStore-Migration | Neue Felder sind `Optional` mit Default-None — msgpack-Roundtrip-kompatibel zu bestehenden Schedules. Integration-Test: alter Schedule ohne `dedup_hash` wird korrekt geladen. |
| canonical_encode hash-Instability zwischen Python-Versionen / msgspec-Upgrades | Eng gehaltener Scope (keine datetime/UUID/dataclass) reduziert Risiko. Plan-Tip: Version-Prefix im Hash (`sha256("v1:" + ...)`) als Escape-Hatch wenn später Bedarf. Nicht v1. |
| Scheduler-Dedup macht Cron-Fires-Behavior subtil anders (Rate=Completion statt Intervall) | Bewusste Semantik-Entscheidung (origin CD7). Test-Szenario in Unit 9 explizit. |
| Leader-Failover führt zu Ghost-`TASK_DUPLICATE_DETECTED`-Events | Akzeptierte v1-Einschränkung. Operator-Filter-Empfehlung: `source`-Feld prüfen. Separates `SCHEDULER_FIRE_DEDUP`-Signal ist Deferred. |
| TTL-Expiry-Race: `unique_for < task_runtime` in completion-Mode | Owner-Check verhindert Data-Corruption. Behavior ist korrekt aber überraschend. Dokumentation im Cookbook-Beispiel + Edge-Case-Test. |
| Sentinel `_MISSING` wird public-API-exposed | Als modul-privater `object()`-Instance implementieren; Unit 6/8 Tests verifizieren dass nicht aus `__init__.py` exportiert. |
| `_RESERVED_RUN_PARAM_NAMES`-Erweiterung bricht User-Tasks mit `unique_for`/`unique_until`-Parametern | Migration-Hinweis in Fehlermeldung (Unit 2). User-Base ist pre-v1, Impact wahrscheinlich gering. Changelog-Entry. |
| Valkey-Unavailability bei SETNX aborted legitime Schedules | Bestehende `BackendConnectionError`-Semantik (analog Cancel-Flag-Pfad). User sieht Exception, kann retry. |

## Documentation / Operational Notes

- **Cookbook-Beispiel** (Teil von Unit 12 oder separater Doc-PR):
  "Idempotent Task-Scheduling mit R3.5" — drei Use-Cases (Submit,
  Webhook, Kombi-mit-Locking) mit Code-Snippets.
- **Migration-Notes** (Teil von Unit 2): `_RESERVED_RUN_PARAM_NAMES`-
  Erweiterung um `unique_for`/`unique_until`. User mit diesen
  Param-Namen in `run()` bekommen klare Fehlermeldung.
- **Signal-Observability-Docs:** neuer Abschnitt in `docs/signals.md`
  (falls existiert, sonst Skip) für `TASK_DUPLICATE_DETECTED`-Payload.
- **Operator-Guide:** bei hoher `TASK_DUPLICATE_DETECTED`-Rate mit
  `source="scheduler.fire"` → Leader-Failover-Häufigkeit prüfen
  (neuer Kapitel "SwarmQ-Troubleshooting" — optional).

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md](../brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md)
- **Related solutions:**
  - [ack-ordering](../solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md)
  - [nack-requeue-false-dlx](../solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md)
  - [async-deadlock-wrapping](../solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md)
- **Related plans:**
  - [feat-phase3b3-task-cancellation-plan.md](./2026-04-16-009-feat-phase3b3-task-cancellation-plan.md) (R3.6 — Pattern-Vorlage für Worker-Terminal-Pfade)
  - [feat-phase3b2-task-locking-plan.md](./2026-04-16-008-feat-phase3b2-task-locking-plan.md) (R3.4 — Lua-Release + lock_release.lua Vorlage)
  - [feat-phase3c-scheduler-plan.md](./2026-04-17-002-feat-phase3c-scheduler-plan.md) (R3.8/R3.9 — Scheduler-Fire-Flow + schedfire-Dedup)
- **Related code:**
  - `src/swarmq/client.py` (Client.schedule, _schedule_deferred, schedule_many)
  - `src/swarmq/worker.py` (Terminal-Pfade, clear_cancel_flag-Template)
  - `src/swarmq/scheduler.py` (_fire_one)
  - `src/swarmq/serialization.py` (encode_json, neue canonical_encode)
  - `src/swarmq/signals.py` (Signal enum, SignalDispatcher)
  - `src/swarmq/task.py` (TaskInfo, _RESERVED_RUN_PARAM_NAMES)
  - `src/swarmq/lua/lock_release.lua` (Lua-Release-Vorlage)

## Implementation Audit (2026-05-26)

Phase 3.5 gemerged in `main` (Merge-Commit `93f576f`), Code-Belege:

  - `src/swarmq/lua/unique_release.lua` vorhanden
  - `src/swarmq/signals.py:42` — `Signal.TASK_DUPLICATE_DETECTED`
  - `src/swarmq/backend/valkey.py:34,81` — `_unique_release_script`
  - `tests/e2e/test_unique_tasks.py` existiert

Unit-Commits: `9fd2717` (U4+U5), `55f967a` (U10+U11). Ce-review-Sweeps:
`5dcb10f` (batches 1-4), `43debd7` (5-6), `a296808` (safe_auto).
