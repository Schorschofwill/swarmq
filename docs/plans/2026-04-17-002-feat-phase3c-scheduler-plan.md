---
title: "feat: Phase 3c — Scheduler (R3.8 Delay/ETA + R3.9 Cron + R3.10 schedule-CLI)"
type: feat
status: active
date: 2026-04-17
deepened: 2026-04-17
origin: docs/brainstorms/2026-04-16-phase-3c-scheduler-requirements.md
---

# feat: Phase 3c — Scheduler (R3.8 Delay/ETA + R3.9 Cron + R3.10 schedule-CLI)

## Overview

SwarmQ bekommt einen embedded Scheduler. Drei User-facing Features in einer
Phase:

- **Delay/ETA** (R3.8): `client.schedule(task, *args, delay=60, **kwargs)`
  und `eta=datetime(...)` als reserved keyword-only args.
- **Cron** (R3.9): `TaskInfo(cron="*/5 * * * *")` statisch und
  `swarmq schedule add <name> --cron ...` dynamisch; beide persistent in
  Valkey. Leader-Election via Valkey-Lock, Heartbeat, Failover.
- **schedule-CLI** (R3.10-Teil): `swarmq schedule list/add/remove/status`
  (4 Subcommands; `prune` wurde nach Review-Finding F-01 deferred —
  siehe Deferred-Section).

Die drei teilen denselben Sorted-Set-Poll-Loop, dasselbe Persistence-Layer,
dieselbe Leader-Election — weshalb sie in einer Phase zusammenliegen.

## Problem Frame

Siehe Origin-Brief. Kurzfassung: ohne Scheduler ist SwarmQ auf
Trigger-basierte Workloads beschränkt. Konkurrenz-Parity-Feature (Celery,
Dramatiq, arq, taskiq haben es alle). Priorität ggb. R3.5 (Unique Tasks)
stammt aus maintainer-Intuition — Post-Shipping wird User-Feedback
gesammelt und die Reihenfolge ggf. korrigiert (siehe Origin „Honest
Framing").

## Requirements Trace

- **R3.8 Delay/ETA** (origin §Scope): `schedule(delay=)`, `schedule(eta=)`
  → Valkey Sorted-Set `sq:sched:zset` → Leader pollt → publish to Main-Queue.
- **R3.9 Embedded Scheduler** (origin §Scope): Leader-Election via
  Valkey-Lock, Heartbeat/TTL, croniter-basierte Cron-Expressions,
  TaskInfo.cron + CLI-Schedules.
- **R3.10-Scheduler-CLI** (origin §Scope): `swarmq schedule list/add/remove/status`.
- **Closed Decisions** 1-19 aus origin (§Closed Decisions) — alle hier umgesetzt:
  - CD10: Publish-before-ZREM mit Consumer-Dedup (`x-swarmq-schedfire`-Header + SETNX)
  - CD11: insert-if-absent TaskInfo-Sync (`disabled`-Marker blockiert Re-Insert)
  - CD16: `TaskInfo.catchup: bool = False` reserviert, `True` → NotImplementedError
  - CD17: `SCHEDULER_LEADER_HANDOVER` vs `SCHEDULER_LEADER_FAILOVER` getrennt
  - CD18: `LockManager.renew()` neu, plus `lock_renew.lua`
  - CD19: Clock-Skew-Double-Execution durch Consumer-SETNX-Dedup ausgeschlossen

## Scope Boundaries

**In-Scope (aus origin, nach Fork-Entscheidungen):**
- Delay/ETA via reserved kwargs auf `Client.schedule`
- `TaskInfo(cron=...)` statisch; `catchup=True` wird beim
  Construction-Zeitpunkt rejected (P2-3 Fail-fast)
- `swarmq schedule` CLI mit 4 Subcommands: **list/add/remove/status**
  (prune wurde deferred, Fork 3)
- Leader-Election mit Heartbeat + Failover + Handover-Signals
- Consumer-Side SETNX-Dedup für Schedule-Fire-Duplicates
  (TTL ≥ retry_max_delay, P1-1)
- 3 neue Lua-Scripts (`sched_poll.lua`, `sched_commit.lua`
  mit Owner-Check, `lock_renew.lua` mutex-only)
- Neues Modul `src/swarmq/scheduler.py`
- Static-cron-Update-Semantik: modifizierte `TaskInfo.cron` werden
  beim Leader-Sync nachgezogen (Fork 1)
- Unit + Integration + E2E Tests (Test-Discipline wie R3.6: `asyncio.Event`
  statt `asyncio.sleep` als Readiness-Barriere)

**Out-of-Scope (aus origin §Non-Goals):**
- Unifizierung DQ + Sorted-Set — zwei separate Mechanismen
- Catch-up-Implementation (`catchup=True`) — nur Feld reserviert
- Timezone-Support (v1 UTC-only)
- Interval-Schedules (`every=timedelta`)
- Workflow-Schedules — Phase 4
- DLQ-CLI — separater Plan
- Schedule-History/Audit-Log
- Extended-Cron-Syntax L/W/# — explizit rejected am API-Eingang

### Deferred to Separate Tasks

- **Catch-up-Logic** (`catchup=True`-Implementation): wird aktuell mit
  `NotImplementedError` im `TaskInfo.__post_init__` rejected; echte
  Logik in einem separaten Plan 3d oder später
- **DLQ-CLI** (`swarmq dlq list/retry/purge`): eigener Plan nach Scheduler
- **Schedule-History**: Phase 4 oder später
- **`swarmq schedule prune`** (TaskInfo-Rename-Cleanup): **aus dieser
  Phase entfernt** (Fork 3). Rationale: Pre-User-Library hat keinen
  unbounded-Storage-Druck (Defs sind O(#TaskInfo)), und
  `--module`-Registry-Loading ist separate Infrastruktur. Orphan-
  Handling ist bis zum Folge-Plan via `schedule remove --permanent`
  erledigbar. Für Code-Rename-Szenarien greift außerdem Fork 1:
  der static-cron-Update propagiert änderungen an *bestehenden*
  Tasks.

## Context & Research

### Relevant Code and Patterns

- **Pattern-Source — R3.4 Locking**:
  - `src/swarmq/locking.py` — `LockManager` wird um `renew()` erweitert.
    Bestehende `acquire`/`release` Semantik unverändert.
  - `src/swarmq/lua/lock_acquire.lua`, `lock_release.lua` — Template für
    das neue `lock_renew.lua`.
- **Pattern-Source — R3.3 Rate-Limit**:
  - `src/swarmq/limits.py:RateLimiter` — `register_script`-Pattern +
    NOSCRIPT-Recovery. Template für `SchedulerLua`-Wrapper.
  - `src/swarmq/lua/sliding_window_counter.lua` — Lua-File-Struktur.
- **Pattern-Source — R3.6 Task Cancellation (gerade gemerged)**:
  - `src/swarmq/worker.py:_cancel_monitor` — Pattern für die
    `SchedulerLoop`-Coroutine (periodic poll via `asyncio.wait(timeout=...)`).
  - `src/swarmq/worker.py:_detached_releases`-Set — Pattern für Shutdown-
    safe Task-Collections.
  - Consumer-Dedup-Pattern: `Worker._pre_start_cancel_check` liest Header
    (`x-swarmq-*`) aus `message.headers` — dasselbe Muster nutzen wir
    für `x-swarmq-schedfire`.
- **Pattern-Source — Config**:
  - `src/swarmq/config.py` — drei neue Felder
    (`scheduler_poll_interval`, `scheduler_heartbeat_interval`,
    `scheduler_lock_ttl`) folgen dem `cancel_poll_interval`-Muster.
- **Client-API**:
  - `src/swarmq/client.py:schedule` — erweitern um `delay` und `eta`
    als reserved keyword-only args (zwischen `queue` und `**kwargs`).
- **Worker-Lifecycle**:
  - `src/swarmq/worker.py:Worker.start` — der Scheduler wird hier
    embedded gestartet. `_execute_shutdown` muss den Scheduler-Lock
    sauber freigeben (HANDOVER-Signal).
- **Registry + TaskInfo**:
  - `src/swarmq/task.py:TaskInfo` — frozen dataclass mit explizitem
    `__init__`. Wird um `cron: str | None = None` und
    `catchup: bool = False` erweitert, mit eager-validation im
    `__post_init__`.
- **CLI-Struktur**:
  - `src/swarmq/cli.py` — aktuell nur `worker`-Subcommand. Neue
    `schedule`-Subgroup mit 4 Subcommands ergänzen. argparse-Pattern
    konsistent halten.
- **Signal-Inventar**:
  - `src/swarmq/signals.py` — `SCHEDULER_LEADER` und `SCHEDULER_STANDBY`
    sind bereits definiert. Neu ergänzt: `SCHEDULER_HEARTBEAT`,
    `SCHEDULER_LEADER_HANDOVER`, `SCHEDULER_LEADER_FAILOVER`.
- **Broker publish-with-headers**:
  - `src/swarmq/broker/rabbitmq.py:publish` — nimmt `**properties` ab,
    muss Header durchreichen (schon so implementiert für Retry-Headers).

### Institutional Learnings

- **`docs/recherche-embedded-scheduler.md`** — vollständiger Design-Guide,
  Ansatz 1 (Valkey Distributed Lock) ist empfohlen und wird hier umgesetzt.
  Enthält Bugs aus RedBeat als Warnungen (§8).
- **`docs/recherche-deepening-lua-scripts.md` §2** — `extend_scheduler_lock.lua`
  Template (für `lock_renew.lua`).
- **`docs/recherche-deepening-lua-scripts.md` §8** — Sorted-Set-Poll + ZREM-Pattern;
  Origin-Doc übernimmt die AT-LEAST-ONCE-Empfehlung (Publish-before-ZREM).
- **`docs/recherche-operational-features.md` §1** — Pattern D (Valkey Sorted Set).
- **`docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`**
  — Ack-Ordering-Discipline: Consumer-Dedup-Check läuft VOR der normalen
  Pipeline, genau wie Cancel-Pre-Start.
- **`docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md`**
  — alle Valkey-I/O im Scheduler in `BackendConnectionError` wrappen.

### External References

- `croniter` (~50 kB, Pure-Python): https://github.com/kiorky/croniter —
  wird als Core-Dep hinzugefügt.
- Keine weiteren externen Recherchen nötig — lokale Recherche-Docs
  decken alle Patterns ab.

## Key Technical Decisions

Übernommen aus Origin-Brief (CD1-CD19) plus Plan-Defaults für Open Questions:

- **Modul-Struktur**: Eigenes `src/swarmq/scheduler.py` (analog
  `locking.py`, `limits.py`). Enthält `SchedulerLoop`,
  `ScheduleStore`, Lua-Script-Wrapper, Idempotency-Key-Helper.
- **Persistence-Layout** (siehe origin CD12):
  - `sq:sched:defs` — Global-Hash. Field-Key = schedule_name, Value =
    JSON-encoded `ScheduleDefinition` (msgspec.Struct, siehe Unit 7).
    Enthält `disabled: bool` als inline-Feld — **einziger** Marker
    für CLI-disabled Schedules (keine separate `sq:sched:disabled:*`-Keys).
  - `sq:sched:zset` — Sorted-Set. Score = next_fire_ts_ms,
    Member = schedule_name (für cron) oder `oneshot:<uuid>` (für delay/eta).
    One-shot-Daten leben vollständig in `sq:sched:defs` — **keine**
    separaten `sq:sched:oneshot:<uuid>`-Hashes.
  - `sq:lock:scheduler:leader` — Leader-Election-Lock (Keypath wird vom
    `LockManager` erzeugt: `lock_name="scheduler:leader"` + fester
    Prefix `sq:lock:` aus `locking.py`).
  - `sq:sched:handover:<previous_owner>` — Transient-Marker, gesetzt
    von outgoing Leader direkt vor `release()`, TTL 5 s. Incoming
    Leader GETs+DELs diesen Key beim Acquire; **Key vorhanden →
    HANDOVER**, **Key absent → FAILOVER** (siehe Unit 10).
  - `sq:sched:fired:<schedule>:<fire_ts_ms>` — Consumer-Dedup-Key
    (SETNX, TTL = `scheduler_dedup_ttl` Config-Field, Default
    `max(60, retry_max_delay + 300)` s = 3900 s bei Default-Config).
    Grund: Dedup-Window muss Retry-Horizont abdecken, sonst kann ein
    retried Message nach >60 s trotz bereits gefeuertem Schedule
    erneut ausgeführt werden.
- **Serialization**: msgspec.json für Schedule-Definitions (Origin CD13).
- **CLI-Output-Format**: Table als Default, JSON via `--format json`.
- **Dedup-Key-TTL** (korrigiert ggb. Origin CD10): **Default `max(60,
  retry_max_delay + 300)` s = 3900 s bei Default-`retry_max_delay=3600`**.
  Ursprünglich behauptete CD10 „60 s deckt retry_max_delay defaults",
  was faktisch falsch war (60 s ≪ 3600 s Retry-Horizont). Neue
  Invariante: `scheduler_dedup_ttl >= retry_max_delay + safety_margin`.
  Konfigurierbar via `scheduler_dedup_ttl`-Config (Unit 2) bzw.
  `SWARMQ_SCHEDULER_DEDUP_TTL`. Validation im `__post_init__`:
  raised `ConfigurationError` wenn `scheduler_dedup_ttl < retry_max_delay`.
- **TaskInfo-Cron-Rename/Delete-Cleanup (Fork 3 — deferred):**
  `swarmq schedule prune` wurde nach Review-Finding P2-7/F-01 in einen
  Folge-Plan verschoben. Modifikationen an existierenden Tasks werden
  durch Fork 1 (static-cron-Update) abgefangen; Orphan-Einträge nach
  task-delete sind bis dahin via `schedule remove --permanent`
  manuell entfernbar.
- **Poll-Intervall-Default**: 1.0 s. Heartbeat 10 s, Lock-TTL 30 s
  (3×-Sicherheitsmarge).
- **Leader-Transition-Signal-Timing**: `SCHEDULER_LEADER_HANDOVER` wird
  **vor** Lock-Release emittiert (User sieht „gleich weg"). Failover
  wird beim erfolgreichen acquire eines Standby emittiert, der vorher
  kein Leader war.
- **Handover-Behavior bei graceful-Shutdown**: Leader released seinen
  Lock aktiv, damit Standby innerhalb von 1 s übernehmen kann (nicht
  30 s TTL warten).
- **Eager-Validation für TaskInfo.cron**: `croniter(cron,
  datetime.now(UTC))` im `__post_init__` (P3-1: `utcnow()` ist in
  Python 3.12+ deprecated; Projekt targeted 3.14). Bei `catchup=True`
  → **sofort `NotImplementedError`** im `__post_init__` (P2-3
  Fail-fast, nicht zur Laufzeit).
- **Pipeline-Position des Consumer-Dedup-Checks**: VOR dem
  Cancel-Pre-Start-Check, weil ein duplikat-Schedule-Fire nicht mal
  die Cancel-Logik betreten sollte. Minimaler Overhead, konsistente
  Ack-Ordering-Discipline.

## Open Questions

### Resolved During Planning

- **Scheduler-Modul**: eigenes `src/swarmq/scheduler.py`.
- **Config-Felder**: 4 separate Felder (`scheduler_poll_interval`,
  `scheduler_heartbeat_interval`, `scheduler_lock_ttl`,
  `scheduler_dedup_ttl` — letzteres per P1-1 aus dem Review ergänzt).
- **CLI-Output-Format**: Table default, `--format json` opt-in.
- **Dedup-Key-TTL**: `max(60, retry_max_delay + 300)` s (P1-1;
  Default 3900 s statt der ursprünglichen 60 s).
- **Rename-Cleanup**: `swarmq schedule prune` **deferred** (Fork 3);
  Fork 1 (static-cron-Update) reduziert den Bedarf.
- **Handover-Timing**: Signal vor Lock-Release; Handover-Marker
  (`sq:sched:handover:<owner>`, TTL 5 s) unterscheidet HANDOVER vs
  FAILOVER (P2-1).
- **ScheduleStore-Wiring**: Lazy-Init im `Client` beim ersten
  delay/eta-Aufruf (Fork 2).
- **Static-Cron-Update**: Leader-Sync propagiert modifizierte
  `TaskInfo.cron`-Strings in den defs-Hash (Fork 1); CLI-Einträge
  bleiben insert-if-absent.

### Deferred to Implementation

- **Exact Method-Namen** (`_sync_taskinfo_crons`, `_poll_once`, etc.) —
  beim Schreiben entscheiden.
- **Lua-Script-Parameter-Packing** — ob `msgspec.json.encode` vor Lua
  oder cjson in Lua. Die recherche empfiehlt Serialization auf Python-Seite.
- **Timing-Toleranzen in E2E-Tests** — werden beim Schreiben kalibriert.
- **Prune-Trigger**: nur manuell via CLI, oder automatisch beim
  Leader-Sync? Beim Schreiben entscheiden; Default: nur manuell.
- **TaskInfo-Cron-Rename-Detection**: vergleich `sq:sched:defs` vs.
  Registry beim Leader-Acquire — Details im Code.

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification.*

### Sequence: Cron-Schedule full flow

```
App-Start (Worker A)                     App-Start (Worker B)
  ├─ TaskInfo._registered_tasks ≡ cron    ├─ (same)
  └─ Worker.start()                       └─ Worker.start()
      ├─ connect broker + backend             ├─ connect broker + backend
      ├─ SchedulerLoop.start()                ├─ SchedulerLoop.start()
      │   └─ LockManager.acquire(                │   └─ acquire → False (A has lock)
      │       "sched:lock:leader", ttl=30s)       │       emit SCHEDULER_STANDBY
      │       → True                              │       → periodic recheck
      │       emit SCHEDULER_LEADER               └─ consume main queue (runs tasks)
      │       + sync_taskinfo_crons()
      │         (insert-if-absent into
      │          sq:sched:defs hash)
      └─ consume main queue

Leader-Loop (A, every 1s):
  ├─ lock_renew.lua → PEXPIRE if owner matches
  │   emit SCHEDULER_HEARTBEAT on success
  ├─ compute next_fire for each cron-schedule → update sq:sched:zset
  ├─ sched_poll.lua:
  │   local due = ZRANGEBYSCORE(zset, -inf, now_ms, LIMIT 0 100)
  │   return due (NO ZREM yet)
  ├─ for each due_member:
  │   ├─ fire_ts = score; task_id = uuid7()
  │   ├─ header = {"x-swarmq-schedfire": f"{schedule}:{fire_ts}"}
  │   ├─ broker.publish(queue, body, headers=header)
  │   └─ sched_commit.lua:
  │       ZREM(zset, member)
  │       (if cron): score = croniter.get_next(now)
  │                  ZADD(zset, score, schedule_name)
  │       HSET(defs, schedule_name, {..., last_fire_at: now})

Leader crash or SIGKILL:
  ├─ lock expires after TTL (30s)
  └─ Worker B's periodic recheck:
      └─ LockManager.acquire → True
          emit SCHEDULER_LEADER_FAILOVER (previous leader was not self)
          + sync_taskinfo_crons (insert-if-absent)
          Loop begins

Leader graceful shutdown (SIGTERM on A):
  ├─ SchedulerLoop.stop():
  │   emit SCHEDULER_LEADER_HANDOVER (before release)
  │   LockManager.release("sched:lock:leader")
  └─ Worker B next recheck (within ~1s):
      └─ acquire → True
          emit SCHEDULER_LEADER (not FAILOVER, because B is new and
          handover was clean)

Consumer (Worker _handle_message), any worker:
  ├─ deserialize message
  ├─ structlog bind
  ├─ if message.headers.get("x-swarmq-schedfire"):
  │   schedfire_value = message.headers["x-swarmq-schedfire"]
  │   if not await backend.set_schedfire_dedup(
  │         schedfire_value,
  │         ttl_ms=int(config.scheduler_dedup_ttl * 1000)):
  │       # duplicate: ack and skip
  │       await broker.ack(message)
  │       structlog.clear_contextvars()
  │       return
  ├─ (existing) Cancel-Pre-Start-Check → etc.
  └─ (existing) normal pipeline
```

### Client.schedule — new keyword-only args

```python
# Directional — do not copy as implementation.
async def schedule(
    self,
    task_name: str,
    *args: Any,
    queue: str | None = None,
    delay: float | None = None,      # NEU, reserviert
    eta: datetime | None = None,      # NEU, reserviert
    **kwargs: Any,
) -> str:
    if delay is not None and eta is not None:
        raise ValueError("Specify delay OR eta, not both")
    if delay is not None or eta is not None:
        return await self._schedule_deferred(
            task_name, args, kwargs, queue=queue,
            delay=delay, eta=eta,
        )
    # existing path unchanged
    ...
```

**Backward-Compat-Guard für `delay` / `eta` als Task-Parameter-Namen
(Review-Finding P1-3):** `delay` und `eta` sind ab diesem Release
SwarmQ-reserved keyword-only args auf `Client.schedule` /
`SwarmQ.schedule`. Task-Funktionen, die ein Parameter `delay` oder `eta`
akzeptieren, würden silently ihre Payload verlieren. **Eager-Guard bei
Task-Registrierung** (Unit 3): `@app.task`-Decorator inspiziert
`inspect.signature(fn)`, erkennt `delay` oder `eta` als positional/
keyword-Parameter, und **raised `ValueError` mit Migration-Hint**:

```
ValueError: task 'send_email' accepts 'delay' as a parameter, which
conflicts with Client.schedule's reserved keyword. Rename the task
parameter OR pass it explicitly via kwargs mapping, e.g.:
    client.schedule('send_email', kwargs={'delay': 60})
```

Alternatively: explicit escape-hatch `kwargs=` (dict) Keyword auf
`Client.schedule` um die kwargs-Forwarding-Semantik zu spalten
— optional, wenn Migration-Guard nicht ausreicht.

## Implementation Units

12 Units, dependency-ordered. Jedes Unit kann atomar committet werden
und die Test-Suite muss am Ende grün sein (TDD-Pflicht, AGENTS.md).

---

- [x] **Unit 1: Signal-Enum-Erweiterung**

**Goal:** Drei neue Signal-Werte für Scheduler-Lifecycle: `SCHEDULER_HEARTBEAT`,
`SCHEDULER_LEADER_HANDOVER`, `SCHEDULER_LEADER_FAILOVER`.

**Requirements:** CD9, CD17 (origin).

**Dependencies:** keine.

**Files:**
- Modify: `src/swarmq/signals.py`
- Test: `tests/unit/test_signals.py`

**Approach:**
- Drei neue Enum-Werte in der `Signal`-Enum, unter „Scheduler lifecycle"-
  Sektion (wo SCHEDULER_LEADER / SCHEDULER_STANDBY schon sind).
- Value-Strings: `"scheduler_heartbeat"`, `"scheduler_leader_handover"`,
  `"scheduler_leader_failover"`.
- Docstring-Payload-Sektion im Class-Docstring erweitern.
- **Rationale HEARTBEAT** (Antwort auf Review-Finding P3-3):
  `SCHEDULER_HEARTBEAT` ist vorwiegend Test-Infrastruktur (E2E
  Leader-Liveness-Probe in Unit 13) + User-Hook für externe
  Monitoring-Dashboards (analog zu existierendem `WORKER_HEARTBEAT`).
  Kein internes Consumer im Worker — bewusst User-space-only.

**Execution note:** Test-first.

**Patterns to follow:**
- Bestehende `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` in `src/swarmq/signals.py`.

**Test scenarios:**
- Happy path — `Signal.SCHEDULER_HEARTBEAT.value == "scheduler_heartbeat"`.
- Happy path — `Signal.SCHEDULER_LEADER_HANDOVER.value == "scheduler_leader_handover"`.
- Happy path — `Signal.SCHEDULER_LEADER_FAILOVER.value == "scheduler_leader_failover"`.
- Edge — `test_signal_enum_has_exactly_N_values` wird um 3 erhöht.

**Verification:**
- `pytest tests/unit/test_signals.py -x` grün.
- Alle bestehenden 548+ Unit-Tests bleiben grün.

---

- [x] **Unit 2: Config-Felder für Scheduler**

**Goal:** Vier neue `SwarmQConfig`-Felder:
`scheduler_poll_interval` (1.0 s default), `scheduler_heartbeat_interval`
(10.0 s), `scheduler_lock_ttl` (30.0 s), `scheduler_dedup_ttl`
(**Default `max(60.0, retry_max_delay + 300.0)` s** — bei
Default-`retry_max_delay=3600` → 3900 s). Plus Env-Var-Overrides.

**Requirements:** origin OQ2 (now resolved) + **review-finding P1-1
(Dedup-TTL muss Retry-Horizont abdecken, sonst Double-Execution)**.

**Dependencies:** Unit 1.

**Files:**
- Modify: `src/swarmq/config.py`
- Test: `tests/unit/test_config.py`

**Approach:**
- Vier Felder mit sinnvollen Defaults.
- Env-Overrides: `SWARMQ_SCHEDULER_POLL_INTERVAL`,
  `SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL`,
  `SWARMQ_SCHEDULER_LOCK_TTL`, `SWARMQ_SCHEDULER_DEDUP_TTL`.
- `scheduler_dedup_ttl` Default via `field(default=None)` +
  `__post_init__` — wenn None: `max(60.0, self.retry_max_delay + 300.0)`.
  Explicit-value überschreibt.
- Validation in `__post_init__`:
  - `scheduler_poll_interval > 0`,
    `scheduler_heartbeat_interval > 0`,
    `scheduler_lock_ttl > 0`,
    `scheduler_dedup_ttl > 0`;
  - `scheduler_lock_ttl >= scheduler_heartbeat_interval * 2`
    (Safety-Margin gegen Heartbeat-Miss);
  - `scheduler_dedup_ttl >= retry_max_delay` (Invariante:
    Dedup-Window deckt Retry-Horizont);
  - sonst `ConfigurationError`.

**Execution note:** Test-first, Triangulation für Validation.

**Patterns to follow:**
- `cancel_poll_interval` in `src/swarmq/config.py` (letzte Addition).

**Test scenarios:**
- Happy path — Defaults korrekt (1.0, 10.0, 30.0, 3900.0 bei default
  `retry_max_delay=3600`).
- Happy path — Explicit-value-override funktioniert für alle vier.
- Happy path — Env-Var-Override für jedes der vier Felder.
- Happy path — `scheduler_dedup_ttl` Default skaliert mit
  `retry_max_delay=7200` → 7500 s.
- Error — Negative oder Null Werte raisen `ConfigurationError`
  (für alle vier Felder je ein Testcase).
- Error — `scheduler_lock_ttl < scheduler_heartbeat_interval * 2` raised
  `ConfigurationError` (mit message die Mindest-Verhältnis erklärt).
- Error — `scheduler_dedup_ttl < retry_max_delay` raised
  `ConfigurationError` (mit message die Retry-Horizont-Invariante
  erklärt; z.B. `dedup_ttl=60, retry_max_delay=3600` → Fehler).

**Verification:**
- `pytest tests/unit/test_config.py -x` grün.

---

- [x] **Unit 3: TaskInfo-Erweiterung — cron + catchup**

**Goal:** `TaskInfo.cron: str | None = None` und
`TaskInfo.catchup: bool = False`. Eager-Validation im `__post_init__`:
Cron-String via croniter validiert; `catchup=True` wirft
`NotImplementedError`; Extended-Syntax (L/W/#) wird rejected.

**Requirements:** CD14, CD16 (origin).

**Dependencies:** Unit 1. **Neue Dependency**: `croniter` in
`pyproject.toml` `[project.dependencies]`.

**Files:**
- Modify: `pyproject.toml` (croniter als Core-Dep)
- Modify: `src/swarmq/task.py`
- Test: `tests/unit/test_task.py`

**Approach:**
- Zwei Felder in `TaskInfo`-frozen-dataclass, signature + default-dict +
  `object.__setattr__` ergänzen.
- Validation:
  - `if cron is not None`: regex-check für verbotene Zeichen `[LW#]`;
    dann `croniter(cron, datetime.now(UTC))` — raises `CroniterBadCronError`
    oder ähnlich. Re-raise als `ValueError` mit klarem Kontext.
  - **`if catchup is True`: sofort `NotImplementedError` im
    `__post_init__`** (Review-Finding P2-3: Fail-fast statt
    silent-skip-at-fire-time). Message:
    `"catchup=True is reserved for a future release; set catchup=False"`.
    Feld bleibt in der Signature als placeholder für Forward-Compat.
  - `if catchup is False and cron is None`: ignore (catchup ohne cron
    ist no-op).
  - `if cron is not None and delivery is FIRE_AND_FORGET and result_ttl=0`:
    explizit in Docstring dokumentiert, aber OK (FAF+cron ist valid,
    siehe origin Scope).
- **Signature-Guard für reserved `delay`/`eta` (Review-Finding P1-3):**
  in `@app.task`-Decorator / bei `register_task` via
  `inspect.signature(fn).parameters` prüfen ob `delay` oder `eta` als
  Parameter-Namen im Task vorkommen. Falls ja: `ValueError` mit
  Migration-Hint (siehe High-Level-Design-Snippet). Das verhindert
  dass alte Tasks, die diese Namen benutzten, silently ihre Payload
  an den Scheduler verlieren.

**Execution note:** Test-first.

**Patterns to follow:**
- `rate_limit` in `src/swarmq/task.py:__post_init__` — eager-parse-Validation.
- `lock` + `hard_timeout` kombinierte Validation in derselben Methode.

**Test scenarios:**
- Happy path — `TaskInfo(cron="*/5 * * * *")` funktioniert.
- Happy path — `TaskInfo(cron="0 3 * * *")` funktioniert.
- Happy path — `TaskInfo(catchup=False)` (default) OK.
- Error — `TaskInfo(cron="not-a-cron")` raised `ValueError`.
- Error — `TaskInfo(cron="0 3 L * *")` raised `ValueError` (Extended-Syntax).
- Error — `TaskInfo(cron="0 3 * * 1#2")` raised `ValueError`.
- **Error — `TaskInfo(catchup=True, cron="* * * * *")` raised
  `NotImplementedError`** (P2-3 Fail-fast, message enthält
  "reserved for a future release").
- **Error — `TaskInfo(catchup=True, cron=None)` raised
  `NotImplementedError`** (catchup=True ist immer rejected, unabhängig
  von cron).
- **Error — Task mit `delay` als Parameter registriert → `ValueError`
  mit Migration-Hint** (P1-3 Backward-Compat-Guard).
- **Error — Task mit `eta` als Parameter registriert → `ValueError`
  mit Migration-Hint** (P1-3).

**Verification:**
- `pytest tests/unit/test_task.py -x -k "cron or catchup"` grün.
- Alle bestehenden TaskInfo-Tests grün.
- `import croniter` in `src/swarmq/task.py` resolveable.

---

- [x] **Unit 4: LockManager.renew + lock_renew.lua**

**Goal:** `LockManager.renew(lock_name, owner_token, ttl_ms) -> bool` für
Heartbeat-Lock-Verlängerung. Neues Lua-Script `lock_renew.lua`:
`if GET(key) == owner then PEXPIRE(key, ttl_ms); return 1 else return 0`.

**Requirements:** CD18 (origin).

**Dependencies:** Unit 1 (kein direkter Code-Dep, aber logische
Reihenfolge).

**Files:**
- Create: `src/swarmq/lua/lock_renew.lua`
- Modify: `src/swarmq/locking.py` (neue Methode)
- Test: `tests/unit/test_locking.py`
- Test: `tests/integration/test_locking_manager.py`

**Approach:**
- Neue Method `async def renew(self, lock_name, owner_token, ttl_ms,
  max_concurrency: int = 1) -> bool`.
- **Mutex-only-Guard (Review-Finding P2-8):** `renew()` validiert
  `max_concurrency == 1` in Python (vor Script-Aufruf). Bei
  `max_concurrency > 1` → `NotImplementedError("lock_renew is
  mutex-only in this release")`. Der Scheduler nutzt nur
  `max_concurrency=1` (Leader-Election), daher reicht das für v1.
  Semaphore-Support kann in einem Folge-Plan ergänzt werden.
- Script via `register_script(load_script("lock_renew"))` instantiiert
  (analog `_acquire_script`, `_release_script`). Der Lua-Code
  behandelt ausschließlich den Mutex-Fall
  (`GET key → check owner → PEXPIRE key`).
- Returns `True` wenn Owner-Match + PEXPIRE erfolgreich, `False` wenn
  Lock verloren oder anderer Owner.
- BackendConnectionError-Wrapping wie bei `acquire`/`release`.

**Execution note:** Test-first.

**Patterns to follow:**
- `docs/recherche-deepening-lua-scripts.md` §2 — template für
  `extend_scheduler_lock.lua` ist praktisch fertig.
- `src/swarmq/locking.py:release` — API-shape und Error-Wrapping.
- `src/swarmq/lua/lock_release.lua` — vergleichbare GET-check-then-action.

**Test scenarios:**
- Happy path (unit, mocked) — `renew` ruft Script mit
  `KEYS=[lock_name]`, `ARGS=[owner_token, ttl_ms]` auf; returnt
  `True` wenn Script `1` liefert.
- Happy path (unit) — Script returnt `0` → `renew` returnt `False`.
- Error (unit) — ValkeyError → `BackendConnectionError`.
- Integration (echter Valkey):
  - Lock via `acquire` → `renew` verlängert TTL (EXPIRE prüft via
    `pttl_ms` nachher).
  - Fremder Owner-Token → renew returnt `False`, Lock bleibt unverändert.
  - Key nicht vorhanden → renew returnt `False`.
- Error (unit) — `renew(..., max_concurrency=2)` raised
  `NotImplementedError` mit eindeutiger Message (P2-8).

**Verification:**
- `pytest tests/unit/test_locking.py -x -k "renew"` grün.
- `pytest tests/integration/test_locking_manager.py -x -k "renew"`
  (mit Dev-Stack SWARMQ_TEST_*_PORT) grün.

---

- [x] **Unit 5: ValkeyBackend — Schedule-Operations**

**Goal:** Neue Methoden auf `ValkeyBackend` für Schedule-Store +
Consumer-Dedup:
- `zadd_schedule(schedule_name, fire_ts_ms)` / `zrem_schedule(name)`
- `zrangebyscore_schedules(max_score_ms, limit)` — nur read, kein claim
- `hset_schedule_def(name, def_json)` / `hget_schedule_def(name)` /
  `hdel_schedule_def(name)` / `hgetall_schedule_defs()`
- `set_schedfire_dedup(key, ttl_ms) -> bool` — SETNX-basiert, Consumer-Dedup
- `set_schedfire_handover_marker(owner_token, ttl_ms)` — setzt
  `sq:sched:handover:<owner_token>=1` mit TTL (Review-Finding P2-1)
- `get_and_del_schedfire_handover_marker() -> bool` — atomic
  `GET + DEL` (Lua-Script) auf `sq:sched:handover:*`; returnt True
  wenn ein Marker existierte (= clean handover), False wenn nicht
  (= FAILOVER)

**Requirements:** CD10, CD12 (origin).

**Dependencies:** Unit 1.

**Files:**
- Modify: `src/swarmq/backend/valkey.py`
- Test: `tests/unit/backend/test_valkey.py`
- Test: `tests/integration/backend/test_valkey_scheduler.py` (neu)

**Approach:**
- Alle Methoden wrappen Valkey-Errors in `BackendConnectionError`.
- Key-Helper privat:
  `_sched_zset_key()`, `_sched_defs_key()`, `_sched_fired_key(schedule, ts)`.
- `set_schedfire_dedup` nutzt `SET key "1" NX PX ttl_ms` und returnt
  `bool(result)` — wiederverwendet das Pattern aus
  `set_cancel_flag` (R3.6).

**Execution note:** Test-first.

**Patterns to follow:**
- `set_cancel_flag` / `is_cancelled` / `clear_cancel_flag` in
  `src/swarmq/backend/valkey.py` — Shape + Error-Wrapping.
- `get_results_batch` — Pipeline-Pattern für Multi-Key-Ops.

**Test scenarios:**
- Happy path (unit, mocked) — jede Method ruft Valkey-Client mit den
  richtigen Args.
- Error (unit) — ValkeyError → `BackendConnectionError` für alle
  mutating Ops.
- Integration:
  - `zadd_schedule` + `zrangebyscore_schedules` roundtrip.
  - `hset_schedule_def` + `hgetall_schedule_defs` roundtrip mit
    JSON-Decoding on read.
  - `set_schedfire_dedup` NX-Semantik: erster Call → True, zweiter
    (selber Key) → False.
  - `set_schedfire_dedup` TTL: nach Ablauf ist Key wieder frei.
  - `set_schedfire_dedup` mit `ttl_ms=scheduler_dedup_ttl*1000`:
    verifiziert dass die aufrufende Stelle (Worker/Leader) die
    Config-TTL durchreicht, nicht den alten 60-s-Hardcode.

**Verification:**
- Neue Unit-Tests grün.
- Integration-Tests auf Dev-Stack grün.

---

- [x] **Unit 6: Lua-Scripts — sched_poll, sched_commit**

**Goal:** Zwei neue Lua-Scripts für die atomare Poll-Sequence.

**Requirements:** CD10 (origin — Publish-before-ZREM).

**Dependencies:** Unit 5.

**Files:**
- Create: `src/swarmq/lua/sched_poll.lua`
- Create: `src/swarmq/lua/sched_commit.lua`
- Test: `tests/integration/test_sched_lua_scripts.py` (neu, Pattern von
  `test_lock_lua_scripts.py`)

**Approach:**
- `sched_poll.lua`:
  ```
  -- KEYS[1] = sq:sched:zset, KEYS[2] = sq:sched:defs
  -- ARGV[1] = now_ms, ARGV[2] = limit
  -- Returns: list of [member, score, def_json] tuples
  local due = redis.call('ZRANGEBYSCORE', KEYS[1],
                          '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
  local result = {}
  for i, member in ipairs(due) do
    local score = redis.call('ZSCORE', KEYS[1], member)
    local def = redis.call('HGET', KEYS[2], member)
    if def then
      table.insert(result, {member, score, def})
    end
    -- skip orphan zset entries (def deleted but zset not cleaned)
  end
  return result
  ```
  **Kein ZREM hier** — Publish erfolgt außerhalb.
- `sched_commit.lua` (**mit Owner-Check gg. Split-Brain-Commit,
  Review-Finding P1-2**):
  ```
  -- KEYS[1] = sq:sched:zset
  -- KEYS[2] = sq:sched:defs
  -- KEYS[3] = sq:lock:scheduler:leader  (leader-lock key)
  -- ARGV[1] = schedule_name (member)
  -- ARGV[2] = next_fire_ts_ms (or -1 for one-shot)
  -- ARGV[3] = updated_def_json (with last_fire_at, or "" to delete def)
  -- ARGV[4] = owner_token (expected lock holder)
  -- Returns: 1 on success, 0 if caller is not current lock holder.
  local owner = redis.call('GET', KEYS[3])
  if owner ~= ARGV[4] then
    return 0  -- stale leader: do not mutate schedule state
  end
  redis.call('ZREM', KEYS[1], ARGV[1])
  if tonumber(ARGV[2]) >= 0 then
    redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), ARGV[1])
  end
  if ARGV[3] == "" then
    redis.call('HDEL', KEYS[2], ARGV[1])
  else
    redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
  end
  return 1
  ```
  Rationale: ein Leader, dessen Lock während eines Poll-Batches abläuft
  (GC-Pause, Swap-Storm), darf nicht nachträglich `ZADD next_fire` des
  neuen Leaders überschreiben. Der GET+ARGV-Vergleich ist atomar
  innerhalb des Lua-Scripts.
- Beide via `backend._client.register_script` in `SchedulerStore`-Wrapper
  (Unit 7) genutzt.

**Execution note:** Test-first. Lua-Scripts werden **nur** via Integration
gegen echten Valkey getestet (kein Mock).

**Patterns to follow:**
- `src/swarmq/lua/sliding_window_counter.lua` — Lua-File-Format.
- `tests/integration/test_lock_lua_scripts.py` — Script-Testing-Pattern.

**Test scenarios (integration only):**
- Happy path — `sched_poll`: leere zset → leere Liste.
- Happy path — `sched_poll`: 3 Schedules mit unterschiedlichen Scores,
  `now_ms` zwischen Score 2 und 3 → Script returnt Schedules 1+2.
- Happy path — `sched_poll` mit orphan zset (def fehlt) → Skip, kein Crash.
- Happy path — `sched_commit` mit positivem next_fire + matching owner
  → ZREM+ZADD+HSET, returns 1.
- Happy path — `sched_commit` mit `next_fire=-1` (one-shot) + matching
  owner → ZREM only, returns 1.
- Happy path — `sched_commit` mit leerem def_json → HDEL statt HSET.
- Error — `sched_commit` mit **falschem owner_token** (Lock gehört
  anderem) → kein ZREM/ZADD/HSET/HDEL, returns 0. Zset + defs
  unverändert (via valkey-cli vor/nach verifiziert).
- Error — `sched_commit` mit **absentem Lock-Key** → kein Mutation,
  returns 0 (GET liefert nil, nil ≠ ARGV[4]).

**Verification:**
- Integration-Tests auf Dev-Stack grün.
- `valkey-cli SCRIPT EXISTS <sha>` zeigt beide Scripts geladen.

---

- [x] **Unit 7: Scheduler-Modul — SchedulerStore + Idempotency-Helper**

**Goal:** Neues Modul `src/swarmq/scheduler.py` mit:
- `ScheduleDefinition` (msgspec.Struct) — Schedule-Metadata.
- `ScheduleStore` — wrapper um ValkeyBackend-Scheduler-Methoden +
  registered Lua scripts (`sched_poll`, `sched_commit`). Lifecycle
  analog `LockManager` (instanziiert in `Worker.start` mit bereits-
  connecteter Backend-Instance).
- Idempotency-Key-Helper (canonical: **schedfire_key**):
  `make_schedfire_key(schedule_name: str, fire_ts_ms: int) -> str`
  liefert `f"{schedule_name}:{fire_ts_ms}"`.
- `parse_schedfire_key(value: str) -> tuple[str, int]` nutzt
  **`rsplit(":", 1)`** damit `schedule_name` selbst Doppelpunkte enthalten
  darf (z.B. `oneshot:<uuid>` → ergibt mit rsplit sauber
  `("oneshot:<uuid>", fire_ts_ms)`). Review-Finding P1-4.
- `render_schedfire_header(key)` setzt den String unverändert als
  Message-Property `x-swarmq-schedfire`. Consumer nutzt
  `parse_schedfire_key` — **derselbe Name in beide Richtungen, kein
  separates schedfire_value-Vokabular mehr** (Review-Finding P3-2).

**Requirements:** CD10, CD13, Modul-Struktur (Planning).

**Dependencies:** Units 5, 6.

**Files:**
- Create: `src/swarmq/scheduler.py`
- Test: `tests/unit/test_scheduler_store.py`
- Test: `tests/integration/test_scheduler_store.py`

**Approach:**
- `ScheduleDefinition` als `msgspec.Struct` (collapsed, Review-Finding
  P3-5 + adv-008):
  `schedule_name, task_name, args, kwargs, queue, cron: str | None,
   fire_ts_ms: int | None` (vereintes Feld für delay/eta/cron-next-fire
   — zset-score ist die authoritative Fire-Time; Feld dient als
   Audit/Display),
  `source: "static" | "cli"` (nur zwei Branches nötig; one-shot-Entries
   sind durch `cron is None` eindeutig diskriminierbar — P3-4),
  `disabled: bool, last_fire_at: int | None`.
  **catchup-Feld entfernt** — weil `TaskInfo(catchup=True)` bereits in
  Unit 3 beim Construction hart rejectet wird, kann kein Schedule mit
  catchup=True jemals in `sq:sched:defs` landen. Forward-Compat: wenn
  ein späteres Release catchup implementiert, wird das Feld dann
  hinzugefügt; msgspec-decode tolerates unknown fields per Default.
- `ScheduleStore.__init__(backend: ValkeyBackend)`:
  `self._poll_script = backend._client.register_script(load_script("sched_poll"))`
  (analog LockManager).
- Methods: `add_schedule(ScheduleDefinition)`, `remove_schedule(name)`,
  `disable_schedule(name)`, `list_schedules()`, `poll_due(now_ms, limit)`
  (nutzt `sched_poll.lua`), `commit_fire(name, next_fire_ts, updated_def,
  owner_token)` (nutzt `sched_commit.lua` mit Owner-Check, P1-2),
  **`update_static_schedule(ScheduleDefinition)`** (Fork 1):
  HSET + ZADD unbedingt, für static-cron-Änderungen.
- `add_schedule` verwendet insert-if-absent (checkt existierenden
  HGET-Wert, wenn vorhanden **unabhängig von source** nicht
  überschreiben — liefert das Semantic aus CD11).
- `update_static_schedule` wird **nur** vom Leader-Sync aufgerufen,
  nachdem verifiziert wurde dass der vorhandene Eintrag
  `source="static"` hat und der Cron-String sich geändert hat. Für
  CLI-Einträge gibt es keinen analogen Updater — der Operator muss
  `swarmq schedule remove` + `schedule add` nutzen.

**Execution note:** Test-first. Mehrere Abstraction-Layer, daher viele
Unit-Tests + selektive Integration.

**Patterns to follow:**
- `src/swarmq/locking.py:LockManager.__init__` — script-registration.
- `src/swarmq/limits.py:RateLimiter` — gleicher Shape.

**Test scenarios:**
- Happy path — `add_schedule(def)` mit neuem Name → schreibt HSET+ZADD.
- Happy path — `add_schedule(def)` mit existierendem CLI-Schedule →
  no-op (insert-if-absent).
- Happy path — `list_schedules()` returnt deserialisierte Objects aus
  HGETALL.
- Happy path — `poll_due(now_ms, 10)` returnt Liste von
  (name, score, def)-Tuples.
- Happy path — `commit_fire(name, next_fire, updated_def, owner_token)`
  triggert `sched_commit.lua` mit ARGV[4]=owner_token (P1-2).
- Happy path — `update_static_schedule(def)` macht unbedingtes HSET +
  ZADD (Fork 1). Verifiziert dass ein existierender Entry mit anderem
  Cron-String überschrieben wird.
- Edge — `make_schedfire_key` ist deterministic: gleicher Input →
  gleicher Output.
- Edge — `parse_schedfire_key("sched_a:1234567890")` →
  `("sched_a", 1234567890)`.
- Edge — `parse_schedfire_key("oneshot:abc-def-123:1234567890")` →
  `("oneshot:abc-def-123", 1234567890)` (rsplit behält Präfix-Kolonen).
- Error — `parse_schedfire_key("no-colon")` → `ValueError`.
- Error — `parse_schedfire_key("sched:not_an_int")` → `ValueError`
  (int-Cast fails; Worker fängt das und ack-skipped die Message
  defensiv).
- Integration: Full roundtrip add → poll → commit.

**Verification:**
- Unit + Integration grün.

---

- [x] **Unit 8: Consumer-Dedup im Worker._handle_message**

**Goal:** Worker prüft bei jedem Message mit `x-swarmq-schedfire`-Header
per `backend.set_schedfire_dedup`, ob es eine Duplicate-Fire ist. Bei
Duplicate: ack + skip + log. Läuft VOR `_pre_start_cancel_check`.

**Requirements:** CD10 (origin).

**Dependencies:** Units 5, 7.

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_schedfire_dedup.py` (neu)

**Approach:**
- Neue Helper-Method `_pre_start_schedfire_dedup(message, task_id,
  task_name, queue) -> bool`. Returnt True wenn Duplicate gefangen
  wurde (caller muss early return).
- Helper prüft `message.headers.get("x-swarmq-schedfire")`. Wenn kein
  Header → return False (normaler Flow). Wenn Header da:
  - `schedfire_key = header_value` (raw string, siehe Unit 7)
  - `is_new = await backend.set_schedfire_dedup(schedfire_key,
    ttl_ms=int(config.scheduler_dedup_ttl * 1000))` (P1-1).
  - Wenn `is_new`: return False → normaler Flow.
  - Wenn nicht new: log-info + `broker.ack(message)` +
    `structlog.contextvars.clear_contextvars()` + return True.
- Call-Site: in `_handle_message` direkt nach `structlog.bind_contextvars`,
  VOR dem `_pre_start_cancel_check`. Consistency mit Pipeline-Stage-
  Discipline.
- BackendConnectionError → nack(requeue=True), analog Pattern aus
  Unit 6 R3.6.

**Execution note:** Test-first.

**Patterns to follow:**
- `Worker._pre_start_cancel_check` (R3.6) — 1:1 dasselbe Muster.

**Test scenarios:**
- Happy path (unit) — Message ohne `x-swarmq-schedfire`-Header:
  `_pre_start_schedfire_dedup` returnt False, kein Backend-Call.
- Happy path (unit) — Header da, Backend returnt True (neu): normaler
  Flow, `run()` wird aufgerufen.
- Happy path (unit) — Header da, Backend returnt False (duplicate):
  `broker.ack` aufgerufen, `run()` NICHT aufgerufen, Contextvars
  gecleared.
- Error (unit) — Backend wirft `BackendConnectionError`:
  `nack(requeue=True)`, no ack, no run.
- Edge — Header mit ungültigem Format (kein `:`) → behandelt als
  potentielle Duplicate-Vorsichtskey (kein Crash).
- Integration — zwei Publishes mit gleichem
  `x-swarmq-schedfire`-Header → nur der erste executes, zweiter ack'd.

**Verification:**
- Unit + Integration grün.
- Keine Regression bei R3.6-Cancel-Tests (Dedup läuft vor Cancel-Check).

---

- [x] **Unit 9: Client.schedule — delay + eta kwargs**

**Goal:** Reserved keyword-only args `delay` und `eta` auf `client.schedule()`
und `SwarmQ.schedule()`. Ein-Shot-Schedule wird via `ScheduleStore`
+ Sorted-Set abgelegt und vom Scheduler-Loop später published.

**Requirements:** CD4 (origin).

**Dependencies:** Unit 7.

**Files:**
- Modify: `src/swarmq/client.py`
- Modify: `src/swarmq/app.py` (kwargs durchreichen)
- Test: `tests/unit/test_client.py`
- Test: `tests/integration/test_scheduler_e2e.py` (neu, wird in Unit 12 erweitert)

**Approach:**
- `Client.schedule` Signature um `delay: float | None = None,
   eta: datetime | None = None` erweitern — keyword-only, vor `**kwargs`.
- **Lazy-Init für `ScheduleStore` (Fork 2 — Review-Finding P2-6):**
  `Client.__init__` setzt `self._schedule_store: ScheduleStore | None =
  None`. Neue private Helper-Coroutine `_ensure_schedule_store()`:
  ```
  if self._schedule_store is None:
      if self._backend._client is None:
          raise RuntimeError(
              "Client.schedule(delay=/eta=) requires a connected "
              "backend. Use 'async with SwarmQ(...)' or call "
              "await backend.connect() before scheduling."
          )
      self._schedule_store = ScheduleStore(self._backend)
  return self._schedule_store
  ```
  Pattern folgt `Worker._rate_limiter` / `Worker._lock_manager`-
  Lazy-Construction. `ScheduleStore` bleibt private; kein User-visible-
  API-Change auf `Client`.
- Validation:
  - `delay` und `eta` gleichzeitig gesetzt → `ValueError`.
  - `delay < 0` → `ValueError`.
  - `eta` in Vergangenheit (> 1s) → `ValueError`
    mit suggestion „use delay=0 for immediate".
- Wenn `delay is None and eta is None`: bestehender Publish-Pfad.
- Sonst:
  - `store = await self._ensure_schedule_store()`
  - `fire_ts_ms = now_ms + int(delay*1000)` oder `int(eta.timestamp()*1000)`.
  - `oneshot_uuid = uuid.uuid7().hex`
  - Store-Def anlegen (als `ScheduleDefinition` mit
    `schedule_name=f"oneshot:{oneshot_uuid}"`, `cron=None`,
    `fire_ts_ms=fire_ts_ms`, `source="cli"` wenn via CLI gesetzt,
    sonst `source="static"` — one-shot wird durch `cron is None`
    diskriminiert, kein separater `"oneshot"`-Source nötig; P3-4).
  - `await store.add_schedule(def)` — schreibt HSET + ZADD.
  - Return `task_id` (vorab generiert wie bei normaler schedule, für
    get_result-Kompatibilität).

**Execution note:** Test-first.

**Patterns to follow:**
- `Client.cancel` (R3.6) — signature-extension-Muster.

**Test scenarios:**
- Happy path (unit) — `schedule(task, delay=60)` → zset hat Entry mit
  Score ≈ now+60s.
- Happy path (unit) — `schedule(task, eta=datetime(2026, 5, 1))` →
  zset hat Entry mit Score = eta-unix-ms.
- Happy path (unit) — `schedule(task, ...)` ohne delay/eta: normaler
  publish-Pfad (keine zset-Operation).
- Error — `delay=-1` → `ValueError`.
- Error — beide delay+eta gesetzt → `ValueError`.
- Error — `eta` 1h in Vergangenheit → `ValueError`.
- Edge — `delay=0` → sofortiger publish (oder sehr kurze delay),
  dokumentieren welche Variante.
- Unit — Lazy-Init (Fork 2): `Client.__init__` lässt
  `_schedule_store=None`; erster `schedule(..., delay=1)`-Aufruf
  konstruiert den Store; zweiter Aufruf recycelt die existierende
  Instanz (kein erneuter `register_script`).
- Error — `Client.schedule(delay=1)` mit unconnected Backend →
  `RuntimeError` mit klarer Connection-Hint-Message (Fork 2).

**Verification:**
- Unit-Tests grün.
- Integration: E2E-Test in Unit 12 nutzt dies.

---

- [x] **Unit 10: SchedulerLoop — Leader-Election + Poll-Loop + Heartbeat**

**Goal:** Haupt-Logik-Coroutine `SchedulerLoop` in `src/swarmq/scheduler.py`.
Embedded in jedem Worker; nur der Lock-Holder pollt die Sorted-Set und
feuert Schedules. Heartbeat erneuert Lock alle `heartbeat_interval`s.
Bei Lock-Verlust: Standby wird inaktiv und checkt periodisch.

**Requirements:** CD7, CD10, CD17, CD18 (origin).

**Dependencies:** Units 4, 6, 7.

**Files:**
- Modify: `src/swarmq/scheduler.py` (neues Modul, bereits in Unit 7 angelegt)
- Modify: `src/swarmq/worker.py` (Start/Stop-Wiring)
- Test: `tests/unit/test_scheduler_loop.py` (neu)
- Test: `tests/integration/test_scheduler_leader_election.py` (neu)

**Approach:**
- `SchedulerLoop(backend, broker, registry, config, signal_dispatcher)`:
  - `self._lock_name = "sched:lock:leader"`
  - `self._owner_token = uuid4().hex`
  - `self._state: "standby" | "leader" | "stopped" = "standby"`
  - `self._last_known_leader: bool = False` (für FAILOVER vs LEADER distinction)
- Methods:
  - `async def start(self)`: launch two coroutines:
    `self._election_task = asyncio.create_task(self._election_loop())` und
    `self._poll_task = asyncio.create_task(self._poll_loop())`.
  - `async def stop(self)`:
    - If leader:
      - emit `SCHEDULER_LEADER_HANDOVER`.
      - **`await backend.set_schedfire_handover_marker(owner_token,
        ttl_ms=5000)`** — schreibt `sq:sched:handover:<owner_token>=1`
        mit 5 s TTL. Incoming Leader GETs+DELs diesen Key und
        emittiert folglich `SCHEDULER_LEADER` statt `FAILOVER`.
      - `lock_manager.release(lock, token, 1)`.
    - Cancel both tasks, await them.
- `_election_loop`:
  - Every `heartbeat_interval`:
    - If standby: try `acquire` → if True:
      - state=leader.
      - **Handover-Marker-Check (Review-Finding P2-1):**
        `marker = await backend.get_and_del_schedfire_handover_marker()`
        liest `sq:sched:handover:*`-Keys (einziger Key existiert
        frühestens 5 s nach clean-release durch voriges Leader).
        - Marker vorhanden → emit `SCHEDULER_LEADER` (clean Handover
          vom vorigen Leader).
        - Marker absent → emit `SCHEDULER_LEADER_FAILOVER` (TTL-expiry
          des vorigen Lock, Leader crashed/paused).
      - `sync_taskinfo_crons()`.
    - If leader: `renew` → if True: emit `SCHEDULER_HEARTBEAT`; if False:
      state=standby (lock lost), log-warn. **Kein HANDOVER-Marker
      bei unfreiwilligem Loss** — bei TTL-Expiry wird nichts gesetzt,
      der nächste Acquirer emittiert FAILOVER.
- `_poll_loop`:
  - Every `poll_interval`:
    - If not leader: continue (no-op poll).
    - If leader: `store.poll_due(now_ms, limit=100)` → for each:
      - Compute header `x-swarmq-schedfire: <fire_key>`
      - `broker.publish(def.queue, encoded_message, headers=...)`
      - Compute `next_fire_ts` (croniter for cron-defs, -1 for one-shot)
      - `store.commit_fire(name, next_fire_ts, updated_def)`
      - Exception in any step: log + continue (at-least-once).
- `sync_taskinfo_crons()` (Fork 1 — static-cron-Update-Semantik):
  iteriert `Task._registered_tasks`, für jeden Task mit
  `task_info.cron`:
  - Konstruiert `ScheduleDefinition` mit `source="static"`, inklusive
    `fire_ts_ms = croniter(cron).get_next()`.
  - HGET des existierenden Eintrags für `schedule_name`.
  - **Update-Entscheidung je nach Quelle:**
    - Existierender Eintrag mit `source="cli"` → no-op
      (CD11-Semantik, insert-if-absent schützt CLI-Autorität).
    - Existierender Eintrag mit `source="static"` und **gleichem
      Cron-String** → no-op.
    - Existierender Eintrag mit `source="static"` aber **anderem
      Cron-String** → `store.update_static_schedule(def)`:
      HSET neuer def + ZADD neuer `fire_ts_ms`. Emitiert
      log-info mit `{old_cron, new_cron}`.
    - Kein existierender Eintrag → `store.add_schedule(def)` (insert).
  - Rationale: Code ist Single-Source-of-Truth für statisch
    deklarierte Crons; CLI-Einträge sind Runtime-State, bleiben
    insert-if-absent. Silent-divergence-Bug (adv-004) ist damit
    geschlossen für den static-Pfad.

**Execution note:** Test-first. Viele mock-based Unit-Tests, plus
Integration mit echten 2 Worker-Instanzen.

**Patterns to follow:**
- `Worker._cancel_monitor` (R3.6) — periodic poll with
  `asyncio.wait(timeout=...)`.
- `SchedulerLoop._election_loop` folgt dem LockManager-acquire/renew-
  Pattern — eine eigene Coroutine ist am klarsten.

**Test scenarios:**
- Unit — `_election_loop`: mocked LockManager, acquire True → state
  wird leader, emit SCHEDULER_LEADER.
- Unit — `_election_loop`: acquire False → state bleibt standby,
  kein Signal.
- Unit — `_election_loop`: leader → renew False → state standby + log-warn.
- Unit — `_election_loop`: sync_taskinfo_crons ruft `add_schedule` für
  neue Schedules auf.
- Unit — `sync_taskinfo_crons`: existierender `source="static"` mit
  **anderem** Cron → `update_static_schedule` wird gerufen (Fork 1).
- Unit — `sync_taskinfo_crons`: existierender `source="static"` mit
  **gleichem** Cron → kein Update, kein add.
- Unit — `sync_taskinfo_crons`: existierender `source="cli"` → no-op
  unabhängig vom Cron-Vergleich (CD11-Schutz).
- Unit — `_poll_loop`: standby → kein Backend-Call.
- Unit — `_poll_loop`: leader → mocked store.poll_due returnt due tasks,
  broker.publish wird mit richtigem Header aufgerufen,
  commit_fire wird aufgerufen.
- Unit — `_poll_loop`: broker.publish wirft → commit_fire NICHT gerufen,
  log.error.
- Edge — SCHEDULER_LEADER_HANDOVER: stop() mit state=leader →
  Signal vor Release emittiert, Handover-Marker geschrieben
  (`sq:sched:handover:<token>`, TTL 5 s).
- Edge — **SCHEDULER_LEADER (clean Handover von Vorgänger):**
  Worker A stop() → Handover-Marker da. Worker B acquire → liest+löscht
  Marker → emittiert `SCHEDULER_LEADER`, **nicht** FAILOVER.
- Edge — **SCHEDULER_LEADER_FAILOVER (TTL-expiry):** Worker A
  simuliert crash (kein release), Lock-TTL läuft ab. Worker B
  acquire → kein Marker da → emittiert `SCHEDULER_LEADER_FAILOVER`.
- Edge — **Handover-Marker expired vor Acquire** (B braucht >5s nach
  A's release): Marker TTL läuft ab, B acquire → kein Marker → wird
  als FAILOVER emittiert. Dokumentieren als akzeptierte False-Positive-
  Rate bei langsamen Standby-Pollern (Tuning via `poll_interval`).
- Unit — `_poll_loop`: leader → **`sched_commit.lua` wird mit
  owner_token aufgerufen** (P1-2); mocked script-call verifiziert
  ARGV[4]=self._owner_token.
- Unit — `_poll_loop`: leader verliert Lock **während** einer
  Batch-Iteration (simuliert via renew=False zwischen publish[i] und
  commit_fire[i+1]) → remaining batch-items werden **NICHT** commit'd,
  state-Transition auf standby, log-warn.
- Integration (echter Backend+Broker, 2 Worker-Instanzen):
  - Beide starten, nur einer wird leader.
  - Leader-Kill → Standby übernimmt innerhalb Lock-TTL.
  - Graceful Shutdown des Leaders → Standby übernimmt innerhalb ~1s
    (release-based Handover).

**Verification:**
- Unit + Integration grün.
- Keine Regression.

---

- [x] **Unit 11: Worker-Integration — Scheduler embedded starten**

**Goal:** Worker startet `SchedulerLoop` beim `start()`, stoppt ihn im
`_execute_shutdown()`. Integration mit dem bestehenden Worker-Lifecycle.

**Requirements:** CD7 (origin — embedded in jedem Worker).

**Dependencies:** Unit 10.

**Files:**
- Modify: `src/swarmq/worker.py`
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_scheduler_integration.py` (neu)

**Approach:**
- Im `Worker.__init__`: `self._scheduler: SchedulerLoop | None = None`.
- `Worker.start()`: nach `_rate_limiter` und `_lock_manager` werden
  instanziiert, zusätzlich `self._scheduler = SchedulerLoop(...)`
  und `await self._scheduler.start()`.
- `_execute_shutdown()`: vor `self._broker.close()` und
  `self._backend.close()` → `await self._scheduler.stop()`.
  Reihenfolge: Consumer cancelled → in-flight tasks drain → locks released
  → **scheduler stop** → broker close → backend close.
- Die `sync_taskinfo_crons()`-Logik iteriert über
  `Task._registered_tasks` — muss nach Registry-Construction laufen
  (bereits der Fall: Registry wird in `__init__` gebaut).

**Execution note:** Test-first. Integration-focused.

**Patterns to follow:**
- `Worker._rate_limiter` / `Worker._lock_manager` — Instanziierung +
  Lifecycle in `start()`.

**Test scenarios:**
- Unit — `Worker.start` instanziiert SchedulerLoop (mock) und ruft start().
- Unit — `_execute_shutdown` ruft scheduler.stop() in richtiger
  Reihenfolge.
- Integration — Worker-Start mit echtem Backend: Scheduler läuft,
  Heartbeat-Signal wird emittiert.
- Integration — Worker-Shutdown: HANDOVER-Signal emittiert, Lock
  released (via valkey-cli verifiable).

**Verification:**
- Unit + Integration grün.

---

- [ ] **Unit 12: CLI — swarmq schedule list/add/remove/status**

**Goal:** Vier neue CLI-Subcommands unter `swarmq schedule`. Erlaubt
Runtime-Management der Schedules ohne Worker-Restart.

**Requirements:** R3.10 Scheduler-CLI-Subset.

**Note (Fork 3 — Review-Finding P2-7/F-01):** `prune` wurde aus dieser
Phase entfernt und in einen Folge-Plan verschoben. Rationale:
Pre-User-Library hat keinen akuten Storage-Growth-Druck (Defs sind
O(#TaskInfo), nicht O(#Fires)). Orphan-Handling ist bis dahin manuell
via `swarmq schedule remove <name> --permanent` erledigbar. Das
`--module`-Registry-Loading, das `prune` braucht, ist separate
Infrastruktur die in einem eigenständigen Plan sauberer designbar ist.

**Dependencies:** Units 5, 7.

**Files:**
- Modify: `src/swarmq/cli.py` (neue Subparsers)
- Test: `tests/unit/test_cli.py`
- Test: `tests/integration/test_cli_schedule.py` (neu)

**Approach:**
- Neue argparse-Subparser-Group `schedule` mit 4 Subcommands:
  - `list [--format table|json]` — HGETALL `sq:sched:defs`, formatiere.
  - `add <name> (--cron <expr> | --at <iso-datetime>)
     --task <task_name> [--args '<json-array>'] [--kwargs '<json-object>']
     [--queue <q>]` — erstellt ScheduleDefinition mit `source="cli"`,
     ruft `store.add_schedule` (insert-if-absent).
  - `remove <name> [--permanent]` — ohne `--permanent`: setzt
    `disabled=True` im def (insert-if-absent blockiert Re-sync);
    mit `--permanent`: HDEL + ZREM.
  - `status` — zeigt aktuellen Leader-Worker (aus `sq:lock:scheduler:leader`
    owner-token via GET), nächste N fällige Schedules aus zset (mit
    next_fire), last_fire_at je Schedule.
- Alle Subcommands verbinden sich zu Valkey (nicht zu RabbitMQ —
  Scheduler-Store ist Valkey-only).
- Helper: `_build_backend_from_env()` extrahiert aus bestehendem
  `_run_worker_async` — refaktorisiert damit Backend-construction
  von Worker-Lifecycle entkoppelt ist.

**Execution note:** Test-first.

**Patterns to follow:**
- Bestehender `swarmq worker`-Subcommand in `src/swarmq/cli.py` —
  argparse-Struktur, env-var-loading, async-entry.

**Test scenarios:**
- Unit (parser-only) — `schedule list --format json` parsed korrekt.
- Unit — `schedule add my_sched --cron "*/5 * * * *" --task mytask --args '[1]'` parsed alle Fields.
- Unit — `schedule remove my_sched --permanent` parsed flag.
- Integration (real Valkey) —
  - `schedule add` dann `schedule list` zeigt den eingetragenen
    Schedule.
  - `schedule remove` ohne `--permanent` → def hat disabled=True.
  - `schedule remove --permanent` → def + zset-entry weg.
  - `schedule status` zeigt Leader-Name und next-N.

**Verification:**
- Unit + Integration grün.
- `swarmq schedule --help` zeigt alle 4 Subcommands (kein `prune`).

---

- [ ] **Unit 13: E2E — Full Scheduler-Lifecycle**

**Goal:** End-to-End Tests mit echtem Broker+Backend+Worker,
decken alle primären Scenarios ab.

**Requirements:** Success Criteria aus Origin-Brief.

**Dependencies:** Units 1-12.

**Files:**
- Test: `tests/e2e/test_scheduler.py` (neu)

**Approach:**
- Fixtures: eigener App-Context für Worker + Client (wie
  R3.6-E2E-Pattern).
- Jeder Test startet 1-2 Worker, scheduled Tasks, prüft Outcomes.
- Timing: `asyncio.Event` oder polling-with-timeout statt
  hardcoded sleeps (AGENTS.md Test-Discipline + R3.6-Review-Finding).

**Patterns to follow:**
- `tests/e2e/test_cancellation.py` — Worker-Start + graceful shutdown.
- `tests/e2e/test_locking.py` — multi-worker coordination.

**Test scenarios (alle integration-level, kein unit-mock):**
- Happy path — `client.schedule(task, delay=0.5)` → Task fires nach ~0.5s,
  result via `get_result` OK.
- Happy path — `client.schedule(task, eta=future)` → analog.
- Happy path — Cron-Schedule feuert deterministisch innerhalb des
  Test-Fensters (Review-Finding P1-6): Test registriert einen Task
  mit `TaskInfo(cron="* * * * *")` (every minute, gültiger 5-Feld-
  Cron), und **injektiert direkt** einen `ScheduleDefinition`-Eintrag
  via `store.add_schedule(def)` mit Score = `now_ms + 500` ms. Der
  embedded Scheduler poll't im Default-Intervall von 1 s und feuert
  den Task sichtbar innerhalb von ≤1.5 s. Kein Warten auf die
  natürliche Minute-Grenze, keine `asyncio.sleep`-Manipulation nötig.
- Happy path — `swarmq schedule add` via CLI → Schedule fires.
- Happy path — 2 Worker, nur einer wird Leader → Signal-verified.
- Happy path — Leader-Kill → Standby übernimmt innerhalb TTL,
  FAILOVER-Signal emittiert.
- Happy path — Graceful Shutdown Leader → HANDOVER-Signal, Standby
  übernimmt innerhalb 1s.
- Happy path — Consumer-Dedup: Force-duplicate publish mit gleichem
  `x-swarmq-schedfire`-Header → nur eine Execution.
- Happy path — **Retry-Dedup-Window**: duplicate publish mit
  `x-swarmq-schedfire` **>60 s** aber **<retry_max_delay** nach
  erstem Fire → zweite Zustellung wird dedup'd (verifiziert dass
  `scheduler_dedup_ttl` breit genug ist; P1-1).
- **Split-Brain-Commit**: simulierter Leader-Lock-Loss mid-batch →
  stale Leader ruft `sched_commit` → Script returnt 0, Zset/defs
  unverändert (P1-2).
- **Clean Handover vs. Failover**: Worker A stop() → Marker → Worker
  B emittiert `SCHEDULER_LEADER`. A simulate-crash (kein release) →
  Lock-TTL läuft → B emittiert `SCHEDULER_LEADER_FAILOVER` (P2-1).
- Edge — `TaskInfo(cron="invalid")` → ImportError at module load.
- Edge — **`TaskInfo(catchup=True)` → NotImplementedError at import**
  (P2-3 Fail-fast — kein Runtime-Warten nötig).
- Edge — **Task mit `delay` als Parameter registriert → ValueError at
  `@app.task`-Decorator** (P1-3 Backward-Compat-Guard).
- Edge — **Static-Cron-Modify-Propagation (Fork 1):** Worker startet
  mit `TaskInfo(cron="*/5 * * * *")`, defs-Hash hat entsprechenden
  Eintrag. Simulate Redeploy: `_registered_tasks` enthält jetzt
  `cron="*/10 * * * *"` für denselben Task. Worker-Restart →
  Leader-Sync → defs-Hash zeigt neuen Cron + neuen fire_ts_ms.
  (Prune-Orphan-Scenario entfernt — `prune` deferred, Fork 3.)

**Verification:**
- E2E-Tests grün auf Dev-Stack.
- Alle bestehenden 574+ Tests bleiben grün (kein Regression).

---

## System-Wide Impact

- **Interaktionsgraph**:
  - `SchedulerLoop` ↔ `LockManager` (neue `renew`-Method).
  - `SchedulerLoop` ↔ `ScheduleStore` ↔ `ValkeyBackend`.
  - `SchedulerLoop` ↔ `RabbitMQBroker.publish` (mit neuen Headers).
  - `Worker._handle_message` ↔ neuer Dedup-Check (vor Cancel-Check).
  - `Client.schedule` / `SwarmQ.schedule` ↔ `ScheduleStore.add_schedule`.
  - CLI ↔ `ScheduleStore` (Valkey-only, kein Broker).
- **Error-Propagation**: Alle neuen Valkey-Calls wrappen in
  `BackendConnectionError`. Scheduler-Loop exception-handling isoliert
  Fehler: ein fehlschlagender Schedule blockt nicht andere.
- **State-Lifecycle-Risiken**:
  - Orphan one-shot-entries in `sq:sched:oneshot:*`-Hash bei Crash
    zwischen Publish und commit_fire → TTL safety-net oder Janitor.
  - Dedup-Keys sammeln sich mit 60s TTL — unkritisch.
  - Leader-Lock bei SIGKILL → TTL-basiertes Recovery (max 30s).
- **API-Surface-Parity**: `Client.schedule` / `SwarmQ.schedule` /
  CLI `schedule add` alle nutzen dieselbe `ScheduleStore`-Abstraction.
- **Integration-Coverage**: Unit 13 (E2E) deckt full-stack.
- **Unchanged invariants**:
  - `{queue}.DQ` (Retry-Delay-Queue) bleibt unberührt — zwei separate
    Mechanismen (origin CD2).
  - Bestehende `Client.schedule` ohne delay/eta funktioniert unverändert.
  - Retry/Cancel/Lock/Rate-Limit-Pipeline greift für gefeuerte
    Scheduled Tasks wie für alle anderen.
  - Broker-Queue-Topologie unverändert.

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Split-Brain-Leader (Lock-TTL-Race) → Double-Publish | gering | niedrig | Consumer-SETNX-Dedup mit `scheduler_dedup_ttl >= retry_max_delay` (P1-1). Plus `sched_commit.lua`-Owner-Check (P1-2): stale Leader kann nicht `ZADD` über neuen state. |
| Leader-GC-pause > TTL → Lock-Loss → Standby übernimmt → alter Leader published nochmal | gering | niedrig | TTL 3× Heartbeat-Interval (30s vs 10s). Publish-Duplikate werden durch Consumer-Dedup abgefangen. Split-Brain-Commit wird durch Owner-Check in `sched_commit.lua` verhindert (P1-2). |
| Crash zwischen Publish und commit_fire → Duplicate-Fire bei nächstem Poll | mittel | niedrig | Consumer-Dedup fängt im gesamten Retry-Horizont (P1-1, dedup_ttl ≥ retry_max_delay). Worst-case: 1 duplicate re-try publish. |
| Long catch-up-Loop bei altem `last_fire_at` → Leader-Acquire stuck | eliminated | — | CD6: Single `croniter.get_next(now=now_utc())`-Call, keine Iteration. |
| `TaskInfo.cron`-**Modify** hinterlässt stale Entry in Valkey | gering | niedrig | Fork 1: Leader-Sync propagiert Cron-Änderungen bei `source="static"`-Einträgen. |
| `TaskInfo.cron`-**Delete/Rename** hinterlässt Orphan in Valkey | mittel | niedrig | Manual `schedule remove --permanent` bis `prune` in Folge-Plan kommt (Fork 3). Orphan wird vom Leader-Sync nicht mehr angefasst (read-only). |
| CLI-removed Schedule wird bei Leader-Sync re-erstellt | eliminated | — | CD11: insert-if-absent + disabled-Marker. |
| Clock-Skew zwischen Workern → Cron-Drift | gering | niedrig | Nur Leader evaluiert Cron; Consumer-Dedup verhindert Double-Execution. |
| Valkey nicht HA → Sorted-Set-Loss bei Restart | mittel | hoch | Dokumentiert als „v1 setzt Valkey-HA voraus". AOF/Sentinel als production-recommendation. |
| Heartbeat kollidiert mit anderen Valkey-Loads | gering | niedrig | `scheduler_heartbeat_interval` konfigurierbar, Default 10 s. |
| Scheduler-Loop-CPU-Overhead auf allen Workers | gering | niedrig | Standby-Workers pollen nicht (early-return if state!=leader). Nur Leader hat Poll-Loop. |

## Documentation / Operational Notes

Post-Merge Housekeeping (nicht Plan-Done-Kriterium):
- README „What works today" um „Scheduler (Delay/ETA + Cron, embedded
  with automatic failover, best-effort)" erweitern.
- CHANGELOG-Note: neue Config-Env-Vars, neue CLI-Subcommands, neue
  Signals, neue TaskInfo-Felder.
- README-Config-Table um `SWARMQ_SCHEDULER_POLL_INTERVAL`,
  `_HEARTBEAT_INTERVAL`, `_LOCK_TTL` erweitern.
- Doku-Abschnitt zu Scheduler in `docs/` — wenn User-Documentation-
  Workstream (Branch `feat/user-documentation`) so weit ist, direkt
  dort aufgenommen.
- Neue Solution-Docs wenn beim Implementieren subtile Issues auftauchen.

## Alternative Approaches Considered

- **Atomic Lua-Claim (ZREM-before-Publish) ohne Leader-Election**: Die
  ursprünglich geplante Variante aus dem Brainstorm. Hat bei Review ein
  At-Most-Once-Problem gezeigt (Crash zwischen Claim und Publish =
  Task-Verlust). Ersetzt durch Publish-before-ZREM + Consumer-Dedup.
- **RabbitMQ Delayed-Message-Exchange-Plugin für Delay/ETA**: Wurde
  2026 archiviert, kein Ersatz im Open-Source-RabbitMQ. Nicht nutzbar.
- **Gestufte DQ-Queues (1s, 5s, 30s, …)**: Ungenaue Verzögerung,
  komplexe Broker-Topologie. Verworfen in origin CD2.
- **Sidecar-Scheduler-Prozess (Celery-Beat-Stil)**: Widerspricht SwarmQ-
  Leitprinzip „Worker starten → alles läuft automatisch".
- **`catchup=True` in v1 implementieren**: Thundering-Herd-Risiko +
  wenig Wert für Pre-v1-Library. Feld reserviert (CD16), Logic später.

## Success Metrics

User-visible (aus Origin-Brief):
- `client.schedule(task, delay=60)` feuert nach ~60s (tolerance 1.5s).
- `TaskInfo(cron="*/5 * * * *")` fires alle 5 Min ohne externen Prozess.
- Zwei Worker starten → nur einer ist Leader (Signal-observable).
- Leader-Kill → Standby übernimmt innerhalb 30s.
- Graceful Shutdown → Standby übernimmt innerhalb 1s.
- `swarmq schedule list/add/remove/status` funktional (4 Subcommands;
  `prune` deferred in Folge-Plan, Fork 3).
- Consumer-Dedup verhindert Duplicate-Execution.
- catchup-skip: Outage → kein Thundering-Herd, nur next-run.

Engineering-internal:
- 0 neue P1-Findings in `compound-engineering:review`.
- Alle bestehenden 574+ Tests grün. Geschätzt +80-120 neue Tests.
- Scheduler-Loop-Idle-CPU <1% auf 4-Core (informell messen).

## Review Changelog (Deepening 2026-04-17)

Nach Document-Review (5 Reviewer: coherence, feasibility, product-lens,
scope-guardian, adversarial) wurden folgende P1-/P2-Findings chirurgisch
in den Plan eingearbeitet:

- **P1-1** (adversarial adv-001): Dedup-TTL 60 s deckt 3600 s
  `retry_max_delay` NICHT → neues Config-Feld `scheduler_dedup_ttl`
  mit Default `max(60, retry_max_delay + 300)` s und
  `ConfigurationError` bei `dedup_ttl < retry_max_delay`.
- **P1-2** (adversarial adv-003): `sched_commit.lua` erhält Owner-Check
  (KEYS[3]=lock_key, ARGV[4]=owner_token); returnt 0 bei Owner-Mismatch.
- **P1-3** (feasibility F1): `delay`/`eta`-kwargs sind reserved, aber
  `@app.task`-Decorator blockiert Tasks mit diesen Parameter-Namen mit
  klarer Migration-Hint — verhindert silent-payload-loss.
- **P1-4** (feasibility F2): `parse_schedfire_key` nutzt `rsplit(":", 1)`
  damit `oneshot:<uuid>`-Member mit Header nicht kollidieren.
- **P1-5** (coherence F001 + scope-guardian F-05): `disabled`-Marker ist
  **nur** als inline `ScheduleDefinition.disabled: bool` in
  `sq:sched:defs`; separate `sq:sched:disabled:*`-Keys entfernt.
- **P1-6** (coherence F003): Unit 13 E2E ersetzt invaliden
  `*/1 * * * *`-Cron mit direkter `store.add_schedule()`-Injection und
  valider 5-Feld-Cron.
- **P1-7** (feasibility F3): Lock-Key-Pfad korrigiert auf
  `sq:lock:scheduler:leader` (LockManager-Prefix ist `sq:lock:`,
  nicht `sq:sched:lock:`).
- **P2-1** (feasibility F5 + adversarial adv-006): FAILOVER-vs-HANDOVER-
  Diskriminierung via transient `sq:sched:handover:<owner>`-Marker
  (TTL 5 s). Outgoing Leader setzt Marker vor release; incoming Leader
  GETs+DELs. Present → `SCHEDULER_LEADER`; absent → `SCHEDULER_LEADER_FAILOVER`.
- **P2-3** (scope-guardian F-03 + adversarial adv-005): `TaskInfo(catchup=True)`
  raised jetzt sofort `NotImplementedError` im `__post_init__`, nicht
  erst beim Fire. Feld ist aus `ScheduleDefinition` entfernt.
- **P2-8** (feasibility F4): `LockManager.renew()` ist explizit
  mutex-only, Semaphore-Support via Guard auf `max_concurrency=1`.

Hygiene-P3s (datetime.now(UTC), terminology `schedfire_key`,
`ScheduleDefinition` collapse auf `fire_ts_ms`, `source` auf
`"static"|"cli"` reduziert) ebenfalls eingearbeitet.

**Architektonische Forks (nach User-Entscheidung eingearbeitet):**
- **Fork 1 (P2-2 resolved — Option B):** `sync_taskinfo_crons`
  propagiert bei `source="static"` modifizierte Crons via neuer
  `ScheduleStore.update_static_schedule()`-Methode. CLI-Einträge
  bleiben insert-if-absent (CD11 geschützt).
- **Fork 2 (P2-6 resolved — Option A):** `ScheduleStore` wird im
  `Client` lazy beim ersten delay/eta-Schedule instanziiert; konsistent
  mit `LockManager`/`RateLimiter`-Construction im Worker.
- **Fork 3 (P2-7 resolved — Option B):** `swarmq schedule prune` wurde
  aus dieser Phase entfernt. Unit 12 hat 4 Subcommands
  (list/add/remove/status). Für modifizierte Crons greift Fork 1;
  für Orphans nach task-delete gibt es `remove --permanent` bis zum
  Folge-Plan.

## Sources & References

- **Origin document:**
  [docs/brainstorms/2026-04-16-phase-3c-scheduler-requirements.md](../brainstorms/2026-04-16-phase-3c-scheduler-requirements.md)
- **Vorgänger-Plan (Pattern-Source):**
  [docs/plans/2026-04-16-009-feat-phase3b3-task-cancellation-plan.md](2026-04-16-009-feat-phase3b3-task-cancellation-plan.md)
- **Locking (R3.4):**
  [docs/plans/2026-04-16-008-feat-phase3b2-task-locking-plan.md](2026-04-16-008-feat-phase3b2-task-locking-plan.md)
- **Recherche Embedded Scheduler:**
  [docs/recherche-embedded-scheduler.md](../recherche-embedded-scheduler.md)
- **Recherche Lua §2 (Lock-Extend), §8 (Sorted-Set Poll):**
  [docs/recherche-deepening-lua-scripts.md](../recherche-deepening-lua-scripts.md)
- **Recherche Operational Features §1:**
  [docs/recherche-operational-features.md](../recherche-operational-features.md)
- **Solution Pipeline-Stage-Ack-Ordering:**
  [docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md](../solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md)
- **Roadmap:**
  [docs/brainstorms/implementation-order-requirements.md](../brainstorms/implementation-order-requirements.md) R3.8/R3.9/R3.10
