---
title: "Phase 3b.2: Task Locking (R3.4)"
type: feature-brief
status: ready-for-planning
date: 2026-04-16
audience: solo-developer (next table-stakes feature before scheduler)
roadmap_refs: [R3.4]
supersedes: docs/brainstorms/2026-04-16-phase-3b2-locking-unique-requirements.md (bundled with R3.5, split per product-lens review)
---

# Phase 3b.2: Task Locking (R3.4)

## Problem Frame

SwarmQ hat aktuell **kein** Mittel, um die Concurrency einer Task gegenüber sich
selbst zu kontrollieren. Wenn dieselbe Task gleichzeitig auf zwei Workern
läuft und auf eine geteilte Resource zugreift (DB-Row, externes API mit
Quotas, exklusive Datei), gibt es kein "max 1 gleichzeitig" — das muss der
User selbst lösen.

Locking ist **table-stakes**: Celery hat es, Dramatiq hat es, taskiq hat
Middleware-Locks. Ohne ist SwarmQ für Production-Workloads (Payment-
Processing, externe API-Aufrufe mit Rate-Limits, exklusive Datei-Zugriffe)
ungeeignet.

## Goal

R3.4 (Task Locking) als alleiniges Phase-3b.2 Bündel ausliefern. **R3.5
(Unique Tasks) wurde bewusst abgespalten** (siehe `supersedes`-Brief): Unique
ist nicht auf demselben Table-Stakes-Niveau und sollte erst **nach** R3.8/R3.9
(Scheduler) kommen — Scheduler ist der grössere Adoption-Hebel laut
Wettbewerbs-Analyse.

## Scope

**In-Scope:**
- **TaskInfo-Erweiterung**
  - `lock="resource_name"` — Mutex (max 1 gleichzeitig)
  - `lock="resource_name", max_concurrency=N` — Semaphore (max N)
  - Lock-Name kann statisch oder via Format-String (`"user-{user_id}"`) aus Args
    gebildet werden — bekannter Celery-Pattern
- **Konflikt-Verhalten** (v1): **nur `requeue`** mit exponentiellem Backoff
  (analog Rate-Limit-Verzögerung). `skip` und `raise` als Opt-in **wenn** ein
  konkreter User-Bedarf entsteht — initial weggelassen (premature config-itis
  laut Scope-Guardian-Review).
- **Lock-Mechanik**
  - Valkey `SET NX PX` mit zufälligem Owner-Token (UUID4 pro Acquisition)
  - Owner-Token in Worker-Memory gehalten, nicht persistiert
  - Atomare Release-Prüfung via Lua: `if GET(key) == owner then DEL(key)`
- **Lock-TTL-Strategie**
  - TTL = `max(30, 0.25 * hard_timeout)` Sekunden
  - Wenn `hard_timeout=None`: Lock wird **abgelehnt** beim Task-Start
    (`ConfigurationError` beim ersten Schedule, nicht silent crash) —
    Lock erfordert ein Time-Bound für sichere TTL-Berechnung
- **Pipeline-Stage-Integration** (laut Worker-Pipeline-Solution-Doc)
  - Lock-Check **nach** Rate-Limit-Stage, **vor** Execution
  - Lock-Check **vor** FIRE_AND_FORGET-Ack
  - Bei Konflikt: Requeue mit `x-swarmq-lock-conflict-count` Header
    (analog `x-swarmq-rate-limit-delay-count`)
  - Backoff-Cap: nach N Conflicts → DLQ-Routing (Header-Wert prüfen)
- **Worker-Shutdown-Integration**
  - Bei Graceful-Shutdown: aktive Locks der laufenden Tasks freigeben
    BEVOR Backend-Connection geschlossen wird (sonst TTL-Wartezeit)
  - Bei Hard-Crash (SIGKILL): Lock läuft via TTL ab — dokumentierte
    Limitation, **kein Heartbeat-Renewal** in v1 (siehe Out-of-Scope)
- **Lua-Scripts** (zwei neue Files in `src/swarmq/lua/`)
  - `lock_acquire.lua` — SET NX PX mit Owner, Semaphore-Counter über
    SortedSet bei `max_concurrency > 1`
  - `lock_release.lua` — atomare Owner-Check + DEL/ZREM
- **Tests** (Unit + Integration mit echtem Valkey + E2E)
  - Mutex: 10 parallele Schedules → max 1 läuft gleichzeitig
  - Semaphore: 10 parallele Schedules mit `max_concurrency=3` → max 3
  - Lock-Release bei Task-Exception (kein Lock-Leak)
  - Lock-TTL läuft korrekt ab bei Worker-Crash-Simulation
  - Owner-Token-Mismatch: Worker A kann Worker B's Lock nicht freigeben
  - Backoff-Cap: Task landet nach N Conflicts in DLQ
  - `hard_timeout=None` + `lock=...` → ConfigurationError beim ersten Schedule
- **Signal**: `TASK_LOCKED` (bereits in `signals.py` definiert) wird hier
  zum ersten Mal emittiert beim Konflikt

**Out-of-Scope:**
- **R3.5 Unique Tasks** — abgespalten, eigener Brief nach Scheduler
- **R3.6 Task Cancellation** — Phase 3b.3
- **Heartbeat-Renewal** für lange-laufende Tasks. Lock-TTL koppelt an
  Hard-Timeout. **Akzeptierte Limitation**: bei Hard-Crash wartet
  der nächste Worker bis zu `max(30, 0.25*hard_timeout)` Sekunden auf
  Lock-TTL-Ablauf. Wenn Heartbeat später konkret gebraucht wird, eigenes
  Bündel (Phase 3c oder später).
- **Konflikt-Verhalten `skip` und `raise`** — nicht in v1
- **Lock-Wait-Queues** (Lock-auf-Lock warten) — nicht im R3.4-Scope
- **Lock-Inspection-API** (`get_lock_holders()`, `force_release_lock()`,
  CLI `swarmq locks list`) — Phase 3c
- **Cross-Cluster-Lock** — nur ein Valkey-Backend
- **Public-API-Stabilität** — wir sind in Phase 3, kann sich noch ändern

## Behavior Constraints

1. **Pattern-Reuse von R3.3**: Lua-Loading via `register_script()` mit
   NOSCRIPT-Recovery (`limits.py:91-122`).
2. **Pipeline-Stage-Discipline** (siehe `docs/solutions/logic-errors/
   worker-pipeline-stage-ack-ordering-2026-04-16.md`):
   - Stage läuft VOR FIRE_AND_FORGET-Ack
   - Conflict-Counter-Header für Backoff-Cap
   - Lock-Release garantiert auch bei Exception (try/finally im Worker)
   - Signal vor Requeue emittieren
3. **Atomarität**: Lock-Acquire = ein Lua-Roundtrip. Release = ein
   Lua-Roundtrip mit Owner-Check.
4. **Owner-Sicherheit**: Lock-Release prüft Owner-Token via Lua — ein
   Worker kann nicht den Lock eines anderen freigeben. Token = `uuid.uuid4().hex`
   pro Acquisition (random, nicht deterministisch).
5. **`hard_timeout=None` + Lock**: harter Konfigurations-Fehler beim Schedule,
   nicht beim Execute (frühes Fail).

## Success Criteria

User-visible:
- `TaskInfo(lock="x")` Tasks laufen nie >1 gleichzeitig (Integration-Test
  mit 10 parallelen Schedules verifiziert)
- `TaskInfo(lock="x", max_concurrency=3)` Tasks laufen nie >3 gleichzeitig
- Lock wird nach Task-Ende garantiert freigegeben (success ODER exception)
- Worker-Crash blockiert Lock max `max(30, 0.25*hard_timeout)` Sekunden
- `TaskInfo(lock="x")` ohne `hard_timeout` → klare Fehlermeldung beim
  ersten Schedule (nicht silent crash später)
- Format-String-Locks (`lock="user-{user_id}"`) funktionieren mit Args
- Comparison-Doc (wenn reaktiviert): SwarmQ Lock-Cell von "missing" auf
  "supported"

Engineering-internal (nice-to-have):
- 0 neue P1/P2 Findings in `compound-engineering:review`
- Alle bestehenden 478 Tests grün (geschätzt 510-525 nach Phase 3b.2)

## Risks

| Risiko | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Lock-TTL läuft ab BEVOR Hard-Timeout greift unter GC-Pause / NTP-Skew | mittel | hoch | TTL = max(30s, 0.25 * hard_timeout) statt fixed 5s; Test mit kurzem hard_timeout (1s) und simuliertem Stall (asyncio.sleep im Task) |
| Worker-Crash → Lock blockiert nächsten Worker für TTL-Dauer | mittel | mittel | Dokumentierte Limitation; Graceful-Shutdown-Integration releast aktive Locks; SIGKILL-Recovery via TTL — als Trade-off gegen Heartbeat-Komplexität |
| Backoff-Cap zu streng → Tasks landen zu früh in DLQ | gering | mittel | Cap konfigurierbar (TaskInfo oder global), default = 10 Conflicts (analog Rate-Limit) |
| Format-String-Lock mit fehlendem Arg → KeyError zur Laufzeit | gering | mittel | Validation beim Task-Definition: Format-String-Placeholder müssen in Task-Signature vorkommen |
| Pipeline-Stage-Ordering-Bug (Lock vor Rate-Limit oder umgekehrt) | gering | hoch | Solution-Doc-Pattern strikt befolgen; Integration-Test prüft beide Stages zusammen |
| Lua-Script-Fehler in Produktion ohne Fallback | gering | hoch | NOSCRIPT-Recovery von R3.3 wiederverwenden; Lua-Scripts haben Unit-Tests |

## Closed Decisions (vom Brainstorm)

Diese Punkte sind **entschieden** (nicht Open Questions für Planning):

1. **Konflikt-Verhalten v1**: nur `requeue` (skip/raise später wenn jemand braucht)
2. **Owner-Token**: random UUID4 pro Acquisition (nicht worker_id-basiert)
3. **Lock-TTL**: `max(30s, 0.25 * hard_timeout)` (nicht fixed Buffer)
4. **`hard_timeout=None` + Lock**: ConfigurationError beim Schedule (nicht silent)
5. **Heartbeat-Renewal**: NICHT in v1 (akzeptierte Limitation)
6. **Pipeline-Position**: Rate-Limit zuerst, dann Lock, dann Execute
7. **Backoff-Cap-Header**: `x-swarmq-lock-conflict-count` (Solution-Doc-Pattern)

## Open Questions (für Planning)

Nur echte Planning-Fragen, keine Wiederholung von Closed Decisions:

1. **Semaphore-Implementierung**: SortedSet mit Score=Timestamp+UUID, ZRANGE
   für Count, oder SET-of-Tokens mit SCARD? Beide gehen — Performance-Trade-off.
2. **Lua-Script-Granularität**: Ein grosses `lock.lua` mit Acquire+Release oder
   zwei kleine? R3.3 (Rate Limit) hat ein einzelnes File — Pattern-Konsistenz vs.
   Lesbarkeit.
3. **Format-String-Validation**: Beim `TaskInfo()` Konstruktor prüfen, beim
   ersten Schedule, oder zur Laufzeit fail-fast?
4. **Default Backoff-Cap-Wert**: 10 Conflicts (wie Rate-Limit), oder anders?
5. **Reaktivierung Comparison-Suite**: Soll Plan 008 die Comparison-Suite
   (von Branch `feat/benchmark-comparison-suite`) reaktivieren, um vor/nach
   Lock-Performance zu messen?

## Dependencies

- **R3.3 (Rate Limiting)** — DONE. Pattern-Source für Lua-Loading +
  Pipeline-Stage-Reihenfolge + Conflict-Counter-Header.
- **R2.4 (Soft/Hard Timeouts)** — DONE. Lock-TTL koppelt an Hard-Timeout.
- **R2.5 (Graceful Shutdown)** — DONE. Worker-Shutdown muss Locks vor
  Backend-Close freigeben.
- **Worker-Pipeline-Stage-Solution-Doc** — vorhanden.

## Non-Goals

- Distributed lock-fairness (FIFO über Worker hinweg)
- Lock-Inspection-API
- Heartbeat-basierte Lock-Renewal
- Cross-Cluster-Lock (nur ein Valkey)
- Lock-Migration zwischen Backends
- R3.5 (Unique Tasks)
- R3.6 (Task Cancellation)
