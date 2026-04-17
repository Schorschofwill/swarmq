---
title: "fix: Phase 3c Scheduler — Review-Follow-up (P1/P2 residual)"
type: fix
status: active
date: 2026-04-17
origin: docs/plans/2026-04-17-002-feat-phase3c-scheduler-plan.md
---

# fix: Phase 3c Scheduler — Review-Follow-up (P1/P2 residual)

## Overview

Folge-Plan für architektonisch größere Findings aus dem 13-Reviewer-
Pass nach Merge von `feat/phase3c-scheduler`. Die Quick-Wins (11 items)
wurden direkt auf dem Feature-Branch eingearbeitet (siehe Commit
e657fa4). Hier geht es um Änderungen die entweder Architektur-Scope
haben, Tests in größerem Umfang verändern, oder neue Produktfeatures
einführen.

## Problem Frame

Die aktuelle Phase-3c-Implementation ist korrekt für den Happy-Path
und die häufigsten Failure-Modi. Nach Review bleiben 10 Findings
die ein kohärentes nächstes Commit-Batch benötigen:

- **H-2** (HIGH / reliability R-002): Dedup-SETNX-Ordering — SETNX
  wird pre-run gesetzt. Wenn der SETNX-Gewinner vor dem `ack`
  crasht, wird die Message redelivered, aber jeder neue Worker
  sieht den Key noch live (bis 3900s TTL) → `ack+skip` ohne
  Execution → Task silently dropped für den gesamten Dedup-Horizont.
  **At-most-once-Hole in einem at-least-once-System.** Architektonisch.

- **H-5** (HIGH / project-standards PS-01): 23 `asyncio.sleep()`
  in neuen Tests — AGENTS.md §Async-Code verbietet das
  unconditional. Die meisten sind entweder Startup-Barrieren
  (Worker-Ready), Post-Event-Damper (nach `first_done.wait()`),
  oder TTL-Expiry-Waits in Integration-Tests.

- **H-6** (HIGH / project-standards PS-02): `@pytest.mark.timeout(5)`
  fehlt auf allen neuen async-Tests. Global ist 30s konfiguriert,
  aber die Per-Test-Marker sind der AGENTS.md-Standard.

- **M-2** (MEDIUM / agent-native + cli-readiness): `swarmq schedule
  add` und `remove` haben kein `--format json`. Add-no-op-exit=0
  ist ununterscheidbar vom echten Insert. Remove ist silent bei
  missing Name.

- **M-3** (MEDIUM / adversarial adv-008): `DecodeError`-Handler in
  `sync_taskinfo_crons` überschreibt CLI-Einträge unkonditionell →
  CD11-Verletzung bei Schema-Inkompatibilität.

- **M-4** (MEDIUM / reliability R-003): Per-Schedule-
  BackendConnectionError-Handling in `sync_taskinfo_crons` fehlt.
  Partial-Sync-Abort ohne per-Schedule-Log.

- **M-5** (MEDIUM / adversarial adv-007): `list_schedules()` crasht
  auf eine malformed entry — ganzes `swarmq schedule list` hardet.

- **M-7** (MEDIUM / api-contract AC-04): `LockManager.renew` akzeptiert
  `max_concurrency>1` in Signature, raised Runtime-NotImplementedError.
  Asymmetrisch zu `acquire()`.

- **M-11** (MEDIUM / adversarial adv-005): Schedfire-Dedup-Key nutzt
  `definition.fire_ts_ms` statt Zset-Score. Divergenz möglich bei
  `update_static_schedule` mit zeitversetztem ZADD.

- **M-14** (MEDIUM / reliability R-004): `add_schedule` /
  `disable_schedule` sind hget+hset — zwei Round-Trips, TOCTOU.

## Scope Boundaries

**In-Scope:**
- H-2 (dedup SETNX semantics) — Pre-run in-flight marker + post-run
  completion marker.
- H-5 (asyncio.sleep cleanup) — pragmatisch: nur die ersetzbaren
  Sleeps durch Events ersetzen, Startup-Barrieren nach einer
  kurzen Evaluation dokumentiert beibehalten oder durch einen
  `worker.wait_ready()`-Helper ersetzen.
- H-6 (pytest.mark.timeout) — via `pyproject.toml` global auf
  test-file-basis, nicht per-test-dekorator.
- M-2 (JSON-Output für add/remove) — klein + agent-wert.
- M-3, M-4, M-5 (Hardening der Sync/List-Pfade).
- M-7 (Typ-Guard für `renew`).
- M-11 (fire_ts_ms aus Zset-Score).
- M-14 (HSETNX + Lua für disable).

**Out-of-Scope (deferred to separate plans):**
- CLI-Module-Split (`cli.py` → `cli_schedule.py`) — ist cli.py
  wirklich zu groß? Entscheidung beim DLQ-CLI-Plan.
- Status-CLI-Horizon konfigurierbar (CLR-004) — kleines Feature,
  eigener Plan.
- SchedulerLoop-Config für Handover-Marker-TTL als eigenes Feld —
  Fix in Quick-Wins deckt den Default-Fall ab, weiteres Tuning
  nur bei konkretem User-Bedarf.
- Security-Hardening (SEC-01/SEC-02: Länge-Limits auf Header und
  Schedule-Namen) — Low-Severity, eigener Security-Pass sinnvoll.

## Implementation Units

---

- [ ] **Unit 1: Dedup-SETNX Pre-run + Post-run Marker (H-2)**

**Goal:** Splitting der Dedup-Logik in einen kurzen Pre-run-Lock
und einen langen Post-run-Marker. Ziel: at-least-once-Garantie bei
SETNX-winner-crash-before-ack wieder herstellen.

**Approach:**
- Neue Backend-Method `set_schedfire_inflight(key, ttl_ms) -> bool`
  — SET NX mit kurzer TTL (z.B. `hard_timeout_ms + safety_margin`
  oder fallback 5 Minuten). Zweck: duplicate während aktiver
  Execution blockieren.
- Bestehende `set_schedfire_dedup(...)` → bleibt als
  "post-run-completion-marker".
- Worker-Pipeline in `_pre_start_schedfire_dedup`: Check POST-
  marker zuerst (wenn schon erfolgreich gerunned → skip).
  Sonst: pre-run-SETNX mit kurzer TTL. Wenn erfolgreich → run.
  Wenn nicht → genau ein anderer Worker hält ihn in-flight, skip.
- Post-run (im normal-completion-finally): setze den Long-TTL-
  Completion-Marker. Bei Error ohne Retry (permanent failure):
  auch Completion-Marker setzen, damit kein anderer Worker versucht.
- In-flight-key wird NICHT expliztit gelöscht — TTL läuft ab,
  oder Completion-Marker wird als Indikator überschrieben.
- Integration-Test: simulate SETNX-winner-crash → redelivery nach
  TTL-Expiry → neue Worker-Instanz kann den Task erfolgreich
  ausführen.

**Files:**
- Modify: `src/swarmq/backend/valkey.py` (neue `set_schedfire_inflight`)
- Modify: `src/swarmq/worker.py` (`_pre_start_schedfire_dedup` +
  neuer Post-run-Marker-Hook im `finally`)
- Modify: `src/swarmq/config.py` (neues `scheduler_inflight_ttl`-Feld?)
- Test: `tests/unit/test_worker.py`,
  `tests/integration/test_worker_schedfire_dedup.py`

**Verification:**
- Unit + Integration grün.
- Neues Integration-Test deckt den Crash-Before-Ack-Case explizit.

---

- [ ] **Unit 2: asyncio.sleep Cleanup in Tests (H-5)**

**Goal:** Ersetze alle `asyncio.sleep`-Calls in Tests die als
Post-Event-Damper oder TTL-Expiry-Waits dienen durch Event- oder
Signal-basierte Barrieren. Startup-Barrieren durch einen expliziten
`worker.wait_ready()` oder Signal-Wait.

**Approach:**
- Inventar: 23 `asyncio.sleep`-Calls in 7 Test-Dateien.
- Klassifizieren: Startup (Worker-Ready), Post-Event (nach
  first_done.wait), TTL-Wait (Valkey-Expiry).
- Für Startup: `SCHEDULER_LEADER`-Signal als Barrier nutzen.
- Für Post-Event: SignalDispatcher-Hook auf `TASK_COMPLETED` oder
  `schedfire_duplicate_acked`-Log → asyncio.Event.
- Für TTL-Wait: kann `asyncio.sleep` bleiben — das ist legitimer
  TTL-Wait auf externe Zeit, kein synchronization-primitive.

**Files:**
- Modify: `tests/e2e/test_scheduler.py`
- Modify: `tests/integration/test_scheduler_leader_election.py`
- Modify: `tests/integration/test_worker_schedfire_dedup.py`
- Evtl. `src/swarmq/worker.py`: `wait_ready()`-Helper der auf
  internen `_ready_event` wartet.

**Verification:**
- Keine `asyncio.sleep`-Calls mehr für Synchronisation.
- TTL-basierte Waits bleiben mit dokumentiertem Kommentar.

---

- [x] **Unit 3: pytest-Timeout-Marker per-File (H-6)**

**Goal:** `@pytest.mark.timeout(5)` auf allen neuen async Tests, oder
file-scope marker via pytest-Convention.

**Approach:**
- Check ob `pytestmark = [pytest.mark.timeout(5)]` auf Modul-Ebene
  einmal reicht (vs. 50+ per-method decorator).
- Falls ja: ein `pytestmark`-Statement pro Test-Datei.

**Files:**
- Modify: alle neuen Test-Dateien in `tests/unit/`, `tests/integration/`,
  `tests/e2e/` aus dem Phase-3c-Branch.

---

- [ ] **Unit 4: --format json für `swarmq schedule add` und `remove` (M-2)**

**Goal:** Agent-readable JSON-Output plus boolean "did-it-happen"-
Indicator für die mutierenden CLI-Commands.

**Approach:**
- `add --format json`: `{"inserted": bool, "name": str}`.
  Wenn `already_exists`: `inserted=false`, exit=0.
  Optional: separate exit-code (z.B. 3) für agents die auf exit-
  code allein matchen.
- `remove --format json`: `{"removed": bool, "permanent": bool,
  "name": str}`. `removed=false` bei missing name, exit=0.
- `ScheduleStore.remove_schedule()` und `disable_schedule()`
  returnen einen `bool` (basierend auf HDEL-Return-Count bzw.
  vorhandenem hget-Eintrag).

---

- [x] **Unit 5: DecodeError + Partial-Sync Hardening (M-3, M-4, M-5)**

**Goal:** sync_taskinfo_crons robust gegen einzelne malformed Entries
und Valkey-Flaps. `list_schedules` toleriert schlechte Entries.

**Approach:**
- `sync_taskinfo_crons`: jeden `add_schedule`/`update_static_schedule`-
  Call in try/except-`BackendConnectionError` mit per-Schedule-Log.
- DecodeError-Pfad checkt existing.source erst wenn decode
  erfolgreich — nur static-Einträge dürfen überschrieben werden;
  malformed CLI-Einträge bleiben quarantined mit Log.
- `list_schedules`: try/except pro entry, malformed Einträge
  werden als `{"schedule_name": "<decode-error>"}` zurückgeliefert
  mit Warning-Log.

---

- [x] **Unit 6: Literal[1]-Annotation für LockManager.renew (M-7)**

**Goal:** Statische Typ-Guard für `max_concurrency=1`-only-Parameter.

**Approach:**
- Import `typing.Literal`, ändere Signature zu
  `max_concurrency: Literal[1] = 1`.
- mypy flagged semaphore-Callsites zur Compile-Time statt Runtime.

---

- [x] **Unit 7: Schedfire-Key nutzt Zset-Score (M-11)**

**Goal:** Dedup-Key-Generation nutzt den authoritativen Zset-Score
statt `definition.fire_ts_ms` (die divergieren können).

**Approach:**
- `SchedulerLoop._poll_tick` pass't `_score_ms` (aktuell ignoriert)
  an `_fire_one`.
- `_fire_one` baut `schedfire_key` aus dem Score, nicht aus
  `definition.fire_ts_ms`.

---

- [x] **Unit 8: HSETNX / Lua für add_schedule + disable_schedule (M-14)**

**Goal:** Atomare insert-if-absent in `add_schedule` via HSETNX,
atomare read-then-set in `disable_schedule` via kurzem Lua-Script.

**Approach:**
- `hsetnx_schedule_def(name, def_json) -> bool` als neue
  Backend-Method.
- `ScheduleStore.add_schedule` nutzt HSETNX + (wenn erfolgreich)
  ZADD. HSETNX=0 → bestehender Eintrag, skip.
- `disable_schedule` als 3-Zeilen-Lua-Script: HGET, decode JSON,
  HSET + ZREM atomar.

---

- [x] **Unit 9: Dedup-Counter-Header persistiert über Redelivery (Fork A)**

**Goal:** `x-swarmq-schedfire-dedup-count`-Inkrement muss in der
requeueten Message sichtbar sein. Aktuell mutiert der Worker
`message.headers` in-memory und ruft `nack(requeue=True)`; AMQP-
Semantik ignoriert die Mutation und redelivered die Original-Header.
Der Counter bleibt effektiv bei 1 egal wie oft Valkey flappt.

**Requirements:** Re-Review-Finding P1-Fork-A (correctness C-FIX-01 +
kieran KP-02 + agent-native Warning 1, 3 Reviewer Agreement).

**Dependencies:** Eigenständig. Kann parallel zu Unit 1 laufen.

**Files:**
- Modify: `src/swarmq/worker.py` (`_pre_start_schedfire_dedup`
  BackendConnectionError-Pfad)
- Test: `tests/unit/test_worker.py`
- Test: `tests/integration/test_worker_schedfire_dedup.py` (neuer Test
  für Counter-Akkumulation über echte Redelivery)

**Approach — gewählte Variante A2 (empfohlen):** Header-Mutation
entfernen, Log-Zeile als einzige Observability-Quelle behalten.

- Der Counter-Inkrement-Code-Pfad wird entfernt:
  ```
  prior = int(message.headers.get("x-swarmq-schedfire-dedup-count", 0))
  message.headers["x-swarmq-schedfire-dedup-count"] = prior + 1
  ```
  entfällt. Der strukturierte `logger.error("schedfire_dedup_backend_error",
  schedfire_key=header_value)` bleibt — Operators sehen Bounces im
  Log-Stream (JSON-strukturiert, greppable).
- Der existierende Unit-Test `test_dedup_backend_error_nacks_for_requeue`
  (der jetzt den Counter-Assert enthält, Commit `1e5432e`) wird
  geändert: Counter-Assertion entfernt; Assert stattdessen dass das
  Log-Event emittiert wurde (via `structlog.testing.capture_logs`).
- Der Akkumulations-Test (`test_dedup_backend_error_accumulates_counter`)
  wird entfernt — der Counter akkumuliert nicht und soll es auch nicht.

**Rationale für A2 statt A1 (publish_to_delay_queue-Pattern):**
- Die transiente Valkey-Unavailability ist ein seltener Fall; der
  Broker-Side-Retry via `nack(requeue=True)` ist angemessen.
- `publish_to_delay_queue` erfordert einen Delay-Wert — der bei
  Valkey-Transient nicht offensichtlich wählbar ist (1s? 5s?
  exponential?). Die rate-limit/lock-Pfade nutzen kalibrierte
  Delays aus der Lua-Script-Antwort bzw. expliziter Config. Für
  Valkey-Flap gibt's diese Kalibrierung nicht.
- Die Symmetrie-Verletzung mit rate-limit/lock wird explizit in
  einem Code-Kommentar adressiert: „Valkey flaps are a backend
  fault, not a per-message policy decision; let the broker handle
  redelivery timing."

**Alternative Varianten (nicht gewählt):**
- **A1 publish_to_delay_queue:** konsistent mit rate-limit/lock,
  aber zusätzliche Komplexität ohne klaren Mehrwert für den
  transienten Valkey-Fall.
- **A3 Status quo + Doku:** behält den broken Code-Pfad, der bei
  Code-Audits verwirrt. Abgelehnt.

**Execution note:** Test-first — Log-Emission-Test zuerst, dann
Counter-Code entfernen, dann Log-Wording verifizieren.

**Test scenarios:**
- Happy path — BackendConnectionError bei dedup-SETNX → `logger.error`
  mit `event="schedfire_dedup_backend_error"` wird emittiert, `schedfire_key`
  korrekt propagiert, `nack(requeue=True)` läuft.
- Edge — Message mit pre-existing (vererbtem) Counter-Header wird
  nicht berührt. Header passt durch ohne Mutation — Operator-Tools
  die externe Counter-Logik hinzufügen sind davon nicht betroffen.

**Verification:**
- Unit-Suite grün. Log-Inhalt als einzige Observability-Source
  verifiziert.
- Integration-Test mit zwei sequenziellen BCE-Rounds zeigt: zwei
  Log-Events emittiert, Counter-Header nie im Broker-Delivery sichtbar.

---

- [x] **Unit 10: `SCHEDULER_LOCK_KEY` als Live-Derivation (Fork B)**

**Goal:** Der Lock-Key-Wert wird zur Import-Zeit aus
`locking.lock_key(SCHEDULER_LOCK_NAME)` berechnet statt hand-getippt.
Die Invariante „SCHEDULER_LOCK_KEY == locking._LOCK_KEY_PREFIX +
SCHEDULER_LOCK_NAME" ist damit by construction korrekt — kein
silent-break wenn der Prefix ändert.

**Requirements:** Re-Review-Finding P2-Fork-B (maintainability M-FIX-02
+ reliability NEW-001 + kieran RR-02, 3 Reviewer Agreement).

**Dependencies:** Unit 1-8 berühren den Key-Wert nicht; Unit 10 ist
unabhängig.

**Files:**
- Modify: `src/swarmq/scheduler.py` (Constant-Derivation)
- Test: `tests/unit/test_scheduler_store.py` (neuer
  `test_scheduler_lock_key_matches_lock_manager_prefix`)

**Approach — gewählte Variante B1 (empfohlen):** Statische
Live-Derivation mit Import aus `locking`.

- In `scheduler.py`:
  ```
  from swarmq.locking import lock_key
  ...
  SCHEDULER_LOCK_NAME = "scheduler:leader"
  SCHEDULER_LOCK_KEY = lock_key(SCHEDULER_LOCK_NAME)
  ```
- Docstring präzisieren: „Live-derived; kann sich nicht von
  `locking._LOCK_KEY_PREFIX + SCHEDULER_LOCK_NAME` unterscheiden."
- Der Test `test_scheduler_lock_key_matches_lock_manager_prefix`
  importiert `_LOCK_KEY_PREFIX` und `SCHEDULER_LOCK_NAME` und
  vergleicht — dient als guardrail falls `lock_key()` jemals auf
  non-trivial mapping erweitert wird.

**Rationale für B1 statt B2 (Runtime-Assertion):**
- B1 ist fail-fast zur Import-Zeit; B2 braucht `__debug__`-guarded
  assert der zur Runtime läuft und bei `-O` verschwindet.
- B1 hat Null Overhead; B2 führt jeden Worker-Start eine Compare-
  Operation aus.

**Alternative Varianten (nicht gewählt):**
- **B2 Runtime-Assertion:** mehr Boilerplate, `-O` macht sie wirkungslos.
- **B3 Status quo + Doku:** lässt die silent-break-Gefahr stehen.

**Execution note:** Test-first. Der Test failed aktuell weil der Wert
hand-getippt ist — greift nur wenn `lock_key("scheduler:leader")`
zufällig das gleiche liefert (was er tut), daher eigentlich nicht
rot. Wir machen ihn rot durch Temporär-Änderung in `locking.py`
(prefix auf z.B. `"sq:lck:"` — dann passt der Test und fängt die
Divergenz ab; danach Prefix zurücksetzen und Derivation einführen).
Simpler: wir implementieren einfach die Derivation und der Test
dient als Regression-Guard.

**Test scenarios:**
- `SCHEDULER_LOCK_KEY == lock_key(SCHEDULER_LOCK_NAME)` — by
  construction, aber als Regression-Guard geprüft.
- Der Wert entspricht dem erwarteten Format
  `"sq:lock:scheduler:leader"` (Snapshot-Test, falls jemand den
  Prefix versehentlich ändert).
- `SCHEDULER_LOCK_KEY` wird von CLI + scheduler.py-commit_fire
  als identischer String genutzt (via `id(constant)` oder
  Import-Equality).

**Verification:**
- Unit-Tests grün.
- Linter-Check: kein hand-getippter `"sq:lock:scheduler:leader"`
  Literal mehr im Code (bereits durch Safe-Auto-Fixes erreicht).

---

## Risks & Dependencies

- Unit 1 (Dedup-SETNX) ändert die at-least-once-Semantik spürbar.
  **E2E-Test mit simulated crash ist Pflicht** bevor Unit 1 als
  completed gilt.
- Unit 2 (asyncio.sleep) könnte flaky-Tests revealen die aktuell
  durch Sleeps maskiert waren. Plan: wenn Sleep durch Event ersetzt
  wird und der Test dann failt, ist das ein echter Bug — fixen.
- Unit 9 (Dedup-Counter) entfernt einen beobachtbaren Header den
  externe Tools theoretisch lesen könnten. Risk gering weil der
  Header erst seit Commit `e657fa4` existiert und nie korrekt
  persistierte — kein realistischer Consumer hat sich darauf
  verlassen können. Changelog-Notiz reicht.
- Unit 10 (Lock-Key-Derivation) ist defensiv; der aktuelle hand-
  getippte Wert ist gleichwertig zum derivierten. Risk: Null.

## Success Metrics

- Alle aufgelisteten P1/P2 Findings aus dem 13-Reviewer-Pass
  abgehakt.
- `grep -rn "asyncio.sleep" tests/` zählt nur TTL-Waits (mit
  Kommentar) — nicht Synchronization.
- `swarmq schedule add --format json` und `swarmq schedule remove
  --format json` liefern agent-parseable Output.
- Full test-suite (unit + integration + e2e) grün, neue Tests
  für Crash-Before-Ack-Szenario grün.

## Sources & References

- Origin: [docs/plans/2026-04-17-002-feat-phase3c-scheduler-plan.md](2026-04-17-002-feat-phase3c-scheduler-plan.md)
- Review-Run: `.context/compound-engineering/ce-review/20260417-002801-13057dc8/`
- AGENTS.md §Async-Code-Discipline
- Solution: docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md
