---
title: "Phase 3c: Scheduler (R3.8 + R3.9 + R3.10-schedule-CLI)"
type: feature-brief
status: ready-for-planning
date: 2026-04-16
audience: solo-developer (next feature after Task Cancellation)
roadmap_refs: [R3.8, R3.9, R3.10-schedule]
---

# Phase 3c: Scheduler (R3.8 + R3.9 + R3.10-schedule-CLI)

## Problem Frame

SwarmQ kann heute Tasks nur **sofort** einreihen. Zwei Klassen von Workloads
sind damit ausgeschlossen:

1. **Verzögerte Ausführung** — "führe diese Task in 60s aus" oder "führe sie
   am 15. April 09:00 aus". Heute muss der Caller selbst warten und dann
   `schedule()` aufrufen — unpraktisch für User-Kontext-Signaturen (Email in
   1h, Reminder, Retry-after-Window).
2. **Periodische Tasks (Cron)** — "alle 5 Minuten", "jeden Tag um 03:00".
   Heute muss ein externer Scheduler (Celery-Beat-Stil) dafür sorgen. Das
   bricht das Leitprinzip "Worker starten → alles läuft automatisch".

Delay/ETA und Cron sind Konkurrenz-Parity-Features: Celery
(`apply_async(countdown=)`, Celery-Beat), Dramatiq
(`send_with_options(delay=...)`, dramatiq-cron), arq (`arq.cron`),
taskiq (Labels) haben sie alle.

> **Honest Framing (Post-Review)**: Die Priorität „Scheduler vor R3.5
> (Unique Tasks)" stammt aus maintainer-Intuition, nicht aus
> User-Feedback (SwarmQ hat noch keine externen Nutzer). Die
> Comparison-Doc, die ursprünglich als Referenz für den
> „größter-Adoption-Hebel"-Claim diente, ist selbst `status: deferred`.
> R3.5 (Unique Tasks) wurde inzwischen zweimal mit derselben Logik
> verschoben. Nach Shipping von Phase 3c sammeln wir externes
> User-Feedback, bevor wir R3.5 oder R3.7 priorisieren. Wenn Phase 3c
> in der Praxis weniger Adoption-Hebel hat als angenommen, korrigieren
> wir die Reihenfolge — statt defensiv an der Annahme festzuhalten.

## Goal

R3.8 (Delay/ETA) + R3.9 (Embedded Scheduler mit Cron) + R3.10-Teil
(`swarmq schedule` CLI) als gemeinsame Phase 3c. Gemeinsam, weil:

- Der Scheduler-Loop ist **gemeinsame Infrastructure** für Delay/ETA und Cron
- Leader-Election muss **einmal** gelöst werden, nicht zweimal
- Persistenz-Layer (Valkey) ist **geteilt**

## Scope

**In-Scope:**

- **R3.8 Delay/ETA**
  - `await client.schedule(task, *args, delay=60, **kwargs)` — relative
    Verzögerung in Sekunden (analog Celery `countdown`, Dramatiq `delay`)
  - `await client.schedule(task, *args, eta=datetime(...), **kwargs)` —
    absoluter Zeitpunkt
  - `delay` und `eta` sind **reserved keyword-only args** (neben dem
    bestehenden `queue=`). Bei Kollision mit Task-Params: Task-Autor
    benennt den Arg um.
  - Valkey Sorted Set `sq:sched:zset` als Datastore, Score =
    Unix-Timestamp-ms des geplanten Runs, Member = serialisierter
    Task-Payload (JSON, analog RabbitMQ-Body-Format).
  - Leader pollt periodisch (Default 1s), published fällige Tasks auf die
    Main-Queue des Tasks via bestehender Broker-publish-Pipeline.
- **R3.9 Embedded Scheduler + Cron**
  - **Leader-Election** via Valkey-Lock (reuse `LockManager`-Pattern aus
    R3.4). Ein Worker pro Deployment hält den Scheduler-Lock und fungiert
    als Leader. Andere Workers laufen als Standby, prüfen periodisch ob
    der Leader-Lock frei ist.
  - **Heartbeat**: Leader erneuert den Lock periodisch (Default alle 10s,
    TTL 30s). Bei Crash übernimmt ein Standby innerhalb von ~10-30s.
  - **Cron-Parser**: `croniter` (de-facto Standard, ~50kB).
  - **Cron-Declaration — Beides**:
    1. **Statisch via `TaskInfo(cron="*/5 * * * *")`**: beim Modul-Import
       wird der Schedule in Valkey persistiert. Beim Worker-Start synct der
       Leader aus allen registrierten TaskInfo-crons → Valkey.
    2. **Dynamisch via CLI**: `swarmq schedule add <name> --cron '...'
       --task <task_name>` schreibt in Valkey, unabhängig vom Code-Stand.
  - **Signals**: `Signal.SCHEDULER_LEADER` / `Signal.SCHEDULER_STANDBY`
    existieren bereits in `signals.py` — werden hier erstmals dispatched.
    Für Leader-Lock-Renewal wird ein **neues** Signal
    `Signal.SCHEDULER_HEARTBEAT` eingeführt (`WORKER_HEARTBEAT` bleibt
    für Worker-weite Heartbeats reserviert — die Scheduler-Lock-Renewal
    fährt nur auf dem Leader und sollte nicht unter Worker-Heartbeat
    gemixt werden).
- **R3.10 Scheduler-CLI-Subset**
  - `swarmq schedule list` — alle aktiven Schedules (statisch + dynamisch)
  - `swarmq schedule add <name> --cron '<expr>' --task <task_name>
    [--args '[...]'] [--kwargs '{...}']` — dynamischer Cron-Schedule
  - `swarmq schedule add <name> --at <iso-datetime> --task <task_name>
    [--args ...]` — one-shot-ETA, wie `schedule(eta=...)` von CLI aus
  - `swarmq schedule remove <name>` — entfernt statisch **oder**
    dynamisch definierten Schedule
  - `swarmq schedule status` — Leader-Worker, nächste N fällige Schedules,
    letzter Run-Zeitpunkt je Schedule

- **Catch-up-Semantik bei Outage**: Verpasste Cron-Runs werden **übersprungen**.
  Beim Recovery wird nur der nächste anstehende Run geplant. „Cron ist
  best-effort scheduling, nicht at-least-once delivery". Verhindert
  Thundering-Herd bei längerem Outage.
- **Tests** (Unit + Integration + E2E):
  - Delay/ETA roundtrip mit echtem Broker+Backend
  - Cron via TaskInfo.cron (statisch) und via CLI (dynamisch)
  - Leader-Election: zwei Worker, nur einer wird Leader
  - Leader-Failover: Leader SIGKILL → Standby übernimmt in ≤30s
  - Catch-up-skip: Leader-Outage 1min, `*/1`-Cron, Recovery → genau 1 Run
    (der nächste), nicht 60 nachgeholt
  - CLI-Roundtrip: `schedule add` → Cron läuft → `schedule remove` → Cron
    stoppt
  - Task mit `cron=...` + `delay=...` gleichzeitig → `ValueError` bei
    TaskInfo-Construction (sinnlose Kombination)
  - Race: zwei Leader kurzzeitig (Split-Brain): Lua-basierter atomic
    Claim im Scheduler-Poll verhindert Double-Publish

**Out-of-Scope:**

- **Unifizierung von `{queue}.DQ` und Sorted Set** — DQ bleibt für
  Retry-Delays (R2.2, funktioniert); Sorted Set ist nur für User-Schedules
  + Cron. Zwei separate Mechanismen, klare Trennung (Broker-intern vs.
  App-Ebene).
- **Raw Unix-Cron-Features** — `L` (last day of month), `W` (weekday-nearest),
  `#` (nth weekday). v1 rejektiert Cron-Strings mit diesen Zeichen an der
  API-Grenze (Regex-Check vor `croniter()`-Call) mit klarer Fehlermeldung:
  „extended cron syntax (L, W, #) not supported in v1; use standard
  5-field format". Später wenn gebraucht opt-in via Config-Flag.
- **Timezone-Support** — v1 ist alles UTC. Config-flag `scheduler_timezone`
  kommt später, wenn User-Bedarf entsteht.
- **Interval-Schedules** (`every=timedelta(minutes=5)`) — stattdessen Cron
  nutzen. Dramatiq/Celery-Beat supporten es; wir überspringen es bis ein
  konkreter Bedarf kommt.
- **Workflow-Schedule** (Chain/Group als Schedule-Payload) — Phase 4 (R4.1).
- **Schedule-History/Audit-Log** — operational nice-to-have, kann später
  via Valkey-Key-Prefix `sq:sched:history:...` nachgerüstet werden.
- **Catch-up-Implementation**: `TaskInfo(cron=..., catchup=True)` ist
  v1 via Feldreservierung im API vorhanden (Closed Decision 16), aber
  wirft bei Aktivierung `NotImplementedError`. Vollständige
  Catch-up-Logik (backlog-bounded replay) kommt separat, ohne Breaking-
  API-Change.
- **Rate-Limit-Interaktion**: Ein Scheduled Task hat dieselbe Rate-Limit-
  Semantik wie ein manuell-gescheduleter Task (durchläuft die
  Rate-Limit-Pipeline normal) — keine Sonderregel.
- **Replication/HA des Scheduler-States**: v1 setzt voraus, dass Valkey
  selbst HA ist. Kein eigener State-Replicator.

### Deferred to Separate Tasks

- **R3.10 DLQ-CLI** (`swarmq dlq list/retry/purge`): eigener Commit-Block
  nach Scheduler. Der `swarmq schedule`-Teil von R3.10 kommt hier.
- **Workflow-Scheduling** (Chain/Group): Phase 4.
- **Scheduler-History**: Phase 4 oder später wenn operativ benötigt.

## Behavior Constraints

1. **Leader-Election ist weich**: Bei kurzem Leader-Outage (unter
   Heartbeat-TTL) bleibt der Cluster Leaderless. Scheduled Tasks werden
   in dieser Zeit **nicht** published. Das ist eine akzeptierte
   Delivery-Latenz-Spitze, nicht ein Datenverlust — die Tasks bleiben im
   Sorted Set und werden beim nächsten Poll veröffentlicht.

2. **Scheduled Tasks durchlaufen die normale Pipeline**: Der Scheduler
   published auf die Main-Queue des Tasks. Rate-Limit, Lock, Retry,
   Cancellation — alle bestehenden Mechanismen greifen danach normal.
   Kein Scheduler-Bypass.

3. **Cron-Berechnung ist deterministisch**: `croniter.get_next()` mit UTC
   als Referenz-Timezone. Wenn derselbe Cron-String auf zwei Workern
   evaluiert wird, kommt dasselbe Ergebnis raus.

4. ~~**Lua-atomares Claim im Poll**: Der Poll-Schritt (Leader) nutzt
   ZRANGEBYSCORE + ZREM in **einem** Lua-Script, damit auch bei
   kurzzeitigem Split-Brain-Szenario (zwei Leader) keine Task doppelt
   published wird — der erste ZREM-Gewinner bekommt sie, der zweite
   bekommt einen leeren Ergebnis-Set.~~
   **[KORRIGIERT 2026-06-03, Härtungs-Review R2]** So wurde es NICHT
   gebaut — Punkt 10 unten (Publish-before-ZREM) hat diese Variante
   bewusst ersetzt: `sched_poll.lua` ist read-only, der Claim erfolgt
   erst im owner-checked `sched_commit.lua` NACH dem Publish. Der reale
   Split-Brain-Schutz ist die Consumer-seitige Schedfire-Dedup
   (inflight-SETNX + completed-Marker); deren inflight-TTL ist seit R2
   an `hard_timeout + 30s` gekoppelt, damit sie nicht während eines
   langen Laufs abläuft. Zusätzlich tritt ein Leader nach konsekutiven
   Renew-Fehlern proaktiv zurück (P1-5), bevor ein Standby übernimmt.

5. **Schedule-Persistence ist kanonisch in Valkey**: Code-`TaskInfo(cron=...)`
   wird beim Start in Valkey gesynct. Bei Widerspruch (CLI-removed
   Schedule, den der Code immer noch deklariert) gewinnt der Valkey-Stand
   — CLI-Overrides sind persistent.

6. **Skip-Semantik für Cron**: Beim Recovery / Leader-Acquire wird für
   jede Schedule genau **ein** `croniter.get_next(now=now_utc())`-Call
   gemacht — `croniter.get_next(now=...)` liefert direkt den nächsten
   zukünftigen Feuerzeitpunkt, ohne Iteration über verpasste Runs. Kein
   Loop, keine Iterationsobergrenze nötig. `last_fire_at` wird nur zu
   Beobachtungszwecken (`swarmq schedule status`) persistiert, nicht für
   Forward-Scheduling genutzt.

7. **Delay/ETA = Sorted Set mit UTC-Unix-ms**: Die `delay=60`-API
   berechnet `score = now_ms + 60*1000` zur Schedule-Zeit; `eta=datetime(...)`
   wird in UTC-Unix-ms konvertiert.

8. **Task-`cron` + one-shot `delay`/`eta` — verschiedene Slots, eine
   exklusive Kombination**: Eine Task mit `TaskInfo(cron=...)` läuft
   periodisch als Cron-Schedule unter ihrem TaskInfo-abgeleiteten
   Schedule-Name. **Zusätzlich erlaubt**: manuelles
   `schedule(task, delay=...)` auf denselben Task-Typ — es publiziert
   eine einzelne one-shot-Instance mit eigener task_id, unabhängig vom
   Cron-Pfad. **Abgelehnt**: ein Dynamic-CLI-Schedule mit demselben
   Schedule-Name wie eine TaskInfo-Cron-Task (würde beides denselben
   Valkey-Key schreiben). Validation-Ort: beim CLI-add bzw. beim
   TaskInfo-Sync (nicht in `TaskInfo.__post_init__`, da `delay`/`eta`
   keine TaskInfo-Felder sind).

## Success Criteria

User-visible:
- `await client.schedule("send_email", "user@x", delay=60)` wartet 60s,
  dann läuft die Task.
- `await client.schedule("report", eta=datetime(2026, 4, 17, 9, 0))`
  läuft um 09:00 UTC am 17.04.
- `TaskInfo(cron="*/5 * * * *")` → Task läuft alle 5 Minuten automatisch,
  sobald der Worker steht. Kein separater Prozess.
- Zwei Worker laufen → beide Main-Queues werden bedient, nur **einer** ist
  Scheduler-Leader (Signal.SCHEDULER_LEADER beobachtbar).
- Leader-Worker SIGKILL → Standby übernimmt innerhalb ~10-30s (Heartbeat-TTL).
- `swarmq schedule list` zeigt alle aktiven Schedules (static + dynamic).
- `swarmq schedule add my_cron --cron "0 3 * * *" --task daily_cleanup`
  → Task läuft jeden Tag um 03:00 UTC.
- Catch-up-skip: Worker-Cluster ist 10min down, Recovery → keine 10
  Backlog-Runs von `*/1`-Cron, sondern nur der nächste.
- Comparison-Doc: SwarmQ Scheduler-Cell von „missing" auf „embedded, no
  separate process, automatic failover".

Engineering-internal (nice-to-have):
- 0 neue P1-Findings im `compound-engineering:review`.
- Alle bestehenden Tests grün. Geschätzt +60-100 neue Tests (Unit +
  Integration + E2E).
- Scheduler-Loop-Overhead: <1% CPU im idle auf einem 4-Core-System
  (Baseline: Poll alle 1s gegen leeres Sorted Set).

## Risks

| Risiko | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Split-Brain: zwei Worker halten den Leader-Lock gleichzeitig (TTL-Race) | gering | mittel | ~~Lua-atomares ZRANGEBYSCORE+ZREM-Claim im Poll~~ [korrigiert 2026-06-03: real ist der Schutz die Consumer-seitige Schedfire-Dedup (inflight-SETNX, TTL ≥ hard_timeout+30s) + proaktiver Leader-Step-down nach Renew-Fehlern; Poll ist read-only, Claim erst im owner-checked Commit nach Publish] |
| Leader-Heartbeat verpasst eine Iteration unter GC-Pause, Lock TTL verfällt | mittel | niedrig | TTL >> Heartbeat-Intervall (3×); kurzer Leaderless-Window ist akzeptiert |
| Cron-Semantik-Drift: croniter-Ergebnisse unterscheiden sich von Celery-Beat-Erwartung | gering | niedrig | croniter ist exakt derselbe Parser wie Celery-Beat nutzt; Dokumentation des 5-Felder-Formats |
| Sorted-Set wächst unbegrenzt bei fehlerhaften Task-Scheduling-Loops | gering | mittel | DEBUG-Log mit ZCARD im Scheduler-Poll. Kein hartes Limit in v1 — Valkey handled Sorted-Sets in Millionen-Bereich problemlos, und eine buggy-Schleife ist ein User-Code-Bug, kein Framework-Szenario. |
| Valkey ist nicht HA / nicht persistent konfiguriert → ETA-Loss bei Valkey-Restart, Leader-Race bei Restart | mittel | hoch | Dokumentation empfiehlt Valkey-AOF oder Sentinel/Cluster für Production. v1 setzt voraus dass Valkey selbst durable+HA ist. Nicht-HA-Valkey ist explizites „alpha-only"-Deployment. |
| Rolling-Deploy emittiert Failover-Signal bei jedem geplanten Worker-Stop | hoch | niedrig | Separate Signal-Events für Planned-Handover (Lock-release → Acquire) vs Unplanned-Failover (TTL-expiry → Acquire). Verhindert Alert-Fatigue. |
| TaskInfo.cron bei Task-Rename/Umbenennung: Schedule bleibt unter altem Namen in Valkey, neue Runs gehen an nichtexistierende Task | mittel | niedrig | Leader prüft beim Sync ob der Task-Name in der Registry existiert; nichtexistente Schedules werden mit Warn-Log geskippt |
| Clock-Skew zwischen Workers beeinflusst Cron-Timing | gering | niedrig | Cron-Evaluation nur im Leader → nur dessen Clock zählt. Double-Execution ausgeschlossen durch Consumer-SETNX-Dedup auf `(schedule_name, fire_ts_ms)` (siehe Closed Decision 10). Grobe NTP-Steps auf dem Leader können zu Cron-Drift führen (akzeptiert, best-effort). |
| Catch-up-skip versäumt wichtige Runs bei langer Downtime | akzeptiert | mittel | Dokumentiert als „best-effort scheduling"; Opt-in-catchup per Schedule später nachrüstbar |

## Closed Decisions (vom Brainstorm)

Diese Punkte sind **entschieden** (nicht Open Questions für Planning):

1. **Scope**: Full — R3.8 + R3.9 + R3.10 Scheduler-CLI gemeinsam.
2. **Delay-Store**: Zwei separate Mechanismen. `{queue}.DQ` (bestehend)
   für Retry-Delays, neuer Valkey Sorted Set `sq:sched:zset` für
   User-Schedules + Cron.
3. **Cron-Parser**: `croniter` (nicht apscheduler, nicht eigener Parser).
4. **Client-API**: Reserved keyword-only args `delay`, `eta` auf
   `client.schedule()` — genau wie `queue` bereits reserved ist.
5. **Cron-Declaration**: Beides — `TaskInfo(cron="...")` statisch **und**
   `swarmq schedule add --cron ...` dynamisch via CLI. Beide in Valkey
   persistiert, CLI gewinnt bei Konflikt.
6. **Catch-up**: Skip, nur nächster anstehender Run wird geplant nach
   Outage. Kein at-least-once für Cron.
7. **Leader-Election**: Valkey-Lock (reuse `LockManager`-Pattern),
   Heartbeat ~10s, TTL ~30s (3×). Embedded in jedem Worker, exactly one
   active.
8. **Timezone**: UTC für v1. Kein Timezone-Parameter.
9. **Scheduler-Signals**: `Signal.SCHEDULER_LEADER` /
   `Signal.SCHEDULER_STANDBY` / `Signal.WORKER_HEARTBEAT` (alle schon in
   `signals.py` vorhanden) werden hier erstmalig dispatched.
10. **Claim-Publish-Sequence: Publish-before-ZREM mit Consumer-Dedup**
    (reviewed, ersetzt naive ZREM-first-Variante):
    - Leader-Poll-Step 1: Lua liest fällige Members via ZRANGEBYSCORE
      (LIMIT 0 100), OHNE ZREM.
    - Leader-Poll-Step 2: für jede Message: broker-publish mit
      Idempotency-Header `x-swarmq-schedfire: <schedule_name>:<fire_ts_ms>`
      und neuer `task_id = uuid7()`.
    - Leader-Poll-Step 3: ZREM des Members NACH Broker-Publisher-Confirm
      (aio-pika garantiert Confirm via `connect_robust` + default
      persistent-delivery).
    - Consumer-Side (Worker `_handle_message`): wenn
      `x-swarmq-schedfire`-Header gesetzt: SETNX mit Key
      `sq:sched:fired:<schedule>:<fire_ts>` TTL=60s. Bei Duplicate
      (`SETNX` returnt 0): ack + skip, kein run(). Mit TTL=60s wird die
      Dedup-Fenster nicht unendlich wachsen.
    - Failure-Modi: (a) Crash zwischen Publish und ZREM → Redelivery
      bei nächstem Poll, SETNX fängt Duplicate ab. (b) Publish fails →
      kein ZREM, nächster Poll retriggert. Beide sicher.
11. **Schedule-Persistence — TaskInfo-Sync nur insert-if-absent**:
    Code-`TaskInfo(cron=...)` wird beim Leader-Acquire in Valkey
    gesynct, **aber nur via insert-if-absent (SETNX-Semantik)**. Das
    überschreibt NIEMALS einen bestehenden Key. Damit:
    - CLI-modifizierte Schedules (via `schedule add`) bleiben bei
      Failover erhalten (keine Ping-Pong-Überschreibung).
    - CLI-removed Schedules bekommen einen `"disabled"`-Marker im
      Hash-Feld, der den Re-Insert durch TaskInfo-Sync blockiert.
    - Neue TaskInfo-Crons (frisch im Code) werden beim ersten
      Leader-Acquire nach Deploy geschrieben.
    Das löst den CD5-vs-CD11-Widerspruch (CLI wins **nur für existing
    schedules**; neue Code-Schedules werden ergänzt).
12. **Schedule-Persistence-Format**: Global-Hash `sq:sched:defs`
    mit Schedule-Name als Feld-Key, HGETALL für list, HSET für add,
    HDEL (+ `disabled`-Tombstone) für remove. Einfach, ein Key, keine
    KEYS-Scans. Sorted-Set `sq:sched:zset` bleibt separat für
    next_run-Scores.
13. **Schedule-Payload-Serialization**: msgspec.json — wiederverwendet
    das bestehende Message-Format. Ein Sorted-Set-Member ist
    effektiv eine deferred Message; kein neuer Serialisierungs-Boundary.
14. **Cron-String Eager-Validation**: `TaskInfo.__post_init__` validiert
    die Cron-Expression via `croniter('...', datetime.utcnow())` — fails
    fast at module-import, konsistent mit bestehendem `rate_limit`-Muster.
15. **croniter als Core-Dependency**: in `[project.dependencies]`
    (nicht optional), weil der Scheduler embedded in jedem Worker läuft.
16. **`TaskInfo.catchup: bool = False` reserviert**: Feld wird in v1
    hinzugefügt, Default ist skip-Semantik. Bei `catchup=True` wirft
    die Scheduler-Loop `NotImplementedError("catchup=True is not yet
    implemented; see plan 3d")`. Kosten: 2 Zeilen in TaskInfo. Gewinn:
    spätere Catch-up-Implementation ist kein breaking API change.
17. **Signal-Trennung für Leader-Transitionen**: Zwei separate Signals
    statt einer: `Signal.SCHEDULER_LEADER_HANDOVER` (planned — Leader
    released seinen Lock im Shutdown) und
    `Signal.SCHEDULER_LEADER_FAILOVER` (unplanned — vorheriger Leader
    Heartbeat-TTL-expired). Verhindert Alert-Fatigue bei
    Rolling-Deploys. Beide sind neu in `signals.py`.
18. **LockManager-Extension für Heartbeat**: Neue
    `LockManager.renew(lock_name, owner_token, ttl_ms) -> bool` plus
    neues `lock_renew.lua`: `if GET(key) == owner then PEXPIRE(key, ttl)
    return 1 else return 0`. Eigenständige Unit im Plan. Template
    existiert in `docs/recherche-deepening-lua-scripts.md` §2.
19. **Clock-Referenz — realistische Mitigation**: Risk-Zeile
    „Valkey-TIME als Referenz" war falsch — `lock_acquire.lua` nimmt
    caller-now_ms, `croniter.get_next()` nutzt Python-datetime. Die
    tatsächliche Mitigation gegen Clock-Skew ist der
    Idempotency-Key-Mechanismus aus Decision 10: selbst wenn zwei
    Worker kurz denselben Fire-Timestamp sehen, dedupiert der
    Consumer-SETNX. Clock-Skew bleibt ein Operational-Risk (grosser
    NTP-Step kann Cron-Drift verursachen), aber Double-Execution ist
    ausgeschlossen.

## Open Questions (für Planning)

Nur echte Planning-Fragen, keine Wiederholung von Closed Decisions:

1. **Scheduler-Module-Struktur**: Eigenes `src/swarmq/scheduler.py`
   (analog `locking.py`, `limits.py`) — im Planning bestätigen.
2. **Poll-Intervall- und Heartbeat-Config-Shape**: Neue
   `SwarmQConfig.scheduler_poll_interval` (Default 1.0s),
   `scheduler_heartbeat_interval` (Default 10s), `scheduler_lock_ttl`
   (Default 30s) — drei separate Felder. Env-Vars folgen
   `SWARMQ_*`-Muster. Details (Validierung, Min/Max-Bounds) im Plan.
3. **CLI-Output-Format**: `swarmq schedule list` als Table (human) oder
   JSON via `--format`-Flag. Konsistent mit bestehenden CLI-Flags.
4. **Consumer-Dedup-Key-TTL**: `sq:sched:fired:<schedule>:<fire_ts>`
   hat TTL 60s nach Closed Decision 10. Ist 60s für alle realistischen
   Retry-Szenarien genug? (Retry-Delay-Default ist `retry_base_delay=1.0s`
   × 2^retries + Jitter — bei `max_retries=3` maximal ~7s. 60s ist
   komfortabler Puffer. Im Plan bestätigen oder anpassen.)
5. **`TaskInfo.cron`-Rename-Cleanup**: Wenn ein Task umbenannt wird,
   bleibt der alte Schedule-Name in Valkey. Cleanup via
   `swarmq schedule prune` (findet verwaiste Schedules)? Oder
   akzeptiert als Operator-Task? Empfehlung: prune-Subcommand in
   Phase 3c.1 oder später.
6. **Shutdown-Signal-Timing**: Wann wird `SCHEDULER_LEADER_HANDOVER`
   emittiert? Vor Lock-Release (User sieht „gleich weg"), nach
   Lock-Release (User sieht „ist weg"), oder beides? Plan-Detail.

## Dependencies

- **R3.4 (Task Locking)** — DONE. `LockManager`-Pattern wird für
  Leader-Election wiederverwendet. `lock_acquire.lua` ist bereits da.
- **R3.3 (Rate Limiting)** — DONE. Pattern für Lua-Script-Loading
  (`load_script`, `register_script`), NOSCRIPT-Recovery.
- **R2.5 (Graceful Shutdown)** — DONE. Leader muss beim Shutdown den
  Lock freigeben, damit Failover nicht bis TTL warten muss.
- **R2.2 (Retry / DQ)** — DONE. Scheduled Tasks durchlaufen Retry/DLQ
  normal (Scheduler published nur, dann greift der bestehende Pfad).
- **croniter** (neue Dependency) — ~50kB Pure-Python. Standard-Cron-Parser.
- **R3.10 CLI-Grundgerüst** — `swarmq`-Commando existiert. Neue
  Subcommands `schedule {list|add|remove|status}` werden ergänzt.

## Non-Goals

- Unified delay-mechanism (Sorted Set + DQ getrennt)
- Timezone-Support in v1
- Catch-up-catchup (skip statt replay)
- Interval-Schedules (`every=...`)
- Workflow-Schedules (Phase 4)
- DLQ-CLI (eigene Phase)
- Schedule-History/Audit-Log
- Distributed fairness für Cron (Leader macht alles)
- Schedule-Validation beim Schreiben in Valkey (Cron-String wird nur beim
  ersten Evaluation geprüft — kein Eager-Validation-Service)
