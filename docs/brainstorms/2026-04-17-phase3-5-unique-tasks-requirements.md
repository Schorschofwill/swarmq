---
title: "Phase 3.5 — Unique Tasks / Deduplication (R3.5)"
type: requirements
status: draft
date: 2026-04-17
origin: docs/brainstorms/implementation-order-requirements.md
---

# Phase 3.5 — Unique Tasks / Deduplication (R3.5)

## Problem Frame

SwarmQ hat heute keine eingebaute Deduplizierung. Nutzer müssen
Idempotenz selbst bauen (ad-hoc SETNX, Datenbank-Unique-Constraints,
Application-Level-Keys). Das verletzt das "batteries-included"-Profil
gegenüber Konkurrenz (Celery QueueOnce, Dramatiq Unique-Middleware,
arq `_id`).

R3.5 fügt Library-Level-Dedup hinzu. Zwei primäre Use-Cases
treiben den Scope:

1. **Submit-Button-Doppelklick** — HTTP-Handler enqueued denselben
   Task bei zufälligem Doppelklick binnen Sekunden. Dedup-Fenster
   kurz (5–60s). Default-Mode `completion` schützt natürlich
   (Duplikate werden bis Task-Ende unterdrückt). Wer nur
   Enqueue-Burst-Schutz will und keine Parallel-Suppression braucht,
   wählt `start`-Opt-in.
2. **Event/Webhook-Idempotenz** — Externe Quelle (Stripe, GitHub)
   liefert dasselbe Event mehrfach (at-least-once-Semantik). Dedup-
   Fenster mittel (Minuten–Stunden). Default-Mode `completion`
   deckt auch SwarmQ-interne Retries ab: Flag überlebt Retry-
   Backoff, zweite Webhook-Delivery im Retry-Fenster wird deduppt.

**Parallel-Run-Schutz** (z. B. "Report für User X darf nicht
parallel laufen") ist **nicht** die Aufgabe von R3.5 — dafür steht
R3.4 Locking (`TaskInfo(lock=..., max_concurrency=1)`) zur
Verfügung, das direkt an der Execution-Stelle mutex'ed. R3.5 +
Locking sind komplementär: Dedup drückt doppelte _Enqueues_ auf
Sekunden/Minuten-Fenstern, Locking verhindert parallele
_Executions_.

Design-Philosophie: **silent-by-default mit Observability via
Signal** — peer-parity mit Celery QueueOnce, Dramatiq-Unique, arq
`_id`. `Client.schedule()` gibt immer einen `task_id` zurück (fresh
oder existing). Nutzer, die Dedup-Events beobachten wollen, hören
das neue `duplicate_detected`-Signal oder lesen strukturierte Logs.

Ziel: ein API, das die beiden Use-Cases ohne Boilerplate abdeckt.

## Scope Boundaries

### In-Scope

- **`TaskInfo`-Felder:**
  - `unique_for: int | None = None` — TTL in Sekunden. `None` =
    Dedup deaktiviert. Wert > 0 = Dedup aktiv mit diesem TTL.
    `unique_for=0` → `ValueError` in `TaskInfo.__post_init__`
    (konsistent mit Fail-fast-Prinzip).
  - `unique_until: Literal["start", "completion"] = "completion"` —
    Wann Flag freigegeben wird. **Default `completion`**:
    Flag wird bei Terminal-State (success ODER final-failure/DLQ
    nach erschöpften Retries ODER cancel) freigegeben. Retry ist
    nicht-terminal — Flag überlebt Retry-Backoff, so dass
    Duplikate während Retries unterdrückt werden (notwendig für
    korrekten Webhook-Idempotenz-Schutz). Rationale: Retry-Problem
    war der Killer gegen `start`-Default. `start` als Opt-in bleibt
    für User mit kürzerem Semantik-Bedarf (Submit-Burst-Schutz,
    keine Parallel-Suppression nötig, Retry-Duplikate akzeptiert
    — siehe Decision 10).
- **Per-Call-Override** auf `Client.schedule()` und
  `Client.schedule_many()`:
  - `unique_for=...` (reserved kwarg) überschreibt TaskInfo-Default
    für diesen Call.
  - `unique_for=None` deaktiviert Dedup für diesen Call (auch wenn
    TaskInfo einen Default setzt).
  - `unique_until=...` analog.
- **Dedup-Key:** Auto-Hash `sha256(task_name + canonical_encode(args, kwargs))`.
  `canonical_encode` sortiert dict-Keys rekursiv vor msgspec-
  Encoding (msgspec.json hat kein `sort_keys`; siehe Closed
  Decisions §2). Kein Queue-, `delay`-, `eta`-, `task_id`- oder
  Dedup-Config-Anteil im Hash. Args/Kwargs müssen msgspec-
  serialisierbar sein; sonst wird bei `schedule()` bereits heute ein
  `ResultSerializationError` geworfen.
- **Silent-by-Default API:** `Client.schedule(...) -> str` gibt
  IMMER eine `task_id` zurück — bei Duplikat ist es die `task_id`
  des zuerst-wins-Calls. Keine neue Exception, kein try/except-
  Boilerplate. `Client.schedule_many(...) -> list[str]` bleibt
  unverändert `list[str]` (Duplikat-Positionen bekommen die
  existing task_id).
- **Observability via Signal:** Neuer Signal-Enum-Member
  `Signal.TASK_DUPLICATE_DETECTED` (konsistent mit
  `TASK_ENQUEUED`/`TASK_COMPLETED` in `src/swarmq/signals.py`; NICHT
  Blinker — SwarmQ hat einen eigenen async `SignalDispatcher`).
  Payload: `(existing_task_id, task_name, dedup_hash, source)`.
  `source ∈ {"client.schedule", "client.schedule_many",
  "scheduler.fire"}`. Parallel dazu strukturierter Log-Event
  `swarmq.dedup.detected`.

  **Asymmetrie Signal-Handler vs Middleware** (wichtig für User
  die raise-Verhalten wollen): Signal-Handler-Exceptions werden
  vom `SignalDispatcher.dispatch` per Design geschluckt (sieh
  `src/swarmq/signals.py`) — reine Observability, kann enqueue
  NICHT abbrechen. Pre-enqueue-Middleware läuft im schedule()-
  Call-Stack und kann exceptions propagieren. User die raise-
  Verhalten brauchen, MÜSSEN Middleware nutzen, nicht Signal-
  Handler.
- **Scheduler-Integration:** `unique_for` greift auch bei
  Cron-/Delay-/ETA-Fires. Duplikat = silent skip, Signal + Log
  werden emittiert (dieselbe Observability-Oberfläche wie Client).
  Schicht-Koexistenz mit dem bestehenden `x-swarmq-schedfire`-
  Dedup: schedfire = per fire-event (Cron-Idempotenz bei
  Leader-Failover), `sq:unique` = per args-identity (User-
  Dedup-Semantik). Beide Layer bleiben, sind orthogonal.
- **Cancel-Integration:** Für `unique_until=completion` (Default):
  `client.cancel(task_id)` + tatsächliche Cancel-Terminalisierung
  gibt Flag frei (zählt als Terminal-State analog zu success/
  failure). Für `unique_until=start` (Opt-in) ist der Flag beim
  Cancel-Zeitpunkt bereits released (Worker hat ihn bei Pickup
  geräumt) — cancel hat keine Dedup-Interaktion.
- **Retry-Verhalten:** Für `completion`-Modus (Default): Flag
  überlebt Retries, wird erst bei Terminal-State freigegeben.
  Webhook-Idempotenz-Use-Case ist dadurch natürlich geschützt.
  Für `start`-Modus (Opt-in): Flag beim ersten Pickup gelöscht;
  Retries haben keinen Dedup-Schutz (Wiederholung im Backoff-
  Fenster kann ein Duplikat durchrutschen lassen — dokumentierte
  Einschränkung; Nutzer die striktere Semantik brauchen, bleiben
  bei `completion`).
- **Fail-fast-Validierung:** `TaskInfo(unique_for=0)` → `ValueError`
  in `TaskInfo.__post_init__` (gleicher Ort wie die bestehenden
  `lock=`/`max_concurrency=`-Konsistenz-Checks in
  `src/swarmq/task.py`). `unique_until` ohne `unique_for=None` wird
  still ignoriert — siehe Decision 6 für volle Validierungs-
  Semantik.
- **Reserved-kwarg-Erweiterung:** `_RESERVED_RUN_PARAM_NAMES`
  (`src/swarmq/task.py`) um `unique_for` und `unique_until` erweitern,
  analog zu den bestehenden `delay`/`eta`/`queue`. Bestehende Tasks
  mit diesen Param-Namen in `run()` müssen umbenennen (Migration-
  Hinweis in Changelog).
- **Deferred-Pfad (delay/eta):** SETNX erfolgt in `_schedule_deferred`
  **vor** `ScheduleStore.add_schedule`, mit TTL = `unique_for` +
  `delay_seconds` (Puffer, damit der Flag bis nach dem Fire lebt).
  Alternative "SETNX zur Fire-Zeit" wurde verworfen, weil sie
  doppelte ScheduleStore-Einträge nicht verhindert — das ist der
  eigentliche Zweck von Dedup für deferred Tasks.
- **Chaos-Safety-Net:** TTL dient als Backstop bei jedem Crash
  zwischen SETNX und Ack (Worker-Crash, Client-Broker-Publish-
  Failure, etc.). Nach TTL-Ablauf kann neuer Task mit gleichen Args
  scheduled werden. Kein zusätzlicher best-effort-Cleanup im Client
  (würde selbst bei broken Connection fehlschlagen und nur Code-
  Komplexität hinzufügen) — TTL ist alleiniger Recovery-Pfad.
  Analogie zum bestehenden Cancel-Flag-Stale-Zombie (gleiche Klasse,
  gleiche Rezeptur).

### Non-Goals

- **Parallel-Run-Schutz als primärer Use-Case** — R3.4 Locking löst
  das; R3.5 fokussiert auf Enqueue-Time-Dedup. User die sowohl
  Dedup als auch Parallel-Mutex brauchen, kombinieren
  `unique_for=...` (Enqueue-Schutz) + `lock=..., max_concurrency=1`
  (Execution-Schutz).
- **DuplicateTaskError-Exception** — silent-by-default macht sie
  überflüssig. Observability via `duplicate_detected`-Signal.
- **Custom-`unique_key`-Override** (String oder Callable) — Auto-
  Hash-only in v1. Nutzer mit Teil-Args-Dedup-Bedarf (typisch:
  Webhook-Handler mit `event_id` + variablem Payload) baut
  Task-Wrapper, der nur `event_id` als Arg akzeptiert. Siehe
  Deferred.
- Cross-Queue-Dedup (z. B. "dedup über alle Queues hinweg") —
  erfasst durch das queue-lose Default-Hashing, aber keine
  explizite Mehr-Queue-API.
- Dedup-Stats/Metrics-Endpoint — kommt später über Progress-
  Tracking (R3.7) oder Monitoring-Extension.
- `release_on_failure=False`-Flag für strikt-success-only-Release —
  nicht in v1. `unique_until=completion` + TTL-Expiry reicht als
  Recovery-Pfad.
- Dedup-Aware-Cancel ("cancel die existierende Dup, enqueue neu") —
  zu viel Policy; Nutzer ruft explizit `cancel` + `schedule` in
  ihrer Reihenfolge auf.

### Deferred to Separate Tasks

- **Custom-`unique_key`-String/Callable-Override** — eigener Plan,
  wenn User-Feedback den Bedarf zeigt (insb. für Webhook-
  Idempotenz-Pattern mit nur `event_id` als Key).
- **Dedup-Metrics** — Teil von R3.7-Follow-up.
- **Cross-Queue-Dedup-API** — separates Design falls nötig.
- **`raise_on_duplicate`-Opt-in-Parameter** — post-v1, falls ein
  signifikanter Anteil der User Raise-Verhalten bevorzugt. Kann
  durch User-Middleware im Signal-Handler heute schon gebaut werden.

## Closed Decisions

1. **API-Oberfläche:** TaskInfo-Default + per-call-Override. Dies
   setzt Präzedenz für override-fähige TaskInfo-Felder. Begründung:
   TaskInfo-Felder fallen in zwei Kategorien — **Category A**
   (execution-shape: `max_retries`, `soft_timeout`, `hard_timeout`,
   `rate_limit`, `lock`) bleibt immutable für Predictability;
   **Category B** (scheduling-intent: `queue`, `delay`, `eta`,
   `unique_*`) ist per-call overridable weil sie _diese_ Submission
   beschreibt, nicht den Task selbst. `unique_*` ist Category B.
   Plan dokumentiert diese Unterscheidung als Invariante.

2. **Hash-Zusammensetzung:** `sha256(task_name.encode() +
   canonical_encode(args, kwargs))`. Neue Helper-Funktion
   `canonical_encode` in `src/swarmq/serialization.py`, die
   rekursiv Dict-Keys sortiert **bevor** msgspec.json encode
   läuft (msgspec.json hat kein `sort_keys`-Flag). Sets werden
   sortiert; Tupel/Listen behalten Order. Nested Strukturen
   werden rekursiv kanonisiert.

3. **Default für `unique_until`:** `completion` (nicht `start`).
   Rationale: deckt beide primären Use-Cases korrekt ab
   (Submit-Button UND Webhook), inkl. SwarmQ-interner Retries.
   `start`-Default hätte Retry-Duplikate durchgelassen — Webhook-
   Schutz wäre dadurch unvollständig. Die Implementation-Kosten
   (Owner-Check-Lua, Terminal-Pfad-Enumeration, TTL-Expiry-Race-
   Handling) sind ohnehin im Scope (weil `completion`-Opt-in
   existiert); die Default-Wahl verschiebt sie nicht. `start`
   bleibt als Opt-in für User die bewusst nur Enqueue-Burst-Schutz
   wollen (z. B. UI-Submit mit kurzem Fenster, wo Retry-Duplikate
   akzeptiert werden).

4. **Silent-by-Default + Signal-Observability:** `Client.schedule()`
   gibt immer `task_id` zurück (fresh oder existing). Bei Duplikat
   wird `duplicate_detected`-Signal emittiert + Log `swarmq.dedup.
   detected`. Kein `DuplicateTaskError`. Rationale: peer-parity
   (Celery/Dramatiq/arq), kein try/except-Boilerplate, einheitliche
   Semantik zwischen Client und Scheduler (beide silent). Nutzer die
   raise-Verhalten wollen, installieren Signal-Handler oder
   pre-enqueue-Middleware.

5. **`completion`-Definition** (Default-Mode-Semantik): success
   ODER final-failure (DLQ nach erschöpften Retries) ODER cancel.
   Retry ist nicht-terminal. Implementation: Flag-Release-Hook im
   Worker bei jedem Terminal-Exit. ENUMERATION aller Worker-
   Terminal-Pfade ist Plan-Zeit-Aufgabe (mindestens: erfolgreicher
   `ack`, retry-exhausted-DLQ-publish, NoRetry-direct-DLQ,
   lock-render-failure-DLQ, lock-empty-render-DLQ,
   lock-conflict-cap-DLQ, pre-start-cancel, mid-execution-cancel,
   hard-timeout-cancel, sowie alle "DLQ-publish-failed →
   fallback-ack"-Pfade in `src/swarmq/worker.py`). Vollständige Liste
   wird im Plan via `grep` auf `_send_to_dlq`-Aufrufer und
   `clear_cancel_flag`-Aufrufer verifiziert. Release via Owner-
   Check-Lua (`unique_release.lua`, analog zu `lock_release.lua`):
   `DEL` nur wenn Value == eigene `task_id`.

6. **TTL-Validierung in `TaskInfo.__post_init__`:**
   - `unique_for=0` → `ValueError` ("use None to disable dedup").
     Fail-fast, verhindert unsichtbaren Typo.
   - `unique_for=None` → Dedup ist aus; `unique_until`-Wert wird
     ignoriert (keine ValueError selbst wenn `unique_until="start"`
     gesetzt ist — vermeidet Sentinel-Diskriminierung
     "explizit vs default" in der frozen-dataclass).
   - `unique_for > 0` → Dedup aktiv; `unique_until` wird angewendet
     (Default `completion`, Opt-in `start`).

7. **Scheduler-Dedup-Layer-Koexistenz:** Zwei Dedup-Layer
   koexistieren — `x-swarmq-schedfire`-Header (per fire-event, für
   Leader-Failover-Idempotenz) bleibt unverändert; neue
   `sq:unique:{hash}`-Schicht (per args-identity) kommt dazu. Der
   Scheduler-Leader führt in `_fire_one` **vor** dem broker.publish
   einen SETNX-Check aus (analog zum Client); bei NX-Fail wird
   `duplicate_detected`-Signal emittiert + skip. Bei NX-Success
   läuft der bestehende publish+commit-Pfad wie gehabt. Rationale:
   uniforme User-Semantik zwischen Client-schedule() und Cron-Fire
   ohne den robusten schedfire-Mechanismus zu entfernen.

8. **Cancel triggert Flag-Release (Default-Mode):** Im
   `completion`-Default gibt Cancel-Terminalisierung den Flag frei
   (analog zu ack/DLQ — zählt als Terminal-State). Für `start`-
   Opt-in ist der Flag zum Cancel-Zeitpunkt schon gelöscht (Worker
   hat ihn bei Pickup geräumt) — kein Cancel-Release nötig.

9. **`schedule_many` Return-Typ unverändert:** Bleibt `list[str]`.
   Duplikat-Positionen bekommen die existing `task_id` (identisch
   zu `schedule()`-Silent-Verhalten). Pro Duplikat wird
   `TASK_DUPLICATE_DETECTED`-Signal emittiert (mit Batch-Index als
   Context). Signatur-Pin (bewahrt bestehende positional-or-keyword-
   `queue`; keine breaking change für heutige Caller):
   `schedule_many(tasks, queue=None, *, unique_for=_MISSING, unique_until=_MISSING)`.
   Bei `_MISSING`: jedes Element nutzt TaskInfo-Default. Bei
   gesetztem Wert: batch-weiter Override (alle Elemente gleich).
   Kein per-Element-Override in v1. `_MISSING` ist ein modul-
   privater Sentinel (`object()`-Instance), kein öffentlicher Typ.

10. **Retry-Semantik nach Mode:**
    - `completion` (Default): Flag überlebt Retries; bleibt bis
      zum Terminal-State (success/DLQ/cancel). Webhook- und
      Parallel-Enqueue-Schutz wirken auch während Retry-Backoff.
    - `start` (Opt-in): Flag bei Pickup gelöscht; während
      Retry-Backoff kein Dedup-Schutz. Dokumentierte Einschränkung
      — User wählen diesen Mode bewusst für Submit-Button-Use-Case
      mit kurzen TTLs.

11. **Deferred-Pfad (delay/eta) Dedup-Timing:** SETNX erfolgt in
    `Client._schedule_deferred` **vor** `ScheduleStore.add_schedule`.
    TTL = `unique_for + fire_offset_seconds`, wobei
    `fire_offset_seconds = delay` bei `delay=`-API, bzw.
    `(eta - now).total_seconds()` bei `eta=`-API. Rationale: Schutz
    gegen doppelte ScheduleStore-Einträge — alternative "SETNX zur
    Fire-Zeit" würde doppelte Deferred-Einträge nicht verhindern.

    **Dokumentierte Semantik** (für User): bei deferred Tasks ist
    "Dedup-Fenster" = `delay` + `unique_for` (nicht nur `unique_for`).
    Das ist im Allgemeinen was User wollen — doppelter Submit-Call
    während der delay-Phase soll auch deduppt werden. User die nur
    das post-fire-Fenster dedupen wollen, setzen eine eigene Logik
    um oder nutzen separates Dedup am Fire-Zeitpunkt (nicht in v1).

## Open Questions (For Plan-Time)

- **Key-Namespace:** `sq:unique:{sha256_hex}` vorschlag. Konsistenz
  mit `sq:cancel:`, `sq:sched:fired:`.
- **SETNX-Value:** `task_id` (für Owner-Check bei
  `unique_until=completion` Default). Ermöglicht Owner-checked
  Release; verhindert dass ein Task den Flag eines anderen löscht.
- **Lua-Scripts:** für `completion`-Default wird `unique_release.lua`
  (DEL-if-owner, analog `lock_release.lua`) gebraucht. Für
  `start`-Opt-in genügt Pickup-`DEL` ohne Owner-Check (Race-
  Fenster ist nur die atomare Pickup-Operation). Plan entscheidet
  ob beide
  Release-Pfade im gleichen Script unifizierbar sind.
- **Scheduler-SETNX-Ort:** direkt in Python im Leader-`_fire_one`
  **vor** `broker.publish`, oder integriert in `sched_commit.lua`?
  Python-Variante ist einfacher (kein Lua-Script-Update), Lua-
  Variante ist atomarer.
- **Stale-Flag-Zombie-Analogie zum Cancel-Flag:** Dedup-Flag kann
  bei Worker-Crash zwischen SETNX und Ack als Orphan zurückbleiben
  — selbe Klasse wie der bekannte Cancel-Flag-Zombie. TTL-Backstop
  ist Haupt-Recovery-Pfad. Client-seitiger try/except-Cleanup bei
  `BrokerConnectionError` ist Best-Effort-Ergänzung.
- **`client.run()` (sync API):** gibt `result` zurück nachdem
  `schedule()` + `get_result()` kombiniert; im Silent-Default
  identisch zum Non-Dedup-Pfad (User bekommt das Ergebnis des
  zuerst-wins-Calls via `get_result(existing_task_id)`).
  Keine Spezialbehandlung nötig.
- **Signal-Naming:** finaler Enum-Name `TASK_DUPLICATE_DETECTED`
  (konsistent mit bestehendem `TASK_ENQUEUED`/`TASK_COMPLETED`-
  Pattern in `src/swarmq/signals.py`).
- **Scheduler-Leader-Failover-Noise:** bei Leader-Crash zwischen
  SETNX und commit_fire, retry'ed der neue Leader → SETNX
  schlägt fehl (Flag noch vom ersten Leader gesetzt) →
  `TASK_DUPLICATE_DETECTED` wird emittiert, obwohl kein User-
  sichtbares Duplikat. Operator-Observability-Lärm. Plan
  entscheidet: (a) Scheduler-Fire-Signal separat benennen, oder
  (b) Schedfire-Header-Dedup fängt das bereits ab (verifizieren!).

## Test Scenarios (High-Level)

**Happy Path (silent):**
- `TaskInfo(..., unique_for=60)` + `schedule()` erfolgt →
  `sq:unique:{hash}` gesetzt mit TTL 60s, task_id returned.
- Gleicher Call binnen 60s → gibt dieselbe task_id zurück (silent);
  `duplicate_detected`-Signal emittiert mit
  `(existing_task_id=task_id, task_name, hash, source="client.schedule")`.
- `swarmq.dedup.detected`-Log enthält dieselben Felder.
- Nach 60s TTL-Ablauf → neuer `schedule()` erzeugt neuen Task.

**Hash-Stabilität:**
- `schedule("t", a=1, b=2)` und `schedule("t", b=2, a=1)` → gleicher
  Hash (canonical_encode sortiert Keys). Zweiter Call wird deduppt.
- `schedule("t", items=[3,1,2])` vs `schedule("t", items=[1,2,3])` →
  verschiedener Hash (Listen-Order ist signifikant).

**`unique_until="completion"` (Default):**
- Flag überlebt Execution. Retries überleben Flag.
- Flag wird bei Terminal-State freigegeben via `unique_release.lua`
  (Owner-Check auf task_id). Enumerate: success-ack, retry-exhausted-
  DLQ, NoRetry-direct-DLQ, lock-render-failure-DLQ, lock-empty-
  render-DLQ, lock-conflict-cap-DLQ, pre-start-cancel, mid-execution-
  cancel, hard-timeout-cancel, plus DLQ-publish-failed-fallback-
  ack-Pfade.
- TTL-Expiry-Race: `unique_for=60` + 120s-Task → bei t+60 verschwindet
  Flag via TTL, neuer Task kann enqueuen; bei t+120 versucht erster
  Task seinen Flag zu releasen — Owner-Check schlägt fehl, DEL wird
  übersprungen, Flag des zweiten Tasks bleibt intakt.
- Retry-Überleben-Test: Task failed + `unique_for=60, max_retries=3,
  retry_base_delay=5` → während Retry-Backoff bleibt Flag gesetzt;
  zweiter `schedule()` mit gleichen Args binnen 60s + (retry-delays
  aufsummiert) gibt dieselbe task_id zurück.

**`unique_until="start"` (Opt-in):**
- Flag wird vom Worker bei Message-Consume gelöscht (vor
  Task-Execution) via Owner-Check-Lua im `start`-Release-Pfad.
- Danach kann Duplikat enqueued werden, läuft parallel zum ersten.
- Retry-Republish: neuer SETNX wird erneut gesetzt (weil Flag nach
  erstem Pickup weg war) — Retry-Backoff-Fenster ist dedup-lückig
  (dokumentierte Einschränkung).

**Scheduler-Integration:**
- Cron `*/1 * * * *` + Task dauert 90s + `unique_for=120,
  unique_until=completion` → zweite Fire bei t+60 wird geskipped;
  `duplicate_detected`-Signal mit `source="scheduler.fire"` emittiert.
  Erste Fire läuft zu Ende (Flag-Release via Worker-Terminal), dritte
  Fire bei t+180 wird normal publiziert.
- Scheduler-`schedfire`-Layer bleibt unverändert funktional
  (Leader-Failover-Idempotenz), beide Layer koexistieren.

**Deferred-Pfad (delay/eta):**
- `schedule("t", unique_for=300, delay=60)` → SETNX in
  `_schedule_deferred` mit TTL=360 (unique_for + delay) **vor**
  ScheduleStore-Insert.
- Zweiter `schedule("t", unique_for=300, delay=60)` binnen 300s →
  silent dedup, gibt erste task_id zurück. Kein doppelter
  ScheduleStore-Eintrag.

**Cancel-Integration:**
- Default (`completion`): `schedule(unique_for=300)` → task running
  → `client.cancel()` → Worker cancels → Flag via
  `unique_release.lua` explizit gelöscht → neue `schedule()` sofort
  akzeptiert.
- `start`-Opt-in: `schedule(unique_for=300, unique_until="start")`
  → task running → `client.cancel()` → Worker cancels. Flag war
  schon bei Pickup weg; neue `schedule()` mit gleichen Args ist
  bereits freigegeben wenn auch TTL abgelaufen ist (sonst hält Flag
  bis TTL-Expiry).

**Batch:**
- `schedule_many([(t, (), {}), (t, (), {})])` mit `unique_for=60` →
  Liste `[task_id_1, task_id_1]` (zweite Position = existing id).
  Zwei Signals emittiert (oder einer? — Plan-Detail: vermutlich
  einer pro Duplikat, also einer hier).
- Gemischte Liste (1 dup + 2 frische) → Liste mit 3 task_ids, davon
  eine wiederholt; ein Signal für den dup.

**Chaos / Edge Cases:**
- Worker crasht nach SETNX vor Publish → TTL-Ablauf-Recovery
  + Client-seitiger best-effort-DEL bei `BrokerConnectionError`.
- Args enthalten unserialisierbare Typen → `ResultSerializationError`
  bereits heute (kein Regression).
- `TaskInfo(unique_for=0)` → `ValueError` in `__post_init__`
  (Fail-fast, nicht Silent-Normalisierung).
- `TaskInfo(unique_until="start")` ohne `unique_for` → `unique_until`
  wird ignoriert (Dedup ist aus); dokumentiert, kein ValueError
  (um Sentinel-Diskriminierung zu vermeiden). Im Zweifel
  Integration-Test: Dedup-Flag wird nicht gesetzt.
- Per-call `unique_for=None` overridet TaskInfo.unique_for=60 →
  dieser Call nicht dedupt (kein Signal, kein SETNX).
- `TaskInfo(lock="report", max_concurrency=1, unique_for=60,
  unique_until="completion")`: Dedup greift am Enqueue, Lock am
  Execute. Kombinierter Test: zwei schedule()-Calls binnen 60s →
  silent dedup (Signal); danach dritter schedule() außerhalb
  Dedup-Fenster, aber Lock hält erste Execution fest → dritter Task
  wartet auf Semaphore/DQ. Beide Features funktionieren orthogonal.

## Success Criteria

- **User-Outcome:** Submit-Button-Guard in ≤ 3 LOC implementierbar
  (nur `unique_for=60` als kwarg); Webhook-Idempotenz-Pattern ebenso
  ohne eigene SETNX-Logik.
- **Zwei primäre Use-Cases** in E2E-Tests idiomatisch
  implementierbar: Submit-Doppelklick, Webhook-Idempotenz.
- **Ein Integrations-Szenario** (nicht primärer Use-Case):
  `Locking + Unique` kombiniert funktioniert ohne Konflikte —
  als Referenz-Cookbook-Beispiel.
- Peer-parity-Check: SwarmQ bietet Pre-enqueue-Middleware-Hook
  (nicht Signal-Handler) als Raise-Opt-in — konsistent mit
  Dramatiq-Middleware-Pattern.
- Keine Regression bestehender Scheduler-, Cancel-, Retry-, Locking-
  Tests.
- Keine neue stuck-state bei Worker-Crash (TTL-Backstop + Client-
  cleanup wirken).

## Dependencies

- **R3.6 Task Cancellation** — ✅ gemerged. Cancel-Terminalisierung
  muss Dedup-Flag-Release triggern.
- **R3.8/R3.9 Scheduler** — ✅ gemerged. Scheduler-Fire-Path (sowohl
  Cron als auch Delay/ETA) muss pre-publish Dedup-Check bekommen.
- **R1.4 Serialization** — bereits vorhanden; wird für Hash-Input
  wiederverwendet.

## Non-Dependencies

- R3.7 Progress Tracking ist unabhängig.
- R3.10 DLQ-CLI ist unabhängig.

## Locking vs Unique — Entscheidungs-Leitfaden

Zwei komplementäre Features, oft verwechselt:

| Dimension              | Unique (R3.5)                          | Locking (R3.4)                         |
|------------------------|-----------------------------------------|----------------------------------------|
| Wann greift            | Vor Enqueue (Client / Scheduler)        | Während Execution (Worker-Acquire)     |
| Was verhindert         | Doppelten _Enqueue_ innerhalb TTL       | Parallele _Execution_ desselben Locks  |
| Key                    | `sha256(task_name + args + kwargs)`     | User-definierter Lock-Name             |
| Lifetime               | TTL-basiert                             | Task-Laufzeit + Renewal                |
| Failure-Mode           | Duplikat geht durch nach TTL            | Lock blockiert bis Acquire gelingt     |

**Anwendungsmuster:**

- **Submit-Burst-Schutz (kurzes Fenster):** `unique_for=30,
  unique_until="start"` — minimale Semantik, Flag bei Pickup weg.
  Geeignet wenn Parallel-Runs unkritisch sind.
- **Webhook-Idempotenz (mittleres Fenster, Retry-sicher):**
  `unique_for=600` (Default `completion`) — Flag überlebt
  SwarmQ-interne Retries, externe Re-Delivery im Retry-Fenster
  wird deduppt.
- **Parallel-Run-Schutz bei Execution:** `lock="report:{user_id}",
  max_concurrency=1` — mutex an der Execution-Stelle, unabhängig
  von Enqueue-Semantik.
- **Kombiniert (Report-Use-Case):** Dedup am Enqueue (Burst-
  Guard oder Webhook-Idempotenz) plus Locking am Execute
  (Parallel-Mutex). `unique_for=30,
  lock="report:{user_id}", max_concurrency=1`. Dedup catcht
  gleichzeitige Submissions derselben Args; Locking catcht den
  Race falls different-args-Submissions (z. B. Submit + Cron) den
  gleichen Lock-Key erzeugen.
