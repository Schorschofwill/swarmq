---
title: "fix: cancel-safe Worker-Teardown + cron-sync-clobber → e2e ohne Reruns"
type: fix
status: completed
date: 2026-06-01
origin: Sitzungs-Audit 2026-06-01 (e2e-Flakiness-Wurzel, empirisch verifiziert)
---

# fix: cancel-safe Worker-Teardown + cron-sync-clobber

## Summary

Audit + **empirische Verifikation** (2026-06-01) ergaben **zwei getrennte,
je bestätigte** Ursachen — NICHT eine gemeinsame. (Korrektur einer früheren
Annahme, die den Orphan fälschlich als Wurzel der Flakiness bezeichnete.)

1. **Cron-sync-clobber — die tatsächliche Wurzel der e2e-Cron/Leader-Flakes.**
   Tests injizieren manuelle „Fires" per `zadd("sq:sched:zset", {task_name: due})`
   direkt nach `leader_event`. Beim **ersten** Leader-Werden ruft
   `SchedulerLoop.sync_taskinfo_crons()` (läuft im election-tick NACH dem
   `SCHEDULER_LEADER`-Emit, `scheduler.py:_election_tick`) für eine noch nicht
   existierende Definition `add_schedule(...)` → ZADD mit dem **nächsten
   Cron-Minuten-Score**. Unter Last landet dieses ZADD **nach** dem manuellen
   zadd → der fällige Score wird auf die Minutengrenze überschrieben → der
   Eintrag wird im Test-Timeout nie fällig → `done`/`first_fired`-Timeout.

2. **Verwaiste Scheduler- (und Broker-) Tasks beim Cancel — Leak + Noise +
   Zombie.** `Worker.start()` (`worker.py:740`) hat kein umschließendes
   `try/finally`; es startet den Scheduler (`_election_task`/`_poll_task` als
   detachte `create_task`s, `scheduler.py:511-513`) und blockiert auf
   `await self._shutdown_event.wait()`. Beim Cancel der `start()`-Coroutine
   (Tests, ProcessManager/`--reload`-execvp, eingebettete Nutzung) propagiert
   `CancelledError` raus → `_execute_shutdown()` (graceful `scheduler.stop()`)
   wird übersprungen → die Tasks leaken.

## Evidence (verifiziert, nicht hergeleitet)

- **Orphan (empirisch):** Nach `start_worker()`-Cancel + await blieben
  `SchedulerLoop._election_loop`, `._poll_loop` UND aio_pika
  `Connection.__reader/__writer/__heartbeat`, `Channel._reader` in
  `asyncio.all_tasks()` PENDING (8 offene Tasks). → Quelle der
  `"Future exception was never retrieved: CancelledError"`-Records.
- **Clobber (empirisch):** Single-App-Worker, realer `poll_interval=0.1`, unter
  6 CPU-Busy-Loops, 15 Läufe: 2 FIRE-TIMEOUTs, beide mit `zscore` exakt auf
  einer Minutengrenze (`…800000`), 9–35 s in der Zukunft → der fällige Score
  WURDE auf den nächsten Cron-Slot überschrieben. (Ein Lauf feuerte erst nach
  10.75 s, weil die echte Minutengrenze nah war — derselbe Mechanismus.)
- **Produkt NICHT betroffen:** `sync_taskinfo_crons` ist bei Folge-Leaderwechseln
  ein No-op (existierende `source="static"`-Def, gleicher Cron → Branch 5).
  Der Clobber tritt nur bei der **ersten** Leadership auf (Def fehlt →
  `add_schedule`), was im Normalbetrieb harmlos ist und nur die manuell
  injizierenden Tests trifft.
- **Fix-Prinzip bereits bewiesen:** `test_no_fires_missed_when_leader_changes`
  nutzt seit `550d40a` `_wait_zscore_present` (auf Sync warten → manueller zadd
  ist letzter Schreiber) → 0/50 unter Last. Der Long-Tail-Fix generalisiert das.

## Key Technical Decisions

- **Die zwei Themen sind unabhängig.** Der Clobber-Fix (Thema 1) ist das, was
  die Reruns überflüssig macht — NICHT der Orphan-Fix. Beide werden gemacht,
  aber nicht verwechselt.
- **Clobber-Fix zentral im Test-Helper.** Manuelle Cron-Fire-Injektion muss
  garantiert letzter Schreiber sein: erst auf die initiale Scheduler-Sync
  warten (Def/zset-Eintrag vorhanden), dann injizieren — als gemeinsamer Helper
  (`_inject_cron_fires` und Geschwister), nicht pro Test dupliziert.
- **Orphan-Fix: graceful vs. abort trennen.** `_execute_shutdown` bleibt der
  graceful Pfad. Der Cancel-Pfad braucht einen `Scheduler.abort()` (Tasks
  cancel+await, KEINE graceful Valkey-Release/Handover — ein Crash macht keinen
  Handover) und ein `try/finally` in `Worker.start()`, das idempotent abort +
  Broker/Backend-Close + Signal-Handler-Entfernung garantiert, `asyncio.shield`-
  umgeben (Muster: `_detached_releases`, `worker.py:1001-1011`).
- **Reruns zuletzt entfernen.** Erst nachdem die volle e2e-Suite über mehrere
  Last-Läufe stabil ist (kein erneutes „zero-rerun reddened main").

## Units (TDD — RED → GREEN → REFACTOR)

- **U1 — Clobber-Wurzel: last-writer-Cron-Injektion.**
  `tests/e2e/combinations/test_timing_order.py::_inject_cron_fires` (und jede
  weitere Stelle mit manuellem `zadd` auf `sq:sched:zset` nach `leader_event`):
  vor dem Injizieren auf den Scheduler-Write warten (Eintrag vorhanden →
  initiale Sync gelaufen), dann als letzter Schreiber injizieren; fällige
  Vergangenheits-Scores. Verifikation: `TestCronCatchupUniqueCombo` + die
  übrigen Treffer ≥50 Läufe unter 6-Hog-Last → 0 Fehler.

- **U2 — Audit aller manuellen Cron-Zadds.** `grep` nach `sq:sched:zset`-zadds in
  `tests/e2e/**`; jede Stelle auf das last-writer-Muster ziehen oder begründen,
  warum unkritisch. Liste im PR.

- **U3 — Scheduler `abort()`** (`scheduler.py`): cancelt+awaitet die Sub-Tasks
  ohne Emit/Marker/Release; idempotent. Unit-Test: nach `start()`+`abort()`
  beide Tasks `done()`, Lock NICHT graceful released (Abgrenzung zu `stop()`).

- **U4 — `Worker.start()` cancel-safe** (`worker.py`): Post-Connect-Body in
  `try/finally`; `finally` → idempotenter `_teardown_on_exit()` (Burst-Monitor
  canceln, `scheduler.abort()` falls `_execute_shutdown` nicht lief,
  Signal-Handler entfernen, Broker/Backend schließen), `shield`-umgeben.
  Unit-Test (Fakes): `start()` als Task, `WORKER_ONLINE` abwarten, canceln →
  Scheduler-Tasks `done()`, KEIN `"Future exception"`-Record im Log-Capture.

- **U5 — Reruns entfernen + verifizieren.** Volle `tests/e2e` über mehrere
  6-Hog-Last-Läufe stabil → `--reruns 2 --reruns-delay 3` aus `.gitlab-ci.yml`
  entfernen, Kommentare aktualisieren, auf CI über ≥3 Läufe grün bestätigen.

- **U6 — Verifikation + Cleanup.** unit+e2e grün; mypy+ruff clean; `"Future
  exception"`-Noise weg; Plan-Status → completed; `/ce-compound`-Learnings:
  (a) cron-sync-clobber-Testmuster, (b) detached-task-Cancel-Teardown.

## Out of Scope

- Produktions-`SIGTERM`-Pfad (graceful, kein Orphan); sync_taskinfo_crons-
  Produktverhalten (No-op bei Folgewechseln — korrekt).
- Bulk-Throughput (`schedule_many` fast path) — separates Deferred-Feature.

## Risks

| Risiko | Mitigation |
|---|---|
| Weitere Long-Tail-Flakes mit ANDERER Ursache als Clobber | U5 erst nach mehreren stabilen Last-Läufen der vollen Suite; bei neuem Muster zuerst empirisch belegen (wie hier), nicht annehmen |
| `try/finally` bricht graceful Pfad / Partial-Start-Rollback | Idempotenz-Guard; Teardown no-op nach `_execute_shutdown`; Rollback-Tests grün halten |
| `shield` lässt Teardown detached statt zu vollenden | CancelledError im finally fangen, geshieldeten Teardown awaiten, dann re-raise |
| `_wait_zscore_present` deckt nicht den post-fire-Reschedule ab | Reschedule passiert NACH dem Fire (Eintrag bereits konsumiert) → unkritisch; in U1 mit Last-Läufen bestätigen |
