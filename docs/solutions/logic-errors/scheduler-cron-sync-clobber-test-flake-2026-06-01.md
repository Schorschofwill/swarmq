---
title: "E2E-Flake: Scheduler-Cron-Sync überschreibt manuell injizierte Fires (clobber)"
date: 2026-06-01
category: logic-errors
module: tests/e2e
problem_type: race_condition
component: scheduler
symptoms:
  - "Scheduler/Cron-e2e-Tests (test_no_fires_missed, TestCronSchedule, TestCronCatchupUniqueCombo) timen sporadisch auf done.wait()/first_fired.wait() aus — nur auf dem langsamen CI-Runner, lokal grün"
  - "Unter 6 CPU-Busy-Loops lokal reproduzierbar (~1/25–1/40); ohne Last 0/30"
  - "Bei Fehlläufen steht der sq:sched:zset-Score des Tasks exakt auf einer Cron-Minutengrenze (…800000), 9–35s in der Zukunft statt fällig"
root_cause: race_condition
resolution_type: test_fix
severity: medium
tags:
  - scheduler
  - cron
  - race-condition
  - flaky-test
  - leader-election
  - e2e
  - zadd
  - load-dependent
---

# E2E-Flake: Scheduler-Cron-Sync überschreibt manuell injizierte Fires

## Problem

Mehrere Scheduler-e2e-Tests injizieren einen „fälligen" Fire direkt per
`zadd("sq:sched:zset", {task_name: due_score})` **unmittelbar nach** dem
`SCHEDULER_LEADER`-Signal und warten dann darauf, dass der Task feuert. Auf dem
CI-Runner timten sie sporadisch aus; lokal waren sie grün. Das `--reruns 2`-
Pflaster maskierte es, machte das e2e-Gate aber unehrlich.

## Root cause (empirisch belegt, nicht angenommen)

`SchedulerLoop._election_tick` emittiert `SCHEDULER_LEADER` und ruft **erst
danach** `sync_taskinfo_crons()`. Bei der ersten Leadership (Definition fehlt)
ruft das `add_schedule(...)` → ZADD des Eintrags mit dem **nächsten
Cron-Minuten-Score**. Der Test sieht das Leader-Signal und zaddt seinen fälligen
Score — aber unter Last landet der Sync-ZADD **nach** dem Test-ZADD und
überschreibt den fälligen Score mit der Minutengrenze. Der Eintrag wird im
Test-Timeout nie fällig → Timeout.

**Beweis:** Single-App-Probe unter 6 CPU-Hogs, realer `poll_interval=0.1`: bei
Fehlläufen stand der zscore exakt auf `…800000` (Minutengrenze), 9–35s in der
Zukunft. Ein erster Probe-Versuch mit `poll_interval=10` zeigte den Clobber
NICHT (kein Fire/Reschedule-Zyklus) — die Reproduktion braucht reale Settings +
CPU-Last.

**Produkt NICHT betroffen:** Bei Folge-Leaderwechseln ist `sync_taskinfo_crons`
ein No-op (existierende `source="static"`-Def, gleicher Cron). Der Clobber tritt
nur bei der ERSTEN Leadership auf — also nur in Tests, die sofort danach manuell
injizieren.

## Solution

Die manuelle Injektion muss **letzter Schreiber** sein: erst warten, bis der
Scheduler den Eintrag geschrieben hat (`zscore(task_name) is not None` → Sync
gelaufen), dann mit fälligem (Vergangenheits-)Score überschreiben. Danach gibt
es keinen konkurrierenden Schreiber mehr (Sync läuft nur einmal pro Leadership;
der Post-Fire-Reschedule passiert erst nach dem konsumierten Fire).

Angewandt zentral in `_inject_cron_fires` und an allen manuellen
`sq:sched:zset`-zadds (`test_scheduler.py` TestCronSchedule + TestCronWithRateLimit
+ test_no_fires_missed via `_wait_zscore_present`).

## Prevention

- **Nie** in den Scheduler-zset injizieren, ohne auf dessen initiale Cron-Sync
  zu warten — sonst Race mit `add_schedule`.
- Timing-Flakes, die lokal nicht reproduzieren, unter **CPU-Last** (mehrere
  Busy-Loops) + realen Settings reproduzieren, bevor man die Ursache annimmt.
  Hier widerlegte die erste Probe die naheliegende Annahme; erst die Last-Probe
  mit dem Minuten-Grenz-Score lieferte den Beweis.

## Scope note

Test-only-Fix (Produktverhalten korrekt). Ermöglichte das Entfernen der
e2e-`--reruns` → ehrliches zero-rerun-Gate.

## References

- Plan: `docs/plans/2026-06-01-002-fix-scheduler-cancel-teardown-and-e2e-determinism-plan.md`
- `src/swarmq/scheduler.py::_election_tick` (Emit vor sync), `sync_taskinfo_crons`
- Verwandt: [worker-cancel-safe-teardown-detached-tasks-2026-06-01.md](../patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md)
