---
title: Nach einem Valkey-Datenverlust sind die Marker des Ausführungsschutzes weg; eine Neuzustellung kann erneut laufen, swarmq meldet den Verlust nur (valkey_data_loss_detected)
applies_when:
  - Valkey läuft ohne Persistenz oder verliert Daten seit dem letzten RDB-Snapshot (Neustart, Failover ohne Replikat-Stand)
  - Änderungen am Ausführungsschutz (worker/gates/execution_guard.py), am Epoch-Key oder an valkey_data_loss_detected
  - eine Fehlersuche meldet eine Doppelausführung nach einem Valkey-Neustart ohne Persistenz
tags: [execution-guard, valkey, persistenz, datenverlust, at-least-once]
id: E-077
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, offene Registerfrage aus 2b/2c, Vor dem Fix Punkt 16)
  - website/explanation/architecture.md:217 (Failure domains, Zeile Valkey restart)
  - Plan 0.12.0, MR 14, BZ-04 (Epoch des Datenbestands, tests/unit/scheduler/test_epoch.py)
tests:
  - tests/unit/scheduler/test_epoch.py::test_report_tick_notices_a_vanished_epoch
  - tests/unit/worker/gates/test_execution_guard.py::test_lost_valkey_data_lets_a_completed_execution_run_again
---

**Entscheidung.** Die Marker des Ausführungsschutzes (Running, Done, Superseded) liegen in Valkey. `architecture.md:217` sagt: „Without persistence, or since the last RDB snapshot: delayed tasks, CLI crons, disabled crons, workflow state, results and the execution-guard and fired markers are lost; redeliveries can run twice“, Recovery: „each worker logs `valkey_data_loss_detected` when the epoch key is gone. Run Valkey as in Valkey requirements“. swarmq rekonstruiert verlorene Marker nicht und hält den Konsum nicht an; es erkennt den Verlust am verschwundenen Epoch-Key und meldet je Worker eine WARNING `valkey_data_loss_detected`. Mit AOF (`appendonly yes`) verfallen nur die kurzlebigen Schlüssel (Locks, Rate-Zähler, Leader-Lock).

**Begründung.** Der Ausführungsschutz ist eine Optimierung über at-least-once, keine zweite Quelle der Wahrheit; eine Rekonstruktion bräuchte einen zweiten dauerhaften Speicher. Persistenz ist eine Betriebsanforderung (Valkey requirements in `configuration.md`).

**Bewusste Folgen.** Nach einem Valkey-Neustart ohne Persistenz kann eine Neuzustellung einer schon erledigten Ausführung erneut laufen; das ist kein Fehler gegen E-009 oder E-043.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
