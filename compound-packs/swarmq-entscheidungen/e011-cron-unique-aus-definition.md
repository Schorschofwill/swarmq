---
title: unique_for/unique_until eines Crons reisen mit der Definition; der Leader nutzt sie statt seiner Registry
applies_when:
  - ein Cron mit unique_for wird von einem Leader gefeuert, der den Task nicht oder anders registriert hat
  - Änderungen am Cron-Report, ScheduleDefinition-Fingerabdruck oder an der Unique-Freigabe in scheduler/fire.py
  - eine Fehlersuche meldet, dass die Unique-Sperre bei PublishIndeterminate oder bei fremdem cli_unique-Flag nicht freigegeben wird
tags: [scheduler, cron, unique, bz-09]
id: E-011
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:129 (Entscheidungen, BZ-09)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:410 (MR 9)
tests:
  - tests/unit/scheduler/test_fire.py::test_old_definition_takes_both_unique_values_from_the_registry
  - tests/unit/scheduler/test_fire.py::test_definition_with_unique_for_beats_the_registry
  - tests/unit/scheduler/test_fire.py::test_publish_failure_releases_the_own_claim_so_the_next_poll_fires
  - tests/unit/scheduler/test_fire.py::test_indeterminate_publish_keeps_the_claim
  - tests/unit/scheduler/test_fire.py::test_publish_failure_keeps_a_cli_unique_claim_of_the_producer
  - tests/unit/scheduler/test_reports.py::TestFingerprint::test_fingerprint_without_unique_for_is_unchanged_since_0_11
  - tests/unit/scheduler/test_befund_bz02_bz07_bz09_bz28_scheduler.py::test_bz09_cron_unique_for_holds_on_leader_without_task_in_registry
---

**Entscheidung.** Cron-Report und `ScheduleDefinition` führen `unique_for` und `unique_until` mit. Ist `definition.unique_for` gesetzt, kommen beide Werte aus der Definition, sonst beide aus der Registry des Leaders (nie gemischt). Der Fingerabdruck enthält sie nur bei gesetztem `unique_for` (sonst byte-gleich zu 0.11). Bei einem Publish-Fehler gibt der Leader die Unique-Sperre nur frei, wenn er sie in genau diesem Fire beansprucht hat; nicht bei fremden `cli_unique`-Flags und nicht bei `PublishIndeterminate`.

**Begründung.** Vorher hing `unique_for` davon ab, welcher Worker gerade Leader war; mit anderer App oder anderem `--module` überlappten lange Cron-Läufe (BZ-09). Bei `PublishIndeterminate` kann der Lauf schon unterwegs sein.

**Bewusste Folgen.** Nach `PublishIndeterminate` feuert der nächste Poll den Cron nicht, bis die Sperre abläuft. Wirkt erst mit einem Leader ≥ 0.12 (Plan Abschnitt 7).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
