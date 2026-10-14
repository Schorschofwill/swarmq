---
title: Ein Leader älter als 0.12 gilt als veraltet und wird einmal je Leaderwechsel gewarnt; die Gauge bleibt stehen, solange er führt
applies_when:
  - Mischbetrieb mit Apps < 0.12 auf demselben Valkey
  - Änderungen an scheduler/leader_version.py oder der Gauge für veraltete Leader
  - eine Fehlersuche meldet eine dauerhaft gesetzte Gauge „Leader veraltet“
tags: [scheduler, leader, version, mischbetrieb, ktd11]
id: E-029
datum: 2026-10-09
status: gilt
quelle:
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:61 (Abschnitt 1, KTD11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:334 (MR 5, KTD11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:729 (Abschnitt 7, KTD11)
tests:
  - tests/unit/scheduler/test_leader_version.py::TestLeaderVersionWatch::test_leader_below_minimum_version_is_reported_once
  - tests/unit/scheduler/test_leader_version.py::TestIsOutdated::test_floor_version_is_current
  - tests/unit/scheduler/test_loop.py::TestLeaderVersion::test_outdated_leader_is_reported_once_per_change
  - tests/unit/test_metrics.py::test_leader_outdated_signal_sets_and_clears_gauge
---

**Entscheidung.** Die Mindestversion des Leaders ist 0.12 (`MINIMUM_LEADER_VERSION`). Ein Leader ohne oder mit älterer Version gilt als veraltet: WARNING `scheduler_leader_outdated` und Signal einmal je Leaderwechsel, Gauge auf 1, bis ein aktueller Leader gesehen wird oder dieser Worker selbst Leader wird. Eine unlesbare Version gilt nicht als veraltet.

**Begründung.** BZ-02, BZ-09 und BZ-11 wirken nur mit einem Leader ≥ 0.12; rotiert die Führung zu einer 0.11-App, sind sie still aus. Das muss sichtbar sein.

**Bewusste Folgen.** Die Gauge steht dauerhaft, solange eine 0.11-App auf demselben Valkey führt; das ist kein Fehlalarm.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
