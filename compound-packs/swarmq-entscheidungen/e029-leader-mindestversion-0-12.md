---
title: Ein Leader älter als 0.12 gilt als veraltet und wird einmal je Leaderwechsel gewarnt; die Gauge bleibt stehen, solange er führt
applies_when:
  - Mischbetrieb mit Apps < 0.12 auf demselben Valkey
  - Änderungen an scheduler/leader_version.py oder der Gauge für veraltete Leader
  - eine Fehlersuche meldet eine dauerhaft gesetzte Gauge „Leader veraltet“
  - Änderungen am Versionsschlüssel der Worker (sq:{sched}:worker_version, report_worker_version, worker_versions) oder an schedule status WORKERS
tags: [scheduler, leader, version, mischbetrieb, ktd11]
id: E-029
datum: 2026-10-09
status: gilt
quelle:
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:61 (Abschnitt 1, KTD11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:334 (MR 5, KTD11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:729 (Abschnitt 7, KTD11)
  - Georg 2026-10-17 (Fehlersuche Runde 4, F4-06, Code-Fix)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (FEHLER F4-06; Vor dem Fix Punkt 5)
tests:
  - tests/unit/scheduler/test_leader_version.py::TestLeaderVersionWatch::test_leader_below_minimum_version_is_reported_once
  - tests/unit/scheduler/test_leader_version.py::TestIsOutdated::test_floor_version_is_current
  - tests/unit/scheduler/test_loop.py::TestLeaderVersion::test_outdated_leader_is_reported_once_per_change
  - tests/unit/test_metrics.py::test_leader_outdated_signal_sets_and_clears_gauge
  - tests/e2e/test_befund_f4_06_schedule_status_workers_same_host.py::test_schedule_status_lists_the_workers_of_two_supervisors_on_one_host
---

**Entscheidung.** Die Mindestversion des Leaders ist 0.12 (`MINIMUM_LEADER_VERSION`). Ein Leader ohne oder mit älterer Version gilt als veraltet: WARNING `scheduler_leader_outdated` und Signal einmal je Leaderwechsel, Gauge auf 1, bis ein aktueller Leader gesehen wird oder dieser Worker selbst Leader wird. Eine unlesbare Version gilt nicht als veraltet.

**Begründung.** BZ-02, BZ-09 und BZ-11 wirken nur mit einem Leader ≥ 0.12; rotiert die Führung zu einer 0.11-App, sind sie still aus. Das muss sichtbar sein.

**Bewusste Folgen.** Die Gauge steht dauerhaft, solange eine 0.11-App auf demselben Valkey führt; das ist kein Fehlalarm.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; F4-06): Versionsmeldung je Prozess.** Die Versionsmeldung der Worker bekommt einen prozesseindeutigen Schlüssel (Owner-Token oder `:p<pid>` des Kindes, wie im Kontrollkanal); die Anzeige in `schedule status` bleibt `<worker_identity>`. Die alten Schlüssel (`sq:{sched}:worker_version:<worker_identity>`, 0.11/0.12) werden für den Mischbetrieb weiter gelesen. Damit erscheinen mehrere Supervisoren eines Hosts ohne eigenes `SWARMQ_WORKER_ID` je mit ihrer Version (`cli.md:377-380`). `SWARMQ_WORKER_ID` je Supervisor eindeutig zu verlangen (Doku-Variante) ist verworfen. Die Leader-Warnung dieser Regel bleibt unverändert. Festgeschrieben bis zum Fix mit dem Nachweis F4-06 (e2e, `xfail(strict=True)`, Marker E-029).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
