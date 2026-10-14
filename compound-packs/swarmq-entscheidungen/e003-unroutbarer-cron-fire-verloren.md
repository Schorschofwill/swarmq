---
title: Ein unroutbarer Cron-Fire gilt als verloren, wird festgeschrieben und gezählt
applies_when:
  - der Leader feuert einen Cron, dessen Ziel-Queue nicht existiert
  - Änderungen an scheduler/fire.py oder scheduler/unroutable.py
  - eine Fehlersuche meldet, dass ein Cron-Lauf bei unroutbarem Publish nicht wiederholt wird
  - eine Fehlersuche meldet, dass das Log scheduler_fire_unroutable nur einmal je 10 min erscheint
tags: [scheduler, cron, unroutable, bz-03]
id: E-003
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:124 (Entscheidungen, BZ-03)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:55 (Abschnitt 1, BZ-03 Oneshot/Cron)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:347 (MR 6, Leader Cron)
tests:
  - tests/unit/scheduler/test_fire_unroutable.py::test_unroutable_cron_fire_is_committed_to_the_next_occurrence
  - tests/unit/scheduler/test_fire_unroutable.py::test_unroutable_cron_warning_is_throttled_per_cron_and_ten_minutes
  - tests/unit/scheduler/test_fire_unroutable.py::test_unroutable_cron_reported_by_another_worker_logs_foreign_hint
  - tests/unit/test_metrics.py::test_fire_unroutable_signal_increments_unroutable_counter
---

**Entscheidung.** Ist ein Cron-Fire unroutbar, gibt der Leader einen in diesem Fire beanspruchten Unique-Claim frei, schreibt den Fire auf den nächsten Termin fest und zählt ihn als verloren (Signal `SCHEDULER_FIRE_UNROUTABLE` mit `kind="cron_lost"`, Zähler). Das Log `scheduler_fire_unroutable` ist auf einmal je Cron und 10 min gedrosselt; meldet ein fremder Worker den Cron, kommt `scheduler_fire_unroutable_foreign` dazu.

**Begründung.** Ein Cron hat einen nächsten Termin; ein Nachholen würde den Poll blockieren und Läufe stauen. Verlust wird sichtbar gemacht statt still hingenommen (BZ-03).

**Bewusste Folgen.** Der betroffene Cron-Lauf fehlt endgültig; das ist kein Fehler, solange er gezählt wird. Das Signal kommt bei jedem Fire, auch wenn das Log gedrosselt ist.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
