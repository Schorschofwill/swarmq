---
title: Der Leader hält einen Cron zurück, den kein lebender Worker meldet und den er nicht kennt, statt ihn zu feuern oder zu verwerfen
applies_when:
  - ein statischer Cron wurde aus dem Code entfernt, seine Definition liegt noch in Valkey
  - Änderungen an scheduler/unowned.py, sq:{sched}:report:known oder am Prune
  - eine Fehlersuche meldet, dass ein Cron nach Leaderwechsel nicht feuert (scheduler_cron_held_unowned)
  - eine Fehlersuche meldet, dass zurückgehaltene Crons nicht gelöscht werden
tags: [scheduler, cron, verwaist, bz-11]
id: E-014
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:131 (Entscheidungen, BZ-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:48 (Abschnitt 1, BZ-11 zurückhalten, nicht verwerfen)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:437 (MR 11, BZ-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:670 (Abschnitt 6, MR 11: terminaler Pfad für unbekannte Tasks ist dead_letter, Georg 2026-10-10)
tests:
  - tests/unit/scheduler/test_unowned.py::test_unowned_cron_is_held_instead_of_fired
  - tests/unit/scheduler/test_unowned.py::test_hold_logs_and_signals_only_on_the_transition
  - tests/unit/scheduler/test_unowned.py::test_poll_fails_open_five_seconds_after_the_takeover
  - tests/unit/scheduler/test_unowned.py::test_cron_never_reported_fires
  - tests/unit/test_metrics.py::test_scheduler_crons_held_gauge_follows_the_unowned_signal
  - tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py::test_bz11_removed_cron_once_reported_is_held_by_the_leader
  - tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py::test_bz11_fire_of_removed_cron_task_dead_letters_with_ttl
---

**Entscheidung.** Ein Cron gilt als verwaist, wenn alle fünf Bedingungen gelten: Meldungen in der aktuellen Amtszeit geladen, Name in `sq:{sched}:report:known`, nicht lebend gemeldet, kein eigener statischer Cron, Definition mit `source == "static"`. Er wird zurückgehalten (Commit auf `now + seen_interval`), nicht verworfen. Log `scheduler_cron_held_unowned` und Signal `SCHEDULER_CRON_UNOWNED` nur beim Übergang, Gauge `swarmq_scheduler_crons_held`. Nach der Übernahme wartet der Poll höchstens 5 s auf das erste Adopt, dann fail-open. Ein Fire eines unbekannten Tasks landet mit Ablaufzeit in der XQ.

**Begründung.** Ein entfernter Cron feuerte bis zum Prune 24 h weiter, jeder Fire landete ohne TTL in der XQ (BZ-11). Verwerfen wäre bei einem vorübergehend fehlenden Worker (Deploy) Datenverlust.

**Bewusste Folgen.** Ein Cron, der nur kurz nicht gemeldet ist, fällt für diese Zeit aus. Ein nie gemeldeter Cron (nicht in `known`) feuert weiter. Wirkt erst mit Leader ≥ 0.12. Die Ablaufzeit entsteht über die XQ-Kopie von `dead_letter` (`expiration = dead_letter_ttl_s`); der Rückfall `route_via_dlx` setzt selbst keine Ablaufzeit (freigegebene Planänderung, Abschnitt 6).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
