---
title: Ein gemeldeter statischer Cron, den seit scheduler_prune_after_s kein Worker meldet, wird entfernt; nie gemeldete Crons nie automatisch
applies_when:
  - Änderungen am Prune des Leaders, an Worker-Meldungen (last_seen) oder an scheduler_prune/scheduler_prune_after_s
  - eine Fehlersuche meldet verschwundene Crons nach längerer Pause einer App oder bleibende Alt-Crons
  - ein Plan will verwaiste Crons anders erkennen (z. B. Owner-Markierung je App)
tags: [scheduler, cron, prune, multi-app]
id: E-049
datum: 2026-10-08
status: gilt
quelle:
  - docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md:88-89 (R11, R13), :110 (AE6), :230-236 (KTD8, user-directed)
tests:
  - tests/unit/scheduler/test_prune.py::TestPlanPrune::test_reported_cron_silent_longer_than_prune_after_is_stale
  - tests/unit/scheduler/test_prune.py::TestPlanPrune::test_never_reported_static_cron_is_an_orphan_not_stale
  - tests/unit/scheduler/test_prune.py::TestPlanPrune::test_cli_cron_is_never_pruned
  - tests/unit/scheduler/test_prune.py::TestLeaderPrunes::test_never_reported_cron_stays_and_is_logged_once_as_candidate
---

**Entscheidung.** Der Leader entfernt Definitionen mit `source == "static"`, die mindestens einmal von einem Worker gemeldet wurden und deren jüngste Meldung älter als `scheduler_prune_after_s` ist (Default 86400 s, mindestens 3 × `scheduler_seen_interval_s`). Nie gemeldete Definitionen (Altbestand, Apps auf älteren Versionen) entfernt er nie automatisch; er loggt sie als `scheduler_cron_prune_candidate`, `schedule prune --orphans` entfernt sie bewusst. CLI-Schedules werden nie gepruned. Ein beim Entfernen deaktivierter Cron kommt deaktiviert zurück. Schalter `scheduler_prune` (`off`, `log`, `on`, Default `on`).

**Begründung.** Aufräumen über Lebenszeichen statt Owner-Markierung je App: Eine Owner-Markierung könnte in Rolling Deploys neue Crons löschen. Apps auf älteren Versionen und länger pausierende Apps verlieren so keine Crons.

**Bewusste Folgen.** Ein statischer Cron einer App, die länger als `scheduler_prune_after_s` keinen Worker laufen hat, verschwindet; der nächste startende Worker legt ihn wieder an. Ein Alt-Cron, der nie gemeldet wurde, bleibt dauerhaft. Beides ist kein Fehler.

**Überdenken nur mit neuem Argument.** Das Zurückhalten nicht gemeldeter Crons vor dem Prune (BZ-11) ist eine eigene Entscheidung aus dem Plan 2026-10-09-2314; der Einwand Scale-to-zero (FS-11) betrifft jene.
