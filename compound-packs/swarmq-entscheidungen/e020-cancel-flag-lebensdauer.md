---
title: Das Cancel-Flag lebt bis Fälligkeit + max_age + Puffer (mindestens 24 h) und wird nach Wirkung auf höchstens 1 h verkürzt, nicht gelöscht
applies_when:
  - cancel() für einen verzögerten oder laufenden Task
  - dieselbe task_id wird nach einem cancel() erneut eingereiht
  - Änderungen an client/results.cancel, clear_cancel_flag, cancellation.py oder am Cancel-Gate
  - eine Fehlersuche meldet, dass ein neu eingereihter Task mit gleicher task_id storniert wird oder cancel() False liefert
tags: [cancel, ttl, client, bz-23]
id: E-020
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:137 (Entscheidungen, BZ-23)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:60 (Abschnitt 1, kein task_name in cancel())
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:64 (Abschnitt 1, BZ-23 Rev. 3 F11: PEXPIRE LT statt Löschen)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:724 (Abschnitt 7, sofort publizierter Task 24 h stornierbar)
tests:
  - tests/unit/test_app.py::TestSwarmQCancel::test_cancel_delegates_to_client_with_computed_ttl
  - tests/unit/test_app.py::TestSwarmQCancel::test_cancel_ttl_follows_result_ttl_above_24h
  - tests/unit/client/test_results.py::test_cancel_of_deferred_task_adds_known_max_age_plus_buffer
  - tests/unit/client/test_results.py::test_cancel_of_deferred_task_outlives_its_fire_time
  - tests/unit/backend/test_valkey.py::TestValkeyBackendClearCancelFlag::test_clear_cancel_flag_shortens_to_one_hour
  - tests/integration/test_worker_cancellation.py::TestRunningCancelIntegration::test_running_task_cancel_via_monitor
  - tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py::test_bz23_cancelled_task_waiting_longer_than_result_ttl_does_not_run
---

**Entscheidung.** Das Cancel-Flag lebt `max(24 h, result_ttl)` + (Fälligkeit − jetzt) + `max_age + 300` s (falls bekannt). `cancel()` bekommt keinen `task_name`. Bei der stornierten Zustellung und beim Abbruch während des Laufs wird das Flag nicht gelöscht, sondern per `PEXPIRE … LT` auf höchstens 1 h verkürzt (LT verlängert ein kürzeres Flag nie).

**Begründung.** Ein Flag mit fest 24 h ließ länger verzögerte, stornierte Tasks doch laufen (BZ-23). Das Löschen nach Wirkung hätte at-least-once-Duplikate (BZ-19) der stornierten Nachricht doch laufen lassen.

**Bewusste Folgen.** Wird dieselbe `task_id` innerhalb 1 h nach Wirkung neu eingereiht, wird sie storniert, und `cancel()` liefert dann False. Ein sofort publizierter Task ist 24 h stornierbar.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
