---
title: Das Cancel-Flag lebt bis Fälligkeit + max_age + Puffer (mindestens 24 h) und wird nach Wirkung auf höchstens 1 h verkürzt, nicht gelöscht
applies_when:
  - cancel() für einen verzögerten oder laufenden Task
  - dieselbe task_id wird nach einem cancel() erneut eingereiht
  - Änderungen an client/results.cancel, clear_cancel_flag, cancellation.py oder am Cancel-Gate
  - eine Fehlersuche meldet, dass ein neu eingereihter Task mit gleicher task_id storniert wird oder cancel() False liefert
  - Änderungen an der Lebensdauer des Workflow-Cancel-Flags (sq:wf:cancel, client/workflows.py, SwarmQ.workflow, apply)
  - eine Fehlersuche meldet, dass ein stornierter Workflow nach Ablauf des Flags doch Glieder ausführt
tags: [cancel, ttl, client, bz-23]
id: E-020
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:137 (Entscheidungen, BZ-23)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:60 (Abschnitt 1, kein task_name in cancel())
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:64 (Abschnitt 1, BZ-23 Rev. 3 F11: PEXPIRE LT statt Löschen)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:724 (Abschnitt 7, sofort publizierter Task 24 h stornierbar)
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-5, Option a; F4-03)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (FEHLER F4-03, ENTSCHEIDUNG R4M-E-01b, LÜCKE PF-5)
tests:
  - tests/unit/test_app.py::TestSwarmQCancel::test_cancel_delegates_to_client_with_computed_ttl
  - tests/unit/test_app.py::TestSwarmQCancel::test_cancel_ttl_follows_result_ttl_above_24h
  - tests/unit/client/test_results.py::test_cancel_of_deferred_task_adds_known_max_age_plus_buffer
  - tests/unit/client/test_results.py::test_cancel_of_deferred_task_outlives_its_fire_time
  - tests/unit/backend/test_valkey.py::TestValkeyBackendClearCancelFlag::test_clear_cancel_flag_shortens_to_one_hour
  - tests/integration/test_worker_cancellation.py::TestRunningCancelIntegration::test_running_task_cancel_via_monitor
  - tests/e2e/test_befund_bz02_bz11_bz23_scheduler.py::test_bz23_cancelled_task_waiting_longer_than_result_ttl_does_not_run
  - tests/unit/client/test_befund_f4_03_reattach_cancel_ttl.py::test_reattached_handle_sets_the_cancel_flag_as_long_as_the_apply_handle
---

**Entscheidung.** Das Cancel-Flag lebt `max(24 h, result_ttl)` + (Fälligkeit − jetzt) + `max_age + 300` s (falls bekannt). `cancel()` bekommt keinen `task_name`. Bei der stornierten Zustellung und beim Abbruch während des Laufs wird das Flag nicht gelöscht, sondern per `PEXPIRE … LT` auf höchstens 1 h verkürzt (LT verlängert ein kürzeres Flag nie).

**Begründung.** Ein Flag mit fest 24 h ließ länger verzögerte, stornierte Tasks doch laufen (BZ-23). Das Löschen nach Wirkung hätte at-least-once-Duplikate (BZ-19) der stornierten Nachricht doch laufen lassen.

**Bewusste Folgen.** Wird dieselbe `task_id` innerhalb 1 h nach Wirkung neu eingereiht, wird sie storniert, und `cancel()` liefert dann False. Ein sofort publizierter Task ist 24 h stornierbar.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; PF-5, F4-03, R4M-E-01b): Workflow-Cancel-Flag.** Die Regel gilt auch für das Workflow-Cancel-Flag (`sq:wf:cancel:{id}`): Es lebt `max(24 h, result_ttl)` wie die Task-Flags, gleich ob das Handle aus `apply()` stammt oder per `app.workflow(id)` neu gebaut ist. Das überholt KTD „TTL = `config.result_ttl`“ aus Plan 2026-05-30-001 `:104`. Beide Varianten von R4M-E-01 fallen damit in einen Fix: `apply()` mit `result_ttl` unter 24 h setzt das Flag auf 24 h (heute genau `result_ttl`), das neu gebaute Handle auf `max(24 h, result_ttl)` (heute fest 24 h, F4-03). Festgeschrieben bis zum Fix mit dem Nachweis F4-03 (`xfail(strict=True)`, Marker E-020); der Test für `result_ttl` unter 24 h folgt mit dem Fix. Der Teil „`get()` meldet den Abbruch nicht mehr“ heilt erst mit FS-32 (Zustand `cancelled`).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
