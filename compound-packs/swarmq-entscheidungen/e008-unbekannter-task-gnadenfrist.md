---
title: Unbekannte Task-Namen werden bis zu einer Gnadenfrist (900 s) zurückgestellt, erst dann DLQ
applies_when:
  - ein Worker erhält eine Nachricht mit einem Task-Namen, den er nicht registriert hat (Rolling Deploy, falsches Modul)
  - Änderungen an worker/gates/unknown_task.py oder an unknown_task_grace_s
  - eine Fehlersuche meldet, dass ein unbekannter Task nicht sofort in der DLQ landet, sondern kreist
  - eine Fehlersuche meldet, dass ein zurückgestellter Cron-Fire seinen Unique-Claim hält
tags: [worker, unbekannter-task, deploy, dlq, bz-06]
id: E-008
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:127 (Entscheidungen, BZ-06)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:59 (Abschnitt 1, BZ-06 Gnadenfrist 900 s, 0 schaltet ab)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:437 (MR 11, BZ-06)
tests:
  - tests/unit/worker/test_befund_bz06_unknown_task.py::test_first_delivery_of_an_unknown_task_is_deferred_not_dead_lettered
  - tests/unit/worker/gates/test_unknown_task_grace.py::test_expired_grace_dead_letters_with_result_and_rejected_signal
  - tests/unit/worker/gates/test_unknown_task_grace.py::test_deferred_cron_fire_keeps_claim_open_and_unique_flag_held
  - tests/unit/worker/gates/test_unknown_task_grace.py::test_grace_zero_dead_letters_at_first_contact
  - tests/unit/test_config_unknown_task_grace.py::test_default_is_fifteen_minutes
  - tests/unit/worker/test_finalize.py::test_start_mode_unknown_task_dlx_fallback_releases_the_flag
---

**Entscheidung.** Eine Nachricht mit unbekanntem Task-Namen wird mit Backoff (1 s bis 60 s, Header `x-swarmq-unknown-since`) zurückgestellt und als `Redelivered` behandelt; das Signal `TASK_UNKNOWN_DEFERRED` zählt die Rückstellungen. Erst nach der Gnadenfrist `unknown_task_grace_s` (Default 900 s; 0 schaltet sie ab) geht sie in die DLQ: Log `task_not_found`, Fehlerergebnis, Workflow-Fail, Unique-Freigabe, `TASK_DLQ`, `TASK_REJECTED(unregistered_task)`.

**Begründung.** Beim Rolling Deploy (neuer Producer, alter Worker) traf die sofortige DLQ jeden neuen Task, Workflows hingen (BZ-06). Das Outcome `Redelivered` verhindert, dass das `finally` den Schedfire-Completed-Marker schreibt und das Unique-Flag freigibt.

**Bewusste Folgen.** Ein wirklich falscher Task-Name kreist bis zu 15 min, bevor er in der DLQ landet. Die Deploy-Reihenfolge „Worker vor Producern“ wird dokumentiert; voll wirkt die Regel erst, wenn alle Worker ≥ 0.12 sind (Plan Abschnitt 7).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
