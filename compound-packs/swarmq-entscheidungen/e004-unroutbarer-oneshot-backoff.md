---
title: Ein unroutbarer Oneshot wird im 60-s-Takt wiederholt und nach einem Tag verworfen
applies_when:
  - ein per delay/eta geplanter Task (Oneshot) zielt auf eine nicht existierende Queue
  - Änderungen an scheduler/unroutable.py, am Feld unroutable_since oder an SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S
  - eine Fehlersuche meldet, dass ein Oneshot nach Ablauf verworfen wird oder sich um 60 s verschiebt
tags: [scheduler, oneshot, unroutable, bz-03]
id: E-004
datum: 2026-10-09
status: gilt
quelle:
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:55 (Abschnitt 1, BZ-03 Oneshot: „Mit Backoff erneut versuchen“)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:347 (MR 6, Leader Oneshot)
tests:
  - tests/unit/scheduler/test_fire_unroutable.py::test_unroutable_oneshot_is_deferred_by_sixty_seconds_from_now
  - tests/unit/scheduler/test_fire_unroutable.py::test_repeatedly_unroutable_oneshot_keeps_its_first_unroutable_since
  - tests/unit/scheduler/test_fire_unroutable.py::test_oneshot_unroutable_past_the_limit_is_dropped
  - tests/unit/scheduler/test_fire_unroutable.py::test_dropped_oneshot_stores_an_error_result_under_its_task_id
  - tests/unit/scheduler/test_fire_unroutable.py::test_dropped_cli_unique_oneshot_releases_its_unique_claim
  - tests/unit/test_config_scheduler_unroutable.py::test_default_is_one_day
---

**Entscheidung.** Ein unroutbarer Oneshot wird mit Score `now + 60 s` festgeschrieben und erneut versucht. `unroutable_since` hält den ersten unroutbaren Versuch fest (nicht die Fälligkeit). Nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (Default 86400) wird er verworfen: Event `scheduler_oneshot_unroutable_dropped`, Signal/Metrik `kind="oneshot_dropped"`, Fehlerergebnis unter der `task_id`, bei `cli_unique` Freigabe des Unique-Claims.

**Begründung.** Ein Oneshot hat keinen nächsten Termin; er soll einen kurzen Deploy-Versatz (Queue entsteht gleich) überleben. Der Commit mit +60 s verhindert, dass unroutbare Oneshots den Poll verstopfen.

**Bewusste Folgen.** Ein Oneshot läuft bis zu 60 s nach dem Anlegen der Queue verspätet. Nach einem Tag ohne Ziel ist er weg; `get_result` liefert dann das Fehlerergebnis.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
