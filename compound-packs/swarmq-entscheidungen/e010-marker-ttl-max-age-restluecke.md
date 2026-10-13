---
title: Done- und Fired-Marker leben mindestens max_age + 300 s; Rückstand nach dem Lauf ohne max_age ist akzeptierte Restlücke
applies_when:
  - ein Task oder Cron-Fire wird nach langem Rückstand (über 65 min) erneut zugestellt
  - Änderungen an execution_guard (TTL, guard_claim.lua, guard_finish.lua) oder am Schedfire-Completed-Marker
  - eine Fehlersuche meldet Doppelausführung nach langem Rückstand bei einem Task ohne max_age
  - eine Fehlersuche meldet, dass Marker bei großem max_age sehr lange in Valkey liegen
tags: [execution-guard, marker, ttl, max-age, cron, bz-12]
id: E-010
datum: 2026-10-10
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:128 (Entscheidungen, BZ-08/BZ-12: Marker-TTL an max_age/Rückstand gekoppelt)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:49 (Abschnitt 1, BZ-12 Restlücke akzeptiert)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:79 (Nachtrag max_age-Untergrenze, Georg 2026-10-10)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:706 (Abschnitt 7, BZ-12; Done-Marker ohne Deckel, Georg 2026-10-11)
tests:
  - tests/unit/worker/gates/test_marker_max_age_floor.py::test_done_marker_of_a_task_with_max_age_outlives_a_backlog
  - tests/unit/worker/gates/test_marker_max_age_floor.py::test_superseded_marker_of_a_task_with_max_age_outlives_a_backlog
  - tests/unit/worker/gates/test_marker_max_age_floor.py::test_completed_marker_of_a_fire_with_max_age_lives_max_age_plus_300
  - tests/unit/worker/gates/test_marker_max_age_floor.py::test_without_max_age_the_done_marker_keeps_the_done_ttl
  - tests/unit/worker/gates/test_befund_bz12_marker_ttl.py::test_deferred_copy_after_a_long_backlog_is_still_skipped
---

**Entscheidung.** Ist `max_age` gesetzt, leben Done-/Superseded-Zustand des Ausführungsschutzes und der Cron-Fire-Completed-Marker mindestens `max_age + 300` s (Cron-Marker gedeckelt bei 7 d, Done-Marker ohne Deckel). Wird zurückgestellt, weil die Ausführung fremd läuft, verlängert der Claim den Zustand auf `max(7 d, max_age + 300)`; `guard_finish` verkürzt nie. Akzeptierte Restlücke: Ein Rückstand, der erst nach dem Lauf entsteht, ist bei Tasks ohne `max_age` nicht abgedeckt. Die Doku empfiehlt `max_age` für Cron- und kritische Tasks.

**Begründung.** Ohne Kopplung liefen Marker (3900 s) bei mehr als 65 min Rückstand ab und es kam zu Doppelausführung trotz Schutz (BZ-12). Eine unbegrenzte Marker-TTL für alle Tasks wäre zu teuer; `max_age` ist der Hebel des Betreibers.

**Bewusste Folgen.** Ohne `max_age` bleibt Doppelausführung nach langem Rückstand möglich und ist kein Fehler. Mit `max_age` kostet jede Ausführung einen kleinen Hash über `max_age + 300` s (etwa 100 MB bei 1 Mio. Tasks/Tag und 24 h). Bei `max_age` über 7 d lebt der Done-Marker entsprechend länger.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
