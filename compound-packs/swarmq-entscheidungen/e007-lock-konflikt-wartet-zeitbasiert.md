---
title: Ein Lock-Konflikt wartet zeitbasiert ab Halterwechsel; DLQ erst bei Stillstand oder nach SWARMQ_LOCK_WAIT_MAX_S
applies_when:
  - ein Task mit Mutex oder max_concurrency findet den Lock belegt
  - Änderungen an worker/gates/lock.py, an den Lock-Zustands-Headern oder an SWARMQ_LOCK_WAIT_MAX_S
  - eine Fehlersuche meldet, dass ein Lock-Konflikt stundenlang zurückgestellt wird statt nach 10 Versuchen in die DLQ zu gehen
  - eine Fehlersuche meldet, dass x-swarmq-lock-conflict-count nicht mehr hochzählt
tags: [lock, konflikt, dlq, workflow, bz-05]
id: E-007
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:126 (Entscheidungen, BZ-05)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:45 (Abschnitt 1, BZ-05 Halterwechsel)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:66 (Abschnitt 1, Lock-Konfliktzähler Rev. 3 F8)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:510 (MR 13, BZ-05)
tests:
  - tests/unit/worker/test_befund_bz05_lock_conflict_cap.py::test_long_lock_conflict_keeps_waiting_instead_of_dead_lettering
  - tests/unit/worker/gates/test_lock_wait.py::test_later_copy_counts_up_and_keeps_the_first_conflict
  - tests/unit/worker/gates/test_lock_wait.py::test_stalled_holder_dead_letters_with_lock_wait_exceeded
  - tests/unit/worker/gates/test_lock_wait.py::test_just_below_the_stall_limit_keeps_waiting
  - tests/unit/worker/gates/test_lock_wait.py::test_store_without_observe_waits_until_the_limit_and_warns_once
  - tests/unit/worker/gates/test_lock_wait.py::test_fingerprint_is_the_owner_set_only
  - tests/unit/worker/test_befund_bz13_workflow_hang.py::test_stalled_lock_holder_step_marks_the_workflow_failed
  - tests/unit/test_config_lock_wait.py::test_default_is_six_hours
---

**Entscheidung.** Ein Lock-Konflikt wird zeitbasiert zurückgestellt, nicht mehr nach 10 Versuchen abgebrochen. Gemessen wird ab dem letzten Halterwechsel (Fingerprint = Menge der Owner, ohne Score/PTTL). In die DLQ geht der Task erst, wenn derselbe Halter länger als die beobachtete PTTL + 60 s hält (Stillstand) oder `SWARMQ_LOCK_WAIT_MAX_S` (Default 21600) ab dem ersten Konflikt erreicht ist; `max_age` bleibt harte Obergrenze. Dann `LockWaitExceeded` mit `TASK_DLQ`, Fehlerergebnis und Workflow-Fail. Ist der Fingerprint nicht messbar, wird bis zur Obergrenze gewartet, ohne Deckel 10. Der Legacy-Header `x-swarmq-lock-conflict-count` wird nicht mehr erhöht; `x-swarmq-lock-conflicts` speist `lock_conflict_count`.

**Begründung.** Der alte Deckel schickte normale lange Läufe nach etwa 51 s ohne Ergebnis in die DLQ (BZ-05). Ein lebender Halter mit wechselnden Ownern ist kein Fehler. Der Legacy-Zähler bleibt stehen, weil 0.11-Pods sonst wartende 0.12-Kopien ab 10 sofort in die DLQ schickten.

**Bewusste Folgen.** Ein Task kann bis zu 6 h auf einen Lock warten. Die Uhren (`holder-since`, `wait-since`) sind Worker-Wanduhren mit ±60 s Toleranz. Gemischte `hard_timeout` unter einem Semaphor-Namen sind eine Grenze (siehe E-022). Log `task_locked` nur beim ersten Konflikt und dann einmal je Minute.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
