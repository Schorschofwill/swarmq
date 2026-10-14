---
title: Mutex- und Semaphor-Leases werden erneuert, solange der Handler lebt; kein Fencing-Token, blockierter Loop ist akzeptierte Restlücke
applies_when:
  - ein Task hält einen Lock länger als die Lock-TTL oder über hard_timeout + 30 s hinaus
  - Änderungen an worker/lock_keeper.py, lock_extend.lua, lock_acquire.lua oder der Lock-Freigabe
  - eine Fehlersuche meldet, dass ein Lock über hard_timeout hinaus gehalten wird
  - eine Fehlersuche meldet, dass bei blockiertem Event-Loop zwei Halter gleichzeitig laufen (task_lock_lost)
  - mehrere Tasks mit unterschiedlichem hard_timeout teilen einen Semaphor-Namen
tags: [lock, lease, semaphor, fencing, bz-25]
id: E-022
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:139 (Entscheidungen, BZ-25)
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:112 (Nachweis BZ-25: behoben bis auf blockierten Event-Loop)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:54 (Abschnitt 1, BZ-25 weiter erneuern)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:510 (MR 13, BZ-25)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:730 (Abschnitt 7, BZ-25/05 gemischte hard_timeout)
tests:
  - tests/integration/test_befund_bz25_lock_lease.py::test_mutex_stays_held_while_the_holder_runs_past_the_lock_ttl
  - tests/unit/worker/test_lock_keeper.py::test_lost_while_running_is_reported_once_and_the_lease_dropped
  - tests/unit/worker/test_lock_keeper.py::test_holding_past_the_original_ttl_warns_once
  - tests/unit/worker/test_lock_keeper.py::test_surely_expired_lock_is_reported_without_a_round_trip
  - tests/unit/worker/test_lock_keeper.py::test_drop_removes_the_lease_and_ends_the_keeper
---

**Entscheidung.** Der Lock-Keeper verlängert Mutex- und Semaphor-Leases in kurzen Schritten, solange der Handler lebt, auch über `hard_timeout + 30 s` hinaus (einmal `task_lock_held_past_hard_timeout`). Scheitert die Erneuerung, gibt es das Log-Event `task_lock_lost` (`stage=running|release|expired`). Es gibt kein Fencing-Token. Eine Lease fällt beim Freigabeversuch aus dem Keeper. `lock_acquire.lua` setzt `PEXPIRE` nur noch verlängernd.

**Begründung.** Locks liefen bei langen Handlern still ab, ein zweiter Halter startete (BZ-25). Fencing würde jede geschützte Ressource zur Mitarbeit zwingen; das ist außerhalb von swarmq.

**Bewusste Folgen.** Akzeptierte Restlücke: Bei blockiertem Event-Loop läuft der Lock ab, das wird nur gemeldet (`task_lock_lost`). Ein per SIGKILL beendeter Halter hinterlässt den Lock bis `t0 + TTL` (bei Überziehung ein Fenster länger). Gemischte `hard_timeout` unter einem Semaphor-Namen: Ein Erwerber mit kurzer TTL räumt Member nach seiner eigenen TTL ab; 0.11-Erwerber verkürzen den Key weiter.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
