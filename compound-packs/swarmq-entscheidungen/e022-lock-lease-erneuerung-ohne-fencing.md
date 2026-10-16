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
  - 'docs/befunde/betrieb-ueber-zeit-2026-10.md:112 (Nachweis BZ-25: behoben bis auf blockierten Event-Loop)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:54 (Abschnitt 1, BZ-25 weiter erneuern)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:510 (MR 13, BZ-25)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:730 (Abschnitt 7, BZ-25/05 gemischte hard_timeout)
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:154-158 (KTD7, Idempotenz über Selbsttreffer, FS-06b)
  - Commit 86798d1 (U11, Wiederholung nach verlorener Antwort erkennt eigenen Token)
  - src/swarmq/lua/lock_acquire.lua (Selbsttreffer Mutex und Semaphor)
tests:
  - tests/integration/test_befund_bz25_lock_lease.py::test_mutex_stays_held_while_the_holder_runs_past_the_lock_ttl
  - tests/unit/worker/test_lock_keeper.py::test_lost_while_running_is_reported_once_and_the_lease_dropped
  - tests/unit/worker/test_lock_keeper.py::test_holding_past_the_original_ttl_warns_once
  - tests/unit/worker/test_lock_keeper.py::test_surely_expired_lock_is_reported_without_a_round_trip
  - tests/unit/worker/test_lock_keeper.py::test_drop_removes_the_lease_and_ends_the_keeper
  - tests/contracts/coordination.py::LockStoreContract::test_acquire_repeated_by_same_owner_returns_token_and_keeps_pttl
  - tests/contracts/coordination.py::LockStoreContract::test_semaphore_acquire_repeated_by_same_owner_returns_token_and_keeps_pttl
  - tests/contracts/coordination.py::LockStoreContract::test_full_semaphore_including_own_token_returns_token_on_repeat
---

**Entscheidung.** Der Lock-Keeper verlängert Mutex- und Semaphor-Leases in kurzen Schritten, solange der Handler lebt, auch über `hard_timeout + 30 s` hinaus (einmal `task_lock_held_past_hard_timeout`). Scheitert die Erneuerung, gibt es das Log-Event `task_lock_lost` (`stage=running|release|expired`). Es gibt kein Fencing-Token. Eine Lease fällt beim Freigabeversuch aus dem Keeper. `lock_acquire.lua` setzt `PEXPIRE` nur noch verlängernd.

**Begründung.** Locks liefen bei langen Handlern still ab, ein zweiter Halter startete (BZ-25). Fencing würde jede geschützte Ressource zur Mitarbeit zwingen; das ist außerhalb von swarmq.

**Bewusste Folgen.** Akzeptierte Restlücke: Bei blockiertem Event-Loop läuft der Lock ab, das wird nur gemeldet (`task_lock_lost`). Ein per SIGKILL beendeter Halter hinterlässt den Lock bis `t0 + TTL` (bei Überziehung ein Fenster länger). Gemischte `hard_timeout` unter einem Semaphor-Namen: Ein Erwerber mit kurzer TTL räumt Member nach seiner eigenen TTL ab; 0.11-Erwerber verkürzen den Key weiter.

**Nachtrag 2026-10-14 (FS-06b, U11, KTD7): Erwerb mit dem eigenen Token ist idempotent.** `lock_acquire.lua` wertet den eigenen Token im Key als erworben: beim Mutex `GET == owner`, beim Semaphor ein lebender eigener Slot, auch wenn das Set voll ist. Die TTL bleibt dabei unverändert, der Selbsttreffer verlängert nicht. Damit übersteht ein Erwerb die Wiederholung, die valkey-py nach verlorener Antwort sendet, ohne dass der eigene Lock als fremd gilt; den Transport-Retry abzuschalten ist verworfen (Verfügbarkeit). Das ist Vertrag für jede `LockStore`-Implementierung (`tests/contracts/coordination.py::LockStoreContract`). Ein fremder Owner bleibt abgewiesen. Bewusste Folge: Wer einen Lock mit gleichem Token zweimal erwirbt, bekommt ihn zweimal „erworben“ gemeldet, ohne neue Frist; die Verlängerung bleibt Sache des Lock-Keepers. Folgen für die Leader-Wahl stehen in E-048 (Nachtrag FS-06b), für den Unique-Claim in E-011 (Nachtrag FS-06b).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
