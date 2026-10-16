---
title: Im Shutdown gibt der Worker übrig gebliebene Handler-Locks erst frei, nachdem er auf die abgebrochenen Tasks gewartet hat
applies_when:
  - Änderungen an WorkerShutdown._release_leftover_locks oder an der Reihenfolge der Shutdown-Schritte
  - eine Fehlersuche meldet, dass ein Lock im Shutdown freigegeben wird, während der abgebrochene Handler noch Nutzercode ausführt
  - eine Fehlersuche meldet doppelte Lock-Freigaben oder task_lock_lost im Shutdown
tags: [shutdown, lock, drain]
id: E-071
datum: 2026-04-16
status: gilt
quelle:
  - docs/plans/2026-04-16-008-feat-phase3b2-task-locking-plan.md:538-550 (Schritt 3b, Reihenfolge ist kritisch, P2-Finding Feasibility-Review)
  - src/swarmq/worker/shutdown.py:217 (_release_leftover_locks)
tests: []
ohne_test: Kein Test trägt bisher den Marker. Kandidaten ohne Marker sind tests/unit/worker/test_lifecycle.py::TestShutdownLockDrain::test_leftover_handler_locks_released_before_backend_close und tests/unit/worker/test_lock_keeper_wiring.py::test_shutdown_drops_leftover_locks_and_closes_the_keeper_before_backend_close; beide prüfen die Freigabe vor dem Backend-Close, nicht die Reihenfolge zur Wartezeit.
---

**Entscheidung.** Locks, die ein Handler beim Shutdown noch hält, gibt der Worker erst frei, nachdem er die abgebrochenen Tasks abgewartet hat (mit Zeitlimit), und vor dem Schließen von Broker und Backend. Die Freigabe ist das letzte Sicherheitsnetz; im normalen Drain gibt der Handler im eigenen `finally` frei.

**Begründung.** Vorher freigegeben, liefe ein abgebrochener Task noch kurz weiter, während ein anderer Worker den Lock schon erworben hat („Reihenfolge ist kritisch“).

**Bewusste Folgen.** Überlebt ein Handler die Wartezeit (Nutzercode, der nach `cancel()` weiterläuft), endet sein Lock samt Keeper-Lease, während er noch läuft; unter dem Supervisor spätestens beim SIGKILL, eingebettet erst mit dem Handler. Bis F3-32 entschieden ist, ist das eine Restlücke.

**Überdenken nur mit neuem Argument.** Offen (F3-32): Laufende Handler-Tasks überspringen und den Lock der TTL überlassen, oder beibehalten und in E-018/E-022 als Grenze dokumentieren. Nicht gedeckt ist F3-12: Gibt der Handler danach im eigenen `finally` noch einmal frei, ist das eine doppelte Freigabe mit Fehlalarm `task_lock_lost` (FEHLER, behoben in 0.12.3: `release_acquired_locks` gibt nur frei, wenn es den Registry-Eintrag selbst entnommen hat).
