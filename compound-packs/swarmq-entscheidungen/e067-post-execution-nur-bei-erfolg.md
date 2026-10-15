---
title: post_execution läuft nur nach erfolgreichem run(); aufgeräumt wird im yield-Finalizer, on_error der Middleware kommt bei jedem Fehler
applies_when:
  - Änderungen an der Ausführung in worker/execution.py oder an Task.pre_execution/post_execution
  - eine Fehlersuche meldet, dass post_execution bei einem Fehler in run() nicht läuft
  - ein Plan oder eine Doku beschreibt post_execution als Aufräum-Hook für Erfolg und Fehler
tags: [task, lifecycle, dishka, middleware, f3-28]
id: E-067
datum: 2026-05-31
status: gilt
quelle:
  - docs/plans/2026-05-31-001-feat-dishka-di-tasks-plan.md:96 (post_execution läuft nur bei Erfolg, Rollback im yield-Finalizer)
  - docs/brainstorms/2026-05-31-dishka-di-tasks-requirements.md:58 (AE4)
  - docs/plans/2026-04-15-001-feat-phase0-phase1-roundtrip-plan.md:757-762 (Fehlerbehandlung um pre_execution/run/post_execution)
  - docs/plans/2026-04-16-001-feat-phase3a-middleware-signals-plan.md:221-222 (post_execute nach erfolgreichem post_execution, on_error bei jedem Fehler)
tests:
  - tests/unit/worker/test_worker_dishka.py::test_finalizer_receives_exception
  - tests/unit/worker/test_worker.py::test_pre_execution_raises_skips_post
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_skips_post_execution
  - tests/unit/worker/test_worker.py::TestWorkerMiddlewareAndSignals::test_post_execute_not_called_when_post_execution_fails
---

**Entscheidung.** `Task.post_execution()` läuft nur, wenn `pre_execution()` und `run()` ohne Ausnahme durchliefen. Aufräumen, das auch bei Fehlern laufen muss, gehört in den `yield`-Finalizer des Dishka-Providers (er erhält die Ausnahme) oder in ein `try/finally` im Task. `post_execute` der Middleware kommt nach erfolgreichem `post_execution`, `on_error` bei jedem Fehler (`pre_execution`, `run`, `post_execution`).

**Begründung.** `post_execution` kann das Ergebnis selbst zum Fehler machen; transaktionale Logik (Commit/Rollback) braucht die Ausnahme und gehört deshalb in den Finalizer (DI-Plan, AE4).

**Bewusste Folgen.** Ressourcen, die nur in `post_execution` freigegeben werden, bleiben bei einem Fehler liegen. Das ist kein Fehler von swarmq. Die Doku (`defining-tasks.md:193` + DE, `api.md:516`), die „success or failure“ verspricht, ist falsch (F3-28, Doku-Fix).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
