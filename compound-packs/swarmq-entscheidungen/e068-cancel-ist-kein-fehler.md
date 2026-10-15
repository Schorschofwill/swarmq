---
title: Ein Cancel ist kein Fehler; im Task kommt asyncio.CancelledError an, TaskCancelledError nur beim Client, die Middleware sieht weder on_error noch post_execute, nur TASK_CANCELLED
applies_when:
  - Änderungen am Cancel-Monitor, an worker/execution.py (Cancel-Zweig) oder an der Middleware-Kette
  - eine Fehlersuche meldet, dass on_error oder post_execute bei einem Cancel nicht laufen
  - eine Fehlersuche meldet, dass im Task-Körper CancelledError statt TaskCancelledError ankommt
  - eine Fehlersuche meldet, dass ein Cancel während post_execution nicht mehr wirkt
tags: [cancel, middleware, signals, task, f3-20, f3-30]
id: E-068
datum: 2026-04-16
status: gilt
quelle:
  - docs/plans/2026-04-16-009-feat-phase3b3-task-cancellation-plan.md:750-754 (run_task.cancel(), CancelledError), :523-558 (TaskCancelledError im Client), :236-238 (Cancel während post_execution ist akzeptierte Limitation), :982-985 (Cancel ist kein Error, beobachtbar über TASK_CANCELLED)
  - src/swarmq/worker/execution.py:236-241 (Cancel umgeht on_error und post_execute)
tests: []
ohne_test: Kein Test trägt bisher den Marker. Kandidaten ohne Marker sind tests/unit/worker/test_worker.py::TestRunningCancelInHandleMessage::test_cancel_during_run_raises_task_cancelled_error, ::test_cancel_during_run_dispatches_task_cancelled_signal und tests/unit/client/test_results.py::test_get_result_cancelled_raises_task_cancelled_error. Für „kein on_error/post_execute bei Cancel“ fehlt ein Test.
---

**Entscheidung.** Ein Cancel bricht den laufenden Task über `run_task.cancel()` ab; im Task-Körper kommt `asyncio.CancelledError` an. `TaskCancelledError` sieht nur der Client (`get_result`, `SwarmQ.run`). Ein Cancel ist kein Fehler: Die Middleware bekommt weder `on_error` noch `post_execute`, beobachtbar ist er nur über das Signal `TASK_CANCELLED`. Ein Cancel während `post_execution` wirkt nicht mehr (das Fenster schließt mit dem Ende von `run()`).

**Begründung.** Cancel ist eine gewollte Unterbrechung, kein Ausfall; er soll weder Retry noch Fehlerzähler oder Fehler-Middleware auslösen (Closed Decision im Cancel-Brainstorm).

**Bewusste Folgen.** Middleware, die Zustand je Task-ID in `pre_execute` anlegt und in `post_execute`/`on_error` abräumt, muss zusätzlich `TASK_CANCELLED` behandeln, sonst bleibt der Zustand liegen. Ein Task, der `TaskCancelledError` fängt, fängt nie etwas. Beides ist kein Fehler; die Doku, die anderes sagt, ist falsch (F3-20, F3-30, Doku-Fix).

**Überdenken nur mit neuem Argument.** Offen (F3-30): Das Muster aus dem eigenen Middleware-Guide (Zustand je Task-ID, `TimingMiddleware`) leckt bei jedem Cancel; der Plan hat das nicht betrachtet. Mögliche Antwort: ein eigener Hook `on_cancel`.
