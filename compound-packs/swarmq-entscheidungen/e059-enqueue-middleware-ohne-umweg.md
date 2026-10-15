---
title: Kein Enqueue über Client oder Workflow umgeht pre_enqueue und TASK_ENQUEUED; bei schedule(delay=/eta=) läuft pre_enqueue vor dem Speichern, TASK_ENQUEUED danach
applies_when:
  - Änderungen an Client.schedule, _schedule_deferred, schedule_many, WorkflowAdvancer oder an der Persistenz verzögerter Schedules
  - ein Plan führt einen neuen Publish- oder Speicherpfad für Tasks ein
  - eine Fehlersuche meldet, dass TASK_ENQUEUED bei delay/eta beim Speichern statt beim Fire feuert
  - eine Fehlersuche meldet, dass ein Veto aus pre_enqueue einen verzögerten Task schon vor dem Speichern verhindert
  - eine Fehlersuche meldet, dass Cron-Fires des Leaders die Enqueue-Middleware umgehen
tags: [middleware, signals, client, scheduler, workflow, b9, f3-02]
id: E-059
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-02)
  - docs/plans/2026-08-30-001-fix-inventar-debug-backlog.md:215 (B9, Workflow-Folgeschritte umgehen Middleware und TASK_ENQUEUED)
  - docs/plans/2026-09-08-001-feat-debug-backlog-loadsuite-plan.md:16, 92 (B9 in 0.6.0 behoben)
  - website/reference/changelog.md:2468 (0.6.0, Workflow steps run through the enqueue middleware)
  - website/guides/middleware.md:12, 27 (every task; pre_enqueue aborts the enqueue)
tests: []
ohne_test: Test entsteht mit dem Fix in 0.12.3 (F3-02, tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py). Kandidat ohne Marker für den Workflow-Teil (B9) ist tests/unit/worker/test_pipeline.py::test_workflow_step_publisher_runs_middleware_then_signals.
---

**Entscheidung.** Jeder Enqueue über den Client (`schedule`, `schedule_many`, `apply`) und jeder Workflow-Folgeschritt läuft durch `pre_enqueue` und löst `TASK_ENQUEUED` aus. Bei `schedule(delay=…/eta=…)` läuft `pre_enqueue` im Producer vor dem Speichern im Scheduler-Store und vor dem Unique-Claim; ein Veto verhindert beides. `TASK_ENQUEUED` feuert nach dem Speichern, nicht erst beim Fire. Header- und Message-Änderungen der Middleware werden mit der Definition gespeichert, und der Leader publiziert sie beim Fire.

**Begründung.** Middleware und Signale versprechen „every task“; ein Umweg an ihnen vorbei war schon bei den Workflow-Schritten ein Fehler (B9, 0.6.0). Beim Speichern ist der Enqueue aus Sicht des Aufrufers erledigt; der Fire liegt in einem anderen Prozess (Leader), der den Producer-Kontext nicht kennt.

**Bewusste Folgen.** Ein Veto wirkt zum Zeitpunkt von `schedule()`, nicht zum Fire-Zeitpunkt. Middleware, die beim Fire neu entscheiden will, sieht den Fire nicht. Statische Crons und CLI-Crons, die der Leader feuert, laufen bis zur Klärung von F3-36 ohne Enqueue-Middleware; das ist kein Fehler gegen diese Regel.

**Überdenken nur mit neuem Argument.** Offen: F3-36 (sollen Leader-Fires durch `pre_enqueue` und `TASK_ENQUEUED` laufen?). Dass `schedule(delay=/eta=)` heute vor `pre_enqueue` zurückkehrt, ist der Befund F3-02 (Fix in 0.12.3).
