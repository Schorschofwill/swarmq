---
title: Das endgültige Scheitern oder der Abbruch eines Workflow-Schritts markiert den Workflow synchron als failed
applies_when:
  - ein neuer oder geänderter terminaler Pfad im Worker (Gate, DLQ, Cancel, Expiry, Lock, unbekannter Task) betrifft Workflow-Schritte
  - eine Fehlersuche meldet einen Workflow, der nach einem terminal beendeten Schritt auf running bleibt
  - ein Plan will das Fail-Mark asynchron machen oder über DLQ-Beobachtung lösen
tags: [workflow, fail-fast, liveness, cancel, dlq, zusage]
id: E-040
datum: 2026-05-30
status: gilt
quelle:
  - docs/plans/2026-05-30-001-feat-workflow-primitives-plan.md:50 (R16, Fail-fast inkl. Liveness), :116 (synchroner Hook, nicht DLQ-Beobachtung)
  - docs/brainstorms/2026-05-30-workflow-primitives-requirements.md:188-196 (R16)
  - Commit 17dac64 (U6, „terminal-failure seam“, Cancel eingeschlossen: „task was cancelled“)
  - src/swarmq/worker/results.py:205-213
tests:
  - tests/unit/worker/test_terminal.py::test_step_without_a_success_result_fails_its_workflow
  - tests/unit/worker/gates/test_unknown_task.py::test_unknown_workflow_step_is_routed_and_fails_its_workflow
---

**Entscheidung.** Endet ein Workflow-Schritt endgültig ohne Erfolg (DLQ, `NoRetry`, Ablauf, Lock-Fehler, unbekannter Task, Abbruch per Cancel), markiert der Worker den Workflow synchron im selben Pfad als `failed` und weckt `get()`. Ein Workflow läuft nie still in den `get()`-Timeout.

**Begründung.** Ohne synchrones Fail-Mark wartet ein Aufrufer bis zum Timeout und ein Chord-Body läuft nie. Eine Beobachtung der DLQ wäre verspätet und unzuverlässig.

**Bewusste Folgen.** Scheitert das Fail-Mark selbst transient, wird die Nachricht neu zugestellt statt bestätigt (Plan 2026-09-08-001 U10). Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert.

**Überdenken nur mit neuem Argument.** Heute teilweise verletzt: FS-12 (Fehlersuche Runde 2, xfail-Nachweis im Branch `test/fehlersuche-runde-2`). Ein `cancel(task_id)` eines einzelnen Schritts, der vor dem Start im Cancel-Gate endet, lässt den Workflow auf `running`. Der Lauf-Cancel und die übrigen terminalen Pfade erfüllen die Zusage. Nebenbefund: Nach `handle.cancel()` bleibt `status()` auf `running`, wenn kein Schritt mehr fertig wird, während `get()` den Abbruch am Flag erkennt (`workflow_orchestration.py:172-178`).
