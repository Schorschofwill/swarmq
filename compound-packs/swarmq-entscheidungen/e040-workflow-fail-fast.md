---
title: Das endgültige Scheitern oder der Abbruch eines Workflow-Schritts markiert den Workflow synchron als failed
applies_when:
  - ein neuer oder geänderter terminaler Pfad im Worker (Gate, DLQ, Cancel, Expiry, Lock, unbekannter Task) betrifft Workflow-Schritte
  - eine Fehlersuche meldet einen Workflow, der nach einem terminal beendeten Schritt auf running bleibt
  - ein Plan will das Fail-Mark asynchron machen oder über DLQ-Beobachtung lösen
  - eine Fehlersuche meldet einen Workflow, der nach gespeichertem Erfolg und gescheitertem Advance auf running bleibt
  - Änderungen am Zustelllimit-Weg für Workflow-Schritte (retry.py, gates/delivery_count.py)
tags: [workflow, fail-fast, liveness, cancel, dlq, zusage]
id: E-040
datum: 2026-05-30
status: gilt
quelle:
  - docs/plans/2026-05-30-001-feat-workflow-primitives-plan.md:50 (R16, Fail-fast inkl. Liveness), :116 (synchroner Hook, nicht DLQ-Beobachtung)
  - docs/brainstorms/2026-05-30-workflow-primitives-requirements.md:188-196 (R16)
  - 'Commit 17dac64 (U6, „terminal-failure seam“, Cancel eingeschlossen: „task was cancelled“)'
  - src/swarmq/worker/results.py:205-213
  - 'Commit 4956358 (U15, FS-12: Cancel vor Start beendet Workflow) und Review 546cd08 (Zurückstellung über die Delay-Queue)'
  - src/swarmq/worker/gates/cancel.py:45-61 (_defer_fail_mark), :117-138
  - Georg 2026-10-17 (Fehlersuche Runde 4, F4-01, Zustelllimit)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (FEHLER F4-01; Vor dem Fix Punkt 1)
tests:
  - tests/unit/worker/test_terminal.py::test_step_without_a_success_result_fails_its_workflow
  - tests/unit/worker/gates/test_unknown_task.py::test_unknown_workflow_step_is_routed_and_fails_its_workflow
  - tests/unit/worker/test_befund_fs12_cancel_before_start.py::test_pre_start_task_cancel_of_a_workflow_step_fails_the_workflow
  - tests/unit/worker/gates/test_cancel.py::TestPreStartCancelFailsWorkflow::test_transient_fail_mark_defers_without_ack_result_or_flag_shortening
---

**Entscheidung.** Endet ein Workflow-Schritt endgültig ohne Erfolg (DLQ, `NoRetry`, Ablauf, Lock-Fehler, unbekannter Task, Abbruch per Cancel), markiert der Worker den Workflow synchron im selben Pfad als `failed` und weckt `get()`. Ein Workflow läuft nie still in den `get()`-Timeout.

**Begründung.** Ohne synchrones Fail-Mark wartet ein Aufrufer bis zum Timeout und ein Chord-Body läuft nie. Eine Beobachtung der DLQ wäre verspätet und unzuverlässig.

**Bewusste Folgen.** Scheitert das Fail-Mark selbst transient, wird die Nachricht neu zugestellt statt bestätigt (Plan 2026-09-08-001 U10). Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert.

**Nachtrag 2026-10-14 (FS-12, U15, Review 546cd08): Pre-Start-Cancel.** Bricht `cancel(task_id)` einen einzelnen Workflow-Schritt im Cancel-Gate vor dem Start ab, markiert der Worker den Workflow als `failed` („task was cancelled“), bevor er das Cancel-Ergebnis speichert; die Markierung prüft, ob der Schritt schon erfolgreich war, und sähe sonst das eigene Cancel-Ergebnis. Scheitert die Markierung vorübergehend (Backend-Ausfall oder verlorene CAS-Runden, `WorkflowStateConflict`), wird die Nachricht abweichend von „Bewusste Folgen“ nicht an derselben Stelle neu zugestellt, sondern über die Delay-Queue zurückgestellt (`task_deferred_backend_unavailable`, `stage=cancel_fail_workflow`): Das Cancel-Gate läuft vor dem Delivery-Count-Gate, ein Requeue an derselben Stelle landete nach `delivery_limit` still in der `.XQ`. Kein Ack, kein Cancel-Ergebnis, das Cancel-Flag bleibt stehen (E-020), die Kopie durchläuft den Zweig erneut. Der Workflow-Cancel-Zweig bleibt unverändert (FS-32).

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; F4-01, R4Z-02): Erfolg gespeichert, Advance gescheitert.** Die Bewusste Folge „Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert“ gilt für Schritte, deren Workflow schon weitergeschaltet ist. Ist das Erfolgsergebnis gespeichert, der Advance aber noch nicht gelaufen, schaltet der Worker im Schutzfall von `fail_workflow_step` mit dem gespeicherten Ergebnis weiter (Advance wiederholen; scheitert er vorübergehend, neu zustellen wie das Fail-Mark). Das gilt auch am Zustelllimit: weiterschalten, das Erfolgsergebnis bleibt und wird nie mit einem Fehlerergebnis überschrieben (heute `retry.py:302`, siehe auch F4-02 und den Nachtrag zu E-043). Der Workflow läuft damit nie still in den `get()`-Timeout. Der Nachweis F4-01 (`tests/unit/test_befund_f4_01_workflow_dead_letter_after_unadvanced_success.py`, `xfail`) verlangt nur einen terminalen Status und trägt deshalb keinen Marker; der festschreibende Test (am Zustelllimit `completed`, Erfolgsergebnis unverändert) folgt mit dem Fix (F4-01).

**Überdenken nur mit neuem Argument.** FS-12 (Fehlersuche Runde 2) ist behoben: Ein `cancel(task_id)` eines einzelnen Schritts, der vor dem Start im Cancel-Gate endet, markiert den Workflow jetzt vor dem Cancel-Ergebnis als `failed` (`tests/unit/worker/test_befund_fs12_cancel_before_start.py`); der Workflow-Cancel-Zweig bleibt unverändert (FS-32). Nebenbefund: Nach `handle.cancel()` bleibt `status()` auf `running`, wenn kein Schritt mehr fertig wird, während `get()` den Abbruch am Flag erkennt (`workflow_orchestration.py:172-178`).
