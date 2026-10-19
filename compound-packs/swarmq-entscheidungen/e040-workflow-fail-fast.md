---
title: Das endgültige Scheitern oder der Abbruch eines Workflow-Schritts markiert den Workflow synchron als failed
applies_when:
  - ein neuer oder geänderter terminaler Pfad im Worker (Gate, DLQ, Cancel, Expiry, Lock, unbekannter Task) betrifft Workflow-Schritte
  - eine Fehlersuche meldet einen Workflow, der nach einem terminal beendeten Schritt auf running bleibt
  - ein Plan will das Fail-Mark asynchron machen oder über DLQ-Beobachtung lösen
  - eine Fehlersuche meldet einen Workflow, der nach gespeichertem Erfolg und gescheitertem Advance auf running bleibt
  - Änderungen am Zustelllimit-Weg für Workflow-Schritte (retry.py, gates/delivery_count.py)
  - eine Fehlersuche meldet, dass ein Workflow nach einer dauerhaften Fehlerantwort des Chord-Skripts (WRONGTYPE) am Zustelllimit running bleibt
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
  - 'Georg 2026-10-19 (Entscheidungsvorlage 0.13.0, Punkt 1: A jetzt, B in 0.13.1)'
  - docs/befunde/entscheidungsvorlage-0-13-0-2026-10-19.md (Punkt 1)
tests:
  - tests/unit/worker/test_terminal.py::test_step_without_a_success_result_fails_its_workflow
  - tests/unit/worker/gates/test_unknown_task.py::test_unknown_workflow_step_is_routed_and_fails_its_workflow
  - tests/unit/worker/test_befund_fs12_cancel_before_start.py::test_pre_start_task_cancel_of_a_workflow_step_fails_the_workflow
  - tests/unit/worker/gates/test_cancel.py::TestPreStartCancelFailsWorkflow::test_transient_fail_mark_defers_without_ack_result_or_flag_shortening
  - tests/unit/test_befund_f4_01_workflow_dead_letter_after_unadvanced_success.py::test_at_the_limit_the_last_advance_completes_the_workflow_and_keeps_the_success
  - tests/unit/test_befund_f4_01_workflow_dead_letter_after_unadvanced_success.py::test_at_the_limit_a_transient_last_advance_dead_letters_without_overwriting_the_success
  - tests/unit/worker/test_terminal.py::test_transient_last_advance_at_the_limit_dead_letters_without_failing_the_workflow
---

**Entscheidung.** Endet ein Workflow-Schritt endgültig ohne Erfolg (DLQ, `NoRetry`, Ablauf, Lock-Fehler, unbekannter Task, Abbruch per Cancel), markiert der Worker den Workflow synchron im selben Pfad als `failed` und weckt `get()`. Ein Workflow läuft nie still in den `get()`-Timeout.

**Begründung.** Ohne synchrones Fail-Mark wartet ein Aufrufer bis zum Timeout und ein Chord-Body läuft nie. Eine Beobachtung der DLQ wäre verspätet und unzuverlässig.

**Bewusste Folgen.** Scheitert das Fail-Mark selbst transient, wird die Nachricht neu zugestellt statt bestätigt (Plan 2026-09-08-001 U10). Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert.

**Nachtrag 2026-10-14 (FS-12, U15, Review 546cd08): Pre-Start-Cancel.** Bricht `cancel(task_id)` einen einzelnen Workflow-Schritt im Cancel-Gate vor dem Start ab, markiert der Worker den Workflow als `failed` („task was cancelled“), bevor er das Cancel-Ergebnis speichert; die Markierung prüft, ob der Schritt schon erfolgreich war, und sähe sonst das eigene Cancel-Ergebnis. Scheitert die Markierung vorübergehend (Backend-Ausfall oder verlorene CAS-Runden, `WorkflowStateConflict`), wird die Nachricht abweichend von „Bewusste Folgen“ nicht an derselben Stelle neu zugestellt, sondern über die Delay-Queue zurückgestellt (`task_deferred_backend_unavailable`, `stage=cancel_fail_workflow`): Das Cancel-Gate läuft vor dem Delivery-Count-Gate, ein Requeue an derselben Stelle landete nach `delivery_limit` still in der `.XQ`. Kein Ack, kein Cancel-Ergebnis, das Cancel-Flag bleibt stehen (E-020), die Kopie durchläuft den Zweig erneut. Der Workflow-Cancel-Zweig bleibt unverändert (FS-32).

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; F4-01, R4Z-02): Erfolg gespeichert, Advance gescheitert.** Die Bewusste Folge „Ein Schritt, der schon erfolgreich war, markiert seinen Workflow bei einem späteren Dead-Letter nicht als gescheitert“ gilt für Schritte, deren Workflow schon weitergeschaltet ist. Ist das Erfolgsergebnis gespeichert, der Advance aber noch nicht gelaufen, schaltet der Worker im Schutzfall von `fail_workflow_step` mit dem gespeicherten Ergebnis weiter (Advance wiederholen; scheitert er vorübergehend, neu zustellen wie das Fail-Mark). Das gilt auch am Zustelllimit: weiterschalten, das Erfolgsergebnis bleibt und wird nie mit einem Fehlerergebnis überschrieben (heute `retry.py:302`, siehe auch F4-02 und den Nachtrag zu E-043). Der Workflow läuft damit nie still in den `get()`-Timeout. Der Nachweis F4-01 (`tests/unit/test_befund_f4_01_workflow_dead_letter_after_unadvanced_success.py`) verlangt nur einen terminalen Status und trägt deshalb keinen Marker.

Umgesetzt in 0.13.0 (Plan 2026-10-17-2103, U14, KTD8, KD6): `fail_workflow_step` holt im Schutzfall den Advance mit dem gespeicherten Ergebnis nach (dieselbe Funktion wie der Ergebnis-Tail, `advance_with_result`) und meldet `StepDone`. Gelingt der Advance, bestätigt der Pfad die Zustellung ohne DLQ-Kopie und ohne `TASK_DLQ`; Unique-Freigabe und Schedfire-Finalisierung laufen wie nach einer DLQ-Kopie, ebenso in den Gate-Pfaden (Ablauf, Lock, Cancel, unbekannter Task, Zustelllimit). Ein Cancel-Ergebnis oder `TASK_CANCELLED` entsteht dann nicht. Scheitert er vorübergehend, wird neu zugestellt (im Cancel-Gate über die Delay-Queue wie die Markierung). `UnroutableMessage` oder ein unerwarteter Fehler markiert den Workflow mit dieser Ursache als `failed`, danach Ack. Am Zustelllimit folgt ein letzter Advance; scheitert auch er vorübergehend, geht die Zustellung als DLQ-Kopie ohne Fehlerergebnis. **Restfall:** Der Workflow bleibt dann `running` bis zur TTL; die DLQ-Kopie hält die Zustellung für den Betreiber fest. Einfache Tasks ohne Workflow folgen weiter E-031. Festgeschrieben durch die drei F4-01-Tests unter `tests:`.

**Nachtrag 2026-10-19 (Georg, Entscheidungsvorlage 0.13.0, Punkt 1A; KD7/KD9): Restfall bei dauerhafter Fehlerantwort.** Der Restfall oben („scheitert auch er vorübergehend, … Der Workflow bleibt dann `running` bis zur TTL“) umfasst auch eine dauerhafte Fehlerantwort des Chord-Skripts (`BackendResponseError`, etwa `WRONGTYPE` auf dem Chord- oder Ergebnis-Key bei intaktem `sq:wf:{wf}`). Sie lässt jeden Advance wie ein Ausfall scheitern (E-079), der Schritt läuft bis zum Zustelllimit erneut, am Limit geht die Nachricht ohne Fehlerergebnis in die DLQ und der Workflow bleibt `running` bis zur TTL; der Erfolg bleibt gespeichert. Das ist bis 0.13.1 eine bewusste Folge und kein Fehler; 0.13.0 schließt die Release-Lücke aus 0.12.4 (KD9) nur insofern, als der Erfolg nicht mehr überschrieben wird. **Entschieden für 0.13.1:** Scheitert der letzte Advance am Zustelllimit mit `BackendResponseError`, markiert der Worker den Workflow als `failed` (Ursache ist der Fehlertext, `get()` wirft `TaskError`), der gespeicherte Erfolg bleibt; nach gelungener Markierung wird die Zustellung ohne DLQ-Kopie bestätigt (wie bei `UnroutableMessage`). Ist `sq:wf:{wf}` selbst unlesbar, bleibt es beim Restfall. Der FIRE_AND_FORGET-Pfad, auf dem der Workflow bei einer Fehlerantwort heute sofort und ohne Limit `running` bleibt, wird dabei mitgenommen. Die festschreibenden Tests dieser Regel bleiben unberührt; die Änderung kommt mit einem weiteren Nachtrag und eigenen Tests.

**Überdenken nur mit neuem Argument.** FS-12 (Fehlersuche Runde 2) ist behoben: Ein `cancel(task_id)` eines einzelnen Schritts, der vor dem Start im Cancel-Gate endet, markiert den Workflow jetzt vor dem Cancel-Ergebnis als `failed` (`tests/unit/worker/test_befund_fs12_cancel_before_start.py`); der Workflow-Cancel-Zweig bleibt unverändert (FS-32). Nebenbefund: Nach `handle.cancel()` bleibt `status()` auf `running`, wenn kein Schritt mehr fertig wird, während `get()` den Abbruch am Flag erkennt (`workflow_orchestration.py:172-178`).
