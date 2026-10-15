---
title: Ein FIRE_AND_FORGET-Schritt wird beim Bau eines Workflows abgelehnt; per sig() oder mit result_ttl=0 lässt er den Workflow zur Laufzeit scheitern
applies_when:
  - Änderungen an Signature-Konstruktion (Task.s, Task.si, sig) oder am Ergebnis-Tail für Workflow-Schritte ohne Ergebnis
  - eine Fehlersuche meldet, dass ein Workflow mit FAF- oder result_ttl=0-Schritt als failed endet
  - ein Plan will FAF-Schritte in Workflows zulassen oder result_ttl=0 bei Task.s() ablehnen
tags: [workflow, fire-and-forget, result-ttl, validation]
id: E-039
datum: 2026-05-30
status: gilt
quelle:
  - docs/plans/2026-05-30-001-feat-workflow-primitives-plan.md:86 („Workflow-Schritte daher AT_LEAST_ONCE, nie FIRE_AND_FORGET“), :214 (FAF-Ablehnung, Runtime-Guard U5), :339, :466
  - website/guides/workflows.md:221 und :238 (Failure modes)
  - src/swarmq/worker/results.py:114-119 (Docstring _finish_without_store, R4.1 U5)
tests:
  - tests/unit/test_workflow.py::test_faf_signature_rejected_in_chain
  - tests/unit/test_workflow.py::test_faf_signature_rejected_in_group
  - tests/unit/test_workflow.py::test_faf_signature_rejected_as_chord_body
  - tests/unit/worker/test_results.py::test_result_ttl_zero_stores_nothing_and_fails_faf_workflow_step
---

**Entscheidung.** Workflow-Schritte müssen AT_LEAST_ONCE sein und ein Ergebnis speichern. Ein `Task.s()`/`Task.si()` eines FIRE_AND_FORGET-Tasks wird beim Bau von chain, group oder chord mit `WorkflowValidationError` abgelehnt. Was sich erst zur Laufzeit zeigt (ein per `sig()` referenzierter FAF-Task oder ein Schritt mit `result_ttl=0`), markiert den Workflow als `failed` mit dem Fehlertyp `WorkflowValidationError`; `get()` wirft `TaskError`.

**Begründung.** Ein FAF-Task bestätigt vor dem Lauf und erreicht die Advance-Naht nie; ein Schritt ohne gespeichertes Ergebnis kann den nächsten Schritt nicht speisen. Ohne Guard hinge der Workflow still bis zum `get()`-Timeout.

**Bewusste Folgen.** Ein Workflow, der wegen eines AT_LEAST_ONCE-Schritts mit `result_ttl=0` zur Laufzeit scheitert, ist kein Fehler. `TaskInfo(result_ttl=0)` mit AT_LEAST_ONCE bleibt außerhalb von Workflows zulässig.

**Überdenken nur mit neuem Argument.** FS-28 betraf nur den Meldungstext: Bei `result_ttl=0` nannte `results.py` fälschlich „is FIRE_AND_FORGET“. Behoben in U17 (Commit 29f9a3e): Die Meldung nennt jetzt die Ursache („is FIRE_AND_FORGET“ bzw. „stores no result (result_ttl=0)“), Nachweis `tests/unit/worker/test_befund_fs28_no_store_step_message.py`. Das Verhalten blieb. Ob `result_ttl=0` schon bei `Task.s()` abgelehnt werden soll, ist eine offene Produktfrage.
