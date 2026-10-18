---
title: Der Fan-in einer Gruppe leitet die Ergebnis-Kennung eines Blatts ab, statt den Schritt-Marker nachzuschlagen; ein fehlendes Ergebnis ist WorkflowResultExpired
applies_when:
  - Änderungen an WorkflowAdvancer._terminal_result_id, _ordered_member_ids oder _refresh_step_tasks (workflow_orchestration.py) oder an _gather_results (workflow_failure.py)
  - eine Fehlersuche meldet einen Workflow, der beim Fan-in mit „resolved 1 of 2 member results“ scheitert, obwohl kein Schritt scheiterte
  - ein Plan will den Guard gegen gekürzte Ergebnislisten entfernen oder wieder auf Nachschlagen in step_tasks umstellen
tags: [workflow, gruppe, chord, fan-in, r4z-01, pf-3, f4-16]
id: E-073
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-3, Option a)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (R4Z-01, jetzt FEHLER F4-16; LÜCKE PF-3)
  - docs/plans/2026-08-30-002-fix-workflow-group-composite-members-plan.md:57 (KD1, für Blätter überholt), :348
  - Commit 4f8820d (workflow_step_task_id, deterministische Schritt-Kennung seit 0.1.2)
  - website/reference/changelog.md:2951-2957 (0.4.0, a wrong answer you cannot detect is worse than a visible failure)
tests:
  - tests/property/stateful/test_workflow_machine.py::test_unmarked_member_completing_first_completes_the_workflow
  - tests/integration/stateful/test_workflow_machine_valkey.py::test_unmarked_member_completing_first_completes_the_workflow_against_valkey
  - tests/unit/test_workflow_advancer.py::test_lost_marker_of_composite_member_still_completes
  - tests/unit/test_workflow_advancer.py::test_flat_group_with_late_marker_completes
  - tests/unit/test_workflow_advancer.py::test_chord_header_with_late_marker_feeds_the_body_every_member
  - tests/unit/test_workflow_advancer.py::test_missing_member_result_raises_expired_before_body_is_enqueued
  - tests/unit/test_workflow_advancer.py::test_derived_member_ids_match_the_enqueued_task_ids
---

**Entscheidung.** Der Fan-in einer Gruppe (auch eines Chord-Headers) leitet die Ergebnis-Kennung eines Blatts aus Workflow-Kennung und Pfad ab (`workflow_step_task_id`, in `_terminal_result_id` über `self._make_task_id(wf_id, path)`), statt den Schritt-Marker in `step_tasks` nachzuschlagen. Ketten und Chords laufen wie bisher auf ihr Abschlussblatt, Gruppen auf die Aggregatkennung (`aggregate_result_id`); damit ist jede Gliedkennung ableitbar. Die Vollständigkeit belegt die Fan-in-Zählung (`chord_complete.lua` zählt N abgeschlossene Glieder, bevor der Fan-in läuft). Fehlt das Ergebnis eines Glieds trotzdem, meldet `_gather_results` (`workflow_failure.py`) `WorkflowResultExpired`. KD1 des Plans 2026-08-30-002 („lautes Scheitern bei fehlendem Schritt-Marker im publish→mark-Fenster“) ist damit überholt; KD1 stand nie als Regel im Register.

**Begründung.** Seit 0.1.2 ist die Kennung eines Blatts deterministisch (4f8820d). Das Nachschlagen ließ einen Workflow auf `failed` kippen, wenn ein Glied fertig wurde, bevor die Neuzustellung des publizierenden Advance seinen Marker nachgetragen hatte (R4Z-01). Seit E-040/KTD11 ist dieses Fenster auch ohne Absturz erreichbar (Valkey-Aussetzer, 25 verlorene CAS-Runden); der Marker ist dort nur verspätet, nicht verloren. Die Ableitung schließt die gekürzte Liste ebenso aus wie der Guard aus 0.4.0, ohne den Workflow scheitern zu lassen: Ein stiller Rechenfehler bleibt ausgeschlossen, das Ziel des Changelogs 0.4.0 bleibt erfüllt.

**Bewusste Folgen.** Ein Glied, dessen Marker fehlt, dessen Ergebnis aber gespeichert ist, zählt beim Fan-in mit; der Workflow wird `completed`. Ein abgelaufenes oder verlorenes Ergebnis eines Glieds ergibt `WorkflowResultExpired`, nicht `WorkflowValidationError`. Bis zum Fix galt das Verhalten als Befund F4-16 (P3), Nachweis mit `xfail(strict=True)`. Der Fix (0.13.0) hat die Klammer `deliverable` der Workflow-Maschine und den Längen-Guard in `_ordered_member_ids` entfernt; `test_guard_catches_lost_marker_of_composite_member` (schrieb KD1 fest) ist zu `test_lost_marker_of_composite_member_still_completes` geworden, die beiden übrigen Guard-Tests zu `test_flat_group_with_late_marker_completes` und `test_chord_header_with_late_marker_feeds_the_body_every_member`. Der Docstring `_refresh_step_tasks` ist angepasst; KTD11 (Plan 2026-09-08-001) trägt seit U22 den Vermerk „für Blätter überholt durch E-073 (0.13.0)“, `workflows.md` „Failure modes“ ist seit U20 angepasst (EN und DE); beides erledigt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
