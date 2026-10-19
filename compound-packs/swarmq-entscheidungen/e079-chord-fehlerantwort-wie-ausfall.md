---
title: Eine Fehlerantwort des Chord-Skripts unter dem Zustelllimit wird wie ein Ausfall neu zugestellt, nicht sofort fail-markiert
applies_when:
  - Änderungen an der Fehlerbehandlung des Advance im Ergebnis-Tail (worker/results.py) oder an ChordManager (backend/valkey/chords.py)
  - eine Fehlersuche meldet, dass ein Workflow-Schritt bei WRONGTYPE auf dem Chord- oder Ergebnis-Key bis zum Zustelllimit erneut läuft statt den Workflow sofort als failed zu markieren
  - ein Plan will BackendResponseError im Advance anders behandeln als BackendConnectionError
  - Änderungen an der Einstufung von Valkey-Fehlerantworten als Ausfall (backend/valkey/connection.py, as_backend_error)
tags: [workflow, chord, valkey, fehlerantwort, zustelllimit, f4-11, kd7]
id: E-079
datum: 2026-10-19
status: gilt
quelle:
  - docs/plans/2026-10-17-2103-fix-fehlersuche-runde-4-plan.md (KD7, R2, KTD3; KD9 Release-Lücke)
  - 'Georg 2026-10-19 (Entscheidungsvorlage 0.13.0, Punkt 1: Option A jetzt, B in 0.13.1)'
  - docs/befunde/entscheidungsvorlage-0-13-0-2026-10-19.md (Punkt 1)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (F4-11; Entscheidung überdenken? Punkt 5)
  - src/swarmq/backend/valkey/chords.py (Fehler über as_backend_error, BackendResponseError ⊂ BackendConnectionError)
  - 'Nachtrag 2026-10-19: umgesetzt in 0.13.1 (src/swarmq/worker/terminal.py _advance_done_step, src/swarmq/worker/results.py _advance_workflow)'
tests:
  - tests/unit/worker/test_befund_f4_11_advance_redelivers_on_chord_outage.py::test_error_reply_in_the_chord_script_redelivers_instead_of_failing_the_workflow
  - tests/unit/worker/test_terminal.py::test_error_reply_of_the_last_advance_at_the_limit_fails_the_workflow_and_acks
  - tests/unit/worker/test_terminal.py::test_failing_fail_mark_after_an_error_reply_at_the_limit_dead_letters
  - tests/unit/worker/test_terminal.py::test_error_reply_of_the_catch_up_below_the_limit_still_redelivers
  - tests/unit/test_befund_f4_01_workflow_dead_letter_after_unadvanced_success.py::test_at_the_limit_an_error_reply_of_the_last_advance_fails_the_workflow_and_keeps_the_success
  - tests/unit/worker/test_results.py::test_error_reply_in_the_advance_of_a_fire_and_forget_step_fails_the_workflow
  - tests/unit/worker/test_results.py::test_outage_in_the_advance_of_a_fire_and_forget_step_does_not_fail_the_workflow
---

**Entscheidung.** Antwortet das Chord-Skript (`chord_complete.lua`) beim Advance eines Gruppen- oder Chord-Glieds mit einer Fehlerantwort (`BackendResponseError`, etwa `WRONGTYPE` auf dem Chord- oder Ergebnis-Key), behandelt der Worker das wie einen Ausfall von Valkey: Der Advance gilt als vorübergehend gescheitert, die Nachricht wird neu zugestellt (`workflow_advance_backend_error`, WARNING, am Limit ERROR). Unter dem Zustelllimit gibt es kein sofortiges Fail-Mark. Am Limit gilt E-040 (Nachtrag 2026-10-17 und 2026-10-19).

**Begründung.** KD7 im Fix-Plan Runde 4 (session-settled, user-directed): konsistent mit den übrigen Stores, die Fehlerantworten ebenfalls über `as_backend_error` als `BackendConnectionError` melden; keine eigene Einstufung je Fehlertext. Die Fehlerantworten, die ein echter Ausfall erzeugt (`OOM`, `READONLY`, `LOADING`, `TRYAGAIN`, `CLUSTERDOWN`, `MASTERDOWN`, `MISCONF`), müssen ohnehin neu zugestellt werden; übrig bleibt nur ein falscher Key-Typ bei intaktem `sq:wf:{wf}`, also eine manuelle Änderung, ein fremder Schreiber oder ein künftiger Bug (KD9: praktisch nur durch manuelle Key-Manipulation erreichbar).

**Bewusste Folgen.** Eine dauerhafte Fehlerantwort lässt den Schritt bis zum Zustelllimit rund `delivery_limit`-mal vollständig neu laufen. Am Limit geht die Nachricht ohne Fehlerergebnis in die DLQ, der gespeicherte Erfolg bleibt (F4-01), und der Workflow bleibt bis 0.13.1 `running` bis zu seiner TTL (E-040, Restfall). Das ist kein Fehler. Sichtbar ist der Fall über `workflow_advance_backend_error` mit `workflow_id` und Fehlertext. Abhilfe: den veränderten Key reparieren oder löschen.

**Überdenken nur mit neuem Argument.** Für 0.13.1 entschieden (Georg 2026-10-19, Option B der Vorlage): Am Zustelllimit markiert der Worker den Workflow bei `BackendResponseError` als `failed` (`get()` wirft `TaskError` mit dem Fehlertext), der Erfolg bleibt; nach gelungener Markierung ACK ohne DLQ-Kopie (wie `UnroutableMessage`); der FIRE_AND_FORGET-Pfad, auf dem der Workflow heute sofort ohne Limit `running` bleibt, wird mitgenommen. Diese Regel bleibt dabei bestehen: Unter dem Limit wird weiter neu zugestellt. Das neue Argument dafür: Lock- und Rate-Limit-Gate enden bei derselben Fehlerantwort schon mit `failed`, nur der Advance-Pfad endet in `running`.

**Nachtrag 2026-10-19 (0.13.1, Option B der Entscheidungsvorlage umgesetzt).** Unter dem Zustelllimit gilt die Regel unverändert: Eine Fehlerantwort wird wie ein Ausfall neu zugestellt (festgeschrieben auch für den nachgeholten Advance in `fail_workflow_step`). Neu:
- **Am Zustelllimit** (`fail_workflow_step` mit `redeliver=False`): Scheitert der letzte Advance mit `BackendResponseError` (kein Ausfall im Sinn von `is_backend_outage`), markiert der Worker den Workflow als `failed`, mit `error_type` `BackendResponseError` und dem Fehlertext als Ursache; `get()` wirft `TaskError` mit diesem Text. Gelingt die Markierung, wird die Zustellung **ohne DLQ-Kopie** und ohne `TASK_DLQ` bestätigt (`StepDone.ACK`, wie bei `UnroutableMessage`), das Unique-Flag wird wie nach einer DLQ-Kopie freigegeben. Der gespeicherte Erfolg des Schritts bleibt und wird nie überschrieben. Scheitert die Markierung (etwa weil `sq:wf:{wf}` selbst einen falschen Typ hat), bleibt es beim Restfall: DLQ-Kopie ohne Fehlerergebnis, Workflow `running` bis zur TTL (`StepDone.DEAD_LETTER`). Ein Ausfall, eine verlorene CAS-Runde (`WorkflowStateConflict`) oder ein gescheiterter Publish am Limit bleiben ebenfalls beim Restfall.
- **FIRE_AND_FORGET:** Ein FAF-Schritt wird nicht neu zugestellt, eine Fehlerantwort heilt dort also nie. Scheitert sein Advance mit `BackendResponseError`, markiert der Ergebnis-Tail den Workflow sofort als `failed` (wie bei `UnroutableMessage`, Event `workflow_fail_mark_error`, falls die Markierung scheitert). Ein Ausfall bleibt unverändert: kein Fail-Mark, der Workflow bleibt `running`.
- **Logs:** Kein neues Event. `workflow_advance_backend_error` (ERROR am Limit und bei FAF) geht der Markierung voraus; scheitert sie, folgt wie bisher `workflow_fail_mark_failed` bzw. `workflow_fail_mark_error`.

