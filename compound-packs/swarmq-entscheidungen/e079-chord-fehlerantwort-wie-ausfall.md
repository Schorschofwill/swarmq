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
tests:
  - tests/unit/worker/test_befund_f4_11_advance_redelivers_on_chord_outage.py::test_error_reply_in_the_chord_script_redelivers_instead_of_failing_the_workflow
---

**Entscheidung.** Antwortet das Chord-Skript (`chord_complete.lua`) beim Advance eines Gruppen- oder Chord-Glieds mit einer Fehlerantwort (`BackendResponseError`, etwa `WRONGTYPE` auf dem Chord- oder Ergebnis-Key), behandelt der Worker das wie einen Ausfall von Valkey: Der Advance gilt als vorübergehend gescheitert, die Nachricht wird neu zugestellt (`workflow_advance_backend_error`, WARNING, am Limit ERROR). Unter dem Zustelllimit gibt es kein sofortiges Fail-Mark. Am Limit gilt E-040 (Nachtrag 2026-10-17 und 2026-10-19).

**Begründung.** KD7 im Fix-Plan Runde 4 (session-settled, user-directed): konsistent mit den übrigen Stores, die Fehlerantworten ebenfalls über `as_backend_error` als `BackendConnectionError` melden; keine eigene Einstufung je Fehlertext. Die Fehlerantworten, die ein echter Ausfall erzeugt (`OOM`, `READONLY`, `LOADING`, `TRYAGAIN`, `CLUSTERDOWN`, `MASTERDOWN`, `MISCONF`), müssen ohnehin neu zugestellt werden; übrig bleibt nur ein falscher Key-Typ bei intaktem `sq:wf:{wf}`, also eine manuelle Änderung, ein fremder Schreiber oder ein künftiger Bug (KD9: praktisch nur durch manuelle Key-Manipulation erreichbar).

**Bewusste Folgen.** Eine dauerhafte Fehlerantwort lässt den Schritt bis zum Zustelllimit rund `delivery_limit`-mal vollständig neu laufen. Am Limit geht die Nachricht ohne Fehlerergebnis in die DLQ, der gespeicherte Erfolg bleibt (F4-01), und der Workflow bleibt bis 0.13.1 `running` bis zu seiner TTL (E-040, Restfall). Das ist kein Fehler. Sichtbar ist der Fall über `workflow_advance_backend_error` mit `workflow_id` und Fehlertext. Abhilfe: den veränderten Key reparieren oder löschen.

**Überdenken nur mit neuem Argument.** Für 0.13.1 entschieden (Georg 2026-10-19, Option B der Vorlage): Am Zustelllimit markiert der Worker den Workflow bei `BackendResponseError` als `failed` (`get()` wirft `TaskError` mit dem Fehlertext), der Erfolg bleibt; nach gelungener Markierung ACK ohne DLQ-Kopie (wie `UnroutableMessage`); der FIRE_AND_FORGET-Pfad, auf dem der Workflow heute sofort ohne Limit `running` bleibt, wird mitgenommen. Diese Regel bleibt dabei bestehen: Unter dem Limit wird weiter neu zugestellt. Das neue Argument dafür: Lock- und Rate-Limit-Gate enden bei derselben Fehlerantwort schon mit `failed`, nur der Advance-Pfad endet in `running`.
