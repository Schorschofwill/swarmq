---
title: Ein am Drain-Ende abgebrochener Task endet als Fehlerergebnis, nicht als Neuzustellung
applies_when:
  - ein Task läuft beim Shutdown oder Recycling länger als der Drain und wird abgebrochen
  - Änderungen am Drain, am ComputeExecutor oder an der Auswertung abgebrochener Handler
  - eine Fehlersuche meldet, dass ein Drain-Abbruch Retry, DLQ oder Workflow-Fail auslöst
tags: [shutdown, drain, cpu-modus, retry, dlq]
id: E-001
datum: 2026-09-03
status: gilt
quelle:
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md (KTD2, user-approved)
tests:
  - tests/unit/worker/compute/test_executor.py::test_outer_cancel_during_drain_kills_child_and_returns_error_result
  - tests/unit/worker/compute/test_executor.py::test_hard_timeout_and_drain_errors_have_no_error_class
---

**Entscheidung.** Bricht der Drain einen laufenden Handler ab, wird das wie ein Timeout als Fehlerergebnis abgebildet (Kind töten, Tod bestätigen, `ExecutionResult` mit Fehler). Der Abbruch wird nie als `CancelledError` propagiert. Retry, DLQ und Signale laufen über den normalen Fehlerpfad. Das gilt für IO- und CPU-Modus („Fehlerpfad wie heute“).

**Begründung.** Ein propagierter Abbruch träfe den Terminal-Default im `finally` von `_handle_message`, der den Schedfire-Completed-Marker und die Unique-Freigabe auf eine unbestätigte Nachricht schreibt (`docs/solutions/logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`). Gewählt statt „Cancel im Retry-Pfad“: Ein stornierter Task darf nicht bis `max_retries` erneut laufen.

**Bewusste Folgen.** Ein Drain-Abbruch verbraucht einen Retry. Bei `max_retries=0` landet der Task in der DLQ, ein Workflow-Schritt markiert den Workflow als fehlgeschlagen.

**Überdenken nur mit neuem Argument.** Offen: FS-01 (Fehlersuche Runde 2). KTD2 setzt voraus, dass Retry und DLQ nach dem Abbruch normal durchlaufen. Am Drain-Ende ist die Publish-Endfrist aber schon verstrichen, sodass Fehlerergebnis, DLQ-Kopie und erneuter Lauf zusammenkommen (siehe FS-02). Außerdem widerspricht `website/explanation/delivery-semantics.md:267-268` („cancelled and redelivered“) dieser Entscheidung.
