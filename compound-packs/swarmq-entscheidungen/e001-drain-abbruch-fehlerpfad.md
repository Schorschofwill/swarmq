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
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:87 (Nachtrag Drain-Abbruch nach FS-02a, Georg 2026-10-14)
tests:
  - tests/unit/worker/compute/test_executor.py::test_outer_cancel_during_drain_kills_child_and_returns_error_result
  - tests/unit/worker/compute/test_executor.py::test_hard_timeout_and_drain_errors_have_no_error_class
---

**Entscheidung.** Bricht der Drain einen laufenden Handler ab, wird das wie ein Timeout als Fehlerergebnis abgebildet (Kind töten, Tod bestätigen, `ExecutionResult` mit Fehler). Der Abbruch wird nie als `CancelledError` propagiert. Retry, DLQ und Signale laufen über den normalen Fehlerpfad. Das gilt für IO- und CPU-Modus („Fehlerpfad wie heute“).

**Begründung.** Ein propagierter Abbruch träfe den Terminal-Default im `finally` von `_handle_message`, der den Schedfire-Completed-Marker und die Unique-Freigabe auf eine unbestätigte Nachricht schreibt (`docs/solutions/logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`). Gewählt statt „Cancel im Retry-Pfad“: Ein stornierter Task darf nicht bis `max_retries` erneut laufen.

**Bewusste Folgen.** Ein Drain-Abbruch verbraucht einen Retry. Bei `max_retries=0` landet der Task in der DLQ, ein Workflow-Schritt markiert den Workflow als fehlgeschlagen.

**Nachtrag 2026-10-14 (FS-02a, bis zur Klärung von FS-01).** Am Drain-Ende ist die Publish-Endfrist verstrichen. Seit FS-02a geht danach kein Publish mehr raus (`BrokerConnectionError`, nichts gesendet). Das gilt für den SIGTERM-Drain und seit FS-02b (U3) ebenso für den Recycling-Drain, dessen Publish-Endfrist auf seinem eigenen Drain-Ende liegt (Recycling-Frist ab Beginn des Herunterfahrens). Ein vom SIGTERM- oder Recycling-Drain abgebrochener Task endet weiter als Fehlerergebnis, und ein Workflow-Schritt markiert den Workflow weiter als fehlgeschlagen. Retry- bzw. DLQ-Kopie gehen aber nicht mehr raus: Das Original wird per `redeliver` neu zugestellt, ohne Retry-Verbrauch und ohne DLQ-Eintrag. Beim Recycling kann sich das bei jedem Recycling wiederholen, begrenzt durch das Delivery-Limit. Ein vom Drain abgebrochener Handler läuft mit `cancelling() > 0` weiter; `settle()` und der Sendeversuch im Publish-Pfad (`default_exchange.publish` in `_publish_attempts`) werten deshalb nur einen Anstieg von `cancelling()` gegenüber dem Eintritt als eigenen Abbruch (KTD9). Eine interne aio-pika-`CancelledError` dort (bei `redeliver` bzw. beim Senden von Retry-, DLQ- oder Advance-Publish) gilt als „nicht erledigt“ (`BrokerConnectionError`), nicht als Abbruch. Der Zeitdeckel der Publish-Frist hebt `cancelling()` an und endet weiter als `PublishIndeterminate`. Restlücke, unverändert gegenüber 0.12.0: Eine interne `CancelledError` aus dem Kanalaufbau (`_ensure_publish_channel`, Redeclare) fällt nicht unter die Regel und geht weiter als `CancelledError` durch. Bei `max_retries=0` im Workflow-Schritt kann so „Workflow `failed`, Schrittergebnis `success`“ entstehen, ohne DLQ-Eintrag als Erklärung (`fix-r-flow.md` 2.1). Festgehalten in `tests/unit/worker/test_fs02_worker_after_publish_deadline.py` (ohne `entscheidung`-Marker).

**Überdenken nur mit neuem Argument.** Offen: FS-01 (Fehlersuche Runde 2). KTD2 setzt voraus, dass Retry und DLQ nach dem Abbruch normal durchlaufen. Am Drain-Ende ist die Publish-Endfrist aber schon verstrichen, sodass Retry- und DLQ-Kopie nicht mehr rausgehen und das Original stattdessen neu zugestellt wird (Nachtrag oben). Dass Fehlerergebnis, DLQ-Kopie und erneuter Lauf zusammenkommen, ist mit FS-02 (U3) behoben. Außerdem widerspricht `website/explanation/delivery-semantics.md:267-268` („cancelled and redelivered“) dieser Entscheidung.
