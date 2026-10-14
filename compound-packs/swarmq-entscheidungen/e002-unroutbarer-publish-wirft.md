---
title: Ein unroutbarer Publish wirft UnroutableMessage statt still verloren zu gehen
applies_when:
  - ein Publish an eine nicht deklarierte Queue (Producer vor Worker, umbenannte Queue, Priorität ohne --priorities)
  - Änderungen an Publish-Channel, Return-Erkennung, Neu-Deklaration oder Workflow-Advance
  - eine Fehlersuche meldet, dass Producer nach einem Upgrade UnroutableMessage bekommen, wo es früher „funktionierte“
  - eine Fehlersuche meldet, dass ein Workflow-Schritt bei unroutbarem Advance sofort fehlschlägt statt neu zugestellt zu werden
tags: [broker, publish, unroutable, workflow, bz-03]
id: E-002
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:124 (Entscheidungen, BZ-03)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:347 (MR 6 – BZ-03)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:700 (Abschnitt 7, „Worker vor Producern“)
tests:
  - tests/unit/broker/test_unroutable.py::test_returned_publish_raises_unroutable_without_retry_and_keeps_channel
  - tests/unit/broker/test_unroutable.py::test_retry_later_redeclares_own_queue_on_throwaway_channel_and_retries
  - tests/unit/broker/test_unroutable.py::test_second_unroutable_publish_raises_after_a_single_redeclare
  - tests/unit/worker/test_results.py::test_unroutable_advance_fail_marks_workflow_and_acks_without_redeliver
---

**Entscheidung.** Publish-Channels laufen mit `on_return_raises=True`; ein vom Broker zurückgegebener Publish wird sofort zu `UnroutableMessage` (Unterklasse von `BrokerConnectionError`), ohne Wiederholung. Der Worker deklariert seine eigene Queue einmal auf einem Wegwerf-Channel neu und versucht es ein zweites Mal; ein zweites Unroutable geht durch. Ein Workflow-Advance mit `UnroutableMessage` markiert den Schritt sofort als fehlgeschlagen, ohne Redeliver.

**Begründung.** Vorher ging die Nachricht still verloren und der Producer bekam eine Erfolgsquittung (BZ-03). Ein lauter Fehler ist besser als ein stiller Verlust; ein Redeliver würde am fehlenden Ziel nichts ändern.

**Bewusste Folgen.** „Worker vor Producern“ gilt dauerhaft für jede neue Queue: Ein Producer, der vor dem ersten Worker sendet, bekommt `UnroutableMessage`. Ein Prioritäts-Publish ohne `--priorities` wirft. Scheitert nur die Fail-Markierung des Workflows vorübergehend, wird doch neu zugestellt (begrenzt durch das Delivery-Limit).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
