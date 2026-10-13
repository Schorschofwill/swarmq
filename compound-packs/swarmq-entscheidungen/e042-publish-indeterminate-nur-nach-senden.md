---
title: PublishIndeterminate nur, wenn der Frame gesendet wurde; was nachweislich nicht gesendet wurde, wird wiederholt oder endet als BrokerConnectionError
applies_when:
  - Änderungen an der Publish-Wiederholschleife, an der Auswertung von Verbindungsabrissen oder an der Publish-Endfrist
  - eine Fehlersuche meldet PublishIndeterminate für einen Publish, dessen Frame nie auf den Socket ging
  - ein Plan will Verbindungsfehler beim Publish pauschal als unbestimmt oder pauschal als wiederholbar behandeln
tags: [broker, publish, rabbitmq, aiormq, zusage]
id: E-042
datum: 2026-10-12
status: gilt
quelle:
  - Commit 2958d54 und 8bce711 (B24, Loadsuite v0.12.0rc1)
  - website/reference/changelog.de.md:154-173 (B24) und :181-188
  - website/reference/errors.de.md:466-472, :485-487
  - src/swarmq/broker/rabbitmq/delivery.py:187-195 (BrokerPublishBlocked: nichts gesendet, sicher wiederholbar)
tests:
  - tests/unit/broker/test_publish_connection_loss.py::test_connection_lost_before_the_frame_was_written_retries_on_a_new_channel
  - tests/unit/broker/test_publish_connection_loss.py::test_connection_lost_while_waiting_for_the_confirm_is_indeterminate_and_not_resent
  - tests/unit/broker/test_publish_connection_loss.py::test_a_reader_failure_before_the_frame_was_written_retries_on_a_new_channel
---

**Entscheidung.** swarmq unterscheidet beim Publish nach Phase. Ist die Nachricht auf den Socket geschrieben und reißt die Verbindung vor dem Publisher-Confirm, endet der Publish als `PublishIndeterminate` und wird nicht erneut gesendet. Riss die Verbindung, bevor der Frame geschrieben war, wiederholt swarmq auf einem neuen Kanal und meldet, wenn das nicht hilft, `BrokerConnectionError`. Das gilt für Client, Retry- und DLQ-Kopien, Workflow-Schritte, Scheduler-Termine und `swarmq dlq retry`.

**Begründung.** Ein Produzent, der wie für `BrokerConnectionError` dokumentiert wiederholt, darf einen schon angekommenen Task nicht doppelt ausführen. Umgekehrt soll ein nachweislich nicht gesendeter Publish nicht als „möglicherweise zugestellt“ gelten, weil das unnötige Zusatzläufe und stehende Sperren erzeugt.

**Bewusste Folgen.** Ein Publish ohne Wiederholung nach einem Abriss beim Confirm ist kein Fehler (siehe auch E-041).

**Überdenken nur mit neuem Argument.** Heute in Randfällen verletzt, zwei Befunde der Fehlersuche Runde 2 (xfail-Nachweise im Branch `test/fehlersuche-runde-2`):
- FS-08: Ein Publish, der in aiormqs Kanal-Sperre wartet, während die Verbindung reißt, hängt bis zur Frist und endet als `PublishIndeterminate`, obwohl sein Frame nie gesendet wurde.
- FS-02 Teil a: Bei schon vor Beginn verstrichener Endfrist wird trotzdem gesendet und `PublishIndeterminate` gemeldet.
