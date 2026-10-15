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
  - 'src/swarmq/broker/rabbitmq/delivery.py:187-195 (BrokerPublishBlocked: nichts gesendet, sicher wiederholbar)'
  - Commit 67cb34b (U8, FS-08) und Review 44722f6 (Fail-open ohne lesbares closing)
  - src/swarmq/broker/rabbitmq/delivery.py:122-138 (_closing_of), :424-435 (_report_closing_unreadable)
tests:
  - tests/unit/broker/test_publish_connection_loss.py::test_connection_lost_before_the_frame_was_written_retries_on_a_new_channel
  - tests/unit/broker/test_publish_connection_loss.py::test_connection_lost_while_waiting_for_the_confirm_is_indeterminate_and_not_resent
  - tests/unit/broker/test_publish_connection_loss.py::test_a_reader_failure_before_the_frame_was_written_retries_on_a_new_channel
  - tests/unit/broker/test_publish_channel_closing.py::test_an_unreadable_closing_falls_back_with_one_warning_per_broker
---

**Entscheidung.** swarmq unterscheidet beim Publish nach Phase. Ist die Nachricht auf den Socket geschrieben und reißt die Verbindung vor dem Publisher-Confirm, endet der Publish als `PublishIndeterminate` und wird nicht erneut gesendet. Riss die Verbindung, bevor der Frame geschrieben war, wiederholt swarmq auf einem neuen Kanal und meldet, wenn das nicht hilft, `BrokerConnectionError`. Das gilt für Client, Retry- und DLQ-Kopien, Workflow-Schritte, Scheduler-Termine und `swarmq dlq retry`.

**Begründung.** Ein Produzent, der wie für `BrokerConnectionError` dokumentiert wiederholt, darf einen schon angekommenen Task nicht doppelt ausführen. Umgekehrt soll ein nachweislich nicht gesendeter Publish nicht als „möglicherweise zugestellt“ gelten, weil das unnötige Zusatzläufe und stehende Sperren erzeugt.

**Bewusste Folgen.** Ein Publish ohne Wiederholung nach einem Abriss beim Confirm ist kein Fehler (siehe auch E-041).

**Nachtrag 2026-10-14 (FS-08, Review U8, Commit 44722f6): Fail-open ohne lesbares `closing`.** Der Wächter gegen die Kanal-Sperre (FS-08) braucht das `closing`-Future des aiormq-Kanals. Ist es an einem echten aio-pika-Kanal nicht lesbar (aio-pika oder aiormq geändert), publiziert swarmq ohne Wächter weiter wie vor FS-08 und meldet das je Broker einmal mit der WARNING `publish_channel_closing_unreadable`. Gewählt statt eines harten Fehlers, analog zum Fail-open beim unlesbaren Blocked-Zustand (E-025, BZ-16): Ein Versionswechsel der Bibliothek soll keinen Publish verhindern. Bewusste Folge: In diesem Zustand kann ein Publish, der in der Kanal-Sperre wartet, bei einem Verbindungsabriss wieder bis zur Frist hängen und als `PublishIndeterminate` enden; die Warnung macht das sichtbar. Test-Doubles ohne aio-pika-Typ bleiben still.

**Überdenken nur mit neuem Argument.** Befunde der Fehlersuche Runde 2 (xfail-Nachweise im Branch `test/fehlersuche-runde-2`):
- FS-08, behoben (U8, Plan 2026-10-14-0748): Ein Publish, der in aiormqs Kanal-Sperre wartete, während die Verbindung riss, hing bis zur Frist und endete als `PublishIndeterminate`, obwohl sein Frame nie gesendet wurde. Seit U8 läuft jeder Versuch gegen `closing` seines Kanals. Schließt der Kanal zuerst, wird der Versuch abgebrochen: mit geschriebenem Frame `PublishIndeterminate`, sonst neuer Kanal bzw. `BrokerConnectionError`. Reißt die Frist, bevor gesendet sein kann, folgt ebenfalls `BrokerConnectionError` (Ergänzung a3); das gilt auch für einen Versuch, der beim Reißen noch in der Sperren-Warteschlange steht, solange kein früherer Versuch womöglich gesendet hat (Review U8).
- FS-02 Teil a, behoben (U3, Plan 2026-10-14-0748): Bei schon vor Beginn verstrichener Endfrist wurde trotzdem gesendet und `PublishIndeterminate` gemeldet. Seit U3 geht nichts raus, der Aufrufer bekommt `BrokerConnectionError`.
