---
title: Reißt die Frist eines gesendeten Publish, verwirft swarmq den Publish-Kanal; alle anderen offenen Confirms auf ihm enden als PublishIndeterminate, der nächste Publish läuft auf einem neuen Kanal
applies_when:
  - Änderungen an _publish_with_retry oder _publish_attempts (broker/rabbitmq/delivery.py) oder an _discard_publish_channel (broker/rabbitmq/connection.py)
  - eine Fehlersuche meldet einen Schub PublishIndeterminate oder retry_publish_failed[PublishIndeterminate] auf einem Worker, ausgelöst durch einen einzigen publish_deadline_exceeded
  - eine Fehlersuche meldet, dass Publishes mit laufender eigener Frist als unbestimmt enden, typischerweise bei der Aufhebung eines Ressourcenalarms
  - ein Plan will nach einem Fristriss nur den einen Publish abbrechen und den Kanal behalten
tags: [broker, publish, deadline, kanal, rabbitmq, at-least-once]
id: E-078
datum: 2026-10-18
status: gilt
quelle:
  - Georg 2026-10-18 (Loadsuite-Abnahme 0.13.0rc1, offener Punkt 1 „Kanalverwurf nach Fristriss“, gewollt, ins Register)
  - swarmq-loadsuite-stoerungen/docs/testlauf-v0.13.0rc1.md (Abschnitt „15 E-041-Belege statt 2–3“, broker_alarm 19:08:10Z, worker-io-2)
  - src/swarmq/broker/rabbitmq/delivery.py, _publish_with_retry (Verwurf nach TimeoutError, R10 aus Plan 2026-08-28-001)
  - src/swarmq/broker/rabbitmq/connection.py, _discard_publish_channel und _close_channel_detached
tests:
  - tests/unit/broker/test_publish_deadline_channel_discard.py::test_a_deadline_breach_discards_the_channel_and_the_other_open_confirms_end_indeterminate
---

**Entscheidung.** Reißt die Publish-Frist (`SWARMQ_PUBLISH_TIMEOUT_S`, im Shutdown die Endfrist) eines Publish, der schon gesendet haben kann, verwirft swarmq den gemeinsamen Publish-Kanal und schließt ihn im Hintergrund (`_discard_publish_channel`, R10). Das Schließen lehnt in aiormq alle offenen Confirm-Futures des Kanals ab. Jeder andere Publish, der auf diesem Kanal gesendet hat und noch auf sein Confirm wartet, endet darum als `PublishIndeterminate` („lost the connection while waiting for the broker's confirm“), auch wenn seine eigene Frist noch läuft. Er wird nicht erneut gesendet (E-042). Der nächste Publish öffnet einen neuen Kanal. Für Retry-, DLQ- und Zurückstellungskopien des Workers gilt dann der E-041-Pfad: Das Original bleibt unbestätigt und wird neu zugestellt.

**Begründung.** Nach einem Fristriss ist der Kanal nicht mehr vertrauenswürdig: Ein Confirm, das nicht in der Frist kam, deutet auf einen still toten Transport oder einen hängenden Broker (der Referenzlauf vom 2026-08-28 stand 62 Minuten). Bliebe der Kanal stehen, verbrennte jeder Folge-Publish erneut die volle Frist. Einen einzelnen Publish auf einem AMQP-Kanal abzubrechen, ohne den Kanal zu schließen, geht nicht: Publisher-Confirms laufen über fortlaufende Delivery-Tags je Kanal, ein verspätet eintreffendes Ack ließe sich nicht mehr sicher dem richtigen Publish zuordnen. Die Mitnahme der übrigen Confirms kostet keine Doppelausführung: Ihre Kopien sind beim Broker und laufen, der Ausführungsschutz lässt das neu zugestellte Original nicht neben oder nach der Kopie laufen. Beleg aus der Loadsuite-Abnahme 0.13.0rc1 (`broker_alarm`, `worker-io-2`): Bei der Aufhebung des Alarms riss die Frist einer Retry-Kopie, die fast die ganzen 20 s im Alarm gewartet hatte, 11 ms nach dem Senden; mit dem Verwurf endeten 12 weitere Kopien auf demselben Kanal als unbestimmt. Alle 13 Originale wurden neu zugestellt und als `done` übersprungen, kein Duplikat ausgeführt, kein Verlust.

**Bewusste Folgen.** Ein einziger Fristriss kann einen Schub `PublishIndeterminate` (im Worker `retry_publish_failed[PublishIndeterminate]`) erzeugen, bei der Aufhebung eines Ressourcenalarms einen ganzen Schub wartender Kopien. Die zusätzlichen Neuzustellungen erhöhen `duplicate_skipped` und kosten je Original einen kurzen Lauf durch die Gates. Ob es dazu kommt, ist ein Zeitzufall: Reißt bei der Aufhebung keine Frist, gehen alle wartenden Kopien durch. Das ist kein Fehler gegen E-041, E-042 oder E-025. Bei einem Produzenten, der viele Publishes parallel über eine Verbindung schickt, gilt dasselbe: Mehrere `PublishIndeterminate` aus einem Fristriss sind erwartet. Ohne Ausführungsschutz (etwa nach einem Valkey-Datenverlust, E-077) kann ein neu zugestelltes Original neben seiner Kopie laufen; das ist at-least-once.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Eine Alternative wäre, nach dem Fristriss die übrigen Confirms bis zu ihrer eigenen Frist abzuwarten und erst dann zu verwerfen; das verlängerte den Stillstand auf einem toten Transport und ist nicht geprüft.
