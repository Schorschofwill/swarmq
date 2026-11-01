---
title: Gibt aio-pika die Wiederherstellung eines vom Broker geschlossenen Publish-Kanals auf, ersetzt swarmq ihn erst nach der Bereitschaftsfrist (10 s); bis dahin warten die Publishes, ohne Fehler und ohne Verlust
applies_when:
  - eine Fehlersuche, ein Lastlauf oder ein Test sieht Publishes rund 10 s stehen, nachdem der Broker den Publish-Kanal kurz hintereinander zweimal geschlossen hat
  - Änderungen an _restored_in_time, _CHANNEL_READY_TIMEOUT_S oder dem Neuöffnen des Publish-Kanals (src/swarmq/broker/rabbitmq/connection.py)
  - ein Plan will die aufgegebene Wiederherstellung von aio-pika (in_restore_state) selbst erkennen
tags: [broker, aio-pika, publish]
id: E-091
datum: 2026-11-01
status: gilt
quelle:
  - "Georg 2026-11-01: „ok, mach weiter“ auf die Empfehlung (a) zu PF-22"
  - docs/befunde/fehlersuche-runde-4-2026-10.md (PF-22, V0161-K2, Gegenprüfung 0.16.1)
  - docs/befunde/saettigungsbuch.toml (S-012)
  - docs/befunde/teststrenge-2026-10.md (TS-06)
tests:
  - tests/integration/broker/test_entscheidung_e091_restore_abandoned.py::test_an_abandoned_restore_heals_after_the_ready_timeout_without_loss
---

**Entscheidung.** Schließt der Broker den Publish-Kanal ein zweites Mal, während aio-pika ihn nach dem ersten Schließen noch wiederherstellt (offenes `basic_qos` in `_on_open`), gibt aio-pika die Wiederherstellung auf (`in_restore_state`). swarmq erkennt das nicht eigens: Der nächste Publish wartet in `_restored_in_time` die Bereitschaftsfrist `_CHANNEL_READY_TIMEOUT_S` (10 s) ab und öffnet dann einen neuen Kanal. Bis dahin warten alle Publishes hinter `_channel_lock`; keiner scheitert, keine Nachricht geht verloren.

**Begründung.** Der Auslöser ist selten: Über den Default-Exchange schließt der Broker den Publish-Kanal etwa nur bei Nachrichten über `max_message_size`, und das zweimal innerhalb eines Qos-Roundtrips. Gemessen (Gegenprüfung 2026-11-01) gelingt der nächste Publish nach 10,06 s auf neuem Kanal. Die Alternative, die aufgegebene Wiederherstellung sofort zu erkennen (Kanal geschlossen, `__restored` nicht gesetzt, kein Wiederherstellungs-Task), griffe in aio-pikas privaten Zustand ein und bräche mit jeder Änderung an `RobustChannel`. Ein Ersatz während einer noch laufenden Wiederherstellung brächte dagegen genau die Waisen zurück, gegen die `_restored_in_time` steht (F4-22, F4-24).

**Bewusste Folgen.**
- In diesem Fall stehen die Publishes eines Prozesses bis zu 10 s; ein Aufrufer mit kürzerer eigener Frist sieht sein Timeout.
- Kein eigenes Signal und kein Log für den Fall; sichtbar ist nur die Publish-Dauer.

**Überdenken nur mit neuem Argument.** Etwa wenn aio-pika eine öffentliche Anzeige für die aufgegebene Wiederherstellung bietet oder selbst neu startet (Upstream-Hinweis, Option (c) in PF-22), oder wenn ein Lastlauf das Muster häufiger zeigt.
