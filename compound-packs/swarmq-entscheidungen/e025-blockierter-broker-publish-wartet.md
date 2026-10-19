---
title: Ein Publish bei blockiertem Broker sendet nichts, wartet bis zur Publish-Frist und scheitert mit BrokerPublishBlocked; Worker nutzen zwei Verbindungen
applies_when:
  - RabbitMQ meldet connection.blocked (Speicher- oder Plattenalarm)
  - Änderungen an broker/rabbitmq/delivery.py, blocked.py, connection.py oder SWARMQ_BROKER_PUBLISH_CONNECTION
  - eine Fehlersuche meldet, dass der erste Publish während eines Alarms als PublishIndeterminate endet
  - eine Fehlersuche meldet, dass ein Worker zwei AMQP-Verbindungen öffnet
  - eine Fehlersuche meldet einbrechenden Konsum eines Workers im Ressourcenalarm (Slots von wartenden Retry- oder DLQ-Kopien belegt)
  - eine Fehlersuche meldet die aiormq-Zeile „Got frame for closed channel“ oder publish_channel_close_incomplete im Alarm
tags: [broker, publish, blocked, verbindung, bz-16]
id: E-025
datum: 2026-10-11
status: gilt
quelle:
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:77 (Nachtrag MR 16, Blocked wartet bis zur Frist, zwei Verbindungen je Worker, Georg 2026-10-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:580 (MR 16, BZ-16)
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-18, Option a; PF-19, Option b; D-1)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-18, PF-19; Vor dem Fix Punkt 15)
  - website/reference/errors.md:573-576 (Grenze der Blockade-Erkennung, D-1)
  - docs/plans/2026-10-19-1500-feat-swarmq-0-14-plan.md (R6, KTD5, U6; PF-19 a, Georg 2026-10-19 „mach das alles“)
  - docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md (Spike PF-19 a, Umsetzung 0.14)
tests:
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseDuringAlarm::test_close_into_the_deadline_during_an_alarm_logs_without_traceback
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseDuringAlarm::test_close_into_the_deadline_outside_an_alarm_keeps_the_traceback
  - tests/unit/broker/test_publish_blocked.py::test_blocked_until_the_deadline_raises_publish_blocked
  - tests/unit/broker/test_publish_blocked.py::test_unblocked_connection_publishes_without_waiting_or_roundtrip
  - tests/unit/broker/test_publish_blocked.py::test_unreadable_state_fails_open_with_one_warning_per_broker
  - tests/unit/broker/test_publish_connection.py::test_injected_connection_is_always_shared
  - tests/unit/broker/test_publish_connection.py::test_first_consume_opens_a_second_connection_for_the_consume_channel
  - tests/unit/test_config_publish_connection.py::test_default_is_separate
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseAfterUnblocked::test_the_close_waits_for_unblocked_and_runs_once_after_it
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseAfterUnblocked::test_an_alarm_outlasting_the_publish_deadline_closes_after_it_without_traceback
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseAfterUnblocked::test_a_connection_ending_during_the_wait_ends_the_close_task
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseAfterUnblocked::test_an_unreadable_closing_future_closes_at_once
  - tests/unit/broker/test_rabbitmq.py::TestChannelCloseAfterUnblocked::test_closing_the_broker_during_the_wait_cancels_the_close_without_leftovers
  - tests/integration/broker/test_pf19_close_after_unblocked.py::test_the_discarded_channel_closes_after_unblocked_without_aiormq_error
  - tests/integration/broker/test_pf19_close_after_unblocked.py::test_a_connection_ending_during_the_alarm_ends_the_waiting_close
  - tests/chaos/test_publish_deadline.py::TestDeferredCloseUnderAlarm::test_a_proxy_cut_during_the_alarm_ends_the_deferred_close
---

**Entscheidung.** Ist die Verbindung blockiert, sendet ein Publish nichts, wartet bis zur Publish-Frist und scheitert dann mit `BrokerPublishBlocked` (Unterklasse von `BrokerConnectionError`); Verbindung und Publish-Kanal bleiben. Eine unblockierte Verbindung kostet keinen Roundtrip; ist der Zustand nicht lesbar, gilt fail-open mit einer WARNING. Worker öffnen zwei Verbindungen (`SWARMQ_BROKER_PUBLISH_CONNECTION=separate`, Default; die Consume-Verbindung entsteht beim ersten `consume()`); bei injizierter `connection=`/`connection_pool=` gilt immer `shared`.

**Begründung.** Publish und Konsum liefen über eine Verbindung; bei einem Broker-Alarm hingen auch die Acks (BZ-16). Mit getrennten Verbindungen können Worker weiter quittieren.

**Bewusste Folgen.** Grenze: RabbitMQ meldet die Blockade erst beim ersten Publish während des Alarms; dieser liegt schon auf dem Socket und endet als `PublishIndeterminate`. Verbindungslimits (`connection_max`, vhost-`max-connections`, Benutzerlimits) und Alarme auf die Verbindungszahl sind vor dem Rollout zu prüfen.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; PF-18, PF-19, D-1).** (1) *Grenze (D-1).* Der auslösende Publish, mit dem RabbitMQ die Blockade erst meldet, kann trotzdem bestätigt werden; unbestimmt (`PublishIndeterminate`) enden nur die Publishes, die vor dem Eintreffen von `connection.blocked` gesendet wurden, je Verbindung null, einer oder mehrere. Die Bewusste Folge „dieser endet als `PublishIndeterminate`“ ist in diesem Sinn zu lesen. (2) *Slot und Konsum (PF-18, R4I-02).* Eine Retry- oder DLQ-Kopie wartet im Alarm in ihrem Slot bis zur Publish-Frist, danach läuft das Original erneut (E-041). Bei jeder Fehlerrate über null fällt der Konsum eines Workers auf etwa Slots / (Taskdauer + Fehlerrate × Frist), gemessen mit Frist 20 s auf ein Viertel, mit der Vorgabe 120 s rechnerisch rund 3 %; ein Task, der bei jedem Lauf scheitert, geht nach rund 19 Runden als `RedeliveryLimitError` in die DLQ. Das ist eine bewusste Folge, kein Fehler. Doku in `architecture.md`/`errors.md`, mit der Empfehlung einer kürzeren Worker-Publish-Frist (Abwägung: mehr Zusatzläufe). Prüfauftrag für den Fix-Plan: die Kopie ohne Slot warten lassen (Blocked-Zustand vor dem Publish prüfen, Nachricht unbestätigt, Slot frei; PF-18 b), bewertet mit einem Integrationstest auf `memory_alarm`. (3) *Kanal-Schließen im Alarm (PF-19, R4I-04).* Das `Channel.Close` eines verworfenen Publish-Kanals läuft im Alarm in die 5-s-Frist; bei der Aufhebung loggt aiormq ERROR „Got frame for closed channel N: <Channel.CloseOk>“. Ausgang richtig, kein Leck. Doku jetzt: die aiormq-Zeile im Event-Katalog erklären, `publish_channel_close_incomplete` aufnehmen und im Alarm ohne Traceback loggen; Kommentar `broker/rabbitmq/connection.py:75-76` berichtigen. Prüfpunkt im Fix-Plan: das Close bis `unblocked` zurückstellen (PF-19 a), mit Integrationstest auf `memory_alarm`, der auch die Kanalnummer-Vergabe prüft (R4I-04b). Festgeschrieben ist das Warten bis zur Frist mit den bestehenden Tests dieser Regel; Integrationstests zu (2) und (3) folgen mit dem Fix-Plan (nach PF-1, Chaos-Stack 4.3.6). Stand 2026-10-18 (0.13.0): Die Doku zu (1) bis (3) ist umgesetzt (U20), `publish_channel_close_incomplete` loggt im Alarm ohne Traceback mit `blocked=True` (PF-19 b, festgeschrieben in `tests/unit/broker/test_rabbitmq.py`). Die Prüfaufträge sind als Spikes beantwortet (U21): PF-18 b nicht in 0.13 umsetzen (`docs/solutions/spikes/2026-10-spike-retry-kopie-ohne-slot.md`, wirksam erst mit Prefetch über `concurrency`, berührt E-019), PF-19 a klein umsetzen empfohlen (`docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md`); beides entscheidet Georg, die Integrationstests auf `memory_alarm` gibt es bis dahin nur als Probe-Code.

**Nachtrag 2026-10-19 (0.14, U6; Umsetzung von (3), PF-19 a).** Ist die Publish-Verbindung beim Verwurf des Publish-Kanals blockiert, schickt swarmq das `Channel.Close` erst, wenn der Broker `connection.unblocked` meldet oder die aiormq-Verbindung endet, höchstens nach der Publish-Frist (`SWARMQ_PUBLISH_TIMEOUT_S`, im Shutdown die Endfrist). Danach schließt es wie bisher mit 5 s Frist. Event und `closing` werden beim Verwurf gelesen (`blocked_close_waiters` in `broker/rabbitmq/blocked.py`), nicht erst beim Warten: Ein Reconnect bringt ein neues aiormq-Objekt. Ist der Zustand nicht lesbar oder die Verbindung nicht blockiert, gilt wie bisher das sofortige Close (fail-open). Ein `close()` des Brokers bricht ein wartendes Close nach 5 s ab (`_drain_pending_channel_closes`, unverändert); ein Shutdown im Alarm dauert also wie bisher höchstens 5 s länger. Folgen: Bei einem Alarm, der kürzer ist als die Publish-Frist, verschwinden die aiormq-ERROR „Got frame for closed channel N: <Channel.CloseOk>“ und `publish_channel_close_incomplete`; an ihrer Stelle loggt aiormq höchstens die WARNING „Delivery tag %r confirmed %r was ignored“ (`aiormq.channel`), wenn der Broker den auslösenden Publish nach der Aufhebung bestätigt (D-1, Ausgang bleibt `PublishIndeterminate`, E-041/E-042 unverändert). Dauert der Alarm länger als die Publish-Frist (etwa `stoerung-1h`: Alarm 60 s, Frist 20 s), läuft das Close nach der Frist wie bisher in die 5-s-Frist, mit `publish_channel_close_incomplete` ohne Traceback (`blocked=True`) und der aiormq-ERROR bei der Aufhebung. Kein Leck, keine doppelte Kanalnummer: Der verworfene Kanal steht bis zum Close in der aiormq-Tabelle, der neue Publish-Kanal bekommt eine andere Nummer (Integrationstest auf `memory_alarm`). Die übrigen Confirms des verworfenen Kanals bleiben bis zum Close offen (Präzisierung in E-078). Festgeschrieben in `TestChannelCloseAfterUnblocked`, im Integrationstest `test_pf19_close_after_unblocked.py` und in der Chaos-Variante (Alarm plus Toxiproxy-Abriss auf 4.3.6); `TestChannelCloseDuringAlarm` bleibt unverändert gültig.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
