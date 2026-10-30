---
title: Vom Broker beendete Consumer meldet nur der Consumer-Wächter neu an; die Consumer-Wiederherstellung von aio-pika 10.2 ist für swarmqs Kanäle abgeschaltet
applies_when:
  - Änderungen an ReleasingRobustChannel (broker/rabbitmq/underlay.py), am Consumer-Wächter (broker/rabbitmq/consumer_watch.py) oder an der Kanalklasse der eigenen Verbindungen (CHANNEL_CLASS in connection.py)
  - ein Upgrade von aio-pika ändert RobustChannel._on_consumer_cancel, _restore_consumers oder die Verbindungsfabrik von RobustConnection
  - eine Fehlersuche meldet, dass aio-pika einen vom Broker beendeten Consumer nicht selbst wieder anmeldet oder dass nach basic.cancel kein Probe-Kanal geöffnet wird
  - eine Fehlersuche meldet die WARNING consumer_recovery_disable_unavailable
  - ein Plan will die Wiederanmeldung aio-pika überlassen und den Wächter nur noch prüfen lassen
tags: [broker, consumer, rabbitmq, aio-pika, aiormq, reconnect]
id: E-089
datum: 2026-10-30
status: gilt
quelle:
  - "Georg 2026-10-30: Wiederanmeldung nur durch swarmq, aio-pikas Wiederherstellung abschalten (vorgelegt aus den U3-Messungen)"
  - docs/plans/2026-10-29-1945-chore-aiormq-7-2-anheben-plan.md (KTD7, U3, U7, Open Questions)
  - tests/integration/broker/test_consumer_cancel_recovery.py (U3-Charakterisierung von aio-pika 10.2.1 auf RabbitMQ 4.3.6 und 4.2.9)
  - src/swarmq/broker/rabbitmq/underlay.py (ReleasingRobustChannel._on_consumer_cancel, _restore_consumers, _consumer_recovery_entries_fit)
  - website/reference/logging.md (consumer_recovery_disable_unavailable)
tests:
  - tests/integration/broker/test_consumer_cancel_recovery.py::test_43_aio_pika_leaves_the_cancelled_consumer_to_the_watch_and_the_connection_stays
  - tests/integration/broker/test_consumer_cancel_recovery.py::test_43_a_woken_connection_factory_does_not_re_consume_the_cancelled_tag
  - tests/integration/broker/test_consumer_cancel_recovery.py::test_a_deleted_queue_gets_no_probe_and_the_watch_fences_it
  - tests/unit/broker/test_underlay.py::test_a_broker_cancel_on_a_swarmq_channel_requests_no_recovery_from_aio_pika
  - tests/unit/broker/test_underlay.py::test_a_woken_connection_factory_opens_no_probe_and_consumes_nothing_on_a_swarmq_channel
  - tests/unit/broker/test_underlay.py::test_changed_recovery_entries_in_aio_pika_fail_open_with_one_warning_per_connection
---

**Entscheidung.** Beendet der Broker einen Consumer (`basic.cancel`, etwa nach `consumer-timeout` ab RabbitMQ 4.3 oder weil die Queue gelöscht wurde), meldet ihn nur der Consumer-Wächter neu an (`consumer_watch.py`: eigener Cancel, Existenzprüfung nach E-083, Anmeldung mit neuem Tag). Die Wiederherstellung, die aio-pika 10.2 dafür mitbringt, ist für swarmqs Kanäle abgeschaltet: `ReleasingRobustChannel` (die `CHANNEL_CLASS` der eigenen Verbindungen) überschreibt `_on_consumer_cancel`, sodass ein `basic.cancel` die Verbindungsfabrik nicht weckt, und `_restore_consumers`, sodass sie auch bei einem Aufwachen aus anderem Grund keinen Probe-Kanal öffnet und nichts anmeldet.

Unberührt bleibt, was aio-pika schon in 10.0 tat: Schließt der Broker den Kanal (bis 4.2 nach `consumer-timeout`) oder bricht die Verbindung ab, legt aio-pikas Restore (`_on_close`, `_on_connected`) die Consumer aus `RobustQueue._consumers` mit ihren Tags auf dem neuen Kanal an. Der Wächter behält diese Tags (KTD3 im Plan 2026-10-08-1541).

Passen die Einstiege in aio-pika nicht mehr (`RobustChannel._on_open` hängt `_on_consumer_cancel` nicht mehr an, oder die Verbindungsfabrik ruft `_restore_consumers` nicht mehr), gilt fail-open: aio-pika stellt wieder her wie dort, und einmal je Verbindung meldet swarmq WARNING `consumer_recovery_disable_unavailable`.

**Begründung.** Gemessen in U3 mit aio-pika 10.2.1 und aiormq 7.2.4 (`test_consumer_cancel_recovery.py`):

- RabbitMQ 4.3.6 behält nach dem Consumer-Timeout den Tag des beendeten Consumers (Zombie). aio-pika meldet mit demselben Tag neu an, der Broker antwortet mit `Connection.Close(530 NOT_ALLOWED - attempt to reuse consumer tag)`, die ganze Verbindung stirbt; mit den Standardtakten (beide 5 s) etwa jedes zweite Mal. Kommt der Wächter zuerst, entsteht kein zweiter Consumer, keine Probe, und die Verbindung bleibt.
- RabbitMQ 4.2.9 schließt den Kanal; der Restore legt den Tag auf einem neuen Kanal an, die Verbindung bleibt. Dafür braucht es aio-pikas neue Wiederherstellung nicht.
- Bei einer gelöschten konsumierten Queue legte die passive Probe sie zwar nicht neu an, aber aio-pika öffnete je `reconnect_interval` einen Probe-Kanal, bis `release_queue` griff.
- aio-pika ruft `consume` der Wiederanmeldung mit einer Frist von 5 s auf dem gemeinsamen Consume-Kanal auf. Läuft die Frist ab, schließt aiormq den Kanal mit 504, und alle Consumer des Workers fallen weg (widerspricht E-053).
- `recovery.cancel()` bricht ein laufendes `probe.close()` ab; das zweite Close beendet die Verbindung mit 504 (R1, wie F4-30).

Der Wächter deckt alle Fälle schon ab, ohne RPCs abzubrechen (E-053) und mit Eingrenzung gelöschter Queues (E-083). Zwei Seiten, die denselben Consumer neu anmelden, sind ein Wettlauf; abgeschaltet wird die Seite, die die Verbindung gefährdet.

**Bewusste Folgen.**
- Ein vom Broker beendeter Consumer fehlt bis zum nächsten Takt des Wächters (`consumer_watch_interval_s`, Standard 5 s) oder bis ein Settle auf seinen Tag den Wächter anstößt (KTD4 im Plan 2026-10-08-1541). Das war vor aio-pika 10.2 genauso und ist kein Fehler; `/ready` misst die Lücke mit Gnadenfrist.
- Die „Zombie“-Prämisse der Settle-Regel (`_may_settle`) und das eigene Abmelden des Wächters bleiben. aiormq beantwortet ein `basic.cancel` erst mit `CancelOk`, wenn der Broker `accept_consumer_cancel_ok` meldet (ab RabbitMQ 4.3.7); auf den Testbrokern 4.3.6 und 4.2.9 ist das nicht prüfbar.
- Kanäle, die nicht `ReleasingRobustChannel` sind (der Publish-Kanal auf einer injizierten Verbindung, Kanäle der App), behalten aio-pikas Verhalten. Consumer laufen nur auf swarmqs eigenen Verbindungen.
- Wer `RobustConnection._request_recovery` selbst ruft, weckt die Fabrik; für swarmqs Kanäle tut sie dann nichts.

**Überdenken nur mit neuem Argument.** Ab RabbitMQ 4.3.7 (`CancelOk` des Clients, kein Zombie mehr) wäre neu zu messen, ob aio-pikas Wiederanmeldung ohne 530 und ohne Fristen auf dem Consume-Kanal auskommt; erst dann ist „aio-pika meldet an, der Wächter prüft nur“ wieder eine Option.
