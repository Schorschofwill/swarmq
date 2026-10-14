---
title: Ein Publish bei blockiertem Broker sendet nichts, wartet bis zur Publish-Frist und scheitert mit BrokerPublishBlocked; Worker nutzen zwei Verbindungen
applies_when:
  - RabbitMQ meldet connection.blocked (Speicher- oder Plattenalarm)
  - Änderungen an broker/rabbitmq/delivery.py, blocked.py, connection.py oder SWARMQ_BROKER_PUBLISH_CONNECTION
  - eine Fehlersuche meldet, dass der erste Publish während eines Alarms als PublishIndeterminate endet
  - eine Fehlersuche meldet, dass ein Worker zwei AMQP-Verbindungen öffnet
tags: [broker, publish, blocked, verbindung, bz-16]
id: E-025
datum: 2026-10-11
status: gilt
quelle:
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:77 (Nachtrag MR 16, Blocked wartet bis zur Frist, zwei Verbindungen je Worker, Georg 2026-10-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:580 (MR 16, BZ-16)
tests:
  - tests/unit/broker/test_publish_blocked.py::test_blocked_until_the_deadline_raises_publish_blocked
  - tests/unit/broker/test_publish_blocked.py::test_unblocked_connection_publishes_without_waiting_or_roundtrip
  - tests/unit/broker/test_publish_blocked.py::test_unreadable_state_fails_open_with_one_warning_per_broker
  - tests/unit/broker/test_publish_connection.py::test_injected_connection_is_always_shared
  - tests/unit/broker/test_publish_connection.py::test_first_consume_opens_a_second_connection_for_the_consume_channel
  - tests/unit/test_config_publish_connection.py::test_default_is_separate
---

**Entscheidung.** Ist die Verbindung blockiert, sendet ein Publish nichts, wartet bis zur Publish-Frist und scheitert dann mit `BrokerPublishBlocked` (Unterklasse von `BrokerConnectionError`); Verbindung und Publish-Kanal bleiben. Eine unblockierte Verbindung kostet keinen Roundtrip; ist der Zustand nicht lesbar, gilt fail-open mit einer WARNING. Worker öffnen zwei Verbindungen (`SWARMQ_BROKER_PUBLISH_CONNECTION=separate`, Default; die Consume-Verbindung entsteht beim ersten `consume()`); bei injizierter `connection=`/`connection_pool=` gilt immer `shared`.

**Begründung.** Publish und Konsum liefen über eine Verbindung; bei einem Broker-Alarm hingen auch die Acks (BZ-16). Mit getrennten Verbindungen können Worker weiter quittieren.

**Bewusste Folgen.** Grenze: RabbitMQ meldet die Blockade erst beim ersten Publish während des Alarms; dieser liegt schon auf dem Socket und endet als `PublishIndeterminate`. Verbindungslimits (`connection_max`, vhost-`max-connections`, Benutzerlimits) und Alarme auf die Verbindungszahl sind vor dem Rollout zu prüfen.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
