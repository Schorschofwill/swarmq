---
title: Geänderte Queue-Argumente führen zu Exit 78 statt Neustartschleife; x-delivery-limit wird tolerant übernommen
applies_when:
  - SWARMQ_QUEUE_TYPE, SWARMQ_DELIVERY_LIMIT oder andere Queue-Argumente ändern sich gegenüber der bestehenden Queue
  - Änderungen an broker/rabbitmq/arg_conflict.py oder declarations.py
  - eine Fehlersuche meldet, dass ein geändertes SWARMQ_DELIVERY_LIMIT nicht wirkt (vorhandener Wert gilt)
  - eine Fehlersuche meldet, dass der Worker bei geändertem Queue-Typ mit QueueArgumentsMismatch nicht startet
tags: [broker, queue, deklaration, konfiguration, bz-15]
id: E-016
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:133 (Entscheidungen, BZ-15)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:50 (Abschnitt 1, BZ-15 Delivery-Limit tolerant)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:378 (MR 8, Kette je Queue)
tests:
  - tests/integration/broker/test_befund_bz15_queue_args_change.py::test_changed_queue_type_is_reported_as_configuration_error
  - tests/integration/broker/test_befund_bz15_queue_args_change.py::test_raising_delivery_limit_keeps_existing_queue_declarable
  - tests/unit/broker/test_declare_chain.py::test_changed_queue_type_raises_queue_arguments_mismatch
  - tests/unit/broker/test_declare_adoption.py::test_differing_delivery_limit_is_adopted_with_warning
  - tests/unit/broker/test_declare_adoption.py::test_all_three_arguments_take_four_attempts
  - tests/unit/broker/test_declare_chain.py::test_unrecognized_refusal_reports_the_broker_message_without_advising_deletion
---

**Entscheidung.** Ein erkannter `PRECONDITION_FAILED` wegen geänderter Queue-Argumente wird zu `QueueArgumentsMismatch` (alt gegen neu in der Meldung, Exit 78) statt einer Neustartschleife. `x-delivery-limit` wird tolerant behandelt: Es gilt der vorhandene Wert, dazu die WARNING `queue_argument_adopted`. Jedes tolerierte Argument wird höchstens einmal übernommen (höchstens 4 Versuche). Ein unerkannter 406/403 bleibt `BrokerConnectionError`. Die Doku beschreibt die Migration.

**Begründung.** Die Doku legte eine Änderung nahe, die zu PRECONDITION_FAILED und Neustartschleife führte (BZ-15). Das Delivery-Limit soll kein Deploy blockieren, ein anderer Queue-Typ schon.

**Bewusste Folgen.** Ein geändertes `SWARMQ_DELIVERY_LIMIT` wirkt auf bestehende Queues nicht; das Gate nutzt `min(Konfiguration, effektives Limit)`. Eine `delivery-limit`-Policy ist nicht erkennbar und für swarmq-Queues nicht unterstützt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
