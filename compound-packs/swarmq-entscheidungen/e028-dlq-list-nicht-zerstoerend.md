---
title: dlq list und dlq inspect lesen nicht zerstörend (Kopie ans Ende + Ack); parallele Listings sehen Teilmengen
applies_when:
  - Änderungen an broker/rabbitmq/admin.py (DLQ lesen, auflisten, inspizieren)
  - eine Fehlersuche meldet, dass zwei gleichzeitige dlq list unterschiedliche Teilmengen zeigen
  - eine Fehlersuche meldet, dass sich die Reihenfolge der DLQ nach dlq list ändert oder abgelaufene Einträge beim Listen verschwinden
tags: [dlq, cli, broker, bz-21]
id: E-028
datum: 2026-10-09
status: gilt
quelle:
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:580 (MR 16, BZ-21: Kopie ans Ende + Ack)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:723 (Abschnitt 7, parallele dlq list sehen Teilmengen)
tests:
  - tests/unit/broker/test_dlq_read.py::test_list_reads_with_ack_and_acks_after_the_copy
  - tests/unit/broker/test_dlq_read.py::test_failed_copy_leaves_the_original_unacknowledged
  - tests/integration/broker/test_rabbitmq_dlq.py::test_message_survives_25_listings
  - tests/integration/broker/test_rabbitmq_dlq.py::test_list_returns_each_message_exactly_once
---

**Entscheidung.** `dlq list`/`dlq inspect` lesen über einen eigenen Channel mit `no_ack=False`, kopieren jede Nachricht ans Ende der DLQ und bestätigen das Original erst nach dem Confirm der Kopie. Abgelaufene Einträge (über `dead_letter_ttl_s`) werden beim Listen ohne Kopie bestätigt. Akzeptierte Grenze: Parallele `dlq list` sehen jeweils Teilmengen.

**Begründung.** Das zerstörende Lesen kostete bei einem Abbruch Nachrichten (BZ-21). Weil die Rückgabe auf RabbitMQ 4.x den Delivery-Count der XQ hochzählt (Spike S4), ist „Kopie ans Ende“ statt „halten und zurückgeben“ gewählt; die Ausführungsidentität (E-009) dedupliziert Kopien.

**Bewusste Folgen.** Die Reihenfolge in der DLQ ändert sich durch Listen. Gleichzeitige Listings sehen nicht alle Einträge.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
