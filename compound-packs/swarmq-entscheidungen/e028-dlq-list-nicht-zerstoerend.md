---
title: dlq list und dlq inspect lesen nicht zerstörend (Kopie ans Ende + Ack); parallele Listings sehen Teilmengen
applies_when:
  - Änderungen an broker/rabbitmq/admin.py (DLQ lesen, auflisten, inspizieren)
  - eine Fehlersuche meldet, dass zwei gleichzeitige dlq list unterschiedliche Teilmengen zeigen
  - eine Fehlersuche meldet, dass sich die Reihenfolge der DLQ nach dlq list ändert oder abgelaufene Einträge beim Listen verschwinden
  - eine Fehlersuche meldet, dass dlq list truncated false oder dlq inspect not found ohne Scan-Limit-Hinweis meldet, obwohl das Lesen am Limit stoppte
tags: [dlq, cli, broker, bz-21]
id: E-028
datum: 2026-10-09
status: gilt
quelle:
  - 'docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:580 (MR 16, BZ-21: Kopie ans Ende + Ack)'
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:723 (Abschnitt 7, parallele dlq list sehen Teilmengen)
  - Georg 2026-10-17 (Fehlersuche Runde 4, F4-05)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (FEHLER F4-05; Vor dem Fix Punkt 4)
tests:
  - tests/unit/broker/test_dlq_read.py::test_list_reads_with_ack_and_acks_after_the_copy
  - tests/unit/broker/test_dlq_read.py::test_failed_copy_leaves_the_original_unacknowledged
  - tests/integration/broker/test_rabbitmq_dlq.py::test_message_survives_25_listings
  - tests/integration/broker/test_rabbitmq_dlq.py::test_list_returns_each_message_exactly_once
  - tests/integration/broker/test_befund_f4_05_dlq_truncated_expired.py::test_list_reports_truncation_when_expired_entries_used_up_the_limit
  - tests/integration/broker/test_befund_f4_05_dlq_truncated_expired.py::test_inspect_reports_the_scan_limit_when_expired_entries_used_it_up
  - tests/unit/broker/test_dlq_read.py::test_list_reports_hit_limit_when_an_expired_entry_used_up_a_read
  - tests/unit/broker/test_dlq_read.py::test_inspect_miss_reports_hit_limit_when_an_expired_entry_used_up_a_read
  - tests/unit/broker/test_dlq_read.py::test_retry_miss_reports_hit_limit_and_a_hit_does_not
  - tests/unit/broker/test_dlq_read.py::test_retry_all_reports_hit_limit_when_an_expired_entry_used_up_a_read
  - tests/unit/test_cli.py::TestDlqHandlerOrchestration::test_list_reports_truncation_from_hit_limit_below_the_limit
  - tests/unit/test_cli.py::TestDlqHandlerOrchestration::test_list_table_shows_the_truncation_hint_from_hit_limit
  - tests/unit/test_cli.py::TestDlqHandlerOrchestration::test_list_trusts_hit_limit_false_even_when_scanned_equals_the_limit
  - tests/unit/test_cli.py::TestDlqHandlerOrchestration::test_inspect_miss_reports_the_scan_limit_from_hit_limit
  - tests/unit/test_cli.py::TestDlqHandlerOrchestration::test_retry_miss_reports_the_scan_limit_from_hit_limit
  - tests/unit/test_cli.py::TestDlqHandlerOrchestration::test_retry_all_reports_truncation_from_hit_limit
---

**Entscheidung.** `dlq list`/`dlq inspect` lesen über einen eigenen Channel mit `no_ack=False`, kopieren jede Nachricht ans Ende der DLQ und bestätigen das Original erst nach dem Confirm der Kopie. Abgelaufene Einträge (über `dead_letter_ttl_s`) werden beim Listen ohne Kopie bestätigt. Akzeptierte Grenze: Parallele `dlq list` sehen jeweils Teilmengen.

**Begründung.** Das zerstörende Lesen kostete bei einem Abbruch Nachrichten (BZ-21). Weil die Rückgabe auf RabbitMQ 4.x den Delivery-Count der XQ hochzählt (Spike S4), ist „Kopie ans Ende“ statt „halten und zurückgeben“ gewählt; die Ausführungsidentität (E-009) dedupliziert Kopien.

**Bewusste Folgen.** Die Reihenfolge in der DLQ ändert sich durch Listen. Gleichzeitige Listings sehen nicht alle Einträge.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; F4-05): Kürzung und Lesungen.** `scanned` zählt weiter nur lebende Einträge, keine Abgelaufenen (`dead-letter-queues.md:271-274`). Ob das Lesen am Limit stoppte, leitet die CLI künftig nicht aus `scanned` ab, sondern aus einem Flag `hit_limit` (bzw. der Zahl der Lesungen), um das der Vertrag von `BrokerAdmin` (`list_dead_letters`, `inspect_dead_letter`, `retry_dead_letter`) erweitert wird. `truncated` und `scan_exhausted` sind damit auch dann richtig, wenn abgelaufene Einträge die Lesungen verbraucht haben. Umgesetzt in 0.13.0 (U17): Die Lesebefehle liefern `DeadLetterScan` (`contracts/broker.py`), eine Tupel-Unterklasse mit `hit_limit`; die Protocol-Signaturen bleiben `tuple[..., int]`. Die CLI liest `hit_limit` und fällt bei Implementierungen ohne das Flag auf `scanned >= limit` zurück. `retry_all_dead_letters` meldet `truncated` ebenso. Festgeschrieben durch die Nachweise F4-05 (Marker E-028) und die Unit-Tests oben.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
