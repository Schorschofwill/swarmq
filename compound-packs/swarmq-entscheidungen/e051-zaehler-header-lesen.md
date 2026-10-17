---
title: Kaputte Zähler-Header zählen wie keine; der Retry-Zähler wird streng gelesen, der Rate-Limit-Zähler nachsichtig
applies_when:
  - Änderungen am Lesen von x-swarmq-retry-count, x-swarmq-rate-limit-count oder anderen Zähler-Headern
  - eine Fehlersuche meldet, dass ein UI-Replay mit x-swarmq-retry-count "3" wieder das volle Retry-Budget bekommt
  - eine Fehlersuche meldet, dass Retry- und Rate-Limit-Zähler unterschiedlich gelesen werden
  - eine Fehlersuche meldet, dass eine per UI neu publizierte Retry-Kopie nicht läuft
tags: [retry, rate-limit, header, replay, robustheit]
id: E-051
datum: 2026-10-14
status: gilt
quelle:
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:174 (KTD15, Retry-Zähler streng lesen)
  - Commit 29f9a3e (U17, FS-19) und Review 06958c8 (Zähler-Helfer gegen inf/bool fest)
  - src/swarmq/worker/gates/execution_guard.py:57-61 (retry_count_of)
  - src/swarmq/worker/gates/__init__.py:227-233 (_as_count), src/swarmq/worker/gates/rate_limit.py:86-88
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-4, Option a)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (ENTSCHEIDUNG R4Z-05, PF-4)
tests:
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_string_retry_count_still_settles_the_failed_task
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_bool_retry_count_counts_like_no_header
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_numeric_string_rate_limit_count_is_read_leniently
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_infinite_rate_limit_count_counts_like_no_header
  - tests/property/stateful/test_retry_chain_machine.py::test_republished_retry_copy_with_text_count_is_skipped_as_superseded
  - tests/integration/stateful/test_retry_chain_machine_valkey.py::test_republished_retry_copy_with_text_count_is_skipped_as_superseded_against_valkey
---

**Entscheidung.** Ein Zähler-Header, der sich nicht als Zähler lesen lässt, zählt wie kein Header; die Nachricht wird normal weiterverarbeitet statt unquittiert hängen zu bleiben. Der Retry-Zähler `x-swarmq-retry-count` wird streng gelesen: Nur ein echtes `int` größer 0 zählt; `bool`, Zeichenketten (auch `"3"`), Gleitkommazahlen und negative Werte zählen wie 0. Der Rate-Limit-Zähler `x-swarmq-rate-limit-count` dient nur der Diagnose und wird nachsichtig gelesen (`int()`, numerische Zeichenketten gelten, nicht lesbare Werte und `inf` zählen 0, negative werden auf 0 gehoben).

**Begründung.** Ein manuelles Neu-Publizieren über die RabbitMQ-UI setzt Header als Zeichenketten; vorher scheiterten Retry-Pfad und Rate-Limit-Gate daran, und die Nachricht hing bis `consumer_timeout` (FS-19). Streng gelesen ist der Retry-Zähler gleich wie im Ausführungsschutz (`retry_count_of`) und bei `swarmq dlq retry`. Beim Diagnose-Zähler schadet Nachsicht nicht.

**Bewusste Folgen.** Ein UI-Replay mit `x-swarmq-retry-count: "3"` bekommt wieder das volle Retry-Budget; das ist kein Fehler. Dass beide Zähler unterschiedlich gelesen werden, ist gewollt.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; PF-4, R4Z-05): UI-Replay einer Retry-Kopie.** Eine per RabbitMQ-UI mit Text-Zähler (`x-swarmq-retry-count: "1"`) und ohne neue Replay-ID neu publizierte Retry-Kopie zählt 0 und gilt gegen den Superseded-Marker als überholt: kein Lauf, kein Ergebnis, keine DLQ-Kopie (`duplicate_delivery_skipped`, `reason=superseded`). Das ist kein Fehler (KTD5, E-009). Wiedereinspielen nur über `swarmq dlq retry` (neue Identität). Optional ein Satz in `dead-letter-queues.md`. Festgeschrieben in den beiden Retry-Ketten-Tests unter `tests:` (Fakes und Valkey).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
