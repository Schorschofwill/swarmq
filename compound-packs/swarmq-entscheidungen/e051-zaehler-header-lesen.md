---
title: Kaputte Zähler-Header zählen wie keine; der Retry-Zähler wird streng gelesen, der Rate-Limit-Zähler nachsichtig
applies_when:
  - Änderungen am Lesen von x-swarmq-retry-count, x-swarmq-rate-limit-count oder anderen Zähler-Headern
  - eine Fehlersuche meldet, dass ein UI-Replay mit x-swarmq-retry-count "3" wieder das volle Retry-Budget bekommt
  - eine Fehlersuche meldet, dass Retry- und Rate-Limit-Zähler unterschiedlich gelesen werden
tags: [retry, rate-limit, header, replay, robustheit]
id: E-051
datum: 2026-10-14
status: gilt
quelle:
  - docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md:174 (KTD15, Retry-Zähler streng lesen)
  - Commit 29f9a3e (U17, FS-19) und Review 06958c8 (Zähler-Helfer gegen inf/bool fest)
  - src/swarmq/worker/gates/execution_guard.py:57-61 (retry_count_of)
  - src/swarmq/worker/gates/__init__.py:227-233 (_as_count), src/swarmq/worker/gates/rate_limit.py:86-88
tests:
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_string_retry_count_still_settles_the_failed_task
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_bool_retry_count_counts_like_no_header
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_numeric_string_rate_limit_count_is_read_leniently
  - tests/unit/worker/test_befund_fs19_counter_headers.py::test_fs19_infinite_rate_limit_count_counts_like_no_header
---

**Entscheidung.** Ein Zähler-Header, der sich nicht als Zähler lesen lässt, zählt wie kein Header; die Nachricht wird normal weiterverarbeitet statt unquittiert hängen zu bleiben. Der Retry-Zähler `x-swarmq-retry-count` wird streng gelesen: Nur ein echtes `int` größer 0 zählt; `bool`, Zeichenketten (auch `"3"`), Gleitkommazahlen und negative Werte zählen wie 0. Der Rate-Limit-Zähler `x-swarmq-rate-limit-count` dient nur der Diagnose und wird nachsichtig gelesen (`int()`, numerische Zeichenketten gelten, nicht lesbare Werte und `inf` zählen 0, negative werden auf 0 gehoben).

**Begründung.** Ein manuelles Neu-Publizieren über die RabbitMQ-UI setzt Header als Zeichenketten; vorher scheiterten Retry-Pfad und Rate-Limit-Gate daran, und die Nachricht hing bis `consumer_timeout` (FS-19). Streng gelesen ist der Retry-Zähler gleich wie im Ausführungsschutz (`retry_count_of`) und bei `swarmq dlq retry`. Beim Diagnose-Zähler schadet Nachsicht nicht.

**Bewusste Folgen.** Ein UI-Replay mit `x-swarmq-retry-count: "3"` bekommt wieder das volle Retry-Budget; das ist kein Fehler. Dass beide Zähler unterschiedlich gelesen werden, ist gewollt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
