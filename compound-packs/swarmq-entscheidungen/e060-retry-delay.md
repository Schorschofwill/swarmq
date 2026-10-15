---
title: Retry(delay) überschreibt den Backoff und zählt gegen max_retries; eine negative Wartezeit wird auf 0 geklemmt, inf und nan lehnt Retry ab
applies_when:
  - Änderungen an exceptions.Retry, RetryHandler, retry_later oder _publish_to_delay_queue
  - eine Fehlersuche meldet, dass Retry(delay=-5) keinen Fehler auslöst, sondern sofort wiederholt
  - eine Fehlersuche meldet, dass Retry unabhängig von retry_exceptions wiederholt
tags: [retry, backoff, exceptions, f3-03]
id: E-060
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-03)
  - docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md:280, 286 (Retry überschreibt Backoff; respektiert max_retries, kein unendlicher Retry)
  - website/guides/scheduling-and-retries.md:646-651 (unabhängig von retry_exceptions, zählt gegen max_retries)
tests:
  - tests/unit/worker/test_retry.py::test_retry_delay_wins_although_retry_exceptions_does_not_list_it
  - tests/unit/worker/test_retry.py::test_retry_retries_although_retry_exceptions_does_not_list_it
  - tests/unit/worker/test_retry.py::test_retry_with_zero_max_retries_ends_terminally
ohne_test: Test entsteht mit dem Fix in 0.12.3 (F3-03, tests/unit/worker/test_befund_f3_03_retry_negative_delay.py).
---

**Entscheidung.** `raise Retry(delay=…)` wiederholt mit genau dieser Wartezeit statt des Backoffs, unabhängig von `retry_exceptions`, und zählt gegen `max_retries`; ist das Budget aufgebraucht (immer bei `max_retries=0`), endet der Task terminal (DLQ bzw. Fehlerergebnis). Eine negative Wartezeit wird auf 0 geklemmt und zählt normal. `inf` und `nan` lehnt schon der Konstruktor von `Retry` mit `ValueError` ab.

**Begründung.** Eine berechnete Wartezeit wie `reset_at - time.time()` nach einem 429 wird leicht negativ. RabbitMQ lehnt `expiration < 0` ab, schließt den Publish-Kanal und reißt parallele Publishes mit; der Rückfall wiederholte bis zum Zustelllimit, ohne gegen `max_retries` zu zählen (F3-03). Klemmen hält den häufigen Fall am Laufen, ein Fehler beim Erzeugen trifft nur unsinnige Werte.

**Bewusste Folgen.** `Retry(delay=-30)` wiederholt sofort und verbraucht einen Retry. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
