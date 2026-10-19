---
title: Retry(delay) überschreibt den Backoff und zählt gegen max_retries; eine negative Wartezeit wird auf 0 geklemmt, inf, nan und Werte über MAX_DEADLINE_S lehnt Retry ab
applies_when:
  - Änderungen an exceptions.Retry, RetryHandler, retry_later oder _publish_to_delay_queue
  - eine Fehlersuche meldet, dass Retry(delay=-5) keinen Fehler auslöst, sondern sofort wiederholt
  - eine Fehlersuche meldet, dass Retry unabhängig von retry_exceptions wiederholt
  - eine Fehlersuche meldet, dass Retry(delay) über zehn Jahre abgelehnt wird
tags: [retry, backoff, exceptions, f3-03]
id: E-060
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-03)
  - docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md:280, 286 (Retry überschreibt Backoff; respektiert max_retries, kein unendlicher Retry)
  - website/guides/scheduling-and-retries.md:646-651 (unabhängig von retry_exceptions, zählt gegen max_retries)
  - 'Georg 2026-10-19 (F4-19, Option a: Obergrenze im Konstruktor)'
  - docs/befunde/fehlersuche-runde-4-2026-10.md (F4-19)
tests:
  - tests/unit/worker/test_retry.py::test_retry_delay_wins_although_retry_exceptions_does_not_list_it
  - tests/unit/worker/test_retry.py::test_retry_retries_although_retry_exceptions_does_not_list_it
  - tests/unit/worker/test_retry.py::test_retry_with_zero_max_retries_ends_terminally
  - tests/unit/worker/test_retry.py::TestRetryDelayBounds::test_clamped_retry_with_exhausted_budget_ends_terminally
  - tests/unit/worker/test_retry.py::TestRetryDelayBounds::test_task_raising_infinite_retry_ends_terminally
  - tests/unit/worker/test_befund_f3_03_retry_negative_delay.py::test_negative_retry_delay_is_clamped_to_zero_and_counts
  - tests/unit/worker/test_befund_f3_03_retry_negative_delay.py::test_non_finite_retry_delay_is_rejected
  - tests/integration/broker/test_befund_f3_03_negative_expiration.py::test_negative_retry_delay_counts_and_spares_parallel_publishes
  - tests/unit/worker/test_befund_f4_19_retry_delay_beyond_bound.py::test_retry_delay_beyond_the_bound_still_counts_against_max_retries
  - tests/unit/worker/test_retry.py::TestRetryDelayBounds::test_delay_at_the_bound_is_retried_with_exactly_the_bound
---

**Entscheidung.** `raise Retry(delay=…)` wiederholt mit genau dieser Wartezeit statt des Backoffs, unabhängig von `retry_exceptions`, und zählt gegen `max_retries`; ist das Budget aufgebraucht (immer bei `max_retries=0`), endet der Task terminal (DLQ bzw. Fehlerergebnis). Eine negative Wartezeit wird auf 0 geklemmt und zählt normal. `inf` und `nan` lehnt schon der Konstruktor von `Retry` mit `ValueError` ab.

**Begründung.** Eine berechnete Wartezeit wie `reset_at - time.time()` nach einem 429 wird leicht negativ. RabbitMQ lehnt `expiration < 0` ab, schließt den Publish-Kanal und reißt parallele Publishes mit; der Rückfall wiederholte bis zum Zustelllimit, ohne gegen `max_retries` zu zählen (F3-03). Klemmen hält den häufigen Fall am Laufen, ein Fehler beim Erzeugen trifft nur unsinnige Werte.

**Bewusste Folgen.** `Retry(delay=-30)` wiederholt sofort und verbraucht einen Retry. Das ist kein Fehler. Wirft ein Task `Retry(delay=math.inf)`, kommt der `ValueError` aus dem Konstruktor als gewöhnlicher Task-Fehler an, nicht als `Retry`: er unterliegt `retry_exceptions` und dem Backoff (endet also terminal, wenn der Filter ihn nicht nennt) und erreicht den Broker nie mit einer unendlichen Wartezeit.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.

**Nachtrag 2026-10-19 (0.13.1, F4-19; Georg, Option a): Obergrenze.** Der Konstruktor von `Retry` lehnt auch `delay > MAX_DEADLINE_S` (315 360 000 s, zehn Jahre, E-063) mit `ValueError` ab, wie `inf` und `nan`; der genaue Grenzwert ist erlaubt und geht als `MAX_DEADLINE_MS` an den Broker. Bis 0.13.0 ging ein größerer Wert ungekappt durch (`retry_max_delay` deckelt nur den berechneten Backoff): RabbitMQ 4.3.6 lehnte die Expiration der Delay-Kopie mit `PRECONDITION_FAILED` ab und schloss den gemeinsamen Publish-Kanal, der Rückfall `redeliver_in_place` zählte nicht gegen `max_retries`; ab etwa 1,8e305 warf schon `int(delay * 1000)` einen `OverflowError` außerhalb jedes `try`, die Zustellung blieb unbestätigt und das Unique-Flag wurde freigegeben. Nach dem Fix ist dieser Pfad über `Retry` unerreichbar: `delay * 1000` bleibt höchstens `MAX_DEADLINE_MS`. Wie bei `inf` kommt der `ValueError` aus dem Konstruktor als gewöhnlicher Task-Fehler an (`retry_exceptions`, Backoff, zählt gegen `max_retries`). Nicht geprüft wird ein nachträglich gesetztes Attribut `delay` oder eine Unterklasse, die `Retry.__init__` umgeht; das gilt schon für `inf`/`nan`.
