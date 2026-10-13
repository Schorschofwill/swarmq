---
title: Der Soft-Timeout bricht run() über asyncio.wait_for ab und wird als retry-fähiger SoftTimeoutError gewertet
applies_when:
  - Änderungen an der Soft-Timeout-Umsetzung (worker/execution.py, compute/child.py) oder an SoftTimeoutError
  - eine Fehlersuche meldet, dass run() keinen fangbaren SoftTimeoutError sieht oder post_execution beim Soft-Timeout fehlt
  - Dokumentation zum Soft-Timeout wird geschrieben oder geändert
tags: [timeout, soft-timeout, retry, worker]
id: E-045
datum: 2026-04-15
status: gilt
quelle:
  - docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md:76 (Soft-Timeout via asyncio.wait_for), :79 (post_execution nicht aufgerufen), :312 (Umwandlung nur bei gesetztem soft_timeout)
  - docs/brainstorms/implementation-order-requirements.md:70 (R2.4, ursprünglich „fangbar“, durch die wait_for-Umsetzung ersetzt)
tests:
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_fires_when_run_exceeds_limit
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_triggers_retry
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_skips_post_execution
---

**Entscheidung.** Ist `soft_timeout` gesetzt, läuft `run()` unter `asyncio.wait_for`. Läuft die Zeit ab, wird `run()` abgebrochen (im Task-Körper als `CancelledError`), der Worker wertet das als `SoftTimeoutError`: Fehlerergebnis, retry-fähig, `post_execution` entfällt. Die Umwandlung gilt nur für den Soft-Timeout selbst.

**Begründung.** `wait_for` liefert den Abbruch ohne eigenen Watchdog und bleibt mit dem Hard-Timeout kombinierbar. Eine in `run()` fangbare Ausnahme lässt sich so technisch nicht liefern.

**Bewusste Folgen.** Ein Task, der den Soft-Timeout reißt, startet beim Retry von vorn; Aufräumen geht nur über `except CancelledError`/`finally` im Task. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Zwei offene Befunde der Fehlersuche Runde 2, beide Fehler gegen diese Entscheidung, nicht Teil davon:
- FS-20 (Doku): `website/guides/scheduling-and-retries.md:674-675` und `website/reference/errors.md:422-441` versprechen einen in `run()` fangbaren `SoftTimeoutError` samt Checkpoint-Beispiel. Die Doku beschreibt die verworfene Anforderung R2.4.
- FS-05: Eine eigene `TimeoutError` des Tasks wird unbedingt zu „SoftTimeoutError after None s“, auch ohne gesetzten `soft_timeout`.
