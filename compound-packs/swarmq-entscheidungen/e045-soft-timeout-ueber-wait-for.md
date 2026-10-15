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
  - 'docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md (U18, KTD14, R6/R20: Nachtrag FS-05/FS-20)'
tests:
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_fires_when_run_exceeds_limit
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_triggers_retry
  - tests/unit/worker/test_worker_timeouts.py::test_soft_timeout_skips_post_execution
  - tests/unit/worker/test_worker_timeouts.py::test_task_that_swallows_the_soft_timeout_cancel_stores_a_success_result
  - tests/unit/worker/test_soft_timeout.py::test_task_raising_its_own_timeout_error_after_the_cancel_keeps_it
---

**Entscheidung.** Ist `soft_timeout` gesetzt, läuft `run()` unter `asyncio.wait_for`. Läuft die Zeit ab, wird `run()` abgebrochen (im Task-Körper als `CancelledError`), der Worker wertet das als `SoftTimeoutError`: Fehlerergebnis, retry-fähig, `post_execution` entfällt. Die Umwandlung gilt nur für den Soft-Timeout selbst.

**Begründung.** `wait_for` liefert den Abbruch ohne eigenen Watchdog und bleibt mit dem Hard-Timeout kombinierbar. Eine in `run()` fangbare Ausnahme lässt sich so technisch nicht liefern.

**Bewusste Folgen.** Ein Task, der den Soft-Timeout reißt, startet beim Retry von vorn; Aufräumen geht nur über `except CancelledError`/`finally` im Task. Das ist kein Fehler.

**Nachtrag 2026-10-14 (FS-05, FS-20; U18).** Verschlucken ergibt Erfolg: Fängt der Task die `CancelledError` des Soft-Timeouts ab und kehrt normal zurück, wirft `wait_for` nichts; der Worker speichert ein Erfolgsergebnis mit dem Rückgabewert, `post_execution` läuft. Aufräumen mit Fehlerergebnis geht nur über `finally` oder `except asyncio.CancelledError: …; raise`. Die Umwandlung in `SoftTimeoutError` gilt nur für den eigenen Ablauf der Frist: Eine innere Hülle (`swarmq/worker/soft_timeout.py`) reicht eine `TimeoutError` des Tasks verpackt durch `wait_for`; sie kommt mit Klasse, Meldung und `error_class` beim Fehlerpfad an und greift bei `retry_exceptions=[TimeoutError]` (IO und CPU-Kind). `wait_for` bleibt. Grenzfall: Fängt der Task den Abbruch und wirft selbst `TimeoutError`, zählt das als Task-Timeout, nicht als Soft-Timeout. Laufen Drain-Abbruch und Fristablauf im selben Durchlauf auf, gilt E-001 (Abbruch, kein `SoftTimeoutError`).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände. Erledigt:
- FS-20 (Doku), behoben (U18, Plan 2026-10-14-0748): `website/guides/scheduling-and-retries.md`, `website/reference/errors.md` und `website/reference/configuration.md` (je EN/DE) versprachen einen in `run()` fangbaren `SoftTimeoutError` samt Checkpoint-Beispiel (verworfene Anforderung R2.4). Sie beschreiben jetzt `CancelledError` im Task, das nötige `raise` und den Erfolg beim Verschlucken.
- FS-05, behoben (U18, Plan 2026-10-14-0748): Eine eigene `TimeoutError` des Tasks wurde unbedingt zu „SoftTimeoutError after None s“, auch ohne gesetzten `soft_timeout`. Nachweis: `tests/unit/worker/test_befund_fs05_task_timeout_error.py`.
