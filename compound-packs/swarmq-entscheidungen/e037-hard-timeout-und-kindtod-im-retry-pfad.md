---
title: Hard-Timeout und Tod des Rechenkinds laufen durch den Retry-Pfad, nicht über Nack mit Requeue
applies_when:
  - Änderungen am Hard-Timeout-Watchdog, am ComputeExecutor oder an der Auswertung von ChildDied
  - eine Fehlersuche meldet, dass ein Hard-Timeout oder Kindtod einen Retry verbraucht oder in der DLQ endet
  - ein Plan will einen Hard-Timeout oder Kindtod per Nack/Requeue an den Broker zurückgeben
tags: [timeout, retry, dlq, cpu-modus, fire-and-forget]
id: E-037
datum: 2026-04-15
status: gilt
quelle:
  - docs/plans/2026-04-15-003-feat-phase2-production-core-plan.md:77 (Hard-Timeout routed durch den Retry-Pfad statt nack(requeue=True)), :78 und :313-314 (FIRE_AND_FORGET nur cancel und loggen)
  - 'docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:92-93 (R8, R9: Kindtod wie Taskfehler, nicht über Nack mit Requeue), :258 (KTD2, user-approved)'
tests:
  - tests/unit/worker/test_worker_timeouts.py::test_hard_timeout_fires_and_retries
  - tests/unit/worker/test_worker_timeouts.py::test_hard_timeout_exhausted_retries_sends_to_dlq
  - tests/unit/worker/test_worker_timeouts.py::test_fire_and_forget_hard_timeout_logged_no_nack
  - tests/unit/worker/compute/test_executor.py::test_child_death_is_an_error_result
---

**Entscheidung.** Überschreitet ein Task seinen `hard_timeout` oder stirbt im CPU-Modus das Rechenkind ohne Ergebnis, wird das als retry-fähiger Taskfehler behandelt: Retry-Zähler, Backoff, DLQ nach `max_retries`. Die Nachricht geht nicht per Nack mit Requeue an den Broker zurück. Bei FIRE_AND_FORGET wird nur abgebrochen und geloggt, ohne Broker-Aufruf auf die schon bestätigte Nachricht.

**Begründung.** Ein Nack mit Requeue würde einen Task, der den Timeout immer reißt oder das Kind deterministisch abstürzen lässt, endlos neu zustellen. Über den Retry-Pfad erreicht auch er die DLQ.

**Bewusste Folgen.** Ein Hard-Timeout oder OOM-Kill verbraucht einen Retry und kann bei `max_retries=0` direkt in der DLQ landen. Das ist kein Fehler. Der Abbruch am Drain-Ende ist gesondert in E-001 geregelt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
