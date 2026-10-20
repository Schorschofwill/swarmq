---
title: swarmq worker baut seine eigene SwarmQ-App; Signal-Handler und Middleware kommen über --configure MODULE:CALLABLE, nicht über eine modulweite app im Task-Modul
applies_when:
  - Änderungen an run_worker_async, an --configure oder an der App-Konstruktion der CLI
  - eine Fehlersuche meldet, dass auf einer modulweiten app registrierte Handler oder Middleware unter swarmq worker nicht wirken
  - eine Doku oder ein Guide registriert Handler auf einer app und startet mit swarmq worker --module
tags: [cli, middleware, signals, worker, b2, f3-06]
id: E-069
datum: 2026-09-08
status: gilt
quelle:
  - docs/plans/2026-09-08-001-feat-debug-backlog-loadsuite-plan.md:82 (R9), :231 (KTD5)
  - website/tutorials/first-email-worker.md:374-377 (The worker CLI builds its own SwarmQ)
  - website/reference/cli.md:99 (--configure)
tests:
  - tests/unit/test_cli.py::TestRunWorkerAsync::test_configure_hook_runs_with_the_app_before_start
  - tests/unit/test_cli.py::TestRunWorkerAsync::test_async_configure_hook_is_awaited
  - tests/unit/test_cli_multiprocess.py::TestCheckForwardsTheStartArguments::test_configure_hook_is_checked_and_reported
---

**Entscheidung.** `swarmq worker` konstruiert seine eigene `SwarmQ`-App. Das Task-Modul liefert nur die Tasks. Signal-Handler und Middleware registriert man in einem Callable, das `--configure MODULE:CALLABLE` nach der App-Konstruktion und vor dem Start mit der App aufruft (ein Awaitable wird abgewartet; jede Ausnahme wird `ConfigurationError`, E-057).

**Begründung.** Handler brauchen eine App-Instanz; zur Importzeit des Task-Moduls gibt es die der CLI noch nicht (B2, R9/KTD5).

**Bewusste Folgen.** Handler und Middleware auf einer modulweiten `app` im Task-Modul bleiben unter `swarmq worker` ohne Warnung stumm. Das ist kein Fehler im Code; Guides, die es anders zeigen, sind falsch (F3-06, Doku-Fix).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
