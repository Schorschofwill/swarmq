---
title: run() darf delay, eta, unique_for und unique_until nicht als Parameter tragen (ValueError beim Klassenbau); queue, timeout und polling sind nicht reserviert, die Grenze ist dokumentiert
applies_when:
  - Änderungen an _RESERVED_RUN_PARAM_NAMES, an der Registrierung von Task-Klassen oder an den Parametern von schedule()/SwarmQ.run
  - eine Fehlersuche meldet, dass run(self, queue) angenommen wird, obwohl schedule() queue abfängt
  - ein Plan will einen weiteren Parameternamen reservieren
tags: [task, api, schedule, kompatibilitaet, f3-10]
id: E-064
datum: 2026-04-17
status: gilt
quelle:
  - docs/plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md:401 (Erweiterung um unique_for/unique_until), :1248 (Migrationshinweis, Changelog)
  - docs/plans/2026-05-31-001-feat-dishka-di-tasks-plan.md:33 (R7, gilt auch für injizierte Parameter)
  - src/swarmq/task.py:37-42 (_RESERVED_RUN_PARAM_NAMES mit Begründung)
  - website/guides/defining-tasks.md:155-158 (Reserved parameter names)
  - Orchestrator nach Gegenprüfung G3/G4 (Fehlersuche Runde 3, F3-10, Doku-Fix statt Reservierung, 2026-10-15)
tests:
  - tests/unit/test_task.py::test_task_subclass_with_delay_parameter_raises
  - tests/unit/test_task.py::test_task_subclass_with_eta_parameter_raises
  - tests/unit/test_task.py::test_task_subclass_reserved_unique_for_raises
  - tests/unit/test_task.py::test_task_subclass_reserved_unique_until_raises
ohne_test: Zu queue entsteht kein Test (F3-10 ist ein Doku-Fix).
---

**Entscheidung.** Eine Task-Klasse, deren `run()` (auch per Dependency Injection) einen Parameter `delay`, `eta`, `unique_for` oder `unique_until` deklariert, wirft beim Klassenbau `ValueError` mit Migrationshinweis. `queue` (bei `schedule()`) sowie `timeout` und `polling` (bei `SwarmQ.run`) werden bewusst nicht reserviert; die Doku nennt sie als Grenze in der Liste „Reserved parameter names“.

**Begründung.** `schedule()` fängt diese Namen ab, bevor die kwargs an den Task gehen; ein so benannter Parameter verlöre still seine Nutzlast. `queue`, `timeout` und `polling` nachträglich zu reservieren, würde bestehende Tasks in einem Patch-Release brechen.

**Bewusste Folgen.** `run(self, queue)` wird angenommen; ein Aufruf mit `queue=` routet den Task, statt das Argument zu übergeben. Das ist kein Fehler gegen diese Regel; die Doku warnt davor (F3-10, Doku-Fix in 0.12.3).

**Überdenken nur mit neuem Argument.** Eine Reservierung von `queue`/`timeout`/`polling` ist ein Kandidat für ein Minor-Release mit Changelog-Hinweis.
