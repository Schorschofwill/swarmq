---
title: Ein Fire ohne Unique-Angabe in der Definition dedupliziert der Leader nach seiner eigenen TaskInfo
applies_when:
  - Änderungen an scheduler/fire.py (Zweig „Static cron or plain CLI“) oder an der Unique-Auflösung beim Fire
  - eine Fehlersuche meldet, dass ein Cron- oder CLI-Fire trotz fehlender Unique-Angabe in der Definition dedupliziert wird
  - ein Fix für FS-14 will den Rückfall auf die Leader-TaskInfo entfernen
tags: [scheduler, unique, dedup, cron, leader]
id: E-032
datum: 2026-04-17
status: gilt
quelle:
  - docs/plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md (Unit 9, Scheduler-seitige Dedup, Skizze „else: Static cron — resolve TaskInfo and SETNX“)
  - docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:107-109 (unique_for greift auch bei Cron-/Delay-/ETA-Fires)
  - src/swarmq/scheduler/fire.py:141 („Static cron or plain CLI — resolve TaskInfo and SETNX here“)
  - website/guides/scheduling-and-retries.md:220-228 (Definition ohne unique_for/unique_until nutzt die TaskInfo des Leaders)
tests:
  - tests/unit/scheduler/test_fire.py::test_old_definition_takes_both_unique_values_from_the_registry
---

**Entscheidung.** Trägt eine Schedule-Definition keine eigenen Werte für `unique_for`/`unique_until` (statischer Cron vor 0.12, CLI-Schedule, `source != "cli_unique"`), löst der Leader beim Fire die `TaskInfo` aus seiner eigenen Registry auf und setzt den Unique-Claim danach. Beide Werte kommen dann gemeinsam aus der Registry, nicht gemischt mit Defaults der Definition.

**Begründung.** `unique_for` soll auch für Cron-, Delay- und ETA-Fires gelten. Definitionen, die keine Werte tragen, haben keine andere Quelle als die Registry des Leaders.

**Bewusste Folgen.** Ein Cron-Fire, das wegen eines laufenden Duplikats übersprungen wird, obwohl die Definition selbst kein `unique_for` nennt, ist kein Fehler. Kennt der Leader den Task nicht, gibt es keine Dedup (nur die Warnung `scheduler_task_not_in_local_registry`).

**Überdenken nur mit neuem Argument.** Offen: FS-14. Der Client speichert bei `schedule(delay=/eta=, unique_for=None)` den ausdrücklichen Opt-out nicht in der Definition (`source="cli"`), sodass dieser Rückfall den Opt-out aushebelt. Das ist ein Fehler gegen E-033, keine Frage dieser Entscheidung: Der Fix muss den Opt-out in der Definition persistieren, statt den Rückfall zu entfernen. Die Produktfrage, ob CLI-Definitionen überhaupt auf die Leader-TaskInfo zurückfallen sollen, ist offen.
