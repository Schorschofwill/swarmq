---
title: unique_for=0 und negative Werte sind ein ValueError, None schaltet die Deduplizierung ab; unique_until ohne unique_for wird still ignoriert
applies_when:
  - Änderungen an TaskInfo._validate_unique, an Client.schedule/schedule_many (Parameter unique_for) oder an der CLI-Option --unique-for
  - eine Fehlersuche meldet, dass unique_for=0 nicht still als „aus“ gilt
  - eine Fehlersuche meldet, dass unique_until ohne unique_for keinen Fehler und keine Warnung auslöst
tags: [unique, dedup, taskinfo, validierung, cli, f3-07]
id: E-056
datum: 2026-04-17
status: gilt
quelle:
  - docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:57, 128, 248 (CD6, fail-fast)
  - docs/plans/2026-04-17-004-feat-phase3-5-unique-tasks-plan.md:163-166 (unique_until ohne unique_for ignoriert, CD6-Kompromiss), :394-395
  - src/swarmq/task.py:253-267 (_validate_unique)
  - src/swarmq/cli/parser_ops.py:97-100 (--unique-for nur als positive Ganzzahl)
  - docs/befunde/fehlersuche-runde-3-2026-10.md (F3-07, Gegenprüfung G2)
tests:
  - tests/unit/test_task.py::test_taskinfo_unique_for_zero_raises
  - tests/unit/client/test_befund_f3_07_call_unique_for_bounds.py::test_schedule_rejects_non_positive_unique_for
  - tests/unit/client/test_befund_f3_07_call_unique_for_bounds.py::test_deferred_schedule_rejects_non_positive_unique_for
  - tests/unit/client/test_befund_f3_07_call_unique_for_bounds.py::test_schedule_many_rejects_non_positive_unique_for
---

**Entscheidung.** `unique_for=0` wirft `ValueError` („use None to disable dedup“), `unique_for < 0` ebenso. Abschalten geht nur mit `None` (E-033). `unique_until` ohne `unique_for` wird still ignoriert. Die CLI nimmt `--unique-for` nur als positive Ganzzahl an. Der Aufrufparameter `unique_for` von `schedule()`/`schedule_many()` folgt derselben Regel; dass er `<= 0` annahm, war der Befund F3-07 (behoben in 0.12.3).

**Begründung.** Ein still zu `None` normalisiertes `0` würde einen Tippfehler verdecken (CD6). Ein Fehler für `unique_until` ohne `unique_for` hätte in der eingefrorenen Dataclass einen Sentinel gebraucht, um „Default“ von „ausdrücklich gesetzt“ zu unterscheiden.

**Bewusste Folgen.** `TaskInfo(unique_until="start")` ohne `unique_for` hat keine Wirkung und meldet nichts. Das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Zum Typ `float` siehe E-055.
