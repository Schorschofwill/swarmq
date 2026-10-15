---
title: TaskInfo prüft jedes Feld beim Import des Task-Moduls; Cron nur als Standard-Ausdruck mit 5 Feldern ohne L/W/#, gemeldete Crons ohne berechenbaren nächsten Termin verwirft der Leader
applies_when:
  - Änderungen an TaskInfo.__post_init__, _FORBIDDEN_CRON_CHARS oder an der Übernahme gemeldeter Crons (scheduler/sync.py)
  - eine Fehlersuche meldet, dass eine Cron-Angabe mit L, W oder # beim Import abgelehnt wird
  - eine Fehlersuche meldet, dass ein ungültiger Wert in TaskInfo erst beim Import statt beim ersten schedule() auffällt
  - ein Plan will die Cron-Syntax erweitern (Sekunden- oder Jahresfeld, L/W/#)
tags: [taskinfo, cron, validierung, scheduler, f3-08, f3-35]
id: E-065
datum: 2026-04-16
status: gilt
quelle:
  - Commit a1a8c99 (TaskInfo.cron mit fail-fast-Validierung, Unit 3)
  - src/swarmq/task.py:30-33 (L/W/# bewusst nicht unterstützt, Ablehnung beim Import), :234-243
  - website/reference/configuration.md:471 (Standard 5-field cron expression), :494-500 (validates every field at class definition time, This is deliberate)
  - docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md:160 (KTD10, Cron-Meldungen)
  - src/swarmq/scheduler/sync.py:30-41 (_validated, nächster Termin muss berechenbar sein)
tests:
  - tests/unit/test_task.py::test_taskinfo_cron_extended_L_syntax_rejected
  - tests/unit/test_task.py::test_taskinfo_cron_extended_W_syntax_rejected
  - tests/unit/test_task.py::test_taskinfo_cron_extended_hash_syntax_rejected
  - tests/unit/test_task.py::test_taskinfo_cron_invalid_syntax_raises
  - tests/unit/scheduler/test_reports.py::TestAdoptReportedCrons::test_invalid_report_is_not_adopted_and_logged
---

**Entscheidung.** `TaskInfo.__post_init__` prüft jedes Feld beim Klassenbau; ein falscher Wert wirft `ValueError` beim Import des Task-Moduls, nicht beim ersten `schedule()`. Ein Cron ist ein Standard-Ausdruck mit 5 Feldern; erweiterte Syntax (`L`, `W`, `#`) wird abgelehnt. Der Leader prüft jeden gemeldeten Cron noch einmal (Quelle `static`, Ausdruck vorhanden, nächster Termin in der Zone berechenbar) und verwirft ihn sonst mit `scheduler_cron_report_invalid`.

**Begründung.** Ein falsch konfigurierter Task soll den Start des Workers scheitern lassen, nicht die Anfrage eines Nutzers Stunden später („This is deliberate“). Die Prüfung beim Leader schützt vor Meldungen älterer oder fremder Worker.

**Bewusste Folgen.** Ein `ValueError` beim Import endet nach E-057 mit Exit 1, nicht 78. Ein Cron mit `L`/`W`/`#` lässt sich nicht nutzen; das ist kein Fehler.

**Überdenken nur mit neuem Argument.** Dass `TaskInfo` Crons ohne nächsten Termin (`0 0 30 2 *`) annimmt und der Leader sie still verwirft, ist der Befund F3-08 (Fix in 0.12.3). Offen ist F3-35: `croniter` nimmt 6- und 7-feldrige Ausdrücke an, obwohl die Doku 5 Felder nennt.
