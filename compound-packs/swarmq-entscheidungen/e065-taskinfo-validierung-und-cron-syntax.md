---
title: TaskInfo prüft jedes Feld beim Import des Task-Moduls; Cron nur als Standard-Ausdruck mit 5 Feldern ohne L/W/#, gemeldete Crons ohne berechenbaren nächsten Termin verwirft der Leader
applies_when:
  - Änderungen an TaskInfo.__post_init__, _FORBIDDEN_CRON_CHARS oder an der Übernahme gemeldeter Crons (scheduler/sync.py)
  - eine Fehlersuche meldet, dass eine Cron-Angabe mit L, W oder # beim Import abgelehnt wird
  - eine Fehlersuche meldet, dass ein ungültiger Wert in TaskInfo erst beim Import statt beim ersten schedule() auffällt
  - ein Plan will die Cron-Syntax erweitern (Sekunden- oder Jahresfeld, L/W/#)
  - Änderungen an der Cron-Prüfung von swarmq schedule add --cron (cli/schedule_cmd.py)
  - eine Fehlersuche meldet, dass WED, @DAILY oder andere großgeschriebene Namen abgelehnt werden
  - eine Fehlersuche meldet, dass R, H oder ein 6/7-feldriger Ausdruck abgelehnt wird
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
  - 'Georg 2026-10-17 (Fehlersuche Runde 4, Produktfrage B: PF-12, PF-13/F3-35, PF-15, F4-09)'
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-12, PF-13, PF-15; FEHLER F4-09)
tests:
  - tests/unit/test_task.py::test_taskinfo_cron_extended_L_syntax_rejected
  - tests/unit/test_task.py::test_taskinfo_cron_extended_W_syntax_rejected
  - tests/unit/test_task.py::test_taskinfo_cron_extended_hash_syntax_rejected
  - tests/unit/test_task.py::test_taskinfo_cron_invalid_syntax_raises
  - tests/unit/scheduler/test_reports.py::TestAdoptReportedCrons::test_invalid_report_is_not_adopted_and_logged
  - tests/unit/test_befund_f3_08_cron_without_next_fire.py::test_taskinfo_rejects_cron_that_never_fires
  - tests/unit/test_task.py::test_taskinfo_cron_accepts_the_crontab5_positive_list
  - tests/unit/test_cli_schedule_add_contract.py::test_cli_cron_accepts_the_crontab5_positive_list
  - tests/unit/test_befund_f4_09_cron_uppercase_names.py::test_standard_names_are_accepted_in_any_case
---

**Entscheidung.** `TaskInfo.__post_init__` prüft jedes Feld beim Klassenbau; ein falscher Wert wirft `ValueError` beim Import des Task-Moduls, nicht beim ersten `schedule()`. Ein Cron ist ein Standard-Ausdruck mit 5 Feldern; erweiterte Syntax (`L`, `W`, `#`) wird abgelehnt. Der Leader prüft jeden gemeldeten Cron noch einmal (Quelle `static`, Ausdruck vorhanden, nächster Termin in der Zone berechenbar) und verwirft ihn sonst mit `scheduler_cron_report_invalid`.

**Begründung.** Ein falsch konfigurierter Task soll den Start des Workers scheitern lassen, nicht die Anfrage eines Nutzers Stunden später („This is deliberate“). Die Prüfung beim Leader schützt vor Meldungen älterer oder fremder Worker.

**Bewusste Folgen.** Ein `ValueError` beim Import endet nach E-057 mit Exit 1, nicht 78. Ein Cron mit `L`/`W`/`#` lässt sich nicht nutzen; das ist kein Fehler.

**Nachtrag 2026-10-17 (Fehlersuche Runde 4, Georg; Produktfrage B: PF-12, PF-13/F3-35, PF-15, F4-09): Positivliste nach crontab(5).** Erlaubt ist genau, was crontab(5) beschreibt: genau 5 Felder; Zahlen, `*`, `-`, `,`, `/`; Monats- und Tagesnamen ohne Rücksicht auf Groß- und Kleinschreibung (`mon-fri`, `WED`, `Jul`); die `@`-Aliase (`@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`/`@annually`, ebenfalls ohne Rücksicht auf die Schreibweise). Alles andere ist `ValueError`: `L`, `W`, `#` als Operatoren, croniters Zufallsfeld `R`/`r`, Hash-Feld `H` und 6- oder 7-feldrige Ausdrücke (Sekunden- oder Jahresfeld). Die Prüfung gilt für `TaskInfo` und ebenso für `swarmq schedule add --cron` (dort `ConfigurationError`, Exit 78). Damit ist F3-35 entschieden (6/7 Felder werden abgelehnt; neues Argument PF-13: `0 0 9 * * *` feuert bei croniter am 9. jedes Monats sekündlich statt täglich um 9 Uhr). „Erweiterte Syntax (`L`, `W`, `#`)“ meint die Operatoren, nicht Buchstaben in Namen (F4-09). Festgeschrieben: die erlaubten Formen in `TaskInfo` und `schedule add --cron` (grün) und großgeschriebene Namen über den Nachweis F4-09 (`xfail(strict=True)`, Marker E-065). Die Ablehnungstests für `R`, `H`, 6/7 Felder und für die CLI folgen mit dem Fix (Bündel 5 des Fix-Plans 0.13).

**Überdenken nur mit neuem Argument.** Dass `TaskInfo` Crons ohne nächsten Termin (`0 0 30 2 *`) annimmt und der Leader sie still verwirft, war der Befund F3-08, behoben in 0.12.3: `TaskInfo` berechnet den nächsten Termin wie der Leader und wirft sonst `ValueError` („has no next fire time“). F3-35 (`croniter` nimmt 6- und 7-feldrige Ausdrücke an, obwohl die Doku 5 Felder nennt) ist mit dem Nachtrag 2026-10-17 entschieden: abgelehnt; umgesetzt wird mit dem Fix-Plan 0.13.
