---
title: Ein Konfigurationsfehler von swarmq worker endet nach einem Versuch mit Exit 78; ImportError und SyntaxError des Task-Moduls zählen dazu, andere Ausnahmen beim Import nicht
applies_when:
  - Änderungen an process_manager/entry.py, cli/worker_cmd.py, run_worker_async oder am Monitor des Supervisors
  - eine Fehlersuche meldet, dass ein Task-Modul mit TypeError oder ValueError auf Modulebene mit Exit 1 und Neustarts endet
  - eine Fehlersuche meldet, dass ein Verbindungsfehler zu Broker oder Backend mit 1 statt 78 endet
  - ein Plan führt eine neue Startablehnung ein
tags: [cli, exit-code, worker, konfiguration, import, b12, f3-05]
id: E-057
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-05)
  - docs/plans/2026-09-08-001-feat-debug-backlog-loadsuite-plan.md:101 (R19), :234 (KTD8, jeder ImportError aus dem Import des Task-Moduls wird ConfigurationError)
  - website/reference/cli.md:35-43 (Exit-Code-Tabelle, Absatz zu 78)
  - website/reference/changelog.md:2461-2467 (0.6.0, Exit code 78 for configuration errors)
tests:
  - tests/unit/process_manager/test_entry.py::TestConfigErrorExitCode::test_import_error_in_the_child_is_a_configuration_error
  - tests/unit/test_cli.py::TestMainErrorRendering::test_module_not_found_exits_with_the_config_code
  - tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py::test_import_error_in_the_task_module_exits_with_config_code
  - tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py::test_syntax_error_in_the_task_module_exits_with_config_code
  - tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py::test_indentation_error_in_the_task_module_exits_with_config_code
  - tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py::test_other_exceptions_while_importing_the_task_module_leave_the_entry
  - tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py::test_syntax_error_in_the_configure_hook_module_exits_with_config_code
  - tests/unit/process_manager/test_check.py::TestSyntaxErrorInPackageInit::test_module_in_a_package_with_a_syntax_error_is_a_configuration_error
  - tests/unit/process_manager/test_check.py::TestSyntaxErrorInPackageInit::test_configure_hook_in_a_package_with_a_syntax_error_is_a_configuration_error
  - tests/unit/process_manager/test_check.py::TestSyntaxErrorInPackageInit::test_worker_start_ends_with_78_without_traceback
---

**Entscheidung.** Jeder Konfigurationsfehler von `swarmq worker` endet nach einem einzigen Versuch mit Exit 78 (`EX_CONFIG`) und einem Logsatz `worker_config_error`, im Elternprozess wie im Kind. Dazu zählen `ConfigurationError`, jeder `ImportError` (einschließlich `ModuleNotFoundError`) und jeder `SyntaxError` beim Import des Task-Moduls. Andere Ausnahmen beim Import (etwa `TypeError` oder `ValueError` auf Modulebene, auch aus `TaskInfo(...)`) bleiben Exit 1. Verbindungsfehler zu Broker oder Backend bleiben Exit 1.

**Begründung.** Ein deterministisch kaputtes Deployment soll nicht fünfmal neu starten und mit `worker_restart_limit_exceeded` enden; Orchestratoren sollen „nicht neu starten“ von „später wieder versuchen“ unterscheiden (B12, KTD8). `SyntaxError` ist genauso deterministisch und fällt unter „task module not importable“. Bei beliebigen Ausnahmen auf Modulebene lässt sich ein Code-Fehler nicht sicher von einem Konfigurationsfehler trennen.

**Bewusste Folgen.** Ein Task-Modul, das beim Import `ValueError` wirft (z. B. eine ungültige `TaskInfo`, E-065), endet mit Exit 1 und Neustarts bis zum Restart-Limit. Das ist kein Fehler. Dass bis 0.12.2 nur `ModuleNotFoundError` und nicht jeder `ImportError` 78 ergibt, war der Befund F3-05 (behoben in 0.12.3: das Worker-Entry fängt `ImportError` und `SyntaxError`, der Pre-Flight im Elternprozess zusätzlich `SyntaxError` aus einem Paket-`__init__`; ein `SyntaxError` in einem `--configure`-Hook-Modul endet ebenso mit 78).

**Nachtrag 2026-10-15 (Review U9): Reichweite des Fangs im Worker-Entry.** Das Worker-Entry fängt `ImportError` und `SyntaxError` um den ganzen Lauf von `run_worker_async`, nicht nur um den Import des Task-Moduls und des `--configure`-Hooks. Ein `ImportError`, der erst nach erfolgreichem Import zur Laufzeit entweicht (etwa ein fauler Import eines optionalen Pakets), endet deshalb ebenfalls mit 78 und beendet den Supervisor, statt einen Neustart auszulösen. Eingrenzen ließe sich der Fang nur über ein zusätzliches Signal von `run_worker_async` an das Entry („Importphase abgeschlossen“), denn das Entry sieht nur die Ausnahme, und `run_worker_async` muss als Ganzes austauschbar bleiben (`tests/unit/process_manager/test_entry.py` ersetzt es durch Attrappen, die direkt einen `ModuleNotFoundError` werfen). Diese Folge ist bewusst hingenommen; ein Gegenbeispiel aus dem Betrieb ist nicht bekannt.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
