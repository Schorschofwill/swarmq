---
title: Schalter aus der Umgebung nehmen nur true/1/yes/on und false/0/no/off an, alles andere ist ein ConfigurationError; der Loglevel gilt überall ohne Rücksicht auf Groß- und Kleinschreibung
applies_when:
  - Änderungen am Lesen von bool-Variablen (SwarmQConfig._ENV_OVERRIDES, logging_settings.parse_env_bool)
  - Änderungen an der Prüfung von log_level, log_lifecycle_level oder am Flag --log-level
  - eine Fehlersuche meldet, dass ein Schalterwert wie „on“ still als „aus“ gilt oder ein unbekannter Wert abgelehnt wird
  - eine Fehlersuche meldet, dass log_level="debug" angenommen wird
tags: [konfiguration, env, logging, cli, breaking]
id: E-086
datum: 2026-10-26
status: gilt
quelle:
  - "Georg 2026-10-26: Produktfragen V4a und V4b, Option „Streng und einheitlich“"
  - docs/befunde/doku-pruefung-2026-10.md (LÜCKEN V4a, V4b)
tests:
  - tests/unit/test_entscheidung_e086_env_switches.py::test_bool_switch_accepts_on_and_off_words
  - tests/unit/test_entscheidung_e086_env_switches.py::test_unknown_bool_switch_value_is_a_configuration_error
  - tests/unit/test_entscheidung_e086_env_switches.py::test_log_json_alias_follows_the_same_rule
  - tests/unit/test_entscheidung_e086_env_switches.py::test_configure_logging_follows_the_same_rule
  - tests/unit/test_entscheidung_e086_env_switches.py::test_embedded_log_levels_ignore_case
  - tests/unit/test_entscheidung_e086_env_switches.py::test_embedded_log_level_from_env_ignores_case
  - tests/unit/test_entscheidung_e086_env_switches.py::test_cli_log_level_flag_ignores_case
  - tests/unit/test_entscheidung_e086_env_switches.py::test_unknown_log_level_stays_a_configuration_error
---

**Entscheidung.**
- Alle bool-Variablen (`SWARMQ_PRIORITIES`, `SWARMQ_BURST_MODE`, `SWARMQ_EXECUTION_GUARD`, `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES`, `SWARMQ_LOG_QUEUE`, `SWARMQ_LOG_JSON`, `SWARMQ_CONFIGURE_LOGGING`) lesen `true`, `1`, `yes`, `on` als „an“ und `false`, `0`, `no`, `off` als „aus“, jeweils in beliebiger Schreibung und ohne Leerraum am Rand. Jeder andere Wert ist ein `ConfigurationError` mit dem Variablennamen (Worker-Start und `swarmq check`: Exit 78). Eine leere Variable gilt weiter als nicht gesetzt.
- `log_level` und `log_lifecycle_level` werden überall in Großbuchstaben umgewandelt, bevor sie geprüft werden: in `SwarmQConfig(...)`, über `SWARMQ_LOG_LEVEL`/`SWARMQ_LOG_LIFECYCLE_LEVEL` im eingebetteten Betrieb und im Flag `swarmq worker --log-level`. Ein unbekannter Level bleibt ein Fehler.

**Begründung.** Bis 0.14.4 galt jeder unbekannte Schalterwert still als „aus“: `SWARMQ_PRIORITIES=on` legte keine Prioritäts-Queues an, und erst ein Publish mit `priority="high"` scheiterte. Der Nachbarschalter `SWARMQ_SCHEDULER` verlangt gerade `on`/`off` und lehnt Unbekanntes ab (E-006). Beim Loglevel nahm nur `SWARMQ_LOG_LEVEL` im Worker jede Schreibung an, eingebettet und als Flag dagegen nur Großbuchstaben. Eine Regel für alle Wege ist leichter zu merken.

**Bewusste Folgen.**
- BREAKING: Ein Wert, der bisher still „aus“ bedeutete (etwa `SWARMQ_BURST_MODE=disabled`), lässt den Worker jetzt mit Exit 78 nicht starten. Der Changelog nennt das.
- `swarmq check` meldet so einen Wert vorab mit 78 (E-006, „ungültige Konfiguration“).
- Ein in Python gesetzter bool (`SwarmQConfig(priorities=True)`) ist nicht betroffen.

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
