---
title: swarmq worker --check prüft alle Werte, die der Start mit 78 ablehnt, aber nicht fehlende Broker- und Backend-URLs; argparse-Fehler kommen als JSON mit Exit 1
applies_when:
  - Änderungen an cli/check_cmd.py, check_worker_config oder am Pre-Flight des Supervisors
  - ein Plan führt eine neue Startablehnung ein (sie gehört auch in --check)
  - eine Fehlersuche meldet, dass --check ohne SWARMQ_BROKER_URL oder SWARMQ_BACKEND_URL ok true meldet
  - eine Fehlersuche meldet, dass --check bei einem argparse-Fehler Exit 1 statt 2 liefert
tags: [cli, check, konfiguration, exit-code, f3-17, f3-19]
id: E-062
datum: 2026-10-15
status: gilt
quelle:
  - Georg 2026-10-15 (Fehlersuche Runde 3, F3-17)
  - docs/plans/2026-08-28-001-fix-publish-deadline-port-guard-logfix-plan.md:306 (ein erfolgreiches --check impliziert den Pre-Flight des Supervisors)
  - docs/plans/2026-09-03-001-feat-cpu-worker-modus-plan.md:446 (--check und Start stimmen überein, Muster TestPreflightCommandAgrees)
  - website/reference/cli.md:11-12 (validates exactly the values the workers would get), :32-33 (Exit 1 für jeden --check-Fehler, auch argparse)
  - Commit 656078c (R5.3 ce-review pass 2, argparse-Fehler unter --check als JSON)
  - src/swarmq/cli/check_cmd.py:22-39 (stabiler Feldsatz des Payloads), :291-309
tests:
  - tests/unit/test_cli_multiprocess.py::TestMainRouting::test_check_argparse_error_emits_json
  - tests/unit/process_manager/test_port_guard.py::TestPreflightCommandAgrees::test_payload_names_both_ranges
  - tests/unit/process_manager/test_port_guard.py::TestPreflightCommandAgrees::test_checks_the_effective_values_cli_over_environment
  - tests/unit/process_manager/test_port_guard.py::TestPreflightCommandAgrees::test_environment_port_is_named_in_the_error
  - tests/unit/process_manager/test_port_guard.py::TestPreflightCommandAgrees::test_reports_the_overlap_instead_of_ok
  - tests/unit/test_befund_f3_17_worker_check_env.py::test_worker_check_rejects_env_values_the_start_rejects
  - tests/unit/test_befund_f3_17_worker_check_env.py::test_worker_check_does_not_require_broker_and_backend_urls
  - tests/unit/test_befund_f3_19_check_argparse_payload.py::test_argparse_error_payload_carries_every_contract_key
  - tests/unit/test_befund_f3_19_check_argparse_payload.py::test_configuration_error_payload_carries_every_contract_key
---

**Entscheidung.** `swarmq worker --check` sagt den Start voraus: Ein `ok: true` heißt, dass der Pre-Flight des Supervisors und die Konfigurationsprüfung des Workers bestehen. Geprüft werden dazu alle Env-Werte, die der Start mit 78 ablehnt (z. B. `SWARMQ_SCHEDULER`, `SWARMQ_CONCURRENCY`, `SWARMQ_SHUTDOWN_TIMEOUT`, `SWARMQ_VALKEY_EVICTION_CHECK`). Fehlende `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL` prüft `--check` nicht; die Doku sagt das. Ein argparse-Fehler unter `--check` kommt als JSON-Payload mit Exit 1 (nicht 2), mit demselben stabilen Feldsatz wie jeder andere `--check`-Fehler.

**Begründung.** Agenten und Deploy-Pipelines verlassen sich darauf, dass ein grünes `--check` keinen Start mit 78 nach sich zieht. `--check` läuft aber oft im Build-Schritt ohne Secrets; die URLs dort zu verlangen, machte die Prüfung dort unbrauchbar. Ein einheitlicher JSON-Vertrag erspart Agenten die Sonderbehandlung von stdout ohne Payload.

**Bewusste Folgen.** Ein `--check` ohne URLs besteht, der Start danach kann wegen der fehlenden URL mit 78 enden. Das ist kein Fehler. Exit 2 für argparse-Fehler gilt unter `--check` nicht; `message-formats.md:525-526` ist falsch (F3-19, Doku-Fix).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt. Dass `--check` bis 0.12.2 vier abgelehnte Env-Werte durchließ, war der Befund F3-17; der unvollständige Payload bei argparse-Fehlern war F3-19 (beide behoben in 0.12.3: `--check` baut die Konfiguration über denselben Helfer `start_config` wie der Pre-Flight des Starts, und jeder Payload, auch im Fehlerpfad, trägt `metrics_ports` und `health_ports`).
