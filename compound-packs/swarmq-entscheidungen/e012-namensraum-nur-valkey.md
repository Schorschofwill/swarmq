---
title: Der optionale App-Namensraum präfixt nur Valkey (Keys, Kanäle, Leader, Kontrollkanal); Queues bleiben ohne Präfix, RabbitMQ trennt per vhost
applies_when:
  - mehrere Apps teilen sich ein Valkey oder einen RabbitMQ
  - Änderungen an keyspace.py, ValkeyBackend.reconcile_namespace, SWARMQ_NAMESPACE oder Queue-Namen
  - eine Fehlersuche meldet, dass zwei Apps mit Namensraum sich trotzdem die Queue default teilen
  - eine Fehlersuche meldet, dass der Worker mit „namespace mismatch“ oder wegen Großbuchstaben mit Exit 78 nicht startet
  - eine Fehlersuche meldet, dass swarmq schedule, dlq, inspect oder check bei ungültigem Namensraum mit 78 statt 1 enden
tags: [namensraum, valkey, multi-app, konfiguration, bz-10]
id: E-012
datum: 2026-10-09
status: gilt
quelle:
  - docs/befunde/betrieb-ueber-zeit-2026-10.md:130 (Entscheidungen, BZ-10)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:47 (Abschnitt 1, BZ-10 nur Valkey + vhost je App)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:72 (Nachtrag Namensraum aus Backend und Konfiguration, Georg 2026-10-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:73 (Nachtrag Quellen des Namensraums, Georg 2026-10-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:76 (Kleinschreibung, technische Entscheidung 2026-10-11)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:714 (Abschnitt 7, BZ-10)
  - src/swarmq/cli/__init__.py:480-489 (BZ-10, MR 15, Namensraum vor jeder Verbindung prüfen, Exit 78)
  - docs/befunde/fehlersuche-runde-3-2026-10.md (F3-24, Nachtrag 2026-10-15)
tests:
  - tests/unit/test_keyspace.py::test_namespace_prefixes_every_key_and_keeps_the_hash_tags
  - tests/unit/test_keyspace.py::test_invalid_namespace_is_rejected
  - tests/unit/test_namespace_lowercase.py::test_uppercase_environment_namespace_is_a_configuration_error
  - tests/unit/test_app_namespace.py::test_different_backend_and_config_namespace_is_a_configuration_error
  - tests/unit/test_namespace_guard.py::test_worker_cli_exits_78_on_an_invalid_namespace
  - tests/unit/backend/test_valkey_namespace.py::test_the_control_channel_uses_the_namespace
  - tests/integration/test_befund_bz10_shared_valkey.py::test_same_lock_name_in_two_apps_does_not_block_the_other_app
  - tests/unit/test_cli_env_check_namespace.py::test_invalid_namespace_exits_78
---

**Entscheidung.** `SWARMQ_NAMESPACE` ist optional; ohne Angabe bleibt alles wie bisher (nicht BREAKING). Gesetzt präfixt er alle Valkey-Keys, Pub/Sub-Kanäle, den Leader und den Kontrollkanal mit `<ns>:sq:` (Hash-Tags bleiben). Queue-Namen bleiben ohne Präfix; für RabbitMQ gilt „eigener vhost je App“. Ein ausdrücklicher Backend-Parameter geht vor der Konfiguration; sind beide gesetzt und verschieden, `ConfigurationError` „namespace mismatch“ (Exit 78). Der Namensraum muss kleingeschrieben sein (`[a-z0-9][a-z0-9_.-]{0,63}`, nicht `sq`).

**Begründung.** Mehrere Apps auf einem Valkey teilten Rate-Limit-, Lock- und Unique-Keys und den Leader (BZ-10). Queue-Präfixe hätten jede bestehende Topologie gebrochen; RabbitMQ hat mit vhosts eine eigene Trennung. Kleinschreibung verhindert zwei Apps, die sich nur in der Schreibweise unterscheiden.

**Bewusste Folgen.** Zwei Apps im selben vhost teilen sich Queues trotz Namensraum. Bei getrennten vhosts ist der Namensraum Pflicht. Nach dem Setzen kein Rollback unter 0.12; Umzug einer App nur mit vollständigem Stopp und Neuanlage der Termine; ACL-Muster und swarmv müssen nachziehen.

**Nachtrag 2026-10-15 (Fehlersuche Runde 3, F3-24): Exitcode außerhalb von `swarmq worker`.** Ein ungültiger Namensraum ist auch bei `swarmq schedule`, `dlq`, `inspect` und `check` ein Konfigurationsfehler mit Exit 78, geprüft vor jeder Verbindung (BZ-10, MR 15). Das ist eine gewollte Ausnahme von `cli.md:32` („jeder Konfigurationsfehler von schedule/dlq/inspect endet mit 1“); die Doku wird nachgezogen (F3-24, Doku-Fix). Festgehalten in `tests/unit/test_cli_env_check_namespace.py::test_invalid_namespace_exits_78` (ohne `entscheidung`-Marker).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
