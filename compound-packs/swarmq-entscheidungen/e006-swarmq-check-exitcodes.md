---
title: swarmq check endet nur mit Exit 78, wenn ein Worker den Start verweigern würde; sonst Warnungen
applies_when:
  - Änderungen an cli/env_check_cmd.py oder an den Warnungen beim Valkey-Verbindungsaufbau
  - eine Fehlersuche meldet, dass swarmq check bei AOF aus, Valkey < 8 oder Cluster-Modus mit Exit 0 endet
  - eine Fehlersuche meldet, dass der Abschnitt queues ohne SWARMQ_BROKER_URL ungeprüft bleibt oder bei unerreichbarem Broker Exit 1 liefert
  - eine Fehlersuche meldet, dass Producer/CLI bei unsicherer Policy nur warnen
  - eine Fehlersuche meldet, dass swarmq check bei SWARMQ_VALKEY_EVICTION_CHECK=bogus oder einem nicht parsebaren Wert 78 statt 0 oder 1 liefern soll
tags: [cli, check, valkey, eviction, bz-04, bz-14]
id: E-006
datum: 2026-10-11
status: gilt
quelle:
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:78 (Nachtrag MR 14, von Georg am 2026-10-11 bestätigt)
  - docs/plans/2026-10-09-2314-fix-betrieb-ueber-zeit-plan.md:80 (Nachtrag MR 18, Abschnitt queues, 2026-10-11)
  - docs/befunde/fehlersuche-runde-3-2026-10.md (F3-18, Gegenprüfung G3, Nachtrag 2026-10-15)
  - src/swarmq/config.py:149-155 (env_valkey_eviction_check, ungültiger Modus gilt als strict)
tests:
  - tests/unit/test_cli_env_check.py::test_evicting_policy_under_strict_fails_like_the_worker
  - tests/unit/test_cli_env_check.py::test_evicting_policy_without_strict_only_warns
  - tests/unit/test_cli_env_check.py::test_without_aof_warns
  - tests/unit/test_cli_env_check.py::test_valkey_before_8_warns
  - tests/unit/test_cli_env_check.py::test_cluster_mode_warns
  - tests/unit/test_cli_env_check_queues.py::test_without_broker_url_queues_stay_unchecked
  - tests/unit/test_cli_env_check_queues.py::test_unreadable_queues_warn_without_failing
  - tests/integration/test_cli_env_check.py::test_unreachable_broker_exits_1
  - tests/unit/backend/test_befund_bz04_eviction_policy.py::test_connect_warns_when_valkey_may_evict_swarmq_keys
  - tests/unit/backend/test_valkey_environment.py::test_check_off_queries_nothing_and_stays_silent
---

**Entscheidung.** `swarmq check` endet mit Exit 78 nur bei Befunden, bei denen ein Worker den Start verweigern würde (verdrängende Policy unter `strict`, ungültige Konfiguration). AOF aus, Valkey < 8 und Cluster-Modus sind Warnungen (Exit 0). Abschnitt `queues`: ohne `SWARMQ_BROKER_URL` ungeprüft und befundfrei; mit URL und unerreichbarem Broker Exit 1 (wie bei Valkey); nicht lesbare Meldungen in Valkey ergeben die Warnung `queues_unchecked`. Producer und CLI-Befehle warnen bei unsicherer Policy einmal je Verbindung; `SWARMQ_VALKEY_EVICTION_CHECK=off` schaltet das ab.

**Begründung.** `check` soll vor dem Upgrade genau das vorhersagen, was der Worker tut, und keine zusätzlichen harten Hürden aufstellen. Producer haben keine Startsperre, sollen das Risiko aber sehen.

**Bewusste Folgen.** Eine Umgebung ohne AOF besteht `check` mit Exit 0 (nur Warnung). Ein Producer startet auch bei verdrängender Policy.

**Nachtrag 2026-10-15 (Fehlersuche Runde 3, F3-18): Was „ungültige Konfiguration“ heißt.** Gemeint ist jeder Wert, an dem `SwarmQConfig` beim Worker-Start scheitert, auch ein ungültiger Modus (`SWARMQ_VALKEY_EVICTION_CHECK=bogus`) und ein nicht parsebarer Wert (`SWARMQ_SHUTDOWN_TIMEOUT=abc`). `swarmq check` endet dann mit 78. Dass es heute im ersten Fall 0 (der Modus wird still zu `strict`) und im zweiten 1 liefert, ist der Befund F3-18 (Fix in 0.12.3), keine Frage dieser Entscheidung. Unberührt bleibt die Nachsicht für Backends ohne `SwarmQConfig` (Producer, CLI-Befehle): `env_valkey_eviction_check()` wertet einen ungültigen Modus als `strict`, damit die Prüfung trotzdem läuft. Für die Startvorhersage von `check` gilt diese Nachsicht nicht. Der Test entsteht mit dem Fix (`tests/unit/test_befund_f3_18_check_invalid_config_exit78.py`).

**Überdenken nur mit neuem Argument.** Keine offenen Einwände bekannt.
