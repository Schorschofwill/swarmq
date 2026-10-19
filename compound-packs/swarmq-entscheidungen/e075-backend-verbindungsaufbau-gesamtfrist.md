---
title: Der Verbindungsaufbau zum Backend (PING beim Verbinden) läuft unter einer Gesamtfrist und endet mit BackendConnectionError; die Wiederholpolitik zur Laufzeit bleibt
applies_when:
  - Änderungen an ValkeyBackend.connect (backend/valkey/connection.py), an socket_connect_timeout oder an der Retry-Politik des Valkey-Clients
  - eine Fehlersuche meldet, dass ein Worker gegen einen Host, der Pakete verwirft, minutenlang im Start hängt
  - ein Plan will Verbindungs- und Befehls-Wiederholung trennen (PF-16 b)
tags: [backend, valkey, verbindung, start, frist, pf-16, f4-12, f4-13, f4-14]
id: E-075
datum: 2026-10-17
status: gilt
quelle:
  - Georg 2026-10-17 (Fehlersuche Runde 4, PF-16, Option a)
  - docs/befunde/fehlersuche-runde-4-2026-10.md (LÜCKE PF-16, R4M-D-02b; Vor dem Fix Punkt 11)
  - Commit 111983a (socket_connect_timeout, bound every hang path)
  - docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-47 (Befehls-Retry-Politik)
  - docs/solutions/spikes/2026-10-spike-verbindungs-retry.md (U21, Messung Frist 600 s → 33,1 s)
  - 'Georg 2026-10-19 (Entscheidungsvorlage 0.13.0, Punkte 3a und 7: Doku jetzt, Variante c in 0.14)'
tests:
  - tests/unit/backend/test_valkey_connect_timeout.py::test_hanging_ping_ends_after_the_connect_deadline
  - tests/unit/backend/test_valkey_connect_timeout.py::test_silent_server_ends_connect_after_the_deadline
  - tests/unit/backend/test_valkey_connect_timeout.py::test_default_connect_deadline_is_thirty_seconds
  - tests/unit/test_config.py::test_config_backend_connect_timeout_default_30
  - tests/unit/test_app.py::TestBackendConnectTimeoutWiring::test_configured_deadline_reaches_the_backend
  - tests/unit/test_app.py::TestBackendConnectTimeoutWiring::test_explicit_backend_parameter_wins_over_the_configuration
  - tests/unit/test_app.py::TestBackendConnectTimeoutWiring::test_without_parameter_and_configuration_the_default_applies
  - tests/unit/backend/test_valkey_handshake_retry.py::test_refused_tcp_is_retried_five_times_without_outer_retry
  - tests/unit/backend/test_valkey_handshake_retry.py::test_handshake_resets_are_retried_until_the_window_is_spent
  - tests/unit/backend/test_valkey_handshake_retry.py::test_handshake_resets_then_refused_tcp_stay_below_twice_the_window
---

**Entscheidung.** `ValkeyBackend.connect()` führt den `PING` beim Verbinden unter einer Gesamtfrist aus (Größenordnung 15–30 s, konfigurierbar; Name und Default legt der Fix-Plan fest). Reißt die Frist, wirft `connect()` `BackendConnectionError` (und schließt den eigenen Client, F4-13). Die Wiederholpolitik zur Laufzeit bleibt unverändert (`Retry(ExponentialBackoff(cap=2, base=0.05), retries=5)`). Ob Verbindungs- und Befehls-Wiederholung getrennt werden (PF-16 b), ist nicht entschieden: Prüfauftrag im Fix-Plan mit Toxiproxy-Test (`timeout`-Toxic), dabei F3-33 mitbetrachten.

**Begründung.** valkey-py kopiert die Retry-Politik des Clients in jede Verbindung und wiederholt dort den Verbindungsaufbau, während der Client den Befehl wiederholt: 31 Verbindungsversuche, mit 5 s Connect-Timeout rund 2,9 min bis zur Ausnahme. Unter K8s bestimmt die Startdauer Readiness und CrashLoop-Takt; 111983a wollte eine Grenze, keine Dauer von Minuten.

**Bewusste Folgen.** Ein Worker, dessen Valkey beim Start länger als die Frist nicht antwortet, endet mit `BackendConnectionError` und wird neu gestartet, statt zu warten. Zur Laufzeit kann ein Befehl ohne `op_timeout` bei einer Netzpartition weiter so lange blockieren wie heute.

**Nachtrag 2026-10-17 (U4).** Vorrang der Gesamtfrist: ein ausdrücklich übergebener Konstruktor-Parameter `connect_timeout_s` vor dem `SwarmQConfig`-Feld `backend_connect_timeout_s` bzw. `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` vor der Vorgabe von 30 s (`SwarmQ` gleicht über `reconcile_connect_timeout` ab). Der Handshake einer neu aufgebauten Pool-Verbindung sendet keinen Health-Check-PING, weil der Treiber sonst TCP und Handshake unter seiner Retry-Politik rekursiv neu aufbaut (gemessen 68 s statt 3,1 s bis zur Ausnahme).

**Nachtrag 2026-10-19 (Georg, Entscheidungsvorlage 0.13.0, Punkte 3a und 7): Die Gesamtfrist ist eine Obergrenze, keine Wartezeit.** Mit dem Stand nach U4 macht der Treiber beim Verbinden höchstens 6 TCP-Versuche (`retries=5`) zu je `socket_connect_timeout` 5 s mit 3,1 s Backoff dazwischen (`backend/valkey/connection.py`, `_RETRY_ATTEMPTS`, `_SOCKET_CONNECT_TIMEOUT_S`, `ExponentialBackoff(cap=2, base=0.05)`). Gegen einen Host, der Pakete verwirft, endet `connect()` darum nach höchstens rund 33 s mit `Valkey connect failed: …`, auch wenn die Frist größer ist (Spike U21: Frist 600 s → 33,1 s); gegen einen Port, der abweist, nach rund 3,1 s. Ein Wert über rund 33 s verlängert nichts, ein kleinerer lässt den Start früher scheitern. Das ist eine bewusste Folge, kein Fehler; die Doku (`configuration.md`, `api.md`, Changelog 0.12.4) sagt es seit 0.13.0rc2. PF-16 (b): jetzt nichts umsetzen (Option a); die Variante „Zeitabläufe beim TCP-Aufbau nicht wiederholen“ kommt in 0.14 als Option (c), mit einer Schleife bis zur Frist in `connect()` (der Start wartet dann wirklich bis zur Frist, zur Laufzeit gilt die kürzere Zahl), einem weiteren Nachtrag und Toxiproxy-Test.

**Überdenken nur mit neuem Argument.** Offen: PF-16 (b) (Trennung der Wiederholungen, betrifft auch die Laufzeit). Spike 2026-10-18 (U21, `docs/solutions/spikes/2026-10-spike-verbindungs-retry.md`): Die Trennung mit 0–1 Wiederholungen ist so nicht haltbar, zwei Tests dieser Regel würden rot und ein Valkey-Neustart von 2 s würde zur Laufzeit nicht mehr überbrückt. Empfohlen ist die Variante „Zeitabläufe beim TCP-Aufbau nicht wiederholen“ (Laufzeit-Partition 178,8 s → 38,1 s, alle Tests grün, `connect()` gegen einen Host, der Pakete verwirft, endet aber nach 5 s statt nach der Gesamtfrist); umgesetzt wird sie erst nach Georgs Ok mit einem Nachtrag. Entschieden am 2026-10-19: in 0.14 als Variante (c), siehe Nachtrag 2026-10-19.
