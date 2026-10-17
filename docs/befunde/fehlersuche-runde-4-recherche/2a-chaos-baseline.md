# Fehlersuche Runde 4 – Spur 2a: Chaos-Baseline lokal (U10)

Stand 2026-10-17, Worktree `test/fehlersuche-runde-4`, Arbeitsstand unter `src/swarmq/` = 0.12.3 (unverändert).
Alle Läufe unter der Testsperre (`scripts/run-tests.sh` → `scripts/with-test-lock.sh`), jeweils mit
`systemd-inhibit --what=sleep` gestartet. Rohlogs: `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad/u10/`
(`lauf1.log`, `lauf2.log`, `lauf3-seed1.log`, `repro-datei-{1,2,3}.log`, `repro-einzel-{1,2,3}.log`, `disk.log`).

## Ergebnis in Kürze

| Durchgang | Seed | Ergebnis | Laufzeit (pytest / gesamt) | Load (1 min) Start → Ende |
|---|---|---|---|---|
| Lauf 1 | 4248374682 | 198 passed, 4 skipped, 6 xfailed, **1 error** (Teardown) | 502 s / 504 s | 1,30 → 1,80 |
| Lauf 2 | 1017202604 | 198 passed, 4 skipped, 6 xfailed | 491 s / 493 s | 2,25 → 0,72 |
| Lauf 3 (Wiederholung Seed Lauf 1) | 4248374682 | 200 passed, 4 skipped, 3 xfailed | 499 s / 500 s | 1,98 → 1,78 |
| Disk-Tests (`SWARMQ_RUN_DISK_TESTS=1`) | 4248374682 | 2 passed, 1 skipped | 1,9 s | 1,76 → 2,08 |

Der dokumentierte Stand „116 passed, 4 skipped“ ist in der **Skip-Zahl und den Skip-Gründen bestätigt**; die
Passed-Zahl ist mit der Suite gewachsen (siehe „Abweichungen zwischen Plan, Doku und Code“). Ein Teardown-Fehler
in Lauf 1 (ResourceWarning, Signatur von TS-02) ließ sich siebenmal nicht reproduzieren und ist als Flake
eingestuft. **Keine Verdachte R4I aus der Baseline.**

Maschine: 24 Kerne, 54 GB RAM; Load während aller Läufe zwischen 0,7 und 2,4, also keine nennenswerte
Fremdlast (U11 fuhr parallel den Loadsuite-Smoke `stoerung-dev-smoke-20261017T115452` mit eigenem Stack auf
5682/6390/6391; weitere Agenten warteten an der Testsperre, liefen also nicht gleichzeitig).

## Ausgangslage

- `uptime` vor Beginn (13:58): `up 5:34, load average: 0,70, 1,26, 1,87`.
- `docker ps` vor Beginn: `swarmq-test-rabbit` (Image `rabbitmq:4-management`, `RABBITMQ_VERSION=4.3.6`, 5699/15699),
  `swarmq-test-valkey` (6399), der Loadsuite-Stack von U11 (`loadsuite-*`, eigener Toxiproxy auf 8484),
  `gitlab-runner`, `portainer`, `arcane`. Kein `swarmq-test-toxiproxy`, kein `swarmq-chaos-*`.
- Testsperre `/run/user/1000/swarmq-itest.lock` frei.
- Kein Docker-Kontext `lastserver` benutzt.

## Aufruf und warum nicht nur `uv run just chaos`

`just chaos` ist `scripts/run-tests.sh -m chaos "$@"` (justfile Z. 67). Ohne weitere Argumente wählt das
**74 Tests** mit Marker `chaos` aus (44 in `tests/chaos`, 28 in `tests/e2e/combinations`, 2 in
`tests/fuzz/test_external_publish_fuzz.py`). Die Erwartung „116 passed, 4 skipped (120 Tests)“ in
`docs/testing/test-strategy.md` (Z. 147–157) gilt aber für das **volle Chaos-Gate**:

```bash
docker run -d --name swarmq-test-toxiproxy --network host \
  ghcr.io/shopify/toxiproxy:2.9.0 -host 127.0.0.1 -port 8474
uv run just chaos -m "chaos or stress or property or fuzz"
```

Ohne diesen Toxiproxy überspringen die Kombinationen in `tests/e2e/combinations` zwölf Tests, und das Gate gilt
laut Doku als nicht bestanden. Gefahren wurde deshalb das volle Gate (das letzte `-m` überschreibt das `-m chaos`
des Rezepts) mit dem Toxiproxy aus der Doku (Ports 8474, 5698, 6398; keine Kollision mit dem Loadsuite-Toxiproxy
auf 8484). Der Container wurde für U10 gestartet und danach wieder entfernt. Die 74 `chaos`-Tests sind darin
vollständig enthalten: In Lauf 2 und 3 waren alle 70 nicht übersprungenen grün, in Lauf 1 ebenso (der Fehler
liegt im Teardown, der Testkörper bestand).

Aufgerufen wurde jeweils `systemd-inhibit --what=sleep … uv run just chaos -m "chaos or stress or property or fuzz" -rfEs --durations=15`,
der zweite Lauf mit `SWARMQ_TEST_SEED=1017202604`.

## Abweichungen zwischen Plan, Doku und Code

1. **Testzahl.** Das Gate umfasst heute 208 Tests (Lauf 1/2), nicht 120. Zugewachsen sind die neuen
   Fuzz-Dateien aus KTD8 (`test_header_text_fuzz`, `test_counter_headers_fuzz`, `test_error_info_fuzz`,
   `test_config_env_fuzz`, `test_cron_fuzz`, `test_lock_template_fuzz`) und die zustandsbasierten Maschinen
   unter `tests/property/stateful/` (U8/U9), darunter Verdachtsnachweise als `xfail(strict=True)` (R4Z-…).
   Die xfails sind erwartete Nachweise dieser Runde, keine Chaos-Abweichung. Die Doku-Zahl 116 sollte beim
   Abschluss der Runde auf den neuen Stand gehoben werden.
2. **Arbeitsbaum während der Läufe nicht stabil.** Parallel arbeitende Agenten haben zwischen Lauf 2 und
   Lauf 3 die Dateien unter `tests/property/stateful/` geändert (Lauf 3: 207 statt 208 Tests, 3 statt 6 xfails;
   `git status` zeigt sie als geändert). `tests/chaos/`, `tests/e2e/combinations/` und
   `tests/fuzz/test_external_publish_fuzz.py` blieben unverändert. Lauf 3 ist deshalb keine exakte
   Wiederholung der Reihenfolge von Lauf 1 (pytest-randomly mischt über die gesammelte Menge).
3. **Die vier Skips sind nicht vier Disk-Tests.** Plan U10 Schritt 3 spricht von den Skips als Disk-Tests; im
   Code sind es drei Disk-Tests plus `test_clock_skew.py::test_worker_subprocess_clock_skew_libfaketime`
   (unbedingtes `@pytest.mark.skip`, libfaketime auf U3.x vertagt). Von den drei Disk-Tests ist einer
   (`test_rabbitmq_disk_alarm_publisher_blocks`, `test_disk_pressure.py:183`) zusätzlich ein unbedingtes
   `@pytest.mark.skip` (dokumentierter Stub); mit `SWARMQ_RUN_DISK_TESTS=1` laufen also nur zwei. Die Doku
   (`test-strategy.md` Z. 154–156) beschreibt das richtig.
4. **Zwei RabbitMQ-Versionen im Gate.** `tests/chaos` läuft gegen den eigenen Stack mit
   `rabbitmq:3.13.7-management-alpine` (`tests/chaos/compose.yml` Z. 9), ebenso der Chaos-Fuzz
   `tests/fuzz/test_external_publish_fuzz.py` (Fixture `chaos_stack`). Die Chaos-Kombinationen in
   `tests/e2e/combinations` laufen dagegen gegen den Teststack **4.3.6** auf 5699 über den Host-Toxiproxy. KTD6 („Die Suite läuft auf RabbitMQ 3.13.7“) gilt also nur für `tests/chaos` und den
   Chaos-Fuzz.

## Skips

| Test | Grund | Mit `SWARMQ_RUN_DISK_TESTS=1` |
|---|---|---|
| `tests/chaos/test_disk_pressure.py::…::test_valkey_oom_surfaces_as_backend_connection_error` (Z. 147) | Modul-`skipif` ohne Variable | **passed** |
| `tests/chaos/test_disk_pressure.py::…::test_recovery_after_freeing_memory` (Z. 201) | Modul-`skipif` ohne Variable | **passed** |
| `tests/chaos/test_disk_pressure.py::…::test_rabbitmq_disk_alarm_publisher_blocks` (Z. 183) | Modul-`skipif`; zusätzlich unbedingter Stub-Skip („requires custom RabbitMQ config“) | skipped (Stub) |
| `tests/chaos/test_clock_skew.py::test_worker_subprocess_clock_skew_libfaketime` (Z. 405) | unbedingter Skip, libfaketime vertagt | – |

Keine weiteren Skips: Kein Toxiproxy-Skip, kein „worker failed to come online“-Skip aus
`test_network_chaos._running_worker` in einem der drei vollen Läufe. Die Skip-Gründe entsprechen der Doku; der
Disk-Lauf bestätigt, dass die beiden lauffähigen Disk-Tests lokal ohne Last grün sind (1,9 s).

## Abweichung A-1: Teardown-Fehler in `test_network_chaos.py` (Lauf 1)

- **Test:** `tests/chaos/test_network_chaos.py::TestNetworkChaos::test_bandwidth_limit_on_rabbitmq`,
  **ERROR at teardown**; der Testkörper bestand. Er lief als letzter Test der Klasse, der Teardown umfasste also
  auch den Abbau des klassenweiten `chaos_stack` und den klassenweiten `_collect_garbage_after_class`
  (`tests/conftest.py`).
- **Seed:** 4248374682. Laufzeit des Tests (call) rund 0,5 s, der Klasse rund 43 s; Gesamtlauf 502 s.
- **Load:** 1,80 (1 min) am Laufende, 24 Kerne – keine Last.
- **Fehlertext (gekürzt):**
  ```
  ExceptionGroup: multiple unraisable exception warnings (3 sub-exceptions)
  1: ResourceWarning: loop is closed            (StreamWriter.__del__)
  2: ResourceWarning: unclosed <socket.socket fd=17, family=2, type=1, proto=6, laddr=('127.0.0.1', 55498)>
                                                 (ausgelöst in tests/conftest.py:59 gc.collect())
  3: ResourceWarning: unclosed transport <_SelectorSocketTransport fd=17>
  ```
- **Abgleich TS-01..03** (`docs/befunde/teststrenge-2026-10.md`): Die Signatur – StreamWriter einer
  geschlossenen Loop plus offener Transport und Socket – ist genau die von **TS-02** (Server oder Proxy schließt
  die Verbindung, „abgeschalteter Toxiproxy“, gescheiterter Wiederverbindungsversuch) und deckt sich Zeile für
  Zeile mit den Filtern von `aiormq_restart_leak` in `tests/support/known_leaks.py`. `test_network_chaos.py`
  steht aber **nicht** in der Fundstellenliste von TS-02 und trägt keinen Leck-Marker, obwohl
  `test_full_down_then_recover`, `test_partial_partition_worker_sees_broker_not_backend` und
  `test_pubsub_disconnect_resubscribe_via_watch_progress` Proxys abschalten bzw. kappen. Die autouse-Fixture
  `close_orphaned_streams_after_module` (TS-02) räumt verwaiste Writer erst am **Modulende** ab; der
  klassenweite GC-Lauf davor findet sie zuerst und lässt den letzten Test der Klasse scheitern. Welcher Dienst
  am Socket hing (AMQP oder Valkey, beide über Toxiproxy), lässt der Fehlertext nicht erkennen; der Valkey-Client
  nutzt ebenfalls StreamWriter. Die Zuordnung zu aiormq ist deshalb wahrscheinlich, nicht belegt.
- **Abgleich Entscheidungsregister:** Kein Eintrag zu ResourceWarnings oder Teardown-Lecks; nicht entschieden,
  aber auch kein Produktverhalten (der Test prüft Vollständigkeit unter 50 kbps, die bestand).
- **Reproduktion (alle grün):**
  - Datei dreimal mit demselben Seed (`tests/chaos/test_network_chaos.py`): 6 passed je 43,7 s.
  - Einzeltest dreimal: 1 passed je 8,9–9,1 s.
  - Voller Lauf mit demselben Seed (Lauf 3, Reihenfolge wegen geänderter Sammlung nicht identisch, Datei lief
    wieder als letzte): 6 passed.
  - 0 von 7 Wiederholungen, Load jeweils unter 2,4.
- **Einstufung: Flake, bekanntes Bibliotheksleck (TS-02-Klasse), nicht lastbedingt.** Kein Verdacht R4I. Hinweis
  für die Teststrenge (kein Produktfehler): TS-02 um `tests/chaos/test_network_chaos.py` ergänzen bzw. die
  Abräumung der verwaisten Writer vor den klassenweiten GC-Lauf ziehen; sonst bleibt das Gate gelegentlich rot,
  ohne dass swarmq etwas falsch macht. Entscheidung darüber gehört in die Gegenprüfung (U17), nicht in U10.

## Laufzeiten (Auswahl, Lauf 1)

Die längsten Einzeltests: `TestLongBackendOutageBeforeStartCombo::test_outage_longer_than_delivery_budget_defers_instead_of_xq`
41,7 s (bewusst, `OUTAGE_S = 40`), `test_oom_simulation_via_external_kill` 15,3 s,
`test_chords_under_latency_each_fire_body_once` 13,5 s, `test_segv_panic_redelivers_then_dlqs_after_max_retries`
12,0 s. Jeder Chaos-Stack braucht rund 6,5 s Setup. Die Unit-Tests über 100 ms unter den Fuzz- und
Property-Dateien (bis 1,8 s) sind Hypothesis-Läufe und werden nur gemeldet (AGENTS.md).

## RabbitMQ 3.13.7 gegen 4.x: welche Tests anderes Verhalten erwarten lassen

Der Chaos-Stack nutzt 3.13.7, die Produktion und alle übrigen Suiten 4.3.6 (`test-strategy.md`,
„RabbitMQ-Versionen“). Relevante Unterschiede: Auf 3.13 zählt jede Rückgabe (auch `nack(requeue=True)`) auf
`x-delivery-count` und damit auf `x-delivery-limit`; ab 4.0 zählen nur gescheiterte Zustellungen (Kanal- oder
Verbindungsabbruch), Requeues stehen nur in `x-acquired-count` (Docstring `test_failure_cascades.py` Z. 1676–1686,
E-046-Nachtrag, E-054). Ab 4.0 gilt für Quorum-Queues ohne Argument ein Standardlimit von 20
(`declarations.py` `_BROKER_DEFAULT_DELIVERY_LIMIT`, Spike S1 auf 4.2.9/4.3.6); swarmq setzt
`x-delivery-limit` aber ausdrücklich (`topology.py` `main_queue_arguments`), daher ist dieser Teil gleich.

| Test (nur `tests/chaos`, Stack 3.13.7) | Versionsbezug | Erwartung auf 4.x |
|---|---|---|
| `test_worker_crash.py::test_segv_panic_redelivers_then_dlqs_after_max_retries` | Endpunkt über `x-delivery-limit=20` **oder** Execution-Guard (`WorkerHungError`) nach `{queue}.XQ` | Absturz = Kanalabbruch zählt auch auf 4.x; meist greift ohnehin vorher der Guard (Docstring). Gleiches Ergebnis erwartet, Pfad kann sich verschieben. |
| `test_worker_crash.py::test_sigkill_inflight_redelivers_no_loss`, `test_50_inflight_sigkill_no_dlq_entries`, `test_oom_simulation_via_external_kill` | Neuzustellung nach Verbindungsabbruch; Zählerstand bleibt unter dem Limit | Gleich; auf 4.x steigt zusätzlich `x-acquired-count`. |
| `test_workflow_chord_idempotency.py::test_header_redelivery_fires_body_exactly_once` | Neuzustellung unbestätigter Gruppenglieder nach SIGKILL | Gleich erwartet. |
| `test_broker_restart.py::test_quorum_queue_survives_restart` und die DLX-Argumente | Quorum-Queue mit expliziten Argumenten über Neustart | Gleich; auf 4.x zusätzlich implizites Limit, hier nicht berührt. |
| `test_broker_restart.py` (übrige) | Wiederverbinden nach Neustart; Schließcode 320, TS-02 | Gleich erwartet; Schließverhalten bei 4.x (Single-Node, Khepri optional) nicht gemessen. |
| `test_resource_exhaustion.py::test_channel_limit_exhaustion_typed_error` | serverseitiges `channel_max` 2047, 2048. Kanal muss typisiert scheitern | Abhängig vom serverseitigen Standard von 4.x – **ungeprüft**, vor einem Wechsel auf 4.x nachmessen. |
| `test_message_corruption.py`, `test_external_publish_fuzz.py` (Chaos-Fuzz) | Fremde Header inkl. Zählerheader | Zählerheader werden auf 4.x vom Broker gesetzt bzw. überschrieben (`x-delivery-count` nur bei Neuzustellung, `x-acquired-count` neu); Annahme- und Ablehnungspfade (E-051, E-054) sind auf 4.x anders belegt. |

Ohne Versionsbezug: `test_backend_restart`, `test_valkey_eviction_policy`, `test_clock_skew`, `test_disk_pressure`
(Valkey-Teile), `test_publish_deadline` (Stillstand über Toxiproxy), `test_network_chaos`.

Umgekehrt: Der 3.13-Fall „Requeue verbraucht das Zustellbudget“, gegen den
`TestLongBackendOutageBeforeStartCombo` geschrieben wurde, wird im Gate **nicht** auf 3.13 geprüft, weil die
Kombinationen gegen 4.3.6 laufen. Der Test bestand auf 4.3.6 (41,7 s); auf 3.13 ist er nie gelaufen.

## Gilt die Baseline für 4.x? (PF-1)

Nein, nur eingeschränkt. PF-1 (`docs/befunde/fehlersuche-runde-4-2026-10.md`, „Chaos-Stack auf 4.3.6 heben?“,
offen bei Georg) trifft zu: Die Baseline belegt das Verhalten von `tests/chaos` **auf 3.13.7**. Für 4.x gelten
nur die Kombinationen aus `tests/e2e/combinations` (28 Tests, alle grün) als Baseline. Für die Crash- und
Neuzustellungs-Tests erwarte ich auf 4.x dasselbe Ergebnis, weil swarmq das Limit explizit setzt und Abstürze auf
beiden Versionen zählen; belegt ist das nicht. Offen und vor einem Wechsel zu messen sind
`test_channel_limit_exhaustion_typed_error` (Server-`channel_max`) und die Zählerheader im Chaos-Fuzz. Die
Empfehlung aus PF-1 (Stack in eigener Änderung nach Runde 4 heben) bleibt richtig; dabei die Tabelle oben als
Prüfliste nehmen.

## Verdachtsliste R4I (Baseline-Teil)

Keine. Die einzige Abweichung (A-1) ist ein nicht reproduzierbares, bekanntes Bibliotheksleck der TS-02-Klasse
und als Flake eingestuft; sie geht als Hinweis zur Teststrenge in die Gegenprüfung.

## Offene Punkte

- Doku-Erwartung in `docs/testing/test-strategy.md` (116/4, 120 Tests) auf den neuen Umfang heben, sobald die
  Runde die Property-/Fuzz-Dateien festgeschrieben hat.
- TS-02 um `tests/chaos/test_network_chaos.py` ergänzen oder die Writer-Abräumung vor den klassenweiten GC-Lauf
  legen (Teststrenge, nicht Produkt).
- PF-1 offen; Messung von `channel_max` und Zählerheadern auf 4.x vor einem Wechsel.
- Der Rezeptname `just chaos` deckt allein nicht das dokumentierte Gate; der Toxiproxy-Container ist eine
  manuelle Vorbedingung. Für U13/U17 gilt: „Baseline“ heißt hier das volle Gate mit Toxiproxy.
