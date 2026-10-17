# Sichtung der Überlebenden, Gruppe F (Prozessmanager und vier CLI-Module)

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R17, KTD9, Einheit U5, Open Question „CLI-Begrenzung“. Stand: swarmq 0.12.3, Branch `test/fehlersuche-runde-4`. Format nach `docs/befunde/fehlersuche-runde-2-recherche/4-mutation-B.md`.

## Kopf

| Punkt | Wert |
|---|---|
| Eingabe | `<scratchpad>/u3/mut-gruppe-F.txt` (Lauf A, nach Vorfilter R17), Kennzahlen aus `3a-mutation-lauf-a.md`; `mut-gruppe-F-gefiltert.txt` nur zum Abgleich |
| Begrenzung | `src/swarmq/process_manager/` vollständig; aus `src/swarmq/cli/` nur `parser_ops.py`, `dlq_cmd.py`, `schedule_cmd.py`, `check_cmd.py` (Open Question: 1703 Überlebende > 1500). Die übrigen CLI-Module sind **nicht** gesichtet (Abschnitt „Nach Welle 3 verschoben“) |
| Gesichtet | **896 Mutanten**: 866 überlebt, 30 ohne Test (alle `schedule_cmd._render_add_result`). Prozessmanager 323, CLI 573 (`schedule_cmd` 302, `dlq_cmd` 140, `check_cmd` 111, `parser_ops` 20) |
| Einordnung | **Testlücke 500**, **Rauschen 373**, **Messartefakt 23**; daraus **3 Verdachte** auf echte Fehler (R4M-F-01 bis -03), alle mit rotem Probe-Test gegen den unmutierten Code |
| Methode | Mutanten je Funktion gebündelt (die 215 Ausgabe-String-Mutanten der CLI je Funktion), Originalcode im Kontext gelesen, gegen Register `compound-packs/swarmq-entscheidungen/`, `docs/befunde/fehlersuche-runde-3-2026-10.md` und `website/reference/cli.md` geprüft. Testlücken mit Vorrang durch Probe-Tests belegt, die gegen den unmutierten Code grün und gegen einen **nachgebildeten** Mutanten rot sind (Plugin `mut_env.py`, ersetzt die Funktion im Namensraum ihres Moduls; kein mutmut-Lauf auf dem Laptop) |
| Proben | `<scratchpad>/u5f/proben/` (vier Dateien, 20 Tests: 4 rot für die Verdachte, 16 grün für die Lücken), Plugins `<scratchpad>/u5f/plug/`, Einordnung je Mutant `<scratchpad>/u5f/einordnung.py` und `einordnung.json`. Nur Unit-Ebene ohne Dienste, keine echten Worker-Prozesse, kein Docker. Nichts unter `src/swarmq` oder `tests/` geändert |

`<scratchpad>` = `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad`. Proben laufen mit `PYTHONPATH=. uv run --extra test pytest <datei> -p no:randomly -n0 --rootdir=. -c pyproject.toml` im Worktree; ein Mutant wird mit `SWARMQ_PROBE_MUT='modul|funktion|klasse|alt|neu|n'` und `PYTHONPATH=.:<scratchpad>/u5f/plug … -p mut_env` nachgebildet.

Einordnung:

- **Echter Fehler (Verdacht):** Das Verhalten des **unmutierten** Codes verletzt eine Zusage; Beleg ist ein roter Probe-Test.
- **Testlücke:** Der Code ist richtig, aber kein Unit-Test hält das Verhalten fest; der Mutant würde eine Regression durchlassen.
- **Rauschen:** äquivalenter Mutant (`None` statt `False`, `float("-INF")`, ungenutzter Parameter `recycling`), Konstante oder Frist ohne Vertrag, Grenzwert ohne fachliche Folge, Text- oder Tabellenausgabe für Menschen, Fehlertext.
- **Messartefakt:** Der Status kommt aus der mutmut-Umgebung, nicht aus dem Testinhalt (Abschnitt „Messbefunde“).

## Tabelle je gesichteter Funktion

Zahlen = Mutanten nach Vorfilter. Verdacht-ID nur, wo die Sichtung einen echten Fehler im Umfeld der Funktion gefunden hat; die Mutanten selbst sind dort Testlücken.

| Modul | Funktion | Mutanten | Einordnung | Verdacht-ID |
|---|---|---|---|---|
| `process_manager/restart_budget.py` | `_heartbeat_deadline` | 1 | Testlücke 1 | – |
| `process_manager/restart_budget.py` | `_check_heartbeats` | 3 | Testlücke 2, Rauschen 1 | – |
| `process_manager/restart_budget.py` | `_replace_hung_worker` | 2 | Testlücke 1, Rauschen 1 | – |
| `process_manager/restart_budget.py` | `_escalate_terminating` | 3 | Testlücke 2, Rauschen 1 | – |
| `process_manager/restart_budget.py` | `_maybe_restart` | 1 | Rauschen 1 | – |
| `process_manager/setpoint.py` | `validate` | 1 | Testlücke 1 | – |
| `process_manager/setpoint.py` | `read_at_start` | 2 | Testlücke 2 | – |
| `process_manager/setpoint.py` | `apply` | 14 | Testlücke 14 | – |
| `process_manager/setpoint.py` | `tick` | 4 | Testlücke 3, Rauschen 1 | – |
| `process_manager/setpoint.py` | `_drain_one` | 3 | Testlücke 3 | – |
| `process_manager/setpoint.py` | `reconcile` | 8 | Testlücke 6, Rauschen 2 | – |
| `process_manager/setpoint.py` | `status_ttl_s` | 1 | Testlücke 1 | – |
| `process_manager/setpoint.py` | `build_status` | 8 | Testlücke 8 | – |
| `process_manager/setpoint_store.py` | `read_setpoint_sync` | 3 | Testlücke 1, Rauschen 2 | – |
| `process_manager/setpoint_store.py` | `write_status_sync` | 2 | Rauschen 2 | – |
| `process_manager/setpoint_store.py` | `_close_quietly` | 1 | Rauschen 1 | – |
| `process_manager/autoscale.py` | `tick` | 10 | Testlücke 5, Rauschen 5 | – |
| `process_manager/autoscale.py` | `scale_down_one` | 1 | Rauschen 1 | – |
| `process_manager/supervisor.py` | `__init__` | 8 | Testlücke 2, Rauschen 5, Messartefakt 1 | – |
| `process_manager/supervisor.py` | `_wait_for_exit` | 1 | Rauschen 1 | – |
| `process_manager/supervisor.py` | `_signal_survivors` | 8 | Testlücke 1, Rauschen 7 | – |
| `process_manager/supervisor.py` | `_shutdown_all_workers` | 8 | Testlücke 3, Rauschen 5 | – |
| `process_manager/supervisor.py` | `_collect_zombies` | 1 | Rauschen 1 | – |
| `process_manager/supervisor.py` | `_monitor_loop` | 3 | Testlücke 1, Rauschen 2 | – |
| `process_manager/supervisor.py` | `_current_target` | 1 | Rauschen 1 | – |
| `process_manager/supervisor.py` | `_check_dead_workers` | 11 | Rauschen 11 | – |
| `process_manager/reload.py` | `__init__` | 3 | Rauschen 3 | – |
| `process_manager/reload.py` | `abort` | 1 | Rauschen 1 | – |
| `process_manager/reload.py` | `step` | 1 | Rauschen 1 | – |
| `process_manager/reload.py` | `_poll` | 4 | Rauschen 4 | – |
| `process_manager/reload.py` | `reexec` | 2 | Testlücke 1, Rauschen 1 | – |
| `process_manager/reload.py` | `_reap_killed` | 1 | Rauschen 1 | – |
| `process_manager/reload.py` | `_restore_signal_mask` | 1 | Messartefakt 1 | – |
| `process_manager/entry.py` | `_logging_settings` | 3 | Testlücke 3 | – |
| `process_manager/entry.py` | `done` | 1 | Rauschen 1 | – |
| `process_manager/entry.py` | `_worker_namespace` | 6 | Testlücke 1, Rauschen 5 | – |
| `process_manager/entry.py` | `worker_subprocess_entry` | 11 | Testlücke 5, Rauschen 6 | – |
| `process_manager/worker_target.py` | `_bind_to_supervisor` | 25 | Testlücke 3, Rauschen 1, Messartefakt 21 | – |
| `process_manager/run.py` | `run_multi_process_worker` | 10 | Testlücke 6, Rauschen 4 | – |
| `process_manager/run.py` | `_run_multi_process_worker_configured` | 103 | Testlücke 68, Rauschen 35 | – |
| `process_manager/check.py` | `start_config` | 2 | Rauschen 2 | – |
| `process_manager/check.py` | `app_dir_hint` | 2 | Rauschen 2 | – |
| `process_manager/check.py` | `_effective_port` | 3 | Rauschen 3 | – |
| `process_manager/check.py` | `_validate_observability_ports` | 16 | Testlücke 7, Rauschen 9 | – |
| `process_manager/check.py` | `_validate_headroom` | 1 | Testlücke 1 | – |
| `process_manager/check.py` | `_validate_configure_spec` | 3 | Testlücke 2, Rauschen 1 | – |
| `process_manager/check.py` | `check_worker_config` | 11 | Testlücke 6, Rauschen 5 | – |
| `process_manager/sampler.py` | `_make_queue_depth_sampler` | 2 | Rauschen 2 | – |
| `process_manager/slots.py` | `lock` | 1 | Rauschen 1 | – |
| `process_manager/config.py` | `__post_init__` | 1 | Rauschen 1 | – |
| `cli/check_cmd.py` | `_seed_check_payload` | 20 | Testlücke 20 | – |
| `cli/check_cmd.py` | `_empty_check_payload` | 2 | Testlücke 2 | – |
| `cli/check_cmd.py` | `_check_task_injection` | 1 | Testlücke 1 | – |
| `cli/check_cmd.py` | `_run_worker_check` | 37 | Testlücke 19, Rauschen 18 | – |
| `cli/check_cmd.py` | `_is_check_invocation` | 51 | Testlücke 1, Rauschen 50 | – |
| `cli/dlq_cmd.py` | `_format_dlq_list_table` | 30 | Testlücke 3, Rauschen 27 | – |
| `cli/dlq_cmd.py` | `_normalise_headers_for_json` | 5 | Testlücke 3, Rauschen 2 | – |
| `cli/dlq_cmd.py` | `_run_dlq_list` | 19 | Testlücke 13, Rauschen 6 | R4M-F-03 |
| `cli/dlq_cmd.py` | `_run_dlq_inspect` | 52 | Testlücke 28, Rauschen 24 | R4M-F-03 |
| `cli/dlq_cmd.py` | `_run_dlq_retry` | 32 | Testlücke 24, Rauschen 8 | R4M-F-03 |
| `cli/dlq_cmd.py` | `_run_dlq_purge` | 2 | Testlücke 2 | – |
| `cli/schedule_cmd.py` | `_format_schedule_table` | 45 | Testlücke 4, Rauschen 41 | – |
| `cli/schedule_cmd.py` | `_run_schedule_list` | 5 | Rauschen 5 | – |
| `cli/schedule_cmd.py` | `_parse_payload` | 3 | Testlücke 2, Rauschen 1 | – |
| `cli/schedule_cmd.py` | `_resolve_fire_time` | 11 | Testlücke 8, Rauschen 3 | – |
| `cli/schedule_cmd.py` | `_render_add_result` | 30 (ohne Test) | Testlücke 30 | R4M-F-02 |
| `cli/schedule_cmd.py` | `_run_schedule_add` | 113 | Testlücke 113 | R4M-F-01 |
| `cli/schedule_cmd.py` | `_run_schedule_remove` | 30 | Testlücke 16, Rauschen 14 | – |
| `cli/schedule_cmd.py` | `_run_schedule_status` | 56 | Testlücke 36, Rauschen 20 | – |
| `cli/schedule_cmd.py` | `_run_schedule_async` | 9 | Testlücke 9 | – |
| `cli/parser_ops.py` | `_add_schedule_parser` | 9 | Testlücke 3, Rauschen 6 | – |
| `cli/parser_ops.py` | `_add_dlq_parser` | 8 | Testlücke 3, Rauschen 5 | – |
| `cli/parser_ops.py` | `_add_inspect_parser` | 2 | Rauschen 2 | – |
| `cli/parser_ops.py` | `_add_check_parser` | 1 | Rauschen 1 | – |

## Verdachtsliste

### R4M-F-01 (P3): `schedule add --unique-for` auf einen schon belegten Namen lässt den Dedup-Claim stehen; der Hinweis der CLI führt danach in eine Sperre durch den eigenen Claim

- **Fundstelle:** `src/swarmq/cli/schedule_cmd.py:215-260` (`_run_schedule_add`). Der Claim wird vor dem Speichern gesetzt (Owner = Schedule-Name, TTL `unique_for` + Zeit bis zum ersten Fire). Freigegeben wird er nur, wenn `store.add_schedule` **wirft** (`except BaseException`, Kommentar „Rollback the dedup claim on store failure (P1 #1)“). Liefert `add_schedule` `False` (Name existiert), bleibt der Claim stehen.
- **Mutanten:** `_run_schedule_add__mutmut_93–105` (Claim-Auswertung und Rollback) und alle übrigen 113 Mutanten der Funktion überleben; kein Unit-Test fährt `schedule add` mit `--unique-for` (nur `tests/integration/test_cli_schedule.py::test_add_unique_twice_reports_the_second_as_duplicate`).
- **Szenario:**
  1. Es gibt den Schedule `nightly` (ohne `--unique-for` oder mit anderen Argumenten).
  2. `swarmq schedule add nightly --task report --args '[1]' --cron '0 3 * * *' --unique-for 3600` beansprucht das Flag für `report([1])` mit Owner `nightly` und meldet „schedule 'nightly' already exists; use 'schedule remove --permanent' first to replace it“.
  3. Der Betreiber folgt dem Hinweis: `schedule remove nightly --permanent`, dann derselbe `add`. Jetzt meldet `claim_unique` den eigenen verwaisten Claim, und die CLI lehnt ab: „rejected — dedup flag already held by 'nightly'“. Der Selbsttreffer gilt bei `schedule add` bewusst als Duplikat (Integrationstest oben, FS-06b). Die Sperre hält `unique_for` plus die Zeit bis 03:00.
  4. Solange der Claim lebt, gilt jeder Producer-Aufruf `report` mit denselben Argumenten und `unique_for` als Duplikat von „nightly“ (gemeinsamer Schlüssel aus `compute_dedup_hash`); `schedule()` gibt dann „nightly“ als `task_id` zurück (E-066-Pfad), ohne dass es diesen Task gibt.
- **Beleg:** `<scratchpad>/u5f/proben/test_probe_schedule_add_orphan_claim.py`, In-Memory-Backend aus `tests/support/inmemory/`:
  - `test_rejected_add_leaves_no_dedup_claim`: rot, `AssertionError: verwaister Dedup-Claim von 'nightly' bleibt fuer unique_for + Fire-Offset`.
  - `test_following_the_hint_lets_the_add_succeed`: rot, Ausgabe `{'inserted': False, 'name': 'nightly', 'already_exists': True, 'dedup_hash': 'bc38…', 'duplicate_of': 'nightly'}`.
- **Quelle der Erwartung:** `website/reference/cli.md:320-324` („claims a dedup flag … A second `schedule add` with the same task, args and kwargs **while the flag is held** is rejected as a duplicate“: das Flag gehört zu einem gespeicherten Schedule); der Hinweistext der CLI selbst (`schedule_cmd.py:196`, „use 'schedule remove --permanent' first to replace it“); das Rollback-Prinzip an derselben Stelle (P1 #1) und im Client (`client/schedule.py`, Kommentar zu „hours of orphan window blocking re-schedule of the same args“).
- **Abgleich Register:** E-058 legt nur die Meldung „already exists“ bei gleichem Namen fest, nicht den Claim. E-011 (Nachtrag FS-06b) und der Integrationstest legen fest, dass ein Selbsttreffer bei `schedule add` ein Duplikat ist; das erklärt Schritt 3, deckt aber nicht, dass der Claim ohne gespeicherten Schedule stehen bleibt. E-056 und E-066 berühren Wertebereich und Producer-Pfad, nicht diesen Fall. Kein Test mit `entscheidung`-Marker widerspricht.
- **Gegenargument für den Gegenprüfer:** Geht die Antwort auf das `HSETNX` verloren (F3-01), wiederholt valkey-py den Befehl, `add_schedule` liefert `False`, obwohl der eigene Schedule gespeichert ist (E-058: CLI meldet weiter „already exists“). Ein pauschales Freigeben bei `False` nähme diesem Schedule seinen Schutz. Eine Fix-Richtung muss das unterscheiden, etwa: freigeben nur, wenn der gespeicherte Eintrag nicht den eigenen `dedup_hash` trägt.
- **Konfidenz:** hoch für den Mechanismus (Code und Probe), mittel für die Häufigkeit (Betreiber legt einen Namen zweimal an, mit `--unique-for`).
- **Einordnungsvorschlag:** FEHLER (P3), alternativ LÜCKE, wenn der Gegenprüfer die Zusage in `cli.md` nicht als Zusage über den Claim liest.

### R4M-F-02 (P4): Ein Dedup-Duplikat meldet in JSON `already_exists: true`, obwohl unter dem Namen nichts existiert

- **Fundstelle:** `src/swarmq/cli/schedule_cmd.py:175-184` (`_render_add_result`): `"already_exists": not inserted`, auch wenn `inserted` wegen `duplicate_of` falsch ist.
- **Mutanten:** alle 30 Mutanten der Funktion „ohne Test“ (kein Unit-Test ruft sie auf), u. a. `_render_add_result__mutmut_11` (`already_exists: inserted`) und `_19/_20` (`duplicate_of`).
- **Szenario:** `schedule add first --args '[1]' --unique-for 60`, dann `schedule add second --args '[1]' --unique-for 60 --format json`. Ausgabe: `inserted: false`, `already_exists: true`, `duplicate_of: "first"`. Ein Agent, der `already_exists` auswertet, hält `second` für angelegt und nimmt die Zeile für einen Namenskonflikt.
- **Beleg:** `<scratchpad>/u5f/proben/test_probe_schedule_add_orphan_claim.py::test_dedup_rejection_does_not_claim_the_name_exists`: rot, `{'inserted': False, 'name': 'second', 'already_exists': True, …}`.
- **Quelle der Erwartung:** schwach. `website/reference/cli.md:324` nennt für das Duplikat nur `inserted: false` und `duplicate_of`; `already_exists` ist nicht dokumentiert, der Feldname sagt aber etwas anderes, und der Tabellenzweig derselben Funktion unterscheidet beide Fälle (`schedule_cmd.py:188-197`).
- **Abgleich Register:** keine Entscheidung zu den JSON-Feldern von `schedule add`; E-058 betrifft nur den Namenskonflikt.
- **Konfidenz:** hoch für das Verhalten, niedrig für die Wirkung.
- **Einordnungsvorschlag:** LÜCKE (Produktfrage: `already_exists` im Duplikatfall `false` oder Feld dokumentieren).

### R4M-F-03 (P3): `dlq list`, `dlq inspect` und `dlq retry` melden „nicht gekürzt“ bzw. „nicht gefunden“, obwohl das Lesen am Limit stoppte, wenn abgelaufene Einträge Lesungen verbraucht haben

- **Fundstelle:** Anzeige in `src/swarmq/cli/dlq_cmd.py:109`, `:115`, `:130`, `:206`, `:234`, `:242` (`truncated`/`scan_exhausted` = `scanned >= limit`). Ursache in `src/swarmq/broker/rabbitmq/admin.py` (Gruppe E): `_drain_dlq_messages` liest `for _ in range(limit)`, bestätigt abgelaufene Einträge ohne Kopie (`continue`, E-028), zählt sie aber nicht in `entries`; `scanned = len(entries)`.
- **Mutanten:** `_run_dlq_list__mutmut_27`, `_run_dlq_inspect__mutmut_11`, `_run_dlq_retry__mutmut_38` (`>=` → `>`) überleben; die Grenze `scanned == limit` prüft kein Test, und den Fall „Limit durch abgelaufene Einträge verbraucht“ auch nicht.
- **Szenario:** `dead_letter_ttl_s` ist gesetzt, die XQ hält einen abgelaufenen und weitere lebende Einträge. `dlq list --limit 2` liest zwei Nachrichten, eine davon abgelaufen: `scanned: 1`, `truncated: false`, obwohl lebende Einträge übrig sind. Schwerer bei `dlq inspect <id>` bzw. `dlq retry <id>`: Liegt der gesuchte Eintrag hinter dem Limit und hat ein abgelaufener Eintrag eine Lesung verbraucht, meldet die CLI „not found … (scanned N messages)“ ohne den Hinweis „scan hit --scan-limit“, und JSON `scan_exhausted: false`: der Betreiber hält den Task für nicht in der DLQ. Der Fall verschwindet nach dem ersten Listing, weil die abgelaufenen Einträge dann bestätigt sind.
- **Beleg:** `<scratchpad>/u5f/proben/test_probe_dlq_truncated_expired.py` (Fakes aus `tests/unit/broker/test_dlq_read.py`): rot, `{'entries': [...], 'scanned': 1, 'limit': 2, 'truncated': False, …}` bei `queue.get.await_count == 2`.
- **Quelle der Erwartung:** Docstring `admin.py` `list_dead_letters`: „`scanned_count` — the number of messages actually read (… so callers can tell ‚DLQ had fewer than limit‘ from ‚stopped at limit‘)“; Docstring `inspect_dead_letter`: „lets the caller distinguish ‚genuinely not in the DLQ‘ from ‚scan stopped at limit before the match‘“; Kommentar `dlq_cmd.py:103-104` (R3.10 P3 #18, Agenten erkennen Kürzung am Feld `truncated`).
- **Abgleich Register:** E-028 entscheidet, dass abgelaufene Einträge beim Listen ohne Kopie bestätigt werden, nicht, wie sie gezählt werden. E-031 (DLQ-Fehlerergebnis ohne TTL-Ausnahme) berührt den Fall nicht.
- **Konfidenz:** hoch für den Mechanismus, mittel für die Häufigkeit (nur mit `dead_letter_ttl_s` und gealterten Einträgen, nur beim ersten Lesen danach).
- **Einordnungsvorschlag:** FEHLER (P3) gegen die zitierten Docstrings. Fundstelle liegt in Gruppe E (`admin.py`); der Bericht `3c-sichtung-E.md` sollte darauf verweisen, damit der Befund nicht doppelt geführt wird.

## Messbefunde und Testhygiene (keine Produktfehler)

### Die „segfault“- und Signal-Status im Prozessmanager sind Artefakte der mutmut-Isolation

- **SIGTERM endet im Forkserver-Enkel mit Exit 0.** mutmut 3.8 installiert im Forkserver `signal.signal(SIGTERM, terminate)` mit `os._exit(0)` (`mutmut/workers/isolation.py:531-542`); der Enkel, der die Tests eines Mutanten ausführt, erbt den Handler. Löst ein Mutant ein SIGTERM an den eigenen Prozess aus, endet der Lauf mit 0 und gilt als **„survived“**.
  - Beleg 1: `supervisor.xǁProcessManagerǁ__init____mutmut_68` (`_reexec_holds_signals = True`) steht als „survived“ in Lauf A. Lokal nachgebildet (`<scratchpad>/u5f/plug/mut_init68.py`) beendet `test_sighup_after_sigterm_is_ignored` den pytest-Prozess per SIGTERM (Exit 143): Der Handler ruft `signal.raise_signal`. Der Mutant ist also erkannt.
  - Beleg 2: `reload._restore_signal_mask__mutmut_1` („survived“) fällt lokal in `test_without_execvpe_the_mask_is_restored_and_a_held_sigterm_delivered` und `test_a_failed_execvpe_restores_the_mask` (`mut_rsm1.py`, 2 rot).
  - Folge: Überlebende im Signal- und Reload-Pfad sind nur nach Nachbildung ohne mutmut belastbar. Alle anderen 1:1 nachgebildeten Mutanten dieses Berichts verhielten sich lokal wie in Lauf A (überlebten die Bestandssuite).
- **SIGKILL bei äquivalenten Mutanten.** `__init____mutmut_65` und `_67` setzen `False` → `None` (äquivalent) und stehen dennoch als „segfault“ (Exit -9). Das stützt die Lesart aus `3a-mutation-lauf-a.md`: harte CPU-Grenze oder OOM der Messumgebung, nicht der Mutant.
- **SIGSEGV bei `supervisor._handle_exit_code__mutmut_1`** (`== 78` → `!= 78`). Nachgebildet (`mut_hec1.py`) gegen die ganze Unit-Suite (`tests/unit`, 5056 Tests): 14 rote Tests (u. a. `test_ordinary_crash_still_restarts`, `test_crash_storm_bounded_and_manager_exits_real_processes`, `test_locked_slot_is_refilled_after_the_restart_window`), kein Absturz, kein Hänger. Der Mutant ist von der Bestandssuite klar erkannt; die Ursache des SIGSEGV liegt in der Umgebung des Forkserver-Enkels (geforkt aus einem Prozess mit Timer-Thread, der Crash-Storm-Test startet echte `spawn`-Kinder). Ohne mutmut-Lauf nicht weiter eingrenzbar (Regel 6). Konfidenz mittel.

### `worker_target.py` (34 %): Abbildung, keine Lücke im Test

- 21 der 25 Überlebenden ändern Argumente von `ctypes.CDLL`/`prctl` oder die Waisen-Bedingung. Der einzige Test, der das Ergebnis prüft (`test_supervisor.py::TestProcessManagerCore::test_child_target_arms_the_parent_death_signal`), ruft `run_worker_entry` in einem `spawn`-Kind auf. mutmut ordnet Tests über Trampolin-Treffer im Testprozess zu; Treffer im Kind sieht es nicht, also lief für diese Mutanten nur `test_start_worker_unblocks_inherited_signals_before_the_entry`, das `prctl` in-process aufruft, das Ergebnis aber nicht prüft. Eingeordnet als Messartefakt.
- Echte Lücke: der Waisenfall (`_25`, `_29`, `_30`: Elternprozess zwischen `fork` und `prctl` gestorben → `SystemExit(0)`) hat keinen Test.
- **Nebenwirkung im Testprozess:** `test_start_worker_unblocks_inherited_signals_before_the_entry` setzt `PR_SET_PDEATHSIG = SIGTERM` auf dem pytest-Prozess selbst (Skript: vor dem Aufruf `0`, danach `15`). Unter xdist bekommt der Worker damit SIGTERM, wenn der Thread endet, der ihn gestartet hat. Bisher folgenlos; Vorschlag: `_bind_to_supervisor` in diesem Test patchen.

### Echter `execvpe` aus einem Unit-Test (aus `3a-mutation-lauf-a.md`)

`reload.x_reexec__mutmut_28` ersetzt den Testprozess, weil `tests/unit/process_manager/test_reload.py:310` `os.execvpe` nicht patcht. Bestätigt durch Lesen des Tests (`_pm()` mit `_reload_requested=True`, kein Patch). Vorschlag: `patch("swarmq.process_manager.reload.os.execvpe", side_effect=AssertionError)` in diesem Test.

## Testlücken

Je Lücke: Fundstelle, Mutanten, was ungeprüft ist, Bezug. „Probe“ heißt: Probe-Test in `<scratchpad>/u5f/proben/` ist grün gegen den unmutierten Code und rot gegen den nachgebildeten Mutanten (Lauf mit `mut_env`). Vorrang: P = sollte Nachweis- oder Regressionstest werden, n = nachrangig.

| ID | Vorrang | Fundstelle, Mutanten | Ungeprüftes Verhalten | Bezug | Probe |
|---|---|---|---|---|---|
| T-F-01 | P | `restart_budget._check_heartbeats` `_12`, `_30` (`continue` → `break`, Z. 76, 89) | Ein toter, kanalloser oder recycelnder Worker vorn in `workers` beendet die Herzschlagprüfung aller folgenden: ein hängender Worker dahinter wird nie ersetzt. Alle Bestandstests haben einen Worker oder keinen zu überspringenden | E-018-Umfeld, R21/R30 | `test_probe_pm_luecken.py::test_a_dead_first_worker_does_not_hide_a_hung_second_one`, `…recycling_first_worker…`: rot unter `_12` bzw. `_30` |
| T-F-02 | P | `restart_budget._escalate_terminating` `_4`, `_14` (`continue` → `break`) | Nach einem schon toten oder gerade getöteten Eintrag fallen alle weiteren aus `_terminating` heraus und bekommen **nie** SIGKILL (die Liste wird mit `remaining` überschrieben) | KTD10, E-024 | `test_escalation_kills_every_overdue_worker_after_a_dead_or_killed_one`: rot unter beiden |
| T-F-03 | P | `restart_budget._replace_hung_worker` `_30` (`now + SLOT_COOLDOWN_S` → `-`) | Dass ein Slot nach erschöpftem Hang-Budget **bis** zum Ende der Abkühlzeit gesperrt bleibt; `test_slot_cooldown.py:79` prüft nur Sperre sofort und Freigabe danach | E-024 | `test_exhausted_hang_budget_keeps_the_slot_locked_until_the_cooldown`: rot |
| T-F-04 | n | `restart_budget._heartbeat_deadline` `_10` (`grace + TERM` → `grace − TERM` im Recycling-Zweig) | Die F3-13-Tests treffen keinen Wert zwischen `grace − 5` und `grace + 5` oberhalb von `heartbeat_timeout_s` | E-018 Nachtrag F3-13 | – |
| T-F-05 | P | `setpoint.apply` 14 Mutanten (`_13`, `_21/_22`, `_36/_37`, `_51`, `_74`, `_77–_82`) | Der Rückgabewert „wirksame Grenzen geändert“: Liefert `apply` fälschlich `True` (etwa bei unerreichbarem Speicher), setzt `tick` je Sollwert-Intervall Abkühl- und Leerlaufbuchhaltung des Autoskalierers zurück, der dann nie entscheidet; fälschlich `False` lässt ihn in der Runde der Änderung entscheiden (KTD3) | Kontrollkanal KTD3/R7 | `test_an_unreachable_store_does_not_count_as_a_change`: rot unter `_13` |
| T-F-06 | P | `setpoint.status_ttl_s` `_2` (`+ SAMPLE_INTERVAL` → `−`) | `test_setpoint.py::TestStatus` setzt `SETPOINT_SAMPLE_INTERVAL = 0.0`; damit ist das Vorzeichen des Intervalls in der Status-TTL unprüfbar | KTD7, `cli.md:253-255`, E-018 offener Punkt | `test_status_ttl_adds_the_sample_interval`: rot |
| T-F-07 | P | `setpoint.build_status` `_23–_27`, `_40/_41`, `_43` | Feld `autoscaling` (nur mit Autoskalierer **und** Bereich `low != high`), Schlüssel `reported_at` (in `cli.md:253` zugesagt) und dessen Zeitzone | R14/R15 | `test_status_reports_autoscaling_only_for_a_range_with_autoscaler`: rot unter `_25`, `_26`, `_27` |
| T-F-08 | P | `supervisor._monitor_loop` `_68` (`autoscale is not None` → `is None`) | Dass der Monitor-Loop den Autoskalierer überhaupt aufruft; mit dem Mutanten skaliert eine `--autoscale`-Gruppe nie | R4.7 | `test_the_monitor_loop_ticks_the_autoscaler`: rot |
| T-F-09 | n | `reload.reexec` `_38` (Flag bleibt nach Rückkehr `True`) | Nach einem Reload ohne `execve` muss `_reexec_holds_signals` zurückgesetzt sein, sonst stellt jedes weitere SIGTERM sich selbst neu zu | KTD8 (Reload) | `test_reexec_without_execvpe_leaves_no_signal_hold`: rot (prüft ein privates Attribut; als Nachweis über das Signalverhalten formulieren) |
| T-F-10 | n | `autoscale.tick` `_47`, `_57`, `_58`, `setpoint.tick` `_21`, `_23`, `setpoint.read_at_start` `_3` | Folge-Takt nach einer Entscheidung: `""`/`None` in `_idle_since`, `_last_scale_decision`, `last_sample` ließen den **nächsten** Takt mit `TypeError` scheitern (Monitor-Loop bricht ab). Kein Test fährt zwei Takte nach einer Skalier- oder Sollwert-Entscheidung; `_54` (`now + idle_since`) zeigt zudem, dass die Leerlauffrist vor `SCALE_DOWN_IDLE_S` nicht geprüft ist | R4.7, Kontrollkanal | – |
| T-F-11 | P | `check_cmd._run_worker_check` Autoscale-Verdrahtung (`_15`, `_28`, `_67`, `_71–_73`), `--configure` (`_21`, `_34`, `_102`, `_107/_108`), `--watch-dir` (`_12`, `_38`, `_42–_44`); `process_manager/check.check_worker_config` `_7/_8` (`validate_pool_mode`), `_49` | `worker --check` mit `--autoscale` (Port-Guard an der Obergrenze), `--configure`, `--watch-dir` und `--pool cpu` + `--autoscale`; `TestPreflightCommandAgrees` deckt nur `--processes` und Ports | E-062 | `test_probe_cli_luecken.py::test_worker_check_rejects_ports_that_overlap_at_the_autoscale_upper_bound`: rot unter `_15` |
| T-F-12 | n | `check_cmd._is_check_invocation` `_6` (`sys.argv[2:]`) | Erkennung von `--check` beim Aufruf ohne `argv` (Konsolenskript); alle Tests übergeben `argv` | E-062 (JSON auch bei argparse-Fehlern) | `test_check_detection_reads_sys_argv_when_main_gets_none`: rot |
| T-F-13 | P | `dlq_cmd` Queue-Verdrahtung: `_run_dlq_purge_3`, `_run_dlq_list_3/_5`, `_run_dlq_inspect_3–_8`, `_run_dlq_retry_18–_21`, `_53–_58` | Welche Queue an den Broker geht. Die Fakes ignorieren das Argument; ein vertauschtes Argument bei `purge` löschte eine fremde DLQ | E-028 (Umfeld) | `test_dlq_purge_purges_exactly_the_named_queue`: rot unter `_run_dlq_purge__mutmut_3` |
| T-F-14 | P | `dlq_cmd` JSON-Verträge: `_run_dlq_inspect_21–_24`, `_37–_44`, `_run_dlq_retry_39–_43`, `_68/_70`, `_71–_74`, `_run_dlq_list_10–_17`; `_normalise_headers_for_json_7/_10/_11`, `_format_dlq_list_table_35/_38/_39` | Schlüssel des Nicht-gefunden-Payloads, `moved` bei Nicht-Fund, `all`, Normalisierung der Header für JSON und Dekodierung ungültiger UTF-8-Bytes (strikt dekodiert → `UnicodeDecodeError`) | R3.10 P3 #18 | – |
| T-F-15 | P | `schedule_cmd._resolve_fire_time` `_10/_11` (Basis `time.time() / 1000`), `_23` (Zeitzone eines `--at` überschrieben), `_24–_28` | Score des ersten Cron-Termins (mit `_10` läge er 1970, das Fire käme sofort) und die Umrechnung von `--at` mit Offset; `cli.md:312` sagt „without a timezone it is read as UTC“ | `cli.md:310-313` | `test_cron_schedule_gets_its_next_fire_in_the_future`, `test_at_with_offset_keeps_its_instant`: rot unter `_10` bzw. `_23` |
| T-F-16 | P | `schedule_cmd._run_schedule_add` (113) und `_render_add_result` (30, ohne Test) | Auf Unit-Ebene ist der Erfolgspfad von `schedule add` ungetestet: Definition (Name, Task, Argumente, Queue, Cron, `source`, `dedup_hash`, `unique_until`), Claim-TTL (`(unique_for + ceil(offset)) * 1000`), Rollback, Ausgabe. Die Integrationstests decken Teile, Lauf A bewertet sie nicht | R4M-F-01, -02, E-011, E-066 | siehe R4M-F-01/-02 |
| T-F-17 | n | `schedule_cmd._run_schedule_status` 36 (Horizont 24 h, Sortierung, JSON-Schlüssel), `_run_schedule_remove` 16 (JSON-Schlüssel, `disable` statt `remove`), `_run_schedule_async` 9 (Verteilung auf Unterbefehle) | JSON-Ausgaben und Verteilung; `test_cli_schedule_status.py` prüft Leader-, Versions- und Worker-Felder, `upcoming` nur leer | – | – |
| T-F-18 | n | `process_manager/run.py` 74 (u. a. `_196–_208`, `_225–_252`, `_255–_277`, `_63/_85`, `_142–_156`, `_173`, `_178–_183`, `run_multi_process_worker_8/_16/_17`) | Verdrahtung CLI → `check_worker_config` → `WorkerConfig` → `ProcessManager`: Pool, Nebenläufigkeit (nur ausdrücklich gegen den CPU-Spielraum, E-061), Ports, `--configure` (E-069), Burst, Sampler, `delivery_limit`; dazu „nur eine der beiden URLs fehlt“ (mit `_8`/`_16` endete das in `KeyError` statt Exit 78). Teilweise durch e2e gedeckt (`tests/e2e/test_logging_pools_e2e.py` mit `--processes` und `--pool`), Lauf A misst das nicht | E-057, E-061, E-062, E-069 | – |
| T-F-19 | n | `process_manager/check._validate_observability_ports` `_67/_68/_73` (Health-Bereich über 65535 nach gültigem Metrics-Bereich), `_72`, `_83/_84/_87` (Grenzen der Überlappung); `_validate_headroom_2`; `_validate_configure_spec_5/_6`; `setpoint.validate_4` | Grenzwerte: Port 65535 genau, sich berührende Bereiche, `--max-processes == --processes`, Sollwert genau am Spielraum (laut `cli.md` erst „exceeds“ abgelehnt), `--configure ':f'` bzw. `'m:'` | R5/R6, Kontrollkanal R10 | – |
| T-F-20 | n | `entry.worker_subprocess_entry` `_5/_7` (Rückfall-Logging ohne Format), `_24/_35/_36` (Herzschlag-Takt), `_logging_settings_3–_5`, `_worker_namespace_7` | Fehlerzeile bei `LoggingConfigError` im gewählten Format (R21/U4); dass der Watcher weiterläuft und im Takt `heartbeat_interval_s` schlägt (mit `_24`/`_35` stirbt er still: kein Herzschlag, kein Shutdown-Event); Rückfallpfad ohne `logging_settings` | E-057 (Umfeld), R21/KTD10 | – |
| T-F-21 | n | `setpoint._drain_one` `_7/_10/_12`, `setpoint.reconcile` `_5/_8/_15/_16/_35/_36` | Auswahl des jüngsten Workers nach Startzeit (nicht nach PID), `acted`-Flag, Untergrenze ohne Autoskalierer (`_current_target`, sonst würden saubere Enden wieder aufgefüllt) | Kontrollkanal KTD11, R12 | – |
| T-F-22 | n | `setpoint.read_at_start_9`, `setpoint.tick_15`, `setpoint_store.read_setpoint_sync_4` | Gruppenschlüssel beim **Lesen** des Sollwerts; `FakeStore.read_setpoint` ignoriert die Gruppe (nur das Schreiben prüft sie). Integration `tests/integration/test_cli_worker_setpoint.py` deckt es, Lauf A nicht | R5 | – |
| T-F-23 | n | `supervisor.__init__` `_9` (Autoscale `min == max`), `_47` (Start-Kontext `spawn`), `_signal_survivors_17`, `_shutdown_all_workers_5/_8/_10` | Gleiche Autoscale-Grenzen erlaubt; `spawn` statt Standard (3.14: `forkserver`); SIGKILL an alle Überlebenden, wenn einer zwischendurch verschwindet; Budget mit `supervised=True` und Einsammeln bis `started + total` | BZ-18/E-018 | – |
| T-F-24 | n | `check_cmd._seed_check_payload` (20), `_empty_check_payload` (2), `_check_task_injection_3` | Werte (nicht nur Schlüssel) im Fehler-Payload von `--check`; `warnings` ist `[]` statt `null`; Modulausgaben gehen nach stderr | E-062, F3-19 | – |
| T-F-25 | n | `parser_ops._add_schedule_parser` `_29/_32/_37`, `_add_dlq_parser` `_30/_33/_38` | `schedule`/`dlq` ohne Unterbefehl: Exit 2 von argparse statt `ConfigurationError` aus dem `else`-Zweig (`pragma: no cover`) | `cli.md` Exit-Codes | – |
| T-F-26 | n | `worker_target._bind_to_supervisor` `_25/_29/_30` | Waisenfall zwischen `fork` und `prctl`: `SystemExit(0)` | Notiz e2e-scheduler-leader-flake | – |

Beobachtung ohne Folge (kein Verdacht): `setpoint.apply` liefert `True`, wenn ein Sollwert gleich den Startgrenzen gelesen wird (Quelle wechselt von `startup` zu `setpoint`, Grenzen bleiben gleich), und beim Löschen eines Sollwerts, der den Startgrenzen glich. Der Docstring sagt „`True`, wenn sich die wirksamen Grenzen geändert haben“. Die Wirkung ist ein einmaliges Zurücksetzen der Autoskalierer-Buchhaltung.

Abgleich mit der Testlücken-Liste aus Runde 2 (Plan U5, Schritt 4): Drain-Frist 0, `lock_wait_max_s` über Kopien, Keeper-Timeout und `_check_late_cancel` liegen außerhalb von Gruppe F. Drain-Frist 0 berührt F nur über `shutdown_grace_s=0` (E-018 Nachtrag F3-42); dazu überlebt in F kein Mutant, die Tests `test_befund_f3_42_shutdown_timeout_zero.py` greifen.

## Zusammenfassung je Modul

Fehler = Verdacht auf echten Fehler im Umfeld (mit Probe), Lücke/Rauschen/Artefakt = Zahl der Mutanten.

| Modul | Mutanten | Fehler | Lücke | Rauschen | Artefakt | Kern |
|---|---:|---:|---:|---:|---:|---|
| `process_manager/restart_budget.py` | 10 | 0 | 6 | 4 | 0 | E-018-Restpunkt: kein neuer Fehler. Die Mutanten zeigen Lücken in Mehr-Worker-Läufen (T-F-01, T-F-02) und an der Abkühlzeit (T-F-03); das `continue` → `break` ist im Code richtig |
| `process_manager/setpoint.py` | 41 | 0 | 38 | 3 | 0 | `changed`-Flag (T-F-05), Status-TTL (T-F-06), Feld `autoscaling` (T-F-07), Folge-Takte (T-F-10) |
| `process_manager/setpoint_store.py` | 6 | 0 | 1 | 5 | 0 | Gruppe beim Lesen (T-F-22); Rest Fristen und Fehlertext |
| `process_manager/autoscale.py` | 11 | 0 | 5 | 6 | 0 | Folge-Takte und Leerlauffrist (T-F-10); Grenzwerte `<`/`<=` sind Rauschen |
| `process_manager/supervisor.py` | 41 | 0 | 7 | 33 | 1 | Autoskalierer im Monitor-Loop (T-F-08); Logfelder und Fristen Rauschen; `__init__68` Artefakt; SIGSEGV-Mutant `_handle_exit_code__mutmut_1` (nicht in der Eingabe, Status segfault) ist erkannt |
| `process_manager/reload.py` | 13 | 0 | 1 | 11 | 1 | `_restore_signal_mask_1` Artefakt (lokal erkannt), `reexec_38` Lücke (T-F-09) |
| `process_manager/entry.py` | 21 | 0 | 9 | 12 | 0 | Exit-Codes (E-057) ohne Überlebende; Lücken im Rückfall-Logging und Herzschlag-Watcher (T-F-20) |
| `process_manager/worker_target.py` | 25 | 0 | 3 | 1 | 21 | Score 34 % ist Abbildung (Test läuft im `spawn`-Kind); Waisenfall ungetestet (T-F-26); Test setzt PDEATHSIG im Testprozess |
| `process_manager/run.py` | 113 | 0 | 74 | 39 | 0 | Verdrahtung in Kinder und Pre-Flight (T-F-18); Rauschen: ungenutzter Parameter `recycling`, Platzhalter-URLs, Defaults |
| `process_manager/check.py` | 38 | 0 | 16 | 22 | 0 | Port- und Spielraum-Grenzen (T-F-19), `validate_pool_mode` unter `--check` (T-F-11); Fehlertexte Rauschen |
| `process_manager/sampler.py`, `slots.py`, `config.py` | 4 | 0 | 0 | 4 | 0 | Fristen, Testpfad `lock(inf)`, Fehlertext |
| `cli/check_cmd.py` | 111 | 0 | 43 | 68 | 0 | Exit-Codes 0/1 ohne Überlebende (E-062); Verdrahtung `--autoscale`/`--configure`/`--watch-dir` (T-F-11); `_is_check_invocation` fast nur äquivalente Parser-Mutanten (50 Rauschen) |
| `cli/dlq_cmd.py` | 140 | 1 (R4M-F-03) | 73 | 67 | 0 | Exit 3 ohne Überlebende; Queue-Verdrahtung (T-F-13), JSON-Verträge (T-F-14); Tabellen und Hinweistexte Rauschen |
| `cli/schedule_cmd.py` | 302 | 2 (R4M-F-01, -02) | 218 | 84 | 0 | 38 % Score: Erfolgspfad von `add` auf Unit-Ebene ungetestet (T-F-16), Fire-Zeit (T-F-15), `status`/`remove` (T-F-17) |
| `cli/parser_ops.py` | 20 | 0 | 6 | 14 | 0 | Pflicht-Unterbefehl (T-F-25); entfernte `default=None` äquivalent |
| **Summe** | **896** | **3** | **500** | **373** | **23** | |

Gebündelte Ausgabe-Strings der CLI (die 215 String-Mutanten aus `3a`): Tabellenköpfe und Hinweistexte für Menschen sind Rauschen; JSON-Schlüssel von `dlq inspect`/`retry`/`list`, `schedule status`/`remove` und der Status-Schlüssel `reported_at` sind Ausgabevertrag und als Lücke gezählt (T-F-07, T-F-14, T-F-17).

## Abgleich mit Register, KTD3/KTD5 und `docs/befunde/README.md`

| Entscheidung | Ergebnis der Sichtung |
|---|---|
| E-006 (`swarmq check`, Exit 78 nur bei Startablehnung) | Gilt für `cli/env_check_cmd.py`, das nach Welle 3 verschoben ist. `cli/check_cmd.py` ist `swarmq worker --check` und fällt unter E-062. Der Auftrag nennt „check_cmd Exitcodes (E-006)“; das ist eine Abweichung zwischen Auftrag und Code. In `check_cmd.py` überlebt kein Mutant an einem Exit-Code |
| E-018 (Gesamtbudget; offener Punkt `restart_budget.py`/`setpoint.py:210` rechnen `grace + TERM`) | Kein neues Argument. Die Überlebenden `_heartbeat_deadline_10` und `status_ttl_s_2` zeigen nur schwach festgehaltene Formeln (T-F-04, T-F-06). Der Zweig `INTENT_DRAINING` (`restart_budget.py:63-64`) hat keinen Überlebenden; er ist durch `test_befund_f3_13_recycle_heartbeat_deadline.py::test_scale_down_drain_keeps_shutdown_grace_plus_term` (Marker E-018) mit der alten Bedeutung festgeschrieben. Eine Änderung müsste erst die Entscheidung ändern |
| F3-41 (Scale-down-Drain bei `heartbeat_interval_s >= shutdown_timeout + 5`) | Bleibt offen; die Mutanten in `_drain_one`/`scale_down_one` betreffen Auswahl und Flag, nicht die Frist. Bestätigt durch Lesen: `_drain_one` und `scale_down_one` tragen keine SIGKILL-Eskalation in `_terminating` ein (Gegenargument aus F3-41 gilt weiter) |
| E-024 (Slot-Abkühlzeit 3600 s) | Eingehalten; T-F-03 schließt die Lücke „Sperre bis zum Ende“ beim Hang-Budget, T-F-02 die SIGKILL-Eskalation |
| E-028 (DLQ nicht zerstörend, Abgelaufene ohne Kopie bestätigt) | R4M-F-03 betrifft die **Zählung** der Abgelaufenen, nicht ihr Bestätigen; kein Widerspruch |
| E-057 (Exit 78 bei Konfigurationsfehler) | In `entry.py` überlebt kein Mutant auf einem Exit-Code-Pfad; die 7 Zeitüberschreitungen aus `3a` liegen in der Herzschlag-Schleife und sind erkannt. `run_multi_process_worker_8/_16` (nur eine URL fehlt → `KeyError`) ist eine Lücke, kein Fehler |
| E-061 (CPU-Slots = zugeteilte CPUs) | Verdrahtung von `concurrency`/`concurrency_source` in den CPU-Spielraum-Check nur durch Lücken betroffen (T-F-18); `--check` und Start rechnen gleich (gelesen: beide teilen durch `max_processes or processes`) |
| E-062 (`worker --check` sagt Start voraus) | Lücke T-F-11: `--autoscale`, `--configure`, `--watch-dir` in `--check` ungetestet; Verhalten richtig |
| E-069 (`--configure`) | Nur Verdrahtungslücken (T-F-11, T-F-18) |
| E-011, E-058, E-066 (Unique-Claim, `schedule add`) | Gegen R4M-F-01/-02 geprüft: legen Meldung und Selbsttreffer fest, nicht den verwaisten Claim |
| KTD3/KTD5 (erlaubte Abweichungen Spur 1/2) | Betreffen Leader, Locks, Retry und Störungsarten; keiner der drei Verdachte fällt darunter |
| `docs/befunde/README.md` | Jeder Verdacht nennt Quelle der Erwartung, Register-Abgleich, Realismus (Häufigkeit) und einen Einordnungsvorschlag; die Einstufung trifft die Gegenprüfung (U17) |

Weitere Abweichungen zwischen Plan bzw. Auftrag und Code:

- Plan U5 nennt `restart_budget.py`/`setpoint.py` als „E-018-Restpunkt“ mit Fehlerverdacht. Im Code ist das `continue` an allen drei Stellen richtig; die überlebenden `break`-Mutanten sind Lücken.
- `3a-mutation-lauf-a.md` führt `_handle_exit_code__mutmut_1` als „SIGSEGV, Ursache ungeklärt“. Nach der Nachbildung ist der Mutant von 14 Tests erkannt; das SIGSEGV kommt nicht aus dem Mutanten.
- Plan U5 verlangt je Mutant einen „roten Probe-Test gegen den unmutierten Code“. Für Lücken ist ein solcher Test per Definition grün; belegt wird dort mit grün gegen Original und rot gegen den nachgebildeten Mutanten.

## Nach Welle 3 verschoben

Nicht gesichtet (Open Question, Begrenzung der CLI-Gruppe); Zahlen aus `mut-gruppe-F.txt` nach Vorfilter, nachgezählt:

| Modul | überlebt | ohne Test |
|---|---:|---:|
| `cli/__init__.py` | 43 | 0 |
| `cli/_shared.py` | 24 | 0 |
| `cli/env_check_cmd.py` | 129 | 27 |
| `cli/inspect_cmd.py` | 45 | 64 |
| `cli/parser.py` | 29 | 0 |
| `cli/pool.py` | 6 | 0 |
| `cli/worker_cmd.py` | 43 | 0 |
| `cli/worker_options.py` | 7 | 0 |
| **Summe** | **326** | **91** |

Hinweise für Welle 3: E-006 gehört zu `env_check_cmd.py` (hier nicht geprüft). `inspect_cmd._format_inspect_table` (64 ohne Test) und `env_check_cmd._seen_interval_s` (27 ohne Test) haben keinen Unit-Test. Der Mechanismus aus „Messbefunde“ (SIGTERM → Exit 0 im mutmut-Enkel) gilt auch für `cli/__init__.py` und `worker_cmd.py`, soweit dort Signale an den eigenen Prozess gehen.
