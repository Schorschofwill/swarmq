# Learnings- und Register-Recherche: Fix-Plan Fehlersuche Runde 3 (swarmq 0.12.3)

Stand: Worktree `swarmq-fehlersuche3`, Branch `test/fehlersuche-runde-3` @ `580ef3c` (2026-10-15).
Suchwurzeln: `docs/solutions/` (16 Dateien) und Pack `swarmq-entscheidungen` (71 Regeln, README ausgenommen).
`CONCEPTS.md` und `docs/solutions/patterns/critical-patterns.md` gibt es nicht.
Skipped pack files: keine (alle Regeln haben Frontmatter mit `applies_when`; der Guard ist grün).

Geltungsbereich: FEHLER F3-01 bis F3-25, F3-31 (entschieden per E-055), Doku-Korrekturen F3-28/29/30, Doku-Sätze F3-33 und F3-37. Nicht behoben werden F3-32, F3-34, F3-35, F3-36, F3-38.

---

## 0. Stand des Registers und des Guards

**Der parallele Agent hat inzwischen committet.** `580ef3c test(register): Guard prüft Listeneinträge als Text, Quellen gequotet, Marker für Altentscheidungen E-012/E-018/E-056–E-070`. Der Arbeitsbaum ist sauber. Der Commit ändert:

- `tests/unit/_entscheidungen.py`: neue Prüfung `_check_text_entries`. Einträge unter `applies_when`, `quelle` und `tests` müssen Text sein. Ein Eintrag mit `a: b` liest YAML als Mapping und fällt jetzt durch. Folge für den Plan: Neue `tests:`- und `quelle:`-Einträge mit Doppelpunkt plus Leerzeichen in Anführungszeichen setzen (Muster: `'docs/plans/…:55 (Abschnitt 1, BZ-03 Oneshot: …)'`).
- 22 Regeln: Quellen gequotet (nur Format).
- E-012, E-018, E-056 bis E-070: bestehende Tests bekamen Marker und stehen jetzt unter `tests:`.
- 20 Testdateien: nur neue `@pytest.mark.entscheidung`-Marker.

Der Plan muss auf `580ef3c` aufsetzen. Konfliktgefahr besteht in jeder Regeldatei E-055 bis E-071 und in den Nachtrags-Regeln, weil jeder Fix dort `tests:`/`ohne_test` ändert.

`uv run --extra test pytest tests/unit/test_entscheidungsregister.py -p no:randomly` ist grün (11 Tests). Eine Gegenprobe per AST zeigt: Jeder Test mit `entscheidung`-Marker steht heute in der `tests:`-Liste seiner Regel.

### Was der Guard prüft (`tests/unit/_entscheidungen.py`)

| Prüfung | Funktion | Wirkung auf einen Fix-Commit |
|---|---|---|
| Pflichtfelder, ID-Format, doppelte IDs, Listeneinträge als Text | `_check_fields`, `_check_text_entries` | neue Einträge quoten, wenn `: ` vorkommt |
| `status` `gilt`/`überholt`, `ersetzt_durch` existiert | `_check_status` | nur bei geänderter Regel |
| `tests` oder `ohne_test` vorhanden | `_check_tests` | `tests: []` mit `ohne_test` ist erlaubt. Eine Regel ohne beides scheitert. |
| jeder Eintrag unter `tests:` existiert (`pfad::[Klasse::]funktion`) und trägt den Marker der Regel | `_test_markers` | Marker und `tests:`-Eintrag müssen **im selben Commit** kommen. Marker wirken als Dekorator der Funktion, der Klasse oder als `pytestmark`. Der Node-ID-Eintrag hat keine Parametrize-ID. |
| jeder benutzte Marker hat eine Regel | `_check_unknown_markers` | keine neuen IDs ohne Regeldatei |

**Was der Guard nicht prüft:**

1. Er prüft nicht, ob ein markierter Test auch unter `tests:` steht. Die README des Packs behauptet „in beiden Richtungen konsistent“, das stimmt nur für Regel → Test. Ein Fix-Commit kann einen Marker ohne Listeneintrag setzen, und der Guard bleibt grün. Der Plan muss das als Review-Punkt führen.
2. Er prüft nicht, ob `ohne_test` nach dem Fix veraltet ist. Text wie „Test entsteht mit dem Fix in 0.12.3“ bleibt sonst stehen.
3. Er kennt den `befund`-Marker nicht. `pyproject.toml:163` beschreibt `befund` als „immer mit xfail strict=True“. In der Praxis von Runde 2 bleibt `befund` nach dem Fix stehen, nur `xfail` fällt weg (`test_befund_fs15_result_wait_polling.py:129-130`: `befund("FS-15")` + `entscheidung("E-030")`, kein xfail; ebenso `test_befund_fs06_…py:93`). Der DoD-Satz im Runde-2-Plan („tragen keinen `befund`/`xfail`-Marker mehr“, Plan `:722`) widerspricht dieser Praxis. Empfehlung: Praxis beibehalten (befund bleibt als Rückverweis) und so in den Plan schreiben.

### Was ein Fix-Commit gleichzeitig ändern muss, damit alles grün bleibt

1. Nachweis-Test: `@pytest.mark.xfail(strict=True, …)` entfernen. Sonst wird er XPASS und damit rot. `@pytest.mark.befund("F3-nn")` bleibt. Dazu kommt `@pytest.mark.entscheidung("E-nnn")` der festschreibenden Regel (Tabelle unten).
2. Regeldatei: Node-IDs unter `tests:` eintragen. `tests: []` durch die Liste ersetzen. `ohne_test` löschen oder auf den verbleibenden ungetesteten Rest kürzen.
3. Nachtrag-Text der Regel von „Fix in 0.12.3“ auf „behoben in 0.12.3 (Unit, Commit)“ ändern. Keine Pflicht des Guards, aber Konvention aus Runde 2 (`faf6638`).
4. Befundregister: Status in der Tabelle „Nachweis-Tests“ und die Statuszeile oben anpassen (Abschnitt 4).
5. Kein bestehender Test mit `entscheidung`-Marker wird geändert. Das prüft nur `git diff` (Runde-2-Plan, Verification Contract `:710`).

---

## 1. Befund → festschreibende Regel → Marker nach dem Fix

Alle Nachweise sind Funktionen auf Modulebene, ohne Klasse. Die Node-ID ist also `pfad::funktion`.

| Befund | Nachweis (git ls-files) | Testfunktionen | Regel / Marker nach Fix | Frontmatter-Wechsel |
|---|---|---|---|---|
| F3-01 | `tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py` | `test_deferred_oneshot_is_indexed_after_lost_hsetnx_reply` | E-058 | `tests: []` → Liste; `ohne_test` löschen |
| F3-02 | `tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py` | `test_pre_enqueue_veto_blocks_deferred_schedule`, `test_pre_enqueue_veto_precedes_unique_claim_on_deferred_schedule`, `test_task_enqueued_fires_after_store_on_deferred_schedule`, `test_middleware_kwargs_change_is_stored_with_deferred_definition` | E-059 | `tests:` ergänzen; `ohne_test` löschen |
| F3-03 | `tests/unit/worker/test_befund_f3_03_retry_negative_delay.py` | `test_negative_retry_delay_is_clamped_to_zero_and_counts`, `test_non_finite_retry_delay_is_rejected` | E-060 | `tests:` ergänzen; `ohne_test` löschen |
| F3-04 | `tests/unit/test_befund_f3_04_finite_deadlines.py` | `test_deadline_field_must_be_finite_kwarg`, `test_deadline_field_must_be_finite_env` | E-063 | `tests:` ergänzen; `ohne_test` löschen |
| F3-05 | `tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py` | `test_import_error_in_the_task_module_exits_with_config_code`, `test_syntax_error_in_the_task_module_exits_with_config_code` | E-057 | `tests:` ergänzen; `ohne_test` löschen |
| F3-07 | `tests/unit/client/test_befund_f3_07_call_unique_for_bounds.py` | `test_schedule_rejects_non_positive_unique_for`, `test_deferred_schedule_rejects_non_positive_unique_for`, `test_schedule_many_rejects_non_positive_unique_for` | E-056 | `tests:` ergänzen; `ohne_test` löschen |
| F3-08 | `tests/unit/test_befund_f3_08_cron_without_next_fire.py` | `test_taskinfo_rejects_cron_that_never_fires` | E-065 | `tests:` ergänzen (E-065 hat kein `ohne_test`); Absatz „Überdenken“ nachziehen |
| F3-09 | `tests/unit/client/test_befund_f3_09_get_results_timeout_zero.py` | `test_get_results_with_zero_timeout_reads_once`, `test_get_results_reads_once_more_when_deadline_passes_during_sleep` | E-030 (Nachtrag) | `tests:` ergänzen |
| F3-11 | `tests/unit/test_befund_f3_11_exception_exports.py` | `test_documented_exceptions_are_exported_from_package` | **keine Regel** | nur `befund` behalten (wie FS-06) |
| F3-12 | `tests/unit/worker/test_befund_f3_12_shutdown_double_release.py` | `test_shutdown_then_handler_finally_releases_each_token_once` | E-071 (mit Nachtrag, siehe unten) | `tests: []` → Liste; `ohne_test` umschreiben |
| F3-13 | `tests/unit/process_manager/test_befund_f3_13_recycle_heartbeat_deadline.py` | `test_recycling_worker_gets_at_least_heartbeat_timeout` | E-018 (Nachtrag) | `tests:` ergänzen |
| F3-14 | `tests/unit/test_befund_f3_14_embedded_cpu_concurrency.py` | `test_embedded_cpu_pool_defaults_to_assigned_cpus` | E-061 | `tests:` ergänzen; `ohne_test` löschen |
| F3-15 | `tests/unit/worker/test_befund_f3_15_progress_started_at.py` | `test_io_progress_started_at_is_wall_clock`, `test_cpu_child_progress_started_at_is_wall_clock` | E-044 (Nachtrag) | `tests:` ergänzen |
| F3-16 | `tests/unit/test_befund_f3_16_fixed_backoff_cap.py` | `test_fixed_backoff_is_capped_by_max_delay` | E-023 (Nachtrag) | `tests:` ergänzen |
| F3-17 | `tests/unit/test_befund_f3_17_worker_check_env.py` | `test_worker_check_rejects_env_values_the_start_rejects` | E-062 | `tests:` ergänzen; `ohne_test` erst nach F3-19 löschen |
| F3-18 | `tests/unit/test_befund_f3_18_check_invalid_config_exit78.py` | `test_check_reports_invalid_configuration_with_exit_78` | E-006 (Nachtrag) | `tests:` ergänzen |
| F3-19 | `tests/unit/test_befund_f3_19_check_argparse_payload.py` | `test_argparse_error_payload_carries_every_contract_key` | E-062 | `tests:` ergänzen; `ohne_test` löschen, wenn F3-17 auch behoben ist |
| F3-31 | `tests/unit/test_befund_f3_31_float_ttl_normalized.py` | `test_task_info_result_ttl_is_normalized_to_int`, `test_task_info_unique_for_is_normalized_to_int`, `test_config_result_ttl_is_normalized_to_int` | E-055 | `tests: []` → Liste; `ohne_test` löschen |

Doku-Befunde ohne Nachweis-Test (Rückverweis der Regel, kein Marker):

| Befund | Regel | Hinweis |
|---|---|---|
| F3-06 | E-069 | Guides auf `--configure` umstellen (EN/DE) |
| F3-10 | E-064 | nur Liste „Reserved parameter names“ ergänzen; E-064 hat dazu `ohne_test` |
| F3-20 | E-068 | plus `agent_spec.py:247` und Spec neu erzeugen (Abschnitt 3) |
| F3-21 | E-004 | Oneshot-Verlust nach `scheduler_unroutable_oneshot_max_s` (E-004, Test `test_config_scheduler_unroutable.py::test_default_is_one_day`) begründet den Doku-Satz |
| F3-22 | E-029 | Mindestversion 0.12 (`leader_version.py:22-23`) |
| F3-23 | keine | neuer Guard-Test „Katalog gegen `spec/errors.json`“ ohne `entscheidung`-Marker |
| F3-24 | E-012 (Nachtrag) | |
| F3-25 | keine | |
| F3-28 | E-067 | |
| F3-29 | E-066 | |
| F3-30 | E-068 | E-068 `ohne_test`: „Für kein on_error/post_execute bei Cancel fehlt ein Test“. Optional einen Charakterisierungstest mit Marker E-068 schreiben (Code bleibt unverändert). |
| F3-33 | keine | K-3 ist laut Befundregister nicht übernommen |
| F3-37 | E-070 | |

### Unstimmigkeiten

1. **F3-03 Integrationsnachweis fehlt.** Das Befundregister plant `tests/integration/broker/test_befund_f3_03_negative_expiration.py` (keine fremden `PublishIndeterminate`). Die Datei gibt es nicht. Sie wird im Fix zuerst rot geschrieben (Testports, `uv run just integration`) oder ausdrücklich gestrichen.
2. **E-055 gilt auch für `schedule()`/`schedule_many()`, aber kein Nachweis prüft das.** Laut E-055 wird `unique_for=3600.0` am Aufruf zu `3600` und `0.2` zu `1`. Der F3-31-Nachweis prüft nur `TaskInfo` und `SwarmQConfig.result_ttl` (sein Docstring schiebt den Aufrufparameter zu F3-07). Der F3-07-Nachweis prüft nur `0` und `-5`. Im Fix also zuerst einen roten Test für `schedule(unique_for=3600.0)` schreiben, sofort und verzögert (Deferred-TTL-Arithmetik, Pattern 6 unten).
3. **E-055 nennt `unique_for` in `SwarmQConfig`.** Das Feld gibt es dort nicht (`config.py:188` hat nur `result_ttl`). Kleiner Textfehler der Regel. `SWARMQ_RESULT_TTL` wird als `"int"` geparst (`config.py:445`), `86400.0` aus der Umgebung ist also ein Parse-Fehler. E-055 regelt die Umgebung nicht. Nicht still mitentscheiden, höchstens melden.
4. **E-055 und E-056 zusammen: Reihenfolge der Prüfungen.** `0.0` und `-0.5` sind Fehler (E-055 „Die übrigen Bereichsregeln bleiben“). `ceil(-0.5) == 0` darf also nicht vor der Bereichsprüfung laufen. Erst `<= 0` prüfen, dann aufrunden. Testfälle: `0.0` → `ValueError`, `-0.5` → `ValueError`, `0.2` → `1`.
5. **F3-12 hat keine passende Regel.** E-071 regelt die Reihenfolge („erst nach der Wartezeit frei“). Ihr Absatz „Überdenken“ nennt F3-12 ausdrücklich als FEHLER. Das zugesagte Verhalten („genau eine Freigabe je Token, kein `task_lock_lost`“) steht aber nur im Runde-2-Plan (KTD6 `:151`, `:395`), nicht im Register. Empfehlung: mit dem Fix einen Nachtrag zu E-071 schreiben und den Nachweis mit E-071 markieren. Den Nebeneffekt F3-32 dabei nicht festschreiben.
6. **F3-11 hat keine Regel.** Die Zusage steht in `errors.md:50-52`. Das ist keine Produktentscheidung, also kein Marker.
7. **Veraltete Nachtrag-Texte nach `580ef3c`:**
   - E-012, Nachtrag F3-24: „Festgehalten in `…test_invalid_namespace_exits_78` (ohne `entscheidung`-Marker)“. Der Test trägt den Marker inzwischen und steht unter `tests:`.
   - E-018, Nachtrag (3): `test_exhausted_budget_still_gives_each_step_the_floor` „(ohne `entscheidung`-Marker)“. Auch er ist jetzt markiert und gelistet.
   - Beides sind reine Textkorrekturen im Pack und gehören in den ersten Register-Commit des Plans.
8. **Offene Testlücken im Register, nicht Teil der Fixes:**
   - E-046, Nachtrag F3-27: Der Test „Absturzschleife endet über `execution_guard_max_orphans`“ fehlt. Das Befundregister (F3-27) verlangt ihn.
   - E-071, `ohne_test`: zwei Kandidaten ohne Marker.
   - Beide sind Kandidaten für eine eigene kleine Unit.
9. **Statustabelle im Befundregister ist veraltet.** Abschnitt „Nachweis-Tests“ führt alle Dateien als „offen“, obwohl sie seit `968ff7f` existieren. F3-31 fehlt in der Tabelle, sein Nachweis existiert aber.
10. **E-065 „Bewusste Folgen“:** Ein `ValueError` beim Import endet nach E-057 mit Exit 1, nicht 78. Der F3-08-Fix (Cron ohne nächsten Termin → `ValueError` beim Klassenbau) ändert also den Exitcode nicht. Den Testfall so formulieren, damit er nicht gegen E-057 läuft.

---

## 2. Bestehende Entscheidungstests je Befund

„Grün“ heißt: Der Test bleibt grün, wenn der Fix die Register-Entscheidung umsetzt. Liegt ein Risiko in der Fix-Form, steht die Bedingung dabei.

| Befund | Betroffene Tests mit Marker | Bleibt grün? |
|---|---|---|
| F3-01 | E-066: `test_schedule.py::TestClientUniqueDeferred::*` (3); E-033: `test_deferred_explicit_none_persists_the_opt_out_as_cli_unique_without_hash`; E-032: `test_deferred_without_registry_and_unique_for_stays_cli` | Ja. Die Idempotenz gilt laut E-058 nur in `ScheduleStore.add` für Oneshots. `swarmq schedule add` meldet weiter „already exists“. Diese Tests laufen gegen Fakes und prüfen die Unique-Logik. |
| F3-02 | E-059: `test_pipeline.py::test_workflow_step_publisher_runs_middleware_then_signals`; E-066 (3), E-033 (3 in `test_schedule.py`), E-032 (`test_schedule.py:1045`, `test_fire.py:134`); falls `scheduler/fire.py` geändert wird: E-011 (8 in `test_fire.py`, `test_befund_bz02_…`), E-004 (5 in `test_fire_unroutable.py`), E-033 `test_fire.py:397`, E-011 `test_reports.py::…test_fingerprint_without_unique_for_is_unchanged_since_0_11` | Ja, mit Bedingungen. `pre_enqueue` läuft vor dem Claim; die E-066-Tests haben keine Middleware. Gespeicherte Header gehören in ein neues Feld am Ende von `ScheduleDefinition`, mit Default. Dieses Feld darf nicht in `code_fields()` eingehen (`scheduler/reports.py:40-49`), sonst bricht der E-011-Abdruck-Test. `_fire_one` liegt an der Komplexitätsgrenze (Abschnitt 6). |
| F3-03 | E-060: `test_retry.py` (3); E-042: `test_publish_connection_loss.py` (3), `test_publish_channel_closing.py:414`; E-026 Delay-Stufen | Ja. Das Klemmen auf 0 ändert weder den Vorrang von `Retry` noch die Zählung. `delay_ms=0` muss eine gültige Delay-Stufe treffen; E-026-Tests vor dem Commit laufen lassen. |
| F3-04 | E-063: `test_config_lock_wait.py::test_must_be_finite`, `test_config_stepdown.py::test_negative_or_infinite_is_rejected`, `test_logging_settings.py::…drain_timeout…` | Ja. Unmarkierte Tests mit `inf`: `test_config.py:857` (erwartet schon `ConfigurationError`), sonst keine Konfig-Tests, die `inf` als „unbegrenzt“ nutzen. |
| F3-05 | E-057: `test_entry.py::TestConfigErrorExitCode::test_import_error_in_the_child_is_a_configuration_error` (`ModuleNotFoundError`), `test_cli.py::TestMainErrorRendering::test_module_not_found_exits_with_the_config_code` | Ja, denn `ModuleNotFoundError` ist eine Teilklasse von `ImportError`. Bedingung aus E-057: nur Import/Syntax **des Task-Moduls**. Ein pauschales `except ImportError` in `entry.py:206` würde auch spätere `ImportError` als 78 werten. Besser an der Importstelle in `ConfigurationError` hüllen (KTD8: „hüllt jeden ImportError … in ConfigurationError“). |
| F3-07 | E-056: `test_task.py::test_taskinfo_unique_for_zero_raises`; E-033: `test_schedule_per_call_override_none_disables_dedup`, `test_schedule_many_batch_override_none_disables_dedup`, `test_deferred_explicit_none_persists_…` | Ja. `None` schaltet weiter ab, `_MISSING` bleibt Default (Pattern 10). Abgelehnt werden nur Zahlen `<= 0`. |
| F3-08 | E-065: `test_task.py` (4 Cron-Tests), `test_reports.py::TestAdoptReportedCrons::test_invalid_report_is_not_adopted_and_logged` | Ja. Die Prüfung beim Leader bleibt als Schutz gegen ältere Worker. F3-35 (6/7 Felder) nicht mitentscheiden: Die neue Prüfung darf nur „kein nächster Termin“ ablehnen. |
| F3-09 | E-030: `test_befund_fs15_result_wait_polling.py` (6), `test_valkey_results.py::…test_pubsub_miss_falls_back_to_polling` | Ja. Die Tests betreffen `wait_for_result` im Backend, der Fix nur `client/results.py` (`get_results`). |
| F3-11 | keine | – |
| F3-12 | E-022: `test_lock_keeper.py` (4), Contract `tests/contracts/coordination.py` (3), `test_befund_bz25_lock_lease.py`; E-018: `test_befund_bz18_cleanup_after_grace.py::test_locks_and_leader_are_released_within_the_grace_period`, `test_shutdown_budget_steps.py` (4); E-034: `test_worker.py::test_start_mode_releases_at_pickup`, `…rate_limited_keeps_flag_for_redelivery`; E-070: `test_pipeline.py::test_gates_run_in_fixed_order_and_ack_comes_last` | Ja, solange Keeper-Semantik (Verlust einmal melden, `drop` beendet Lease), die Reihenfolge nach E-071 und die Gate-Reihenfolge unverändert bleiben. Die Untergrenze je Schritt (E-018 Nachtrag (3)) nicht verkürzen. |
| F3-13 | E-018: alle 8 Tests; E-038: `test_recycling.py`, `test_befund_fs30_recycling_drain.py` | Ja. Nur `_heartbeat_deadline` (`restart_budget.py:54-58`) für `INTENT_RECYCLE`. Der Scale-down-Drain (`INTENT_DRAINING`) und `shutdown_budget()` bleiben, also auch `test_supervised_default_budget` und `test_default_shutdown_fits_into_kubernetes_termination_grace`. Kein anderer Test prüft `worker_hung_during_drain`. |
| F3-14 | E-061: `test_slots.py::test_effective_concurrency_cpu_uses_assigned_cpus`, `…_explicit_wins_for_every_pool` | Ja. Unmarkiert: `test_config.py:18` (`concurrency == 10` bei io) und `test_cli_multiprocess.py:172` bleiben richtig. Muster für „nicht gesetzt“: `_apply_pool_defaults` wie bei `recycle_rss_mb` (`test_config.py:658-666`). |
| F3-15 | E-044: `test_task.py::TestUpdateProgress::*` (4) | Ja, mit Bedingung. Die markierten Tests setzen `t._task_start_time = _time.monotonic()` direkt (`test_task.py` um Zeile 702, 719, 757, 777). Wird `_task_start_time` auf die Wanduhr umgedeutet oder umbenannt, ändern sich Laufzeit und Throttle-Grundlage in diesen Tests. Empfehlung: `_task_start_time` monoton lassen und ein **zweites** Feld für die Wanduhr einführen (E-044 Nachtrag: „für `elapsed`/`eta_seconds` darf daneben die monotone Uhr laufen“). |
| F3-16 | E-023: `test_backoff.py::test_fixed_strategy_returns_base_delay` (base 5, max 3600), 3 Tests in `test_befund_bz26_backoff.py` | Ja, `min(5, 3600) == 5`. **Aber:** Der unmarkierte Property-Test `tests/property/test_backoff_properties.py::test_fixed_ignores_attempt_and_cap` pinnt das alte Verhalten („intentionally un-capped“, `d == base` bei `_BASE` bis 1e3 und `_MAX` ab 0). Er wird nach dem Fix rot und muss angepasst werden (`d == min(base, max_delay)`). Er ist unmarkiert, also erlaubt; er läuft nur in `uv run just property`, nicht in `just unit`. |
| F3-17 | E-062: `test_cli_multiprocess.py::TestMainRouting::test_check_argparse_error_emits_json`, `test_port_guard.py::TestPreflightCommandAgrees` (4, Klassenmarker) | Ja, wenn `--check` beim Bau von `SwarmQConfig` fehlende URLs nicht als Fehler meldet (E-062). `config_validation.py:85-90` verlangt nicht-leere URLs. Also einen Platzhalter einsetzen (`config.INJECTED_URL`) oder die URL-Prüfung umgehen. |
| F3-18 | E-006: 10 Tests (`test_cli_env_check.py` 5, `test_cli_env_check_queues.py` 2, `test_valkey_environment.py`, `test_befund_bz04_…`, Integration `test_unreachable_broker_exits_1`); E-005: `test_config_valkey_eviction.py::test_default_is_strict`; E-012: `test_cli_env_check_namespace.py::test_invalid_namespace_exits_78` | Ja, mit Bedingung. Die Fixture `fake` in `test_cli_env_check.py:80-86` setzt nur `SWARMQ_BACKEND_URL`. Baut der Fix ein `SwarmQConfig` mit leerer Broker-URL, endet jeder dieser E-006-Tests mit 78. Gleiche Lösung wie F3-17 (Platzhalter). Die Nachsicht von `env_valkey_eviction_check()` für Producer und CLI bleibt (E-006 Nachtrag). |
| F3-19 | E-062 (wie F3-17) | Ja. Exit 1 bleibt, nur der Payload bekommt die fehlenden Schlüssel. |
| F3-31 | E-056 (`test_taskinfo_unique_for_zero_raises`), E-011 (Abdruck-Test) | Ja für E-056 (Reihenfolge siehe Unstimmigkeit 4). Zu E-011 siehe Mischbetrieb in Abschnitt 3. |
| F3-20/F3-22/F3-21 (Doku) | E-068 (3), E-029 (4), E-004 (6) | Ja, kein Codepfad betroffen. Nur `agent_spec.py` (Text) und die erzeugte Spec ändern sich. |

---

## 3. Relevante Learnings aus `docs/solutions/`

### 3.1 Feste Testports und Testsperre
**Datei**: `docs/solutions/tooling-decisions/feste-testports-und-testsperre-2026-10-14.md` (severity high)
- Integration und e2e nur über `uv run just integration|e2e|all` (Ports 5699/6399, `flock` über `scripts/with-test-lock.sh`). `tests/support/services.py` verweigert 5672/6379.
- Massenhaft Timeouts: erst `pgrep -af pytest` und `rabbitmq-diagnostics alarms`, dann die Tests verdächtigen.
- **Konsequenz:** F3-01 (Integration) und ein neuer F3-03-Integrationstest laufen nur unter der Sperre. Parallele Units committen Unit-Gates selbst, Integration gesammelt.

### 3.2 Unique-Tasks-Muster (R3.5)
**Datei**: `docs/solutions/patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md`
- Pattern 2: Ein Claim vor einem fehlbaren Schritt (Publish, `pre_enqueue`, `add_schedule`) wird bei Fehler zurückgegeben. **F3-02:** E-059 dreht die Reihenfolge, also `pre_enqueue` vor dem Claim. Dann braucht ein Veto keinen Rollback mehr. Ein Fehler beim Speichern nach dem Claim braucht ihn weiterhin.
- Pattern 6: Deferred-TTL `unique_for + ceil(fire_offset)`. **F3-31/E-055:** Die Normalisierung (`ceil`) muss vor dieser Arithmetik liegen, damit `px` ganzzahlig bleibt.
- Pattern 7: `msgspec.Struct` nur am Ende mit Default erweitern, keine neue `Literal`-Variante in `source`. **F3-02:** Die gespeicherten Middleware-Header bekommen ein neues Feld am Ende von `ScheduleDefinition`. `ScheduleDefinition` ist `msgspec.Struct, kw_only=True, frozen=True` ohne `forbid_unknown_fields` (`scheduler/definition.py:21`, msgspec 0.21.1). Ein alter Leader ignoriert das Feld also ohne Fehler, aber er publiziert dann ohne die Middleware-Header. Das ist eine Rollout-Grenze wie bei FS-14 („Producer und Leader auf 0.12.3“), kein Bruch.
- Pattern 10: `_MISSING`-Sentinel für „nicht übergeben“. **F3-07, F3-14:** Für `concurrency` „nicht gesetzt“ ein Sentinel oder `None` mit Auflösung in `_apply_pool_defaults`.

### 3.3 Invarianten der Worker-Pipeline
**Datei**: `docs/solutions/patterns/invarianten-der-worker-pipeline-2026-08.md`
- Nur Runner und Ergebnis-Tail sprechen die vier Broker-Verben. Terminal-Zustände sind ein Summentyp (`worker/outcome.py`). Locks gibt nur der Besitzer per Token frei.
- **Konsequenz F3-12:** Die doppelte Freigabe ist eine Owner-/Token-Frage zwischen `_release_leftover_locks` (Shutdown) und dem `finally` des Handlers (`pipeline.py:711-712`). Lösung über Entnahme genau einmal, also wer das Token aus der Tabelle nimmt, gibt frei. Muster: `f21459f` („Shutdown entnimmt Lock-Einträge vor der Freigabe“). Kein neues Flag.

### 3.4 Cancel-sicherer Teardown und DI-Finalizer
**Dateien**: `docs/solutions/patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md`, `docs/solutions/architecture-patterns/dishka-di-integration-async-task-queue.md` (Zeile 99, 119)
- Ein Shutdown-Cancel kann im `finally` des Handlers landen. Freigaben laufen als getrackte, geshieldete Tasks, die der Shutdown abwartet („SwarmQ already uses this exact pattern for lock release“).
- **Konsequenz F3-12:** Genau an dieser Nahtstelle entsteht die doppelte Freigabe. Der Nachweis ist event-gesteuert (AGENTS.md: kein `asyncio.sleep`).
- **Konsequenz F3-28 (Doku):** Die Begründung für „aufräumen im `yield`-Finalizer“ steht in Zeile 99 (Exception wird an den Finalizer weitergereicht). Die Doku-Korrektur kann darauf verweisen.

### 3.5 Ack-Reihenfolge und DLX
**Dateien**: `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md`, `docs/solutions/logic-errors/nack-requeue-false-routes-via-dlx-not-data-loss-2026-04-16.md`
- Rule 2: Entscheidung und Übergabe eines Retry-Pfads komplett in `try/except` wie `_evaluate_retry`. `nack(requeue=True)` bei dauerhaftem Fehler ist eine Endlosschleife.
- **Konsequenz F3-03:** Der Rückfall `redeliver_in_place` nach `PRECONDITION_FAILED` ist genau diese Schleife ohne `max_retries`. Das Klemmen vor dem Publish beseitigt den Auslöser; den Rückfallpfad nicht umbauen (E-060, E-042).

### 3.6 Schedfire-Dedup und Deckel
**Datei**: `docs/solutions/logic-errors/schedfire-retry-dedup-collision-silent-drop-2026-06-02.md`
- Der Completed-Marker gilt `scheduler_dedup_ttl`. Ein Retry, der länger wartet, kollidiert mit ihm.
- **Konsequenz F3-16:** Nur mit `min(base_delay, max_delay)` hält die Invariante `scheduler_dedup_ttl >= retry_max_delay` (`config_validation.py:212-218`) auch für `fixed`. Das ist der eigentliche Nutzen des Fixes und gehört in den Changelog.

### 3.7 Lock-TTL und Lease
**Datei**: `docs/solutions/logic-errors/lock-ttl-shorter-than-hard-timeout-duplicate-execution-2026-06-02.md`
- Die Lösung damals: statische TTL `hard_timeout + 30 s`, „kein Renewal“. Das Learning ist teilweise überholt: E-022 führt Lease-Erneuerung ohne Fencing ein. Für F3-12 nur als Hintergrund lesen: TTL-Heilung ist das letzte Netz (E-018 Nachtrag (3), E-071).

### 3.8 uv.lock-Drift
**Datei**: `docs/solutions/tooling-decisions/uv-lock-drift-from-pyproject-optional-extras.md`
- Jede Änderung an `pyproject.toml` (auch die Version) bekommt `uv lock` und `uv lock --check` im selben Commit. `UV_FROZEN` in der CI fängt das nicht.
- **Konsequenz:** Release-Commit 0.12.3 ändert `pyproject.toml` und `uv.lock` gemeinsam (wie `c24cbf5`).

### 3.9 Messen vor dem Planen
**Datei**: `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md`
- **Konsequenz:** Für F3-12 („bei CPU gelegentlich“) und F3-13 keine Annahmen über Häufigkeit in den Plan schreiben. Die Nachweise sind deterministisch; das genügt.

### 3.10 Was es in `docs/solutions/` nicht gibt
Für folgende Themen gibt es kein Learning. Die Quellen liegen woanders:
- **Architektur-Guard 800 Zeilen:** `tests/unit/test_architecture_guards.py:37` (`_MAX_MODULE_LINES = 800`, `_LOC_EXCEPTIONS` leer seit U21, KTD7 Plan 0.2.0). Siehe Abschnitt 6.
- **Transport-Retry-Idempotenz:** `docs/befunde/fehlersuche-runde-2-recherche/2-absturz.md:120-137` (K-3: „alle Lua-Skripte und NX-Befehle gegen Doppelausführung mit gleichem Owner prüfen“). Vorlage im Code: `d7d94c5` (idempotenter Claim, Lua und In-Memory, Contract-Test). Die Empfehlung aus Runde 2, den Toxiproxy-Spike (`docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md`) um „Retry-Politik setzt idempotente Skripte voraus“ zu ergänzen, ist nicht umgesetzt (`fix-r-learnings.md` Runde 2, Empfehlung 7). F3-01 ist der nächste Fall dieser Klasse. Nach dem Fix lohnt ein Learning per `/ce-compound`.
- **Mischbetrieb/compat:** `tests/compat/_harness.py` (Basen 0.11.0, 0.11.1, 0.12.0, nur von PyPI per `uv pip install -c constraints-<v>.txt`). `pytest -m compat` ist opt-in (`pyproject.toml:150,164`) und nicht Teil von `just all`. **Konsequenz:** Für „rolling-deploy-sicher neben 0.12.0–0.12.2“ braucht die Suite eine Basis 0.12.2. Das geht nur, wenn 0.12.2 vorher auf PyPI liegt (dann `constraints-0.12.2.txt` nach dem Muster in `constraints-0.12.0.txt` erzeugen). Sonst muss der Harness eine Git-Tag-Installation lernen. compat-Fälle braucht F3-02 (neues Definitionsfeld: 0.12.2-Leader liest 0.12.3-Definition, Rollback) und F3-01 (nur Fire-Index, keine Formatänderung).
- **Mischbetrieb F3-31 (Cron-Abdruck):** `code_fields()` nimmt `unique_for` in den Abdruck auf, sobald es gesetzt ist (`scheduler/reports.py:46-48`). `msgspec.json.encode(3600.0)` ergibt `3600.0`, `3600` ergibt `3600`. Ein 0.12.2-Worker mit `unique_for=3600.0` und ein 0.12.3-Worker (normalisiert) melden verschiedene Abdrücke für denselben Cron. Der Leader sieht dann einen Konflikt, bis alle Worker neu sind. Ob ein solcher Float-Cron unter 0.12.2 überhaupt übernommen wird (Befundtext F3-31: „ein Cron mit `unique_for=3600.0` wird vom Leader nie übernommen“), ändert nur das Ausmaß. In die Upgrade-Hinweise aufnehmen.
- **Changelog-Konvention:** Keep a Changelog, EN `website/reference/changelog.md` und DE `website/reference/changelog.de.md`. Abschnitt `## [0.12.3] — unreleased` / `— unveröffentlicht` mit Unterabschnitten `### Upgrade notes` / `### Upgrade-Hinweise` und `### Fixed` / `### Behoben` (Muster: 0.12.2-Entwurf, Zeile 7ff.). Es gibt keinen Test für EN/DE-Gleichstand; das ist ein Review-Punkt. Verhaltensänderungen, die in die Upgrade-Hinweise gehören:
  - F3-04: `inf` in Fristfeldern ist jetzt Exit 78 beim Start.
  - F3-05: `ImportError`/`SyntaxError` ergeben 78 und keine Neustarts mehr.
  - F3-14: Eingebettete CPU-Pools nehmen weniger Slots.
  - F3-16: `fixed` wird gedeckelt.
  - F3-17/F3-18: `--check` und `check` liefern andere Exitcodes.
  - F3-02: `TASK_ENQUEUED` feuert jetzt auch bei `delay`/`eta`, Veto wirkt, Header werden gespeichert; der Leader muss 0.12.3 sein.
  - F3-31: Floats werden angenommen und aufgerundet.
- **Spec-Generierung:** `python -m swarmq.agent_spec` erzeugt `website/reference/agent/spec/*.json` und `llms.txt`. Der Guard ist `tests/unit/test_agent_spec.py::test_committed_spec_files_match_generators`; die Versionszeile in `index.json` ist ausgenommen. **Konsequenz:** F3-20 ändert `agent_spec.py:247-252` (`TaskCancelledError`: `when` ohne `run`) und erzeugt die Spec im selben Commit neu. `error-catalog-machine.md` (EN/DE) ist handgeschrieben; F3-23 bekommt dafür einen neuen Sync-Test.

---

## 4. Ablauf „Befund behoben“ nach `docs/befunde/README.md`

Die README beschreibt nur Aufnahme und Einstufung, keinen Schritt „behoben“. Verbindlich sind die Sätze im Register selbst und die Praxis von Runde 2:

1. Befundregister Runde 3, Zeile 25: „Wird ein Befund behoben, wird sein Test XPASS (rot), und die Markierung muss entfernt werden.“ Gemeint ist `xfail`; `befund` bleibt (Abschnitt 0).
2. Registertabelle „Nachweis-Tests“: Spalte Status auf `behoben (Unit, Commit; Review Commit)`. Vorlage: `fehlersuche-runde-2-2026-10.md:95-106`, etwa `behoben (U3, 7a2bb19; Review f79fc04, 1f2f1eb)`. Grenzen gehören dazu, etwa „behoben für Worker-Kopien … Grenze: … (xfail)“ (FS-10).
3. Statuszeile oben fortschreiben (Vorlage Runde 2, Zeile 4: „Stand …: … behoben: FS-…“). In Runde 3 steht heute „Nichts behoben.“
4. Doku-Befunde ohne Nachweis (F3-06, F3-20 bis F3-25) und die Doku-Korrekturen zu F3-28/29/30, F3-33, F3-37: in die Tabelle mit Status und Commit aufnehmen. Heute fehlen sie dort.
5. Regel: `tests:`/`ohne_test` wie in Abschnitt 0, Nachtrag-Text auf „behoben“.
6. Abschluss beim Release (Runde-2-Plan U21, Schritt 4): alle behobenen Befunde auf „behoben (MR, Commit)“. `pytest -m befund --runxfail` zeigt danach nur noch offene Nachweise. In Runde 3 gibt es nach dem Fix keine offenen FEHLER-Nachweise mehr; LÜCKEN haben keine.
7. Kein Nachweis darf einem `entscheidung`-Test widersprechen (README, letzter Absatz). Das gilt nach dem Fix weiter, weil der Nachweis selbst den Marker bekommt.

---

## 5. Was Runde 2 gelernt hat: Review-Ablauf und Release

### 5.1 Review je Unit
- **Der Plan beschreibt keinen Review-Schritt.** `docs/plans/2026-10-14-0748-fix-fehlersuche-runde-2-plan.md` nennt `correctness`, `reliability`, `testing` oder `learnings` nirgends. Er hat nur das Execution Profile („Strenges TDD je Unit“, `:24`), Stop-Bedingungen (`:18-23`), den Verification Contract (`:704-716`) und die Definition of Done (`:718-726`).
- Das Review kam aus Georgs Auftrag vom 2026-10-14 („nach jedem schritt einen review und fixe die dort gefundenen fehler“). Umgesetzt wurde es als `ce-code-review` je Unit:
  - Branch `review/uN`, Scope `branch-remote`, Dateien nur per `git show <head-ref>:<pfad>`, weil parallele Worker im Worktree arbeiten.
  - Personas über einen Launcher. Verfügbar sind unter anderem `correctness-reviewer`, `reliability-reviewer`, `testing-reviewer`, `project-standards-reviewer`, `learnings-researcher` und `adversarial-reviewer` (`…/ce-code-review/references/personas/`).
  - Findings auf Deutsch; Fixes als eigene Commits `fix(review): … (Un)` bzw. `test(review): …`, etwa `f79fc04`, `44722f6`, `0a78c17`, `21a1d46`.
- **Gelernte Lektion** (Fortschritt 2026-10-15): In Runde 2 lief je Unit kein Learnings-/Pack-Reviewer. Ergebnis war ein eigener Nachgang: Pack-Review 0.12.2 mit neuen Regeln E-050 bis E-054 und Nachträgen (`faf6638`). **Konsequenz:** Im Runde-3-Plan je Unit ausdrücklich vier Linsen festlegen: correctness, reliability, testing, learnings mit Pack `swarmq-entscheidungen`. Die Learnings-Linse prüft dabei `tests:`-Liste gegen Marker (Lücke des Guards), veraltete `ohne_test`-Texte und Nachträge.
- Weitere Lehren aus der Runde-2-Durchführung:
  - Commits während paralleler Worker mit `--no-verify`, weil `prek` sonst unstaged Dateien anderer Worker stasht; die Gates laufen vorher manuell.
  - Ein gemeinsamer Unit-Lauf nach drei parallel fertigen Units meldete 19 Fehler durch vermischte Fixtures. Daher vor dem Commit jede Unit einzeln prüfen (Commit-Gate: Ergebnis ausdrücklich prüfen).
  - `mypy` wie die CI: `uv run just types` (mypy über `src/swarmq tests`, basedpyright, stubtest), nicht nur `uv run mypy`.

### 5.2 Release-Ablauf eines Patch-Releases (Fundstellen)
- Runde-2-Plan U7 (`:439-451`) und U21 (`:688-702`):
  1. Release-Branch `release/0.12.x`, alle Gates aus dem Verification Contract.
  2. rc-Tag `v0.12.3rc1` nach Georgs Ok pushen; die Loadsuite pinnt per Git-Tag.
  3. Loadsuite 1 h und 6 h auf dem rc-Tag, Mischbetrieb-Suite.
  4. Nach der Abnahme: Merge, finales Tag `v0.12.3` auf dem Merge-Commit, PyPI-Upload durch Georg.
  5. Befundregister auf „behoben (MR, Commit)“.
- Release-Commit (Vorlage `c24cbf5 chore(release): Version 0.12.1rc1`, `baf0e22 chore(release): Version 0.12.0`): ändert `pyproject.toml` (`version`), `uv.lock`, `website/reference/agent/spec/index.json` (Spec neu erzeugt), `changelog.md` und `changelog.de.md`. `baf0e22` nennt als Gates zusätzlich compat, chaos und `mkdocs --strict`.
- PyPI (Memory `pypi-release-status.md`): `uv build`, Upload führt Georg per `!`-Befehl in **einer** Zeile aus (`UV_PUBLISH_TOKEN=… uv publish dist/swarmq-X-py3-none-any.whl dist/swarmq-X.tar.gz`). Der Classifier blockiert `uv publish` und das Token-Lesen durch Claude.
- Push, MR und Merge laut Runde-2-Plan „erst nach Georgs Ok“ (`:25`).
- **Reihenfolge-Risiko:** `pyproject.toml` steht auf `0.12.1rc1`. 0.12.1 final und 0.12.2 sind weder getaggt noch auf PyPI (Tags: `v0.12.1rc1`, `v0.12.0`). 0.12.3 setzt auf einem unveröffentlichten 0.12.2 auf. Der Plan braucht eine Abhängigkeit: erst 0.12.2 veröffentlichen, dann compat-Basis 0.12.2 (Abschnitt 3.10), dann 0.12.3.

---

## 6. Architektur-Guard und Größengrenzen

**Guard:** `tests/unit/test_architecture_guards.py:37` `_MAX_MODULE_LINES = 800`. `test_no_module_exceeds_line_limit_outside_exception_list` zählt physische Zeilen (`_architecture.py:43-45`). `_LOC_EXCEPTIONS` ist leer („Seit U21 leer (KTD7)“). Eine neue Ausnahme braucht einen Eintrag „Modul → Unit, die es unter das Limit bringt“. Der Guard meldet auch veraltete Einträge.

**Weitere Größenmelder** (ruff, `pyproject.toml:297-306`, gelten nur für `src/`): `max-statements = 60` (PLR0915), `max-complexity = 12` (C901). Gemessen mit abgesenkten Schwellen (10/50):

| Datei | Zeilen (Grenze 800) | Funktion an der Grenze | wahrscheinlich berührt von |
|---|---|---|---|
| `src/swarmq/worker/pipeline.py` | **797** | Funktion ab `:578`: 58 Anweisungen (Grenze 60) | F3-12 (`finally` `:704-713`). Jede neue Zeile braucht Auslagerung, z. B. die Freigabe-Entnahme in `worker/shutdown.py` oder ein eigenes Modul (Lock-Tabelle) |
| `src/swarmq/client/schedule.py` | 668 | – | F3-01, F3-02, F3-07, F3-31; F3-02 braucht vermutlich die meisten Zeilen |
| `src/swarmq/config.py` | 674 | – | F3-14, F3-31 |
| `src/swarmq/config_validation.py` | 251 | `_validate_core` Komplexität **12** (= Grenze) | F3-04: Endlichkeit nicht in `_validate_core` verzweigen, sondern `_validate_finite` (`:163`) erweitern |
| `src/swarmq/task.py` | 504 | – | F3-08, F3-15, F3-31 |
| `src/swarmq/cli/check_cmd.py` | 309 | – | F3-17, F3-19 |
| `src/swarmq/process_manager/entry.py` | 226 | `worker_subprocess_entry`: **60 Anweisungen** (= Grenze), Komplexität 11 | F3-05: nur das `except`-Tupel erweitern (0 Anweisungen) oder an der Importstelle hüllen; keine neue Anweisung in dieser Funktion |
| `src/swarmq/worker/shutdown.py` | 538 | – | F3-12 |
| `src/swarmq/backend/valkey/schedules.py` | 528 | – | F3-01 (`add`, `:120-127`) |
| `src/swarmq/scheduler/fire.py` | – | `_fire_one` (`:83`) Komplexität **12** (= Grenze) | F3-02 (Leader publiziert gespeicherte Header): in einen Helfer auslagern |
| `src/swarmq/process_manager/restart_budget.py` | 208 | – | F3-13 (`_heartbeat_deadline`, `:54-58`) |
| `src/swarmq/agent_spec.py` | 744 | – | F3-20 (nur Text) |
| `src/swarmq/client/results.py` | 248 | – | F3-09 |
| `src/swarmq/backoff.py` | 90 | – | F3-16 |

Weitere Module über 700 Zeilen, voraussichtlich nicht berührt: `logging_file.py` 771, `logging_queue.py` 760.

---

## Empfehlungen für den Plan

1. **Erste Unit „Register-Vorbereitung“** auf `580ef3c`:
   - Nachtrag-Texte E-012/E-018 berichtigen.
   - E-055-Textfehler (`SwarmQConfig.unique_for`) melden, nicht still ändern.
   - Statustabelle im Befundregister nachziehen (F3-31 aufnehmen, Dateien existieren).
   - F3-03-Integrationsnachweis entscheiden (schreiben oder streichen).
   - Nachtrag E-071 für F3-12 vorbereiten.
2. **Pro Fix-Unit derselbe Dreiklang:** `xfail` weg, `entscheidung` dazu, `tests:` ergänzen, `ohne_test` kürzen, Registerstatus. Review mit den Linsen correctness, reliability, testing und learnings (Pack).
3. **Unmarkierte Tests mit alter Prämisse** dürfen angepasst werden. Bisher gefunden: `tests/property/test_backoff_properties.py::test_fixed_ignores_attempt_and_cap` (F3-16). Weitere Suche je Unit mit `grep` auf das alte Verhalten.
4. **Fix-Formen, die Entscheidungstests sonst brechen würden:**
   - F3-17/F3-18: URL-Platzhalter, nicht leere URL validieren (E-062, E-006).
   - F3-15: `_task_start_time` monoton lassen (E-044-Tests).
   - F3-02: neues Feld nicht in `code_fields()` (E-011).
   - F3-05: Hüllung an der Importstelle (E-057-Wortlaut).
5. **Größe:**
   - F3-12 lagert aus `pipeline.py` aus.
   - F3-05 fügt in `worker_subprocess_entry` keine Anweisung hinzu.
   - F3-04 erweitert `_validate_finite`, nicht `_validate_core`.
   - F3-02 fügt in `_fire_one` keine Verzweigung hinzu.
6. **Mischbetrieb:**
   - compat-Basis 0.12.2 setzt das Release von 0.12.2 voraus.
   - Upgrade-Hinweise für F3-02 („Leader und Producer auf 0.12.3“) und F3-31 (Abdruck-Konflikt während des Rollouts).
7. **Nach den Fixes per `/ce-compound`:** Learning „Transport-Retry und NX-Befehle“ (F3-01 nach FS-06), Ergänzung im Toxiproxy-Spike, und das Lock-TTL-Learning als teilweise überholt (E-022) markieren.
