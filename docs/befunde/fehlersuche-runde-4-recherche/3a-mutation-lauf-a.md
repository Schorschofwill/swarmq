# Mutation Lauf A (Spur M, U3): Unit-bewertet, Welle 2

Plan: `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md`, R14–R17, KTD4, KTD9, Einheit U3 (Approach Schritt 3). Stand: swarmq 0.12.3, Branch `test/fehlersuche-runde-4`. Grundlage: lokale Kopie `mutants-a/` (nicht committet) mit `ergebnisse.txt` (Ausgabe von `mutmut results --all true`) und der mutmut-Datenbank `mutants/**/*.meta`.

Dieser Bericht enthält Kennzahlen und einen Vorfilter, keine Verdachte. Die Sichtung der Überlebenden folgt in U5 (`3c-sichtung-{D,E,F}.md`).

## Ergebnis in Kürze

- 9714 Mutanten: 7075 getötet (72,8 %), 2302 überlebt, 314 ohne Test, 18 timeout, 4 segfault, 1 „check was interrupted by user“. Die Zählung aus `ergebnisse.txt` stimmt mit der Datenbank überein.
- Vorfilter (vier Regeln, AST-basiert): von 2302 Überlebenden bleiben **1703** (D 183, E 328, F 1192), von 314 ohne Test bleiben 300. Aussortiert: 599 Mutanten, nicht gelöscht, sondern in `mut-gruppe-{D,E,F}-gefiltert.txt`.
- **Die CLI-Begrenzung aus der Open Question greift:** 1703 > 1500. Mit Begrenzung auf `parser_ops.py`, `dlq_cmd.py`, `schedule_cmd.py`, `check_cmd.py` bleiben 1377 Überlebende (mit „ohne Test“: 1586).
- Die 4 „segfault“ und 4 der 18 „timeout“ sind keine Abstürze bzw. Zeitüberschreitungen im engeren Sinn: mutmut ordnet Exit-Codes pauschal zu (SIGKILL = segfault, 255 = timeout). Der eine „interrupted by user“ ist ein Test, der unter dem Mutanten das echte `os.execvpe` erreicht und die Testsitzung durch die swarmq-CLI ersetzt.
- Schwach getestet (Score unter 50 %): `cli/inspect_cmd.py` 19 %, `process_manager/worker_target.py` 34 %, `cli/schedule_cmd.py` 38 %, `backend/valkey/execution_guard.py` 40 %, `backend/valkey/chords.py` 0 % (kein Unit-Test).

## Rahmen

| Punkt | Wert |
|---|---|
| Ort | Lastserver über Docker-Kontext `lastserver`, Compose-Projekt `swarmq-mutation` (`docker/mutation/`) |
| Container | 3 CPUs, 8 GB (`docker/mutation/compose.yml`), Abbild `ghcr.io/astral-sh/uv:python3.14-bookworm` mit `uv sync --frozen --extra test --extra reload` |
| Kinder | `mutmut run --max-children 3` |
| Dauer | 2026-10-17 10:41–11:57 (76 min, rund 128 Mutanten/min) |
| Testauswahl | `pytest_add_cli_args_test_selection = ["tests/unit"]`, vom Entrypoint für `MUTMUT_LAUF=a` gesetzt |
| mutmut | 3.8.0 |
| Prozess-Isolation | `process_isolation = "forkserver"`, `forkserver_warmup = "none"` |
| Zeitlimit | mutmut-Standard: `timeout_multiplier` 15; Wand-Zeitlimit je Mutant = halbes CPU-Limit, dann SIGXCPU; harte CPU-Grenze 1 s später (SIGKILL) |

Konfiguration aus `[tool.mutmut]` in `pyproject.toml`:

- `source_paths`: `backend/valkey/{__init__, chords, connection, control, environment, execution_guard, locks, rate_limit, script_pipeline}.py`, `broker/rabbitmq/{__init__, admin, arg_conflict, blocked, connection, consumer_watch, declarations, header_guard, redeclare, topology}.py`, `client/workflows.py`, `client/results.py`, `process_manager/`, `cli/` (R14; ohne `valkey/schedules.py`, `valkey/results.py`, `rabbitmq/delivery.py`, `rabbitmq/publish_phase.py`).
- `pytest_add_cli_args`: `-p no:randomly -q --no-header`, abgewählt `test_architecture_guards.py`, `tests/unit/typing`, `test_benchmark_guard.py`, `test_logging_architecture.py`, `TestStartupAbortLeavesNoScheduler`, `test_second_start_does_not_start_a_second_report_loop`, `TestPublicSurfaceEquality::test_no_symbols_added` (sieht die Trampolin-Namen, die mutmut in `swarmq.cli` einfügt).
- `also_copy`: `tests`, `typings`, `src/swarmq`, `website`, `docs`, `scripts`, `benchmarks`, `overrides`, `README.md`, `AGENTS.md`, `mkdocs.yml`, die drei Compose-Dateien, `uv.lock`, `justfile`, `compound-packs`.

Ohne Mutanten (keine Funktionen, nur Re-Exporte): `broker/rabbitmq/__init__.py`, `process_manager/__init__.py`, `cli/__main__.py`.

Gruppen nach U5: **D** = `backend/valkey/`, **E** = `broker/rabbitmq/` + `client/`, **F** = `process_manager/` + `cli/`.

## Kennzahlen

### Gesamt und je Gruppe

| Gruppe | Mutanten | getötet | überlebt | ohne Test | timeout | segfault | sonst | Score |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| D Valkey-Backend | 1113 | 712 | 254 | 146 | 1 | 0 | 0 | 64,0 % |
| E Broker + Client | 2039 | 1554 | 439 | 45 | 1 | 0 | 0 | 76,2 % |
| F Prozessmanager + CLI | 6562 | 4809 | 1609 | 123 | 16 | 4 | 1 | 73,3 % |
| **Summe** | **9714** | **7075** | **2302** | **314** | **18** | **4** | **1** | **72,8 %** |

Score = getötet / Mutanten. Zählt man timeout, segfault und interrupted als erkannt und „ohne Test“ nicht mit, sind es 75,5 %.

### Je Modul

| Gruppe | Modul | Mutanten | getötet | überlebt | ohne Test | timeout | segfault | Score |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| D | `backend/valkey/__init__.py` | 6 | 5 | 1 | 0 | 0 | 0 | 83 % |
| D | `backend/valkey/chords.py` | 42 | 0 | 0 | 42 | 0 | 0 | 0 % |
| D | `backend/valkey/connection.py` | 174 | 130 | 44 | 0 | 0 | 0 | 75 % |
| D | `backend/valkey/control.py` | 113 | 85 | 28 | 0 | 0 | 0 | 75 % |
| D | `backend/valkey/environment.py` | 232 | 167 | 65 | 0 | 0 | 0 | 72 % |
| D | `backend/valkey/execution_guard.py` | 113 | 45 | 17 | 51 | 0 | 0 | 40 % |
| D | `backend/valkey/locks.py` | 248 | 145 | 50 | 53 | 0 | 0 | 58 % |
| D | `backend/valkey/rate_limit.py` | 73 | 44 | 29 | 0 | 0 | 0 | 60 % |
| D | `backend/valkey/script_pipeline.py` | 112 | 91 | 20 | 0 | 1 | 0 | 81 % |
| E | `broker/rabbitmq/admin.py` | 495 | 349 | 128 | 18 | 0 | 0 | 71 % |
| E | `broker/rabbitmq/arg_conflict.py` | 259 | 208 | 51 | 0 | 0 | 0 | 80 % |
| E | `broker/rabbitmq/blocked.py` | 27 | 24 | 3 | 0 | 0 | 0 | 89 % |
| E | `broker/rabbitmq/connection.py` | 367 | 262 | 93 | 12 | 0 | 0 | 71 % |
| E | `broker/rabbitmq/consumer_watch.py` | 222 | 198 | 24 | 0 | 0 | 0 | 89 % |
| E | `broker/rabbitmq/declarations.py` | 282 | 241 | 41 | 0 | 0 | 0 | 85 % |
| E | `broker/rabbitmq/header_guard.py` | 20 | 16 | 4 | 0 | 0 | 0 | 80 % |
| E | `broker/rabbitmq/redeclare.py` | 29 | 19 | 10 | 0 | 0 | 0 | 66 % |
| E | `broker/rabbitmq/topology.py` | 23 | 23 | 0 | 0 | 0 | 0 | 100 % |
| E | `client/results.py` | 119 | 94 | 24 | 0 | 1 | 0 | 79 % |
| E | `client/workflows.py` | 196 | 120 | 61 | 15 | 0 | 0 | 61 % |
| F | `cli/__init__.py` | 219 | 157 | 62 | 0 | 0 | 0 | 72 % |
| F | `cli/_shared.py` | 191 | 150 | 41 | 0 | 0 | 0 | 79 % |
| F | `cli/check_cmd.py` | 437 | 323 | 113 | 0 | 1 | 0 | 74 % |
| F | `cli/dlq_cmd.py` | 358 | 208 | 150 | 0 | 0 | 0 | 58 % |
| F | `cli/env_check_cmd.py` | 732 | 530 | 175 | 27 | 0 | 0 | 72 % |
| F | `cli/inspect_cmd.py` | 144 | 28 | 50 | 66 | 0 | 0 | 19 % |
| F | `cli/parser.py` | 718 | 689 | 29 | 0 | 0 | 0 | 96 % |
| F | `cli/parser_ops.py` | 831 | 810 | 20 | 0 | 1 | 0 | 97 % |
| F | `cli/pool.py` | 75 | 68 | 7 | 0 | 0 | 0 | 91 % |
| F | `cli/schedule_cmd.py` | 513 | 196 | 287 | 30 | 0 | 0 | 38 % |
| F | `cli/worker_cmd.py` | 181 | 117 | 63 | 0 | 1 | 0 | 65 % |
| F | `cli/worker_options.py` | 83 | 76 | 7 | 0 | 0 | 0 | 92 % |
| F | `process_manager/autoscale.py` | 94 | 55 | 39 | 0 | 0 | 0 | 59 % |
| F | `process_manager/check.py` | 269 | 213 | 56 | 0 | 0 | 0 | 79 % |
| F | `process_manager/config.py` | 11 | 10 | 1 | 0 | 0 | 0 | 91 % |
| F | `process_manager/entry.py` | 144 | 107 | 30 | 0 | 7 | 0 | 74 % |
| F | `process_manager/reload.py` | 152 | 114 | 36 | 0 | 0 | 1 (+1 interrupted) | 75 % |
| F | `process_manager/restart_budget.py` | 197 | 114 | 83 | 0 | 0 | 0 | 58 % |
| F | `process_manager/run.py` | 328 | 198 | 127 | 0 | 3 | 0 | 60 % |
| F | `process_manager/sampler.py` | 22 | 19 | 2 | 0 | 1 | 0 | 86 % |
| F | `process_manager/setpoint.py` | 299 | 208 | 91 | 0 | 0 | 0 | 70 % |
| F | `process_manager/setpoint_store.py` | 52 | 45 | 6 | 0 | 1 | 0 | 87 % |
| F | `process_manager/slots.py` | 28 | 27 | 1 | 0 | 0 | 0 | 96 % |
| F | `process_manager/supervisor.py` | 446 | 334 | 108 | 0 | 1 | 3 | 75 % |
| F | `process_manager/worker_target.py` | 38 | 13 | 25 | 0 | 0 | 0 | 34 % |

## Auffälligkeiten

### Wie mutmut 3.8 Exit-Codes einordnet

Die Status kommen aus `mutmut/stats.py` (`status_by_exit_code`), nicht aus einer Analyse des Laufs. Geprüft in der Datenbank (`exit_code_by_key`):

| Status | Exit-Code | Anzahl | Bedeutung |
|---|---|---:|---|
| timeout | -24 (SIGXCPU) | 15 | echtes Zeitlimit: Wand-Timer oder CPU-Limit |
| timeout | 255 | 3 | **kein Zeitlimit**: der Forkserver-Enkel fängt eine Ausnahme aus `test_runner.run_tests` und endet mit `os._exit(-1)` (`mutmut/workers/isolation.py`); mutmut zählt 255 als timeout |
| segfault | -9 (SIGKILL) | 3 | **kein Absturz**: SIGKILL kommt von der harten CPU-Grenze (1 s nach SIGXCPU, wenn SIGXCPU nicht beendet) oder vom OOM-Killer des Containers (8 GB). Welches von beiden, zeigt die Datenbank nicht |
| segfault | -11 (SIGSEGV) | 1 | echter SIGSEGV im Testprozess, Ursache nicht geprüft |
| check was interrupted by user | 2 | 1 | pytest-ExitCode 2; hier aber von einem fremden Prozessabbild (siehe unten) |

Alle 23 Mutanten dieser Zeilen gelten als erkannt; keiner ist Sichtungskandidat.

### Die 4 „segfault“

| Mutant | Fundstelle | Mutation | Exit |
|---|---|---|---|
| `reload.x__block_shutdown_signals__mutmut_4` | `process_manager/reload.py:204` | `pthread_sigmask(SIG_BLOCK, _SHUTDOWN_SIGNALS)` → `pthread_sigmask(SIG_BLOCK, )` | -9 |
| `supervisor.xǁProcessManagerǁ__init____mutmut_65` | `process_manager/supervisor.py:190` | `_shutdown_signal_logged = False` → `None` | -9 |
| `supervisor.xǁProcessManagerǁ__init____mutmut_67` | `process_manager/supervisor.py:191` | `_reexec_holds_signals = False` → `None` | -9 |
| `supervisor.xǁProcessManagerǁ_handle_exit_code__mutmut_1` | `process_manager/supervisor.py:606` | `exitcode == CONFIG_ERROR_EXIT_CODE` → `!=` | -11 |

Alle vier liegen im Signal- und Reload-Pfad des Supervisors (KTD8 des Reload-Plans). Die drei SIGKILL-Fälle verhalten sich wie hängende Tests; die SIGSEGV-Ursache bei `_handle_exit_code__mutmut_1` (jeder Exit gilt als Konfigurationsfehler) ist ungeklärt. Ein Nachstellen bräuchte einen mutmut-Lauf, den die Agentenregeln auf dem Laptop nicht erlauben.

### Die 18 „timeout“

| Modul | Mutanten (Zeile, Mutation) | Exit |
|---|---|---|
| `process_manager/entry.py` | `worker_subprocess_entry__mutmut_30/32/33/39/40/41` (Zeilen 201–209: `while True` → `while False`, Herzschlag-Bedingung `and` → `or` bzw. `is None`, `os.kill(...)` ohne Argument, `asyncio.sleep(None)`); `_6` (Zeile 175, `drain_timeout=None`) | -24; `_6`: 255 |
| `process_manager/run.py` | `_run_multi_process_worker_configured__mutmut_99/106/124` (Zeilen 113/119: `getattr(None, "processes")`, `"PROCESSES"`, `cli_value(args, None)`) | -24 |
| `process_manager/sampler.py` | `_make_queue_depth_sampler__mutmut_2` (Zeile 60, `asyncio.timeout(None)`) | -24 |
| `process_manager/setpoint_store.py` | `read_setpoint_sync__mutmut_2` (Zeile 74, `asyncio.timeout(None)`) | -24 |
| `process_manager/supervisor.py` | `_wait_for_exit__mutmut_1` (Zeile 396, `<` → `<=` in der Warteschleife) | -24 |
| `backend/valkey/script_pipeline.py` | `_settle__mutmut_1` (Zeile 148, `future.set_exception(None)`) | -24 |
| `client/results.py` | `get_results__mutmut_15` (Zeile 165, `deadline - loop.time()` → `+`) | -24 |
| `cli/worker_cmd.py` | `run_worker_async__mutmut_51` (Zeile 79, Standard `""` → `"XXXX"` für `SWARMQ_BROKER_URL`) | -24 |
| `cli/check_cmd.py` | `_check_logging_settings__mutmut_11` (Zeile 101, `probe_dict_config(path, None)`) | 255 |
| `cli/parser_ops.py` | `_add_dlq_parser__mutmut_321` (Zeile 361, Hilfetext in Kleinbuchstaben) | 255 |

Zwölf der 15 echten Zeitlimits liegen im Prozessmanager. Die Timeouts entstehen dort, wo eine Mutation eine Schutzfrist (`asyncio.timeout`, Herzschlag-Schleife, Warteschleife) aushebelt. Das ist erwartbar und zeigt, dass die Tests die Frist mittelbar abdecken. Die drei 255-Fälle sind Ausnahmen im Testrunner, keine Zeitlimits; bei `_add_dlq_parser__mutmut_321` (nur Hilfetext) ist das ungewöhnlich, aber ohne Folgen für die Bewertung.

### Der eine „check was interrupted by user“

`swarmq.process_manager.reload.x_reexec__mutmut_28`, `process_manager/reload.py:171`:

```diff
-            if not pm._reload_requested or pm._shutdown_signal is not None:
+            if not pm._reload_requested and pm._shutdown_signal is not None:
```

Exit-Code 2 ist bei mutmut „interrupted by user“. Herleitung aus dem Testcode: `test_without_execvpe_the_mask_is_restored_and_a_held_sigterm_delivered` (`tests/unit/process_manager/test_reload.py:310`) ruft `reexec` mit `_reload_requested=True` und `_shutdown_signal="SIGTERM"`, patcht `os.execvpe` aber nicht, weil der unmutierte Code vorher zurückkehrt. Unter dem Mutanten läuft das echte `os.execvpe(sys.executable, [sys.executable, "-m", "swarmq.cli"], {})`. Der Testprozess wird durch die swarmq-CLI ersetzt, die ohne Unterbefehl mit dem argparse-Fehlercode 2 endet. Der Mutant ist also erkannt (der Test kommt nie zurück), nur der Status ist irreführend. Nebenbefund für U5/F: Ein Unit-Test, der unter einer Regression das echte `execvpe` erreicht, ersetzt den Testprozess, statt rot zu werden. Ein Patch auf `os.execvpe` mit `side_effect=AssertionError` in diesem Test würde das abfangen (Testqualität, kein Produktfehler).

### Module und Funktionen ohne Unit-Test („no tests“, 314)

mutmut vergibt „no tests“ ohne Testlauf, wenn kein Unit-Test die Funktion aufruft (Exit-Code 33).

| Gruppe | Modul | Funktionen ohne Unit-Test (Mutanten) |
|---|---|---|
| D | `backend/valkey/chords.py` | ganzes Modul: `chord_set_key` (4), `ChordManager.__init__` (10), `record_member` (10), `record_and_check_complete` (18) |
| D | `backend/valkey/execution_guard.py` | `renew` (17), `_finish` (13), `release` (7), `mark_done` (6), `mark_superseded` (6), `run_key` (1), `state_key` (1) |
| D | `backend/valkey/locks.py` | `ValkeyLockStore.observe` (47), `_as_text` (6) |
| E | `broker/rabbitmq/admin.py` | `purge_dead_letters` (15), `_extract_task_id` (2), `_passive_message_count` (1) |
| E | `broker/rabbitmq/connection.py` | `RabbitMQConnection.ping` (12) |
| E | `client/workflows.py` | `ClientWorkflows.workflow` (15) |
| F | `cli/inspect_cmd.py` | `_format_inspect_table` (66) |
| F | `cli/schedule_cmd.py` | `_render_add_result` (30) |
| F | `cli/env_check_cmd.py` | `_seen_interval_s` (27) |

Gruppe D bestätigt den Runde-2-Befund „Valkey-Backend nur über Integrationstests geprüft“: 146 Mutanten ohne Unit-Test (Runde 2 zählte 512 über die damaligen Valkey-Module). Lauf B (U4) bewertet genau diese Module mit `tests/integration/backend`.

### Weitere Beobachtungen

- `client/workflows.py`, `ClientWorkflows.apply`: Fast alle Argumente des Publish-Aufrufs (`workflow_id`, `task_id`, `queue`, `kwargs`, die Workflow-Header) lassen sich durch `None` ersetzen oder entfernen, ohne dass ein Unit-Test scheitert. Der Workflow-Start ist auf Unit-Ebene nur grob geprüft.
- `backend/valkey/locks.py` und `rate_limit.py`: Die Parameter an die Lua-Skripte (`keys`, `args`, `now_ms`) überleben fast jede Mutation; die Unit-Tests mocken die Skripte und prüfen deren Eingaben nicht.
- `backend/valkey/environment.py:46`: `cast("dict[str, Any]", raw)` → `cast(raw)` überlebt, obwohl `cast` mit einem Argument einen `TypeError` wirft. Der Fehler wird also irgendwo verschluckt (Kandidat für U5/D).
- `process_manager/restart_budget.py` hat 83 Überlebende, davon 73 in Logaufrufen; nach dem Vorfilter bleiben 10, alle Kontrollfluss (E-018-Restpunkt aus KTD4).

## Vorfilter (R17)

Textfilter über den Export, umgesetzt AST-basiert: Für jeden Mutanten wird die erste geänderte Zeile in der Originalquelle gesucht, die Spalte der Änderung bestimmt und der kleinste AST-Knoten an dieser Stelle samt Elternkette geprüft. Skripte: `<scratchpad>/u3/export.py` (Diff aus der mutmut-Datenbank mit `mutmut.mutation.diff_apply.get_diff_for_mutant`, dieselbe Funktion wie `mutmut show`) und `<scratchpad>/u3/klassifiziere.py`.

| Regel | Trifft, wenn | Begründung |
|---|---|---|
| V1 Docstring | Konstante ist erste Anweisung von Modul, Klasse oder Funktion | ändert nur Doku. Traf keinen Mutanten: mutmut 3.8 mutiert Docstrings nicht |
| V2 Typannotation | Annotation eines Parameters, Rückgabetyps, `AnnAssign`; oder der Typ-String in `typing.cast(...)` geändert bzw. durch `None` ersetzt | `cast` gibt zur Laufzeit nur den Wert zurück. **Nicht gefiltert:** das Entfernen des Typ-Arguments (`cast(raw)`), weil das einen `TypeError` wirft (12 Mutanten bleiben drin) |
| V3 `__repr__` | Mutation in `__repr__`, `__str__`, `__rich_repr__` | Darstellung. Traf keinen Mutanten |
| V4 Log | Mutation in den Argumenten eines Logger-Aufrufs (`<…log…>.debug/info/warning/error/exception/critical/log`), bis zur umgebenden Anweisung | ändert nur Ereignisname, Feldnamen, Feldwerte oder Hinweistext der Logzeile |
| V6 Fehlertext | String in `raise X("…")` oder als Argument eines Konstruktors `*Error`/`*Exception`/`*Warning`, verändert oder durch `None` ersetzt | Ausnahmetyp und Kontrollfluss bleiben gleich |
| V7 Freitext | Satz aus mindestens drei Wörtern, nur XX-Rahmen oder Groß-/Kleinschreibung mutiert, kein Dict-Schlüssel | Hinweis- oder Ausgabetext für Menschen |

Bewusst **nicht** gefiltert: übrige String-Mutationen (643 nach Filter), vor allem Dict-Schlüssel in JSON-Ausgaben der CLI, Tabellenköpfe, Valkey-Schlüssel, Umgebungsvariablen-Namen, Skriptnamen und `getattr`-Namen. Ein Dict-Schlüssel in `--format json` ist Teil des Ausgabevertrags; ein Umgebungsvariablen- oder Skriptname steuert Verhalten. Davon liegen 455 in `cli/`.

### Ergebnis je Gruppe

| Gruppe | überlebt vor | überlebt nach | ohne Test vor | ohne Test nach | gefiltert (V2 / V4 / V6 / V7) |
|---|---:|---:|---:|---:|---|
| D | 254 | 183 | 146 | 138 | 79 (33 / 19 / 25 / 2) |
| E | 439 | 328 | 45 | 41 | 115 (6 / 60 / 35 / 14) |
| F | 1609 | 1192 | 123 | 121 | 419 (9 / 272 / 70 / 68) |
| **Summe** | **2302** | **1703** | **314** | **300** | **613** (48 / 351 / 130 / 84) |

Die Spalte „gefiltert“ zählt Überlebende und Mutanten ohne Test zusammen (599 + 14). Am stärksten wirkt der Filter in `process_manager/restart_budget.py` (83 → 10), `supervisor.py` (108 → 41), `autoscale.py` (39 → 11), `reload.py` (36 → 13) und `broker/rabbitmq/redeclare.py` (10 → 1): Dort sind die meisten Überlebenden Logzeilen.

### Exporte

Im Scratchpad `/tmp/claude-1000/-home-georg-Dokumente-Development-Privat-swarmq/608b3617-a1bc-47f1-bd40-e763775cafe6/scratchpad/u3/`:

| Datei | Inhalt |
|---|---|
| `mut-gruppe-D.txt`, `mut-gruppe-E.txt`, `mut-gruppe-F.txt` | nach Vorfilter: Kopfzeile `Name \| Status \| Datei:Zeile \| Funktion`, darunter die geänderten Zeilen (`-`/`+`); Status `survived` oder `no tests`; sortiert nach Modul und Zeile |
| `mut-gruppe-{D,E,F}-gefiltert.txt` | aussortierte Mutanten im selben Format mit Regel je Mutant; Kopf mit Regeltexten |
| `alle.json` | alle 9714 Mutanten mit Status, Exit-Code, für Nicht-Getötete Diff und Datei:Zeile |
| `klass.json` | Überlebende und „ohne Test“ mit Filterregel |

Die Zeilennummern stammen aus der Originalquelle des Worktrees; alle 2639 nicht getöteten Mutanten ließen sich einer Zeile zuordnen (der Diff von `mutmut show` zählt Zeilen relativ zur Funktion).

## Empfehlung zur CLI-Begrenzung (Open Question)

Die Regel lautet: Mehr als etwa 1500 Überlebende nach Vorfilter → die CLI-Gruppe wird auf `parser_ops.py`, `dlq_cmd.py`, `schedule_cmd.py`, `check_cmd.py` begrenzt, der Rest geht nach Welle 3.

| Zählweise | ohne Begrenzung | mit Begrenzung |
|---|---:|---:|
| überlebt nach Vorfilter, alle Gruppen | 1703 | 1377 |
| + ohne Test | 2003 | 1586 |
| Gruppe F überlebt | 1192 | 866 |
| Gruppe F überlebt + ohne Test | 1313 | 896 |
| jetzt zu sichten (E + F überlebt, D folgt nach U4) | 1520 | 1194 |

**Empfehlung: Begrenzung anwenden.** 1703 liegt deutlich über der Schwelle. Nach Welle 3 wandern 326 Überlebende und 91 ohne Test aus `cli/__init__.py` (43), `_shared.py` (24), `env_check_cmd.py` (129 + 27 ohne Test), `inspect_cmd.py` (45 + 64), `parser.py` (29), `pool.py` (6), `worker_cmd.py` (43), `worker_options.py` (7).

Zwei Einschränkungen:

- Die vier verbleibenden CLI-Module bringen zusammen 543 Überlebende (`schedule_cmd.py` 272, `dlq_cmd.py` 140, `check_cmd.py` 111, `parser_ops.py` 20) und 30 ohne Test. 215 davon sind String-Mutationen, meist JSON-Schlüssel und Tabellenköpfe der Ausgabe. Der F-Agent sollte sie je Funktion bündeln statt einzeln zu bewerten.
- Mit der Begrenzung bleibt die Gesamtzahl einschließlich „ohne Test“ (1586) knapp über 1500. Das zählt nicht gegen die Regel, weil „ohne Test“ in D nach U4 ohnehin durch Lauf B neu bewertet wird (138 der 300).

`mut-gruppe-F.txt` enthält weiter alle CLI-Module; die Begrenzung ist ein Auftrag an U5, kein Löschen im Export. Die Blöcke je Modul liegen zusammen, weil der Export nach Modul sortiert ist.

## Einstieg für U5: 20 auffällige Überlebende (Kontrollfluss)

Auswahl aus den Modulen der Vorrangliste in U5 Schritt 3, nur nach Vorfilter, nur Kontrollfluss, Rechnung oder Aufrufargumente. Das ist eine Einstiegsliste, keine Bewertung: Ob ein Mutant einen Fehler oder nur eine Testlücke zeigt, klärt U5 mit Probe-Test und Abgleich gegen das Entscheidungsregister.

| # | Gr. | Fundstelle | Mutant(en) | Mutation | Warum auffällig |
|---|---|---|---|---|---|
| 1 | D | `backend/valkey/locks.py:62`, `:130` | `LockManager.acquire__mutmut_3/4/7`, `extend__mutmut_17` | `now_ms`: `+` → `-`, `* 1000` → `/ 1000`, `//` → `/` | Zeitbasis für die Lock-Skripte (Ablauf, Mehrfach-Halter); kein Unit-Test prüft den übergebenen Wert |
| 2 | D | `backend/valkey/locks.py:66–67`, `:159` | `acquire__mutmut_17/18/21`, `ValkeyLockStore.acquire__mutmut_1–4` | `keys=None`, `args=None`, `lock(None)`, Argumente `None` | Schlüssel und Owner-Token an den Skriptaufruf ungeprüft (Owner-Vergleich laut U5) |
| 3 | D | `backend/valkey/locks.py:205`, `:215` | `ValkeyLockStore.observe__mutmut_5/33` (ohne Test) | `max_concurrency == 1` → `!= 1` | Zweig String-Lock vs. Sorted Set; `observe` hat 47 Mutanten ohne Unit-Test |
| 4 | D | `backend/valkey/execution_guard.py:85` | `renew__mutmut_16` (ohne Test) | `int(result) == 1` → `!= 1` | Renew-Ergebnis des Execution-Guards invertiert; `renew`, `release`, `mark_done`, `mark_superseded`, `_finish` ohne Unit-Test |
| 5 | D | `backend/valkey/execution_guard.py:75` | `claim__mutmut_2/3` | `backlog_ttl_ms or 0` → `and 0` / `or 1` | Backlog-TTL des Claims wird nicht geprüft |
| 6 | D | `backend/valkey/rate_limit.py:49–50` | `RateLimiter.check__mutmut_11/12/14` | `window_ts = now_ms + …`, `% ` → `/`, `prev_ts = window_ts + …` | Fensterberechnung des Sliding-Window-Limits ungeprüft |
| 7 | D | `backend/valkey/rate_limit.py:63` | `check__mutmut_36` | `bool(int(allowed_raw))` → `bool(None)` | „erlaubt“ wäre immer falsch, ohne dass ein Unit-Test scheitert |
| 8 | E | `broker/rabbitmq/connection.py:576` | `_ensure_consume_channel__mutmut_6` | `_consume_prefetch is not None` → `is None` | Prefetch nach Neuanlage des Consume-Kanals würde nicht gesetzt (Docstring nennt genau diesen Fall) |
| 9 | E | `broker/rabbitmq/connection.py:585`, `:591` | `_ensure_channel_tracked__mutmut_8/17` | `not channel.is_closed` → `channel.is_closed` | Fast-Path und Re-Check unter dem Lock: geschlossener Kanal würde zurückgegeben |
| 10 | E | `broker/rabbitmq/connection.py:486`, `:492` | `set_publish_deadline__mutmut_7/9` | `current > at` → `>=`; `continue` → `break` | Nachziehen der Publish-Deadline (FS-02-Bereich): nach einem abgelaufenen Deckel würden die übrigen nicht mehr nachgezogen |
| 11 | E | `broker/rabbitmq/connection.py:316`, `:411`, `:643` | `_connect_injected__mutmut_2`, `_drain_pending_channel_closes__mutmut_8`, `_throwaway_channel_message_count__mutmut_2` | `and` → `or`; `not task.done()` → `task.done()`; `or` → `and` | Verbindungs- und Aufräumpfade ohne scharfe Prüfung |
| 12 | E | `broker/rabbitmq/consumer_watch.py:168`, `:250` | `cancel_all__mutmut_3`, `tick__mutmut_19` | `continue` → `break` | Ein Eintrag ohne Tag würde Abmelden bzw. Neuanmelden aller folgenden Consumer überspringen |
| 13 | E | `broker/rabbitmq/consumer_watch.py:318`, `:326` | `_consume__mutmut_17/25/26` | `_stopping or …` → `and`; `missing_ticks = 0` → `None`/`1` | Neuanmeldung während des Stopps; Zähler für fehlende Consumer nach Neuanmeldung |
| 14 | E | `broker/rabbitmq/redeclare.py:59` | `_redeclare_on_throwaway_channel__mutmut_2` | `connection is None or connection.is_closed` → `and` | einziger Überlebender des Moduls nach Vorfilter |
| 15 | E | `client/workflows.py:110–117` | `ClientWorkflows.apply__mutmut_33/35/40/48` | `workflow_id=None`, `task_id=None`, Workflow-Header entfernt | Publish der Workflow-Schritte im Unit-Test nicht geprüft |
| 16 | E | `client/workflows.py:178`, `:219`, `:196` | `_mark_apply_failed__mutmut_28`, `_unpack__mutmut_4`, `workflow__mutmut_1` (ohne Test) | `version`-Standard 0 → 1; `is_cancelled`-Standard; `raw = None` | CAS-Version beim Markieren eines gescheiterten Starts; `workflow()` ohne Unit-Test |
| 17 | F | `process_manager/restart_budget.py:76`, `:89`, `:92` | `_check_heartbeats__mutmut_12/30/35` | `continue` → `break`; `stale <= deadline` → `<` | Ein Worker in Recycling oder ohne Kanal würde die Herzschlagprüfung aller folgenden Worker beenden (E-018-Umfeld) |
| 18 | F | `process_manager/restart_budget.py:146–153`, `:170`, `:117` | `_escalate_terminating__mutmut_4/5/14`, `_maybe_restart__mutmut_5`, `_replace_hung_worker__mutmut_6` | `continue` → `break`; `>=` → `>`; `t >= cutoff` → `>`; `<=` → `<` | SIGKILL-Eskalation würde nach dem ersten toten Worker abbrechen; Grenzen des Neustartfensters ungeprüft |
| 19 | F | `process_manager/setpoint.py:96–122`, `:185`, `:193` | `apply__mutmut_13/36/37/51/74/77/78/79/82`, `reconcile__mutmut_5/20` | Rückgabe `changed` invertiert; `and` → `or`, `==` → `!=`; `break` → `return` | Ob ein Sollwert als Änderung gilt, wird nicht geprüft; Untergrenze bei Autoscaling (E-018-Restpunkt) |
| 20 | F | `process_manager/setpoint.py:36`, `:210`, `:229` | `_validate__mutmut_4`, `_status_ttl_s__mutmut_2`, `_build_status__mutmut_25/26/27` | `high > headroom` → `>=`; `+ SAMPLE_INTERVAL` → `-`; Feld `autoscaling` falsch | Grenzwert der Sollwert-Prüfung, TTL des Statusberichts, gemeldeter Autoscaling-Zustand |

Für `blocked.py` bleiben nach Vorfilter nur drei Mutanten, alle in den `getattr`-Wächtern von `_unblocked_event` (Zeilen 38–39); keiner betrifft den Kontrollfluss des Blockiert-Zustands.

## Abweichungen zwischen Plan und Lauf

- Plan U3 nennt als Kennzahlen „Mutanten gesamt, getötet, überlebt, timeout, ohne Test“. Der Lauf hat zusätzlich „segfault“ und „interrupted“; beide sind oben erklärt und zählen als erkannt.
- KTD4 nennt `[tool.mutmut]` ohne Zeitlimit-Einstellungen; es galten die mutmut-Standards (`timeout_multiplier` 15).
- Der Container hatte 8 GB Speicher (`compose.yml`); im Plan steht keine Grenze. Ein OOM-Kill als Ursache der drei SIGKILL-Fälle ist damit möglich, aber nicht belegt.
