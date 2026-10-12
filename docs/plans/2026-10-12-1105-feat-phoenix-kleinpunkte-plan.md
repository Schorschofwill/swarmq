---
title: Konfliktfreie Punkte aus der Phoenix-Migrationsanalyse (P1, P2, P5b, P6, P7b, P8, P9, P12)
type: feat
date: 2026-10-12
revision: 1 (Erstfassung; alle Code-Stellen gegen origin/main 5bfa106 geprüft)
artifact_readiness: decisions-pending (Abschnitt 1.2)
execution: code
---

# Konfliktfreie Punkte aus der Phoenix-Migrationsanalyse

## Goal Capsule

- **Objective:** Die acht konfliktfreien Punkte aus der Phoenix-Migrationsanalyse umsetzen (Phoenix-Nummern A4, A5, A6, A10, A11, A13 sowie die Alternative zu „XQ-Queue-Argumente“ und ein Log-Bug):
  - **P1** Scheduler je Worker abschaltbar (A4)
  - **P2** `FromComponent` und Aliase unter `from __future__ import annotations` (A5)
  - **P5b** Giftnachrichten mit Ablaufzeit in die XQ (Alternative zu den XQ-Queue-Argumenten)
  - **P6** Importpfad des Workers, `--app-dir` (A6)
  - **P7b** `task_sent_to_dlq` ohne liegende DLQ-Kopie (Log-Bug)
  - **P8** `Retry` trotz `retry_exceptions` (A10)
  - **P9** Fehlerklasse im Ergebnis, `error_class` (A11)
  - **P12** Doku an den Code angleichen (A13)
  - **Hauptbedingung:** Kein Punkt ändert Verhalten, das nicht in diesem Plan steht. Das gilt auch im Mischbetrieb mit 0.12.0rc1 und 0.11.1.
- **Authority:**
  - Zuerst gelten die Entscheidungen in Abschnitt 1. Offene Fragen (1.2) blockieren nur ihre eigene Unit.
  - Danach gelten die Querschnittsregeln Q1–Q12 (Abschnitt 2), zuletzt die Unit-Beschreibungen.
  - AGENTS.md gilt für jede Unit: strikt TDD, ein roter Test zur Zeit, keine src-Datei über 800 Zeilen.
- **Ausführung:**
  - MR 1–8 in der Reihenfolge aus Abschnitt 4.
  - Gearbeitet wird lokal im Worktree `swarmq-check` auf Branches von `feat/phoenix-kleinpunkte`. Das Hauptverzeichnis `../swarmq` wird nicht angefasst.
  - Push, MR, Merge, Tag und Release nur nach Georgs ausdrücklicher Freigabe.
- **Stop conditions:** Die betroffene Unit wird angehalten und notiert, unabhängige Units laufen weiter. Anlässe:
  - Ein bestehender Test oder eine Golden-Datei ändert sich, die nicht in Abschnitt 6 steht.
  - Ein Spike (Abschnitt 3) widerspricht einer Annahme des Plans.
  - Eine offene Entscheidung aus 1.2 ist für die Unit noch nicht getroffen.
  - Eine src-Datei überschreitet 800 Zeilen und lässt sich nicht ohne Verhaltensänderung teilen.
  - Es taucht eine echte Produktfrage auf, die nicht in 1.2 steht.
- **Tail ownership:** Georg. Den PyPI-Upload führt er per `!` aus.

---

## 1. Entscheidungen

### 1.1 Von Claude technisch entschieden (\* = mit Begründung, Georg kann widersprechen)

- \* **T1 (P1) Feldname und Werte:** Konfigurationsfeld `scheduler: str = "on"`, Werte `on|off`, Env `SWARMQ_SCHEDULER`, CLI `--no-scheduler` (setzt `off`).
  - Begründung: Die Namensregel `SWARMQ_<FELD>` (`test_agent_spec.py:141-155`) erzwingt den Feldnamen. `on|off` folgt dem Muster von `scheduler_prune` (`config.py:293`).
  - `--no-scheduler` kommt in die Tabelle `_OPTIONS` (`cli/worker_options.py:20-28`). Damit gilt Flag > Env > Default wie bei `--burst`, und der Supervisor reicht den Wert über `resolved_fields` an die Kinder.
  - Die Werteliste kommt in `_CONFIG_CHOICES` (`agent_spec.py:411`), damit die Spec sie nennt.
- \* **T2 (P1) Ort des Schalters:** `SchedulerLoop` bekommt den Parameter `candidate: bool = True`. Mit `False` startet `start()` nur den Melde-Takt (`_report_loop` plus der einmalige `_report_crons()`), nicht `_election_loop`, `_poll_loop` und `_adopt_loop` (`scheduler/loop.py:207-216`).
  - Begründung: Die Meldungen (Crons nach KTD10, Worker-Version für KTD11, konsumierte Queues für BZ-14, Epoch für BZ-04) laufen in `_report_crons` (`loop.py:312-334`). Fallen sie weg, entfernt der Leader die statischen Crons dieses Workers nach `scheduler_prune_after_s` (`prune.py`), und `swarmq check` meldet seine Queues als verwaist.
  - `stop()` und `abort()` bleiben unverändert. Der Zweig `state == "leader"` wird nie erreicht.
- \* **T3 (P1) Start-WARNING:** `scheduler_disabled` (WARNING, einmal je Worker-Start, Logger `swarmq.worker.lifecycle`) mit einem `hint`: Dieser Worker bewirbt sich nicht um die Leader-Rolle. Crons, `delay`/`eta`-Tasks und Oneshots feuern nur, wenn ein anderer Worker am selben Valkey (und Namensraum) Leader ist.
- \* **T4 (P1) `swarmq check`:** neuer Abschnitt `scheduler` mit `{"scheduler": "on"|"off"}` aus der Umgebung des Aufrufs. Bei `off` gibt es den Befund `scheduler_disabled` mit Stufe `warn` (Exit 0). Der Bericht wächst nur additiv (`build_report`, `cli/env_check_cmd.py:315-330`). Eine Prüfung, ob überhaupt ein Leader lebt, ist Folgearbeit (Abschnitt 9).
- \* **T5 (P2) Tolerante Auswertung:** Bei der Klassendefinition löst `maybe_wrap_task` die Annotationen einer Lifecycle-Methode per `typing.get_type_hints(func, include_extras=True, format=annotationlib.Format.FORWARDREF)` auf. Das passiert immer dann, wenn eine Roh-Annotation ein String ist (PEP 563).
  - Vorab geprüft auf CPython 3.14.4: Unter `from __future__ import annotations` liefert der Aufruf für `a: Annotated[C, "m"]` das ausgewertete `Annotated`, für `b: Undefined` ein `ForwardRef('Undefined')`. Es gibt keinen `NameError`.
  - Parameter, die zu einem DI-Marker auflösen (`default_parse_dependency` liefert einen Key), werden injiziert. `ForwardRef`-Parameter werden übersprungen.
  - Die bestehende Regel bleibt: Ein String, der `FromDishka` nennt und nicht auflösbar ist, ist weiter ein strikter `NameError` mit Hinweis (`dishka.py:341-351`).
  - Die Abwägung aus `docs/solutions/architecture-patterns/dishka-di-integration-async-task-queue.md` (Abschnitt 2 „Gate the eager get_type_hints…“) bleibt erhalten: Ein Task ohne DI bricht nie. Ein nicht auflösbarer Name ist kein Fehler, er wird übersprungen.
  - Begründung gegen die längere String-Suche: Aliase (Phoenix `Depends`) lassen sich nicht per Teilstring erkennen.
- \* **T6 (P2) Nachprüfung beim Start:** `validate_registered_tasks` wertet die Annotationen nicht gewrappter Lifecycle-Methoden erneut tolerant aus. Dann ist das Modul vollständig geladen, spätere Forward-Refs lösen sich auf. Ergibt sich dabei ein DI-Key, folgt ein `ConfigurationError` (Exit 78). Er nennt Task und Parameter und erklärt, dass die Annotation bei der Klassendefinition auflösbar sein muss.
  - Begründung: Ein spätes Wrappen beim Start griffe nicht in Kindprozessen (CPU-Modus, Supervisor-Kinder importieren neu). Lautes Scheitern ist besser als stilles Nicht-Injizieren.
  - Bleibt eine Annotation auch beim Start unauflösbar, gibt es keinen Fehler und keine Warnung. Das ist eine Grenze, Abschnitt 7.
- \* **T7 (P2) Validierung im eingebetteten Worker:** `SwarmQ.start_worker()` (`app.py:245`) ruft `validate_registered_tasks` mit den Task-Klassen der App-Registry auf, vor `worker.start()` und damit vor jeder Verbindung. Der frühe Aufruf in `cli/worker_cmd.py:67-69` bleibt, damit die Fehlerreihenfolge der CLI gleich bleibt (DI vor `SWARMQ_BROKER_URL`). Ein doppelter Aufruf ist idempotent.
- \* **T8 (P6) Ort:** Die Funktion `apply_app_dir(args)` liegt in `cli/_shared.py` und wird in `cli.main()` direkt nach dem Parsen aufgerufen, vor jeder Verzweigung.
  - Begründung: `_shared` steht schon in der eingefrorenen Oberfläche von `swarmq.cli` (`test_cli_module_surface.py:93`). Ein neues Submodul oder ein neuer Name in `cli/__init__.py` würde `test_no_symbols_added` brechen.
  - Die Option `--app-dir DIR` hängt an den Unterbefehlen `worker` (auch `--check`) und `inspect`. `swarmq check` importiert kein Modul und bekommt die Option nicht (Abschnitt 10).
  - Kinder erben den Pfad, weil er vor Spawn und Forkserver gesetzt ist (Spike S2).
- \* **T9 (P7b) Kein neues Event:** Im Zweig „DLQ-Publish gescheitert, neu zugestellt“ (`worker/retry.py:301-309`) entfällt `task_sent_to_dlq`. Das bestehende `dlq_publish_failed` (ERROR, `retry.py:302`) beschreibt den Fall schon.
  - Im Zweig mit `on_publish_failure` (DLX-Rückfall der Lock- und Unknown-Pfade) bleibt `task_sent_to_dlq`, denn die Nachricht liegt danach über den Broker-DLX in der XQ.
  - KTD11 (Log-Niveau) bleibt unverändert.
- \* **T10 (P8) Reihenfolge in `evaluate`:** Der Filter `retry_exceptions` (`retry.py:96-97`) gilt nicht für `Retry`. `NoRetry` (Schritt 1) und die Budgetprüfung (Schritt 4) bleiben wie sie sind.
  - Der CPU-Modus braucht keine eigene Änderung: `Retry` ist picklebar und behält `delay` (`tests/unit/worker/compute/test_envelope.py:31`). Die Bewertung läuft im selben `RetryHandler` (`pipeline.py:543`).
- \* **T11 (P9) Feld nur auf Fehlerergebnissen:** `build_result_dict` bekommt `error_class: str | None = None`. Der Schlüssel wird nur geschrieben, wenn ein Wert vorliegt.
  - Begründung: Das Erfolgs-Ergebnis bleibt byte-identisch, also auch `golden/values/result.msgpack` (`test_golden_fixtures.py:49-52`). Ebenso `test_cancellation.py:111-124` (exakte Dict-Gleichheit ohne das Feld).
  - Im Schema `_TaskResult` (`agent_spec.py:592-599`, `forbid_unknown_fields`) kommt `error_class: str | None = None` ans Ende, als optionales Feld.
- \* **T12 (P9) Helfer:** `error_class_of(exc)` und `error_text(exc)` liegen im neuen Modul `swarmq/worker/error_info.py`.
  - `error_class_of` liefert `f"{type(exc).__module__}.{type(exc).__qualname__}"`, bei `RemoteTaskError` stattdessen `exc.remote_type` (`worker/compute/envelope.py:21-26`).
  - `error_text` liefert `str(exc) or type(exc).__name__`, wie `_error_fields` (`logging.py:341-346`).
  - Die leere Meldung wird an einer Stelle je Executor gefüllt: in `_fail` von `worker/execution.py:281-286` und `worker/compute/executor.py:144`.
- \* **T13 (P9) Workflows:** `WorkflowAdvancer.fail` speichert zusätzlich `error.class` (additiv). Das bisherige `error.type` bleibt, auch das fest verdrahtete `"TaskError"` aus `worker/terminal.py:55`. `WorkflowHandle._resolve_terminal` (`workflow_orchestration.py:196-198`) und `client/workflows._unpack` (`client/workflows.py:222-223`) reichen die Klasse an `TaskError.error_class` durch.
- \* **T14 (P9) `TaskError`:** Die Klasse bekommt `__init__(self, *args, error_class: str | None = None)` und das Attribut `error_class`. `TaskError("x")` bleibt gültig, `TaskCancelledError` bleibt unverändert, und die Klasse bleibt picklebar (Test).
- \* **T15 (P12d) `ResultExpiredError`:** Die Klasse bekommt einen Docstring: „Derzeit nie geworfen; stabiler Hook für eine spätere Trennung von abgelaufen und noch nicht fertig.“ `_ERROR_SEMANTICS["ResultExpiredError"]["transport_field"]` wird `"not raised (reserved)"`. Beides fließt per Generierung in `spec/errors.json`.
- \* **T16 (P5b) Kein neuer Header:** Grund und Fehlertext reisen im bestehenden `x-swarmq-traceback` als `"<reason>: <error>"` (über `bounded_header_text`). Damit bleibt `golden/headers.txt` unverändert.
  - `TASK_REJECTED` feuert weiter mit denselben `reason`-Werten, nach der liegenden Kopie, genau einmal. Die Metrik `swarmq_tasks_rejected_total{reason}` bleibt.
  - `TASK_DLQ` feuert auf diesen Pfaden nicht. Ein Ergebnis wird nicht gespeichert und die Unique-Freigabe nicht ausgeführt, wie heute: Nur der Transport ändert sich.

### 1.2 Offen – Georg entscheidet

| ID | Frage | Optionen | Empfehlung |
|---|---|---|---|
| E1 (P6) | Standard von `--app-dir` | **A** Arbeitsverzeichnis immer voranstellen (wie uvicorn). **B** Opt-in: nur mit `--app-dir`; bei ImportError ein Hinweis auf die Option. **C** Arbeitsverzeichnis voranstellen wie `python -m` es tut, also nicht unter `PYTHONSAFEPATH`/`-P` (`sys.flags.safe_path`). Ein ausdrückliches `--app-dir` gilt immer. | **C.** `swarmq worker` verhält sich dann wie `python -m swarmq.cli worker` heute, und wer `PYTHONSAFEPATH` bewusst setzt, behält den Schutz. Bei A überdecken lokale Dateien Pakete ohne Ausweg. Bei B bleibt die Phoenix-Falle für jeden neuen Nutzer. |
| E2 (P1) | Semantik von `SWARMQ_SCHEDULER=off` | **A** Keine Leader-Bewerbung, kein Feuern, kein Adopt/Prune. Die Meldungen (Crons, Version, Queues, Epoch) laufen weiter (T2). **B** `SchedulerLoop` gar nicht starten. **C** Bewerben, aber als Leader nicht feuern. | **A.** B lässt den Leader die Crons dieses Workers nach 24 h löschen und meldet seine Queues als verwaist. C hält die Leader-Rolle besetzt und legt das Feuern für alle still. |
| E3 (P12b) | kwargs-Änderung in `pre_execute` | **A** Vertrag: Eine Änderung am bestehenden `message["kwargs"]`-Dict (und an der `message["args"]`-Liste) sieht der Task, im IO- und im CPU-Modus. Eine Neuzuweisung (`message["kwargs"] = {...}`) bleibt unsichtbar. Ein Test pinnt das. **B** Undefiniert: Die Doku beschreibt das heutige Verhalten, rät aber davon ab, sich darauf zu verlassen. | **A.** Das Verhalten ist in beiden Modi heute schon so (`pipeline.py:378-385` reicht `ctx.kwargs` selbst durch; `compute/executor.py:137` kopiert erst nach `run_pre_execute`). Ein Vertrag kostet nur einen Test, und Phoenix kann darauf bauen. |
| E4 (P8) | Budget von `Retry` bei `max_retries=0` | **A** `Retry` verbraucht das Budget wie heute. Bei `max_retries=0` endet `Retry` terminal (DLQ bzw. Fehlerergebnis); die Doku sagt das. **B** `Retry` zählt nicht gegen das Budget. **C** eigenes Budget für `Retry`. | **A.** B erlaubt Endlosschleifen ohne Obergrenze, C ist ein neues Feature. |
| E5 (P9) | Feldname und Form | **A** `error_class` (so dokumentiert, `message-formats*.md:87,100`; `agent_spec.py:84`), voll qualifiziert als `modul.qualname`, eingebaute Klassen als `builtins.ValueError`. **B** wie A, aber ohne `builtins.`. **C** nur der Klassenname. | **A.** Der Name steht schon in Doku und Spec. Die Form entspricht `exception_class` in `spec/errors.json` und ist eindeutig. |
| E6 (P5b) | Bodies über `max_message_body_bytes` (Env `SWARMQ_MAX_MESSAGE_BODY_BYTES`, Default 16 MiB) | **A** Wie die anderen Pfade als Kopie mit Ablaufzeit; scheitert die Kopie, `route_via_dlx` wie heute. Vorbedingung ist Spike S1. **B** `body_too_large` bleibt bei `route_via_dlx` ohne Ablaufzeit (heute); dokumentierte Grenze. **C** Kopie mit gekürztem Body. | **A, wenn S1 besteht, sonst B.** Der Broker hat den Body schon einmal angenommen, und er liegt im Speicher. Offen ist nur der Aufwand in `dead_letter_generation` (`identity.py:141-153`, `json.loads` des ganzen Bodys). C verliert Daten und wird nicht empfohlen. |
| E7 (P9) | `error_class` auf Pfaden ohne Task-Exception | **A** Die swarmq-Klasse nennen, wo es eine gibt (`TaskNotFoundError`, `LockWaitExceeded`, `RedeliveryLimitError`, `UnroutableMessage`, `ResultSerializationError`). Sonst `null` (Ablauf durch `max_age`, `WorkerHungError` ohne Klasse). **B** überall `null` außer bei echten Task-Exceptions. | **A.** Das deckt sich mit `error_type` in `task_sent_to_dlq`, und ein Client kann so ohne Textvergleich unterscheiden. |
| E8 (P2) | DI-Prüfung bei `swarmq worker --check` | **A** `--check` importiert das Modul (nur im Check-Prozess, nicht beim Supervisor-Start) und ruft `validate_registered_tasks`. Ein Fehler erscheint im bestehenden JSON-Fehlerpfad. **B** `--check` bleibt bei `find_spec` (`process_manager/check.py:47-66`); DI wird erst im Worker geprüft. | **A.** `--check` soll Startfehler vorab finden. Der Import läuft dort in einem kurzlebigen Prozess, den man genau dafür startet. `check_worker_config` im Supervisor-Start bleibt ohne Import. |
| E9 (Nebenbefund N1) | Python 3.14 (PEP 649): Ein Task **ohne** `from __future__ import annotations` mit einem unauflösbaren Namen in `run()` scheitert schon bei der Klassendefinition. Ursache ist `inspect.signature(run)` in `task.py:476`, unabhängig von dishka. | **A** In MR 7 mitbeheben: `annotation_format=annotationlib.Format.FORWARDREF` in `task.py:476` und an allen `inspect.signature`-Stellen in `dishka.py`, mit Test. **B** eigener Fix-MR. **C** nur dokumentieren. | **A.** Es ist dieselbe Abwägung („ein Task ohne DI darf nicht brechen“), sie ist auf 3.14 heute schon verletzt, und der Fix ist klein. |

---

## 2. Querschnittsregeln (Q1–Q12)

- **Q1 Wire-Kompatibilität:** Neue Felder sind additiv und optional. Das gilt für `error_class` im Ergebnis, `error.class` im Workflow-Zustand und `error_class` an `TaskError`. Kein bestehender Header, Key oder Wert ändert sich.
  - Clients lesen Ergebnisse mit `.get()` (`client/workflows.py:219-224`) und Workflow-Fehler mit `.get()` (`workflow_orchestration.py:197-198`).
- **Q2 Mischbetrieb (beide Richtungen):**
  - 0.12-Worker mit neuem Client: Ein Ergebnis ohne `error_class` ergibt `TaskError.error_class is None` (Unit-Test).
  - Neuer Worker mit 0.12- bzw. 0.11.1-Client: Das zusätzliche Feld wird ignoriert, `TaskError(message)` bleibt gleich. Ein Szenario in `tests/compat/` (Basis 0.11.1, opt-in `-m compat`) belegt das (MR 3).
  - Neue und 0.12-Worker an derselben Queue (P5b): 0.12 routet Giftnachrichten über den DLX ohne Ablaufzeit, der neue Worker kopiert mit Ablaufzeit. Beides landet in `.XQ`, `dlq list` liest beides (KTD6 deckt Nachrichten ohne `x-swarmq-dead-lettered-at` ab).
  - `SWARMQ_SCHEDULER=off` auf einem neuen Worker neben 0.12-Workern: Die anderen bewerben sich weiter. Ein 0.12-Worker kennt die Variable nicht und ignoriert sie (Doku: erst nach dem Update wirksam).
- **Q3 Golden-Dateien:** Bestehende Einträge bleiben byte-identisch. Neue sind additiv. Jede betroffene Datei steht in Abschnitt 6. Die CLI-Hilfe-Fixtures `tests/unit/fixtures/cli_help/*.txt` gelten als Golden.
- **Q4 Konfiguration und Spec:**
  - Jede neue `SWARMQ_*`-Variable ist ein Feld von `SwarmQConfig` und steht als Literal in `config.py` (`test_agent_spec.py:128-155`).
  - Jede Unit, die Fehlerklassen, Konfigurationsfelder, CLI-Optionen oder Nachrichtenschemata ändert, erzeugt die Spec neu: `uv run python -m swarmq.agent_spec` (`website/reference/agent/spec/*`, `website/llms.txt`). Geprüft wird das von `test_committed_spec_files_match_generators` (`test_agent_spec.py:456-467`). Das ist ein eigener TDD-Zyklus.
- **Q5 Dateigrenzen:** Keine src-Datei über 800 Zeilen (`test_architecture_guards.py`). Gefährdet (Stand 5bfa106):
  - `agent_spec.py` 742
  - `pipeline.py` 715
  - `config.py` 668
  - `workflow_orchestration.py` 664
  - `lifecycle.py` 607
  - `scheduler/loop.py` 568
  - `cli/__init__.py` 526
  - Neue Logik kommt in neue Module: `worker/error_info.py` (MR 3), die Reject-Ablage in `worker/gates/reject.py` (MR 8). Neue Untermodule werden, falls nötig, in `_SPLIT_MODULES` eingetragen.
- **Q6 Reinheit:** `worker`, `scheduler` und `client` importieren weder `backend.valkey` noch `broker.rabbitmq`, auch nicht unter `TYPE_CHECKING` (`test_architecture_guards.py`).
- **Q7 Optionale Fähigkeiten:** Neue Broker- oder Backend-Fähigkeiten werden nicht über `isinstance` auf `runtime_checkable`-Protocols erkannt. Dieser Plan ändert das öffentliche Protocol `swarmq.contracts.broker` **nicht**.
- **Q8 Hot-Path:** Im Normalpfad kommt kein Roundtrip dazu. `error_class_of` läuft nur im Fehlerpfad.
- **Q9 Bestehende Tests:** Nur die in Abschnitt 6 genannten Tests und Golden-Dateien ändern ihre Erwartung.
- **Q10 Ein roter Test zur Zeit:** Jeder Zyklus beginnt mit genau einem roten Test. Golden- und Spec-Aktualisierungen sind eigene Zyklen. Signaturen ändern sich nur additiv mit Default.
- **Q11 Doku je Unit:**
  - Jede Unit schreibt ihren Changelog-Eintrag unter `## [Unreleased]` in `website/reference/changelog.md` **und** `changelog.de.md` selbst.
  - Doku-Abschnitte immer EN und DE (`*.md` und `*.de.md`).
- **Q12 Gates je Unit** (vor jedem Commit ausdrücklich prüfen, kein `pytest | tail && git commit`):
  1. `uv run ruff format --check .`
  2. `uv run ruff check src/ tests/ typings/`
  3. `uv run mypy src/swarmq tests`
  4. `uv run basedpyright`
  5. `uv run python -m mypy.stubtest msgpack croniter --mypy-config-file pyproject.toml --ignore-missing-stub`
  6. Unit: `uv run --extra test pytest tests/unit -q`
  7. Integration und e2e **nur** über den Wrapper mit festen Testports (`SWARMQ_TEST_RABBITMQ_PORT=5699 SWARMQ_TEST_RABBITMQ_MGMT_PORT=15699 SWARMQ_TEST_VALKEY_PORT=6399`), nie 5672/6379. Vorher `pgrep -af pytest` prüfen und den Lauf per `flock` serialisieren, weil andere Sitzungen dieselben Container nutzen.
  8. Spec-Drift: `uv run python -m swarmq.agent_spec`, danach `git diff --exit-code website/reference/agent/spec website/llms.txt` (nur in Units, die Q4 betrifft).
  9. Neue Unterdrückungen (`# noqa`, `# type: ignore`, `# pyright: ignore`) nennen Code und Grund.

---

## 3. Spikes (vorab, als Tests im Repo)

| Spike | Frage | Betrifft | Ort |
|---|---|---|---|
| S1 | Lässt sich eine Nachricht mit `max_message_body_bytes + 1` Bytes (Default 16 MiB) per `RabbitMQBroker.dead_letter` als Kopie nach `.XQ` legen, auf RabbitMQ 4.3.6 (5699) und, falls der Container läuft, 4.2.9 (5742)? Wie lange dauert das, und wie viel Speicher braucht `dead_letter_generation` (`json.loads` des ganzen Bodys, `identity.py:123-132`)? Was passiert über `max_message_size` des Brokers? | MR 8, E6 | `tests/integration/spikes/test_s_p5b_oversize_copy.py` |
| S2 | Erben Supervisor-Kinder (`multiprocessing.get_context("spawn")`, `process_manager/supervisor.py:166`) und der Forkserver des CPU-Modus (`worker/compute/wiring.py`) einen zur Laufzeit vorangestellten `sys.path`-Eintrag auf CPython 3.14? Findet der Forkserver-Preload das Task-Modul daraus? Ein ImportError im Preload wird von CPython still geschluckt, deshalb prüft der Test im Kind selbst. | MR 6 | `tests/integration/test_app_dir_spawn.py` (> 100 ms, echte Prozesse) |
| S3 | Verhalten von `get_type_hints(..., format=Format.FORWARDREF)` mit dishka ≥ 1.10.1: `Annotated[X, FromComponent("c")]`, Alias `Depends = FromDishka`, `FromDishka[X] \| None`, PEP 649 ohne Future-Import. Liefert `default_parse_dependency` für einen `ForwardRef` sicher `None`? | MR 7 | Unit-Tests in `tests/unit/test_dishka_annotations.py` (rein, schnell) |

Widerspricht ein Spike einer Annahme, gilt die Stop-Condition für die betroffene Unit.

---

## 4. Reihenfolge

| # | MR | Punkte | Voraussetzung |
|---|---|---|---|
| 1 | Log-Bug `task_sent_to_dlq` | P7b | – |
| 2 | `Retry` trotz `retry_exceptions` | P8 | 1 (gleiche Datei `retry.py`), E4 |
| 3 | Fehlerklasse im Ergebnis | P9 | 2 (gleiche Datei `retry.py`), E5, E7 |
| 4 | Doku an den Code | P12a, b, d, f, DLQ-Guide | E3 (nur für P12b) |
| 5 | Scheduler abschaltbar | P1, P12c | E2 |
| 6 | `--app-dir` | P6 | 5 (gleiche Dateien `cli/parser.py`, `cli_help/worker.txt`, `spec/cli.json`), E1, S2 |
| 7 | `FromComponent` unter PEP 563, Validierung | P2, P12e, N1 (E9) | 6 (wegen E8: `--check` importiert über `--app-dir`), E8, E9, S3 |
| 8 | Giftnachrichten mit Ablaufzeit | P5b | 4 (DLQ-Guide), E6, S1 |

**Gründe für die Reihenfolge:**
- **MR 1 bis 3 nacheinander:** Alle drei ändern `worker/retry.py` (312 Zeilen). Parallel entstünden Konflikte.
- **MR 4 vor MR 8:** MR 4 korrigiert `dead-letter-queues*.md:281-286` auf den heutigen Stand (unbekannte Tasks seit BZ-06 nicht mehr über den DLX). MR 8 schreibt denselben Abschnitt danach auf den neuen Stand. Wird MR 8 wegen E6 zurückgestellt, ist die Doku trotzdem richtig.
- **MR 5 vor MR 6:** Beide ändern die Worker-Optionen, `cli_help/worker.txt` und `spec/cli.json`.
- **P12c in MR 5, P12e in MR 7:** Der falsche Kommentar `pipeline.py:116-121` wird erst wahr, wenn es den Schalter gibt. Die `FromComponent`-Doku beschreibt erst nach MR 7 funktionierendes Verhalten.
- **MR 8 zuletzt:** Er hängt an Spike S1 und an E6 und ist die einzige Unit mit Transportänderung.

---

## 5. Units

### MR 1 – P7b: `task_sent_to_dlq` nur bei liegender Kopie

- **Befund (geprüft):** `RetryHandler.dead_letter` loggt am Ende immer `task_sent_to_dlq` auf ERROR (`worker/retry.py:310-311`). Das gilt auch im Zweig, in dem der DLQ-Publish scheiterte und `redeliver_in_place` die Nachricht zurückgab (`retry.py:301-309`, `placed=False`). Dort loggt der Code bereits `dlq_publish_failed` (ERROR).
- **Dateien:** `src/swarmq/worker/retry.py`; Doku `website/reference/logging.md`, `logging.de.md` (Zeilen zu `task_sent_to_dlq` und `dlq_publish_failed`).
- **Test-Liste** (`tests/unit/worker/test_retry.py`, Kopfkommentar ergänzen):
  1. DLQ-Publish scheitert mit `BrokerConnectionError`, ohne `on_publish_failure` → `dlq_publish_failed` einmal, **kein** `task_sent_to_dlq`, `redeliver` einmal. (RED)
  2. DLQ-Publish scheitert, mit `on_publish_failure` (DLX-Rückfall) → `task_sent_to_dlq` einmal (die Nachricht liegt über den DLX in `.XQ`). Charakterisierung, pinnt T9.
  3. `SettleFailedAfterPublish` → `placed=True`, `task_sent_to_dlq` einmal (Charakterisierung).
  4. Workflow-Fehlmarkierung scheitert vorübergehend (`fail_workflow` → `False`) → kein `task_sent_to_dlq` (der Code kehrt vorher zurück; pinnt das).
  5. Erfolg → `task_sent_to_dlq` einmal mit `error_type` (besteht schon, `test_retry.py:240-283`).
- **Doku:** `task_sent_to_dlq` erscheint nur, wenn die Kopie in `.XQ` liegt oder der DLX-Rückfall lief. Bei gescheitertem Publish mit Neuzustellung erscheint nur `dlq_publish_failed`.
- **Changelog (Fixed):** „`task_sent_to_dlq` is no longer logged when the dead-letter publish failed and the message was redelivered; `dlq_publish_failed` reports that case.“ / DE entsprechend.
- **Gates:** Q12 1–6.

### MR 2 – P8: `Retry` trotz `retry_exceptions`

- **Befund (geprüft):** `evaluate` prüft den Filter (`retry.py:95-97`) vor der Sonderbehandlung von `Retry` (`retry.py:106-108`). Ein `raise Retry(delay=…)` aus einem Task mit `retry_exceptions=[…]` ohne `Retry` landet terminal in der DLQ.
- **Dateien:** `src/swarmq/worker/retry.py`; Doku `website/guides/scheduling-and-retries.md`/`.de.md` (Abschnitt um Zeile 560-600), `website/reference/agent/decision-trees.md`/`.de.md` (Mermaid Zeile 104-112 bzw. 109-116 und Text 58-66), `website/reference/errors.md`/`.de.md` (Abschnitt `Retry`, Zeile 337ff.), `website/reference/agent/error-catalog-machine.md`/`.de.md` (Zeile 17: `controlled`).
- **Test-Liste** (`tests/unit/worker/test_retry.py`):
  1. Task mit `retry_exceptions=[KeyError]`, `max_retries=3`, `raise Retry()` → `retry_later` mit Backoff-Verzögerung, kein `dead_letter`. (RED)
  2. Wie 1 mit `Retry(delay=7)` → `delay_ms == 7000`.
  3. Wie 1, Budget erschöpft (`x-swarmq-retry-count == 3`) → terminal (`dead_letter`).
  4. `max_retries=0`, `raise Retry(delay=5)` → terminal (pinnt E4 A).
  5. `ValueError` nicht im Filter → weiter terminal (besteht, `test_retry.py:263`, `test_worker.py:1004`).
  6. CPU-Modus: `RetryHandler.evaluate` mit einer aus dem Envelope rekonstruierten `Retry(delay=5)` (`error_envelope` → `error()`) und Filter → `retry_later` mit 5000 ms. Unit, ohne Prozess.
  7. e2e (Wrapper): Task mit Filter wirft beim ersten Versuch `Retry(delay=0.1)` und gelingt beim zweiten → Ergebnis vorhanden, `TASK_RETRYING` einmal. In `tests/e2e/test_retry_band.py` oder einer neuen Datei `tests/e2e/test_retry_with_filter.py`.
- **Doku:**
  - `Retry` gilt unabhängig von `retry_exceptions`.
  - `Retry` verbraucht das Budget. Bei `max_retries=0` endet ein `Retry` terminal (DLQ, bei abgeschaltetem Dead-Lettering das Fehlerergebnis).
  - Im Entscheidungsbaum steht `Retry` als eigener Zweig vor dem Filter. Nebenbei korrigiert: „NoRetry → DLQ via DLX“ heißt richtig „Worker legt Kopie in die DLQ“ (Abschnitt 10).
- **Changelog (Changed):** „`raise Retry(...)` now retries even when the task sets `retry_exceptions` without listing `Retry`; it used to go straight to the DLQ. `Retry` still counts against `max_retries`; with `max_retries=0` it ends terminally.“ / DE.
- **Gates:** Q12 1–7.

### MR 3 – P9: Fehlerklasse im Ergebnis (`error_class`)

- **Befund (geprüft):**
  - `build_result_dict` (`cancellation.py:30-59`) schreibt nur `error_message`. `TaskError` ist leer (`exceptions.py:181-182`).
  - Die Doku verspricht `error_class` (`message-formats*.md:87,100`, `error-catalog-machine*.md:26,88`, `agent_spec.py:84`).
  - Das Schema `_TaskResult` (`agent_spec.py:592-599`) kennt das Feld nicht und verbietet unbekannte Felder.
  - Die Doku zeigt außerdem `"error_message": null`; der Code schreibt `""`.
- **Schreibstellen** (alle geprüft):
  - Ausführung und Ergebnis-Tail: `worker/results.py:61-80` (`_encode_with_fallback`, auch FIRE_AND_FORGET und der Rückfall bei `ResultSerializationError`).
  - Dead-Letter: `worker/retry.py:251-253` (`RetryHandler.dead_letter`). Neuer Parameter `error_class: str | None = None`; `_terminal_failure` übergibt `error_class_of(exc)`.
  - Gate-Pfade über `dead_letter` (nach E7): Lock (`pipeline.py:156-161`), Delivery-Count (`pipeline.py:164-166`), Ausführungsschutz (`pipeline.py:179-183`), unbekannter Task (`gates/unknown_task.py:112`).
  - Expiry-Gate: `worker/gates/expiry.py:86-97`.
  - Scheduler, unroutbarer Oneshot: `scheduler/unroutable.py:204-216`.
  - Workflows: `workflow_failure.py:100-113` (`fail`), `worker/terminal.py:55`, `worker/results.py:168`, Leser `workflow_orchestration.py:196-198` und `client/workflows.py:209-224`.
- **Dateien:**
  - neu `src/swarmq/worker/error_info.py`
  - `src/swarmq/cancellation.py`, `exceptions.py`, `worker/execution.py`, `worker/compute/executor.py`, `worker/results.py`, `worker/retry.py`, `worker/terminal.py`, `worker/gates/expiry.py`, `worker/gates/unknown_task.py`, `worker/gates/lock.py` und `delivery_count.py` (nur Durchreichen), `worker/pipeline.py` (Lambdas), `scheduler/unroutable.py`, `workflow_failure.py`, `workflow_orchestration.py`, `client/workflows.py`, `agent_spec.py`
- **Test-Liste** (TDD-Reihenfolge):
  - Unit, rein:
    1. `build_error_result_dict(..., error_class="builtins.ValueError")` enthält den Schlüssel; ohne Argument fehlt er (`test_cancellation.py`). (RED)
    2. `build_result_dict` für Erfolg und Cancel schreibt nie `error_class`.
    3. `error_class_of(ValueError())` → `"builtins.ValueError"`; eigene Klasse → `"modul.Qualname"`; verschachtelt → `"modul.Aussen.Innen"`; `RemoteTaskError(remote_type="x.Y")` → `"x.Y"` (`tests/unit/worker/test_error_info.py`).
    4. `error_text(ValueError())` → `"ValueError"`; `error_text(ValueError("x"))` → `"x"`.
    5. `TaskError("m").error_class is None`; `TaskError("m", error_class="a.B").error_class == "a.B"`; Pickle-Rundreise erhält beides; `TaskCancelledError` unverändert.
  - Unit, Worker:
    6. Task wirft `ValueError()` (leer), `max_retries=0` → das gespeicherte DLQ-Ergebnis hat `error_message == "ValueError"` und `error_class == "builtins.ValueError"`.
    7. Wie 6 mit `dead_letter=False` (Ergebnis-Tail) → dieselben Felder.
    8. FIRE_AND_FORGET-Fehler → Ergebnis mit `error_class`.
    9. Nicht serialisierbarer Rückgabewert → `error_class == "swarmq.exceptions.ResultSerializationError"`.
    10. Gate-Pfade nach E7: unbekannter Task (Gnadenfrist 0) → `swarmq.exceptions.TaskNotFoundError`; Ende der Lock-Wartezeit → `swarmq.exceptions.LockWaitExceeded`; Delivery-Limit über `x-acquired-count` → `swarmq.exceptions.RedeliveryLimitError`; `WorkerHungError` und Expiry → kein Schlüssel.
    11. CPU-Modus: Envelope mit `remote_exception` (nicht rekonstruierbar) → `error_class == remote_type`; rekonstruierbare Exception → deren Klasse (`tests/unit/worker/compute/test_executor.py`).
    12. Unroutbarer Oneshot → `error_class == "swarmq.exceptions.UnroutableMessage"` (`tests/unit/scheduler/test_fire_unroutable.py`).
  - Unit, Client und Workflow:
    13. `_unpack` mit `error_class` → `TaskError.error_class` gesetzt; ohne Schlüssel (Ergebnis eines 0.12-Workers) → `None` (`tests/unit/client/test_results.py`).
    14. `WorkflowAdvancer.fail(wf, "TaskError", msg, error_class="a.B")` speichert `error.class`; `WorkflowHandle.get()` wirft `TaskError` mit `error_class`; ein Zustand ohne `class` → `None` (`tests/unit/test_workflow_handle.py`, `test_workflow_advancer.py`).
    15. `fail_workflow_step` reicht die Klasse aus `dead_letter` durch (`tests/unit/worker/test_terminal.py`).
  - Contract und Spec:
    16. `test_result_schema_validates_real_builder_output` bekommt einen Fall mit `error_class` (erst RED gegen das alte Schema, dann `_TaskResult.error_class: str | None = None`).
    17. Spec neu erzeugen (`messages/result.schema.json`, `errors.json` wegen Docstring und `init_params` von `TaskError`). Eigener Zyklus.
    18. Golden additiv: `golden/values/error_result.msgpack` mit einem festen Fehlerergebnis samt `error_class`, Generator in `tests/integration/backend/_golden.py` (`value_fixtures`). Bestehende Dateien bleiben byte-identisch.
  - Integration (Wrapper):
    19. Echter Valkey: Der Worker speichert ein Fehlerergebnis; `client.get_result` wirft `TaskError` mit `error_class` und unverändertem Text.
  - Compat (opt-in `-m compat`, Basis 0.11.1):
    20. Neuer Worker, 0.11.1-Client: `get_result` wirft `TaskError` mit derselben Meldung.
- **Doku:**
  - `website/reference/agent/message-formats.md`/`.de.md`: `error_class` nur auf Fehlerergebnissen, Form nach E5, im CPU-Modus der Name aus dem Kind. `error_message` ist `""` statt `null`; eine leere Meldung wird auf den Klassennamen gefüllt. Die Decoding-Priorität nennt `TaskError.error_class`.
  - `website/reference/agent/error-catalog-machine.md`/`.de.md` (Zeile 88: „advisory“ bleibt).
  - `website/reference/api.md`/`.de.md` (Abschnitt `TaskError`), `website/reference/errors.md`/`.de.md` (`TaskError`).
  - `website/guides/workflows.md`/`.de.md` (Fehlerfall von `handle.get()`).
- **Changelog (Added):** „Error results carry `error_class` (fully-qualified exception class) and `TaskError.error_class` exposes it; in CPU mode it is the class reported by the child. An empty exception message is stored as the class name.“ (Changed): „The stored `error_message` of an exception without text is now its class name instead of an empty string.“ / DE.
- **Gates:** Q12 1–8. Q5: `agent_spec.py` (742) wächst um höchstens 2 Zeilen.

### MR 4 – P12: Doku an den Code angleichen (a, b, d, f, DLQ-Guide)

- **(a) `defining-tasks.md`/`.de.md:107-124`:** Die Doku verspricht einen `SerializationError` für `datetime`, `Decimal` und `UUID`. Die Klasse gibt es nicht. `encode_json` nutzt `msgspec.json.Encoder()` ohne Einschränkung (`serialization.py:17-26`).
  - Die Doku beschreibt künftig, was wie ankommt:
    - `datetime`/`date`/`time` als ISO-8601-`str`
    - `Decimal` und `UUID` als `str`
    - `bytes` als Base64-`str`
    - `tuple`/`set`/`frozenset` als `list`
    - Unbekannte Objekte führen beim `schedule()` zu `ResultSerializationError` (der Name ist irreführend, aber so heißt die Klasse).
    - Mit `unique_for` lehnt `canonical_encode` zusätzlich `bytes` und eigene Klassen mit `TypeError` ab (`serialization.py:76-112`).
  - Rückgabewerte (`run()`): msgpack mit Ext-Typen, d. h. `datetime`, `date`, `time`, `Decimal`, `UUID`, `set` und `frozenset` kommen typgleich zurück (`serialization.py:150-176`, `207-235`).
  - **Keine Verhaltensänderung.** Ob der Client diese Typen ablehnen soll, ist eine Produktfrage (Abschnitt 8/9).
- **(b) `middleware.md`/`.de.md:173-177`:** Je nach E3:
  - E3 A: „Eine Änderung am bestehenden `message["kwargs"]`-Dict bzw. an der `message["args"]`-Liste sieht `run()`, auch im CPU-Modus; eine Neuzuweisung nicht.“
  - E3 B: Die Doku beschreibt dasselbe Verhalten als „heute so, nicht garantiert“.
- **(d) `ResultExpiredError`:** T15. Doku `website/reference/errors.md`/`.de.md:304-313` bzw. `:323ff.` und `error-catalog-machine*.md:42-43`: „derzeit nie geworfen; nach Ablauf wirft `get_result` `ResultNotReadyError`“ (`client/results.py:79-83`).
- **(f) `@app.task`:** Kein Doku-Fehler; der Ausdruck steht nur im Celery-Vergleich. Nichts zu tun.
- **DLQ-Guide `dead-letter-queues.md`/`.de.md:281-286`:** Unbekannte Tasks laufen seit BZ-06 über den Worker-Dead-Letter mit Ablaufzeit, nicht mehr über den DLX. Die Aufzählung nennt künftig nur Delivery-Limit, `undecodable`, `body_too_large`, `middleware_chain_factory` und die DLX-Rückfälle. MR 8 ändert das erneut.
- **Test-Liste:**
  1. Charakterisierung (a), `tests/unit/test_serialization.py`: `encode_json` von `datetime`, `Decimal`, `UUID`, `bytes`, `set` ergibt die dokumentierten JSON-Werte; ein eigenes Objekt ergibt `ResultSerializationError`. Ohne Produktionscode; die Tests sind beim ersten Lauf grün und sichern die Doku ab.
  2. Nur bei E3 A: IO-Modus, eine Middleware fügt in `pre_execute` `message["kwargs"]["tenant"] = "t"` hinzu → `run()` erhält `tenant`; eine Neuzuweisung `message["kwargs"] = {...}` hat keine Wirkung (`tests/unit/test_middleware.py`). Charakterisierung.
  3. Nur bei E3 A: dasselbe im CPU-Modus über `ComputeExecutor._build_job` nach `run_pre_execute` (`tests/unit/worker/compute/test_executor.py`).
  4. (d) Docstring und `transport_field`: `test_error_catalog_known_semantics` erweitern (RED), dann `agent_spec.py` und `exceptions.py` ändern, danach Spec neu erzeugen (eigener Zyklus).
  5. (d) e2e `tests/e2e/combinations/test_failure_cascades.py:565` von `pytest.raises((ResultExpiredError, TaskError, Exception))` auf `pytest.raises(ResultNotReadyError)` schärfen, dazu den Docstring `:519`. Im RED-Schritt prüfen, dass der Test mit der geschärften Erwartung grün ist. Ist er rot, gilt die Stop-Condition.
- **Changelog (Changed, nur Doku):** „Docs: argument serialization, middleware argument changes, `ResultExpiredError` (never raised) and the DLQ retention note now describe the actual behaviour.“ / DE.
- **Gates:** Q12 1–8 (e2e nur der eine Test über den Wrapper).

### MR 5 – P1: Scheduler abschaltbar (+ P12c)

- **Befund (geprüft):**
  - `worker/lifecycle.py:157-182` erzeugt und startet `SchedulerLoop` immer.
  - `config.py` hat kein Feld dafür, `cli/parser.py` keine Option.
  - Der Kommentar `worker/pipeline.py:116-121` behauptet „only possible if explicitly disabled“.
  - Der Leader feuert auch `delay`/`eta`-Tasks, denn `client/schedule.py:203-218` legt sie im ScheduleStore ab.
- **Dateien:** `src/swarmq/config.py` (Feld, `_ENV_OVERRIDES`), `config_validation.py` (`_validate_scheduler_mode`), `scheduler/loop.py` (`candidate`), `worker/lifecycle.py` (Durchreichen, WARNING), `worker/pipeline.py` (Kommentar), `cli/parser.py` (`--no-scheduler`), `cli/worker_options.py` (`_OPTIONS`), `cli/env_check_cmd.py` (Abschnitt `scheduler`), `agent_spec.py` (`_CONFIG_CHOICES`).
- **Test-Liste:**
  - Unit (`tests/unit/test_config_scheduler_switch.py`):
    1. Default `SwarmQConfig(...).scheduler == "on"`. (RED)
    2. `SWARMQ_SCHEDULER=off` → `"off"`; Programmwert wird von der Env überschrieben (Konvention).
    3. Ungültiger Wert (`"no"`) → `ConfigurationError`, Meldung nennt `on`, `off`.
  - Unit (`tests/unit/scheduler/test_loop_candidate.py`):
    4. `SchedulerLoop(candidate=False).start()` meldet einmal (`report_cron`, `report_worker_version` aufgerufen) und startet keine Wahl: `lock_manager.acquire` wird nie aufgerufen, auch nach mehreren Takten (Event-gesteuert, kein `asyncio.sleep`).
    5. `candidate=False`: der Melde-Takt läuft weiter (zweite Meldung nach `scheduler_seen_interval_s`, gesteuerte Uhr).
    6. `candidate=False`: `stop()` sendet kein `SCHEDULER_LEADER_HANDOVER` und gibt keinen Lock frei.
    7. `candidate=True` (Default) unverändert: bestehende Tests in `tests/unit/scheduler/test_loop.py` bleiben grün.
  - Unit (`tests/unit/worker/test_lifecycle*.py`):
    8. `scheduler="off"` → `SchedulerLoop` mit `candidate=False`, WARNING `scheduler_disabled` genau einmal mit `hint`.
    9. `scheduler="on"` → kein `scheduler_disabled`.
  - Unit, CLI (`tests/unit/test_cli_worker_options.py`, `test_cli_parser.py`):
    10. `--no-scheduler` → `args.scheduler == "off"`; Flag > `SWARMQ_SCHEDULER=on` > Default.
    11. `worker_config_kwargs` enthält `scheduler` und nennt es in `resolved_fields`.
    12. Golden `tests/unit/fixtures/cli_help/worker.txt` (eigener Zyklus).
  - Unit, `swarmq check` (`tests/unit/test_cli_env_check.py`):
    13. `SWARMQ_SCHEDULER=off` → `report["scheduler"] == {"scheduler": "off"}`, Befund `scheduler_disabled` mit Stufe `warn`, `ok` bleibt `True`.
    14. Ohne Variable → `{"scheduler": "on"}`, kein Befund (`test_cli_env_check.py:98` bleibt grün).
  - Spec:
    15. `test_agent_spec.py`: `config.json` nennt `scheduler` mit `env_var == "SWARMQ_SCHEDULER"` und `choices == ["off", "on"]`; `cli.json` nennt `--no-scheduler`. Danach neu erzeugen.
  - Integration (Wrapper, `tests/integration/test_scheduler_switch.py`):
    16. Zwei Worker an einem Valkey, A mit `off`, B mit `on`. B wird Leader; A wird es auch nach Stopp von B nicht. A meldet seinen statischen Cron (`report:*` vorhanden), der Cron feuert über B.
    17. Nur ein Worker mit `off`: ein `delay`-Task feuert nicht innerhalb von 2 × `scheduler_poll_interval` (bewusste Wartezeit, begründet im Test). Danach startet ein Worker mit `on`, und der Task läuft.
- **Doku:**
  - `website/reference/configuration.md`/`.de.md`: neue Zeile `SWARMQ_SCHEDULER`.
  - `website/reference/cli.md`/`.de.md`: `--no-scheduler`.
  - `website/guides/scheduling-and-retries.md`/`.de.md`: neuer Abschnitt „Scheduler auf einzelnen Workern abschalten“, Use-Case (lokale Entwicklung gegen eine Kopie von Live-Daten), mit diesen Folgen:
    - Der Leader feuert Crons, Oneshots **und** `delay`/`eta`-Tasks. Haben alle Worker am selben Valkey/Namensraum den Scheduler aus, bleiben diese liegen, bis ein Worker mit `on` startet.
    - Prune, Adopt, die Prüfung verwaister Queues (BZ-14) und die stündliche Valkey-Prüfung (BZ-04) laufen nur beim Leader.
    - Die Meldungen laufen weiter, der Leader löscht die Crons dieses Workers also nicht.
  - `website/reference/logging.md`/`.de.md`: Event `scheduler_disabled`.
  - `website/guides/deployment.md`/`.de.md`: Hinweis in der Liste der Umgebungsvariablen.
  - Kommentar `pipeline.py:116-121` korrigieren (P12c).
- **Changelog (Added):** „`SWARMQ_SCHEDULER=on|off` and `swarmq worker --no-scheduler` keep a worker out of the scheduler leader election. It still reports its crons. If no worker runs the scheduler, crons and `delay`/`eta` tasks do not fire.“ / DE.
- **Gates:** Q12 1–8. Q5: `config.py` 668, `lifecycle.py` 607, `loop.py` 568, jeweils + ≤ 15.

### MR 6 – P6: Importpfad des Workers (`--app-dir`)

- **Befund (geprüft):**
  - Der Console-Script-Einstieg `swarmq = "swarmq.cli:console_main"` (`pyproject.toml:105`) hat in `sys.path[0]` das venv-`bin`, nicht das Arbeitsverzeichnis.
  - Module importieren `cli/worker_cmd.py:34` (Worker), `cli/worker_cmd.py:166` (`--configure`), `cli/inspect_cmd.py:59` (`inspect`) und `process_manager/check.py:47` (`find_spec` für `worker --check` und den Supervisor-Start).
  - `python -m swarmq.cli` hat das Arbeitsverzeichnis schon im Pfad, außer unter `-P`/`PYTHONSAFEPATH`.
  - Hilfetext `cli/parser.py:89-91`: „Must be importable from the current Python path.“
- **Dateien:** `src/swarmq/cli/_shared.py` (`apply_app_dir`, argparse-Typ `_app_dir_path`), `cli/__init__.py` (Aufruf in `main`), `cli/parser.py` (Option an `worker`), `cli/parser_ops.py` (Option an `inspect`), `process_manager/check.py` und `cli/worker_cmd.py` (Hinweis im ImportError).
- **Test-Liste:**
  - Unit (`tests/unit/test_cli_app_dir.py`):
    1. `apply_app_dir` mit ausdrücklichem Verzeichnis stellt den absoluten Pfad an `sys.path[0]` (per `monkeypatch` auf `sys.path`). (RED)
    2. Idempotent: Steht der Pfad schon an Stelle 0, kommt er nicht doppelt.
    3. Default nach E1 (bei C): ohne Flag und ohne `safe_path` → Arbeitsverzeichnis; mit `sys.flags.safe_path` (gepatcht) → nichts; ausdrückliches `--app-dir` unter `safe_path` → gesetzt.
    4. `--app-dir /gibt/es/nicht` → argparse-Fehler, Exit 2, Meldung nennt den Pfad.
    5. `main(["worker", "--module", "appmod", "--check"])` in einem Temp-Verzeichnis mit `appmod.py`, `sys.path[0]` auf ein fremdes `bin` gesetzt → JSON `ok: true`.
    6. `main(["inspect", "--module", "appmod"])` ebenso → Task gelistet.
    7. ImportError von `--module` → Meldung nennt `--app-dir` und das verwendete Verzeichnis.
    8. `--reload`: der Neustart per `os.execvpe` parst `sys.orig_argv` neu, `apply_app_dir` läuft erneut (Unit auf `main`).
    9. Golden `cli_help/worker.txt` und `inspect.txt` (eigener Zyklus); `test_worker_help_describes_current_supervisor_reload_and_check_semantics` bleibt grün.
    10. Spec: `cli.json` nennt `--app-dir` für `worker` und `inspect` (`test_cli_spec_arguments_match_parser_per_command`), neu erzeugen.
  - Integration (S2, `tests/integration/test_app_dir_spawn.py`, echte Prozesse, Begründung im Test):
    11. `spawn`-Kind aus einem Prozess mit vorangestelltem App-Verzeichnis importiert ein Modul, das nur dort liegt.
    12. Forkserver des CPU-Modus: Das Kind meldet `sys.path[0] == app_dir` und hat das Task-Modul in `sys.modules`, ohne es selbst nachzuladen. Das prüft, dass der Preload nicht still scheiterte.
  - e2e (Wrapper):
    13. Das echte Console-Script `.venv/bin/swarmq worker --module appmod --pool cpu` aus einem Temp-Arbeitsverzeichnis führt einen Task aus (`tests/e2e/test_app_dir_console_script.py`).
- **Doku:**
  - `website/reference/cli.md`/`.de.md`: `--app-dir`, Standard nach E1, Wirkung auf `--configure`, Supervisor-Kinder und CPU-Kinder.
  - **Risiko:** Lokale Dateien überdecken installierte Pakete (z. B. eine `valkey.py` im Arbeitsverzeichnis). `PYTHONSAFEPATH` bzw. `-P` schaltet das ab (bei E1 C).
  - `website/guides/deployment.md`/`.de.md` (Docker `WORKDIR`).
  - Hilfetext `cli/parser.py:89-91` anpassen.
- **Changelog (Added/Fixed):** „`swarmq worker` and `swarmq inspect` find task modules in the working directory again (`--app-dir`, default per E1); the console script used to search only the venv's `bin` directory.“ / DE.
- **Gates:** Q12 1–8. Q5: `cli/__init__.py` 526 + ≤ 3; `cli/parser.py` 364.

### MR 7 – P2: `FromComponent` und Aliase unter PEP 563, Validierung (+ P12e, N1)

- **Befund (geprüft):**
  - Die Vorprüfung `_raw_mentions_fromdishka` liefert für String-Annotationen nur `"FromDishka" in raw` (`dishka.py:312-320`). `Annotated[X, FromComponent("c")]` und Aliase (Phoenix `Depends`) werden unter `from __future__ import annotations` still nicht injiziert, ohne Warnung.
  - `validate_registered_tasks` läuft nur in `cli/worker_cmd.py:67-69`, nicht in `SwarmQ.start_worker()` (`app.py:245-285`) und nicht bei `worker --check`. `swarmq check` importiert kein Modul (Abschnitt 10).
  - **N1 (E9):** Auf 3.14 scheitert ein Task ohne Future-Import mit unauflösbarem Namen in `run()` schon an `task.py:476` (`inspect.signature`), auch ohne dishka (lokal reproduziert).
- **Dateien:** `src/swarmq/dishka.py`, `app.py` (`start_worker`), `cli/check_cmd.py` und/oder `process_manager/check.py` (nur bei E8 A, im Check-Pfad), `task.py` (nur bei E9 A).
- **Test-Liste** (`tests/unit/test_dishka_annotations.py`, Modul-Fixtures als Dateien unter `tests/unit/fixtures/dishka_modules/`, importiert per `importlib`):
  1. Modul mit Future-Import, `svc: Annotated[Svc, FromComponent("c")]` → Task gewrappt, `_dishka_injected_keys` enthält den Key mit Komponente `"c"`. (RED)
  2. Alias `Depends = FromDishka`, `svc: Depends[Svc]` unter Future-Import → gewrappt.
  3. Nicht-DI-Task mit Future-Import und `x: Undefined` → kein Fehler, nicht gewrappt (Regressionstest der Abwägung).
  4. Nicht-DI-Task mit `TYPE_CHECKING`-Import unter Future-Import → kein Fehler.
  5. String mit `FromDishka` und unauflösbarem Typ → weiter `NameError` mit Hinweis (besteht).
  6. Alias-Annotation mit Forward-Ref auf eine später im Modul definierte Klasse → bei der Definition übersprungen; `validate_registered_tasks` wirft `ConfigurationError` mit Task- und Parameternamen (T6).
  7. Bei Definition und Start unauflösbar → kein Fehler (Grenze, pinnt Abschnitt 7).
  8. PEP 649 ohne Future-Import, ausgewertetes `Annotated[Svc, FromComponent("c")]` → gewrappt (besteht schon; Absicherung).
  9. Eingebettet: `SwarmQ.start_worker()` mit DI-Task ohne Container → `ConfigurationError` vor `broker.connect` (`tests/unit/test_app.py`).
  10. Eingebettet mit auflösbarem Container → kein Fehler, `worker.start` läuft.
  11. Nur bei E8 A: `worker --check` mit DI-Task ohne Container → JSON `ok: false`, `error_type: "ConfigurationError"`, Exit 1. Der Supervisor-Start importiert weiter nicht (Spy auf `importlib.import_module` in `check_worker_config`).
  12. Nur bei E9 A: Nicht-DI-Task ohne Future-Import mit `x: Undefined` in `run()` → Klassendefinition gelingt (`tests/unit/test_task.py`). Reservierte Parameter werden weiter erkannt.
  13. CPU-Modus: Das Kind importiert das Modul neu, und das Wrappen bei der Klassendefinition gilt dort ebenso (Unit über `maybe_wrap_task` mit frisch geladenem Modul; e2e `tests/e2e/test_cpu_mode.py` um einen `FromComponent`-Task unter Future-Import erweitern, Wrapper).
- **Doku (P12e):**
  - `website/reference/api.md`/`.de.md` (Abschnitt DI, Zeile 960ff.): `FromComponent`, `Annotated[X, FromComponent("c")]`, Aliase, PEP 563/649. Die Annotation muss bei der Klassendefinition auflösbar sein, sonst `ConfigurationError` beim Start oder (unauflösbar) keine Injektion. Validierung jetzt auch eingebettet und bei `worker --check` (E8).
  - `website/guides/cpu-mode.md`/`.de.md` (Tabelle Zeile 22, Liste Zeile 62).
- **Changelog (Fixed):** „`Annotated[X, FromComponent(...)]` and aliases of `FromDishka` are injected under `from __future__ import annotations`; they were silently skipped. The DI check also runs in embedded workers (`SwarmQ.start_worker()`) and in `swarmq worker --check`.“ Bei E9 A zusätzlich: „On Python 3.14 a task without DI no longer fails at class definition because of an unresolvable annotation.“ / DE.
- **Gates:** Q12 1–7. Q5: `dishka.py` 397 + ≤ 40.

### MR 8 – P5b: Giftnachrichten mit Ablaufzeit in die XQ

- **Befund (geprüft):**
  - `body_too_large` (`worker/decode.py:94-107`), `undecodable` (`decode.py:126-135`) und `middleware_chain_factory` (`worker/pipeline.py:400-410`) gehen per `route_via_dlx` (`gates/__init__.py:320-350`, RabbitMQ `nack(requeue=False)`) ohne `expiration` in die XQ.
  - Unbekannte Tasks laufen dagegen über `RetryHandler.dead_letter` (`gates/unknown_task.py:78-117`). Die Kopie bekommt `expiration=dead_letter_ttl_s`, `x-swarmq-dead-lettered-at` und `x-swarmq-dead-letter-ttl-ms` (`broker/rabbitmq/delivery.py:133-154`) sowie den Traceback-Header.
- **Design:**
  - Neue Funktion `reject_to_dead_letter(deps, delivery, *, reason, error, task_id=None, task_name=None)` in `worker/gates/reject.py`:
    1. Header = Original-Header + `x-swarmq-traceback = bounded_header_text(f"{reason}: {error}")`.
    2. `dead_letter_copy(deps, delivery, headers)` (`gates/__init__.py:306-316`; `SettleFailedAfterPublish` zählt als liegend).
    3. Danach `TASK_REJECTED` mit unverändertem `reason` und Workflow-Fehlmarkierung best effort, wie heute in `route_via_dlx`.
    4. Scheitert die Kopie, folgt `route_via_dlx` mit demselben `reason` als Rückfall. Er feuert `TASK_REJECTED` selbst, also kein doppeltes Signal.
  - Die drei Pfade rufen `reject_to_dead_letter` statt `route_via_dlx`; bei E6 B bleibt `body_too_large` bei `route_via_dlx`.
  - **Regeln aus 0.12, geprüft:**
    - **BZ-08, Generation:** `dead_letter_generation` vergibt ohne Task-ID im Body keine Generation (`identity.py:141-153`). Bei `undecodable`/`body_too_large` gibt es daher keine Replay-ID, wie in MR 12 festgelegt. Bei `middleware_chain_factory` ist der Body dekodiert; die Kopie bekommt eine Generation wie jede Worker-DLQ-Kopie.
    - **Ausführungsschutz:** `GuardedBroker.dead_letter` setzt Done nur mit Lease (`guarded_broker.py:33-36`). Die Decode-Pfade liegen vor jedem Claim, also No-op. Den Middleware-Pfad im RED-Schritt prüfen.
    - **Q6 aus 0.12:** `_publish_to_dead_letter_queue` entfernt die Zustands-Header wie bei jeder DLQ-Kopie.
    - **Größenprüfung:** nach E6. Bei A: Die Kopie trägt denselben Body, den der Broker schon angenommen hat. `guard_headers` greift nur auf Header. Ist der Body für den Broker zu groß (anderes `max_message_size`), scheitert die Kopie, und der Rückfall `route_via_dlx` greift wie heute. Spike S1 klärt Aufwand und Speicher.
    - **FIRE_AND_FORGET** nach Früh-Ack: kein Verb (unverändert, `pipeline.py:405`).
  - Kein Eingriff in das öffentliche Protocol `swarmq.contracts.broker` (Q7).
- **Dateien:** neu `src/swarmq/worker/gates/reject.py`; `worker/decode.py`, `worker/pipeline.py`; Doku `website/guides/dead-letter-queues.md`/`.de.md`; Plan `docs/plans/2026-10-08-1541-fix-einbindungsfehler-plan.md` (KTD6, Grenze Zeile 223, mit Vermerk „geändert durch Plan 2026-10-12-1105, MR 8“).
- **Test-Liste:**
  - Unit (`tests/unit/worker/gates/test_reject.py`):
    1. `reject_to_dead_letter` ruft `broker.dead_letter` mit `x-swarmq-traceback == "undecodable: …"` und den Original-Headern, danach `TASK_REJECTED` genau einmal mit `reason="undecodable"`. (RED)
    2. `dead_letter` wirft `BrokerConnectionError` → `route_via_dlx` einmal, `TASK_REJECTED` genau einmal.
    3. `SettleFailedAfterPublish` → kein Rückfall, `TASK_REJECTED` einmal.
    4. Kopie und Rückfall scheitern → die Ausnahme geht durch wie heute bei `route_via_dlx`.
    5. Workflow-Header → `fail_workflow` best effort, `may_redeliver=False` (wie `_fail_workflow_best_effort`).
  - Unit, Pfade:
    6. `decode_and_validate` mit kaputtem JSON → `dead_letter` statt `route_via_dlx` (`tests/unit/worker/test_decode.py`).
    7. Fehlende Felder (`task.name`) → ebenso (`tests/unit/worker/test_worker.py:373-382`).
    8. Body über der Grenze → nach E6 (`tests/unit/worker/test_pipeline.py`, Signal bleibt `body_too_large`).
    9. Kaputte Middleware-Factory → `dead_letter`, Signal mit `task_id`/`task_name` (`test_pipeline.py:377-420`).
    10. FIRE_AND_FORGET nach Früh-Ack → kein Verb (besteht).
  - Unit, Broker:
    11. `RabbitMQBroker.dead_letter` mit undekodierbarem Body → `expiration` gesetzt, `x-swarmq-dead-lettered-at` gesetzt, **keine** `x-swarmq-replay-id` (`tests/unit/broker/test_dead_letter_ttl_header.py`).
  - Integration (Wrapper, `tests/integration/broker/test_reject_ttl.py`):
    12. Kaputte Nachricht in eine Queue publizieren, Worker läuft → in `.XQ` liegt sie mit `expiration` (ms aus `SWARMQ_DEAD_LETTER_TTL_S`) und `x-swarmq-dead-lettered-at`. `swarmq dlq list` zeigt sie.
    13. Nach E6 A: Body über der Grenze → ebenso, auf 4.3.6 (S1).
  - Mischbetrieb: Q2, keine eigene compat-Szene nötig (beide Wege enden in `.XQ`).
- **Doku:**
  - `dead-letter-queues.md`/`.de.md`, Tabelle „Three ways into the `.XQ`“ (Zeile 48-52): Die Zeile „Rejection by the worker“ nennt „Worker publishes a copy with retention“. Header jetzt `x-swarmq-traceback` (`<reason>: <error>`) plus Ablage-Stempel; `x-death` nur noch im Rückfall.
  - Abschnitt „Retention“ (Zeile 281-286): Ohne Ablaufzeit kommen nur noch Delivery-Limit und DLX-Rückfälle (sowie `body_too_large` bei E6 B).
  - Die Policy-Empfehlung (Zeile 288ff.) bleibt.
- **Changelog (Changed):** „Messages the worker rejects (`undecodable`, `middleware_chain_factory`, and `body_too_large` per E6) now go to `{queue}.XQ` as a copy with the dead-letter retention and the diagnostic header, like unregistered tasks; they no longer hold up expiry at the head of the `.XQ`. The broker's dead-letter exchange remains the fallback.“ / DE.
- **Gates:** Q12 1–7. Q5: `pipeline.py` 715 ± 0 (Aufruf ersetzt Aufruf).

---

## 6. Bestehende Tests und Golden-Dateien mit geänderter Erwartung (abschließend)

| MR | Test/Datei | Grund |
|---|---|---|
| 1 | keine | Neue Tests, keine geänderte Erwartung. Bricht im RED-Lauf ein bestehender Test, der `task_sent_to_dlq` im Redeliver-Zweig erwartet, gilt die Stop-Condition. |
| 2 | keine | Die bestehenden Filtertests nutzen `ValueError`/`ConnectionError`, nicht `Retry` (`test_retry.py:251-263`, `test_worker.py:1004-1068`). |
| 3 | `tests/unit/test_agent_spec.py::test_result_schema_validates_real_builder_output` (zusätzlicher Fall mit `error_class`) | optionales Feld im Schema |
| 3 | `website/reference/agent/spec/messages/result.schema.json`, `website/reference/agent/spec/errors.json` (`TaskError`: Docstring, `init_params`) | Q4-Generierung |
| 3 | `tests/integration/backend/_golden.py` (`value_fixtures` + `error_result.msgpack`), neue Datei `tests/integration/backend/golden/values/error_result.msgpack` (additiv) | Form des Fehlerergebnisses pinnen; `result.msgpack` bleibt byte-identisch |
| 3 | Tests mit exakter Gleichheit auf gespeicherte Fehlerergebnisse: geprüft, keine betroffen. `test_cancellation.py:111-124` bleibt grün, weil das Feld ohne Wert fehlt; die übrigen prüfen einzelne Felder. | – |
| 4 | `tests/e2e/combinations/test_failure_cascades.py:519,524,565` (`ResultNotReadyError` statt `(ResultExpiredError, TaskError, Exception)`) | P12d: Assertion schärfen |
| 4 | `tests/unit/test_agent_spec.py::test_error_catalog_known_semantics` (Fall `ResultExpiredError`), `website/reference/agent/spec/errors.json` | T15 |
| 5 | `tests/unit/fixtures/cli_help/worker.txt` (`--no-scheduler`) | neue Option |
| 5 | `website/reference/agent/spec/config.json`, `website/reference/agent/spec/cli.json` | Q4-Generierung |
| 6 | `tests/unit/fixtures/cli_help/worker.txt` (`--app-dir`, neuer `--module`-Hilfetext), `tests/unit/fixtures/cli_help/inspect.txt` (`--app-dir`) | neue Option; `root.txt` bleibt unverändert, weil die Option je Unterbefehl hängt |
| 6 | `website/reference/agent/spec/cli.json` | Q4-Generierung |
| 7 | nur bei E8 A: Tests von `swarmq worker --check`, die einen Lauf **ohne** Import erwarten (grep `find_spec`/`import_module` in `tests/unit/test_cli*.py`, `tests/unit/process_manager/test_check*.py`). Im RED-Schritt ermitteln und hier nachtragen, bevor sie geändert werden. | `--check` importiert |
| 7 | nur bei E9 A: keine erwartet. Prüfen: Tests, die einen `NameError` bei der Klassendefinition eines Nicht-DI-Tasks erwarten (grep `NameError` in `tests/unit/test_task.py`, `test_dishka.py`). | N1 |
| 8 | `tests/unit/worker/test_worker.py:356-382` (`test_invalid_json_nacked_no_requeue`, `test_missing_task_name_nacked_no_requeue`: `dead_letter` statt `route_via_dlx`) | P5b |
| 8 | `tests/unit/worker/test_retry.py:465-482` (`route_via_dlx.assert_awaited_once` → `dead_letter` für die undekodierbare Nachricht) | P5b |
| 8 | `tests/unit/worker/test_decode.py:23-37` (Fixture `_deps`: `broker.dead_letter = AsyncMock()`) | P5b |
| 8 | `tests/unit/worker/test_pipeline.py:318-420` (Reject-Tests): Signal-Assertions bleiben. Nur Erwartungen an Broker-Verben ändern sich, falls vorhanden. | P5b |
| 8 | prüfen: `tests/e2e/combinations/test__invariants.py` (grep `route_via_dlx`/`requeue=False`) | P5b |

Die Doku-Dateien (`website/**/*.md`) sind nicht golden-getestet (geprüft: kein Test liest `website/` außer der Spec unter `website/reference/agent/spec` und `website/llms.txt`).

---

## 7. Bekannte Grenzen (Changelog)

- **P1:** Schalten alle Worker am selben Valkey/Namensraum den Scheduler ab, feuern keine Crons, Oneshots und `delay`/`eta`-Tasks. Prune, Adopt, die Prüfung verwaister Queues und die stündliche Valkey-Prüfung ruhen. Ein 0.12-Worker ignoriert `SWARMQ_SCHEDULER`.
- **P2:** Eine DI-Annotation, die weder bei der Klassendefinition noch beim Start auflösbar ist (etwa nur unter `TYPE_CHECKING` importiert), wird nicht injiziert und nicht gemeldet. Das ist der Preis dafür, dass Tasks ohne DI nie brechen.
- **P5b:** Scheitert die Kopie, landet die Nachricht wie bisher ohne Ablaufzeit in `.XQ` (DLX-Rückfall). Bei E6 B gilt das für `body_too_large` immer. Die Broker-Policy auf `\.XQ$` bleibt die einzige vollständige Begrenzung.
- **P6:** Mit dem Standard nach E1 (A oder C) können Dateien im Arbeitsverzeichnis installierte Pakete überdecken.
- **P8:** `Retry` verbraucht das Budget. Bei `max_retries=0` endet `Retry` terminal (E4 A).
- **P9:** `error_class` ist beratend. Ergebnisse älterer Worker haben das Feld nicht. Auf Pfaden ohne Exception-Klasse fehlt es (E7). Im CPU-Modus steht dort der Name aus dem Kind, auch wenn sich die Klasse im Elternprozess nicht laden ließ.

---

## 8. Nicht in diesem Plan

| Punkt | Grund |
|---|---|
| A7 Skip-Veto in Middleware | Widerspricht der Middleware-Doku: Middleware entscheidet nicht über die Ausführung (`guides/middleware*.md`, „What middleware should not do“). |
| A8 eigene Header | Eigenes Feature mit Wire-Folgen (Header-Golden, Producer- und Worker-API). Gehört in einen eigenen Plan. |
| A9 Log-Niveau | Regel KTD11 (Logging-Pläne 2026-10-06). Georg entscheidet, dieser Plan ändert sie nicht (auch nicht in MR 1). |
| A12 | Durch BZ-01 (Delay-Stufen, 0.12) erledigt. |
| Guard-Freigabe durch den Supervisor | Widerspricht der Entscheidung „TTL 60 s bleibt“. |
| XQ-Queue-Argumente (`x-message-ttl`, `x-max-length` an `.XQ`) | Widerspricht KTD6 („Rest-TTL pro Nachricht statt Queue-Argument“, Plan 2026-10-08-1541) und BZ-15 (keine Argumentänderung an Bestandsqueues). P5b ist die konforme Alternative. |
| Client-Ablehnung von `datetime`/`Decimal`/`UUID` | Produktfrage. MR 4 korrigiert nur die Doku. |
| Middleware beim Scheduler-Feuer und bei `delay`/`eta` | Gehört zum A8-Feature. |

---

## 9. Folgearbeit

- **Produktfrage Serialisierung:** Soll `schedule()` `datetime`, `Decimal` und `UUID` ablehnen oder typgleich transportieren (P12a)?
- **`swarmq check`:** Lebt überhaupt ein Leader (Leader-Lock in Valkey)? Nötig, sobald `SWARMQ_SCHEDULER=off` verbreitet ist.
- **swarmv:** `error_class` anzeigen. Prüfen, ob swarmv Ergebnisse mit strenger Struktur dekodiert (das zusätzliche Feld dürfte sonst stören).
- **A8 eigene Header** samt Middleware beim Scheduler-Feuer und bei `delay`/`eta`.
- **N1**, falls E9 nicht A.
- **`ResultExpiredError`** wirklich werfen (Trennung „abgelaufen“ von „noch nicht fertig“), falls gewünscht.
- **Release:** 0.12.0 (final) oder 0.12.1 mit diesen Punkten; Tail bei Georg.

---

## 10. Abgleich mit der Vorprüfung (Abweichungen und Nebenbefunde)

| Punkt | Vorprüfung | Geprüfter Stand |
|---|---|---|
| P2 / P6 | `swarmq check` ist betroffen (Validierung, `--app-dir`). | `swarmq check` (`cli/env_check_cmd.py`) importiert kein Modul und hat kein `--module`. Betroffen sind `swarmq worker --check` (nur `find_spec`, `process_manager/check.py:47-66`) und `swarmq inspect`. |
| P8 | CPU-Modus prüfen (`compute/envelope.py`). | Die Datei liegt unter `worker/compute/envelope.py`. `Retry` ist picklebar und behält `delay` (Test `test_envelope.py:31`); der Filter läuft im gemeinsamen `RetryHandler`. Im CPU-Modus ist nichts zusätzlich zu ändern. |
| P5b | `max_body_bytes` | Feld `max_message_body_bytes`, Env `SWARMQ_MAX_MESSAGE_BODY_BYTES`, Default 16 MiB (`config.py:337`). |
| P5b | Pfad `pipeline.py:~407` | `pipeline.py:400-410` (`_resolve_chain_or_route`). |
| P7b | Falsches `task_sent_to_dlq` bei `placed=False` | Nur im Redeliver-Zweig falsch. Im Zweig `on_publish_failure` liegt die Nachricht über den DLX in der XQ. `dlq_publish_failed` existiert (ERROR). |
| P9 | Schema/Golden prüfen | Das Schema `_TaskResult` verbietet unbekannte Felder und muss erweitert werden. `golden/values/result.msgpack` ist ein **Erfolgs**-Ergebnis und bleibt mit T11 unverändert. Die Doku zeigt `error_message: null`, der Code schreibt `""`. `worker/terminal.py:55` setzt den Workflow-Fehlertyp immer auf `"TaskError"`. |
| P12d | e2e-Assertion | `get_result` nach Ablauf wirft `ResultNotReadyError` (`client/results.py:79-83`). Daran richtet sich die geschärfte Assertion. |
| P12b | nur kwargs | Auch die `args`-Liste ist dasselbe Objekt (`pipeline.py:378-385`). Im CPU-Modus wird erst nach `pre_execute` kopiert (`compute/executor.py:123-137`). |
| P6 | `python -m` | `python -m swarmq.cli` hat das Arbeitsverzeichnis schon im Pfad (außer `-P`). Nur das Console-Script ist betroffen; die compat-Suite startet mit `python -m` und ist daher nicht betroffen. |
| Nebenbefund N1 | – | Python 3.14 (PEP 649): `task.py:476` (`inspect.signature`) wirft bei einem unauflösbaren Namen in `run()` ohne Future-Import einen `NameError`. Lokal reproduziert, auch ohne dishka. Die dokumentierte Abwägung gilt heute nur unter PEP 563 (E9). |
| Nebenbefund | – | Der Entscheidungsbaum (`decision-trees*.md:104-112`) zeigt „NoRetry → DLQ via DLX“. Tatsächlich legt der Worker eine Kopie ab. Wird in MR 2 korrigiert. |
| Zeilen | – | Bestätigt: `lifecycle.py:157-182`, `pipeline.py:116-121`, `retry.py:95-97` und `298-311`, `cancellation.py:30-59`, `client/schedule.py:203-218`, `dishka.py:312-320`, `cli/worker_cmd.py:34,67-69`, `cli/parser.py:89-91`, `decode.py:94-107,126-135`, `gates/unknown_task.py:78-117`, `dead-letter-queues.md:281-286`, `test_failure_cascades.py:565`, `defining-tasks.md:118-124`, `middleware.md:175-177`, `message-formats.md:87,100`. |
