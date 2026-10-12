---
title: Konfliktfreie Punkte aus der Phoenix-Migrationsanalyse (P1, P2, P5b, P6, P7b, P8, P9, P12)
type: feat
date: 2026-10-12
revision: 2 (Stress-Review eingearbeitet, 2026-10-12; Befunde und Umgang in Abschnitt 10. Revision 1: Erstfassung, alle Code-Stellen gegen origin/main 5bfa106 geprüft)
artifact_readiness: decisions-made (Abschnitt 1.2, von Georg am 2026-10-12 entschieden)
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
  - `--no-scheduler` kommt in die Tabelle `_OPTIONS` (`cli/worker_options.py:20-28`). Damit gilt Flag > Env > Default wie bei `--burst`.
  - \* (Rev. 2, Review P0-2) Der Wert reist **über `WorkerConfig`** an die Supervisor-Kinder: neues Feld `scheduler: str = "on"` in `process_manager/config.py:40-75`, gesetzt in `process_manager/run.py:157-176` aus `args.scheduler`. Die Kinder bauen ihre Namespace aus den `WorkerConfig`-Feldern (`entry.py:136-155`); erst damit landet `scheduler` im Kind in `worker_config_kwargs` und `resolved_fields`. Ohne das Feld verwürfe der Supervisor das Flag still, die Env-Variable wirkte dagegen.
  - Env-Art `"str"` wie `SWARMQ_SCHEDULER_PRUNE` (`config.py:475`), also case-sensitiv (Review P2-25).
  - Die Werteliste kommt in `_CONFIG_CHOICES` (`agent_spec.py:411`), damit die Spec sie nennt. `config_validation.py:41-55` bekommt im Protocol-Stub `scheduler: str` und die ClassVar der gültigen Werte (mypy/basedpyright).
- \* **T2 (P1) Ort des Schalters:** `SchedulerLoop` bekommt den Parameter `candidate: bool = True`. Mit `False` startet `start()` nur den Melde-Takt (`_report_loop` plus der einmalige `_report_crons()`), nicht `_election_loop`, `_poll_loop` und `_adopt_loop` (`scheduler/loop.py:207-216`).
  - Begründung: Die Meldungen (Crons nach KTD10, Worker-Version für KTD11, konsumierte Queues für BZ-14, Epoch für BZ-04) laufen in `_report_crons` (`loop.py:312-334`). Fallen sie weg, entfernt der Leader die statischen Crons dieses Workers nach `scheduler_prune_after_s` (`prune.py`), und `swarmq check` meldet seine Queues als verwaist.
  - `stop()` und `abort()` bleiben unverändert. Der Zweig `state == "leader"` wird nie erreicht.
  - (Rev. 2, Review P1-12) Der Idempotenz-Wächter in `start()` prüft heute `self._election_task is not None` (`loop.py:208`). Bei `candidate=False` bliebe er `None`, ein zweiter `start()` startete einen zweiten `_report_loop`. Der Wächter wird auf `self._report_task` umgestellt (läuft in beiden Fällen), mit Test.
- \* **T3 (P1) Start-WARNING:** `scheduler_disabled` (WARNING, einmal je Worker-Start, Logger `swarmq.worker.lifecycle`) mit einem `hint`: Dieser Worker bewirbt sich nicht um die Leader-Rolle. Crons, `delay`/`eta`-Tasks und Oneshots feuern nur, wenn ein anderer Worker am selben Valkey (und Namensraum) Leader ist.
  - Die WARNING kommt je Worker-Prozess, also N-mal bei `--processes N` und erneut nach jedem Recycle (Doku nennt das; Review P2-25).
- \* **T4 (P1) `swarmq check`:** neuer Abschnitt `scheduler` mit `{"scheduler": "on"|"off"}` aus der Umgebung des Aufrufs. Bei `off` gibt es den Befund `scheduler_disabled` mit Stufe `"warning"` (Exit 0; Review P1-11: die Stufe heißt `"warning"`, nicht `warn`, `env_check_cmd.py:164ff.`). Der Bericht wächst nur additiv (`build_report`, `cli/env_check_cmd.py:315-330`). Eine Prüfung, ob überhaupt ein Leader lebt, ist Folgearbeit (Abschnitt 9).
  - `_render_table` (`env_check_cmd.py:332-342`) gibt nur die fest genannten Abschnitte `valkey`, `shutdown`, `queues` aus; `scheduler` kommt in diese Liste (mit Test der Tabellenausgabe).
  - Ungültiger Wert (`SWARMQ_SCHEDULER=no`): Befund `scheduler_invalid` mit Stufe `"error"`, `ok: false`, damit Exit 78 (`CONFIG_ERROR_EXIT_CODE`). Begründung: Der Worker lehnt denselben Wert beim Start mit `ConfigurationError` (Exit 78) ab; `check` soll genau das vorab melden. Der Abschnitt zeigt den Rohwert.
- \* **T5 (P2) Tolerante Auswertung** (Rev. 2 überarbeitet nach Review P0-4, P1-16, P1-17, P1-19):
  - **Lesen immer tolerant:** `maybe_wrap_task` liest Roh-Annotationen per `annotationlib.get_annotations(func, format=Format.FORWARDREF)` statt `getattr(func, "__annotations__", {})` (`dishka.py:335`; unter PEP 649 wirft der Zugriff sonst `NameError`) und die Signatur per `inspect.signature(func, annotation_format=Format.FORWARDREF)`. Aufgelöst wird per `typing.get_type_hints(func, include_extras=True, format=Format.FORWARDREF)`.
  - Vorab geprüft auf CPython 3.14.4: Unter `from __future__ import annotations` liefert der Aufruf für `a: Annotated[C, "m"]` das ausgewertete `Annotated`, für `b: Undefined` ein `ForwardRef('Undefined')`. Es gibt keinen `NameError`. Eine kaputte String-Annotation (`"list[int"`) ergibt dagegen `SyntaxError` (Probe `pedge.py`).
  - **Regel 1 – bestehender strikter Pfad bleibt:** Nennt eine Roh-Annotation als Text `FromDishka` (PEP-563-String oder der Text eines ganzen `ForwardRef`, z. B. `ForwardRef('FromDishka[Later] | None')` unter PEP 649), läuft die Methode unverändert durch den strikten Pfad (`get_type_hints` strikt, `NameError` mit Hinweis, `dishka.py:344-351`). Das hält auch den Fall `svc: FromDishka[Svc], x: OnlyTC` unter PEP 563 beim heutigen `NameError`.
  - **Regel 2 – kein DI, nie ein Fehler:** `SyntaxError` und `TypeError` aus der toleranten Auswertung werden gefangen; die Methode gilt dann als ohne DI und wird übersprungen (heute bricht daran kein Task ohne DI). Ergibt kein Parameter einen DI-Key (`default_parse_dependency`), wird nicht gewrappt; die R18-Erkennung (`_annotation_mentions_fromdishka`) läuft auf den ausgewerteten Hints wie heute.
  - **Regel 3 – DI-Key, alles auflösbar:** Enthält kein Key einen `ForwardRef` und ist keine andere Annotation der Methode ein `ForwardRef`, wird gewrappt (`wrap_injection`, dessen striktes `get_type_hints`, dishka `base.py:114`, dann nicht scheitert).
  - **Regel 4 – DI-Key, aber etwas unaufgelöst** (Key mit `ForwardRef` darin, z. B. `Annotated[ForwardRef('Later'), FromComponent("c")]`, oder ein unauflösbarer Nicht-DI-Parameter daneben): **nicht** an `wrap_injection` übergeben (das würfe einen rohen `NameError`, Probe `p_classdef.py`).
    - Unter PEP 563 (Roh-Annotationen sind Strings): Die Methode bleibt ungewrappt, wie heute bei Aliasen; T6 meldet sie beim Start.
    - Unter PEP 649 (keine Strings): `NameError` mit Hinweis bei der Klassendefinition. Das erhält das heutige Verhalten, denn heute scheitert ein solcher DI-Task schon an `inspect.signature` (`task.py:476`, `dishka.py:334`). E9 nimmt nur Tasks **ohne** DI aus.
    - Begründung gegen den Review-Vorschlag (strikter Hinweis-`NameError` auch unter PEP 563 für Aliase): Das wäre ein neuer Importfehler für Alias-Tasks, die heute importierbar sind (Hauptbedingung). Der Weg über T6 bleibt im geplanten Verhalten „ungewrappte DI-Methode → `ConfigurationError` beim Start“.
  - Die Abwägung aus `docs/solutions/architecture-patterns/dishka-di-integration-async-task-queue.md` (Abschnitt 2 „Gate the eager get_type_hints…“) bleibt erhalten: Ein Task ohne DI bricht nie. Ein nicht auflösbarer Name ist kein Fehler, er wird übersprungen.
  - **Bewusst neu (im Plan, Changelog):** Unter PEP 563 wird `Depends[X] | None` (Alias, optional) jetzt wie `FromDishka[X] | None` von R18 erkannt und beim Start als wirkungslos abgelehnt (`ConfigurationError`). Unter PEP 649 ist das heute schon so.
  - Begründung gegen die längere String-Suche: Aliase (Phoenix `Depends`) lassen sich nicht per Teilstring erkennen.
- \* **T6 (P2) Nachprüfung beim Start:** `validate_registered_tasks` wertet die Annotationen nicht gewrappter Lifecycle-Methoden erneut tolerant aus. Dann ist das Modul vollständig geladen, spätere Forward-Refs lösen sich auf. Ergibt sich dabei ein DI-Key, folgt ein `ConfigurationError` (Exit 78). Er nennt Task und Parameter und erklärt, dass die Annotation bei der Klassendefinition auflösbar sein muss. Bleibt beim Start ein Nicht-DI-Parameter derselben Methode unauflösbar (Regel 4, Review P1-17), nennt die Meldung diesen Parameter als Ursache.
  - Begründung: Ein spätes Wrappen beim Start griffe nicht in Kindprozessen (CPU-Modus, Supervisor-Kinder importieren neu). Lautes Scheitern ist besser als stilles Nicht-Injizieren.
  - Ergibt eine Annotation auch beim Start keinen erkennbaren DI-Key (unauflösbar, ohne Marker), gibt es keinen Fehler und keine Warnung. Das ist eine Grenze, Abschnitt 7.
- \* **T7 (P2) Validierung im eingebetteten Worker:** `SwarmQ.start_worker()` (`app.py:245`) ruft `validate_registered_tasks` mit den Task-Klassen der App-Registry auf, nach der bestehenden Prüfung auf injizierte Verbindungen (deren Fehler und Tests `test_app.py:115-149` bleiben vorn) und vor `worker.start()`, also vor jeder Verbindung. Der frühe Aufruf in `cli/worker_cmd.py:67-69` bleibt, damit die Fehlerreihenfolge der CLI gleich bleibt (DI vor `SWARMQ_BROKER_URL`). Ein doppelter Aufruf ist idempotent.
  - (Rev. 2, Review P0-5) `SwarmQ.registry` kopiert das globale `Task._registered_tasks` (`app.py:66-68`). In der Unit-Suite registriert `tests/unit/worker/test_worker_dishka.py:58-71` DI-Tasks auf Modulebene ohne Container; die Session-Fixture in `tests/conftest.py:18-21` stellt nur den Stand nach der Sammlung wieder her. Ohne Isolation würfe `start_worker()` in `tests/unit/test_app.py` daher `ConfigurationError`. Die betroffenen `test_app.py`-Tests bekommen eine Fixture, die `Task._registered_tasks` vor dem Bau der App leert (Abschnitt 6).
- \* **T8 (P6) Ort:** Die Funktion `apply_app_dir(args)` liegt in `cli/_shared.py` und wird in `cli.main()` direkt nach dem Parsen aufgerufen, vor jeder Verzweigung.
  - Begründung: `_shared` steht schon in der eingefrorenen Oberfläche von `swarmq.cli` (`test_cli_module_surface.py:152`). Ein neues Submodul oder ein neuer Name in `cli/__init__.py` würde `test_no_symbols_added` brechen.
  - (Rev. 2, Review P0-3) Aufruf ausdrücklich als Modulattribut `_shared.apply_app_dir(args)`. **Verboten** ist, `apply_app_dir` in die Import-Liste `from swarmq.cli._shared import (...)` (`cli/__init__.py:49`) aufzunehmen: Jeder so importierte Name wird Attribut von `swarmq.cli`.
  - (Rev. 2, Review P1-13, P2-26) Semantik: Der absolute Pfad kommt an `sys.path[0]`; steht er dort schon, passiert nichts (ein späteres Vorkommen bleibt stehen, wie bei `python -m`). `safe_path` wird als Parameter injiziert (Default `sys.flags.safe_path`), weil `sys.flags` schreibgeschützt ist. Wirft `os.getcwd()` `FileNotFoundError` (gelöschtes Arbeitsverzeichnis), wird ohne Option still nichts vorangestellt.
  - (Rev. 2, Review P1-14) `_is_check_invocation` (`check_cmd.py:211-225`) bekommt `--app-dir` als Wert-Option im Detektor, sonst würde ein Wert wie `--app-dir --check` falsch gelesen.
  - Die Option `--app-dir DIR` hängt an den Unterbefehlen `worker` (auch `--check`) und `inspect`. `swarmq check` importiert kein Modul und bekommt die Option nicht (Abschnitt 10).
  - Kinder erben den Pfad, weil er vor Spawn und Forkserver gesetzt ist (Spike S2).
- \* **T9 (P7b) Kein neues Event:** Im Zweig „DLQ-Publish gescheitert, neu zugestellt“ (`worker/retry.py:298-309`) entfällt `task_sent_to_dlq`. Das bestehende `dlq_publish_failed` (ERROR, `retry.py:303`) beschreibt den Fall schon.
  - (Rev. 2, Review P1-10) Exakte Bedingung: `task_sent_to_dlq` genau dann, wenn `placed or on_publish_failure is not None`. `placed` wird schon nach `dead_letter_copy` gesetzt (`retry.py:284-286`), also vor `maybe_release_unique` und `TASK_DLQ`. Wirft einer der beiden, liegt die Kopie trotzdem, und das Event bleibt. Die vorbestehende Doppelbehandlung in diesem Fall (Kopie liegt, trotzdem `dlq_publish_failed` und Redeliver) ändert MR 1 nicht (Abschnitt 9).
  - Im Zweig mit `on_publish_failure` (DLX-Rückfall der Lock- und Unknown-Pfade) bleibt `task_sent_to_dlq`, denn die Nachricht liegt danach über den Broker-DLX in der XQ.
  - KTD11 (Log-Niveau) bleibt unverändert.
- \* **T10 (P8) Reihenfolge in `evaluate`:** Der Filter `retry_exceptions` (`retry.py:96-97`) gilt nicht für `Retry`. `NoRetry` (Schritt 1) und die Budgetprüfung (Schritt 4) bleiben wie sie sind.
  - Der CPU-Modus braucht keine eigene Änderung: `Retry` ist picklebar und behält `delay` (`tests/unit/worker/compute/test_envelope.py:31`). Die Bewertung läuft im selben `RetryHandler` (`pipeline.py:543`).
- \* **T11 (P9) Feld nur auf Fehlerergebnissen:** `build_result_dict` bekommt `error_class: str | None = None`. Der Schlüssel wird nur geschrieben, wenn ein Wert vorliegt.
  - Begründung: Das Erfolgs-Ergebnis bleibt byte-identisch, also auch `golden/values/result.msgpack` (`test_golden_fixtures.py:49-52`). Ebenso `test_cancellation.py:111-124` (exakte Dict-Gleichheit ohne das Feld).
  - Im Schema `_TaskResult` (`agent_spec.py:592-599`, `forbid_unknown_fields`) kommt `error_class: str | None = None` ans Ende, als optionales Feld.
- \* **T12 (P9) Helfer** (Rev. 2 überarbeitet nach Review P1-6, P1-8): `error_class_of(exc)` liegt im neuen Modul `swarmq/worker/error_info.py`.
  - `error_class_of` liefert `f"{type(exc).__module__}.{type(exc).__qualname__}"`, bei `RemoteTaskError` stattdessen `exc.remote_type` (`worker/compute/envelope.py:21-26`).
  - **Keine Füllung leerer Meldungen.** Revision 1 wollte eine leere Meldung auf den Klassennamen füllen. Das wirkte weit über das Ergebnis hinaus: auf `x-swarmq-traceback` der Retry- und DLQ-Kopie (`retry.py:140,279`), auf `TASK_DLQ error=` und `task_sent_to_dlq` (`retry.py:296,311`), auf `error.message` im Workflow-Zustand und auf den `TaskError`-Text bei 0.11.1- und 0.12-Clients (z. B. `raise Retry()` → `"Retry"`). Das ist ungeplantes Verhalten im Mischbetrieb (Hauptbedingung). `error_message` bleibt daher `""` wie heute; die Klasse steht in `error_class`. Der Helfer `error_text` entfällt. Ob leere Meldungen gefüllt werden sollen, steht in Abschnitt 9.
  - **Wert je Pfad (E7 A wörtlich: echte Task-Exception oder öffentliche swarmq-Klasse, sonst `null`):**
    - Exception aus dem Task (IO-Modus, im CPU-Modus rekonstruiert) → deren Klasse.
    - `RemoteTaskError` mit `remote_type` → `remote_type`; mit `remote_type="unknown"` (`envelope.py:108`, Kind meldete keine Details) → `null`.
    - Vom Worker nur als Text erzeugte Fehler ohne öffentliche Klasse → `null`: harter Timeout und Drain-Abbruch (`RuntimeError(msg)`, `execution.py:198`, `compute/executor.py:209-214`), `SlotUnavailable`, `ChildDied` und sonstige Pool-Fehler (`compute/executor.py:216-224`; interne Klassen aus `swarmq.worker.compute.pool` gehen nicht auf den Wire). Umsetzung: Die Executoren markieren diese Exceptions über einen Helfer in `error_info.py` (z. B. `mark_worker_error(exc)`), `error_class_of` liefert für markierte `None`. `error_type` in den Logs bleibt unverändert.
    - Gate-Pfade: siehe T17.
- \* **T13 (P9) Workflows:** `WorkflowAdvancer.fail(wf_id, error_type, error_message, *, error_class=None)` speichert zusätzlich `error.class` (additiv), **nur wenn ein Wert vorliegt** (wie T11). Aufrufer übergeben das Keyword nur mit Wert (Rev. 2, Review P0-1: `test_workflow_advancer.py:743,825` und `test_terminal.py:95` prüfen exakte Gleichheit bzw. `assert_awaited_once_with` ohne Keyword und bleiben so grün). Das bisherige `error.type` bleibt, auch das fest verdrahtete `"TaskError"` aus `worker/terminal.py:55`. `WorkflowHandle._resolve_terminal` (`workflow_orchestration.py:196-198`) und `client/workflows._unpack` (`client/workflows.py:222-223`) reichen die Klasse an `TaskError.error_class` durch.
- \* **T14 (P9) `TaskError`:** Die Klasse bekommt `__init__(self, *args, error_class: str | None = None)` und das Attribut `error_class`. `TaskError("x")` bleibt gültig, `TaskCancelledError` bleibt unverändert, und die Klasse bleibt picklebar (Test).
- \* **T15 (P12d) `ResultExpiredError`:** Die Klasse bekommt einen Docstring: „Derzeit nie geworfen; stabiler Hook für eine spätere Trennung von abgelaufen und noch nicht fertig.“ `_ERROR_SEMANTICS["ResultExpiredError"]["transport_field"]` wird `"not raised (reserved)"`. Beides fließt per Generierung in `spec/errors.json`.
- \* **T16 (P5b) Kein neuer Header:** Grund und Fehlertext reisen im bestehenden `x-swarmq-traceback` als `"<reason>: <error>"` (über `bounded_header_text`). Damit bleibt `golden/headers.txt` unverändert.
  - `TASK_REJECTED` feuert weiter mit denselben `reason`-Werten, nach der liegenden Kopie, genau einmal. Die Metrik `swarmq_tasks_rejected_total{reason}` bleibt.
  - `TASK_DLQ` feuert auf diesen Pfaden nicht. Ein Ergebnis wird nicht gespeichert, wie heute: Nur der Transport ändert sich. `reject_to_dead_letter` gibt selbst kein Unique-Flag frei; die bestehende Freigabe der Pipeline nach einem Middleware-Factory-Fehler (`test_worker.py:2772-2803`) bleibt unverändert.
  - `swarmq dlq list` zeigt für diese Nachrichten neu `undecodable: …` usw. in `ERROR_PREVIEW` (`dlq_cmd.py:44-47`); das kommt in den Changelog (Review P2-23).
- \* **T17 (P9, E7) Transport der Klasse über die Gate-Pfade** (Rev. 2, Review P1-7): Gates und Pipeline-Lambdas bleiben unverändert (feste Stelligkeit: `delivery_count.py:37`, `lock.py:164-170`, `unknown_task.py:31-35`, `pipeline.py:158,165,182,453`; `test_delivery_count.py:109` entpackt genau vier Argumente).
  - `RetryHandler.dead_letter` bekommt das Keyword `error_class` mit einem Sentinel als Default. Nur `_terminal_failure` übergibt `error_class=error_class_of(exc)` (auch `None`).
  - Ohne Keyword bildet `dead_letter` `error_type` über eine feste Tabelle in `error_info.py` ab: `TaskNotFoundError`, `LockWaitExceeded`, `RedeliveryLimitError` → `swarmq.exceptions.<Name>`. Jeder andere `error_type` → `null`, darunter `WorkerHungError` (Delivery-Count und Ausführungsschutz) und `ValueError` der Lock-Render-Pfade (`lock.py:216,242`; kein Task-Fehler, keine swarmq-Klasse, E7 A).
  - Begründung: schont `pipeline.py` (715 Zeilen) und vermeidet Signaturänderungen an vier Callables samt ihren Tests.
- \* **T18 (P2, E8) Ort und Form der DI-Prüfung bei `worker --check`** (Rev. 2, Review P0-5, P1-18):
  - Der Import gehört nur in `cli/check_cmd.py::_run_worker_check`, nach `check_worker_config`. Nicht in `process_manager/check.py`, sonst importierte auch der Supervisor-Start, und dessen Meldungen änderten sich (`test_cli_multiprocess.py:261`).
  - Der Import läuft unter `contextlib.redirect_stdout(sys.stderr)`: Auf stdout steht genau eine JSON-Zeile (`tests/e2e/test_logging_e2e_runtime.py:375`), Ausgaben des Anwendungsmoduls dürfen sie nicht stören.
  - Geprüft werden nur die Tasks, die der Import neu registriert hat (Differenz von `Task._registered_tasks` vor und nach dem Import). Im kurzlebigen Check-Prozess ist das dasselbe wie „alle“. In der Unit-Suite verhindert es, dass die rund 20 `--check`-Tests mit `--module=json` an global registrierten DI-Testtasks scheitern (`test_cli_multiprocess.py:237-626`, `test_cli_pool.py:93`, `test_namespace_guard.py:104`).
  - Der Hilfetext von `--check` (`cli/parser.py:341`) nennt künftig auch die DI-Prüfung. Damit ändern sich `cli_help/worker.txt` und `spec/cli.json` auch in MR 7 (Abschnitt 6).

### 1.2 Von Georg am 2026-10-12 entschieden

Alle neun Fragen wurden wie empfohlen entschieden („mach das so“). Es gilt jeweils die Spalte „Empfehlung“: E1 C, E2 A, E3 A, E4 A, E5 A, E6 A bei bestandenem Spike S1, sonst B, E7 A, E8 A, E9 A (in MR 7).

| ID | Frage | Optionen | Empfehlung (= Entscheidung) |
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

- **Q1 Wire-Kompatibilität:** Neue Felder sind additiv und optional. Das gilt für `error_class` im Ergebnis, `error.class` im Workflow-Zustand und `error_class` an `TaskError`. Kein bestehender Header, Key oder Wert ändert sich; insbesondere bleiben `error_message`, `x-swarmq-traceback`, `TASK_DLQ error=` und `error.message` bei leerer Exception-Meldung `""` (T12, Rev. 2).
  - Clients lesen Ergebnisse mit `.get()` (`client/workflows.py:219-224`) und Workflow-Fehler mit `.get()` (`workflow_orchestration.py:197-198`).
- **Q2 Mischbetrieb (beide Richtungen):**
  - 0.12-Worker mit neuem Client: Ein Ergebnis ohne `error_class` ergibt `TaskError.error_class is None` (Unit-Test).
  - Neuer Worker mit 0.12- bzw. 0.11.1-Client: Das zusätzliche Feld wird ignoriert, `TaskError(message)` bleibt gleich. Ein Szenario in `tests/compat/` (Basis 0.11.1, opt-in `-m compat`) belegt das für 0.11.1 (MR 3). Die compat-Suite kennt nur 0.11.x; für 0.12.0rc1 belegt es ein Unit-Test auf `client/workflows._unpack` bzw. den Ergebnis-Leser mit einem Ergebnis, das `error_class` enthält und ohne (Review P2-28).
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
  6. Unit: `uv run --extra test --extra reload pytest tests/unit -q` (wie CI, `.gitlab-ci.yml:51`, und die Agentenregeln; Review P2-30)
  7. Integration und e2e **nur** über den Wrapper mit festen Testports (`SWARMQ_TEST_RABBITMQ_PORT=5699 SWARMQ_TEST_RABBITMQ_MGMT_PORT=15699 SWARMQ_TEST_VALKEY_PORT=6399`), nie 5672/6379. Vorher `pgrep -af pytest` prüfen und den Lauf per `flock` serialisieren, weil andere Sitzungen dieselben Container nutzen.
  8. Spec-Drift: `uv run python -m swarmq.agent_spec`, danach `git diff --exit-code website/reference/agent/spec website/llms.txt` (nur in Units, die Q4 betrifft).
  9. Neue Unterdrückungen (`# noqa`, `# type: ignore`, `# pyright: ignore`) nennen Code und Grund.

---

## 3. Spikes (vorab, als Tests im Repo)

| Spike | Frage | Betrifft | Ort |
|---|---|---|---|
| S1 | Zuerst `max_message_size` des Test-Brokers auslesen (auf RabbitMQ 4.x nach Kenntnisstand 16 MiB, nicht gemessen; Review P1-21). `SWARMQ_MAX_MESSAGE_BODY_BYTES` im Spike unter diese Grenze setzen, sonst käme ein Body von Grenze + 1 gar nicht erst in die Queue. Dann: Lässt sich eine Nachricht mit `max_message_body_bytes + 1` Bytes per `RabbitMQBroker.dead_letter` als Kopie nach `.XQ` legen, auf RabbitMQ 4.3.6 (5699) und, falls der Container läuft, 4.2.9 (5742)? Wie lange dauert das, und wie viel Speicher braucht `dead_letter_generation` (`json.loads` des ganzen Bodys, `identity.py:123-132`)? Was passiert über `max_message_size` des Brokers, und reißt eine abgelehnte Kopie den Consume-Kanal mit (Redeliver-Schleife)? | MR 8, E6 | `tests/integration/spikes/test_s_p5b_oversize_copy.py` |
| S2 | (Vorab bestätigt durch Review P2-26: der Forkserver übernimmt `sys_path` aus `spawn.get_preparation_data`; der Spike-Test bleibt als Absicherung.) Erben Supervisor-Kinder (`multiprocessing.get_context("spawn")`, `process_manager/supervisor.py:166`) und der Forkserver des CPU-Modus (`worker/compute/wiring.py`) einen zur Laufzeit vorangestellten `sys.path`-Eintrag auf CPython 3.14? Findet der Forkserver-Preload das Task-Modul daraus? Ein ImportError im Preload wird von CPython still geschluckt, deshalb prüft der Test im Kind selbst. | MR 6 | `tests/integration/test_app_dir_spawn.py` (> 100 ms, echte Prozesse) |
| S3 | Verhalten von `get_type_hints(..., format=Format.FORWARDREF)` mit dishka ≥ 1.10.1: `Annotated[X, FromComponent("c")]`, Alias `Depends = FromDishka`, `FromDishka[X] \| None`, PEP 649 ohne Future-Import. Liefert `default_parse_dependency` für einen `ForwardRef` sicher `None`? Rev. 2 (Review P0-4, Proben bestätigt): `Annotated[ForwardRef('Later'), FromComponent("c")]` und `Depends[Later]` ergeben einen Key **mit** `ForwardRef` (nicht `None`), `wrap_injection` wirft dann `NameError`; unter PEP 649 wird `FromDishka[Later] \| None` ein einziger `ForwardRef('FromDishka[Later] \| None')`; `"list[int"` ergibt `SyntaxError`. Die Spike-Tests pinnen diese drei Fälle (T5 Regeln 1, 2, 4). | MR 7 | Unit-Tests in `tests/unit/test_dishka_annotations.py` (rein, schnell) |

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
  6. Kopie liegt, aber `maybe_release_unique` wirft (ohne `on_publish_failure`) → `task_sent_to_dlq` einmal (pinnt die exakte Bedingung aus T9; die vorbestehende Doppelbehandlung bleibt, Abschnitt 9).
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
  6. CPU-Modus: `RetryHandler.evaluate` mit einer aus dem Envelope rekonstruierten `Retry(delay=5)` und Filter → `retry_later` mit 5000 ms. Unit, ohne Prozess. Die Instanz entsteht über `pickle.loads(pickle.dumps(error_envelope(...))).error()` (Review P2-29).
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
  - Ausführung und Ergebnis-Tail: `worker/results.py:61-80` (`_encode_with_fallback`, auch FIRE_AND_FORGET mit `result_ttl > 0` und der Rückfall bei `ResultSerializationError`). Die Klasse kommt aus `execution.task_exception` über `error_class_of`.
  - Dead-Letter: `worker/retry.py:251-253` (`RetryHandler.dead_letter`), Keyword `error_class` nach T17; `_terminal_failure` übergibt `error_class_of(exc)`.
  - Gate-Pfade über `dead_letter` (Lock, Delivery-Count, Ausführungsschutz, unbekannter Task; Lambdas `pipeline.py:158,165,182,453`): **unverändert**, die Klasse entsteht in `dead_letter` über die Tabelle (T17).
  - Expiry-Gate `worker/gates/expiry.py:86-97`: kein Wert (E7), Code unverändert, nur Test.
  - Scheduler, unroutbarer Oneshot: `scheduler/unroutable.py:204-216`.
  - Workflows: `workflow_failure.py:100-113` (`fail`), `worker/terminal.py:55`, `worker/results.py:168`, Leser `workflow_orchestration.py:196-198` und `client/workflows.py:209-224`.
- **Dateien:**
  - neu `src/swarmq/worker/error_info.py` (`error_class_of`, Markierung worker-erzeugter Fehler, Tabelle der Gate-Klassen)
  - `src/swarmq/cancellation.py`, `exceptions.py`, `worker/execution.py` und `worker/compute/executor.py` (nur Markierung nach T12), `worker/results.py`, `worker/retry.py`, `worker/terminal.py`, `scheduler/unroutable.py`, `workflow_failure.py`, `workflow_orchestration.py`, `client/workflows.py`, `agent_spec.py`
  - nicht geändert (Rev. 2): `worker/pipeline.py`, `worker/gates/lock.py`, `delivery_count.py`, `unknown_task.py`, `expiry.py`
- **Test-Liste** (TDD-Reihenfolge):
  - Unit, rein:
    1. `build_error_result_dict(..., error_class="builtins.ValueError")` enthält den Schlüssel; ohne Argument fehlt er (`test_cancellation.py`). (RED)
    2. `build_result_dict` für Erfolg und Cancel schreibt nie `error_class`.
    3. `error_class_of(ValueError())` → `"builtins.ValueError"`; eigene Klasse → `"modul.Qualname"`; verschachtelt → `"modul.Aussen.Innen"`; `RemoteTaskError(remote_type="x.Y")` → `"x.Y"`; `RemoteTaskError(remote_type="unknown")` → `None`; markierter Worker-Fehler → `None` (`tests/unit/worker/test_error_info.py`).
    4. Tabelle der Gate-Klassen: `TaskNotFoundError`, `LockWaitExceeded`, `RedeliveryLimitError` → `swarmq.exceptions.*`; `WorkerHungError`, `ValueError` → `None`.
    5. `TaskError("m").error_class is None`; `TaskError("m", error_class="a.B").error_class == "a.B"`; Pickle-Rundreise erhält beides; `TaskCancelledError` unverändert (geprüft über `task_id`, `args` und `error_class is None`, **nicht** über `str()`: die Meldung wird nach Pickle heute schon verdoppelt, vorbestehend, Review P2-28).
  - Unit, Worker:
    6. Task wirft `ValueError()` (leer), `max_retries=0` → das gespeicherte DLQ-Ergebnis hat `error_class == "builtins.ValueError"` und unverändert `error_message == ""`; `x-swarmq-traceback` der DLQ-Kopie und `TASK_DLQ error` bleiben `""` (pinnt T12, Rev. 2).
    7. Wie 6 mit `dead_letter=False` (Ergebnis-Tail) → dieselben Felder.
    8. `raise Retry()` mit verbleibendem Budget → `x-swarmq-traceback` der Retry-Kopie bleibt `""` (pinnt T12, Review P1-6).
    9. FIRE_AND_FORGET-Task mit `result_ttl=60`, Fehler → Ergebnis mit `error_class` (ohne `result_ttl > 0` speichert FAF nichts, `pipeline.py:518`; Review P1-9).
    10. Nicht serialisierbarer Rückgabewert → `error_class == "swarmq.exceptions.ResultSerializationError"`.
    11. Gate-Pfade nach E7/T17: unbekannter Task (Gnadenfrist 0) → `swarmq.exceptions.TaskNotFoundError`; Ende der Lock-Wartezeit → `swarmq.exceptions.LockWaitExceeded`; Delivery-Limit über `x-acquired-count` → `swarmq.exceptions.RedeliveryLimitError`; `WorkerHungError`, Lock-Render-Fehler und Expiry → kein Schlüssel.
    12. Worker-erzeugte Fehler → kein Schlüssel: harter Timeout (IO und CPU), Drain-Abbruch, `ChildDied`, `SlotUnavailable`, sonstiger Pool-Fehler (`tests/unit/worker/test_execution*.py`, `tests/unit/worker/compute/test_executor.py`; Review P1-8).
    13. CPU-Modus: Envelope mit `remote_exception` (nicht rekonstruierbar) → `error_class == remote_type`; rekonstruierbare Exception → deren Klasse; Envelope ohne Detail (`remote_type="unknown"`) → kein Schlüssel.
    14. Unroutbarer Oneshot → `error_class == "swarmq.exceptions.UnroutableMessage"` (`tests/unit/scheduler/test_fire_unroutable.py`).
  - Unit, Client und Workflow:
    15. `_unpack` mit `error_class` → `TaskError.error_class` gesetzt; ohne Schlüssel (Ergebnis eines 0.12.0rc1- oder 0.11.1-Workers) → `None` (`tests/unit/client/test_results.py`; belegt Q2 für 0.12.0rc1).
    16. `WorkflowAdvancer.fail(wf, "TaskError", msg, error_class="a.B")` speichert `error.class`; ohne Keyword fehlt `class` (die exakten Vergleiche `test_workflow_advancer.py:743,825` bleiben grün); `WorkflowHandle.get()` wirft `TaskError` mit `error_class`; ein Zustand ohne `class` → `None` (`tests/unit/test_workflow_handle.py`, `test_workflow_advancer.py`).
    17. `fail_workflow_step` reicht die Klasse aus `dead_letter` als Keyword durch, nur mit Wert; ohne Wert bleibt der Aufruf `fail(wf, "TaskError", msg)` (`tests/unit/worker/test_terminal.py:95` bleibt grün).
    18. Workflow-Schritt mit `dead_letter=False`, Task wirft `RuntimeError("boom")` → `error.class == "builtins.RuntimeError"` im Zustand (Review P0-1). Dazu ändert sich `test_retry.py:414` (Abschnitt 6).
  - Contract und Spec:
    19. `test_result_schema_validates_real_builder_output` bekommt einen Fall mit `error_class` (erst RED gegen das alte Schema, dann `_TaskResult.error_class: str | None = None`).
    20. `_ERROR_SEMANTICS["TaskError"]["transport_field"]` wird `"error_message, error_class, is_error=true"` (`test_error_catalog_known_semantics` zuerst RED; Review P2-28).
    21. Spec neu erzeugen (`messages/result.schema.json`, `errors.json` wegen Docstring, `init_params` und `transport_field` von `TaskError`). Eigener Zyklus.
    22. Golden additiv: `golden/values/error_result.msgpack` mit einem festen Fehlerergebnis samt `error_class`, Generator in `tests/integration/backend/_golden.py` (`value_fixtures`). Bestehende Dateien bleiben byte-identisch. Der Byte-Test liegt neben `test_result_envelope_is_byte_identical_and_decodes_to_fixed_input` unter `tests/integration/backend/` und läuft daher nur über den Wrapper, nicht in Gate 6 (Review P2-28).
  - Integration (Wrapper):
    23. Echter Valkey: Der Worker speichert ein Fehlerergebnis; `client.get_result` wirft `TaskError` mit `error_class` und unverändertem Text.
  - Compat (opt-in `-m compat`, Basis 0.11.1):
    24. Neuer Worker, 0.11.1-Client: `get_result` wirft `TaskError` mit derselben Meldung, auch bei leerer Exception-Meldung.
- **Doku:**
  - `website/reference/agent/message-formats.md`/`.de.md`: `error_class` nur auf Fehlerergebnissen, Form nach E5, im CPU-Modus der Name aus dem Kind; auf Pfaden ohne Wert **fehlt der Schlüssel** (er ist nicht `null`, Review P2-28). `error_message` ist `""` statt `null`, auch bei einer Exception ohne Text. Die Decoding-Priorität nennt `TaskError.error_class`.
  - `website/reference/agent/error-catalog-machine.md`/`.de.md` (Zeile 88: „advisory“ bleibt).
  - `website/reference/api.md`/`.de.md` (Abschnitt `TaskError`), `website/reference/errors.md`/`.de.md` (`TaskError`).
  - `website/guides/workflows.md`/`.de.md` (Fehlerfall von `handle.get()`).
- **Changelog (Added):** „Error results carry `error_class` (fully-qualified exception class) and `TaskError.error_class` exposes it; in CPU mode it is the class reported by the child. Failures the worker generates itself (hard timeout, drain, lost compute child, expiry) carry no class.“ / DE. (Rev. 2: kein „Changed“-Eintrag mehr; `error_message` bleibt unverändert.)
- **Gates:** Q12 1–8. Q5: `agent_spec.py` (742) wächst um höchstens 2 Zeilen.

### MR 4 – P12: Doku an den Code angleichen (a, b, d, f, DLQ-Guide)

- **(a) `defining-tasks.md`/`.de.md:107-127`:** Die Doku verspricht einen `SerializationError` für `datetime`, `Decimal` und `UUID`. Die Klasse gibt es nicht. `encode_json` nutzt `msgspec.json.Encoder()` ohne Einschränkung (`serialization.py:17-26`).
  - Die Doku beschreibt künftig, was wie ankommt:
    - `datetime`/`date`/`time` als ISO-8601-`str`
    - `Decimal` und `UUID` als `str`
    - `bytes` als Base64-`str`
    - `tuple`/`set`/`frozenset` als `list`
    - Unbekannte Objekte führen beim direkten `schedule()` zu `ResultSerializationError` (der Name ist irreführend, aber so heißt die Klasse; `client/schedule.py:291`).
    - (Rev. 2, Review P1-15) Mit `delay`/`eta` laufen die Argumente stattdessen über `msgspec.json.encode(definition)` in den ScheduleStore (`scheduler/definition.py:156`): Ein unbekanntes Objekt ergibt dort einen rohen `TypeError`, keinen `ResultSerializationError`.
    - Auf beiden Wegen werden Dataclasses, `msgspec.Struct` und `Enum` still kodiert (als Objekt bzw. Wert), Dict-Schlüssel vom Typ `int` werden zu Strings (`{1: …}` → `{"1": …}`), `float("nan")` wird `null`.
    - Mit `unique_for` lehnt `canonical_encode` zusätzlich `bytes` und eigene Klassen mit `TypeError` ab (`serialization.py:76-112`).
  - Rückgabewerte (`run()`): msgpack mit Ext-Typen, d. h. `datetime`, `date`, `time`, `Decimal`, `UUID`, `set` und `frozenset` kommen typgleich zurück (`serialization.py:150-176`, `207-235`).
  - **Keine Verhaltensänderung.** Ob der Client diese Typen ablehnen soll, ist eine Produktfrage (Abschnitt 8/9).
- **(b) `middleware.md`/`.de.md:173-177`:** Je nach E3 (Rev. 2, Review P2-27: zusätzlich der Hinweis, dass ein Retry den Originalbody zustellt und `pre_execute` erneut läuft; eine Änderung wirkt also nur für den laufenden Versuch):
  - E3 A: „Eine Änderung am bestehenden `message["kwargs"]`-Dict bzw. an der `message["args"]`-Liste sieht `run()`, auch im CPU-Modus; eine Neuzuweisung nicht.“
  - E3 B: Die Doku beschreibt dasselbe Verhalten als „heute so, nicht garantiert“.
- **(d) `ResultExpiredError`:** T15. Doku `website/reference/errors.md`/`.de.md:304-313` bzw. `:323ff.`, die Übersichtstabelle `errors.md`/`.de.md:40` (Review P2-27) und `error-catalog-machine*.md:42-43`: „derzeit nie geworfen; nach Ablauf wirft `get_result` `ResultNotReadyError`“ (`client/results.py:79-83`).
- **(f) `@app.task`:** Kein Doku-Fehler; der Ausdruck steht nur im Celery-Vergleich. Nichts zu tun.
- **DLQ-Guide `dead-letter-queues.md`/`.de.md:281-286`:** Unbekannte Tasks laufen seit BZ-06 über den Worker-Dead-Letter mit Ablaufzeit, nicht mehr über den DLX. Die Aufzählung nennt künftig nur Delivery-Limit, `undecodable`, `body_too_large`, `middleware_chain_factory` und die DLX-Rückfälle. MR 8 ändert das erneut.
- **Test-Liste:**
  1. Charakterisierung (a), `tests/unit/test_serialization.py`: `encode_json` von `datetime`, `Decimal`, `UUID`, `bytes`, `set` ergibt die dokumentierten JSON-Werte; ein eigenes Objekt ergibt `ResultSerializationError`; Dataclass, `Struct` und `Enum` werden kodiert, `{1: "a"}` wird `{"1": "a"}`, `nan` wird `null`. Für den `delay`/`eta`-Weg: `msgspec.json.encode` einer `ScheduleDefinition` mit einem eigenen Objekt in `args` ergibt `TypeError` (`tests/unit/scheduler/test_definition.py`). Ohne Produktionscode; die Tests sind beim ersten Lauf grün und sichern die Doku ab.
  2. Nur bei E3 A: IO-Modus, eine Middleware fügt in `pre_execute` `message["kwargs"]["tenant"] = "t"` hinzu → `run()` erhält `tenant`; eine Neuzuweisung `message["kwargs"] = {...}` hat keine Wirkung (`tests/unit/test_middleware.py`). Charakterisierung.
  3. Nur bei E3 A: dasselbe im CPU-Modus über die öffentliche `ComputeExecutor.execute()` mit einem Fake-Pool, der den übergebenen Job festhält (`tests/unit/worker/compute/test_executor.py`). Nicht über die private `_build_job` (AGENTS.md, Verbot 6; Review P2-27).
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
- **Dateien:** `src/swarmq/config.py` (Feld, `_ENV_OVERRIDES`), `config_validation.py` (`_validate_scheduler_mode`, Protocol-Stub `scheduler: str` und ClassVar), `scheduler/loop.py` (`candidate`, Wächter auf `_report_task`), `worker/lifecycle.py` (Durchreichen, WARNING), `worker/pipeline.py` (Kommentar), `cli/parser.py` (`--no-scheduler`), `cli/worker_options.py` (`_OPTIONS`), `process_manager/config.py` (Feld `scheduler` in `WorkerConfig`) und `process_manager/run.py` (setzen), `cli/env_check_cmd.py` (Abschnitt `scheduler`, `_render_table`), `agent_spec.py` (`_CONFIG_CHOICES`).
- **Test-Liste:**
  - Unit (`tests/unit/test_config_scheduler_switch.py`):
    1. Default `SwarmQConfig(...).scheduler == "on"`. (RED)
    2. `SWARMQ_SCHEDULER=off` → `"off"`; Programmwert wird von der Env überschrieben (Konvention).
    3. Ungültiger Wert (`"no"`) → `ConfigurationError`, Meldung nennt `on`, `off`.
  - Unit (`tests/unit/scheduler/test_loop_candidate.py`):
    4. `SchedulerLoop(candidate=False).start()` meldet einmal (`report_cron`, `report_worker_version` aufgerufen) und startet keine Wahl: `lock_manager.acquire` wird nie aufgerufen, auch nach mehreren Takten (Event-gesteuert, kein `asyncio.sleep`).
    5. `candidate=False`: der Melde-Takt läuft weiter (zweite Meldung nach `scheduler_seen_interval_s`, gesteuerte Uhr).
    6. `candidate=False`: `stop()` sendet kein `SCHEDULER_LEADER_HANDOVER` und gibt keinen Lock frei.
    6a. `candidate=False`: ein zweiter `start()` startet keinen zweiten `_report_loop` (Wächter auf `_report_task`, Review P1-12).
    7. `candidate=True` (Default) unverändert: bestehende Tests in `tests/unit/scheduler/test_loop.py` bleiben grün.
  - Unit (`tests/unit/worker/test_lifecycle*.py`):
    8. `scheduler="off"` → `SchedulerLoop` mit `candidate=False`, WARNING `scheduler_disabled` genau einmal mit `hint`.
    9. `scheduler="on"` → kein `scheduler_disabled`.
    9a. Eingebettet: `SwarmQ(..., scheduler="off").start_worker()` (Worker gemockt bis zum Lifecycle) → `SchedulerLoop` mit `candidate=False` (Review P2-25).
  - Unit, CLI (`tests/unit/test_cli_worker_options.py`, `test_cli_parser.py`):
    10. `--no-scheduler` → `args.scheduler == "off"`; Flag > `SWARMQ_SCHEDULER=on` > Default.
    11. `worker_config_kwargs` enthält `scheduler` und nennt es in `resolved_fields`.
    11a. Supervisor-Weg (Review P0-2): `--no-scheduler` ohne Env → `WorkerConfig.scheduler == "off"` (`process_manager/run.py`), und die Kind-Namespace aus `worker_subprocess_entry` (`entry.py:136-155`) ergibt `SwarmQConfig.scheduler == "off"`, auch wenn im Kind `SWARMQ_SCHEDULER=on` gesetzt ist (`tests/unit/process_manager/test_run.py`, `test_entry*.py`). Ohne Flag mit `SWARMQ_SCHEDULER=off` → ebenfalls `"off"`.
    12. Golden `tests/unit/fixtures/cli_help/worker.txt` (eigener Zyklus).
  - Unit, `swarmq check` (`tests/unit/test_cli_env_check.py`):
    13. `SWARMQ_SCHEDULER=off` → `report["scheduler"] == {"scheduler": "off"}`, Befund `scheduler_disabled` mit Stufe `"warning"`, `ok` bleibt `True`.
    14. Ohne Variable → `{"scheduler": "on"}`, kein Befund (`test_cli_env_check.py:98` bleibt grün).
    14a. Tabellenausgabe (`--format table`) enthält die Zeile `scheduler  scheduler  off` (Review P1-11).
    14b. `SWARMQ_SCHEDULER=no` → Befund `scheduler_invalid` mit Stufe `"error"`, `ok: false`, Exit 78 (T4).
  - Spec:
    15. `test_agent_spec.py`: `config.json` nennt `scheduler` mit `env_var == "SWARMQ_SCHEDULER"` und `choices == ["off", "on"]`; `cli.json` nennt `--no-scheduler`; `test_config_spec_enum_choices_match_validation` deckt `scheduler` ab (Review P2-25). Danach neu erzeugen.
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
  - `website/reference/logging.md`/`.de.md`: Event `scheduler_disabled` (einmal je Worker-Prozess, also N-mal bei `--processes N` und nach jedem Recycle).
  - `website/reference/cli.md`/`.de.md`, Abschnitt `swarmq check`: neuer Abschnitt `scheduler`, Befunde `scheduler_disabled` (warning) und `scheduler_invalid` (error).
  - `website/guides/deployment.md`/`.de.md`: Hinweis in der Liste der Umgebungsvariablen.
  - Kommentar `pipeline.py:116-121` korrigieren (P12c).
- **Changelog (Added):** „`SWARMQ_SCHEDULER=on|off` and `swarmq worker --no-scheduler` keep a worker out of the scheduler leader election. It still reports its crons. If no worker runs the scheduler, crons and `delay`/`eta` tasks do not fire.“ / DE.
- **Gates:** Q12 1–8. Bei Änderungen am Supervisor (`process_manager/`) zusätzlich die betroffenen Integrations-/e2e-Tests über den Wrapper. Q5: `config.py` 668, `lifecycle.py` 607, `loop.py` 568, jeweils + ≤ 15.

### MR 6 – P6: Importpfad des Workers (`--app-dir`)

- **Befund (geprüft):**
  - Der Console-Script-Einstieg `swarmq = "swarmq.cli:console_main"` (`pyproject.toml:105`) hat in `sys.path[0]` das venv-`bin`, nicht das Arbeitsverzeichnis.
  - Module importieren `cli/worker_cmd.py:34` (Worker), `cli/worker_cmd.py:166` (`--configure`), `cli/inspect_cmd.py:59` (`inspect`) und `process_manager/check.py:47` (`find_spec` für `worker --check` und den Supervisor-Start).
  - `python -m swarmq.cli` hat das Arbeitsverzeichnis schon im Pfad, außer unter `-P`/`PYTHONSAFEPATH`.
  - Hilfetext `cli/parser.py:89-91`: „Must be importable from the current Python path.“
- **Dateien:** `src/swarmq/cli/_shared.py` (`apply_app_dir`, argparse-Typ `_app_dir_path`), `cli/__init__.py` (Aufruf `_shared.apply_app_dir(args)` in `main`, kein neuer Importname, T8), `cli/check_cmd.py` (`--app-dir` im Detektor `_is_check_invocation`), `cli/parser.py` (Option an `worker`), `cli/parser_ops.py` (Option an `inspect`), `process_manager/check.py` und `cli/worker_cmd.py` (Hinweis im ImportError; im Kind ist `app_dir` unbekannt, dort nennt der Hinweis `sys.path[0]`, Review P2-26); `tests/unit/conftest.py` (Fixture, siehe unten).
- **Test-Liste:**
  - Testisolation (Review P1-13, vor dem ersten Zyklus): Eine autouse-Fixture in `tests/unit/conftest.py` sichert `sys.path` und stellt ihn nach jedem Test wieder her. Aus `sys.modules` entfernt sie nur neue Einträge, deren Datei in einem während des Tests hinzugekommenen `sys.path`-Verzeichnis liegt; nie `swarmq.*` oder Bibliotheken, sonst entstünden beim nächsten Import doppelte Klassenobjekte (`isinstance` auf Exceptions bräche). Rund 22 bestehende Unit-Tests rufen `main(["worker"|"inspect", ...])` und hängten sonst das Arbeitsverzeichnis dauerhaft an `sys.path`. Die neuen Tests nutzen eindeutige Modulnamen je Test (`appmod_<testname>`), damit der `sys.modules`-Cache sie nicht reihenfolgeabhängig macht.
  - Unit (`tests/unit/test_cli_app_dir.py`):
    1. `apply_app_dir` mit ausdrücklichem Verzeichnis stellt den absoluten Pfad an `sys.path[0]` (per `monkeypatch` auf `sys.path`). (RED)
    2. Idempotent: Steht der Pfad schon an Stelle 0, kommt er nicht doppelt. Steht er nur weiter hinten, kommt er zusätzlich an Stelle 0 (wie `python -m`).
    3. Default nach E1 (bei C): ohne Flag und ohne `safe_path` → Arbeitsverzeichnis; mit `safe_path=True` (injizierter Parameter, T8) → nichts; ausdrückliches `--app-dir` unter `safe_path` → gesetzt.
    3a. `os.getcwd()` wirft `FileNotFoundError`, ohne Option → nichts vorangestellt, kein Fehler (Review P2-26).
    4. `swarmq worker --app-dir /gibt/es/nicht --module m` → argparse-Fehler, Exit 2, Meldung nennt den Pfad.
    4a. Dasselbe mit `--check` → JSON-Fehler auf stdout, Exit 1 (`cli/__init__.py:220-245`; Review P1-14).
    4b. `_is_check_invocation(["worker", "--app-dir", "d", "--check"])` → `True`; ein Wert `--app-dir --check` wird nicht als Flag gelesen.
    5. `main(["worker", "--module", "appmod_check", "--check"])` in einem Temp-Verzeichnis mit `appmod_check.py`, `sys.path[0]` auf ein fremdes `bin` gesetzt → JSON `ok: true`.
    6. `main(["inspect", "--module", "appmod_inspect"])` ebenso → Task gelistet.
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
  - **Risiko:** Lokale Dateien überdecken installierte Pakete (z. B. eine `valkey.py` im Arbeitsverzeichnis) und auch Module der Standardbibliothek (`queue.py`, `select.py`; Review P2-26). `PYTHONSAFEPATH` bzw. `-P` schaltet das ab (bei E1 C).
  - `website/reference/cli.md`/`.de.md:96` (Beschreibung von `--module`, Review P2-26).
  - `website/guides/deployment.md`/`.de.md` (Docker `WORKDIR`).
  - Hilfetext `cli/parser.py:89-91` anpassen.
- **Changelog (Added/Fixed):** „`swarmq worker` and `swarmq inspect` find task modules in the working directory again (`--app-dir`, default per E1); the console script used to search only the venv's `bin` directory.“ / DE.
- **Gates:** Q12 1–8. Q5: `cli/__init__.py` 526 + ≤ 3; `cli/parser.py` 364.

### MR 7 – P2: `FromComponent` und Aliase unter PEP 563, Validierung (+ P12e, N1)

- **Befund (geprüft):**
  - Die Vorprüfung `_raw_mentions_fromdishka` liefert für String-Annotationen nur `"FromDishka" in raw` (`dishka.py:312-320`). `Annotated[X, FromComponent("c")]` und Aliase (Phoenix `Depends`) werden unter `from __future__ import annotations` still nicht injiziert, ohne Warnung.
  - `validate_registered_tasks` läuft nur in `cli/worker_cmd.py:67-69`, nicht in `SwarmQ.start_worker()` (`app.py:245-285`) und nicht bei `worker --check`. `swarmq check` importiert kein Modul (Abschnitt 10).
  - **N1 (E9):** Auf 3.14 scheitert ein Task ohne Future-Import mit unauflösbarem Namen in `run()` schon an `task.py:476` (`inspect.signature`), auch ohne dishka (lokal reproduziert).
  - (Rev. 2, Review P1-16) Weitere Stellen, die nach dem Fix von `task.py:476` sonst neu scheitern: `dishka.py:334-335` (`inspect.signature`, `getattr(func, "__annotations__", {})` wirft unter PEP 649 `NameError`) und `worker/gates/lock.py:155` (`render_lock_name`, `inspect.signature(task_class.run)`; gefangen werden dort nur `TypeError` und `KeyError`). Ohne Fix definierte sich ein Lock-Task, scheiterte aber bei jeder Zustellung mit `NameError`.
- **Dateien:** `src/swarmq/dishka.py`, `app.py` (`start_worker`), `cli/check_cmd.py` (nur dort, T18; **nicht** `process_manager/check.py`), `cli/parser.py` (Hilfetext von `--check`), `task.py` und `worker/gates/lock.py` (E9, `annotation_format=Format.FORWARDREF`); Tests: `tests/unit/test_app.py` (Registry-Isolation, T7).
- **Test-Liste** (`tests/unit/test_dishka_annotations.py`, Modul-Fixtures als Dateien unter `tests/unit/fixtures/dishka_modules/`, importiert per `importlib`):
  1. Modul mit Future-Import, `svc: Annotated[Svc, FromComponent("c")]` → Task gewrappt, `_dishka_injected_keys` enthält den Key mit Komponente `"c"`. (RED)
  2. Alias `Depends = FromDishka`, `svc: Depends[Svc]` unter Future-Import → gewrappt.
  3. Nicht-DI-Task mit Future-Import und `x: Undefined` → kein Fehler, nicht gewrappt (Regressionstest der Abwägung).
  4. Nicht-DI-Task mit `TYPE_CHECKING`-Import unter Future-Import → kein Fehler.
  4a. Nicht-DI-Task mit kaputter String-Annotation (`x: "list[int"`) → kein Fehler, nicht gewrappt (T5 Regel 2, Review P1-19).
  5. String mit `FromDishka` und unauflösbarem Typ → weiter `NameError` mit Hinweis (besteht).
  5a. Future-Import, `svc: FromDishka[Svc], x: OnlyTC` (`OnlyTC` nur unter `TYPE_CHECKING`) → weiter `NameError` mit Hinweis (T5 Regel 1, pinnt heutiges Verhalten).
  6. Alias-Annotation mit Forward-Ref auf eine später im Modul definierte Klasse (`Depends[Later]` und `Annotated[Later, FromComponent("c")]`) unter Future-Import → bei der Definition kein Fehler und nicht gewrappt (T5 Regel 4, kein Aufruf von `wrap_injection`); `validate_registered_tasks` wirft `ConfigurationError` mit Task- und Parameternamen (T6).
  6a. Future-Import, Alias `svc: Depends[Svc]` (auflösbar) plus `x: OnlyTC` → Klassendefinition gelingt, nicht gewrappt; `validate_registered_tasks` wirft `ConfigurationError`, die Meldung nennt `x` als unauflösbar (Review P1-17, T5 Regel 4/T6).
  7. Bei Definition und Start unauflösbar und ohne DI-Key (`x: Undefined`) → kein Fehler, auch nicht bei der Registrierung beim Bau der App (Grenze, pinnt Abschnitt 7; Registrierung als Nachtrag N2).
  7a. R18 unter PEP 563: `svc: Depends[Svc] | None` → beim Start `ConfigurationError` als wirkungslos (bewusst neu, T5; Review P2-24).
  8. PEP 649 ohne Future-Import, ausgewertetes `Annotated[Svc, FromComponent("c")]` → gewrappt (besteht schon; Absicherung).
  8a. PEP 649, `g: Depends[Later]` mit später definierter Klasse → `NameError` mit Hinweis bei der Definition (T5 Regel 4, erhält heutiges Verhalten).
  8b. PEP 649, `o: FromDishka[Later] | None` (ein einziger `ForwardRef`) → `NameError` mit Hinweis (T5 Regel 1, S3).
  9. Eingebettet: `SwarmQ.start_worker()` mit DI-Task ohne Container → `ConfigurationError` vor `broker.connect` (`tests/unit/test_app.py`, mit isolierter Registry, T7).
  10. Eingebettet mit auflösbarem Container → kein Fehler, `worker.start` läuft.
  11. E8 A: `worker --check` mit DI-Task ohne Container → JSON `ok: false`, `error_type: "ConfigurationError"`, Exit 1; stdout enthält genau eine JSON-Zeile, auch wenn das Modul beim Import druckt (T18). Der Supervisor-Start importiert weiter nicht (Spy auf `importlib.import_module` in `check_worker_config`).
  11a. `worker --check --module=json` bei global registriertem DI-Testtask → `ok: true` (nur neu registrierte Tasks werden geprüft, T18).
  11b. Golden `cli_help/worker.txt` mit dem neuen `--check`-Hilfetext und Spec `cli.json` (eigener Zyklus, T18).
  12. E9 A: Nicht-DI-Task ohne Future-Import mit `x: Undefined` in `run()` → Klassendefinition gelingt (`tests/unit/test_task.py`). Reservierte Parameter werden weiter erkannt.
  12a. E9 A: Lock-Task ohne Future-Import mit `x: Undefined` und `lock="user-{lock_id}"` → `render_lock_name` rendert (`tests/unit/worker/gates/test_lock.py`; Review P1-16).
  13. CPU-Modus: Das Kind importiert das Modul neu, und das Wrappen bei der Klassendefinition gilt dort ebenso (Unit über `maybe_wrap_task` mit frisch geladenem Modul; e2e `tests/e2e/test_cpu_mode.py` um einen `FromComponent`-Task unter Future-Import erweitern, Wrapper).
  14. Supervisor-Kind mit T6-Fehler → Exit 78 (`tests/unit/process_manager/test_entry*.py` bzw. e2e über den Wrapper; Review P2-24).
- **Doku (P12e):**
  - `website/reference/api.md`/`.de.md` (Abschnitt DI, Zeile 960ff.): `FromComponent`, `Annotated[X, FromComponent("c")]`, Aliase, PEP 563/649. Die Annotation muss bei der Klassendefinition auflösbar sein, sonst `ConfigurationError` beim Start (PEP 563) bzw. `NameError` bei der Definition (PEP 649) oder (unauflösbar und ohne erkennbaren Marker) keine Injektion. Das gilt auch für Nicht-DI-Parameter einer DI-Methode. Validierung jetzt auch eingebettet und bei `worker --check` (E8).
  - `website/guides/cpu-mode.md`/`.de.md` (Tabelle Zeile 22, Liste Zeile 62).
- **Changelog (Fixed):** „`Annotated[X, FromComponent(...)]` and aliases of `FromDishka` are injected under `from __future__ import annotations`; they were silently skipped. The DI check also runs in embedded workers (`SwarmQ.start_worker()`) and in `swarmq worker --check`.“ Bei E9 A zusätzlich: „On Python 3.14 a task without DI no longer fails at class definition because of an unresolvable annotation.“ (Changed, Rev. 2): „Under `from __future__ import annotations`, a DI method that uses an alias or `FromComponent` but cannot be resolved at class definition (forward reference, or an unresolvable non-DI parameter next to it) now fails worker start with `ConfigurationError` instead of silently running without injection; `Depends[X] | None` is rejected like `FromDishka[X] | None`.“ / DE.
- **Gates:** Q12 1–8 (Spec wegen `cli.json`). Q5: `dishka.py` 397 + ≤ 100 (Review P2-24; Grenze 800 ungefährdet).

### MR 8 – P5b: Giftnachrichten mit Ablaufzeit in die XQ

- **Befund (geprüft):**
  - `body_too_large` (`worker/decode.py:94-107`), `undecodable` (`decode.py:126-135`) und `middleware_chain_factory` (`worker/pipeline.py:403-412`) gehen per `route_via_dlx` (`gates/__init__.py:320-350`, RabbitMQ `nack(requeue=False)`) ohne `expiration` in die XQ.
  - Unbekannte Tasks laufen dagegen über `RetryHandler.dead_letter` (`gates/unknown_task.py:78-117`). Die Kopie bekommt `expiration=dead_letter_ttl_s`, `x-swarmq-dead-lettered-at` und `x-swarmq-dead-letter-ttl-ms` (`broker/rabbitmq/delivery.py:133-154`) sowie den Traceback-Header.
- **Design:**
  - Neue Funktion `reject_to_dead_letter(deps, delivery, *, reason, error, task_id=None, task_name=None)` in `worker/gates/reject.py`:
    1. Header = Original-Header + `x-swarmq-traceback = bounded_header_text(f"{reason}: {error}")`.
    2. `dead_letter_copy(deps, delivery, headers)` (`gates/__init__.py:306-316`; `SettleFailedAfterPublish` zählt als liegend).
    3. Danach `TASK_REJECTED` mit unverändertem `reason` und Workflow-Fehlmarkierung best effort, wie heute in `route_via_dlx`. Dafür wird `_fail_workflow_best_effort` (`gates/__init__.py:357`) in einem Refactoring-Zyklus zu `fail_workflow_best_effort` umbenannt (internes Paket, keine öffentliche API; Review P2-23), damit `reject.py` keinen privaten Namen importiert.
    4. Scheitert die Kopie, folgt `route_via_dlx` mit demselben `reason` als Rückfall. Er feuert `TASK_REJECTED` selbst, also kein doppeltes Signal.
  - Die drei Pfade rufen `reject_to_dead_letter` statt `route_via_dlx`; bei E6 B bleibt `body_too_large` bei `route_via_dlx`.
  - **Regeln aus 0.12, geprüft:**
    - **BZ-08, Generation:** `dead_letter_generation` vergibt ohne lesbare Task-ID im Body keine Generation (`identity.py:123-153`). Bei `body_too_large` und bei `undecodable` ohne lesbare `task.id` gibt es daher keine Replay-ID, wie in MR 12 festgelegt. (Rev. 2, Review P2-22) Ist der Body JSON mit `task.id`, aber z. B. ohne `task.name` (`undecodable` wegen fehlender Felder), bekommt die Kopie eine Generation und damit eine Replay-ID. Bei `middleware_chain_factory` ist der Body dekodiert; die Kopie bekommt eine Generation wie jede Worker-DLQ-Kopie.
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
    11. `RabbitMQBroker.dead_letter` mit undekodierbarem Body → `expiration` gesetzt, `x-swarmq-dead-lettered-at` gesetzt, **keine** `x-swarmq-replay-id` (`tests/unit/broker/test_dead_letter_ttl_header.py`). Zweiter Fall: JSON mit `task.id`, ohne `task.name` → Generation und Replay-ID vorhanden (Review P2-22).
  - Integration (Wrapper, `tests/integration/broker/test_reject_ttl.py`):
    12. Kaputte Nachricht in eine Queue publizieren, Worker läuft → in `.XQ` liegt sie mit `expiration` (ms aus `SWARMQ_DEAD_LETTER_TTL_S`) und `x-swarmq-dead-lettered-at`. `swarmq dlq list` zeigt sie.
    13. Nach E6 A: Body über der Grenze → ebenso, auf 4.3.6 (S1).
  - Mischbetrieb: Q2, keine eigene compat-Szene nötig (beide Wege enden in `.XQ`).
- **Doku:**
  - `dead-letter-queues.md`/`.de.md`, Tabelle „Three ways into the `.XQ`“ (Zeile 48-52): Die Zeile „Rejection by the worker“ nennt „Worker publishes a copy with retention“. Header jetzt `x-swarmq-traceback` (`<reason>: <error>`) plus Ablage-Stempel; `x-death` nur noch im Rückfall.
  - Abschnitt „Retention“ (Zeile 281-286): Ohne Ablaufzeit kommen nur noch Delivery-Limit und DLX-Rückfälle (sowie `body_too_large` bei E6 B).
  - Die Policy-Empfehlung (Zeile 288ff.) bleibt.
- **Changelog (Changed):** „Messages the worker rejects (`undecodable`, `middleware_chain_factory`, and `body_too_large` per E6) now go to `{queue}.XQ` as a copy with the dead-letter retention and the diagnostic header, like unregistered tasks; they no longer hold up expiry at the head of the `.XQ`. `swarmq dlq list` shows the reason (e.g. `undecodable: …`) in `ERROR_PREVIEW`. The broker's dead-letter exchange remains the fallback.“ / DE.
- **Gates:** Q12 1–7. Q5: `pipeline.py` 715 ± 0 (Aufruf ersetzt Aufruf).

---

## 6. Bestehende Tests und Golden-Dateien mit geänderter Erwartung (abschließend)

| MR | Test/Datei | Grund |
|---|---|---|
| 1 | keine | Neue Tests, keine geänderte Erwartung. Bricht im RED-Lauf ein bestehender Test, der `task_sent_to_dlq` im Redeliver-Zweig erwartet, gilt die Stop-Condition. |
| 2 | keine | Die bestehenden Filtertests nutzen `ValueError`/`ConnectionError`, nicht `Retry` (`test_retry.py:251-263`, `test_worker.py:1004-1068`). |
| 3 | `tests/unit/test_agent_spec.py::test_result_schema_validates_real_builder_output` (zusätzlicher Fall mit `error_class`) | optionales Feld im Schema |
| 3 | `tests/unit/test_agent_spec.py::test_error_catalog_known_semantics` (Fall `TaskError`: `transport_field` nennt `error_class`) | Review P2-28 |
| 3 | `tests/unit/worker/test_retry.py:414` (`test_disabled_dead_lettering_propagates_workflow_failure`): `fail.assert_awaited_once_with(_WF_ID, "TaskError", "boom", error_class="builtins.RuntimeError")` | T13, Review P0-1: echte Task-Exception, die Klasse wird jetzt durchgereicht |
| 3 | `website/reference/agent/spec/messages/result.schema.json`, `website/reference/agent/spec/errors.json` (`TaskError`: Docstring, `init_params`, `transport_field`) | Q4-Generierung |
| 3 | `tests/integration/backend/_golden.py` (`value_fixtures` + `error_result.msgpack`), neue Datei `tests/integration/backend/golden/values/error_result.msgpack` (additiv) | Form des Fehlerergebnisses pinnen; `result.msgpack` bleibt byte-identisch |
| 3 | Geprüft und **nicht** betroffen (Rev. 2): `test_workflow_advancer.py:743,825` und `test_terminal.py:95` (exakte Gleichheit bzw. Aufruf ohne Keyword; T13 schreibt und übergibt die Klasse nur mit Wert), `test_cancellation.py:111-124` (Feld fehlt ohne Wert), `test_delivery_count.py:109` (Gates unverändert, T17). Alle Tests, die `error_message == ""` oder einen leeren `x-swarmq-traceback` erwarten, bleiben grün (keine Füllung, T12). | – |
| 4 | `tests/e2e/combinations/test_failure_cascades.py:519,524,565` (`ResultNotReadyError` statt `(ResultExpiredError, TaskError, Exception)`) | P12d: Assertion schärfen |
| 4 | `tests/unit/test_agent_spec.py::test_error_catalog_known_semantics` (Fall `ResultExpiredError`), `website/reference/agent/spec/errors.json` | T15 |
| 5 | `tests/unit/fixtures/cli_help/worker.txt` (`--no-scheduler`) | neue Option |
| 5 | `tests/unit/test_agent_spec.py::test_config_spec_enum_choices_match_validation` (Fall `scheduler`) | Review P2-25 |
| 5 | `website/reference/agent/spec/config.json`, `website/reference/agent/spec/cli.json` | Q4-Generierung |
| 6 | `tests/unit/conftest.py`: neue autouse-Fixture (Sicherung von `sys.path` und der aus App-Verzeichnissen geladenen Module) | Review P1-13: Testisolation der rund 22 Tests mit `main(["worker"|"inspect", ...])`; deren Erwartungen ändern sich nicht |
| 6 | `tests/unit/fixtures/cli_help/worker.txt` (`--app-dir`, neuer `--module`-Hilfetext), `tests/unit/fixtures/cli_help/inspect.txt` (`--app-dir`) | neue Option; `root.txt` bleibt unverändert, weil die Option je Unterbefehl hängt |
| 6 | `website/reference/agent/spec/cli.json` | Q4-Generierung |
| 7 | `tests/unit/test_app.py`, Tests `test_start_worker_proceeds_for_url_constructed_backends` (`:151-160`) und `test_start_worker_creates_and_starts_worker` (`:383ff.`) sowie jeder weitere Test, der `start_worker()` bis zum Worker-Bau durchläuft: Fixture, die `Task._registered_tasks` vor dem Bau der App leert | T7, Review P0-5: global registrierte DI-Testtasks aus `test_worker_dishka.py:58-71` |
| 7 | `tests/unit/fixtures/cli_help/worker.txt` (Hilfetext von `--check` nennt die DI-Prüfung), `website/reference/agent/spec/cli.json` | T18, Review P1-18 |
| 7 | Geprüft und **nicht** betroffen (Rev. 2): die rund 20 `--check`-Tests mit `--module=json` (`test_cli_multiprocess.py:237-626`, `test_cli_pool.py:93`, `test_namespace_guard.py:104`) dank Differenzprüfung (T18); `test_cli_multiprocess.py:261` (Supervisor-Start importiert weiter nicht); `tests/e2e/test_logging_e2e_runtime.py:375` (stdout bleibt eine JSON-Zeile). Weitere Tests von `worker --check`, die einen Lauf ohne Import erwarten, im RED-Schritt per grep (`find_spec`/`import_module` in `tests/unit/test_cli*.py`, `tests/unit/process_manager/test_check*.py`) prüfen; betroffene hier nachtragen, bevor sie geändert werden. | – |
| 7 | E9 A: keine erwartet. Prüfen: Tests, die einen `NameError` bei der Klassendefinition eines Nicht-DI-Tasks erwarten (grep `NameError` in `tests/unit/test_task.py`, `test_dishka.py`). | N1 |
| 8 | `tests/unit/worker/test_worker.py:356-382` (`test_invalid_json_nacked_no_requeue`, `test_missing_task_name_nacked_no_requeue`: `dead_letter` statt `route_via_dlx`) | P5b |
| 8 | `tests/unit/worker/test_worker.py:1948-1971` (`test_factory_exception_nacks_without_requeue`: `dead_letter` statt `route_via_dlx`) | P5b, Review P1-20 |
| 8 | `tests/unit/worker/test_worker.py:2772-2803` (`test_middleware_factory_failure_with_unique_header_releases_flag`: `dead_letter` statt `route_via_dlx`; `release_unique` bleibt) | P5b, T16 |
| 8 | `tests/unit/worker/test_worker.py:2990-3007` (`test_oversized_body_nacked_without_decode`: nur bei E6 A `dead_letter` statt `route_via_dlx`) | P5b, E6 |
| 8 | `tests/unit/worker/test_lifecycle.py:677-688` (`TestPayloadShapeValidation`: `dead_letter` statt `route_via_dlx.assert_awaited_once_with`) | P5b, Review P1-20 |
| 8 | `tests/unit/worker/test_pipeline_settle.py:74-87` (`test_lost_channel_before_task_context_logs_one_warning`: der Kanalfehler muss jetzt auf `dead_letter` **und** dem Rückfall `route_via_dlx` liegen, damit `delivery_settle_failed` weiter genau einmal ohne `task_id` erscheint) | P5b |
| 8 | `tests/unit/worker/test_retry.py:465-482` (`route_via_dlx.assert_awaited_once` → `dead_letter` für die undekodierbare Nachricht) | P5b |
| 8 | `tests/unit/worker/test_decode.py:23-37` (Fixture `_deps`: `broker.dead_letter = AsyncMock()`) | P5b |
| 8 | `tests/unit/worker/test_pipeline.py:318-420` (Reject-Tests): Signal-Assertions bleiben. Nur Erwartungen an Broker-Verben ändern sich, falls vorhanden. | P5b |
| 8 | `tests/chaos/test_message_corruption.py:286-296`: nur der Kommentar („routes to `{queue}.XQ` via the DLX“ → Kopie mit Ablaufzeit, DLX als Rückfall); die Assertion `dlq_count in (0, 1)` bleibt | P5b, Review P1-20 |
| 8 | Geprüft und **nicht** betroffen: `tests/e2e/combinations/test__invariants.py` (nur FAF-Beobachtungsobjekte), `tests/unit/worker/gates/test_reject_signal.py` (ruft `route_via_dlx` direkt), `test_guarded_broker.py`, `test_pipeline_execution_guard.py` (nur Spies) | Review P1-20 |

Die Doku-Dateien (`website/**/*.md`) sind nicht golden-getestet (geprüft: kein Test liest `website/` außer der Spec unter `website/reference/agent/spec` und `website/llms.txt`).

---

## 7. Bekannte Grenzen (Changelog)

- **P1:** Schalten alle Worker am selben Valkey/Namensraum den Scheduler ab, feuern keine Crons, Oneshots und `delay`/`eta`-Tasks. Prune, Adopt, die Prüfung verwaister Queues und die stündliche Valkey-Prüfung ruhen. Ein 0.12-Worker ignoriert `SWARMQ_SCHEDULER`.
- **P2:** Eine DI-Annotation, die weder bei der Klassendefinition noch beim Start auflösbar ist (etwa nur unter `TYPE_CHECKING` importiert), wird nicht injiziert und nicht gemeldet, solange kein Marker erkennbar ist. Das ist der Preis dafür, dass Tasks ohne DI nie brechen. Dazu gehört unter PEP 649 ein Alias in einer ganz unauflösbaren Annotation (`Depends[Later] | None` wird ein einziger `ForwardRef` ohne erkennbaren Marker); `FromDishka` im Text bleibt dagegen ein `NameError` mit Hinweis (T5 Regel 1).
- **P2:** PEP 563 und PEP 649 melden eine unauflösbare DI-Methode verschieden: unter PEP 563 `ConfigurationError` beim Start, unter PEP 649 `NameError` bei der Definition (T5 Regel 4). Beides erhält das jeweilige Verhalten von 0.12.
- **P2:** `worker --check` prüft nur die Tasks, die der Import von `--module` registriert (T18).
- **P2 (N2, behoben):** Der Bau der App (`SwarmQ(...)`) wertete die Annotationen von `run()` strikt aus (`registry.py`, `inspect.get_annotations(eval_str=True)`) und scheiterte bei einem unauflösbaren Namen mit `NameError`. N2 von Georg am 2026-10-12 entschieden: tolerant lesen, als Nachtrag in MR 7. Ein unauflösbarer Rückgabetyp steht als `ForwardRef` in `RegistryEntry.return_type`, eine kaputte String-Annotation als roher Text; keine Grenze mehr.
- **P5b:** Scheitert die Kopie, landet die Nachricht wie bisher ohne Ablaufzeit in `.XQ` (DLX-Rückfall). Bei E6 B gilt das für `body_too_large` immer. Die Broker-Policy auf `\.XQ$` bleibt die einzige vollständige Begrenzung.
- **P5b:** Endet die Kopie mit `PublishIndeterminate` und greift danach der DLX-Rückfall, kann die Nachricht zweimal in `.XQ` liegen (einmal mit, einmal ohne Ablaufzeit; Review P2-23).
- **P6:** Mit dem Standard nach E1 (A oder C) können Dateien im Arbeitsverzeichnis installierte Pakete überdecken.
- **P8:** `Retry` verbraucht das Budget. Bei `max_retries=0` endet `Retry` terminal (E4 A).
- **P9:** `error_class` ist beratend. Ergebnisse älterer Worker haben das Feld nicht. Auf Pfaden ohne Exception-Klasse fehlt es (E7), ebenso bei vom Worker erzeugten Fehlern (harter Timeout, Drain, verlorenes Rechenkind, Kind ohne Fehlerdetail; T12). Im CPU-Modus steht dort der Name aus dem Kind, auch wenn sich die Klasse im Elternprozess nicht laden ließ.
- **P9:** Eine Exception ohne Text ergibt weiter `error_message == ""`; nur `error_class` nennt dann die Klasse (T12, Rev. 2).

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
- **Leere Fehlermeldungen füllen** (aus Revision 1 gestrichen, T12): Soll `error_message`/`x-swarmq-traceback` bei einer Exception ohne Text den Klassennamen tragen? Wirkt auf alte Clients und Header, daher eigene Entscheidung.
- **Doppelbehandlung nach liegender DLQ-Kopie** (vorbestehend, Review P1-10): Wirft `maybe_release_unique` oder `TASK_DLQ` nach `dead_letter_copy`, loggt `dead_letter` `dlq_publish_failed` und stellt neu zu, obwohl die Kopie liegt.
- **Nicht picklebares `Retry` im CPU-Modus** (vorbestehend, Review P2-29): Es wird zu `Retry(message)` mit `delay=""` rekonstruiert und ergibt in `evaluate` einen `ValueError` (`envelope.py:169`).
- **`TaskCancelledError` nach Pickle** (vorbestehend, Review P2-28): Die Meldung wird verdoppelt.

---

## 10. Abgleich mit der Vorprüfung (Abweichungen und Nebenbefunde)

| Punkt | Vorprüfung | Geprüfter Stand |
|---|---|---|
| P2 / P6 | `swarmq check` ist betroffen (Validierung, `--app-dir`). | `swarmq check` (`cli/env_check_cmd.py`) importiert kein Modul und hat kein `--module`. Betroffen sind `swarmq worker --check` (nur `find_spec`, `process_manager/check.py:47-66`) und `swarmq inspect`. |
| P8 | CPU-Modus prüfen (`compute/envelope.py`). | Die Datei liegt unter `worker/compute/envelope.py`. `Retry` ist picklebar und behält `delay` (Test `test_envelope.py:31`); der Filter läuft im gemeinsamen `RetryHandler`. Im CPU-Modus ist nichts zusätzlich zu ändern. |
| P5b | `max_body_bytes` | Feld `max_message_body_bytes`, Env `SWARMQ_MAX_MESSAGE_BODY_BYTES`, Default 16 MiB (`config.py:337`). |
| P5b | Pfad `pipeline.py:~407` | `pipeline.py:403-412` (`_resolve_chain_or_route`). |
| P7b | Falsches `task_sent_to_dlq` bei `placed=False` | Nur im Redeliver-Zweig falsch. Im Zweig `on_publish_failure` liegt die Nachricht über den DLX in der XQ. `dlq_publish_failed` existiert (ERROR). |
| P9 | Schema/Golden prüfen | Das Schema `_TaskResult` verbietet unbekannte Felder und muss erweitert werden. `golden/values/result.msgpack` ist ein **Erfolgs**-Ergebnis und bleibt mit T11 unverändert. Die Doku zeigt `error_message: null`, der Code schreibt `""`. `worker/terminal.py:55` setzt den Workflow-Fehlertyp immer auf `"TaskError"`. |
| P12d | e2e-Assertion | `get_result` nach Ablauf wirft `ResultNotReadyError` (`client/results.py:79-83`). Daran richtet sich die geschärfte Assertion. |
| P12b | nur kwargs | Auch die `args`-Liste ist dasselbe Objekt (`pipeline.py:378-385`). Im CPU-Modus wird erst nach `pre_execute` kopiert (`compute/executor.py:123-137`). |
| P6 | `python -m` | `python -m swarmq.cli` hat das Arbeitsverzeichnis schon im Pfad (außer `-P`). Nur das Console-Script ist betroffen; die compat-Suite startet mit `python -m` und ist daher nicht betroffen. |
| Nebenbefund N1 | – | Python 3.14 (PEP 649): `task.py:476` (`inspect.signature`) wirft bei einem unauflösbaren Namen in `run()` ohne Future-Import einen `NameError`. Lokal reproduziert, auch ohne dishka. Die dokumentierte Abwägung gilt heute nur unter PEP 563 (E9). |
| Nebenbefund N2 | – | `TaskRegistry.register` (`registry.py:29`) wertet die Annotationen von `run()` mit `eval_str=True` aus; ein unauflösbarer Name lässt den Bau von `SwarmQ(...)` mit `NameError` scheitern, mit und ohne Future-Import. `RegistryEntry.return_type` wird nirgends gelesen. N2 von Georg am 2026-10-12 entschieden: tolerant lesen, als Nachtrag in MR 7 (`annotationlib.Format.FORWARDREF`; Tests in `test_app.py`, `test_registry.py`, Test 7 um die Registrierung erweitert). |
| Nebenbefund | – | Der Entscheidungsbaum (`decision-trees*.md:104-112`) zeigt „NoRetry → DLQ via DLX“. Tatsächlich legt der Worker eine Kopie ab. Wird in MR 2 korrigiert. |
| Zeilen | – | Bestätigt: `lifecycle.py:157-182`, `pipeline.py:116-121`, `retry.py:95-97` und `298-311`, `cancellation.py:30-59`, `client/schedule.py:203-218`, `dishka.py:312-320`, `cli/worker_cmd.py:34,67-69`, `cli/parser.py:89-91`, `decode.py:94-107,126-135`, `gates/unknown_task.py:78-117`, `dead-letter-queues.md:281-286`, `test_failure_cascades.py:565`, `defining-tasks.md:118-127`, `middleware.md:175-177`, `message-formats.md:87,100`. |

### Review-Befunde und Umgang (Revision 2)

Stress-Review vom 2026-10-12 gegen `feat/phoenix-kleinpunkte` (1f9d3a8). Jeder P0- und P1-Befund wurde im Worktree selbst nachgeprüft (Code gelesen, Proben `p_classdef.py`, `p563.py`, `p649.py`, `p649di.py`, `pedge.py` auf CPython 3.14.4 mit dishka 1.10.1 erneut ausgeführt). Die Zeilenabweichungen um 1–3 Zeilen sind dort korrigiert, wo der Plan sie zitiert.

| ID | Befund (kurz) | Umgang | Wo im Plan |
|---|---|---|---|
| P0-1 | T13: exakte Gleichheit in `test_workflow_advancer.py:743,825`, `test_retry.py:414`, `test_terminal.py:95` | übernommen: Klasse nur mit Wert schreiben/übergeben; `test_retry.py:414` in Abschnitt 6; neuer Test 18 | T13, MR 3, Abschnitt 6 |
| P0-2 | `--no-scheduler` erreicht die Supervisor-Kinder nicht | übernommen (bestätigt: `WorkerConfig` ohne Feld, Kind-Namespace aus `WorkerConfig`): Feld + `run.py`, Test 11a | T1, MR 5 |
| P0-3 | `apply_app_dir` im Import bräche `test_no_symbols_added` | übernommen: Aufruf als `_shared.apply_app_dir(args)` | T8, MR 6 |
| P0-4 | Skip-Regel trägt nicht (Key mit `ForwardRef`, `NameError` aus `wrap_injection`) | übernommen und erweitert: T5 Regeln 1–4; unter PEP 649 bleibt der heutige `NameError` (mit Hinweis), unter PEP 563 meldet T6 beim Start | T5, S3, MR 7 Tests 6, 8a, 8b |
| P0-5 | Registry-Verschmutzung: `--check`- und `start_worker`-Tests würden rot | übernommen: Differenzprüfung bei `--check` (T18), Registry-Isolation in `test_app.py` (Abschnitt 6) | T7, T18, Abschnitt 6 |
| P1-6 | Füllen leerer Meldungen wirkt auf Header, `TASK_DLQ`, Workflow, alte Clients | übernommen, aber **anders gelöst** als vorgeschlagen: statt Changelog-Eintrag keine Füllung mehr (Hauptbedingung, Mischbetrieb); Pin-Tests 6–8; Füllung als Folgefrage in Abschnitt 9 | T12, Q1, MR 3 |
| P1-7 | Transport der Klasse über Gate-Callables mit fester Stelligkeit | übernommen (empfohlene Variante): Tabelle in `dead_letter`, Gates/Pipeline unverändert | T17, MR 3 |
| P1-8 | ungeplante Werte auf Timeout-, Pool-, `unknown`- und Lock-Render-Pfaden | übernommen: je Fall festgelegt (E7 A wörtlich → `null`), Tests 11–13 | T12, T17, MR 3 |
| P1-9 | FAF speichert ohne `result_ttl > 0` nichts | übernommen: Test 9 mit `result_ttl=60` | MR 3 |
| P1-10 | `placed` vor Release und `TASK_DLQ` gesetzt | übernommen: Bedingung `placed or on_publish_failure is not None`, Test 6; Doppelbehandlung nach Abschnitt 9 | T9, MR 1, Abschnitt 9 |
| P1-11 | Stufe `"warning"`, `_render_table` fest, ungültiger Wert | übernommen; ungültiger Wert → Befund `error`, Exit 78 (wie der Worker-Start) | T4, MR 5 |
| P1-12 | Idempotenz-Wächter auf `_election_task` | übernommen: Wächter auf `_report_task`, Test 6a | T2, MR 5 |
| P1-13 | Testisolation `sys.path`/`sys.modules` | übernommen, mit Einschränkung: aus `sys.modules` nur Module aus neu hinzugekommenen Pfaden entfernen (sonst doppelte Klassenobjekte); Idempotenz wie `python -m` festgelegt | T8, MR 6 |
| P1-14 | argparse-Fehler unter `--check`, Detektor kennt `--app-dir` nicht | übernommen: Tests 4a, 4b, Detektor erweitert | T8, MR 6 |
| P1-15 | Doku-Aussage gilt nur für den direkten Publish | übernommen: `delay`/`eta` → roher `TypeError`; Dataclass/Struct/Enum, `int`-Schlüssel, `nan` dokumentiert und charakterisiert | MR 4 (a) |
| P1-16 | E9 zu eng (`dishka.py:335`, `lock.py:155`) | übernommen: Lesen immer tolerant, `lock.py` in E9, Test 12a | T5, MR 7 |
| P1-17 | DI-Methode mit unauflösbarem Nicht-DI-Parameter → neuer roher `NameError` | übernommen, aber **anders gelöst** als vorgeschlagen: kein neuer Importfehler unter PEP 563 (Hauptbedingung), sondern nicht wrappen und T6 meldet beim Start; FromDishka-Strings behalten den heutigen Hinweis-`NameError`; Test 6a, Changelog | T5 Regel 4, T6, MR 7 |
| P1-18 | Ort des Imports, stdout, Hilfetext | übernommen: nur `check_cmd.py`, `redirect_stdout(sys.stderr)`, Hilfetext samt Golden/Spec | T18, MR 7, Abschnitt 6 |
| P1-19 | `SyntaxError` bei kaputter String-Annotation | übernommen: `SyntaxError`/`TypeError` fangen, Test 4a | T5 Regel 2, MR 7 |
| P1-20 | Abschnitt 6 für MR 8 unvollständig | übernommen und ergänzt: zusätzlich `test_worker.py:2772-2803`, `:2990-3007` und `test_pipeline_settle.py:74-87` gefunden; `test__invariants.py` gestrichen | Abschnitt 6 |
| P1-21 | S1: `max_message_size` des Brokers zuerst auslesen | übernommen | S1 |
| P2-22 | BZ-08: Replay-ID bei `undecodable` mit `task.id` | übernommen | MR 8 |
| P2-23 | privater Helfer, doppelte XQ bei `PublishIndeterminate`, `dlq list` | übernommen: Umbenennung als Refactoring, Grenze in Abschnitt 7, Changelog | MR 8, Abschnitt 7 |
| P2-24 | Budget `dishka.py`, fehlende Tests | übernommen: ≤ 100 Zeilen, Tests 7a und 14 | MR 7 |
| P2-25 | Stub, Env-Art, Spec-Test, N-fache WARNING, eingebettet, Doku | übernommen; Env-Art `"str"` wie `SWARMQ_SCHEDULER_PRUNE` | T1, T3, MR 5 |
| P2-26 | `getcwd`, `safe_path` injizieren, Hinweis im Kind, Stdlib, `cli.md:96`, S2 | übernommen | T8, MR 6, S2 |
| P2-27 | Test 3 über private Methode, Retry-Hinweis, `errors.md:40` | übernommen | MR 4 |
| P2-28 | `TaskCancelledError` nicht über `str()`, `transport_field`, Golden-Ort, `null` vs. fehlend, `expiry.py`, compat 0.12.0rc1 | übernommen | MR 3, Q2, Abschnitt 6, Abschnitt 9 |
| P2-29 | Herkunft der Instanz in Test 6, nicht picklebares `Retry` | übernommen: Test präzisiert, Altfehler nach Abschnitt 9 | MR 2, Abschnitt 9 |
| P2-30 | Gate 6 ohne `--extra reload` | übernommen | Q12 |

Abgelehnt wurde kein Befund. Zwei Korrekturvorschläge (P1-6, P1-17) wurden in der Sache übernommen, aber mit einer Lösung umgesetzt, die das Verhalten im Mischbetrieb bzw. beim Import erhält. Neue Produktfragen für Georg sind dabei nicht entstanden; die Frage nach dem Füllen leerer Meldungen steht als Folgearbeit in Abschnitt 9 und blockiert keine Unit.
