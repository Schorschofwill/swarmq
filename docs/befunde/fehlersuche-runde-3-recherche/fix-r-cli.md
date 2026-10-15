# Fix-Recherche CLI, Startprüfung, Exitcodes, Tooling (F3-05, F3-17, F3-18, F3-19, F3-24, justfile)

Stand: Worktree `swarmq-fehlersuche3`, Branch `test/fehlersuche-runde-3` @ 968ff7f (0.12.2-Entwurf mit Nachweisen), Register `compound-packs/swarmq-entscheidungen/` mit den Nachträgen vom 2026-10-15.
Laufzeit und Werkzeuge aus `.venv`: CPython 3.14.4, just 1.58.0. valkey-py, aio-pika und croniter spielen in diesem Paket keine Rolle.
Nur gelesen. Ausgeführt wurden die vier Nachweisdateien mit `--runxfail -p no:randomly`. Alle 10 Fälle scheitern an der fachlichen Assertion (F3-05: `1 == 78`; F3-17: `ok: true`; F3-18: rc 0 bzw. 1; F3-19: fehlende Schlüssel). Dazu liefen Proben ohne Dienste in `scratchpad/fs3/cliprobe/` (nicht im Repo). Der Supervisor war dort durch einen Stub ersetzt, es wurde nichts gestartet.

**Querschnitt Mischbetrieb.** Kein Fix in diesem Paket ändert ein Nachrichtenformat, einen Header, einen Key oder ein Lua-Skript. Alle Änderungen wirken nur lokal im CLI-Prozess bzw. im Worker-Kind derselben Installation. Supervisor und Kind haben immer dieselbe Version. Ein 0.12.3-Worker neben 0.12.0–0.12.2 verhält sich auf der Leitung gleich. Es ändern sich nur Exitcodes und `--check`-Ergebnisse. Ein Rollback stellt das alte, nachsichtigere Verhalten wieder her, Zustand bleibt keiner zurück. Bei den Befunden steht nur noch, wo Betreiber eine Änderung merken.

**Querschnitt Größengrenzen.** Der Architektur-Guard (`tests/unit/test_architecture_guards.py:37`, `_MAX_MODULE_LINES = 800`, keine Ausnahmen) ist für dieses Paket unkritisch. Stand der berührten Dateien: `cli/check_cmd.py` 309, `cli/env_check_cmd.py` 402, `cli/__init__.py` 528, `process_manager/check.py` 370, `process_manager/entry.py` 226, `config.py` 674, `agent_spec.py` 744. Der größte Puffer fehlt bei `agent_spec.py` (56 Zeilen). Dort ändert sich höchstens Text in vorhandenen Zeilen. Größte Dateien im Paket insgesamt: `worker/pipeline.py` 797, `logging_file.py` 771, `logging_queue.py` 760, `agent_spec.py` 744. Keiner der Fixes hier berührt `pipeline.py`.

**Querschnitt Codeüberlappung.**
- `cli/check_cmd.py`: F3-17 und F3-19, also dieselbe Unit.
- `process_manager/check.py`: F3-05 (`_validate_module_importable`) und F3-17 (neuer Helfer `start_config`). Am besten in einer Unit oder nacheinander.
- `process_manager/run.py:150`: F3-17 (Helfer) und der Nebenbefund N-1.
- `cli/env_check_cmd.py`: nur F3-18. Es nutzt den Helfer aus F3-17, deshalb F3-17 vor F3-18.
- `website/reference/cli.md` und `cli.de.md` (Exitcode-Tabelle, `--check`-Zeile, Abschnitt `swarmq check`): F3-05, F3-17, F3-18, F3-24, außerdem F3-38 aus einem anderen Paket (Zeile 78). Doku-Edits gehören in einen Schritt, sonst entstehen Merge-Konflikte in derselben Tabellenzeile.
- `website/reference/agent/spec/cli.json` und `website/llms.txt` sind generiert (`python -m swarmq.agent_spec`). Der Drift-Test steht in `tests/unit/test_agent_spec.py:470-485`. Jede Änderung an Hilfetexten (`parser.py`, `parser_ops.py`) oder an `agent_spec._cli_exit_codes` braucht eine Regenerierung. Sie sollte einmal am Ende geschehen, sonst kollidieren die Units dort.
- `changelog.md` und `changelog.de.md`: Es gibt noch keinen Abschnitt `[0.12.3]`, oben steht `[0.12.2] — unreleased`. Die erste Unit legt ihn an, alle anderen hängen an.

---

## F3-05 (P2): ImportError und SyntaxError beim Import des Task-Moduls enden mit Exit 1 statt 78

### Ursache (verifiziert)

- **Kind (Hauptfall).** `cli/worker_cmd.py:33-58` (`run_worker_async`) importiert das Task-Modul. Ein `ImportError`, der nicht ein fehlendes dishka und nicht das Modul selbst betrifft, wird unverändert weitergeworfen (`:58`). Dasselbe gilt für einen `SyntaxError`, den `except ImportError` gar nicht fängt. `process_manager/entry.py:206` fängt nur `(ConfigurationError, ModuleNotFoundError)`. Alles andere verlässt `worker_subprocess_entry`, und das Kind endet mit 1. Der Supervisor startet es fünfmal neu und endet mit `worker_restart_limit_exceeded` / `all_worker_slots_exhausted` und Exit 1. Probe: `--configure brokenpkg.hooks:setup` mit `SyntaxError` lief genau so in fünf Neustarts.
- **Elternprozess (zusätzlich gefunden, gleiche Regel).** `process_manager/check.py:59-67` (`_validate_module_importable`) ruft `importlib.util.find_spec`. Bei `pkg.tasks` führt das `pkg/__init__.py` im Supervisor aus. `ImportError` und `ValueError` werden zu `ConfigurationError` (78). Ein `SyntaxError` in `pkg/__init__.py` fällt durch, `cli/__init__.py:374` fängt ihn nicht, und `main` endet mit rohem Traceback und Exit 1. Probe: `--module brokenpkg.tasks` mit Syntaxfehler in `brokenpkg/__init__.py` ergibt rc 1 mit Traceback auf stderr, ohne `worker_config_error`. E-057 verlangt 78 „im Elternprozess wie im Kind“. Derselbe Weg gilt für `_validate_configure_spec` (`check.py:240-245`).
- **`--configure`-Hook im Kind.** `worker_cmd.py:170-173` hüllt nur `ImportError` in `ConfigurationError`. Ein `SyntaxError` im Hook-Modul geht durch und ergibt heute Exit 1 mit Neustarts. Laut `cli.md:35` heißt „`--configure` hook failed“ aber 78. Der Docstring `:163` sagt „Jeder Fehler wird … gehüllt“. Es ist kein eigener Befund, Option A behebt es mit.

### Nachweis-Test

`tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py` prüft das entschiedene Verhalten korrekt. Ein echtes Modul in `tmp_path` läuft durch `worker_subprocess_entry` und den echten `run_worker_async`. Erwartet werden 78, `worker_config_error` mit der Ursache im `error` und beim `SyntaxError` `error_type == "SyntaxError"`. Die letzte Bedingung schließt eine Lösung aus, die den `SyntaxError` in `ConfigurationError` hüllt und dabei den Typ im Logsatz verliert. Der Test deckt das Kind ab, nicht den Elternprozess und nicht „andere Ausnahmen bleiben 1“. Dafür gibt es Zusatztests unten.

### Optionen

- **A (empfohlen): breiter fangen im Entry, im Eltern-Pre-Flight `SyntaxError` ergänzen.**
  - `entry.py:206`: `except (ConfigurationError, ImportError, SyntaxError) as exc:`. `ImportError` umfasst `ModuleNotFoundError`, `SyntaxError` umfasst `IndentationError` und `TabError`. `_log_config_error` übernimmt `type(exc).__name__` unverändert, also `error_type` `SyntaxError` bzw. `ImportError`.
  - `check.py:61`: `except (ImportError, SyntaxError, ValueError)` (ValueError siehe Rückfrage unten). Der Elternprozess meldet dann `ConfigurationError` mit 78 und Logsatz.
  - Kommentar in `entry.py:208-215` anpassen (E-057 statt nur ModuleNotFoundError).
  - Vorteil: wenige Zeilen, und beide E-057-Tests bleiben unverändert grün. `test_import_error_in_the_child_is_a_configuration_error` mockt `run_worker_async` und wirft `ModuleNotFoundError` direkt. Der Fang muss also im Entry bleiben und darf nicht nur am Importaufruf sitzen. Ein `SyntaxError` im `--configure`-Hook wird gleich mit 78.
  - Nachteil: Auch ein `ImportError`, der später in `run_worker_async` entsteht und nicht vom Task-Modul kommt, ergibt 78. Beispiele sind ein `SwarmQProvider` ohne dishka-Extra (`dishka.py:469`) und eine kaputte Installation. Das ist ebenso deterministisch, ein Neustart hilft dort nicht. Für `ModuleNotFoundError` gilt dieser breite Fang schon heute, festgeschrieben durch den E-057-Test. Ausnahmen aus Tasks erreichen das Entry nicht, die Pipeline fängt sie.
- **B: eng am Import.** In `run_worker_async` `except (ImportError, SyntaxError)` um `import_module` legen und eine private Unterklasse von `ConfigurationError` werfen, die den Ursprungstyp für den Logsatz mitführt. Der Entry-Fang bleibt bei ModuleNotFoundError. Das ist genauer am Wortlaut „beim Import des Task-Moduls“, kostet aber Sonderlogik für `error_type` und einen neuen Typ. Der Elternteil braucht ohnehin dieselbe Änderung wie in A. Kein Gewinn, der den Aufwand rechtfertigt.

### Frage aus dem Auftrag: fehlendes optionales Paket im Kind

Ein Task-Modul importiert ein nicht installiertes Paket (`ModuleNotFoundError`, z. B. `pandas`). Das ergibt **schon heute 78** (`entry.py:206`), festgeschrieben in `test_entry.py::TestConfigErrorExitCode::test_import_error_in_the_child_is_a_configuration_error` (E-057, mit genau diesem Beispiel). E-057 nennt „jeder `ImportError` (einschließlich `ModuleNotFoundError`)“ ausdrücklich. Das ist also gewollt. Ein fehlendes optionales Paket von swarmq selbst (prometheus, watchfiles, PyYAML) erreicht das Entry nicht als ImportError: `metrics.py:76` schluckt ihn, `hot_reload.py:58-62` und `logging_file.py:173/284` hüllen ihn in `ConfigurationError`. Fehlt dishka, wird das in `worker_cmd.py:47-52` ebenfalls zu `ConfigurationError`. Alle diese Wege sind und bleiben 78.

### Pfade

- **Supervisor-Kind:** Hauptfix in `entry.py`.
- **Elternprozess:** `find_spec` über `_validate_module_importable`. Ein SyntaxError im Paket-`__init__` wird 78. Andere Ausnahmen im Paket-`__init__`, etwa `TypeError`, fallen weiter durch und ergeben Exit 1 mit Traceback. Das entspricht E-057.
- **`--check`:** `_check_task_injection` (`check_cmd.py:92-111`) importiert. `ImportError` und `SyntaxError` landen im allgemeinen `except Exception` (`:194-198`) mit `ok: false` und rc 1, `error_type` ist der Klassenname. Laut E-062 bleibt rc 1, hier ist nichts zu tun. Eine Feinheit: Ein SyntaxError in `pkg/__init__` meldet `--check` nach dem Fix in `_validate_module_importable` als `error_type: "ConfigurationError"`, heute als `"SyntaxError"`. Das ist zulässig, der Typ ist Klassenname und kein fester Wert.
- **Eingebettet (`SwarmQ(...)` im eigenen Prozess):** Den Import macht der Nutzer selbst, swarmq vergibt keinen Exitcode. Hier ist nichts zu tun.
- **CPU-Kind (`worker/compute/child.py`):** importiert das Task-Modul nicht selbst. Der Loop-Prozess hat es vor dem Poolstart schon in `run_worker_async` importiert, also greift derselbe Fix. Der Forkserver-Preload (`compute/preload.py`, `wiring.preload_modules`) lädt das Modul erneut. CPython 3.14.4 schluckt dort einen `ImportError` im Preload (`multiprocessing/forkserver.py:235-236`). Da der Loop-Prozess vorher scheitert, wird das nie erreicht. `envelope.py:152` importiert nur Ausnahmeklassen beim Rückbau und fängt alles. Kein Handlungsbedarf.
- **`--reload` (Entwickler):** Wer beim Editieren einen Syntaxfehler speichert, sieht künftig sofort Exit 78 aus dem Supervisor. Heute kommen fünf Neustarts in Sekunden, danach `all_worker_slots_exhausted` und Exit 1. Am Ende steht also in beiden Fällen ein Abbruch, jetzt ist er nur schneller und eindeutig. Erwähnenswert im Changelog.

### Rückfrage an Georg (Abweichung vom Wortlaut von E-057, besteht schon heute)

Ein `ValueError` auf Modulebene in `pkg/__init__.py` (bei `--module pkg.tasks`) wird schon heute im Elternprozess zu 78 (`check.py:61`). Probe: `valpkg/__init__.py` mit `raise ValueError` ergibt rc 78 und `worker_config_error`. Das `ValueError` im Fang war für fehlerhafte Punktpfade gedacht. E-057 sagt aber: `ValueError` auf Modulebene ergibt Exit 1. Das ist ein Randfall und kein Bestandteil von F3-05. Empfehlung: im Patch nicht anfassen und E-057 in einem Nachtrag präzisieren („im Elternprozess wird ein Fehler beim Auflösen des Paketpfads zu 78“). Alternative: den Modulnamen vor `find_spec` per Regex prüfen und `ValueError` aus dem Fang nehmen. Dann endet derselbe Randfall mit rohem Traceback und Exit 1, was nicht schöner ist.

### Folgefehler und Entscheidungen

- E-057-Tests: `test_entry.py::…::test_import_error_in_the_child_is_a_configuration_error` und `test_cli.py::TestMainErrorRendering::test_module_not_found_exits_with_the_config_code` bleiben mit Option A grün. Der zweite mockt `run_multi_process_worker` mit `ModuleNotFoundError`, und `cli/__init__.py:374` bleibt unverändert. Optional kann dort `ImportError` und `SyntaxError` dazukommen, aus Symmetrie. Einen heutigen Auslöser gibt es nicht.
- `TestConfigErrorExitCode` (`test_entry.py:212ff.`) enthält weitere 78-Tests. Sie werden durch den breiteren Fang nicht berührt.
- Supervisor: `_handle_exit_code` (`supervisor.py:596-605`) behandelt 78 schon heute ohne Neustart (R29/R20). Dort ändert sich nichts.
- Agent-Spec: `agent_spec.py:480-488` sagt bei 78 „task module not importable“. Das passt, optional mit „(ImportError, SyntaxError)“ ergänzen. `agent_spec.py:472` (Exit 1: „ConfigurationError, …, ModuleNotFoundError“) beschreibt schedule/dlq/inspect und bleibt.
- Mischbetrieb: Es ändert sich nur der Exitcode des Kinds. Mit `RestartPreventExitStatus=78` startet ein kaputtes Deployment nicht mehr neu, das ist die Absicht. Rollback: wieder fünf Neustarts.

### Doku (EN+DE)

- `website/reference/cli.md:35` / `cli.de.md:36`: „task module not importable (any `ImportError`, including `ModuleNotFoundError`, or `SyntaxError` while importing it; other exceptions at module level stay `1`)“.
- `website/reference/agent/error-catalog-machine.md:66` / `.de.md:67`: Spalte Auslöser um `SyntaxError` ergänzen.
- `src/swarmq/agent_spec.py:483` (optional) und danach `spec/cli.json` sowie `llms.txt` regenerieren.
- Changelog 0.12.3 (Fixed): EN „`swarmq worker`: an `ImportError` such as `cannot import name …` or a `SyntaxError` in the task module (or in its package `__init__`, or in a `--configure` hook module) now ends the run with exit `78` after one attempt instead of five restarts and exit `1` (F3-05). Other exceptions raised while importing stay exit `1`.“ DE entsprechend.

### Testszenarien

1. Nachweis: Kind mit `from os import gibt_es_nicht` ergibt `SystemExit(78)`, `worker_config_error.error` nennt den Namen.
2. Nachweis: Kind mit Syntaxfehler ergibt 78, `error_type == "SyntaxError"`.
3. Neu: Kind mit `IndentationError` ergibt 78 (Unterklasse, Randfall).
4. Neu, E-057 „andere bleiben 1“: Task-Modul mit `raise ValueError("x")` auf Modulebene. Erwartet: keine `SystemExit(78)`, die Ausnahme verlässt das Entry (also Kind-Exit 1). Dieser Test schreibt die zweite Hälfte von E-057 fest.
5. Neu, Elternprozess: `--module pkg.tasks` mit Syntaxfehler in `pkg/__init__.py`, `main([...])` mit Stub für `run_multi_process_worker`-Interna oder direkt `check_worker_config(module="pkg.tasks", …)`. Erwartet: `ConfigurationError` bzw. rc 78 und kein Traceback auf stderr.
6. Neu: `--configure` mit Hook-Modul, das einen `SyntaxError` enthält, ergibt im Kind 78 (über `_run_configure_hook` und den Entry-Fang).
7. Regression: Ein `RecycleAckTimeout` ergibt weiter 75, `ForkSourceRejected` weiter 71. Die `except`-Reihenfolge in `entry.py:190-217` bleibt.

### Marker und Register nach dem Fix

- Nachweis: `xfail` und `befund("F3-05")` entfernen (`pyproject.toml:163`: befund immer mit xfail), `@pytest.mark.entscheidung("E-057")` setzen (2 Tests). Test 4 bekommt ebenfalls E-057.
- `e057-…md`: `tests:` um die beiden Nachweis-Tests und Test 4 ergänzen, `ohne_test:` streichen.

### Dateien

`src/swarmq/process_manager/entry.py`, `src/swarmq/process_manager/check.py`, `tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py`, ggf. `tests/unit/process_manager/test_check.py` oder `tests/unit/test_cli.py` (Elternfall), `website/reference/cli.md`, `cli.de.md`, `agent/error-catalog-machine.md`, `.de.md`, `changelog.md`, `changelog.de.md`, `compound-packs/swarmq-entscheidungen/e057-exit-78-konfigurationsfehler.md`, optional `src/swarmq/agent_spec.py` mit `spec/cli.json` und `llms.txt`.

Aufwand S. Risiko niedrig.

---

## F3-17 (P3): `swarmq worker --check` prüft die Env-Werte des Starts nicht

### Ursache (verifiziert)

Der Start lehnt die vier Werte erst im Pre-Flight des Supervisors ab: `process_manager/run.py:150-155` baut `SwarmQConfig(broker_url, backend_url, logging_settings=…)`. Dort wirft `_apply_env_overrides` bzw. die Validierung `ConfigurationError`, das ergibt 78 über `cli/__init__.py:374`. `--check` (`cli/check_cmd.py:114-200`) prüft Logging, Namensraum, `resolve_worker_options` und `check_worker_config`, baut aber kein `SwarmQConfig`. Proben bestätigen das:
- `SWARMQ_SCHEDULER=maybe`: „scheduler must be one of ['on', 'off']“
- `SWARMQ_CONCURRENCY=0`: „concurrency must be >= 1“
- `SWARMQ_SHUTDOWN_TIMEOUT=abc`: „must be an integer“
- `SWARMQ_VALKEY_EVICTION_CHECK=bogus`: „valkey_eviction_check must be one of …“

Weitere Werte, die nur `SwarmQConfig` ablehnt und `--check` heute durchlässt, sind zum Beispiel `SWARMQ_QUEUE_TYPE=foo`, `SWARMQ_SCHEDULER_TIMEZONE=Mars/X`, `SWARMQ_DEAD_LETTER_TTL_S=0` und alle `_POSITIVE_FIELDS`.

### Nachweis-Test

`tests/unit/test_befund_f3_17_worker_check_env.py` prüft die vier Fälle mit `ok: false`, rc 1 und `error_type == "ConfigurationError"`, ohne URLs. Das ist korrekt nach E-062. **Lücke:** Der Test würde auch grün, wenn der Fix fälschlich fehlende URLs ablehnte, denn auch dann käme `ok: false` mit `ConfigurationError`. Nötig ist ein Gegentest: gültige Umgebung ohne `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL` (ausdrücklich `delenv`) ergibt `ok: true`, rc 0. Die vorhandenen `ok: true`-Tests in `test_cli_multiprocess.py` (z. B. `:376`) entfernen die URL-Variablen nicht und hängen damit von der Umgebung ab.

### Optionen

- **A (empfohlen): gemeinsamer Helfer für Pre-Flight-Konfiguration.** In `process_manager/check.py` kommt `start_config(*, logging_settings, broker_url="", backend_url="", **overrides) -> SwarmQConfig` hinzu. Er baut `SwarmQConfig` genau wie `run.py:150`. Fehlende URLs ersetzt er durch Platzhalter (`amqp://check.invalid/`, `valkey://check.invalid/0`), weil E-062 sie nicht prüfen lässt. `SwarmQConfig` prüft URLs nur auf „nicht leer“ (`config_validation.py:85-91`), also stört der Platzhalter nicht. Ein gesetzter `SWARMQ_BROKER_URL` überschreibt ihn ohnehin über `_apply_env_overrides`.
  - `run.py:150` ruft den Helfer mit den echten URLs auf. Das Verhalten ändert sich nicht.
  - `check_cmd._run_worker_check` ruft ihn nach `check_worker_config` und vor `_check_task_injection` auf: billige Prüfung vor dem teuren Import. `_check_logging_settings` gibt dazu seine `settings` zurück, statt `None`. Dann überspringt `SwarmQConfig` die Logging-Env wie im Start (`run.py:147-149`).
  - So prüfen `--check` und Start garantiert dieselbe Konfiguration, auch für spätere Validierungen wie F3-04 („Fristen sind endlich“) oder F3-38.
- **B: Prüfung in `check_worker_config` selbst.** Das ist die öffentliche API (`process_manager.__init__:7`), der Docstring verspricht „implies the supervisor would get past pre-flight“. Dafür bräuchte es ein neues kwarg, `run.py` würde doppelt bauen, und externe Aufrufer bekämen im Patch neues Wurfverhalten. Für 0.12.3 zu breit.

### Nebenbefund N-1 (neu, nicht im Register, Gegenprüfung nach `docs/befunde/README.md` nötig)

Der Pre-Flight in `run.py:150` baut `SwarmQConfig` **ohne** `worker_config_kwargs(args)` (`resolved_fields`). Damit liest er `SWARMQ_CONCURRENCY` und `SWARMQ_SCHEDULER` aus der Umgebung, obwohl die Flags sie überstimmen. Proben mit gestubbtem Supervisor:
- `SWARMQ_CONCURRENCY=0 swarmq worker --module … --concurrency 4` ergibt rc 78 („concurrency must be >= 1, got 0“).
- `SWARMQ_SCHEDULER=maybe swarmq worker … --no-scheduler` ergibt rc 78.

Das widerspricht `cli.md:4-10` („a flag wins in every process“) und `configuration.md:9-13`. Für Logging-Werte ist der Fall ausdrücklich gelöst (`run.py:147-149`), für die Worker-Optionen nicht. Das Kind selbst nutzt `resolved_fields` (`worker_cmd.py:134-141`). Für F3-17 heißt das: Der Helfer muss **eine** Quelle für Start und `--check` sein. Bekäme nur `--check` die `resolved_fields`, meldete es in genau diesen Fällen `ok: true`, und der Start endete mit 78. Das wäre ein E-062-Bruch. Wird N-1 bestätigt, reicht der Helfer `worker_config_kwargs(args)` durch, und beide Pfade sind behoben. Bis dahin bildet er den heutigen Pre-Flight ab.

### Folgefehler und Entscheidungen

- E-062-Tests: `test_cli_multiprocess.py::TestMainRouting::test_check_argparse_error_emits_json` ist unberührt. `test_port_guard.py::TestPreflightCommandAgrees::*` (4 Tests) setzen gültige Port-Umgebungen, und der Helfer ändert die Portprüfung nicht. Beide bleiben grün.
- Bestehende `--check`-Tests: Sie laufen ohne URL und setzen keine ungültigen `SWARMQ_*`-Werte, das zeigt ein Abgleich mit grep in `test_cli_multiprocess.py`. `tests/unit/conftest.py:18-40` räumt nur die Logging-Variablen ab. Steht in der Umgebung eines Entwicklers ein ungültiger `SWARMQ_*`-Wert, scheitern `--check`-Tests künftig. Das ist gewollt, aber eine mögliche Ursache für lokale Überraschungen.
- `run.py`-Tests, die `SwarmQConfig` patchen: Mit grep fand sich keiner, der `swarmq.process_manager.run.SwarmQConfig` patcht. Der Import in `run.py:145` ist lokal, und der Helfer importiert ebenfalls lokal.
- Mischbetrieb: Nur das `--check`-Ergebnis ändert sich. Eine Deploy-Pipeline, die `--check` als Gate nutzt, bricht jetzt früher ab, im Build statt beim Start. Das ist die Absicht. Rollback: `--check` wieder nachsichtiger.

### Doku (EN+DE)

- `website/reference/cli.md:123` / `cli.de.md:127` (`--check`-Zeile): einfügen „…and builds the worker configuration from the `SWARMQ_*` variables like the supervisor's pre-flight, so every value the start rejects with exit `78` fails here too. Missing `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL` are not checked (`--check` runs in build steps without secrets); the start then still ends with `78`.“
- `src/swarmq/cli/parser.py:362-373` (Hilfetext `--check`): Satz ergänzen, danach `spec/cli.json` und `llms.txt` regenerieren.
- `website/reference/configuration.md:12` / `.de.md`: „validates the same resolved values“ stimmt dann. Optional der Zusatz „except missing URLs“.
- Changelog 0.12.3 (Fixed): „`swarmq worker --check` now rejects every `SWARMQ_*` value the start rejects with exit `78` (e.g. `SWARMQ_SCHEDULER=maybe`, `SWARMQ_CONCURRENCY=0`); missing broker/backend URLs stay unchecked (F3-17).“

### Testszenarien

1. Nachweis: die vier Env-Werte, jeweils `ok: false`, rc 1, `ConfigurationError`.
2. Neu (E-062): gültige Umgebung, URL-Variablen per `delenv` entfernt, ergibt `ok: true`, rc 0.
3. Neu: `SWARMQ_QUEUE_TYPE=foo` ergibt `ok: false`. Das belegt, dass der Fix allgemein gilt und nicht nur für vier Variablen.
4. Neu, Gleichlauf (Muster `TestPreflightCommandAgrees`): Für jeden Fall aus 1 endet `run_multi_process_worker` mit `ConfigurationError`, mit gestubbtem `ProcessManager`. Beide Wege nutzen nachweislich denselben Helfer.
5. Neu: `--log-format human` mit `SWARMQ_LOG_FORMAT=xml` ergibt `ok: true` (Flag überstimmt, Logging-Settings werden durchgereicht wie im Start).
6. Falls N-1 bestätigt: `--concurrency 4` mit `SWARMQ_CONCURRENCY=0` ergibt `ok: true`, und der Start besteht den Pre-Flight.

### Marker und Register nach dem Fix

Nachweis: `xfail`/`befund` entfernen, `entscheidung("E-062")`, ebenso für Test 2. `e062-…md`: `tests:` ergänzen, aus `ohne_test:` den F3-17-Teil streichen (F3-19 siehe unten).

### Dateien

`src/swarmq/process_manager/check.py`, `src/swarmq/process_manager/run.py`, `src/swarmq/cli/check_cmd.py`, `src/swarmq/cli/parser.py`, `tests/unit/test_befund_f3_17_worker_check_env.py`, `tests/unit/process_manager/test_port_guard.py` oder ein neuer Gleichlauftest, `website/reference/cli.md`, `cli.de.md`, optional `configuration.md`/`.de.md`, `agent/spec/cli.json`, `website/llms.txt`, Changelogs, `e062-…md`.

Aufwand S. Risiko niedrig, das Hauptrisiko sind lokale Umgebungen mit kaputten `SWARMQ_*`-Werten.

---

## F3-18 (P3): `swarmq check` meldet ungültige Konfiguration mit 0 bzw. 1 statt 78

### Ursache (verifiziert)

- `cli/env_check_cmd.py:394` holt den Modus über `env_valkey_eviction_check()` (`config.py:149-155`). Ein ungültiger Wert wird dort still `strict`. Der Bericht ist OK, Exit 0.
- `env_check_cmd.py:260-267` (`_shutdown_section`) parst `SWARMQ_SHUTDOWN_TIMEOUT` mit `parse_env_value`. `abc` ergibt `ConfigurationError` in `build_report`, das allgemeine `except Exception` (`:395-397`) schreibt eine Zeile, Exit 1.
- Allgemein baut `swarmq check` kein `SwarmQConfig`. Nur `SWARMQ_SCHEDULER` (`_scheduler_section`, `:270-298`) und `SWARMQ_NAMESPACE` (`cli/__init__.py:483-489`) sagen den Start voraus.

### Nachweis-Test

`tests/unit/test_befund_f3_18_check_invalid_config_exit78.py` erwartet rc 78 für beide Fälle, Valkey-Grenze gemockt. Das entspricht dem E-006-Nachtrag. Der Test prüft nur den Exitcode, nicht die Meldung. Ein Zusatztest sollte die Variable in stderr verlangen.

### Optionen

- **A (empfohlen): Vorprüfung vor jeder Verbindung, wie beim Namensraum.** In `run_env_check` nach dem URL-Check und vor `_describe_backend`: `start_config(backend_url=url, broker_url=…, scheduler="on", resolved_fields=("scheduler",))` mit dem Helfer aus F3-17. Bei `ConfigurationError` kommt `swarmq check: <Meldung>` auf stderr, Rückgabe `CONFIG_ERROR_EXIT_CODE`.
  - Warum `scheduler` ausgenommen: `test_cli_env_check.py:303-315` (`test_invalid_scheduler_value_fails_like_the_worker`) erwartet bei `SWARMQ_SCHEDULER=no` einen **Bericht** mit Befund `scheduler_invalid` und 78. Ohne Ausnahme bräche die Vorprüfung vorher ab, und der Test würde rot. Mit `resolved_fields=("scheduler",)` ignoriert `SwarmQConfig` die Env-Variable. Probe: `resolved ok` bei `SWARMQ_SCHEDULER=maybe`. Der Abschnitt `scheduler` bleibt die eine Stelle für diesen Wert.
  - Danach ist der Modus garantiert gültig, `env_valkey_eviction_check()` liefert den echten Wert. `_shutdown_section` und `_seen_interval_s` können nicht mehr am Parsen scheitern.
  - Vorteil: Die Vorhersage stimmt auch, wenn Valkey nicht erreichbar ist. Der Worker scheitert dann an der Konfiguration (78), nicht an der Verbindung (1). Das Muster ist dasselbe wie beim Namensraum, kein neues Berichtsformat.
  - Nachteil: Bei ungültiger Konfiguration kommt kein JSON-Bericht auf stdout. Beim ungültigen Namensraum ist das heute genauso.
- **B: Befund `config_invalid` im Bericht.** Es bräuchte einen Abschnitt `config`, eine Ebene `error` und 78 über `ok: false`. Das hält den Bericht vollständig, setzt aber eine Valkey-Verbindung voraus. Ist Valkey unerreichbar, kommt 1 statt 78, die Vorhersage ist dann falsch. Dazu kommen ein neues Feld im JSON-Bericht und eine Erweiterung der Tabellenausgabe. Nicht empfohlen.

### Abgrenzung zum Eviction-Fallback „strict“ (E-006-Nachtrag)

`env_valkey_eviction_check()` bleibt unverändert. Producer und CLI-Backends (`schedule`, `dlq`, `inspect`, Kontrollkanal) werten `bogus` weiter als `strict`, damit die Prüfung läuft. Nur `run_env_check` prüft vorher streng. Der Docstring von `env_valkey_eviction_check` bekommt den Zusatz „`swarmq check` lehnt einen ungültigen Wert vorher ab (E-006)“. `configuration.md:59` wird angepasst (siehe Doku).

### Folgefehler und Entscheidungen

- E-006-Tests (`test_cli_env_check.py:119/133/161/172/190`, `test_cli_env_check_queues.py:45/120`, `test_befund_bz04_eviction_policy.py`, `test_valkey_environment.py`): Sie setzen nur gültige Modi (`strict|warn|off`) und keine ungültigen Werte. Sie bleiben grün. `test_evicting_policy_without_strict_only_warns` mit `warn`/`off` ist gültig.
- E-012: `test_cli_env_check_namespace.py:126` (`test_invalid_namespace_exits_78`) greift vorher in `main` und bleibt grün.
- E-015 (`test_cli_env_check_queues.py:70`) ist nicht berührt.
- Neue 78-Fälle, die bisher 0 oder 1 waren: jeder ungültige `SWARMQ_*`-Wert, auch Logging (`SWARMQ_LOG_FORMAT=xml`). Ohne Flags sagt das den Worker korrekt voraus, denn auch der scheitert dann mit 78. Ein Betreiber, der den Worker mit Flags startet, die einen ungültigen Env-Wert überstimmen, bekommt von `swarmq check` 78 und vom Worker einen Start. `swarmq check` kennt keine Worker-Flags. Das ist eine Lücke, nach E-006 („genau vorhersagen“) aber vertretbar. Sie gehört in einen Satz der Doku.
- `build_report` bleibt rein. Seine direkten Aufrufer in Tests übergeben gültige Umgebungen.
- Mischbetrieb: `swarmq check` läuft vor dem Upgrade mit der **neuen** Version. Ein Gate, das bisher bei 0 weiterlief, stoppt jetzt bei kaputter Konfiguration. Genau das soll der Befehl leisten.

### Doku (EN+DE)

- `website/reference/cli.md:558-563` / `cli.de.md:577ff.`: „`78` when a worker would refuse to start in this environment (evicting policy under `strict`, invalid `SWARMQ_NAMESPACE` or `SWARMQ_SCHEDULER`, or any other `SWARMQ_*` value the worker rejects, such as `SWARMQ_VALKEY_EVICTION_CHECK=bogus` or `SWARMQ_SHUTDOWN_TIMEOUT=abc`; checked before connecting, message on stderr, no report)“.
- `cli.md:35` / `cli.de.md:36` (78-Zeile, Teil `swarmq check`): dieselbe Ergänzung kurz.
- `website/reference/configuration.md:59` / `.de.md`: „`swarmq check` rejects an invalid value with exit `78`, like the worker; for producers and other CLI commands an invalid value counts as `strict`.“
- `src/swarmq/cli/parser_ops.py:417` (Hilfe `check`) und `src/swarmq/agent_spec.py:485-487` (78-Bedeutung): Text ergänzen, danach `spec/cli.json` und `llms.txt` regenerieren. `test_cli_spec_documents_exit_78_of_check` prüft nur „`swarmq check`“ im Text und bleibt grün.
- Modul-Docstring `env_check_cmd.py:21-24`.
- Changelog 0.12.3 (Fixed): „`swarmq check` now exits `78` for any `SWARMQ_*` value a worker would reject, e.g. `SWARMQ_VALKEY_EVICTION_CHECK=bogus` (was `0`, silently treated as `strict`) or `SWARMQ_SHUTDOWN_TIMEOUT=abc` (was `1`) (F3-18).“

### Testszenarien

1. Nachweis: `EVICTION_CHECK=bogus` und `SHUTDOWN_TIMEOUT=abc` ergeben rc 78.
2. Neu: stderr nennt die Variable bzw. das Feld. Es wird kein Valkey-Client gebaut (Fake zählt `from_url`-Aufrufe = 0), damit belegt der Test „vor jeder Verbindung“.
3. Neu: `EVICTION_CHECK=bogus` mit **unerreichbarem** Valkey (Fake wirft beim `ping`) ergibt 78 und nicht 1.
4. Regression: `SWARMQ_SCHEDULER=no` ergibt weiter einen JSON-Bericht mit `scheduler_invalid` und 78 (bestehender Test).
5. Regression E-006: `EVICTION_CHECK=WARN` (Großschreibung, `mode` normalisiert) ergibt 0 mit Befund `warning`.
6. Abgrenzung: `env_valkey_eviction_check()` mit `bogus` liefert weiter `strict`. Das ist der Festschreibungstest des E-006-Nachtrags für die Producer-Nachsicht. Er sollte `entscheidung("E-006")` tragen, falls noch keiner existiert (grep fand keinen).

### Marker und Register nach dem Fix

Nachweis: `xfail`/`befund` entfernen, `entscheidung("E-006")`. `e006-…md`: `tests:` um die Nachweise und Test 6 ergänzen. Der Nachtrag nennt den Test bereits.

### Dateien

`src/swarmq/cli/env_check_cmd.py`, `src/swarmq/config.py` (nur Docstring `env_valkey_eviction_check`), `src/swarmq/cli/parser_ops.py`, `src/swarmq/agent_spec.py`, `tests/unit/test_befund_f3_18_check_invalid_config_exit78.py`, `tests/unit/test_cli_env_check.py` (Zusatztests), `website/reference/cli.md`, `cli.de.md`, `configuration.md`, `configuration.de.md`, `agent/spec/cli.json`, `website/llms.txt`, Changelogs, `e006-…md`. Abhängig von F3-17 (Helfer `start_config`).

Aufwand S. Risiko niedrig.

---

## F3-19 (P3): Argparse-Payload unter `--check` unvollständig, Doku nennt Exit 2

### Ursache (verifiziert)

- `cli/check_cmd.py:291-309` (`_emit_argparse_check_error`) schreibt nur acht Schlüssel. Es fehlen `pool`, `slots`, `configure`, `metrics_ports` und `health_ports`. Probe `main(["worker","--check"])` bestätigt das.
- **Zusätzlich gefunden:** Auch der normale Fehlerpfad ist unvollständig. `_WORKER_CHECK_PAYLOAD_KEYS` (`:26-39`) und `_seed_check_payload` (`:42-63`) enthalten `metrics_ports`/`health_ports` nicht. Nur der Erfolgspfad setzt sie (`:185-186`). Probe `--module=does_not_exist_x --check` ergibt einen Payload ohne beide Ports. `message-formats.md` beschreibt die Felder aber als Teil des Payloads (`list[int] | null`). Das ist derselbe Vertragsbruch („stable field-set“).
- Doku: `website/reference/agent/message-formats.md:530-531` und `.de.md:531-532` nennen „exit `2`“. Maßgeblich ist laut E-062 Exit 1 (`cli.md:33`, Code `:309`).

### Nachweis-Test

`tests/unit/test_befund_f3_19_check_argparse_payload.py` prüft elf Vertragsschlüssel. Die beiden Port-Schlüssel fehlen in seiner Liste `_CONTRACT_KEYS`, obwohl der Auftrag und die Tabelle sie nennen. Empfehlung: beide in `_CONTRACT_KEYS` aufnehmen. Der Test trägt noch keinen `entscheidung`-Marker und darf vor dem Fix erweitert werden. Außerdem braucht es einen zweiten Test für den `ConfigurationError`-Pfad.

### Optionen

- **A (empfohlen): eine Schlüsselquelle.** `metrics_ports` und `health_ports` in `_WORKER_CHECK_PAYLOAD_KEYS` aufnehmen und in `_seed_check_payload` als `None` setzen. `_emit_argparse_check_error` baut den Payload aus der Konstante, alle Werte `None`. Danach werden `ok=False`, `warnings=[]`, `error` und `error_type="ArgparseError"` gesetzt. Damit kann keiner der drei Pfade (Erfolg, Fehler, Argparse) den Feldsatz mehr verlieren. `warnings` bleibt `[]`: Der Typ ist `list[str]`, und so ist es bei jedem anderen Fehler auch. `reload` bleibt `null` („Rest null“ nach E-062). `pool` wird `null`, nicht wie im Seed-Payload `io`, weil argparse nichts aufgelöst hat.
- B: nur die fehlenden drei Schlüssel im Argparse-Pfad ergänzen. Lässt den Fehlerpfad ohne Ports und die Doppelpflege bestehen. Nicht empfohlen.

### Folgefehler und Entscheidungen

- E-062: `test_check_argparse_error_emits_json` prüft `ok`, `error_type` und Text. Er bleibt grün.
- `tests/unit/test_cli_module_surface.py:67/91/113` führt die privaten Namen als `internal`. Die Namen bleiben, der Test bleibt grün.
- Konsumenten: Neue Schlüssel im Fehlerpfad sind additiv. Ein Agent, der `dict.get` nutzt (so empfiehlt es `check_cmd.py:22-25`), merkt nichts. Kein Bruch, auch nicht im Mischbetrieb.

### Doku (EN+DE)

- `website/reference/agent/message-formats.md:530-531`: „On an argparse error (exit `1`, not argparse's usual `2`) the payload has `error_type: "ArgparseError"`, `warnings: []` and every other field `null`.“ DE `:531-532` entsprechend.
- Optional `message-formats.md:488-490` („exits `0` (`ok: true`) or `1`“) passt bereits.
- Changelog 0.12.3 (Fixed): „`swarmq worker --check`: every payload, also on argparse errors and validation failures, now carries the full field set including `pool`, `slots`, `configure`, `metrics_ports` and `health_ports` (`null` when unknown) (F3-19).“ Unter Docs: „message formats: argparse errors under `--check` exit `1`, not `2`.“

### Testszenarien

1. Nachweis (erweitert): `main(["worker","--check"])` ergibt rc 1, alle 13 Schlüssel vorhanden, `slots`/`configure`/`metrics_ports`/`health_ports` `None`.
2. Neu: `--module=does_not_exist --check` (`ConfigurationError`-Pfad) ergibt alle 13 Schlüssel, die Ports `None`.
3. Neu: unerwartete Ausnahme (Muster `test_check_reports_unexpected_exception`) ergibt alle 13 Schlüssel.
4. Neu, Guard: Die Menge der Schlüssel im Erfolgspayload ist gleich der Menge in `_WORKER_CHECK_PAYLOAD_KEYS`. Der Test liest den öffentlichen Vertrag aus der Doku-Tabelle oder hält eine eigene Liste, wie der Nachweis.

### Marker und Register nach dem Fix

Nachweis: `xfail`/`befund` entfernen, `entscheidung("E-062")`. `e062-…md`: `tests:` ergänzen. `ohne_test:` entfällt ganz, sobald F3-17 und F3-19 erledigt sind.

### Dateien

`src/swarmq/cli/check_cmd.py`, `tests/unit/test_befund_f3_19_check_argparse_payload.py`, ggf. `tests/unit/test_cli_multiprocess.py` (Test 2/3), `website/reference/agent/message-formats.md`, `.de.md`, Changelogs, `e062-…md`. Gleiche Datei wie F3-17, also eine Unit.

Aufwand XS. Risiko sehr niedrig.

---

## F3-24 (P3, Doku): ungültiges `SWARMQ_NAMESPACE` endet bei `schedule`/`dlq`/`inspect` mit 78

### Ursache (verifiziert)

`cli/__init__.py:480-489` prüft `env_namespace()` vor jedem Befehl außer `worker`, bei `ConfigurationError` mit `CONFIG_ERROR_EXIT_CODE`. Das ist gewollt (BZ-10, MR 15, E-012-Nachtrag). `website/reference/cli.md:32` / `cli.de.md:33` sagen dagegen: „every configuration error of `swarmq schedule`, `swarmq dlq` and `swarmq inspect` … exit 1“. Die 78-Zeile (`cli.md:35`) nennt den Namensraum nur für `swarmq check` (`:558-563`).

### Fix (nur Doku)

- `cli.md:32` (Zeile 1): nach der Aufzählung ergänzen: „Exception: an invalid `SWARMQ_NAMESPACE` ends every command with `78` (see below).“
- `cli.md:35` (Zeile 78): „`swarmq schedule`, `dlq`, `inspect` and `check`: an invalid `SWARMQ_NAMESPACE`, checked before any connection.“
- DE `cli.de.md:33/36` entsprechend.
- `configuration.md:60` (`SWARMQ_NAMESPACE`) nennt laut Register schon 78. Prüfen, ob dort „every command“ steht, sonst angleichen (EN+DE).
- Optional `agent_spec.py:472` (Exit 1) und `:480-488` (Exit 78) um den Namensraum ergänzen, danach `spec/cli.json` regenerieren.
- Changelog 0.12.3 (Docs): „CLI reference: an invalid `SWARMQ_NAMESPACE` ends `schedule`, `dlq` and `inspect` with exit `78`, not `1` (F3-24).“

### Folgen

Es gibt keinen Nachweis-Test (Doku-only). Der festschreibende Test `test_cli_env_check_namespace.py::test_invalid_namespace_exits_78` trägt schon `entscheidung("E-012")` (Zeile 126). Der E-012-Nachtrag sagt dagegen „ohne `entscheidung`-Marker“. Das stimmt nicht mit dem Code überein und gehört in Register-Hygiene, nicht in diesen Fix. Optional käme ein Test für `schedule list` mit ungültigem Namensraum hinzu (rc 78, keine Verbindung), der `inspect` und `dlq` mit abdeckt. Mit grep fand sich kein solcher Test außer für `check`. Er würde E-012 vollständig festschreiben.

### Dateien

`website/reference/cli.md`, `cli.de.md`, ggf. `configuration.md`/`.de.md`, optional `src/swarmq/agent_spec.py` mit `spec/cli.json` und `llms.txt`, Changelogs, optional ein Test in `tests/unit/test_cli_env_check_namespace.py`.

Aufwand XS.

---

## Tooling: `just integration`/`just e2e` grenzen bei Pfad-Argument nicht ein

### Ursache (verifiziert)

`justfile:47-52` übergibt immer zuerst den Suiten-Pfad: `scripts/run-tests.sh tests/integration "$@"`. Mit `uv run just integration tests/integration/test_x.py` läuft pytest mit `tests/integration tests/integration/test_x.py`. pytest sammelt das Verzeichnis, und der Pfad grenzt nicht ein. Doppelte Pfade entfernt pytest von sich aus, solange `--keep-duplicates` nicht gesetzt ist. Dasselbe gilt für `unit` (`:35-36`), `property` (`:39-40`) und `stress` (`:60-61`). `chaos` (`:56-57`) übergibt keinen Pfad und nutzt `testpaths = ["tests"]` (`pyproject.toml:144`). Für Integration ginge das nicht, denn ohne Pfad liefe die ganze Suite einschließlich Unit. `scripts/run-tests.sh:25` reicht `"$@"` unverändert weiter.

### Optionen

- **A (empfohlen): Standardpfad in `run-tests.sh`, Erkennung eines Pfad-Arguments dort.** Neue Option `--suite PFAD`, also `run-tests.sh [--no-services] [--suite PFAD] <pytest-Argumente>`. Die Rezepte werden zu `scripts/run-tests.sh --suite tests/integration "$@"`, analog für `e2e`, `unit`, `stress` und `property` (`--suite tests/property -m property`). `run-tests.sh` lässt den Suiten-Pfad weg, sobald ein Benutzerargument ein Pfad ist. Ohne Argumente oder nur mit Optionen bleibt alles wie bisher. `_all` und `_coverage` übergeben keine Argumente und ändern sich nicht. Die zentrale Stelle ist mit dem vorhandenen uv-Stub testbar.
- B: Logik in jedem Rezept (bash-`if` im justfile). Das ist fünfmal dieselbe Logik und schwerer zu testen. Nicht empfohlen.
- C: eigenes Rezept `integration-path PFAD`. Einfach, ändert aber die gewohnte Bedienung und löst `e2e` nicht mit. Nicht empfohlen.

### Robuste Erkennung eines Pfad-Arguments

Ein Argument zählt als Pfad, wenn alle drei Bedingungen gelten:
1. Es beginnt nicht mit `-`. Damit fallen `--ignore=…`, `-kfoo` und `-x` heraus.
2. Es ist nicht der Wert einer vorangehenden Option, die ein getrenntes Argument nimmt. Das betrifft vor allem `-k`, `-m`, `-p`, `-o`, `-c`, `-W`, `--deselect`, `--ignore`, `--ignore-glob`, `--rootdir`, `--confcutdir`, `--basetemp`, `--junitxml`, `--maxfail`, `--tb`, `--durations`, `--timeout`, `--randomly-seed`, `--log-level` und `--log-file`. Nur bei Optionen, deren Wert ein existierender Pfad sein kann (`-k tests`, `--ignore tests/x.py`, `--deselect …`, `--rootdir`, `-c`, `--confcutdir`, `--basetemp`), ist das wirklich nötig. Bei den übrigen ist es Vorsicht.
3. Es existiert nach Abschneiden einer Node-ID (`${arg%%::*}`) als Datei oder Verzeichnis. Das Rezept läuft im Repo-Wurzelverzeichnis, denn just setzt das Arbeitsverzeichnis auf das Verzeichnis des justfile.

Damit bleiben `-k foo`, `-k tests`, `-m slow` und `--ignore tests/integration/x.py` ohne Eingrenzung. `tests/integration/test_x.py::test_y[a-b]` grenzt ein. Ein Tippfehler im Pfad existiert nicht, die Suite bleibt also davor, und pytest meldet „file or directory not found“. Der Fehler bleibt dadurch sichtbar.

Skizze für `run-tests.sh`. `set -u` mit leerem Array funktioniert in bash ab 4.4, wie schon bei `lock=()`.

```bash
suite=()
if [[ "${1:-}" == "--suite" ]]; then suite=("$2"); shift 2; fi
has_path_arg() {
    local prev="" arg
    for arg in "$@"; do
        case "$prev" in
            -k|-m|-p|-o|-c|-W|--deselect|--ignore|--ignore-glob|--rootdir|--confcutdir|--basetemp|--junitxml|--maxfail|--tb|--durations|--timeout|--randomly-seed|--log-level|--log-file)
                prev=""; continue ;;
        esac
        prev="$arg"
        [[ "$arg" == -* ]] && continue
        [[ -e "${arg%%::*}" ]] && return 0
    done
    return 1
}
if has_path_arg "$@"; then suite=(); fi
exec "${lock[@]}" uv run --extra test --extra reload "${runner[@]}" "${suite[@]}" "$@" "--randomly-seed=${seed}"
```

Reihenfolge: `--no-services` zuerst, dann `--suite`, wie die Rezepte es aufrufen. Achtung bei `property`: Das Rezept selbst übergibt `-m property`. Mit `--suite tests/property -m property "$@"` sieht `has_path_arg` auch `-m property` und überspringt `property` als Optionswert. Das ist korrekt. Die Ausgabe bleibt ohne Benutzerargumente `pytest tests/property -m property`, die bestehenden Assertions passen weiter.

### Tests (`tests/unit/test_justfile.py`, echtes just und uv-Stub, bestehendes Muster)

Die Pfade sollten absolut in `tmp_path` liegen. So hängen die Tests nicht von Repo-Dateien ab, und die Existenzprüfung ist deterministisch.
1. `just integration <tmp>/test_x.py` ergibt einen Aufruf, der `pytest <tmp>/test_x.py` enthält und nicht `pytest tests/integration`.
2. `just e2e <tmp>/test_x.py::test_y` grenzt auf die Node-ID ein.
3. `just integration -k foo` ergibt `pytest tests/integration -k foo`.
4. `just integration -k tests` (der Wert existiert als Verzeichnis) ergibt weiter `pytest tests/integration -k tests`.
5. `just integration --ignore <tmp>/x.py` ergibt weiter `pytest tests/integration --ignore …`.
6. `just unit <tmp>/test_x.py` grenzt ein und sperrt weiter nicht (`lock=free`).
7. Nicht existierender Pfad: Die Suite bleibt davor.
8. Regression: `test_all_holds_the_lock_once_across_every_suite`, `test_coverage_measures_every_suite_under_one_lock` und `test_recipes_run_the_ci_commands` (stress) bleiben ohne Änderung grün.

Laufzeit: Jeder Test startet just und bash echt, also über 100 ms. Die Datei begründet das schon im Kopf (`:7-9`). Die Testliste im Kopf wird ergänzt.

### Doku

- `justfile:1-7` (Kopfkommentar): „Ein Pfad-Argument grenzt die Suite ein (`uv run just integration tests/integration/test_x.py`), Optionen wie `-k` nicht.“
- `scripts/run-tests.sh:4` (Aufrufzeile).
- `docs/testing/test-strategy.md` (um `:221` und `:272`): Beispiel mit Pfad.
- `AGENTS.md` („Tests ausführen“): Ein Halbsatz wäre nützlich, weil Agenten Integrationstests nur über `just` starten dürfen. Das ist eine Änderung an den Projektanweisungen und braucht Georgs Ok.
- Kein Changelog, es ist internes Tooling.

### Dateien

`justfile`, `scripts/run-tests.sh`, `tests/unit/test_justfile.py`, `docs/testing/test-strategy.md`, optional `AGENTS.md`.

Aufwand S. Risiko niedrig. Die CI ruft `just integration`/`just e2e` ohne Argumente auf (`test-strategy.md:71-72`) und bleibt gleich.

---

## Dateien je Befund

| Befund | Produktionscode | Tests | Doku/Register |
|---|---|---|---|
| F3-05 | `src/swarmq/process_manager/entry.py`, `src/swarmq/process_manager/check.py` (`_validate_module_importable`), opt. `src/swarmq/agent_spec.py` | `tests/unit/process_manager/test_befund_f3_05_import_error_exit78.py`, neuer Elternfall-Test (z. B. `tests/unit/process_manager/test_check.py` oder `tests/unit/test_cli.py`) | `website/reference/cli.md`, `cli.de.md`, `agent/error-catalog-machine.md`, `.de.md`, Changelogs, `e057-…md`, opt. `agent/spec/cli.json`, `website/llms.txt` |
| F3-17 | `src/swarmq/process_manager/check.py` (`start_config`), `src/swarmq/process_manager/run.py`, `src/swarmq/cli/check_cmd.py`, `src/swarmq/cli/parser.py` | `tests/unit/test_befund_f3_17_worker_check_env.py`, Gleichlauftest (`tests/unit/process_manager/test_port_guard.py` o. neu) | `cli.md`, `cli.de.md`, opt. `configuration.md`/`.de.md`, `agent/spec/cli.json`, `llms.txt`, Changelogs, `e062-…md` |
| F3-18 | `src/swarmq/cli/env_check_cmd.py`, `src/swarmq/config.py` (Docstring), `src/swarmq/cli/parser_ops.py`, `src/swarmq/agent_spec.py` | `tests/unit/test_befund_f3_18_check_invalid_config_exit78.py`, `tests/unit/test_cli_env_check.py` | `cli.md`, `cli.de.md`, `configuration.md`, `configuration.de.md`, `agent/spec/cli.json`, `llms.txt`, Changelogs, `e006-…md` |
| F3-19 | `src/swarmq/cli/check_cmd.py` | `tests/unit/test_befund_f3_19_check_argparse_payload.py`, opt. `tests/unit/test_cli_multiprocess.py` | `agent/message-formats.md`, `.de.md`, Changelogs, `e062-…md` |
| F3-24 | (opt. `src/swarmq/agent_spec.py`) | opt. `tests/unit/test_cli_env_check_namespace.py` | `cli.md`, `cli.de.md`, ggf. `configuration.md`/`.de.md`, Changelogs |
| Tooling | `justfile`, `scripts/run-tests.sh` | `tests/unit/test_justfile.py` | `docs/testing/test-strategy.md`, opt. `AGENTS.md` |
| N-1 (neu, offen) | `src/swarmq/process_manager/run.py` bzw. Helfer in `process_manager/check.py` | Gleichlauftest Flag schlägt ungültige Env | `docs/befunde/fehlersuche-runde-3-2026-10.md` (Aufnahme nach Gegenprüfung) |

## Wechselwirkungen mit anderen Befunden

- **F3-17 → F3-18:** F3-18 nutzt den Helfer `start_config` aus F3-17. Reihenfolge F3-17 vor F3-18, oder beide in einer Unit („Startvorhersage“).
- **F3-17/F3-19:** gleiche Datei `check_cmd.py`, gleiche Regel E-062. Eine Unit.
- **F3-05 und F3-17:** beide in `process_manager/check.py`, aber verschiedene Funktionen. Konfliktarm, aber nacheinander mergen.
- **F3-04 (Fristen endlich), F3-38 (`concurrency > 65535`), F3-13/F3-14 (Konfiguration):** Jede neue Validierung in `SwarmQConfig` wirkt nach F3-17/F3-18 automatisch in `swarmq worker --check` und `swarmq check`. Das ist gewollt (E-062: „eine neue Startablehnung gehört auch in `--check`“). Die Tests dieser Units sollten einen `--check`-Fall mitnehmen, oder F3-17/F3-18 kommen zuerst, damit die anderen Units es testen können.
- **F3-38 und F3-05 und F3-18 und F3-24:** alle ändern die Zeile `78` der Exitcode-Tabelle in `cli.md:35`/`cli.de.md:36`. Doku-Änderungen dort bündeln.
- **F3-25 (DE-Changelog 0.1.0 `--check`):** `changelog.de.md`, anderer Abschnitt, nur Merge-Nähe.
- **F3-06 (Guides, modulweite `app` gegen CLI-eigene `SwarmQ`):** berührt `worker_cmd.py` nur thematisch. Ein Fix dort (falls es einen gibt) darf den Importblock `worker_cmd.py:33-58` nicht so umbauen, dass `SyntaxError` wieder durchrutscht. Mit Option A sitzt der Fang im Entry, das ist robust.
- **Generierte Spec:** `spec/cli.json` und `llms.txt` ändern sich bei F3-05 (opt.), F3-17, F3-18 und F3-24 (opt.). Einmal am Ende regenerieren, sonst gibt es Konflikte in einer generierten Datei.
- **N-1:** Wird es bestätigt, gehört der Fix in denselben Helfer wie F3-17, mit kleinem Mehraufwand. Ohne Bestätigung bleibt der Helfer beim heutigen Verhalten, und `--check` und Start stimmen trotzdem überein.

## Widersprüche zum Register / offene Punkte

1. **E-057 und Eltern-`ValueError`:** Ein `ValueError` auf Modulebene in einem Paket-`__init__` ergibt im Elternprozess schon heute 78 (`check.py:61`). Nach dem Wortlaut von E-057 wäre es 1. Der Patch ändert das nicht. Empfehlung: E-057 präzisieren (Rückfrage an Georg).
2. **E-012-Nachtrag:** Dort steht „`test_invalid_namespace_exits_78` (ohne `entscheidung`-Marker)“. Der Test trägt den Marker aber (`test_cli_env_check_namespace.py:126`). Das ist Register-Hygiene.
3. **F3-17-Nachweis:** Er schreibt „fehlende URLs bleiben ungeprüft“ (E-062) nicht positiv fest. Ein Gegentest ist nötig, sonst bliebe ein falscher Fix grün.
4. **F3-19-Nachweis:** Er prüft `metrics_ports`/`health_ports` nicht, und der normale Fehlerpfad hat sie ebenfalls nicht. Den Test erweitern und den Fix breiter als „nur argparse“ anlegen.
5. **N-1 (neu):** Der Supervisor-Pre-Flight ignoriert „Flag schlägt Env“ für `concurrency` und `scheduler` (`run.py:150`). Das widerspricht `cli.md:4-10`. Eine Gegenprüfung nach `docs/befunde/README.md` steht vor jedem Fix aus. Die Entscheidung, ob F3-17 den Fix mitnimmt, liegt bei Georg bzw. beim Orchestrator.
