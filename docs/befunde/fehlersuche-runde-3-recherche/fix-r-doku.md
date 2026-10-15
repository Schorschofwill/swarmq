# Fix-Recherche Doku und Exporte (F3-06, -10, -11, -20, -21, -22, -23, -25, -28, -30, -33, -37)

Stand: Worktree `swarmq-fehlersuche3`, Branch `test/fehlersuche-runde-3` @ 968ff7f (0.12.2-Entwurf, Nachweise, Register E-055–E-071).
Bibliotheken aus `.venv`: valkey 6.1.1, aio-pika 10.0.1, dishka 1.10.1.
Nur gelesen. Ausgeführt: der Nachweis F3-11 mit `--runxfail -p no:randomly` (rot an der fachlichen Assertion, 0,14 s) und zwei Lese-Skripte, die die Tabelle in `agent/error-catalog-machine.md` (EN, DE) gegen `spec/errors.json` abgleichen.

**Querschnitt Mischbetrieb.** Neun der zwölf Punkte ändern nur Doku. Code ändert sich an zwei Stellen, beide ohne Format- oder Protokollwirkung:
- F3-11: zwei zusätzliche Namen in `swarmq.__all__`, rein additiv.
- F3-20: ein Wert in der erzeugten Spec (`TaskCancelledError.when` von `["run","result"]` auf `["result"]`). Das ändert kein Schema, keine Nachricht und keinen Key.
Rolling Deploy und Rollback gehen in beide Richtungen. Einziger Rückweg-Hinweis: Code mit `from swarmq import PublishRejected` läuft auf ≤ 0.12.2 nicht (ImportError beim Import). Wer zurückrollen können muss, importiert aus `swarmq.exceptions`. Das gehört in den Changelog.

**Querschnitt Doku-Tests.** Es gibt keinen Doku-Build in der CI und keinen Strict-Modus (`mkdocs.yml` ohne `strict`, `.readthedocs.yaml` baut nicht strikt, `.gitlab-ci.yml` ohne mkdocs). Snippets werden nicht ausgeführt, Links nicht geprüft, EN/DE-Parität nicht automatisch geprüft. Folgende Tests lesen Doku oder erzeugte Doku-Artefakte:

| Test | Liest | Wirkung auf dieses Paket |
|---|---|---|
| `tests/unit/test_befund_f3_11_exception_exports.py` | Tabelle „Hierarchy at a glance“ in `website/reference/errors.md`, Regex `^\| \`(\w+)\` \| \`` | Bricht, wenn F3-20 die Tabellenzeile `TaskCancelledError` (`errors.md:38`) umformatiert. Form `` | `Name` | `Parent` | Text | `` beibehalten. |
| `tests/unit/test_agent_spec.py::test_committed_spec_files_match_generators` | `website/reference/agent/spec/*.json`, `website/llms.txt` | Bricht nach der Änderung an `agent_spec.py`, bis `uv run python -m swarmq.agent_spec` neu erzeugt hat (F3-20). Die Version in `index.json` wird beim Vergleich ausgeblendet. |
| `tests/unit/test_agent_spec.py` (übrige) | nur Code | F3-20 bekommt dort seinen roten Test, F3-23 den Guard. |
| `tests/unit/test_entscheidungsregister.py` (Guard) | Register-Frontmatter `tests:` gegen `entscheidung`-Marker | Jeder neue Marker (E-067, E-068) braucht den Eintrag in der Regel-Datei, sonst rot. |

Der Architektur-Guard (`tests/unit/test_architecture_guards.py:37`, 800 Zeilen) gilt nur für `src/`. Betroffen wären `src/swarmq/agent_spec.py` (744 Zeilen, Änderung +0) und `src/swarmq/__init__.py` (181, +4). `pipeline.py` berührt dieses Paket nicht.

---

## F3-06 (P2): Guides registrieren Handler und Middleware auf einer modulweiten `app`

### Ursache
- `src/swarmq/cli/worker_cmd.py:135` baut die App der CLI, `:145` ruft den `--configure`-Hook. Jede App hat ihren eigenen Dispatcher (`src/swarmq/app.py:78`). Eine `app` im Task-Modul ist eine andere Instanz. Dort registrierte Handler und Middleware laufen unter `swarmq worker` nie, ohne Warnung.
- Das ist entschieden (E-069). Der Code ist richtig, die Guides sind falsch.

### Präzisierung gegenüber dem Register
- Nicht jedes Signal ist betroffen. `TASK_ENQUEUED`, `WORKFLOW_STEP_ENQUEUED` und `TASK_DUPLICATE_DETECTED` mit Quelle `client.*` feuert die App, die publiziert (`client/schedule.py:135, 330, 405, 454`, `workflow_enqueue.py:118, 124`). Für den Producer ist die modulweite `app` also richtig. `TASK_DUPLICATE_DETECTED` mit `source="scheduler.fire"` feuert der Leader (`scheduler/fire.py:371`).
- Bei Middleware gilt dasselbe: `pre_enqueue`/`post_enqueue` laufen in der publizierenden App, `pre_execute`, `post_execute`, `on_error`, `on_retry` in der App des Workers.
- `locking-and-dedup.md:340` ist **nicht** betroffen. Der Verify-Abschnitt dort prüft nur Logzeilen (`task_lock_conflict`, `task_succeeded`), keinen Handler.
- Zusätzlich betroffen, nicht im Register: der Verify-Text `middleware.de.md:223-226` (DE-Gegenstück zu `middleware.md:216-219`).
- Richtig sind `reference/cli.md:99`, `:174-198` und das Tutorial `tutorials/first-email-worker.md:359-450` (es nutzt einen programmatischen Worker). `reference/api.md:405` ist ein API-Beispiel ohne CLI-Bezug; ein Satz dort ist optional.

### Optionen
**A: Hinweisblock im Signals-Guide, `--configure` in den Verify-Abschnitten, Querverweise in den übrigen Guides (empfohlen).** Die vielen Snippets mit `@app.on_signal` bleiben lesbar. Ein Block erklärt einmal, welche App gemeint ist. Middleware- und Rate-Limit-Guide zeigen den Hook ausdrücklich.

**B: Alle Snippets in `def setup(app)` umschreiben.** Konsequent, aber über 20 Snippets in vier Guides (EN+DE). Für Producer-Signale wäre es falsch.

### Textvorschläge

**`website/guides/signals.md`, nach `:41` (Ende des Absatzes zu `**kwargs`) einfügen:**

```markdown
!!! warning "Register on the app that dispatches the signal"
    A handler runs on the `SwarmQ` instance that dispatches the signal.
    Worker-side signals (`TASK_STARTED`, `TASK_COMPLETED`, `TASK_FAILED`,
    `TASK_DLQ`, `TASK_RATE_LIMITED`, `WORKER_*`, `SCHEDULER_*` and the
    others the worker emits) fire on the worker's app. `swarmq worker`
    builds that app itself: a module-level `app` in your task module is
    a different instance, and handlers on it never run, without a
    warning. Register them in a function and pass it with `--configure`:

    ```python title="myapp/hooks.py"
    from swarmq import Signal, SwarmQ
    from swarmq.logging import get_logger

    log = get_logger("myapp.signals")


    def setup(app: SwarmQ) -> None:
        @app.on_signal(Signal.TASK_FAILED)
        async def on_task_failed(task_id, task_name, exception, **kwargs):
            log.error("myapp_task_attempt_failed", task_id=task_id,
                      task_name=task_name, error=exception)
    ```

    ```bash
    swarmq worker --module myapp.tasks --configure myapp.hooks:setup
    ```

    An embedded worker (`await app.start_worker()`) uses the app you
    built. `TASK_ENQUEUED`, `WORKFLOW_STEP_ENQUEUED` and
    `TASK_DUPLICATE_DETECTED` from a `client.*` source fire in the
    process that publishes; register those on the producer's app. The
    examples below write `app` for whichever app applies. See
    [`--configure`](../reference/cli.md#registering-middleware-with-configure).
```

**`website/guides/signals.de.md`, nach `:43`:**

```markdown
!!! warning "An der App registrieren, die das Signal auslöst"
    Ein Handler läuft an der `SwarmQ`-Instanz, die das Signal auslöst.
    Worker-Signale (`TASK_STARTED`, `TASK_COMPLETED`, `TASK_FAILED`,
    `TASK_DLQ`, `TASK_RATE_LIMITED`, `WORKER_*`, `SCHEDULER_*` und die
    übrigen, die der Worker sendet) feuern an der App des Workers.
    `swarmq worker` baut diese App selbst: Eine modulweite `app` in deinem
    Task-Modul ist eine andere Instanz, und Handler darauf laufen nie,
    ohne Warnung. Registriere sie in einer Funktion und übergib sie mit
    `--configure`:

    (gleicher Code- und Bash-Block wie EN)

    Ein eingebetteter Worker (`await app.start_worker()`) nutzt die App,
    die du gebaut hast. `TASK_ENQUEUED`, `WORKFLOW_STEP_ENQUEUED` und
    `TASK_DUPLICATE_DETECTED` mit einer `client.*`-Quelle feuern in dem
    Prozess, der publiziert; diese registrierst du an der App des
    Producers. Die Beispiele unten schreiben `app` für die jeweils
    passende App. Siehe
    [`--configure`](../reference/cli.md#middleware-per-configure-registrieren).
```

**`signals.md:10` (Voraussetzungen):** `- A \`SwarmQ\` instance.` ersetzen durch `- The \`SwarmQ\` instance that dispatches the signal; under \`swarmq worker\`, a \`--configure\` hook receives it.` DE `signals.de.md:11`: `- Die \`SwarmQ\`-Instanz, die das Signal auslöst; unter \`swarmq worker\` bekommt sie ein \`--configure\`-Hook.`

**`signals.md:286-291` (Verify):**

```markdown
Register one handler for `TASK_COMPLETED` that logs an event of its
own in a `--configure` hook, schedule a task, watch the worker log:

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup \
  --log-level INFO --log-format human
```
```
Nach `:296` ergänzen: `If the line does not appear, check that the handler is registered in the hook, not on a module-level \`app\` in \`myapp/tasks.py\`.`
DE `signals.de.md:298-303`: „Registriere in einem `--configure`-Hook einen Handler für `TASK_COMPLETED`, der ein eigenes Event loggt, …“, Bash-Zeile wie EN; nach `:309`: „Erscheint die Zeile nicht, prüfe, ob der Handler im Hook registriert ist und nicht an einer modulweiten `app` in `myapp/tasks.py`.“

**`website/guides/middleware.md`, nach `:140` (Ende „Registering middleware“) einfügen:**

```markdown
**Which app.** Hooks run on the app that does the work:
`pre_enqueue` and `post_enqueue` on the app that calls `schedule()`,
`pre_execute`, `post_execute`, `on_error` and `on_retry` on the
worker's app. `swarmq worker` builds its own app, so middleware added
to a module-level `app` in `myapp/tasks.py` never runs in the worker.
Add it in a `--configure` hook:

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ

from myapp.middleware import TimingMiddleware


def setup(app: SwarmQ) -> None:
    timing = TimingMiddleware()
    app.add_middleware(timing)

    @app.on_signal(Signal.TASK_CANCELLED)
    async def forget_cancelled(task_id, **kwargs):
        timing.forget(task_id)  # see "Sharing state between hooks"
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```
```
DE `middleware.de.md`, nach `:143`: „**Welche App.** Hooks laufen an der App, die die Arbeit macht: `pre_enqueue` und `post_enqueue` an der App, die `schedule()` aufruft, `pre_execute`, `post_execute`, `on_error` und `on_retry` an der App des Workers. `swarmq worker` baut seine eigene App; Middleware an einer modulweiten `app` in `myapp/tasks.py` läuft im Worker also nie. Füge sie in einem `--configure`-Hook hinzu:“ (gleiche Blöcke).

**`middleware.md:209-219` (Verify):** Bash-Zeile `swarmq worker --module myapp.tasks --configure myapp.hooks:setup --log-level DEBUG --log-format human`. Satz `:216-219` ersetzen:
`You should see the hook emit log entries on every task. If nothing appears, check that the worker starts with \`--configure\` and that the hook adds the middleware. Middleware added to a module-level \`app\` in \`myapp/tasks.py\` is not on the worker's app.`
DE `middleware.de.md:216-226`: Bash wie EN; „Du solltest bei jedem Task Log-Einträge des Hooks sehen. Wenn nichts erscheint, prüfe, ob der Worker mit `--configure` startet und der Hook die Middleware hinzufügt. Middleware an einer modulweiten `app` in `myapp/tasks.py` hängt nicht an der App des Workers.“

**`website/guides/rate-limiting.md:96`:** `Hook \`Signal.TASK_RATE_LIMITED\` to record when limits are active:` ersetzen durch `Hook \`Signal.TASK_RATE_LIMITED\` to record when limits are active. The worker dispatches it, so under \`swarmq worker\` register the handler in a \`--configure\` hook (see [Signals](signals.md#registering-a-handler)):` und das Snippet `:98-118` in `def setup(app: SwarmQ) -> None:` einrücken. DE `rate-limiting.de.md:100-101`: „Hake `Signal.TASK_RATE_LIMITED`, um aufzuzeichnen, wann Limits greifen. Der Worker löst es aus; unter `swarmq worker` registrierst du den Handler deshalb in einem `--configure`-Hook (siehe [Signals](signals.md#einen-handler-registrieren)):“ plus gleiches Snippet.

**`website/guides/locking-and-dedup.md`, nach `:308` (Ende der `source`-Liste):**
`The \`client.*\` sources fire on the app that calls \`schedule()\`; register the handler there. \`"scheduler.fire"\` fires on the scheduler leader, a worker: under \`swarmq worker\` register it in a \`--configure\` hook ([Signals](signals.md#registering-a-handler)).`
DE `locking-and-dedup.de.md`, nach `:324`: „Die `client.*`-Quellen feuern an der App, die `schedule()` aufruft; dort registrierst du den Handler. `"scheduler.fire"` feuert am Scheduler-Leader, einem Worker: unter `swarmq worker` in einem `--configure`-Hook registrieren ([Signals](signals.md#einen-handler-registrieren)).“

### Folgefehler
- Entscheidungen: E-069 (Tests `tests/unit/test_cli.py:400, 433`) bleibt unberührt, kein Code ändert sich.
- F3-02 (Code-Paket) ändert `middleware.md:12, 27` und `signals.md:62` (+ DE). Gleiche Dateien, andere Hunks. Siehe Unit-Schnitt.
- F3-30 ändert dieselben Middleware-Stellen (Beispiel und Hook). Beide gehören in eine Unit; der Hook-Code oben enthält schon den Cancel-Teil.
- Optional: `tutorials/first-email-worker.md:419-420` könnte `--configure` als Alternative zum programmatischen Worker nennen. Nicht nötig, der Text ist richtig.

### Testszenarien
Kein Nachweis-Test (Doku). Händisch: Hook aus dem Block in ein Beispielprojekt legen, `swarmq worker --module … --configure myapp.hooks:setup`, Task scheiternd planen. Erwartung: `myapp_task_attempt_failed` im Log. Gegenprobe: Handler auf modulweiter `app` → keine Zeile.

### Dateien
`website/guides/signals.md`, `website/guides/signals.de.md`, `website/guides/middleware.md`, `website/guides/middleware.de.md`, `website/guides/rate-limiting.md`, `website/guides/rate-limiting.de.md`, `website/guides/locking-and-dedup.md`, `website/guides/locking-and-dedup.de.md`.

### Register nach dem Fix
Keine Änderung. E-069 hat Tests.

---

## F3-10 (P3): `queue`, `timeout`, `polling` als `run()`-Parameter

### Ursache
- `src/swarmq/task.py:37-42`: `_RESERVED_RUN_PARAM_NAMES = {"delay", "eta", "unique_for", "unique_until"}`.
- `src/swarmq/client/schedule.py:184-193`: `schedule()` nimmt `queue` als Schlüsselwort ab. `client/results.py:169-177` und `app.py:197-205`: `run()` nimmt zusätzlich `timeout` und `polling` ab.
- Entschieden (E-064): nicht reservieren, Grenze dokumentieren.

### Weitere Beobachtung (außerhalb E-064)
- `schedule_many()` ist nicht betroffen. Die kwargs je Task gehen als Dict durch.
- `sig(task_name, *args, immutable=False, **kwargs)` (`src/swarmq/workflow.py:242-251`) nimmt `immutable` ab. `Task.s()`/`si()` nicht. Optional in die Liste aufnehmen; E-064 nennt den Namen nicht.
- Positionell übergeben funktioniert: `schedule("t", "eu")` gibt `"eu"` an `run(self, queue)` weiter. Das ist der Ausweg ohne Umbenennen.

### Optionen
**A: Zweiter Absatz unter „Reserved parameter names“ (empfohlen, entspricht E-064).**
**B: Eigener Abschnitt.** Kein Vorteil.

### Textvorschlag
**`website/guides/defining-tasks.md`, nach `:172` einfügen:**

```markdown
A few more keyword names belong to the calls that start a task, but
swarmq does not reject them on `run()`:

- `queue` — `app.schedule()` and `app.run()` read it as the target
  queue and do not pass it on. `schedule("export", queue="eu")` routes
  the task to the queue `eu`: the publish fails with
  `UnroutableMessage` if no such queue exists, or the task runs
  without the argument (`TypeError` for a required parameter).
- `timeout`, `polling` — `app.run()` reads them to wait for the result
  and does not pass them on.
- `immutable` — `sig()` reads it for the workflow signature.

Rename such a parameter, or pass the value positionally
(`schedule("export", "eu")`).
```

**`website/guides/defining-tasks.de.md`, nach `:177`:**

```markdown
Weitere Schlüsselwörter gehören zu den Aufrufen, die einen Task
starten, swarmq lehnt sie auf `run()` aber nicht ab:

- `queue` — `app.schedule()` und `app.run()` lesen es als Ziel-Queue
  und geben es nicht weiter. `schedule("export", queue="eu")` routet
  den Task in die Queue `eu`: Gibt es sie nicht, scheitert der Publish
  mit `UnroutableMessage`, sonst läuft der Task ohne das Argument
  (`TypeError` bei einem Pflichtparameter).
- `timeout`, `polling` — `app.run()` liest sie für das Warten auf das
  Ergebnis und gibt sie nicht weiter.
- `immutable` — `sig()` liest es für die Workflow-Signatur.

Benenne einen solchen Parameter um oder übergib den Wert positionell
(`schedule("export", "eu")`).
```
Wenn `immutable` nicht aufgenommen werden soll (E-064 nennt es nicht), die Zeile in EN und DE weglassen.

### Folgefehler
- E-064-Tests (`tests/unit/test_task.py:458, 473, 605, 619`) bleiben grün, kein Code.
- Kein Konflikt mit F3-28: anderer Abschnitt derselben Datei, gleiche Unit.

### Dateien
`website/guides/defining-tasks.md`, `website/guides/defining-tasks.de.md`.

### Register nach dem Fix
Keine Änderung (E-064 `ohne_test` für `queue` bleibt).

---

## F3-11 (P3): `BrokerPublishBlocked` und `PublishRejected` fehlen in `swarmq`

### Ursache
- `src/swarmq/__init__.py:19-44` importiert beide nicht, `__all__` (`:118-180`) nennt sie nicht. Sie kamen mit 51bdedb / a1c266a dazu, ohne Begründung für die Lücke.
- Nachweis `tests/unit/test_befund_f3_11_exception_exports.py`: liest die Tabelle in `errors.md`, prüft jede Klasse außer `WorkflowStateConflict` und `SettleFailedAfterPublish` auf `__all__` und Identität. Mit `--runxfail`: `AssertionError … Left contains 2 more items, first extra item: 'BrokerPublishBlocked'`. Der Test prüft das Richtige.

### Optionen
**A: Eager-Import wie alle anderen Exceptions (empfohlen).** `swarmq.exceptions` wird ohnehin eager geladen (`__init__.py:19`). Kein Lazy-Eintrag in `_LAZY_SURFACE` nötig, kein Einfluss auf `test_cli_import_laziness.py`.
**B: Lazy über `_LAZY_SURFACE`.** Kein Nutzen, uneinheitlich.

Code (`src/swarmq/__init__.py`):
- Import-Block `:19-44`: `BrokerPublishBlocked` nach `BrokerConnectionError`, `PublishRejected` nach `PublishIndeterminate`.
- `__all__`: `"BrokerPublishBlocked"` nach `"BrokerConnectionError"` (`:122`), `"PublishRejected"` nach `"PublishIndeterminate"` (`:141`). Die Liste ist isort-sortiert (ruff RUF022); `uv run ruff check --fix` ordnet sie richtig ein.
- Stubs: keine. `typings/` enthält nur `croniter` und `msgpack`; `stubtest` (`justfile:32`) prüft nur diese. mypy und basedpyright sehen den normalen Import.

### Folgefehler
- Aufrufer: keine im Code. Doku `errors.md:50-52` stimmt danach. `deployment.md:222` nennt `PublishRejected` ohne Importpfad.
- Tests: `tests/unit/test_exceptions.py:104-150` und `test_exceptions_unroutable_export.py` bleiben grün.
- Der Nachweis wird grün. Wegen `xfail(strict=True)` muss der xfail-Marker im selben Commit weg, sonst XPASS → rot.
- F3-20 ändert `errors.md:38`. Die Zeilenform muss bleiben, sonst findet der Regex des Nachweises die Klasse nicht mehr (dann fällt die Voraussetzung `len(documented) > 20` nicht auf, aber eine Klasse fehlt still).
- Mischbetrieb: rein additiv. Rollback: siehe Querschnitt.

### Changelog
EN (0.12.3, Fixed): `- **F3-11: \`BrokerPublishBlocked\` and \`PublishRejected\` can be imported from \`swarmq\`.** The error catalog promised it; before, only \`swarmq.exceptions\` had them. Code that must also run on 0.12.0–0.12.2 keeps importing from \`swarmq.exceptions\`.`
DE (Behoben): `- **F3-11: \`BrokerPublishBlocked\` und \`PublishRejected\` lassen sich aus \`swarmq\` importieren.** Der Fehlerkatalog versprach das; bisher gab es sie nur in \`swarmq.exceptions\`. Code, der auch auf 0.12.0–0.12.2 laufen muss, importiert weiter aus \`swarmq.exceptions\`.`

### Testszenarien
- Nachweis ohne xfail: alle Tabellenklassen außer den zwei Ausnahmen in `__all__`, `swarmq.X is swarmq.exceptions.X`.
- Gegenprobe: `WorkflowStateConflict`, `SettleFailedAfterPublish` bleiben außerhalb von `__all__` (nicht testen, ist keine Zusage; nur nicht versehentlich exportieren).

### Dateien
`src/swarmq/__init__.py`, `tests/unit/test_befund_f3_11_exception_exports.py` (xfail entfernen, `befund("F3-11")` bleibt), Changelog (EN+DE).

### Marker nach dem Fix
`@pytest.mark.befund("F3-11")` bleibt, `xfail` entfällt. Keine Register-Regel betroffen, kein `entscheidung`-Marker.

---

## F3-20 (P3): `TaskCancelledError` kommt im Task nie an

### Ursache
- `src/swarmq/worker/execution.py:190-197`: Der Cancel-Monitor ruft `run_task.cancel()`; im Task kommt `asyncio.CancelledError` an. CPU-Modus: das Kind bekommt SIGTERM → `task.cancel()` (`worker/compute/child.py:229`), nach 1 s SIGKILL (`guides/cpu-mode.md:51-55`).
- `TaskCancelledError` werfen nur `client/workflows.py:221` (`get_result`/`app.run`) und `workflow_orchestration.py:180, 195` (`WorkflowHandle`).
- Entschieden (E-068).

### Fundstellen (vollständig, EN+DE)
| Datei:Zeile | Aussage | Im Register |
|---|---|---|
| `website/reference/errors.md:38` | Tabelle: „`app.cancel(task_id)` tripped during execution“ | nein (unscharf) |
| `website/reference/errors.md:277-279` | „Also raised inside a task body …“ | ja |
| `website/reference/errors.de.md:39`, `:295-297` | dto. | ja |
| `website/guides/scheduling-and-retries.md:740-744` | „raises `TaskCancelledError` inside the task coroutine“ | ja |
| `website/guides/scheduling-and-retries.de.md:771-775` | dto. | ja (`:773`) |
| `website/reference/agent/message-formats.md:307-310` | „the worker raises `TaskCancelledError` inside the task coroutine“ | **nein, neu** |
| `website/reference/agent/message-formats.de.md:310-314` | dto. | **nein, neu** |
| `website/reference/agent/error-catalog-machine.md:40` | `when: run, result` | ja |
| `website/reference/agent/error-catalog-machine.de.md:41` | dto. | **nein, neu** |
| `src/swarmq/agent_spec.py:247-254` → `website/reference/agent/spec/errors.json:363-366` | `"when": ["run", "result"]` | ja |

Richtig bleiben: `api.md:215, 251, 739` (Client), `error-catalog-machine.md:84, 116`, `workflows.md:203, 239, 247`.

Das Vokabular `when` meint die Lebenszyklus-Stufe: `run` steht für Ausnahmen im Worker während der Ausführung (`Retry`, `NoRetry`, `SoftTimeoutError`, `ResultSerializationError`). `TaskError` steht trotz `app.run()` nur auf `result`. Richtig für `TaskCancelledError` ist also `["result"]`.

### Optionen
**A: Spec-Wert korrigieren, Doku angleichen (empfohlen).** Roter Test zuerst in `tests/unit/test_agent_spec.py`:
```python
@pytest.mark.entscheidung("E-068")
def test_task_cancelled_error_surfaces_only_at_the_client() -> None:
    """E-068: im Task kommt asyncio.CancelledError an, TaskCancelledError nur beim Client."""
    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}
    assert by_name["TaskCancelledError"]["when"] == ["result"]
```
Dann `agent_spec.py:249` auf `"when": ["result"]`, dann `uv run python -m swarmq.agent_spec` (schreibt alle Teile neu; nur `errors.json` darf sich ändern, `index.json` nur bei geänderter Paketversion).
**B: zusätzlich Docstring an `TaskCancelledError`** (landet als `doc` in `errors.json`). Braucht eine weitere Assertion. Kein Muss.

### Textvorschläge
**`errors.md:38`** (Form beibehalten!): `| \`TaskCancelledError\` | \`TaskError\` | \`get_result()\` or \`app.run()\` on a task cancelled with \`app.cancel(task_id)\`; the task itself sees \`asyncio.CancelledError\` |`
DE `errors.de.md:39`: `| \`TaskCancelledError\` | \`TaskError\` | \`get_result()\` oder \`app.run()\` für einen per \`app.cancel(task_id)\` abgebrochenen Task; der Task selbst sieht \`asyncio.CancelledError\` |`

**`errors.md:277-279`:**
```markdown
**When raised.** On the client: `get_result()` and `app.run()` raise
it when the stored result carries the cancel flag, and a workflow
handle raises it for a cancelled workflow. Never inside the task body:
the worker cancels the running `run()` coroutine, so the task sees
`asyncio.CancelledError`. Clean up in `finally`, or catch
`asyncio.CancelledError`, clean up and re-raise it. In CPU mode the
compute child is killed instead
([behaviour changes](../guides/cpu-mode.md#behaviour-changes-for-task-authors)).
```
DE `errors.de.md:295-297`:
```markdown
**Wann geraist.** Beim Client: `get_result()` und `app.run()` werfen
sie, wenn das gespeicherte Ergebnis das Cancel-Flag trägt, ein
Workflow-Handle bei einem abgebrochenen Workflow. Nie im Task-Body:
Der Worker bricht die laufende `run()`-Coroutine ab, der Task sieht
`asyncio.CancelledError`. Räume in `finally` auf, oder fange
`asyncio.CancelledError`, räume auf und wirf sie erneut. Im CPU-Modus
wird stattdessen das Rechenkind beendet
([Verhaltensänderungen](../guides/cpu-mode.md#verhaltensanderungen-fur-task-autoren)).
```
(DE-Anker vor dem Einfügen im Build prüfen; Umlaute werden im Slug entfernt.)

**`scheduling-and-retries.md:740-744`:**
```markdown
The call is fire-and-forget — it sets a flag in Valkey and returns.
The worker's cancel monitor checks the flag every
`SWARMQ_CANCEL_POLL_INTERVAL` seconds (default 1) and cancels the
running `run()` coroutine: inside the task the cancel arrives as
`asyncio.CancelledError`, not as `TaskCancelledError`. `finally`
blocks run; if you catch `CancelledError` to clean up, re-raise it.
The task ends as cancelled: no retry, no DLQ, no `on_error` or
`post_execute` of the middleware, only the `TASK_CANCELLED` signal.
The client sees `TaskCancelledError` from `get_result()` or
`app.run()`. See [`app.cancel()`](../reference/api.md#cancel) for the
stale-flag caveat.
```
DE `scheduling-and-retries.de.md:771-775`:
```markdown
Der Aufruf ist Fire-and-forget — er setzt ein Flag in Valkey und
kehrt zurück. Der Cancel-Monitor des Workers prüft das Flag alle
`SWARMQ_CANCEL_POLL_INTERVAL` Sekunden (Default 1) und bricht die
laufende `run()`-Coroutine ab: Im Task kommt der Cancel als
`asyncio.CancelledError` an, nicht als `TaskCancelledError`.
`finally`-Blöcke laufen; wer `CancelledError` zum Aufräumen fängt,
wirft sie erneut. Der Task endet als abgebrochen: kein Retry, keine
DLQ, kein `on_error` oder `post_execute` der Middleware, nur das
Signal `TASK_CANCELLED`. Der Client sieht `TaskCancelledError` aus
`get_result()` oder `app.run()`. Siehe
[`app.cancel()`](../reference/api.md#cancel) für den Stale-Flag-Caveat.
```

**`message-formats.md:309-310`:** „When set while the task is running, the worker cancels the running coroutine; the task sees `asyncio.CancelledError`, and the stored result has `is_cancelled=true`, which `get_result()` raises as `TaskCancelledError`.“
DE `message-formats.de.md:312-314`: „Wird es gesetzt, während der Task läuft, bricht der Worker die laufende Coroutine ab; der Task sieht `asyncio.CancelledError`, und das gespeicherte Ergebnis hat `is_cancelled=true`, das `get_result()` als `TaskCancelledError` wirft.“

**`error-catalog-machine.md:40`** und **`.de.md:41`:** Spalte `when` von `run, result` auf `result`.

### Folgefehler
- E-068-Tests (`test_worker.py:2140, 2173`, `client/test_results.py:102`) bleiben grün, Code ändert sich nicht. Der Test-Name `test_cancel_during_run_raises_task_cancelled_error` ist irreführend (er prüft `is_cancelled`); wegen Marker nicht umbenennen.
- Die Spec ist ein veröffentlichtes Artefakt. Ein Agent, der `when` auswertet, sieht `run` nicht mehr. Kein Schemabruch, aber ein Satz im Changelog.
- F3-23 nutzt dieselben Dateien und denselben Test (`test_agent_spec.py`). Gleiche Unit.
- `message-formats.md` ändert auch F3-19 (`:525-526`), `scheduling-and-retries.md` auch F3-03, F3-16, F3-29. Siehe Unit-Schnitt.

### Changelog
EN: `- **F3-20 (docs, agent spec): a cancel arrives in the task as \`asyncio.CancelledError\`.** The error catalog, the cancellation guide and the message formats said the worker raised \`TaskCancelledError\` inside the task; a task catching it never caught anything. Only the client raises \`TaskCancelledError\`. \`spec/errors.json\` now lists \`TaskCancelledError\` with \`when: ["result"]\` instead of \`["run", "result"]\`.`
DE: `- **F3-20 (Doku, Agent-Spec): Ein Cancel kommt im Task als \`asyncio.CancelledError\` an.** Fehlerkatalog, Cancel-Abschnitt und Nachrichtenformate sagten, der Worker werfe \`TaskCancelledError\` im Task; ein Task, der sie fing, fing nie etwas. \`TaskCancelledError\` wirft nur der Client. \`spec/errors.json\` führt \`TaskCancelledError\` jetzt mit \`when: ["result"]\` statt \`["run", "result"]\`.`

### Testszenarien
- Neuer Test oben: rot vor, grün nach der Änderung an `agent_spec.py:249`.
- Drift-Guard `test_committed_spec_files_match_generators`: rot nach der Code-Änderung, grün nach dem Neuerzeugen.
- Guard aus F3-23 prüft danach auch die `when`-Spalte der Markdown-Tabellen.

### Dateien
`src/swarmq/agent_spec.py`, `website/reference/agent/spec/errors.json` (erzeugt), `tests/unit/test_agent_spec.py`, `website/reference/errors.md`, `website/reference/errors.de.md`, `website/reference/agent/error-catalog-machine.md`, `website/reference/agent/error-catalog-machine.de.md`, `website/reference/agent/message-formats.md`, `website/reference/agent/message-formats.de.md`, `website/guides/scheduling-and-retries.md`, `website/guides/scheduling-and-retries.de.md`, `compound-packs/swarmq-entscheidungen/e068-cancel-ist-kein-fehler.md` (`tests:` ergänzen), Changelog (EN+DE).

### Register nach dem Fix
E-068 `tests:` um `tests/unit/test_agent_spec.py::test_task_cancelled_error_surfaces_only_at_the_client` ergänzen.

---

## F3-21 (P3): Migrationsschritt nennt `delay`/`eta` als Inhalt der Delay-Queues

### Ursache
- `website/explanation/delivery-semantics.md:865-870` (DE `:919-925`): „They hold pending retries and deferred tasks (`delay`/`eta`)“.
- `client/schedule.py:218-221`: `delay > 0` oder `eta` gehen in den Scheduler-Store. Richtig steht es in `scheduling-and-retries.md:33-36` und `:434-437`.
- Folge nach E-004: Ein Oneshot, der fällig wird, während die Queue gelöscht ist, wird alle 60 s versucht und nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (Default 1 Tag) verworfen.

### Textvorschlag
**`delivery-semantics.md:863-870`** ersetzen:
```markdown
1. Stop the producers of the queue (including the scheduler crons that
   publish to it). Deferred tasks (`delay=`/`eta=`) are not in the
   queue: they wait in the Valkey scheduler store, and the scheduler
   leader keeps publishing them when they fall due, also during the
   migration. One that falls due while the queue is deleted is retried
   every 60 s and dropped after
   `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (default one day; see
   [missing queues](../guides/scheduling-and-retries.md#missing-queues)).
   Keep the gap between deleting and recreating the queue short, or
   raise that limit on the workers for the migration.
2. Let the workers drain `{queue}`, `{queue}.DQ` and the
   [delay tiers](#delay-tiers) `{queue}.DQ.*`. They hold pending
   retries and deferrals (lock conflicts, rate limits, an unavailable
   backend) that may be due only much later: wait for them, or move
   them to a temporary queue with a shovel. Never delete a delay queue
   that still holds messages; they would be lost.
```
**`delivery-semantics.de.md:917-925`:**
```markdown
1. Die Producer der Queue stoppen (auch die Crons des Schedulers, die
   an sie publizieren). Verzögerte Tasks (`delay=`/`eta=`) liegen nicht
   in der Queue: Sie warten im Scheduler-Store in Valkey, und der
   Scheduler-Leader publiziert sie bei Fälligkeit weiter, auch während
   der Migration. Wird einer fällig, während die Queue gelöscht ist,
   versucht der Leader es alle 60 s erneut und verwirft ihn nach
   `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (Default ein Tag; siehe
   [Fehlende Queues](../guides/scheduling-and-retries.md#fehlende-queues)).
   Die Lücke zwischen Löschen und Neuanlegen kurz halten oder das Limit
   für die Migration auf den Workern erhöhen.
2. Die Worker `{queue}`, `{queue}.DQ` und die
   [Delay-Stufen](#delay-stufen) `{queue}.DQ.*` leeren lassen. Sie
   enthalten ausstehende Retries und Rückstellungen (Lock-Konflikte,
   Rate-Limits, nicht erreichbares Backend), die womöglich erst viel
   später fällig sind: auf sie warten oder sie per Shovel in eine
   Zwischen-Queue verschieben. Eine Delay-Queue mit Nachrichten nie
   einfach löschen; die Nachrichten gingen verloren.
```
DE-Anker `#fehlende-queues` vor dem Einfügen prüfen (Überschrift in `scheduling-and-retries.de.md` suchen).

Optional, bewusst weggelassen: „alle Worker mit `SWARMQ_SCHEDULER=off`“. Das hält auch alle übrigen Crons an, und Worker vor Einführung des Schalters ignorieren ihn (`scheduling-and-retries.md:447-448`).

### Folgefehler
Keine. E-004-Tests unberührt. `scheduling-and-retries.md` wird nur verlinkt, nicht geändert.

### Dateien
`website/explanation/delivery-semantics.md`, `website/explanation/delivery-semantics.de.md`.

---

## F3-22 (P3): `SCHEDULER_LEADER_OUTDATED` als „ohne Zeitzonen“ beschrieben

### Ursache
`scheduler/leader_version.py:22-23` (`MINIMUM_LEADER_VERSION = (0, 12)`), E-029. Code-Docstring `signals.py:128-132` ist richtig, ebenso `configuration.md:155`, `logging.md:750`, `scheduling-and-retries.md:363-372`.

### Fundstellen
`website/guides/signals.md:83`, `website/guides/signals.de.md:87`, `website/reference/api.md:667`, `website/reference/api.de.md:687`. Weitere Treffer gibt es nicht (grep „time zone support“/„Zeitzonen-Unterstützung“).

### Textvorschläge
- `signals.md:83`, Spalte „Fires“: `The scheduler leader runs a swarmq below the minimum leader version (currently 0.12; \`outdated=True\`, once per leader), or a current leader follows (\`outdated=False\`); see [leader version](scheduling-and-retries.md#leader-version)`
- `signals.de.md:87`: `Der Scheduler-Leader führt ein swarmq unter der Mindestversion für Leader aus (derzeit 0.12; \`outdated=True\`, einmal pro Leader), oder ein aktueller Leader folgt (\`outdated=False\`); siehe [Leader-Version](scheduling-and-retries.md#leader-version)`
- `api.md:667`: `A non-leader saw a scheduler leader running a swarmq below the minimum leader version (currently 0.12; \`leader_version\` \`None\` means before 0.10.0; an unreadable version does not count); once per leader with \`outdated=True\`, again with \`outdated=False\` when a current leader follows.`
- `api.de.md:687`: `Ein Nicht-Leader hat einen Scheduler-Leader gesehen, der ein swarmq unter der Mindestversion für Leader ausführt (derzeit 0.12; \`leader_version\` \`None\` heißt vor 0.10.0; eine unlesbare Version zählt nicht); einmal pro Leader mit \`outdated=True\`, erneut mit \`outdated=False\`, wenn ein aktueller Leader folgt.`

### Folgefehler
E-029-Tests unberührt. `signals.md` gehört zur F3-06-Unit, `api.md` zur Referenz-Sammelunit.

### Dateien
`website/guides/signals.md`, `website/guides/signals.de.md`, `website/reference/api.md`, `website/reference/api.de.md`.

---

## F3-23 (P3): Maschinenkatalog unvollständig, Guard fehlt

### Ursache
- `website/reference/agent/error-catalog-machine.md:4` verspricht jede Klasse einmal. Abgleich gegen `spec/errors.json`: EN und DE haben je 25 Zeilen, es fehlen `QueueArgumentsMismatch`, `BrokerPublishBlocked`, `PublishRejected`. Keine Duplikate, keine Fremdklassen.
- Die übrigen 25 Zeilen stimmen in `parent`, `origin`, `when`, erstem Wort von `retriable` und `dlq_on_uncaught` und im Exitcode mit der Spec überein (Skript-Abgleich). Nur `transport_field` weicht absichtlich ab (Markdown ausführlicher, z. B. `LoggingConfigError`).
- Der bestehende Sync-Test `tests/unit/test_agent_spec.py:57-60` (`test_error_catalog_covers_every_swarmq_error_no_more_no_less`) prüft nur `build_error_catalog()` gegen die Klassenhierarchie, nicht die Markdown-Tabelle. Der Drift-Guard `:466-477` prüft die JSON-Dateien.

### Guard-Test (Vorschlag, `tests/unit/test_agent_spec.py`, neuer Abschnitt „U7: Markdown-Katalog“)
```python
_AGENT_DOCS = DEFAULT_SPEC_DIR.parent
_CATALOG_ROW = re.compile(
    r"^\| `swarmq\.exceptions\.(\w+)` \| `(\w+)` \| (\w+) \| ([^|]+) \| ([^|]+) \| ([^|]+) \|",
    re.MULTILINE,
)


@pytest.mark.parametrize("page", ["error-catalog-machine.md", "error-catalog-machine.de.md"])
def test_machine_error_catalog_table_matches_the_generated_catalog(page: str) -> None:
    rows = _CATALOG_ROW.findall((_AGENT_DOCS / page).read_text(encoding="utf-8"))
    names = [row[0] for row in rows]
    assert sorted(names) == sorted(_all_swarmq_errors())  # jede Klasse genau einmal
    by_name = {e["name"]: e for e in build_error_catalog()["errors"]}
    for name, parent, origin, when, retriable, dlq in rows:
        entry = by_name[name]
        assert parent == entry["parent"], name
        assert origin == entry["origin"], name
        assert [w.strip() for w in when.split(",")] == entry["when"], name
        assert retriable.split()[0] == entry["retriable"], name
        assert dlq.split()[0] == entry["dlq_on_uncaught"], name
```
- Zuerst rot (drei fehlende Klassen), dann Zeilen ergänzen. `transport_field` und Exitcode bewusst nicht vergleichen (Prosa).
- Prüft nach F3-20 auch `when: result` bei `TaskCancelledError` in beiden Sprachen.
- `test_agent_spec.py` hat 490 Zeilen; der Größen-Guard gilt nicht für `tests/`.

### Zeilen (Werte aus `spec/errors.json`)
`error-catalog-machine.md` nach `:28`:
`| \`swarmq.exceptions.QueueArgumentsMismatch\` | \`ConfigurationError\` | library | startup | no | n/a | \`78\` | \`worker_config_error\` log line |`
nach `:33`:
`| \`swarmq.exceptions.BrokerPublishBlocked\` | \`BrokerConnectionError\` | library | schedule, run | yes | n/a | \`1\` | \`stderr line\` |`
`| \`swarmq.exceptions.PublishRejected\` | \`BrokerConnectionError\` | library | schedule, run | yes | n/a | \`1\` | \`stderr line\` |`
DE `error-catalog-machine.de.md` nach `:29` bzw. `:34` gleich, Transportspalte bei `QueueArgumentsMismatch`: `Logsatz \`worker_config_error\``.

Entscheidungsmatrix (`error-catalog-machine.md:104-117`, DE `:106-119`), optional, aber passend zu „appears once“ nicht nötig. Empfohlen, weil die Nachbarn dort stehen:
- EN: `| \`BrokerPublishBlocked\` | The broker blocked publishing (memory or disk alarm); nothing was sent. Retry later; free memory or disk on the broker. |`
- EN: `| \`PublishRejected\` | The queue refused the message (full with \`reject-publish\`, or a quorum queue without quorum); nothing was stored. Fix the queue, then retry. |`
- EN: `| \`QueueArgumentsMismatch\` | Do not restart unchanged (exit \`78\`). Restore the previous setting or migrate the queue. |`
- DE: `| \`BrokerPublishBlocked\` | Der Broker hat das Publizieren blockiert (Speicher- oder Plattenalarm); nichts wurde gesendet. Später erneut versuchen; Speicher oder Platte am Broker freigeben. |`
- DE: `| \`PublishRejected\` | Die Queue hat die Nachricht abgelehnt (voll mit \`reject-publish\` oder Quorum-Queue ohne Quorum); nichts wurde gespeichert. Queue reparieren, dann erneut versuchen. |`
- DE: `| \`QueueArgumentsMismatch\` | Nicht unverändert neu starten (Exit \`78\`). Vorherige Einstellung wiederherstellen oder die Queue migrieren. |`

### Folgefehler
- F3-05 (Code-Paket) kann die Exit-78-Zeile `error-catalog-machine.md:66` (DE `:67`) berühren („`ImportError` of the task module“). Andere Zeilen derselben Datei; besser in die gleiche Unit legen oder nacheinander.
- Kein Register-Eintrag (keine Entscheidung).

### Dateien
`tests/unit/test_agent_spec.py`, `website/reference/agent/error-catalog-machine.md`, `website/reference/agent/error-catalog-machine.de.md`, Changelog (EN+DE).

### Changelog
EN: `- **F3-23 (docs): the machine-readable error catalog lists every class.** \`QueueArgumentsMismatch\`, \`BrokerPublishBlocked\` and \`PublishRejected\` were missing from the table; a test now compares the table with the generated catalog.`
DE: `- **F3-23 (Doku): Der maschinenlesbare Fehlerkatalog führt jede Klasse.** \`QueueArgumentsMismatch\`, \`BrokerPublishBlocked\` und \`PublishRejected\` fehlten in der Tabelle; ein Test gleicht sie jetzt mit dem erzeugten Katalog ab.`

---

## F3-25 (P3): DE-Changelog 0.1.0 ohne `worker --check`

### Fundstelle
EN `website/reference/changelog.md:2849-2854`. DE `website/reference/changelog.de.md` nach `:3027` (Ende des R5.3-Eintrags, vor `### Hinzugefügt — Phase 5 Welle 1`) einfügen:
```markdown
- **`swarmq worker --check`.** Dry-Run-Validierung: prüft, ob
  `--module` importierbar ist, und mit `--reload`, dass `watchfiles`
  installiert ist und die Watch-Dirs auflösbar sind. Druckt eine
  JSON-Zusammenfassung und endet mit `0`/`1`, ohne den Supervisor zu
  starten oder Broker/Backend anzufassen. Für Smoke-Tests von
  systemd-Units und Dev-Containern vor einem vollen Worker-Start.
```
Historischer Eintrag, gibt den Stand von 0.1.0 wieder (keine Angleichung an heutiges `--check`). Kein eigener 0.12.3-Changelog-Eintrag nötig; höchstens Sammelsatz „DE-Changelog ergänzt“.

### Dateien
`website/reference/changelog.de.md` (Hunk weit unten, konfliktfrei zu 0.12.3-Einträgen oben, aber dieselbe Datei).

---

## F3-28 (ENTSCHEIDUNG, Doku P3): `post_execution` nur bei Erfolg

### Ursache
- `src/swarmq/worker/execution.py:203-210`: `post_execution` nur im `else`-Zweig. `:292-297`: nach fehlgeschlagenem `pre_execution` kein `run`, kein `post_execution`. CPU-Kind `worker/compute/child.py:196-197` gleich.
- E-067 gilt. Marker: **schon gesetzt.** `tests/unit/worker/test_worker_dishka.py:169` (AE4, `test_finalizer_receives_exception`) trägt `entscheidung("E-067")`, ebenso `test_worker.py:212`, `:1368` und `test_worker_timeouts.py:250`; alle stehen in E-067 `tests:`.
- Lücke im Festschreiben: Kein Test prüft direkt „`run()` wirft → `post_execution` läuft nicht“. Der AE4-Test hat keine `post_execution` in `DiFailTask` (`test_worker_dishka.py:67-72`), er prüft nur den Finalizer. `test_pre_execution_raises_skips_post` deckt nur den `pre_execution`-Fall.

### Fundstellen
`website/guides/defining-tasks.md:174-195` (Beispiel `:179-191`, Satz `:193-195`), `website/guides/defining-tasks.de.md:179-200` (Satz `:198-200`), `website/reference/api.md:516-517`, `website/reference/api.de.md:533-534`. Richtig: `errors.md:420`, `scheduling-and-retries.md:690` (Soft-Timeout).

### Optionen
**A: Beispiel auf `try/finally` in `run()` umbauen, Satz ersetzen, auf Dishka-Finalizer verweisen (empfohlen).**
**B: Beispiel behalten, nur Satz ändern.** Dann zeigt das Beispiel ein Leck. Abgelehnt.

### Textvorschläge
**`defining-tasks.md:176-195`:**
```markdown
Override `pre_execution` and `post_execution` to run code on the
worker around the task body. `post_execution` runs only after
`pre_execution` and `run()` succeeded. It does not run when either
raises, on a soft or hard timeout, or on a cancel; an exception in it
turns the attempt into a failure. Use it for follow-up work on
success. Cleanup that must run on every outcome belongs in
`try/finally` inside `run()`:

```python
class WithSession(Task):
    task_info = TaskInfo(name="with_session", queues=["default"])

    async def run(self, user_id: int) -> dict:
        session = await open_db_session()
        try:
            return await self.fetch(session, user_id)
        finally:
            await session.close()
```

With [dependency injection](../reference/api.md#dependency-injection-dishka),
put it in the provider's `yield` finalizer instead; it also receives
the exception, for commit or rollback. For cross-cutting concerns that
should apply to **every** task, use [middleware](middleware.md)
instead; its `on_error` runs on every failure.
```
(Im Beispiel `return await session.fetch(user_id)` statt `self.fetch(...)`, wie bisher.)

**`defining-tasks.de.md:181-200`:**
```markdown
Überschreibe `pre_execution` und `post_execution`, um auf dem Worker
vor und nach dem Task-Body Code auszuführen. `post_execution` läuft
nur, wenn `pre_execution` und `run()` erfolgreich waren. Es läuft
nicht, wenn eines davon wirft, nicht bei Soft- oder Hard-Timeout und
nicht bei einem Cancel; eine Ausnahme darin macht den Versuch zum
Fehlschlag. Nutze es für Folgearbeit nach Erfolg. Aufräumen, das bei
jedem Ausgang laufen muss, gehört in `try/finally` in `run()`:

(gleiches Beispiel)

Mit [Dependency Injection](../reference/api.md#dependency-injection-dishka)
gehört es stattdessen in den `yield`-Finalizer des Providers; er
bekommt auch die Ausnahme, für Commit oder Rollback. Für
Querschnittsthemen, die auf **jeden** Task wirken sollen, nutze
stattdessen [Middleware](middleware.md); ihr `on_error` läuft bei
jedem Fehlschlag.
```

**`api.md:516-517`:** `- \`post_execution()\` — called after \`run()\`, only when \`pre_execution()\` and \`run()\` succeeded (not on an exception, a timeout or a cancel). An exception from it fails the attempt. Cleanup for every outcome belongs in \`try/finally\` in \`run()\` or a Dishka \`yield\` finalizer. Default: no-op.`
**`api.de.md:533-534`:** `- \`post_execution()\` — nach \`run()\` aufgerufen, nur wenn \`pre_execution()\` und \`run()\` erfolgreich waren (nicht bei Ausnahme, Timeout oder Cancel). Eine Ausnahme darin lässt den Versuch scheitern. Aufräumen für jeden Ausgang gehört in \`try/finally\` in \`run()\` oder in einen Dishka-\`yield\`-Finalizer. Default: No-Op.`

### Festschreibender Test (empfohlen)
Neue Datei `tests/unit/worker/test_worker_hooks_failure_cancel.py` (nicht `test_worker.py`, das mit 3045 Zeilen auch von Code-Units angefasst wird):
```python
@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-067")
async def test_run_raises_skips_post_execution(broker, backend, config): ...
    # Task: run() wirft ValueError, post_execution setzt Flag → Flag bleibt False,
    # gespeichertes Ergebnis is_error=True
```
Der Test ist von Anfang an grün (Charakterisierung einer Entscheidung, AGENTS.md „samt festschreibendem Test“). E-067 `tests:` ergänzen.

### Folgefehler
- E-045 (`test_soft_timeout_skips_post_execution`) und E-067 bleiben grün. Kein Code.
- `api.md` teilen sich mehrere Pakete (F3-07 `:122`, F3-09 `:263`, F3-02 `:597-598`, `:646`, F3-15 `:756`, dazu F3-22, F3-30). Siehe Unit-Schnitt.

### Dateien
`website/guides/defining-tasks.md`, `website/guides/defining-tasks.de.md`, `website/reference/api.md`, `website/reference/api.de.md`, `tests/unit/worker/test_worker_hooks_failure_cancel.py` (neu), `compound-packs/swarmq-entscheidungen/e067-post-execution-nur-bei-erfolg.md`.

### Changelog
EN: `- **F3-28 (docs): \`post_execution\` runs only on success.** The task guide and the API reference said it ran on success and failure, and showed it closing a database session. That session leaked on every failure. The guide now cleans up in \`try/finally\` or in a Dishka \`yield\` finalizer.`
DE: `- **F3-28 (Doku): \`post_execution\` läuft nur bei Erfolg.** Task-Guide und API-Referenz sagten „bei Erfolg und Fehler“ und schlossen dort eine DB-Session, die bei jedem Fehlschlag offen blieb. Der Guide räumt jetzt in \`try/finally\` oder im Dishka-\`yield\`-Finalizer auf.`

---

## F3-30 (ENTSCHEIDUNG, Doku P3): Cancel ohne `on_error`/`post_execute`

### Ursache
`src/swarmq/worker/execution.py:236-244`: Bei `is_cancelled` nur `task_cancelled` und `TASK_CANCELLED`. Der CPU-Pfad nutzt dieselbe Methode (`worker/compute/executor.py:185`). `pre_execute` lief vorher (`execution.py:272`). Ein Cancel vor dem Start (Cancel-Gate) erreicht `pre_execute` nicht und hinterlässt nichts. E-068 gilt.

### Fundstellen
- `website/guides/middleware.md:65-102` (`TimingMiddleware`, Zustand je `task_id`, Abräumen nur in `post_execute`/`on_error`), `:153-154` („On error … `on_error` runs“), `:160-162` („Clean up on `post_execute` and `on_error`“).
- DE `middleware.de.md:67-104`, `:156-158`, `:164-166`.
- `website/reference/api.md:596-612` (Error semantics, Registration), `:651` (`TASK_CANCELLED`). DE `api.de.md:614-630`, `:671`.

### Optionen
**A: Im Guide `forget()` am Beispiel plus `TASK_CANCELLED`-Handler im `--configure`-Hook; Satz in der API-Referenz (empfohlen, E-068 konform).**
**B: Hook `on_cancel` einführen.** Neues Verhalten, kein Patch-Thema; E-068 nennt es nur als Überdenken.

### Textvorschläge
**`middleware.md`, im Beispiel nach `:72` (Konstruktor) ergänzen:**
```python
    def forget(self, task_id: str) -> None:
        # A cancelled task gets neither post_execute nor on_error.
        self._starts.pop(task_id, None)
```
DE `middleware.de.md` nach `:73`: gleicher Code, Kommentar `# Ein abgebrochener Task bekommt weder post_execute noch on_error.`

**`middleware.md:153-154`** ergänzen: `A cancel (\`app.cancel()\`) is not an error: after \`pre_execute\`, neither \`on_error\` nor \`post_execute\` runs; only the \`TASK_CANCELLED\` signal fires.`
DE `:156-158`: `Ein Cancel (\`app.cancel()\`) ist kein Fehler: Nach \`pre_execute\` läuft weder \`on_error\` noch \`post_execute\`; nur das Signal \`TASK_CANCELLED\` feuert.`

**`middleware.md:160-162`:**
```markdown
1. **Keyed dict on the middleware instance**, as in the timing
   example above. Clean up on `post_execute` and `on_error`, and on
   the `TASK_CANCELLED` signal for cancelled tasks (see the hook under
   [registering middleware](#registering-middleware)); otherwise every
   cancelled task leaves an entry behind. Works because middleware is
   singleton per `SwarmQ` app.
```
DE `:164-166`:
```markdown
1. **Keyed Dict auf der Middleware-Instanz**, wie im Timing-Beispiel
   oben. Cleanup bei `post_execute` und `on_error`, für abgebrochene
   Tasks zusätzlich beim Signal `TASK_CANCELLED` (siehe den Hook unter
   [Middleware registrieren](#middleware-registrieren)); sonst bleibt
   je abgebrochenem Task ein Eintrag liegen. Funktioniert, weil
   Middleware singleton pro `SwarmQ`-App ist.
```
Der Hook-Code mit `forget_cancelled` steht im F3-06-Vorschlag.

**`api.md`, nach `:603` (Ende „Error semantics“) neuer Punkt:**
`- A cancelled task gets no \`on_error\` and no \`post_execute\`, only \`pre_execute\` before it: a cancel is not an error. Middleware that keeps state per task also handles the \`TASK_CANCELLED\` signal (\`task_id\`, \`task_name\`, \`queue\`).`
DE `api.de.md` nach `:621`: `- Ein abgebrochener Task bekommt kein \`on_error\` und kein \`post_execute\`, nur vorher \`pre_execute\`: Ein Cancel ist kein Fehler. Middleware mit Zustand je Task behandelt zusätzlich das Signal \`TASK_CANCELLED\` (\`task_id\`, \`task_name\`, \`queue\`).`
`api.md:651` (`TASK_CANCELLED`, Spalte Fires): `Cancelled before or during execution. No \`on_error\`/\`post_execute\` follows.` DE `:671`: `Vor oder während der Ausführung abgebrochen. Es folgt kein \`on_error\`/\`post_execute\`.`

### Festschreibender Test (empfohlen, schließt `ohne_test` von E-068)
In derselben neuen Datei wie F3-28:
```python
@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-068")
async def test_cancel_during_run_skips_on_error_and_post_execute(...):
    # Worker mit middleware_chain=_make_middleware_chain_mock() (Muster test_worker.py:1300ff.),
    # cancel_poll_interval=0.05, backend.is_cancelled ab dem 2. Poll True (Muster :2150-2155)
    # Erwartung: run_pre_execute awaited, run_on_error und run_post_execute nicht awaited,
    # TASK_CANCELLED dispatcht.
```
Grün von Anfang an. Laufzeit wie die Nachbarn (~100 ms, Poll 50 ms); ggf. Begründung im Test (AGENTS.md: Überschreitung melden, nicht abschwächen).

### Folgefehler
- F3-02 ändert `api.md:597-598` (gleicher Block „Error semantics“). Überlappender Hunk → zwingend gleiche Unit oder strikte Reihenfolge.
- F3-06 ändert dieselben Middleware-Stellen. Gleiche Unit.

### Dateien
`website/guides/middleware.md`, `website/guides/middleware.de.md`, `website/reference/api.md`, `website/reference/api.de.md`, `tests/unit/worker/test_worker_hooks_failure_cancel.py` (neu), `compound-packs/swarmq-entscheidungen/e068-cancel-ist-kein-fehler.md` (`tests:` ergänzen, `ohne_test` streichen).

### Changelog
EN: `- **F3-30 (docs): middleware cleanup on cancel.** A cancel runs neither \`on_error\` nor \`post_execute\`; the middleware guide's per-task state leaked one entry per cancelled task. The guide now also cleans up on \`TASK_CANCELLED\`.`
DE: `- **F3-30 (Doku): Aufräumen der Middleware beim Cancel.** Ein Cancel ruft weder \`on_error\` noch \`post_execute\`; der Zustand je Task aus dem Middleware-Guide behielt je abgebrochenem Task einen Eintrag. Der Guide räumt jetzt auch bei \`TASK_CANCELLED\` auf.`

---

## F3-33 (LÜCKE, nur Doku-Satz): `task_lock_lost` nach wiederholter Freigabe

### Ursache
`src/swarmq/lua/lock_release.lua:45-61` liefert beim zweiten Lauf 0; valkey-py 6.1.1 wiederholt Befehle nach Verbindungsfehlern (`backend/valkey/connection.py:247`: `Retry(ExponentialBackoff(cap=2.0, base=0.05), retries=5)`). `worker/gates/lock.py:621-641` loggt dann `task_lock_lost`, `stage="release"`. Betroffen ist nur `stage="release"`.

### Textvorschlag
**`website/reference/logging.md:645`**, in der Beschreibung `at release (\`stage="release"\`)` ersetzen durch:
`at release (\`stage="release"\`; also when the Valkey client repeated the release after a lost reply: the first attempt released the lock, the repeat finds it gone)`
**`website/reference/logging.de.md:663`**, `bei der Freigabe (\`stage="release"\`)` ersetzen durch:
`bei der Freigabe (\`stage="release"\`; auch wenn der Valkey-Client die Freigabe nach einer verlorenen Antwort wiederholt hat: Der erste Versuch hat den Lock freigegeben, die Wiederholung findet ihn nicht mehr)`

### Folgefehler
Keine. Kein Test, solange F3-33 offen ist. Der `hint` im Code bleibt.

### Dateien
`website/reference/logging.md`, `website/reference/logging.de.md`.

---

## F3-37 (LÜCKE, nur Doku): Lock-Requeue verbraucht einen Rate-Slot

### Ursache
`src/swarmq/worker/pipeline.py:642` (Gates: Ausführungsschutz, Expiry, Rate-Limit, Lock), `lua/sliding_window_counter.lua:55` (INCR beim Durchlass), `worker/gates/lock.py:95-110` (Requeue 100 ms, verdoppelt, Deckel 60 s). E-070 gilt.

### Textvorschläge
**`website/guides/rate-limiting.md:50`:** `- **Under the limit** — the counter is incremented and the message moves on to the lock check (if the task has one) and \`run()\`. The slot counts from this moment, also when \`run()\` does not follow (see [with a lock](#combining-with-retries-and-locks)).`
DE `rate-limiting.de.md:51`: `- **Unter dem Limit** — der Counter wird erhöht, und die Nachricht geht weiter zur Lock-Prüfung (falls der Task einen Lock hat) und zu \`run()\`. Der Slot zählt ab hier, auch wenn \`run()\` nicht folgt (siehe [Mit einem Lock](#kombination-mit-retries-und-locks)).`

**`rate-limiting.md:152-153`:**
```markdown
- **With a lock.** The rate-limit check runs first; the lock (`lock`,
  `max_concurrency`) is tried only after it passes. A rate-limited
  message never attempts the lock. The other way round, a message that
  passes the rate limit and then finds the lock taken has used a slot,
  although `run()` did not run: it goes back with the lock backoff
  (100 ms, doubling up to 60 s) and passes the rate limit again on every
  redelivery. With many copies waiting for one lock, a large part of
  the budget goes to these attempts, and fewer tasks run than the limit
  allows. Leave headroom in `rate_limit`, or keep the number of copies
  that compete for one lock small (for example with `unique_for`).
```
DE `rate-limiting.de.md:161-162`:
```markdown
- **Mit einem Lock.** Zuerst läuft die Rate-Limit-Prüfung; den Lock
  (`lock`, `max_concurrency`) versucht der Worker erst danach. Eine
  rate-limited Nachricht versucht den Lock nie. Umgekehrt hat eine
  Nachricht, die das Rate-Limit passiert und dann den Lock besetzt
  findet, einen Slot verbraucht, obwohl `run()` nicht lief: Sie geht
  mit dem Lock-Backoff zurück (100 ms, verdoppelt bis 60 s) und
  passiert das Rate-Limit bei jeder Neuzustellung erneut. Warten viele
  Kopien auf einen Lock, geht ein großer Teil des Budgets an diese
  Versuche, und es laufen weniger Tasks, als das Limit erlaubt. Lass
  Luft im `rate_limit` oder halte die Zahl der Kopien klein, die um
  einen Lock konkurrieren (etwa mit `unique_for`).
```
DE-Anker `#kombination-mit-retries-und-locks` passt zur Überschrift `rate-limiting.de.md:155`.

### Folgefehler
E-070 (`test_pipeline.py:168`) unberührt. Gleiche Datei wie F3-06 (`:96-118`), gleiche Unit.

### Dateien
`website/guides/rate-limiting.md`, `website/guides/rate-limiting.de.md`.

---

## Changelog (Sammlung)

`changelog.md`/`.de.md` haben noch keinen Abschnitt 0.12.3. Alle Pakete schreiben dort. Empfehlung: Eine Unit legt den Abschnitt an (`## [0.12.3] — unreleased`, `### Fixed`/`### Behoben`), die anderen liefern ihre Punkte als Text, oder die Release-Unit sammelt am Ende. Texte dieses Pakets stehen je Befund oben. Für F3-06, F3-10, F3-21, F3-22, F3-33, F3-37 reicht ein Sammelpunkt:
EN: `- **Docs:** handlers and middleware under \`swarmq worker\` go through \`--configure\` (F3-06); \`queue\`, \`timeout\` and \`polling\` as \`run()\` parameters (F3-10); the queue migration step names where deferred tasks wait (F3-21); \`SCHEDULER_LEADER_OUTDATED\` means a leader below 0.12 (F3-22); \`task_lock_lost\` after a repeated release (F3-33); a lock conflict uses a rate-limit slot (F3-37).`
DE: `- **Doku:** Handler und Middleware unter \`swarmq worker\` gehen über \`--configure\` (F3-06); \`queue\`, \`timeout\` und \`polling\` als \`run()\`-Parameter (F3-10); der Migrationsschritt nennt, wo verzögerte Tasks warten (F3-21); \`SCHEDULER_LEADER_OUTDATED\` heißt Leader unter 0.12 (F3-22); \`task_lock_lost\` nach wiederholter Freigabe (F3-33); ein Lock-Konflikt verbraucht einen Rate-Limit-Slot (F3-37).`

---

## Unit-Schnitt (parallel, ohne Dateikonflikte)

Andere Pakete ändern ebenfalls Doku. Gemeinsam genutzte Dateien sind: `api.md`/`.de.md` (F3-02, F3-07, F3-09, F3-15 und hier F3-22, F3-28, F3-30), `scheduling-and-retries.md`/`.de.md` (F3-03, F3-16, F3-29 und hier F3-20), `agent/message-formats.md`/`.de.md` (F3-19 und hier F3-20), `middleware.md`/`signals.md` (F3-02 und hier F3-06, F3-30), `agent/error-catalog-machine.md` (evtl. F3-05 und hier F3-20, F3-23), `changelog.md`/`.de.md` (alle). `cli.md`, `configuration.md` fasst dieses Paket nicht an.

| Unit | Befunde | Dateien (exklusiv) | Abhängigkeit |
|---|---|---|---|
| **D1 Guides: CLI-App, Cancel, Rate-Limit** | F3-06, F3-30 (Guide-Teil), F3-37, F3-22 (signals-Teil) | `website/guides/signals.md`, `website/guides/signals.de.md`, `website/guides/middleware.md`, `website/guides/middleware.de.md`, `website/guides/rate-limiting.md`, `website/guides/rate-limiting.de.md`, `website/guides/locking-and-dedup.md`, `website/guides/locking-and-dedup.de.md` | F3-02 ändert `middleware.md:12, 27` und `signals.md:62` (+DE). Entweder liefert F3-02 diese Sätze an D1, oder D1 läuft nach dem Merge von F3-02. Getrennte Hunks, aber gleiche Dateien. |
| **D2 Fehlerkatalog, Spec, Exporte** | F3-11, F3-20 (Spec/Katalog/errors), F3-23 | `src/swarmq/__init__.py`, `src/swarmq/agent_spec.py`, `website/reference/agent/spec/errors.json`, `tests/unit/test_agent_spec.py`, `tests/unit/test_befund_f3_11_exception_exports.py`, `website/reference/errors.md`, `website/reference/errors.de.md`, `website/reference/agent/error-catalog-machine.md`, `website/reference/agent/error-catalog-machine.de.md` | F3-05 soll `error-catalog-machine*.md:66/67` nicht selbst ändern, sondern den Satz an D2 geben (oder nacheinander). |
| **D3 Lebenszyklus, Migration, Logging** | F3-10, F3-28 (Guide-Teil), F3-21, F3-33, F3-25, Festschreibtests E-067/E-068 | `website/guides/defining-tasks.md`, `website/guides/defining-tasks.de.md`, `website/explanation/delivery-semantics.md`, `website/explanation/delivery-semantics.de.md`, `website/reference/logging.md`, `website/reference/logging.de.md`, `tests/unit/worker/test_worker_hooks_failure_cancel.py` (neu), `compound-packs/swarmq-entscheidungen/e067-post-execution-nur-bei-erfolg.md`, `compound-packs/swarmq-entscheidungen/e068-cancel-ist-kein-fehler.md` | Den E-068-Eintrag für den Spec-Test aus D2 übernimmt D3 (eine Unit je Regel-Datei). D2 setzt nur den Marker; D3 trägt beide Tests in E-068 ein. Läuft D3 vor D2, ist der Guard bis zum D2-Merge rot → D3 nach D2 mergen oder den Eintrag in D2 legen und D3 nur E-067 ändern lassen. |
| **D4 Referenz-Sammelunit (zuletzt, seriell)** | F3-20 (Guide/Formate), F3-22 (api), F3-28 (api), F3-30 (api), Changelog aller Pakete inkl. F3-25-Sammelsatz | `website/reference/api.md`, `website/reference/api.de.md`, `website/guides/scheduling-and-retries.md`, `website/guides/scheduling-and-retries.de.md`, `website/reference/agent/message-formats.md`, `website/reference/agent/message-formats.de.md`, `website/reference/changelog.md`, `website/reference/changelog.de.md` | Nach den Code-Units, die dieselben Dateien berühren (F3-02, F3-03, F3-07, F3-09, F3-15, F3-16, F3-19, F3-29). Alternativ: jede Code-Unit ändert ihre Hunks selbst, D4 läuft danach und rebased. Wegen des überlappenden Blocks `api.md:596-612` (F3-02 und F3-30) nie parallel zu F3-02. |

F3-25 liegt in `changelog.de.md` weit unten. Es kann in D4 oder als eigener Mini-Commit nach D4 laufen, nicht parallel zu einer anderen Changelog-Änderung.

D1, D2 und D3 sind untereinander dateifrei und parallel umsetzbar (Ausnahme E-068, siehe D3). D4 läuft zuletzt.

---

## Dateien je Befund

| Befund | Dateien |
|---|---|
| F3-06 | `website/guides/signals.md`, `.de.md`; `website/guides/middleware.md`, `.de.md`; `website/guides/rate-limiting.md`, `.de.md`; `website/guides/locking-and-dedup.md`, `.de.md` |
| F3-10 | `website/guides/defining-tasks.md`, `.de.md` |
| F3-11 | `src/swarmq/__init__.py`; `tests/unit/test_befund_f3_11_exception_exports.py`; Changelog EN+DE |
| F3-20 | `src/swarmq/agent_spec.py`; `website/reference/agent/spec/errors.json`; `tests/unit/test_agent_spec.py`; `website/reference/errors.md`, `.de.md`; `website/reference/agent/error-catalog-machine.md`, `.de.md`; `website/reference/agent/message-formats.md`, `.de.md`; `website/guides/scheduling-and-retries.md`, `.de.md`; `compound-packs/swarmq-entscheidungen/e068-cancel-ist-kein-fehler.md`; Changelog EN+DE |
| F3-21 | `website/explanation/delivery-semantics.md`, `.de.md` |
| F3-22 | `website/guides/signals.md`, `.de.md`; `website/reference/api.md`, `.de.md` |
| F3-23 | `tests/unit/test_agent_spec.py`; `website/reference/agent/error-catalog-machine.md`, `.de.md`; Changelog EN+DE |
| F3-25 | `website/reference/changelog.de.md` |
| F3-28 | `website/guides/defining-tasks.md`, `.de.md`; `website/reference/api.md`, `.de.md`; `tests/unit/worker/test_worker_hooks_failure_cancel.py` (neu); `compound-packs/swarmq-entscheidungen/e067-post-execution-nur-bei-erfolg.md`; Changelog EN+DE |
| F3-30 | `website/guides/middleware.md`, `.de.md`; `website/reference/api.md`, `.de.md`; `tests/unit/worker/test_worker_hooks_failure_cancel.py` (neu); `compound-packs/swarmq-entscheidungen/e068-cancel-ist-kein-fehler.md`; Changelog EN+DE |
| F3-33 | `website/reference/logging.md`, `.de.md` |
| F3-37 | `website/guides/rate-limiting.md`, `.de.md` |

## Wechselwirkungen mit anderen Befunden

- **F3-02 (Enqueue-Middleware bei `delay`/`eta`):** gleiche Dateien wie F3-06/F3-30 (`middleware.md:12, 27`, `signals.md:62`, `api.md:597-598, 646`, jeweils DE). `api.md:596-612` überlappt direkt mit F3-30. Nur nacheinander oder in einer Hand.
- **F3-36 (Cron-Fires ohne Enqueue-Middleware, offen):** Wenn F3-02 den Middleware-Guide anpasst, sollte der Hinweis „Cron-Fires laufen nicht durch `pre_enqueue`“ in denselben Absatz (`middleware.md:12, 27`), nicht in die F3-06-Blöcke.
- **F3-05 (Exit 78 bei ImportError/SyntaxError):** berührt eventuell `error-catalog-machine.md:66` und `.de.md:67`. Gleiche Dateien wie F3-23.
- **F3-19 (`message-formats.md:525-526`):** gleiche Datei wie F3-20 (`:307-310`). Getrennte Hunks.
- **F3-03, F3-16, F3-29 (`scheduling-and-retries.md`):** gleiche Datei wie F3-20 (`:740-744`). Getrennte Hunks.
- **F3-07, F3-09, F3-15 (`api.md:122, 263, 756`):** gleiche Datei wie F3-22, F3-28, F3-30. Getrennte Hunks.
- **F3-11 ↔ F3-20:** Der F3-11-Nachweis parst die Tabelle in `errors.md`. F3-20 ändert Zeile `:38`; die Spaltenform muss bleiben.
- **F3-20 ↔ F3-23:** gleicher Test und gleiche Katalogdateien; der F3-23-Guard prüft nach F3-20 auch `when` der Markdown-Zeile. In einer Unit umsetzen, F3-20 zuerst (sonst verlangt der Guard das alte `run, result`).
- **E-068:** F3-20 (Spec-Test) und F3-30 (Middleware-Test) tragen beide in E-068 ein. Eine Unit ändert die Regel-Datei.
- **Register-Guard (`tests/unit/test_entscheidungsregister.py`):** wird gerade von einem anderen Agenten geändert. Die neuen Marker E-067/E-068 erst eintragen, wenn dessen Stand gemergt ist.

## Abweichungen von Register und Regeln

- Keine Empfehlung widerspricht einer Regel.
- F3-06: Das Register nennt `locking-and-dedup.md:340`; die Stelle ist nicht betroffen. Für Producer-Signale (`TASK_ENQUEUED`, `client.*`-Dedup) ist die modulweite `app` richtig; die Doku darf nicht pauschal „immer `--configure`“ sagen. Neu gefunden: `middleware.de.md:223-226`.
- F3-20: Neu gefunden sind `message-formats.md:307-310` (+DE `:310-314`) und `error-catalog-machine.de.md:41`.
- F3-28: Der AE4-Marker ist schon gesetzt. Kein Test prüft aber „`run()` wirft → `post_execution` läuft nicht“; ein Festschreibtest wird empfohlen.
- F3-10: `sig(immutable=)` ist ein weiterer abgefangener Name, den E-064 nicht nennt. Aufnahme in die Doku ist optional.
