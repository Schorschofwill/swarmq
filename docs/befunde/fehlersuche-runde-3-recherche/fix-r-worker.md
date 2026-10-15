# Fix-Recherche Worker, Shutdown, Supervisor, Progress (F3-03, F3-12, F3-13, F3-15)

Stand: Worktree `swarmq-fehlersuche3` @ 580ef3c (Branch `test/fehlersuche-runde-3`, Basis 0.12.2-Entwurf, Nachweise aus 968ff7f, Register-Marker aus 580ef3c).
Bibliotheken aus `.venv`: aio-pika 10.0.1, aiormq 7.0.0, pamqp 4.0.1, valkey 6.1.1, CPython 3.14.4.
Nur gelesen. Ausgeführt: die sechs Nachweise der vier Befunde mit `--runxfail -p no:randomly`. Alle sechs sind rot an der fachlichen Assertion (siehe je Befund). Dazu ein Einzeiler gegen `aio_pika.Message` (Kodierung von `expiration`). Kein Integrationstest, kein Dienst.

**Querschnitt Mischbetrieb:** Keiner der vier Fixes ändert Lua-Skripte, Header, Keys, Queue-Argumente oder das Format des Progress-HASH. Alle wirken lokal in einem Prozess (Worker, CPU-Kind derselben Installation, Supervisor). Ein 0.12.3-Prozess neben 0.12.0–0.12.2 verhält sich je Zustellung bzw. je Pod alt oder neu. Zwischen den Versionen wandert kein neuer Zustand. Rolling-Deploy und Rollback gehen in beide Richtungen. Bei jedem Befund steht nur, wo es Feinheiten gibt.

**Querschnitt Größengrenze:** Der Guard `tests/unit/test_architecture_guards.py:37` (`_MAX_MODULE_LINES = 800`, physische Zeilen, `_LOC_EXCEPTIONS` leer) betrifft im Paket nur `src/swarmq/worker/pipeline.py` (797). Die empfohlenen Fixes berühren `pipeline.py` nicht oder machen sie kürzer (F3-12, optionales Refactoring −2 bis −4 Zeilen). Die anderen Dateien dieses Pakets liegen weit unter der Grenze: `worker/shutdown.py` 538, `worker/execution.py` 354, `worker/compute/child.py` 407, `worker/gates/lock.py` 641, `worker/retry.py` 353, `exceptions.py` 270, `task.py` 504, `process_manager/supervisor.py` 638, `process_manager/restart_budget.py` 208, übrige `process_manager/*` ≤ 370. Paketweit nahe an der Grenze, aber hier nicht berührt: `logging_file.py` 771, `logging_queue.py` 760, `agent_spec.py` 744.

---

## F3-03 (P2): `Retry(delay<0)` wird ungeprüft übernommen

### Ursache
- `src/swarmq/exceptions.py:257-260`: `Retry.__init__` übernimmt `delay` ohne Prüfung.
- `src/swarmq/worker/retry.py:125-131`: `delay_seconds = exc.delay`, danach `delay_ms = int(delay_seconds * 1000)`. Das liegt außerhalb des `try` um `retry_later` (`:163-171`).
  - Negativ: `delay_ms < 0` geht an `broker.retry_later`.
  - `inf`: `int(inf)` wirft `OverflowError`, `nan` wirft `ValueError`, beides schon in `:131`. Die Ausnahme verlässt den RetryHandler, die Nachricht bleibt unbestätigt (Hänger).
- `src/swarmq/broker/rabbitmq/delivery.py:655-669` (`retry_later`) → `_delay_target` (`declarations.py:119-130`, `queue_names.py:80-86`) → `_publish_to_delay_queue` (`delivery.py:259-273`, `expiration=delay_ms / 1000`).
- aio-pika 10.0.1, `aio_pika/message.py:31-49`: `encode_expiration` rechnet `str(int(seconds * 1000))`. Gemessen: `-5` → `"-5000"`, `-0.001` → `"-1"`, `-0.0` → `"0"`, `0.0004` → `"0"`.
- RabbitMQ lehnt `expiration < 0` mit `PRECONDITION_FAILED … {value_negative,-1000}` ab und schließt den Publish-Kanal (Beleg live in `C-grenzwerte.md`, R3C-02). Der Rückfall `retry_publish_failed` → `redeliver_in_place` (`retry.py:165-171`) zählt nicht gegen `max_retries`. Bei Quorum-Queues endet das erst am Zustelllimit (Default 20).
- Heute schon gilt: Werte in `(-0,001; 0)` ergeben über `int()` bereits `delay_ms = 0`. Erst ab `-0,001 s` wird es negativ.

### Nachweis-Test
- `tests/unit/worker/test_befund_f3_03_retry_negative_delay.py`, zwei Tests (drei Fälle). Mit `--runxfail` rot an der Kernaussage: `delay_ms=-5000` statt 0; `Retry(delay=inf|nan)` wirft nicht.
- Prüft E-060 korrekt: geklemmt auf 0, Retry-Zähler +1, kein `redeliver`. `inf`/`nan` → `ValueError` mit „delay“ im Text, und zwar beim Erzeugen.
- Lücken (kein Widerspruch zur Regel):
  - „zählt normal gegen `max_retries`“ wird nur über den Header geprüft, nicht am Budgetende.
  - `on_retry`/`TASK_RETRYING` werden nicht geprüft.
  - `-0.0` und kleine negative Werte fehlen.
- Der im Register genannte Integrationstest `tests/integration/broker/test_befund_f3_03_negative_expiration.py` existiert nicht (Register `:157`). Er muss mit dem Fix neu entstehen. Wichtig: Mit Fix A (unten) bleibt der Broker-Pfad unverändert. Ein Integrationstest, der `broker.retry_later(delay_ms=-1000)` direkt ruft, würde also nie grün. Er muss über einen echten Worker laufen (Task wirft `Retry(delay=-5)`), oder es braucht zusätzlich die Broker-Klemme (Option C).

### Optionen
**A: Klemmen und Prüfen in `Retry.__init__` (empfohlen).**
```python
def __init__(self, delay: float | None = None) -> None:
    if delay is not None:
        if not math.isfinite(delay):
            raise ValueError(f"Retry delay must be finite, got {delay!r}")
        if delay < 0:          # -0.0 bleibt -0.0; int(-0.0*1000) == 0, ok
            delay = 0.0
    self.delay = delay
```
- Genau der Wortlaut von E-060: Klemmen auf 0; `inf`/`nan` lehnt „schon der Konstruktor“ ab.
- Alle Folgeverbraucher sehen denselben Wert: `delay_ms` (`retry.py:131`), `chain.run_on_retry(..., delay=…)` (`:137`), `Signal.TASK_RETRYING` (`:138-145`), Log `task_retrying` (`:172`). Keine Middleware bekommt mehr `-5`.
- `-0.0`: `-0.0 < 0` ist `False`. Der Wert bleibt `-0.0`, `int(-0.0 * 1000) == 0`. Wer im Signal lieber `0.0` sehen will, schreibt `if delay <= 0: delay = 0.0`. Das ist ohne Folgen.
- `math.isfinite("x")` wirft `TypeError`. Das ist erwünscht, siehe Nachbarpfad CPU-Kind unten.
- Nutzer sieht `exc.delay == 0.0` statt des geworfenen Werts. Das ist die entschiedene Folge („wiederholt sofort“).

**B: Klemmen im `RetryHandler`, Prüfung im Konstruktor.**
- `delay_seconds = max(0.0, exc.delay)` in `retry.py:126`. `inf`/`nan` muss trotzdem in `__init__` abgelehnt werden (E-060).
- Vorteil: `exc.delay` bleibt der geworfene Wert (Diagnose). Nachteil: Die Regel liegt an zwei Stellen. Das Klemmen muss vor `run_on_retry`/Signal stehen, sonst sehen Hooks den negativen Wert.

**C (optional, zusätzlich): Klemme in `RabbitMQBroker.retry_later`.** `delay_ms = max(0, delay_ms)` vor `_delay_target`.
- Verteidigung in der Tiefe für alle sechs Aufrufer von `retry_later`: `retry.py:164`, `gates/rate_limit.py:109`, `gates/schedfire.py:269`, `gates/unknown_task.py:138`, `gates/backend_defer.py:98`, `gates/execution_guard.py:195`, `gates/__init__.py` (`redeliver_in_place`).
- Keiner dieser Pfade liefert nach Lesart des Codes negative Werte (Backoff-Funktionen, Lua-`retry_after`). Nicht nötig für E-060, aber billig. Dann trägt auch ein direkter Broker-Integrationstest.
- E-060 nennt `_publish_to_delay_queue` unter `applies_when`. Die Klemme widerspricht der Regel nicht.

### Folgefehler
- **Aufrufer von `Retry`:** nur Nutzercode und `RetryHandler.evaluate`. In `src/` wird `Retry` sonst nur exportiert (`__init__.py:34,149`) und in `agent_spec.py:279` beschrieben.
- **CPU-Kind (`worker/compute/envelope.py`):**
  - Ein `Retry` reist gepickelt. `Exception.__reduce__` liefert `(Retry, (), {'delay': …})`, gemessen. Beim Entpickeln läuft `Retry()` (delay `None`, Prüfung neutral), danach setzt `__dict__` den schon im Kind geklemmten Wert. Kein Problem.
  - `reconstruct_exception` (`:152-175`) probiert zuerst `Retry(info.message)`. Bei `Retry` ist `message == ""`, heute entsteht also `delay=""`, und `retry.py:131` wirft `ValueError` (`int("")`). Mit A wirft `math.isfinite("")` einen `TypeError`. Die Schleife fängt das und fällt auf `Retry()` zurück, also Backoff. Kleine Verbesserung, nur erreichbar, wenn ein `Retry` nicht picklebar ist (praktisch nie).
  - `tests/unit/worker/compute/test_envelope.py:27-31` (`Retry(delay=5.0)`) bleibt grün.
- **`inf`/`nan` im Task:** Der `ValueError` fliegt jetzt im Nutzercode. Er ist eine normale Task-Ausnahme: Backoff statt fester Wartezeit, und er unterliegt `retry_exceptions`. Steht `ValueError` dort nicht, endet der Task terminal. Das folgt aus E-060 und gehört in den Changelog.
- **`delay=0` auf dem Delay-Queue-Pfad:**
  - `delay_ms=0` → `delay_tier_for` liefert die Stufe `{queue}.DQ.1s` (`0 <= 1000`), sonst `{queue}.DQ`. Keine eigene Queue.
  - aio-pika kodiert `expiration="0"`. RabbitMQ erlaubt 0 („value_negative“ greift erst unter 0).
  - Die Delay-Queues haben kein `x-message-ttl` auf Queue-Ebene (`topology.py:49-55`). Die Nachricht läuft am Kopf der Stufe sofort ab und geht per DLX zurück in die Haupt-Queue. Steht ein älterer 1-s-Eintrag davor, wartet sie höchstens bis zu dessen Ablauf (TTL wird am Kopf geprüft). Das ist dieselbe Grenze wie bei jeder Stufe (E-026).
  - Live ungeprüft für `expiration="0"` auf Quorum-Delay-Queues. Der neue Integrationstest sollte genau diesen Rundlauf zeigen.
- **Rückfall `redeliver_in_place` zählt nicht gegen `max_retries`, bleibt das ein Problem?**
  - Für F3-03 nicht mehr: Negative Werte erreichen den Broker nicht.
  - Andere Ursachen für `retry_publish_failed`: Verbindungsverlust (`PublishIndeterminate`), Broker-Alarm (`BrokerPublishBlocked`), Publish-Endfrist im Shutdown, unroutbar nach Neu-Deklarieren. Diese sind vorübergehend, dafür ist der Rückfall gedacht. Das Zustelllimit begrenzt die Schleife.
  - Deterministisch bleiben zwei Fälle:
    1. `MessageHeadersTooLarge` (`header_guard.py:37-41`). Praktisch kaum erreichbar, weil die Zustellung mit denselben Headern ankam und der Traceback gekürzt wird.
    2. **Zu große Wartezeit.** Nach RabbitMQ-Quelltext (`rabbit_misc:check_expiry`, `?MAX_EXPIRY_TIMER = 4294967295` ms, etwa 49,7 Tage; nicht live geprüft) lehnt der Broker größere `expiration` mit `value_too_large` ab, ebenfalls mit Kanalabbau. `Retry(delay=5e6)` (58 Tage) oder `Retry(delay=1e300)` (endlich, also von E-060 nicht erfasst) erzeugt denselben Kollateralschaden wie F3-03. Gleiches gilt für den Backoff-Pfad, wenn `retry_max_delay` über 49,7 Tagen liegt (F3-04 macht ihn nur endlich).
  - **Nicht von E-060 gedeckt; offene Frage an Georg** („obere Grenze klemmen oder ablehnen?“). Kein Teil von 0.12.3, außer Georg entscheidet.
  - Klassische Queues haben kein Zustelllimit (`config.py:242-244`). Dort dreht sich jede deterministische Rückfall-Schleife ohne Ende. Das ist älter als F3-03 und hier nur vermerkt.
- **Entscheidungen und ihre Tests:**
  - E-060: `test_retry.py::test_retry_delay_wins_although_retry_exceptions_does_not_list_it`, `::test_retry_retries_although_retry_exceptions_does_not_list_it` und `::test_retry_with_zero_max_retries_ends_terminally` nutzen positive Wartezeiten. Sie bleiben grün.
  - E-023/F3-16 (`backoff.py`) wird nicht berührt.
- **Bibliothek:** aio-pika 10.0.1 prüft `expiration` nicht. pamqp 4.0.1 kodiert den String. Die Prüfung liegt allein beim Broker.
- **Mischbetrieb/Rollback:** Lokal im Worker bzw. Kind. Ein 0.12.2-Worker behält den Fehler, bis er ersetzt ist. Kein Formatthema.

### Doku (EN+DE)
- `website/reference/errors.md:353-383` / `errors.de.md:373-405` (Abschnitt `Retry`): „A negative `delay` is treated as 0 (retry immediately) and still counts against `max_retries`; `inf`/`nan` raise `ValueError` when `Retry` is created.“ DE sinngemäß.
- `website/guides/scheduling-and-retries.md:646-651` / `.de.md:672-678`: derselbe Satz. Am Beispiel `reset_at - time.time()` erklärt das den Fall.
- Changelog `website/reference/changelog.md` / `.de.md`, neuer Abschnitt `## [0.12.3] — unreleased`, „Fixed“: „**F3-03: `Retry(delay<0)` no longer closes the publish channel.** A negative delay (e.g. `reset_at - time.time()` after a 429) is clamped to 0 and counts against `max_retries`; before, RabbitMQ rejected the publish, closed the channel (parallel publishes failed with `PublishIndeterminate`) and the message was redelivered without counting. `Retry(delay=inf|nan)` now raises `ValueError` in the task; before the worker failed and left the message unacknowledged.“
- Bekommt `Retry` einen Docstring, ändert sich `website/reference/agent/spec/errors.json` (`"doc": null` heute, `agent_spec.py:381`). Dann neu erzeugen: `uv run python -m swarmq.agent_spec`.

### Register nach dem Fix
- Nachweis-Test: `xfail` entfernen, `befund("F3-03")` bleibt, dazu `@pytest.mark.entscheidung("E-060")` an beiden Tests.
- E-060: `tests:` um `tests/unit/worker/test_befund_f3_03_retry_negative_delay.py::test_negative_retry_delay_is_clamped_to_zero_and_counts` und `::test_non_finite_retry_delay_is_rejected` ergänzen, `ohne_test:` streichen.
- Befundregister `:157` (Status) nachziehen. Den Integrationstest dort richtig beschreiben (über den Worker, nicht über `retry_later`).

### Testszenarien
1. `Retry(delay=-5.0)` → `Retry.delay == 0`; RetryHandler: `retry_later(delay_ms=0)`, Header `retry_count=1`, kein `redeliver` (Nachweis).
2. `Retry(delay=-0.0)` und `Retry(delay=-0.0005)` → `delay_ms == 0`.
3. `Retry(delay=0)` → `delay_ms == 0`, Ziel `{queue}.DQ.1s`, wenn deklariert (Unit gegen `_delay_target`).
4. `Retry(delay=inf)`, `Retry(delay=-inf)`, `Retry(delay=nan)` → `ValueError` mit „delay“ (Nachweis plus `-inf`).
5. Budget: `max_retries=2`, Header `retry_count=2`, `Retry(delay=-5)` → terminal (DLQ bzw. Fehlerergebnis), kein `retry_later`.
6. `on_retry` und `TASK_RETRYING` erhalten `delay=0` (nur bei Option A garantiert).
7. Integration (mit Testports 5699/6399, über `uv run just integration`): ein Worker; Task wirft immer `Retry(delay=-5)`, `max_retries=2`. Erwartet: drei Läufe, danach DLQ. Parallel 20 normale Publishes, keiner endet in `PublishIndeterminate` oder `BrokerConnectionError`. Nach dem Fix grün, vorher rot.
8. Optional C: `RabbitMQBroker.retry_later(delay_ms=-1000)` → publiziert mit `expiration="0"`.

### Dateien
- `src/swarmq/exceptions.py` (A); bei B zusätzlich `src/swarmq/worker/retry.py`; bei C `src/swarmq/broker/rabbitmq/delivery.py`.
- `tests/unit/worker/test_befund_f3_03_retry_negative_delay.py`, neu `tests/integration/broker/test_befund_f3_03_negative_expiration.py` (oder unter `tests/integration/worker/`).
- `website/reference/errors.md`, `errors.de.md`, `website/guides/scheduling-and-retries.md`, `.de.md`, `website/reference/changelog.md`, `.de.md`.
- `compound-packs/swarmq-entscheidungen/e060-retry-delay.md`, `docs/befunde/fehlersuche-runde-3-2026-10.md`.
- Aufwand S. Risiko niedrig.

---

## F3-12 (P2): Doppelte Lock-Freigabe mit Fehlalarm `task_lock_lost` im Shutdown

### Ursache
- Shutdown-Schritt 3 bricht laufende Handler ab und wartet höchstens `min(1.0, step_timeout)` (`worker/shutdown.py:117-123`).
- `_release_leftover_locks` (`shutdown.py:217-269`) wartet auf `_detached_releases` und entnimmt dann je Handler den Registry-Eintrag (`:243-245`, `pop` vor der Freigabe). Es gibt die Tokens frei und ruft `LockKeeper.drop`. Das ist E-071.
- Räumt der abgebrochene Handler danach noch auf (Nutzercode im `except CancelledError`, langsamer Fehlerpfad, Kind-Kill), erreicht er später sein `finally`: `pipeline.py:691-712` → `_release_locks_and_slot` (`:787-797`) → `_detached_lock_release` (`:352-362`) → `gates/lock.py:591-641` `release_acquired_locks`.
- `release_acquired_locks` entnimmt den Registry-Eintrag (`:615`, `pop(handler_id, None)`), ignoriert aber, dass er schon fehlt. Es gibt die **lokale** Liste `acquired_locks` frei. Derselbe Token wird ein zweites Mal freigegeben. `release` liefert `False`, es folgt WARNING `task_lock_lost stage=release` (`:631-641`).
- Der Nachbarpfad `_release_after_teardown` (`pipeline.py:772-785`) prüft seit f21459f `handler_id not in self._active_handler_locks` (`:783-784`). Diese Prüfung ist aber nicht atomar mit dem `pop`: Zwischen der Prüfung und dem ersten Schritt des neu angelegten Freigabe-Tasks (`_detached` → `_track` → `create_task`) kann die Abräumschleife des Shutdowns den Eintrag nehmen und freigeben. Das Fenster ist eng, aber dasselbe Muster.

### Nachweis-Test
- `tests/unit/worker/test_befund_f3_12_shutdown_double_release.py::test_shutdown_then_handler_finally_releases_each_token_once`. Mit `--runxfail` rot an der Kernaussage: `Token mehrfach freigegeben: {'…': 2}`.
- Prüft die Festlegung korrekt: genau eine Freigabe je Token, kein `task_lock_lost`. F3-32 wird bewusst nicht geprüft; ob der Lock bei noch laufendem Handler frei wird, bleibt offen.
- Er setzt `_SHUTDOWN_STEP_MIN_S = 0.01` (wie `test_pipeline_finally.py`). Das Aufräumen ist per Event gesteuert, die Verkürzung macht den Test also nicht unzuverlässiger (AGENTS.md, Verbot 9, eingehalten). Laut Docstring hat G1 dasselbe Ergebnis mit 1,0 s gesehen. Das Register (`:165`) nennt „mit echter 1-s-Untergrenze nach `tests/integration/`“ als Möglichkeit, keine Pflicht. Kein Widerspruch, ein Integrationstest ist nicht nötig.

### Optionen
**A: Registry-Eintrag atomar entnehmen und nur freigeben, was man selbst entnommen hat (empfohlen).** In `gates/lock.py:615`:
```python
if active_handler_locks.pop(handler_id, None) is None:
    # Der Shutdown hat die Locks dieses Handlers schon abgeraeumt (E-071):
    # nicht noch einmal freigeben -- sonst Fehlalarm task_lock_lost (F3-12).
    return
```
- `pop` ist synchron, Shutdown-Schleife und Handler-Freigabe laufen im selben Loop. Wer zuerst entnimmt, gibt frei; der andere sieht nichts. Das schließt auch das Fenster in `_release_after_teardown`.
- Invariante: Jeder Token in `ctx.acquired_locks` steht im selben synchronen Schritt in der Registry (`gates/lock.py:581-584`, `_acquire_and_register`). Ein Eintrag fehlt also nur, wenn ihn schon jemand entnommen hat: der Handler selbst (kein zweiter Aufruf je Handler) oder der Shutdown.
- `drop` entfällt im Skip-Fall zu Recht. Der Shutdown hat die Lease schon gedroppt (`shutdown.py:246`), und der Keeper ist geschlossen (`:269`).
- `pipeline.py` wächst nicht. Optionales Refactoring: Die nun doppelte Prüfung `pipeline.py:783-784` entfernen und den Docstring `:775-779` kürzen. Das gibt 2–4 Zeilen Luft (797 → 793–795). Abgesichert durch `test_pipeline_finally.py::test_cancel_during_di_teardown_racing_shutdown_cleanup_releases_once` und `::test_teardown_follow_up_ending_during_shutdown_cleanup_releases_once`.

**B: Tombstone im Worker.** `_release_leftover_locks` merkt sich entnommene `handler_id`s in einem Set. `_release_locks_and_slot` prüft das Set.
- Mehr Zustand (wann leeren?), Zeilen in `pipeline.py` (über 800, braucht Auslagerung), dasselbe Atomaritätsproblem wie heute, wenn die Prüfung vor dem `create_task` liegt.
- Verworfen.

**C: Prüfung in `_release_locks_and_slot` statt in `_release_after_teardown`.** Wie heute in `:783`, nur zentral. Nicht atomar (siehe Ursache), und sie kostet Zeilen in `pipeline.py`. Verworfen.

### Folgefehler
- **Aufrufer von `release_acquired_locks`:** nur `Worker._release_acquired_locks` (`pipeline.py:227-241`) und der Test `tests/unit/worker/test_lock_lost_at_shutdown.py::test_release_acquired_locks_names_the_task_from_the_lease`. Der übergibt eine Registry mit Eintrag und bleibt grün.
- **Shutdown-Seite** (`shutdown.py:243-267`) bleibt unverändert. Tests, die die Registry direkt füllen (`test_lifecycle.py:384-454`, `test_lock_keeper_wiring.py:77`, `test_lock_lost_at_shutdown.py:30,47`, `test_recycling_sigterm.py:142`, `test_shutdown_budget_steps.py:122,140`), testen nur den Shutdown und bleiben grün.
- **`test_pipeline_finally.py:376-495`** (drei Renn-Tests, ohne Marker): Alle erwarten genau eine Freigabe. Mit A bleiben sie grün, die Wirkung kommt nun aus `gates/lock.py` statt aus `pipeline.py:783`.
- **Neue Grenze:** Scheitert die Freigabe des Shutdowns (`shutdown_lock_release_failed`, etwa Timeout), versucht der Handler heute später ein zweites Mal. Mit A nicht mehr. Der Lock heilt dann über die TTL (der Keeper hat ihn gedroppt). Das ist dieselbe Folge wie jede gescheiterte Freigabe (KTD11) und kein neuer Fehler. Meist ist das Backend dann ohnehin geschlossen.
- **E-071** (Freigabe erst nach der Wartezeit, vor dem Backend-Close): Reihenfolge unverändert, Regel eingehalten.
- **E-022** (Lease endet mit der Freigabe, `task_lock_lost` nur bei echtem Verlust): Der Fehlalarm entfällt, ein echter Verlust meldet weiter. Die E-022-Tests in `test_lock_keeper.py:126,163,180,364` und `tests/contracts/coordination.py` berühren `release_acquired_locks` nicht.
- **F3-32 (offen) bleibt unberührt.** A legt nicht fest, ob der Shutdown den Lock eines noch laufenden Handlers freigibt. Beide möglichen Antworten passen zu A: Bei (a) „laufende Handler überspringen“ bleibt der Registry-Eintrag stehen, und der Handler gibt später selbst frei. Bei (b) „beibehalten“ gilt der heutige Ablauf. Ein Tombstone (B) müsste bei (a) wieder umgebaut werden.
- **Mischbetrieb:** Worker-lokal, kein Valkey-Format. Rollback bringt nur den Fehlalarm zurück.

### Doku (EN+DE)
- Keine Verhaltensdoku nötig. `task_lock_lost` in `website/reference/logging.md` / `.de.md` bleibt richtig.
- Changelog EN+DE, 0.12.3 „Fixed“: „**F3-12: no false `task_lock_lost` at shutdown.** When a cancelled handler with `lock=` cleaned up for longer than the shutdown waited, the shutdown released its lock and the handler's own `finally` released the same token again and warned `task_lock_lost` (‚a second holder may have run in parallel‘). Each token is now released once.“

### Register nach dem Fix
- Nachweis-Test: `xfail` entfernen, `befund("F3-12")` bleibt. **Kein** `entscheidung("E-071")`: Der Test prüft nicht die Reihenfolge, die E-071 festlegt.
- E-071: Im Abschnitt „Überdenken“ ergänzen: „F3-12 behoben in 0.12.3 (`release_acquired_locks` gibt nur frei, was es selbst aus der Registry entnommen hat).“ `tests: []` und `ohne_test:` bleiben, die Kandidatenliste bleibt gültig.
- Befundregister `:165` nachziehen.

### Testszenarien
1. Nachweis: Shutdown räumt ab, danach `finally` des Handlers → je Token eine Freigabe, kein `task_lock_lost`.
2. Neuer Unit-Test auf die Funktion (öffentlich im Modul, Muster `test_lock_lost_at_shutdown.py:58`): `release_acquired_locks(store, {}, handler_id, [token], drop=spy)` → `store.release` nicht gerufen, `drop` nicht gerufen, kein Log.
3. Gegenprobe: Registry mit Eintrag → Freigabe wie bisher, bei `False` weiter `task_lock_lost` (bestehender Test).
4. Semaphor-Lock mit zwei Tokens desselben Handlers, Shutdown räumt ab → je Token eine Freigabe.
5. Rennen „Prüfung vor `create_task`“ aus `_release_after_teardown`: durch die bestehenden Tests `test_pipeline_finally.py:404,460` abgedeckt, nach dem optionalen Refactoring weiter grün.

### Dateien
- `src/swarmq/worker/gates/lock.py` (Fix).
- Optional: `src/swarmq/worker/pipeline.py` (nur Entfernen der doppelten Prüfung, −2 bis −4 Zeilen).
- `tests/unit/worker/test_befund_f3_12_shutdown_double_release.py`, `tests/unit/worker/test_lock_lost_at_shutdown.py` (neuer Fall 2) oder `tests/unit/worker/gates/test_lock.py`.
- `website/reference/changelog.md`, `.de.md`; `compound-packs/swarmq-entscheidungen/e071-leftover-locks-nach-wartezeit.md`; `docs/befunde/fehlersuche-runde-3-2026-10.md`.
- Aufwand S. Risiko niedrig.

---

## F3-13 (P2): Supervisor tötet recycelnden Worker vor dem nächsten Lebenszeichen

### Ursache
- `src/swarmq/process_manager/restart_budget.py:54-58`: `_heartbeat_deadline` liefert für `INTENT_DRAINING` **und** `INTENT_RECYCLE` die Frist `shutdown_grace_s + SHUTDOWN_TERM_S` (`SHUTDOWN_TERM_S = 5.0`, `supervision.py:82`), sonst `heartbeat_timeout_s`.
- Verbraucher sind `_check_heartbeats` (`:85`, plus `deadline_s` im Log `:93`) und `_replace_hung_worker` (`:116`, nur Log).
- Ablauf beim Recycling: Absicht `INTENT_RECYCLE` → `expected_shutdown = True`, Ersatz, Bestätigung (`:70-82`). Ab dann prüft `:83-95` den Recycler gegen diese Frist. Bei Überschreitung folgen `worker_hung_during_drain` und `_terminate_worker` (SIGTERM, SIGKILL nach 5 s).
- Mit `heartbeat_interval_s >= shutdown_timeout + 5` (z. B. Intervall 15, `shutdown_timeout=10`, oder `shutdown_timeout=0` bei Default-Intervall 5) gilt ein gesunder Recycler zwischen zwei Beats als hängend. Er bekommt SIGTERM. Danach greift im Worker das Budget ab dem Signal (E-018 Nachtrag FS-30), und Langläufer brechen nach E-001 ab. Das verletzt E-038 (Recycling ohne erzwungenen Abbruch).
- Der Intent bleibt nach `request_recycle` auf `INTENT_RECYCLE` stehen (`supervision.py:43-48`, `mark_draining` nur aus `INTENT_NONE`). Der Recycler wird also über den ganzen Drain nach der Recycling-Frist geprüft.

### Nachweis-Test
- `tests/unit/process_manager/test_befund_f3_13_recycle_heartbeat_deadline.py::test_recycling_worker_gets_at_least_heartbeat_timeout`. Mit `--runxfail` rot: `worker_hung_during_drain` nach 16 s bei Frist 15 s.
- Prüft E-018 (Nachtrag 2026-10-15) korrekt: nur `INTENT_RECYCLE`, Frist mindestens `heartbeat_timeout_s` (60 > 16). Den Scale-down nimmt er ausdrücklich aus.
- Lücke: Er pinnt nicht, dass `INTENT_DRAINING` unverändert bleibt. Dafür einen zweiten Test ergänzen (Szenario 2).

### Optionen
**A: Frist je Intent trennen, Recycling mit `max(...)` (empfohlen, = E-018 Nachtrag, Option 1).**
```python
def _heartbeat_deadline(self, info: WorkerInfo) -> float:
    intent = info.channel.intent_value() if info.channel is not None else None
    if intent == INTENT_RECYCLE:
        return max(self.shutdown_grace_s + self.SHUTDOWN_TERM_S, self.heartbeat_timeout_s)
    if intent == INTENT_DRAINING:
        return self.shutdown_grace_s + self.SHUTDOWN_TERM_S
    return self.heartbeat_timeout_s
```
- „mindestens `heartbeat_timeout_s`“ heißt `max`. Bei großem `shutdown_timeout` (z. B. 120 → 125 s) bleibt die heutige, längere Frist. Ein reines `heartbeat_timeout_s` würde sie verkürzen; das ist nicht entschieden.
- Keine neue Validierung. `shutdown_timeout=0` bleibt zulässig: Frist `max(5, 60) = 60`.
- Etwa 4 Zeilen mehr in `restart_budget.py` (208). Unkritisch.

**B: Validierung `heartbeat_interval_s < shutdown_timeout + SHUTDOWN_TERM_S`.** Von E-018 ausdrücklich verworfen, weil sie `shutdown_timeout=0` einschränkt. Nicht weiter verfolgt.

### Folgefehler
- **Bewusste Folge, im Nachtrag noch nicht ausgesprochen:** Mit den Defaults (`shutdown_timeout=30`, `config.py:217`; `heartbeat_timeout_s=60`, `config.py:350`) steigt die Frist für einen **eingefrorenen** Recycler von 35 s auf 60 s. Ein hängender Recycler (oft wegen RSS recycelt) hält seinen Speicher und sein Prefetch also bis zu 25 s länger. Der Ersatz läuft schon, Kapazität geht nicht verloren. Vorschlag: diesen Satz in den E-018-Nachtrag (Bewusste Folgen) aufnehmen.
- **Bestehende Tests:**
  - `tests/unit/process_manager/test_supervisor.py:1534-1567` (`test_acknowledged_recycler_that_freezes_in_drain_is_killed_not_replaced`): `heartbeat_timeout_s=1.0`, Default-Gnade 30 s, `stale=120` > `max(35, 1)`. Bleibt grün.
  - Die übrigen Recycling-Tests (`:1509`, `:1674-1816`) prüfen keine Frist.
  - Kein Test referenziert `_heartbeat_deadline` direkt (grep).
- **E-018-Tests** (`test_shutdown_budget.py:18`, `test_befund_bz18_shutdown_budget.py:76`, `test_shutdown_budget_steps.py`, `test_recycling_sigterm.py:113`, `test_befund_bz18_cleanup_after_grace.py:44`): Sie prüfen das Worker-Budget bzw. die Supervisor-Eskalation, nicht die Heartbeat-Frist. Sie bleiben grün.
- **E-038:** wird erfüllt (kein erzwungener Abbruch eines gesunden Recyclers).
- **Nachbarn mit `shutdown_grace_s + SHUTDOWN_TERM_S`:** `process_manager/setpoint.py:210` (`status_ttl_s`) betrifft den Abbau des Supervisors, nicht das Recycling. Er bleibt unverändert. Der E-018-Absatz „Überdenken“ nennt `restart_budget.py:57` und `setpoint.py:210`. Nach dem Fix gilt `:57` nur noch für `INTENT_DRAINING`; den Text anpassen.
- **Supervisor-Shutdown:** `_check_heartbeats` endet sofort bei `_shutting_down` (`:62-63`). Ein SIGTERM an den ganzen Pod ist also nicht betroffen.
- **F3-04 (Fristen endlich):** Mit `heartbeat_timeout_s=inf` würde die Recycling-Frist unendlich. F3-04 schließt das über die Validierung, kein eigener Schutz nötig.
- **Mischbetrieb/Rollback:** Supervisor und Worker kommen aus derselben Installation (Kinder per `spawn`). Kein geteilter Zustand. Rollback stellt nur die alte Frist wieder her.

### Doku (EN+DE)
- `website/reference/configuration.md:47` / `.de.md:48` (`SWARMQ_HEARTBEAT_TIMEOUT_S`): Satz ergänzen: „A self-recycling worker gets at least this long between heartbeats (or `shutdown_timeout + 5 s`, whichever is larger) before the supervisor kills it; a scale-down drain gets `shutdown_timeout + 5 s`.“
- **Nebenbefund Doku (nicht im Register):** `worker_hung_during_drain` (`restart_budget.py:89`) fehlt im Event-Katalog `website/reference/logging.md:699-700` / `logging.de.md:717-718`. Neben `worker_hung` eine Zeile ergänzen: WARNING, „A draining or recycling worker stopped beating; it is terminated (SIGTERM, then SIGKILL), not replaced. Fields `worker_id`, `pid`, `stale_s`, `deadline_s`.“ Bitte als Doku-Lücke in den Plan aufnehmen oder als eigenen Befund führen.
- Changelog EN+DE, 0.12.3 „Fixed“: „**F3-13: a recycling worker is no longer killed between two heartbeats.** With a heartbeat interval at or above `shutdown_timeout + 5 s` (e.g. `shutdown_timeout=0`), the supervisor took a healthy recycling worker for hung, sent SIGTERM and aborted its long runners. A recycling worker now gets at least `heartbeat_timeout_s`. A frozen recycler is therefore killed after up to 60 s (defaults) instead of 35 s.“

### Register nach dem Fix
- Nachweis-Test: `xfail` entfernen, `befund("F3-13")` bleibt, dazu `@pytest.mark.entscheidung("E-018")`.
- E-018 `tests:` um `tests/unit/process_manager/test_befund_f3_13_recycle_heartbeat_deadline.py::test_recycling_worker_gets_at_least_heartbeat_timeout` ergänzen (der Nachtrag nennt die Datei schon). Optional auch den neuen Scale-down-Test (Szenario 2).
- Nachtrag ergänzen: bewusste Folge „eingefrorener Recycler bis `max(...)`“. Den Absatz „Überdenken“ zu `restart_budget.py:57` anpassen.
- Befundregister `:166` nachziehen.

### Testszenarien
1. Nachweis: Recycler bestätigt, Gnade 10 s, `heartbeat_timeout_s=60`, letzter Beat vor 16 s → kein SIGTERM, kein `worker_hung_during_drain`.
2. Scale-down unverändert: Kanal `mark_draining()`, `expected_shutdown=True`, Gnade 10 s, `heartbeat_timeout_s=60`, Beat vor 16 s → SIGTERM, `worker_hung_during_drain` mit `deadline_s=15`.
3. `shutdown_grace_s=0` (also `shutdown_timeout=0`), Recycler, Beat vor 30 s → kein SIGTERM; Beat vor 61 s → SIGTERM, `deadline_s=60`.
4. Große Gnade: `shutdown_grace_s=100`, `heartbeat_timeout_s=60`, Recycler, Beat vor 70 s → kein SIGTERM (Frist 105, `max` statt Verkürzung).
5. Eingefrorener Recycler (bestehender Test `:1534`) → SIGTERM, kein zweiter Ersatz.

### Dateien
- `src/swarmq/process_manager/restart_budget.py`.
- `tests/unit/process_manager/test_befund_f3_13_recycle_heartbeat_deadline.py`, neuer Scale-down-Fall dort oder in `tests/unit/process_manager/test_supervisor.py` (`TestRecycleIntent`).
- `website/reference/configuration.md`, `.de.md`; `website/reference/logging.md`, `.de.md` (Nebenbefund); `website/reference/changelog.md`, `.de.md`.
- `compound-packs/swarmq-entscheidungen/e018-shutdown-timeout-gesamtbudget.md`; `docs/befunde/fehlersuche-runde-3-2026-10.md`.
- Aufwand S. Risiko niedrig.

---

## F3-15 (P3, Code und Doku): `ProgressInfo.started_at` zeigt ein Datum von 1970

### Ursache
- IO-Pfad: `src/swarmq/worker/execution.py:98` setzt `task_instance._task_start_time = time.monotonic()`.
- CPU-Kind: `src/swarmq/worker/compute/child.py:163` macht dasselbe.
- `src/swarmq/task.py:412-416`: `started_at = datetime.fromtimestamp(self._task_start_time, tz=UTC).isoformat()`. Das ist die Sekundenzahl seit Systemstart als Unix-Zeit (gemessen: `1970-01-06T16:09:18+00:00`).
- `task.py:417-421`: `elapsed`/`eta_seconds` nutzen dieselbe monotone Startzeit. Das ist richtig und muss so bleiben.
- Doku: `website/reference/api.md:756-757` und `api.de.md:778-779` nennen `datetime` bzw. `datetime | None`. Die Dataclass hat `str` (`progress.py:40-41`), das Agent-Schema `website/reference/agent/spec/messages/progress.schema.json` auch.

### Nachweis-Test
- `tests/unit/worker/test_befund_f3_15_progress_started_at.py`, IO (`_handle_message`) und CPU-Kind (`child.run_job` mit Kanal-Shim). Beide rot: `started_at=1970-01-06…`.
- Prüft E-044 (Nachtrag 2026-10-15) korrekt: Wanduhr für `started_at` (±5 min um `now(UTC)`), `eta_seconds` klein und ≥ 0 aus der monotonen Laufzeit.
- Kleiner Robustheitshinweis: Bei `update_progress(1, 2)` direkt nach dem Start ist `elapsed > 0` nötig, sonst wird `eta_seconds` leer und `int("")` scheitert. Unter Linux (ns-Auflösung von `monotonic`) praktisch ausgeschlossen. Keine Änderung nötig.

### Optionen
**A: Zwei Zeitwerte (empfohlen, = Register Punkt 12 und E-044 Nachtrag).**
- `task.py`: neues Klassenattribut `_task_started_wall: float | None = None` neben `_task_start_time` (`:289`).
- `execution.py:98` und `child.py:163`: zusätzlich `… ._task_started_wall = time.time()`. `_task_start_time` bleibt `time.monotonic()`.
- `task.py:412-416`: `started_at` aus `_task_started_wall`. Fehlt es, bleibt der leere String wie heute bei fehlender Startzeit; `from_hash` macht daraus `None`.
- `elapsed`/`eta_seconds` unverändert aus der monotonen Uhr. Robust gegen NTP-Sprünge.
- `started_at` bleibt über alle Meldungen eines Laufs gleich.

**B: Wanduhr aus der monotonen Laufzeit zurückrechnen.** `time.time() - (loop_time - self._task_start_time)` beim Update.
- Kein Verdrahtungsaufwand. Aber `started_at` springt mit jedem Wanduhr-Sprung und kann zwischen zwei Meldungen abweichen.
- Widerspricht „zwei Zeitwerte führen“ (Register Punkt 12). Verworfen.

**Verworfen: nur Wanduhr.** `_task_start_time = time.time()` würde `elapsed` an die Wanduhr hängen. Das widerspricht dem Nachtrag („monotone Uhr für `elapsed`/`eta_seconds`“) und bricht Tests, die `_task_start_time` monoton setzen (`tests/unit/test_task.py:676-839`, `tests/stress/test_concurrency.py:481`).

### Folgefehler
- **Tests mit `_task_start_time`:** `test_task.py:676,702,719,757,777,800,821,839` und `tests/stress/test_concurrency.py:481` setzen nur die monotone Zeit. Mit A rechnen sie ETA und Drosselung unverändert. `started_at` wird dort leer; keiner dieser Tests prüft `started_at`.
- Die vier E-044-Tests (`TestUpdateProgress::test_throttles_second_call_within_window`, `::test_final_update_always_flushes`, `::test_backend_hset_failure_swallowed`, `::test_publish_failure_swallowed`) bleiben grün.
- **Property-Tests** `tests/property/test_progress_properties.py:41-163`: prüfen nur `from_hash` mit beliebigem Text. Unberührt.
- **Leser von `started_at`:** in `src/` nur `progress.py` (Dekodieren). Kein Code rechnet damit.
- **Dashboard swarmv** (`../swarmv` @ e53be10): liest den Progress-HASH nicht. `started_at` dort stammt aus eigenen Hooks bzw. der eigenen Tabelle (`workflow_baum.py:68`, `templates/tasks/detail.html:46`). Die Tabelle `progress` in `directory/schema.py:210` ist ein eigener Fortschrittszeiger des Lesers. Keine Folge.
- **Nutzer-Tasks**, die `__init__` überschreiben, ohne `super()` zu rufen: Das Klassenattribut liefert den Default, der Worker setzt den Instanzwert danach. Unverändert sicher.
- **Mischbetrieb:**
  - Das HASH-Format (`sq:progress:{task_id}`, Felder als `str`) bleibt gleich.
  - Ein Task auf einem 0.12.2-Worker schreibt weiter 1970, auf einem 0.12.3-Worker das echte Datum. Clients jeder Version lesen `str`.
  - CPU-Kind und Loop-Prozess stammen aus derselben Installation.
  - Alte Einträge verfallen mit `result_ttl`. Rollback bringt nur das falsche Datum zurück.
- **Agent-Schema:** Der Docstring von `ProgressInfo` sagt schon „ISO-8601 string“. Wird er nicht geändert, bleibt `progress.schema.json` gleich (`agent_spec.py:609`, Drift-Test `test_agent_spec.py:418`).

### Doku (EN+DE)
- `website/reference/api.md:756-758` / `api.de.md:778-780`: `updated_at` → `str` („ISO 8601, UTC“), `started_at` → `str | None` („ISO 8601, UTC; wall-clock start of the run“). Bei `eta_seconds` ergänzen: „`None` also on the final update“ (wie `message-formats.md:151`).
- `website/reference/agent/message-formats.md:150` / `.de.md`: „Empty until first update“ ist missverständlich (leer nur ohne Worker-Verdrahtung). Optional präzisieren, kein Muss.
- Changelog EN+DE, 0.12.3 „Fixed“: „**F3-15: `ProgressInfo.started_at` shows the real start time.** It came from the monotonic clock and read as a date in January 1970 (IO and CPU workers). It is now the UTC wall-clock start; `eta_seconds` still uses the monotonic run time. The API reference listed `updated_at`/`started_at` as `datetime`; they are ISO-8601 strings.“

### Register nach dem Fix
- Beide Nachweis-Tests: `xfail` entfernen, `befund("F3-15")` bleibt, dazu `@pytest.mark.entscheidung("E-044")`.
- E-044 `tests:` um `tests/unit/worker/test_befund_f3_15_progress_started_at.py::test_io_progress_started_at_is_wall_clock` und `::test_cpu_child_progress_started_at_is_wall_clock` ergänzen. Den Nachtrag auf „behoben in 0.12.3“ setzen.
- Befundregister `:168` nachziehen.

### Testszenarien
1. IO: `update_progress(1, 2)` → `started_at` in `[before − 5 min, now + 5 min]`, `0 <= eta_seconds <= 300` (Nachweis).
2. CPU-Kind: dasselbe über den Rückkanal (Nachweis).
3. Uhrensprung: `_task_started_wall` fest, `time.time` um +3600 s gepatcht, `_task_start_time = monotonic() - 2` → `started_at` unverändert, ETA wie ohne Sprung (zeigt die Trennung der Uhren).
4. Ohne Verdrahtung der Wanduhr (nur `_task_start_time` gesetzt) → `started_at == ""`, ETA berechnet (Rückwärtskompatibilität der Testhelfer).
5. Mehrere Meldungen eines Laufs → identisches `started_at`.

### Dateien
- `src/swarmq/task.py`, `src/swarmq/worker/execution.py`, `src/swarmq/worker/compute/child.py`.
- `tests/unit/worker/test_befund_f3_15_progress_started_at.py`, Zusatzfälle in `tests/unit/test_task.py` (`TestUpdateProgress`).
- `website/reference/api.md`, `api.de.md`, optional `website/reference/agent/message-formats.md`, `.de.md`; `website/reference/changelog.md`, `.de.md`.
- `compound-packs/swarmq-entscheidungen/e044-fortschritt-best-effort.md`; `docs/befunde/fehlersuche-runde-3-2026-10.md`.
- Aufwand S. Risiko niedrig.

---

## Dateien je Befund

| Datei | F3-03 | F3-12 | F3-13 | F3-15 |
|---|---|---|---|---|
| `src/swarmq/exceptions.py` | x (A) | | | |
| `src/swarmq/worker/retry.py` | nur bei B | | | |
| `src/swarmq/broker/rabbitmq/delivery.py` | nur bei C | | | |
| `src/swarmq/worker/gates/lock.py` | | x | | |
| `src/swarmq/worker/pipeline.py` | | optional (−2 bis −4 Zeilen) | | |
| `src/swarmq/process_manager/restart_budget.py` | | | x | |
| `src/swarmq/task.py` | | | | x |
| `src/swarmq/worker/execution.py` | | | | x |
| `src/swarmq/worker/compute/child.py` | | | | x |
| `tests/unit/worker/test_befund_f3_03_retry_negative_delay.py` | x | | | |
| `tests/integration/broker/test_befund_f3_03_negative_expiration.py` (neu) | x | | | |
| `tests/unit/worker/test_befund_f3_12_shutdown_double_release.py` | | x | | |
| `tests/unit/worker/test_lock_lost_at_shutdown.py` oder `gates/test_lock.py` | | x | | |
| `tests/unit/process_manager/test_befund_f3_13_recycle_heartbeat_deadline.py` | | | x | |
| `tests/unit/process_manager/test_supervisor.py` (optional) | | | x | |
| `tests/unit/worker/test_befund_f3_15_progress_started_at.py` | | | | x |
| `tests/unit/test_task.py` | | | | x |
| `website/reference/errors.md`, `.de.md` | x | | | |
| `website/guides/scheduling-and-retries.md`, `.de.md` | x | | | |
| `website/reference/configuration.md`, `.de.md` | | | x | |
| `website/reference/logging.md`, `.de.md` | | | x (Nebenbefund) | |
| `website/reference/api.md`, `.de.md` | | | | x |
| `website/reference/agent/message-formats.md`, `.de.md` | | | | optional |
| `website/reference/agent/spec/errors.json` | nur bei neuem Docstring | | | |
| `website/reference/changelog.md`, `.de.md` | x | x | x | x |
| `compound-packs/swarmq-entscheidungen/e060-retry-delay.md` | x | | | |
| `compound-packs/swarmq-entscheidungen/e071-leftover-locks-nach-wartezeit.md` | | x | | |
| `compound-packs/swarmq-entscheidungen/e018-shutdown-timeout-gesamtbudget.md` | | | x | |
| `compound-packs/swarmq-entscheidungen/e044-fortschritt-best-effort.md` | | | | x |
| `docs/befunde/fehlersuche-runde-3-2026-10.md` | x | x | x | x |

`tests/unit/_entscheidungen.py` und `tests/unit/test_entscheidungsregister.py` berührt keiner dieser Fixes. Der Guard prüft die neuen `tests:`-Einträge gegen die Marker automatisch.

## Wechselwirkungen mit anderen Befunden

- **Changelog und Befundregister** ändern alle Units. Konfliktfrei nur, wenn jede Unit ihre Einträge am Ende des Abschnitts `## [0.12.3] — unreleased` anfügt, oder wenn ein Sammel-Commit am Ende sie schreibt. Den Abschnitt einmal vorab anlegen.
- **F3-03 ↔ F3-04 / F3-16:**
  - Alle drei betreffen Retry-Wartezeiten, aber in verschiedenen Dateien: F3-03 in `exceptions.py`, F3-04 in `config_validation.py` (`retry_max_delay` endlich), F3-16 in `backoff.py`.
  - Gemeinsame Doku-Datei: `website/guides/scheduling-and-retries.md`/`.de.md`. F3-16 ändert vermutlich `:601-602`, F3-03 ändert `:646-651`. Verschiedene Absätze, aber derselbe Merge-Bereich.
  - Gemeinsame Restlücke: **zu große Wartezeit** (über etwa 49,7 Tage nach RabbitMQ `check_expiry`) schließt den Kanal wie F3-03. Das gilt für `Retry(delay=…)` und für einen sehr großen `retry_max_delay`. Weder E-060 noch die F3-04-Festlegung („endlich“) deckt das. Offene Frage für Georg.
- **F3-12 ↔ F3-32 (offen):**
  - Der empfohlene Fix entscheidet F3-32 nicht und passt zu beiden Antworten.
  - Der Nachweis-Test prüft F3-32 nicht.
  - Entscheidet Georg F3-32 (a), ändert sich `shutdown.py:243-267`. `gates/lock.py` aus F3-12 bleibt dann richtig.
- **F3-12 ↔ F3-26:** F3-26 (Untergrenzen je Token im Shutdown) liegt in `shutdown.py`. F3-12 nach Option A berührt `shutdown.py` nicht. Kein Dateikonflikt.
- **F3-13 ↔ F3-26 / E-018:** Alle Nachträge stehen in derselben Regeldatei `e018-…md`. Wer E-018 nach F3-13 ändert, hängt nur an. F3-26 ist ohne Fix.
- **F3-13 ↔ F3-05:** F3-05 (Exit 78 bei `ImportError`) ändert `process_manager/entry.py:206`. F3-13 ändert `process_manager/restart_budget.py`. Gleiches Paket, verschiedene Dateien, kein Konflikt.
- **F3-13 ↔ F3-04:** Ohne F3-04 könnte `heartbeat_timeout_s=inf` die Recycling-Frist unendlich machen. F3-04 schließt das.
- **F3-15 ↔ F3-14:** F3-14 (CPU-Slots eingebettet) ändert die Pool-/Config-Seite, F3-15 nur `compute/child.py:163`. Kein Konflikt zu erwarten. Vor dem Merge prüfen, ob F3-14 `child.py` anfasst.
- **pipeline.py-Grenze:** Keiner der vier Fixes braucht Zeilen in `pipeline.py`. Das optionale Refactoring zu F3-12 schafft 2–4 Zeilen Luft für andere Units, die dort ändern müssen (z. B. Lebenszyklus-Befunde aus Paket A).

## Was einer Register-Entscheidung widerspricht oder sie nicht umsetzbar macht

- Nichts davon widerspricht einer Regel. Alle vier Fixes sind konform umsetzbar (E-060, E-071/E-022, E-018 Nachtrag, E-044 Nachtrag). Kein Test mit `entscheidung`-Marker muss geändert werden.
- **E-060 lässt eine Lücke:** Endliche, aber zu große Wartezeiten (über etwa 49,7 Tage) lösen denselben Kanalabbau aus. Neue Frage an Georg, kein Bestandteil von 0.12.3 ohne Entscheidung.
- **E-018 Nachtrag ist unvollständig:** Die Folge „eingefrorener Recycler wird mit Defaults erst nach 60 s statt 35 s getötet“ steht dort nicht. Sie ergibt sich aus „mindestens `heartbeat_timeout_s`“ und sollte als bewusste Folge ergänzt werden.
- **Registerangabe zu F3-03:** Der genannte Integrationstest existiert nicht. Als Broker-Test (direkt `retry_later`) könnte er mit dem empfohlenen Fix nie grün werden. Er muss über den Worker laufen.
