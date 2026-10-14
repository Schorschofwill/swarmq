# Fix-Recherche Worker-Pipeline, Gates, Workflows (FS-04, -05, -12, -13, -17, -19, -20, -27, -28)

Stand: Worktree `swarmq-fehlersuche` @ 906afac (v0.12.0 + Nachweise), Register `swarmq-entscheidungen` @ 45c6336.
Bibliotheken aus `.venv`: dishka 1.10.1, aio-pika 10.0.1, aiormq 7.0.0, valkey 6.1.1, CPython 3.14.4.
Nur gelesen. Ausgeführt: die Nachweise FS-04/05/12/13 mit `--runxfail -p no:randomly`, 12 Fälle, alle rot an der Kern-Assertion. Dazu ein Wegwerfskript zu `wait_for` (unter `scratchpad/rw/wf.py`, nicht im Repo).

**Querschnitt Mischbetrieb:** Keiner der neun Fixes ändert Lua-Skripte, Header-Formate, Marker oder Keys. Alle sind lokale Entscheidungen eines Workers, Producer und Leader bleiben unberührt. Ein 0.12.1-Worker neben 0.12.0-Workern verhält sich je Zustellung entweder alt oder neu, ohne dass Zustand zwischen ihnen wandert. Rolling-Deploy geht in beide Richtungen. Bei jedem Befund steht nur noch, wo es Feinheiten gibt.

**Querschnitt Codeüberlappung (Reihenfolge der MRs):**
- `pipeline.py` `_process_message`/finally: FS-04 und FS-03 (anderer Bearbeiter). FS-03 will das Semaphor-Warten in try/finally ziehen. Beide in einem MR oder FS-04 direkt nach FS-03.
- `gates/cancel.py`: FS-12 und FS-13 (gleiche Funktion, Zeilen 128–138).
- `finalize.maybe_release_unique` und alle Aufrufer: FS-13. FS-17 braucht FS-13 für den start-Modus.
- `gates/expiry.py:118-132`: FS-17 und FS-10 (FS-10-Nachweis `test_fs10_expired_dlq_copy_drops_broker_counters` prüft dieselbe Zeile).
- `worker/execution.py` und `compute/child.py`: FS-05 und FS-20 (Doku muss zum Fix passen).

---

## FS-04 (P1): Scheiternder Dishka-Finalizer überspringt Lock-Freigabe und Semaphor-Rückgabe

### Ursache
- `src/swarmq/worker/pipeline.py:680-715`: Das finally von `_process_message` führt fünf Schritte nacheinander und ungeschützt aus:
  1. `_detached(execution_guard.settle)` :694-695
  2. `_finalize_delivery` :696
  3. `_teardown_di_scope` :707-708
  4. `_detached_lock_release` :712-713
  5. `self._semaphore.release()` :715
- `_teardown_di_scope` (:318-332) ruft `di_scope.__aexit__` über `_detached` auf, also `asyncio.shield`. Eine Ausnahme des inneren Tasks kommt beim Aufrufer an.
- Dishka 1.10.1, `dishka/async_container.py:309-340`: `__aexit__` sammelt alle `Exception` aus `agen.asend(exception)` und wirft danach `ExitError("Cleanup context errors", errors)`. `ExitError` ist eine `ExceptionGroup[Exception]`, also eine `Exception`. Eine `BaseException` aus einem Finalizer (CancelledError, KeyboardInterrupt) wird nicht gesammelt. Sie bricht sofort durch, und die übrigen Exits laufen nicht.
- Damit entfallen die Schritte 4 und 5. `LockKeeper.drop` läuft nur in `release_acquired_locks` (`gates/lock.py:587`), der Keeper verlängert also weiter (E-022). `contain_delivery_errors` (`worker/settle.py`) loggt nur `delivery_handler_failed`. Zu diesem Zeitpunkt ist schon bestätigt: Der Ergebnis-Tail ackt vor dem finally.
- Dieselbe Lücke ist breiter als der Befund. Wirft Schritt 1 oder 2, oder kommt dort ein äußerer Abbruch an (`await asyncio.shield` wirft beim Aufrufer `CancelledError`), entfallen ebenfalls DI-Teardown, Lock-Freigabe und Semaphor. Heute fangen `finalize_schedfire` und `release_unique` nur `Exception` bzw. `BackendConnectionError` (`backend/valkey/results.py:468-480`). Ein fremder Fehler im Unique-Release eines Dritt-Backends würde also genauso durchschlagen.

### Optionen
**A: try/finally-Kaskade im finally (empfohlen).** Die Reihenfolge bleibt, jeder spätere Schritt wird gegen den früheren abgesichert:
```
finally:
    try:
        try:
            [settle]; await _finalize_delivery(...)
        finally:
            if _di_scope is not None: await _teardown_di_scope(...)
    finally:
        try:
            if acquired_locks ...: await _detached_lock_release(...)
        finally:
            self._semaphore.release()
```
- Die Ausnahme (`ExitError`) propagiert danach wie heute an `contain_delivery_errors`. Log-Event und Outcome bleiben gleich, es kommt kein neues Event hinzu.
- Aufrufer bzw. Nachbarpfade: Nur `_process_message` ist betroffen. Der CPU-Modus hat keinen Scope im Elternprozess (`compute/executor.py:9`, `di_scope=None`). Der Kind-Teardown in `compute/child.py:207-212` ist FS-26 (LÜCKE) und bleibt außen vor.
- Bei einem äußeren Abbruch (Shutdown) in Schritt 1–3 laufen Lock-Freigabe und Semaphor-Rückgabe jetzt trotzdem. Nach einer zugestellten `CancelledError` laufen weitere `await`s normal, solange niemand erneut `cancel()` ruft. Der Shutdown wartet ohnehin auf `_detached_releases` (`shutdown.py:127-129`) und räumt danach `_active_handler_locks` ab (`:141-166`).
- Neues Rennen, klein: Läuft die Freigabe des Handlers noch, während der Shutdown die Registry durchgeht (nur wenn `asyncio.wait(_detached_releases, step_timeout)` abläuft), kann derselbe Token zweimal freigegeben werden. Der zweite Versuch meldet `task_lock_lost stage=release`, ein Fehlalarm. Dasselbe Rennen gibt es heute schon für einen Abbruch während `_detached_lock_release`. Abhilfe ist möglich, aber kein Muss: In `release_acquired_locks` den Registry-Eintrag vor dem Release entnehmen statt danach.
- Entscheidungen:
  - E-022: Erneuerung „solange der Handler lebt“. Mit dem Fix endet sie zuverlässig mit dem Handler, das ist konform.
  - Die Semaphor-Invariante (`tests/unit/worker/test_worker.py:418-424`) wird gestärkt.
  - Kein entscheidung-Test bricht. Keiner der Marker-Tests prüft die Reihenfolge im finally (`grep _teardown_di_scope tests/` findet nur den Nachweis).

**B: `contextlib.AsyncExitStack`.** Die Aufräumschritte werden beim Erwerb registriert (Semaphor → Locks → DI → Guard-Settle/Finalize) und laufen in umgekehrter Reihenfolge, jeder auch nach einer Ausnahme des vorigen.
- Robust gegen künftige Schritte, aber ein größerer Umbau.
- Passt gut, falls FS-03 den Semaphor-Erwerb ohnehin in den geschützten Bereich zieht. Dann sollten beide Befunde in einem MR auf B umgestellt werden.
- Gleiche Folgen wie A.

**C: Nur `_teardown_di_scope` fängt `Exception` und loggt `di_scope_exit_failed`.**
- Kleinster Eingriff, deckt aber weder `BaseException` aus Finalizern noch Fehler oder Abbrüche in Schritt 1–2 ab.
- Bringt ein neues Log-Event, das Logging-Katalog und Doku (R18 „feste Event-Namen“) nachziehen müssten.
- Als Ergänzung zu A denkbar, allein unvollständig.

**Verworfen: Lock und Semaphor vor dem DI-Teardown freigeben.** Der Commit im Finalizer liefe dann ohne Mutex. Ein zweiter Halter könnte starten, bevor der geschützte Commit fertig ist. Das bricht den gegenseitigen Ausschluss.

### Offene Nebenfrage (nicht entschieden, nur Abhängigkeit)
„Ack vor DI-Scope-Ende“: Heute bestätigt der Ergebnis-Tail vor dem Scope-Exit, ein scheiternder Commit landet also nach dem Ack (der DI-Plan sagt das Gegenteil).
- Fix A/B ist von dieser Frage unabhängig. Er schützt Lock und Semaphor, egal wo der Teardown später sitzt.
- Abhängigkeit: Entscheidet Georg später „Commit vor Ack, Fehler → Retry“, wandert der Teardown in den try-Teil vor den Tail. Die Vorbedingung `broker.ack.assert_awaited_once()` in beiden FS-04-Nachweisen wird dann falsch und muss mitgeändert werden. Die Kern-Assertions (Lock frei, Semaphor frei) bleiben gültig.

### Empfehlung
- A, oder B zusammen mit FS-03, falls dessen Fix den Semaphor-Erwerb umbaut. Mit FS-03 abstimmen, weil die Codestelle dieselbe ist.
- Tests:
  - xfail und befund-Marker in `tests/unit/worker/test_befund_fs04_dishka_finalizer.py` entfernen (2 Tests).
  - Zusätzlich: (a) `concurrency=1`, scheiternder Finalizer, dann läuft eine zweite Zustellung durch (das ist der Szenariotest aus N-2). (b) `execution_guard.settle` bzw. `maybe_release_unique` wirft eine fremde Ausnahme, Lock und Semaphor sind trotzdem frei. (c) Ein Finalizer wirft `BaseException` bzw. ein äußerer Abbruch trifft den DI-Teardown, der Semaphor wird zurückgegeben (mit Event-Synchronisation, ohne sleep).
- Dateien: `src/swarmq/worker/pipeline.py`, optional `src/swarmq/worker/gates/lock.py` (Registry-Pop vor dem Release).
- Aufwand S (bei B mit FS-03: M). Risiko niedrig bis mittel, weil das finally der Kern der Pipeline ist und Abbruch-Pfade betroffen sind.

---

## FS-05 (P2): `TimeoutError` aus dem Task wird zu `SoftTimeoutError`, auch im CPU-Kind

### Ursache
- `src/swarmq/worker/execution.py:183-187` (`_await_run_result`): `except TimeoutError` fängt jede `TimeoutError` aus `await run_task`, auch ohne gesetzten `soft_timeout`. Das ergibt den Text „soft timeout after Nones“.
- `src/swarmq/worker/compute/child.py:192-197` hat dasselbe Muster.
- Folgen:
  - `retry_exceptions=[TimeoutError]` greift nicht, denn `SoftTimeoutError` ist kein `TimeoutError` (`exceptions.py:257`). Prüfung in `retry.py:108-113`, Ergebnis: DLQ statt Retry.
  - `error_class` und Text sind falsch.
  - Die Eltern-Seite im CPU-Modus (`compute/executor.py:239`) stellt nur `SoftTimeoutError:` voran, sie folgt also dem Kind.
- Bibliotheksverhalten, CPython 3.14.4:
  - `asyncio.wait_for` mit `timeout > 0` ist `async with timeouts.timeout(t): return await fut` (`tasks.py:487-488`). `Timeout.__aexit__` (`timeouts.py`) wandelt nur eine `CancelledError` aus dem eigenen Ablauf in `raise TimeoutError from exc_val` um.
  - Eine `TimeoutError` des Tasks läuft unverändert durch.
  - Das eigene `asyncio.timeout()` des Tasks erzeugt dieselbe Form (`TimeoutError` mit `__cause__ = CancelledError`, verifiziert mit `rw/wf.py`). Eine Unterscheidung über `__cause__` taugt deshalb nicht.

### Optionen
**C: Hülle innerhalb von `wait_for` (empfohlen).**
- Eine kleine Hilfsfunktion (z. B. `run_with_soft_timeout(coro, soft_timeout)` in einem leichten Modul, damit `child.py` sie lazy importieren kann) legt eine innere Coroutine um `run()`. Diese wandelt eine `TimeoutError` des Tasks in eine private Hüll-Ausnahme um. `wait_for` bleibt unverändert außen.
- Eine `TimeoutError`, die aus `wait_for` kommt, ist dann sicher der eigene Ablauf, also `SoftTimeoutError`. Die Hülle wird ausgepackt, das Original mit Traceback weitergeworfen.
- Ohne `soft_timeout` wird nichts umgewickelt, und `except TimeoutError` entfällt. Die Original-Exception läuft in den allgemeinen `except Exception`-Zweig.
- Im IO-Pfad fängt `_await_run_result` dann `SoftTimeoutError` (bzw. eine private Unterklasse, damit ein Task, der selbst `SoftTimeoutError` wirft, nicht als Ablauf zählt). Im Kind wirft die Hilfsfunktion `SoftTimeoutError`, Envelope und `executor.py:239` bleiben unverändert.
- E-045 bleibt wörtlich erhalten: `run()` läuft unter `asyncio.wait_for`, der Abbruch kommt als `CancelledError` im Task an, und nur der eigene Ablauf wird umgewandelt. Das ist genau die Zeile „Die Umwandlung gilt nur für den Soft-Timeout selbst“.
- Kein entscheidung-Test bricht:
  - `test_worker_timeouts.py::test_soft_timeout_fires_when_run_exceeds_limit`, `::test_soft_timeout_triggers_retry` und `::test_soft_timeout_skips_post_execution` prüfen den echten Ablauf.
  - `compute/test_child.py::test_run_job_soft_timeout_becomes_soft_timeout_error` und `compute/test_executor.py::test_soft_timeout_message_mirrors_today` bleiben gültig.
- Abbrüche: Hard-Timeout und Cancel-Monitor brechen `run_task` von außen ab. Dabei steigt der Cancel-Zähler, `Timeout.__aexit__` wandelt nicht um, und die `CancelledError` geht wie heute in den Zweig mit `cancel_reason`. Unverändert.
- Grenzfall: Läuft der Soft-Timeout ab und der Task fängt die `CancelledError` und wirft selbst `TimeoutError`, zählt das als Task-Timeout, nicht als Soft-Timeout. Vertretbar.
- `soft_timeout=0` (wird nicht validiert, `task.py:187`): Der `timeout <= 0`-Zweig von `wait_for` (`tasks.py:475-485`) bricht ab, bevor die Hülle startet, das Ergebnis ist `SoftTimeoutError` wie heute. Dazu kommt wie heute ein „coroutine never awaited“-RuntimeWarning für den `run()`-Coroutine. Das betrifft die Suite (Warnungen als Fehler, U3) nur, falls ein Test `soft_timeout=0` nutzt.

**A: Eigenes `asyncio.timeout()` als Kontextmanager und `cm.expired()`.**
- Am genauesten: Soft-Timeout heißt „unsere Frist lief ab“, auch im Grenzfall oben.
- Ersetzt aber `wait_for` durch `timeout()`. Technisch ist das dasselbe (3.12+), widerspricht jedoch dem Wortlaut von E-045 („läuft `run()` unter `asyncio.wait_for`“). Mit `soft_timeout <= 0` verhält es sich anders (Abbruch erst beim ersten `await` statt vor dem Start).
- Nur zusammen mit einer Präzisierung des Registertexts.

**Verworfen: B, `SoftTimeoutError` erbt von `TimeoutError`.**
- Würde nur den Retry-Filter „reparieren“. Text und `error_class` blieben falsch.
- `retry_exceptions=[TimeoutError]` würde zusätzlich echte Soft-Timeouts wiederholen, eine neue Semantik.
- Ändert die Klassenhierarchie in `agent_spec.py:295` und `errors.json`.

### Mischbetrieb
- Dieselbe Nachricht geht bei einem 0.12.0-Worker in die DLQ und bei einem 0.12.1-Worker in den Retry, je nach Worker. Harmlos.
- Der Envelope zwischen Kind und Eltern stammt aus derselben Installation.
- `error_class` im Ergebnis ist ein String, und 0.12.0-Clients lesen ihn unverändert.

### Empfehlung
- Option C.
- Tests:
  - xfail aus den 4 Nachweisen in `tests/unit/worker/test_befund_fs05_task_timeout_error.py` entfernen.
  - Zusätzlich: (a) eine `TimeoutError`-Unterklasse (wie `aiohttp.ServerTimeoutError`) behält Klasse und `error_class`. (b) Ein abgelaufener Soft-Timeout ergibt weiter `SoftTimeoutError` mit „soft timeout after Xs“ (bestehende Tests decken das ab). (c) Hard-Timeout und Cancel bleiben unverändert (bestehend).
- Dateien: `src/swarmq/worker/execution.py`, `src/swarmq/worker/compute/child.py`, neues Hilfsmodul (z. B. `src/swarmq/worker/soft_timeout.py`).
- Aufwand S. Risiko niedrig.

---

## FS-20 (P3, Doku): `except SoftTimeoutError` in `run()` kann nie greifen

### Ursache
Die Doku beschreibt die verworfene Anforderung R2.4. E-045 sagt ausdrücklich, dass diese Doku zu korrigieren ist („Fehler gegen diese Entscheidung“). Betroffene Stellen:
- `website/reference/errors.md:413-441`: „When raised … cancels the task with this class as the cause“, dazu ein Beispiel mit `except SoftTimeoutError: await checkpoint()`. DE: `errors.de.md:435-465`.
- `website/guides/scheduling-and-retries.md:674-675` („raises `SoftTimeoutError` inside `run()`“). DE: `scheduling-and-retries.de.md:701-702`.
- Weitere Stellen mit derselben falschen Aussage:
  - `website/reference/configuration.md:441` und `configuration.de.md:447` („raises `SoftTimeoutError` inside the task“).
  - Tabellenzeile `errors.md:44` bzw. `errors.de.md:45` („asked to wrap up“ bzw. „sollte aufräumen“).

### Optionen
1. **Doku an E-045 anpassen (einzige zulässige Option).** Inhalt:
   - Bei Ablauf von `soft_timeout` bricht der Worker `run()` ab; im Task kommt `asyncio.CancelledError` an.
   - Aufräumen geht über `try/finally` oder `except asyncio.CancelledError: …; raise`.
   - Der Worker speichert `SoftTimeoutError` als Fehlerergebnis. Es ist retry-fähig und zählt nicht als `TimeoutError` für `retry_exceptions`. `post_execution` entfällt.
   - Wichtig und bisher nirgends dokumentiert: Wer die `CancelledError` **verschluckt** und normal zurückkehrt, erzeugt ein **Erfolgsergebnis**. In 3.14 wirft `wait_for` dann nichts (verifiziert mit `rw/wf.py`: Rückgabe `"partial"`). Das Beispiel muss also `raise` enthalten.
   - `hard_timeout` greift nur, wenn der Task den Abbruch ignoriert oder hinauszögert. Das relativiert den Abstand `hard_timeout > soft_timeout`.
   - Nach FS-05 außerdem: Nur ein abgelaufener `soft_timeout` erzeugt `SoftTimeoutError`; eine eigene `TimeoutError` des Tasks bleibt, was sie ist.
2. **Verworfen: Soft-Timeout als fangbare Ausnahme in den Task werfen.** Widerspricht E-045 (Begründung: technisch nicht lieferbar, die `wait_for`-Umsetzung ist entschieden). Ginge nur mit neuer Entscheidung.

### Folgen
- Kein Code, kein Mischbetrieb.
- `agent_spec.py:295` („when: run“, „retriable: controlled“) bleibt richtig.
- Die Doku wird von keinem Test geprüft. `test_agent_spec.py` liest nur das maschinenlesbare Spec.
- Nach dem Fix den Abschnitt „Überdenken“ in E-045 nachziehen.

### Empfehlung
- Option 1 im selben MR wie FS-05, damit die Doku das neue FS-05-Verhalten beschreibt.
- Tests: kein Pflichttest (Doku). Sinnvoll wäre ein Unit-Test, der festschreibt „`run()` sieht beim Soft-Timeout `CancelledError`, und ein Task, der sie verschluckt, liefert ein Erfolgsergebnis“. Als `@pytest.mark.entscheidung("E-045")` nur mit gleichzeitiger Ergänzung der `tests:`-Liste von E-045, sonst schlägt der Register-Guard an.
- Dateien: die sechs Doku-Dateien oben.
- Aufwand S. Risiko niedrig.

---

## FS-12 (P2): `cancel(task_id)` vor dem Start eines Workflow-Schritts lässt den Workflow hängen

### Ursache
- `src/swarmq/worker/gates/cancel.py:36-160`: Beim gesetzten per-Task-Flag geht das Gate diesen Weg: Cancel-Ergebnis → `clear_cancel_flag` → `finalize_schedfire(terminal)` → `maybe_release_unique` → `ack` → `TASK_CANCELLED`, Ergebnis `Terminal.already_released()`.
- Weder `deps.fail_workflow` noch `advance` wird aufgerufen. Der Workflow bleibt `RUNNING` bis zur TTL, und ein Chord-Body läuft nie.
- Der Nachbarpfad für einen Abbruch während des Laufs fail-markiert dagegen (`results.py:205-223`, `"TaskError", "task was cancelled"`). Ebenso Expiry (`expiry.py:67-71`) und Dead-Letter (`retry.py:271-272`, BZ-13).

### Optionen
**A: Im Cancel-Gate per-Task-Cancel fail-markieren (empfohlen).**
- Nur wenn `workflow_cancel` falsch ist: `if deps.fail_workflow is not None and not await deps.fail_workflow(delivery, ctx, "task was cancelled", True): return Redelivered`.
- Platz: nach der `task_info`-Auflösung und **vor** `store_result`. Gleiche Reihenfolge wie in `RetryHandler.dead_letter`, weil `fail_workflow_step._step_succeeded` (`terminal.py:83-94`) das gespeicherte Ergebnis liest. Der per-Task-Pfad überschreibt ein Erfolgsergebnis bedingungslos (`cancel.py:95-98`). Danach käme der Schutz „Schritt schon erledigt“ zu spät, und ein spätes Duplikat eines erledigten Schritts würde den Workflow kippen.
- Aufrufer bzw. Nachbarpfade:
  - `RetryHandler.fail_workflow` (`retry.py:193-217`) holt `task_info` selbst. Unbekannter Task und FIRE_AND_FORGET bedeuten `redeliver=False`.
  - Scheitert die Markierung transient, stellt `fail_workflow_step` per `redeliver_in_place` neu zu. Das nimmt einen vorhandenen Schedfire-Claim per Kopie mit (`gates/__init__.py:260-285`). Danach ruft `_pre_pipeline_gates` `finalize_schedfire(terminal=False)` auf. Der Claim ist schon vom Kontext genommen, also ein No-op. Konsistent.
  - Das Cancel-Flag bleibt dann stehen (es wird erst danach verkürzt), sodass die Neuzustellung den Zweig wieder sieht (E-020 bleibt erhalten).
- `WorkflowFailure.fail` (`workflow_failure.py:100-121`) ist idempotent: no-op bei terminalem Zustand, Abgleich per CAS bei `_StaleWorkflowState`. Ein Absturz zwischen Markierung und Ack ist deshalb sicher: Die Neuzustellung markiert erneut (no-op), speichert und ackt.
- Entscheidungen:
  - E-040 verlangt genau das (Abbruch per Cancel eingeschlossen). Der Fix erfüllt die Regel. `test_terminal.py::test_step_without_a_success_result_fails_its_workflow` und `gates/test_unknown_task.py::test_unknown_workflow_step_is_routed_and_fails_its_workflow` sind nicht betroffen.
  - E-020 (Flag verkürzen statt löschen) bleibt.
  - E-039 ist nicht berührt.
- Neuer Fehlermodus: Bei dauerhaft scheiternder Markierung (Backend erreichbar für `is_cancelled`, aber `WorkflowStateConflict` in Folge) stellt das Gate neu zu, **bevor** das Delivery-Count-Gate läuft (Reihenfolge Schedfire → Cancel → DeliveryCount, `pipeline.py:429`). Die swarmq-eigene Deckelung greift also nicht. Begrenzt wird das nur durch das `x-delivery-limit` der Quorum-Queue im Broker. Das gilt heute schon für die übrigen Rückstellungen des Cancel-Gates (`defer_backend_unavailable`). Akzeptabel, aber benennen.
- Mischbetrieb: Die CAS-Markierung (`compare_and_store_workflow_state`) gibt es in 0.12.0 schon. Ein 0.12.0-Worker lässt den Workflow wie bisher hängen, sonst gibt es keine Wechselwirkung.

**B: Fail-Markierung beim Client in `cancel(task_id)`.** Verworfen. Der Client kennt die Workflow-Zugehörigkeit eines `task_id` nicht ohne zusätzlichen Lesezugriff. Läuft der Schritt schon, markiert der Laufpfad ohnehin, das ergäbe doppelte Zuständigkeit. Wäre außerdem eine API-Änderung.

### Abgrenzung
- Den Workflow-Cancel-Zweig (`workflow_cancel=True`, `handle.cancel()`) **nicht** anfassen. Dort wäre ein `failed` falsch. Erwartet ist `cancelled` (FS-32, LÜCKE, nicht Teil des Plans).
- Sind sowohl das per-Task-Flag als auch das Workflow-Flag gesetzt, gewinnt der per-Task-Pfad (`failed`). Das ist vertretbar, kurz dokumentieren.

### Empfehlung
- Option A. Doku: in `website/guides/workflows.md` (Failure modes, ca. :229-238) eine Zeile „ein Schritt wird per `cancel(task_id)` abgebrochen (vor oder während des Laufs) → `failed`, `TaskError` mit "task was cancelled"“. DE entsprechend.
- Tests:
  - xfail in `tests/unit/worker/test_befund_fs12_cancel_before_start.py` entfernen.
  - Zusätzlich: (a) transient scheiternde Markierung → `Redelivered`, kein Ack, kein Cancel-Ergebnis, Flag nicht verkürzt. (b) Workflow-Cancel-Pfad ruft `fail` nicht. (c) Schritt bereits erfolgreich (Erfolgsergebnis gespeichert) → keine Markierung (`workflow_fail_skipped_step_done`). (d) unbekannter Task als Schritt → Markierung ohne Neuzustellung.
- Dateien: `src/swarmq/worker/gates/cancel.py`, Doku `website/guides/workflows*.md`.
- Aufwand S. Risiko niedrig.

---

## FS-13 (P2): `unique_until="start"`: Terminales Ende vor dem Start gibt das Flag nicht frei (fünf Pfade)

### Ursache
- `src/swarmq/worker/finalize.py:121-126`: `if mode == "start": return` beruht auf der Annahme „schon beim Pickup freigegeben“.
- Die Freigabe läuft aber erst in `pipeline.py:639` (`_release_unique_on_start`), nach Guard, Expiry, Rate-Limit und Lock (E-034). Alle terminalen Pfade davor geben deshalb nicht frei:

| Pfad | Stelle | Freigabe über |
|---|---|---|
| max_age | `gates/expiry.py` → `Terminal.release_only()` | finally `pipeline.py:490-491` |
| Pre-Start-Cancel | `gates/cancel.py:138` | direkt |
| Ende der Lock-Wartezeit, Render-Fehler | `gates/lock.py` → `RetryHandler.dead_letter` | `retry.py:318`; DLX-Rückfall `lock.py:510`, `:537` |
| unbekannter Task | `gates/unknown_task.py` → `dead_letter` | `retry.py:318`; DLX-Rückfall `unknown_task.py:108` |
| Delivery-Limit | `gates/delivery_count.py:115` → `dead_letter` | `retry.py:318` |

- E-008 (unbekannter Task) sagt „Unique-Freigabe“ ausdrücklich zu und wird im start-Modus also ebenfalls verletzt.

### Optionen
**A: Merker im Kontext (empfohlen, wie MA-4 vorgeschlagen).**
- `GateContext` bekommt ein Feld, z. B. `start_release_reached: bool = False`. `_release_unique_on_start` setzt es, sobald der Startpunkt erreicht ist, unabhängig von Modus und Erfolg des Releases.
- `maybe_release_unique` erhält statt `task_id` den `ctx`; die Signatur `GateDeps.maybe_release_unique` (`gates/__init__.py:149`) wird zu `(delivery, ctx, task_info)`. Im start-Modus No-op **nur**, wenn der Merker gesetzt ist, sonst Freigabe wie im completion-Modus. Das ist owner-geprüft über `unique_release.lua` (GET == task_id → DEL).
- Aufrufer, die angepasst werden: `cancel.py:138`, `lock.py:510`, `lock.py:537`, `unknown_task.py:108`, `retry.py:318`, `pipeline.py:270-280` und `:491`.
- Tests mit `maybe_release_unique=AsyncMock()` in GateDeps-Fabriken (rund 12 Dateien) brechen nicht, da `AsyncMock` jede Signatur nimmt. Die Assertions dort sind argumentlos (`assert_awaited_once()`).
- Entscheidungen:
  - **E-034**: Die Lage der Freigabe bleibt (nach Rate-Limit und Lock), und Zurückstellungen (`Redelivered`) geben weiter nicht frei. `test_start_mode_rate_limited_keeps_flag_for_redelivery` bleibt grün.
  - Die Einmal-Freigabe (`test_start_mode_releases_at_pickup`, `call_order.count("release") == 1`) bleibt grün, weil der Merker auf dem Erfolgspfad die zweite Freigabe unterdrückt. Ebenso nach dem Start bei erschöpften Retries (`dead_letter` → No-op), das entspricht „Nach dem Start gibt es keine zweite Freigabe“.
  - `test_pipeline.py::test_gates_run_in_fixed_order_and_ack_comes_last` (genau ein `release_unique`) und `test_worker.py:2742-2750` (Override „start“, genau ein Release) bleiben grün.
  - **E-008** wird erfüllt.
  - E-043 und E-031 sind nicht berührt.
- Restrisiko, entspricht dem completion-Modus: Gibt es zwei Kopien **derselben** `task_id` (Neuzustellung nach unbestimmtem Publish, Duplikat), kann das terminale Ende der einen Kopie das Flag freigeben, während die andere noch zurückgestellt wartet. Ein neues `schedule()` käme dann durch. Das gleiche Fenster hat der completion-Modus heute. Der Owner-Check schützt nur gegen fremde `task_id`s.
- Retry-Kopien im start-Modus: Der Merker ist pro Zustellung (neuer `ctx`). Läuft eine Retry-Kopie vor ihrem Start ab, gibt sie owner-geprüft mit derselben `task_id` frei. Hat inzwischen ein neues `schedule()` das Flag (andere `task_id`), ist das ein No-op. Unschädlich.

**B: start-No-op streichen, immer owner-geprüft freigeben.** Verletzt E-034 („genau einmal“; `test_start_mode_releases_at_pickup` zählt zwei Releases). Unzulässig ohne neue Entscheidung.

**C: Freigabe beim Abholen (wie es die Doku sagt).** Hebt ADV-002/E-034 auf. Unzulässig. Die Doku `website/guides/locking-and-dedup.md:215-217` („releases the instant a worker picks up“) ist dagegen ungenau. Mitkorrigieren: „sobald der Task startet; endet die Nachricht vorher terminal, wird ebenfalls freigegeben“.

### Mischbetrieb
Keiner. Das Skript `unique_release.lua` bleibt unverändert, der 0.12.1-Worker gibt nur öfter frei. Ein 0.12.0-Worker lässt das Flag wie bisher bis `unique_for` stehen.

### Wechselwirkungen
- FS-12: gleiches Gate, gleicher Block. Zusammen oder direkt nacheinander.
- FS-17: Dessen `release_only` gibt im start-Modus erst mit FS-13 frei.
- FS-03: Der Schedfire-Pfad ist nicht berührt (das Flag ist getrennt vom Fire-Claim).

### Empfehlung
- Option A.
- Tests:
  - xfail aus `tests/unit/worker/test_befund_fs13_unique_until_start.py` entfernen (5 Parameterfälle).
  - Zusätzlich: (a) start-Modus, Task läuft und scheitert terminal (Retries erschöpft) → genau **ein** Release (der beim Start). (b) Lock- bzw. Unknown-DLX-Rückfall im start-Modus → Freigabe. (c) Rate-Limit-Requeue im start-Modus → keine Freigabe (bestehender E-034-Test).
- Dateien: `src/swarmq/worker/finalize.py`, `pipeline.py`, `gates/__init__.py` (GateContext, GateDeps), `gates/cancel.py`, `gates/lock.py`, `gates/unknown_task.py`, `retry.py`, Doku `locking-and-dedup*.md`. Nach dem Fix den Abschnitt „Überdenken“ in E-034 nachziehen.
- Aufwand M (Signatur über sieben Aufrufstellen). Risiko niedrig bis mittel.

---

## FS-17 (P3): Expiry-Gate wertet gescheiterten Ack nach liegender DLQ-Kopie als Fehlschlag

### Ursache
- `src/swarmq/worker/gates/expiry.py:118-132` ruft `deps.broker.dead_letter(...)` direkt auf und fängt `BrokerConnectionError`. `SettleFailedAfterPublish` ist eine Unterklasse davon (`exceptions.py:81`), also landet auch dieser Fall in `redeliver_in_place` und `Terminal.never_release()`.
- Die übrigen DLQ-Pfade nutzen `dead_letter_copy` (`gates/__init__.py:304-315`). Dort gilt `SettleFailedAfterPublish` als terminal, wie es E-043 verlangt.
- Folgen mit Ausführungsschutz: `GuardedBroker.dead_letter` setzt bei `SettleFailedAfterPublish` schon den Done-Marker (`guarded_broker.py:51-61`). Das Redeliver danach (`GuardedBroker.redeliver` → Lease schon genommen → `nack` auf dem toten Kanal) scheitert typischerweise mit `task_expired_nack_also_failed`. Die Neuzustellung wird als `done` übersprungen, das Unique-Flag hängt bis `unique_for`, und die Logzeilen sind irreführend.

### Optionen
**A: `dead_letter_copy` verwenden (empfohlen).**
- `await dead_letter_copy(deps, delivery, dict(delivery.headers or {}))` im bestehenden `try`. Bei `SettleFailedAfterPublish` gibt es `True` und `dlq_ack_failed_after_publish`, danach `Terminal.release_only()`. Andere `BrokerConnectionError` wie heute.
- Entscheidungen:
  - E-043 wird erfüllt. Die Tests `test_retry.py::test_dead_letter_with_failed_ack_after_publish_is_terminal` und `gates/test_lock_dlq_ack_failed.py::*` sind nicht betroffen.
  - E-031 (Fehlerergebnis trotz `result_ttl=0`) bleibt, die Speicherlogik `expiry.py:84-104` wird nicht angefasst. Der falsche Kommentar `:81-83` lässt sich im selben MR korrigieren, so verlangt es E-031.
- Ohne Ausführungsschutz (`SWARMQ_EXECUTION_GUARD=false`) läuft die Neuzustellung des Originals erneut ins Expiry-Gate: zweite DLQ-Kopie, zweites `TASK_EXPIRED`. Das ist die von E-043 akzeptierte Folge („Mit `SWARMQ_EXECUTION_GUARD=false` kann die Neuzustellung erneut verarbeitet werden“).

**B: Expiry auf `RetryHandler.dead_letter` umstellen.** Verworfen. Das ändert Signal (`TASK_DLQ` statt `TASK_EXPIRED`), Fehlerergebnis-TTL (`DEFAULT_RESULT_TTL_S` statt `config.result_ttl`) und Workflow-Reihenfolge, also mehr Verhalten als nötig.

### Wechselwirkungen
- FS-10 ändert, je nach Fixort, dieselbe Zeile (Header der Expiry-Kopie ohne Broker-Zähler). Filtert FS-10 in `dead_letter_copy` oder im Broker-Adapter, bekommt der Expiry-Pfad das mit Option A automatisch. Filtert FS-10 nur in `RetryHandler.dead_letter`, bleibt Expiry ungedeckt (der FS-10-Nachweis `test_fs10_expired_dlq_copy_drops_broker_counters` würde das melden). Reihenfolge: FS-17 vor oder mit FS-10.
- FS-13: Im start-Modus gibt `release_only` erst mit FS-13 frei.

### Empfehlung
- Option A.
- **Einen Nachweis-Test gibt es nicht.** Nach AGENTS.md zuerst einen roten Test schreiben: `broker.dead_letter` wirft `SettleFailedAfterPublish` → kein `redeliver`/`nack`, `release_unique(hash, task_id)` wird aufgerufen (completion-Modus), Log `dlq_ack_failed_after_publish` statt `task_expired_dlq_publish_failed`. Optional ein zweiter Fall mit Ausführungsschutz: Done-Marker gesetzt.
- Ein Marker `entscheidung("E-043")` für den neuen Test geht nur mit Ergänzung der `tests:`-Liste von E-043.
- Dateien: `src/swarmq/worker/gates/expiry.py`, neuer Test z. B. `tests/unit/worker/gates/test_expiry_dlq_ack_failed.py`.
- Aufwand S. Risiko niedrig.

---

## FS-19 (P3): Nicht ganzzahlige Header `x-swarmq-retry-count` / `x-swarmq-rate-limit-count`

### Ursache
- `src/swarmq/worker/retry.py:116-119`: `retry_count = delivery.headers.get(HEADER_RETRY_COUNT, 0)`, danach `retry_count >= task_info.max_retries`. Mit `"3"` (RabbitMQ-UI-Republish, Header als String) wirft das `TypeError`. Der Task ist zu diesem Zeitpunkt schon gelaufen.
- `src/swarmq/worker/gates/rate_limit.py:86`: `int(original_headers.get(...))` außerhalb jedes `try`. Mit nicht numerischen Werten gibt es `ValueError`.
- Beide Ausnahmen laufen aus dem try von `_process_message` in das finally mit Default-Outcome `Terminal.release_in_finally()` (`pipeline.py:637`):
  - Es wird der **Schedfire-Completed-Marker** geschrieben und das Unique-Flag freigegeben, und zwar auf eine **unbestätigte** Nachricht.
  - Danach gibt es kein Broker-Verb, nur `delivery_handler_failed`. Die Nachricht hängt bis `consumer_timeout`.
  - Je Runde läuft der Task erneut (Retry-Pfad), bis zum Delivery-Limit.
- Die Regel „kaputter Header zählt wie keiner“ gilt bereits an drei Stellen, aber verschieden streng:
  - `execution_guard.retry_count_of` (`:57-60`): nur `int > 0`, `"3"` → 0
  - `gates/__init__._as_count` (`:204-210`): `int()`, `"3"` → 3
  - `gates/lock._as_int` (`:118-124`): wie `_as_count`
  - `decode.attempt_number` (`:75-86`): nur `int`
- **Nachbarpfade mit derselben Klasse:**
  - `pipeline.py:385` (`_build_message_dict`) reicht den Rohwert als `retry_count` an Middleware und Dishka-`TaskContext` (`execution.py:116-121`) weiter. Ein String landet beim Nutzer.
  - CPU-Modus `compute/executor.py:127`: `int(message_dict.get("retry_count", 0))` wirft bei nicht numerischem Wert `ValueError`, und zwar **vor** dem Lauf. Das ist derselbe Hänger wie im Retry-Pfad, nur im CPU-Modus.
  - `retry.py:134` setzt `message_dict["retry_count"] = retry_count + 1`.

### Optionen
**A: Retry-Zähler einmal normalisieren, über `retry_count_of` (strikt; empfohlen).**
- Ein Helfer (am besten `retry_count_of` aus `execution_guard.py`, oder dorthin verschoben, wo `decode` ihn findet) wird in `retry.py:116`, `pipeline.py:385` und damit `compute/executor.py:127` benutzt.
- `"3"` zählt dann wie kein Header (0). Das passt zu `swarmq dlq retry`, das den Zähler bewusst zurücksetzt („operator's try-again intent“, `broker/rabbitmq/admin.py:400-402`). Ebenso zu `attempt_number` und zur Lease des Ausführungsschutzes (`GuardLease.retry_count`), sodass Superseded-Vergleiche (`guarded_broker.py:79-81`) dieselbe Zahl sehen.
- Rate-Limit-Zähler über `_as_count`. Er dient nur der Diagnose und ist in keiner Entscheidung festgelegt.

**B: Lenient über `_as_count` (`"3"` → 3).**
- Ein UI-Replay mit `"3"` geht nach dem nächsten Fehler sofort wieder in die DLQ.
- Die Guard-Lease liest dieselbe Zustellung als 0. Die Superseded-Logik bleibt korrekt (der neue Zähler 4 ist größer als 0), aber das Logfeld `attempt` fehlt, während die Retry-Logik mit 3 rechnet. Uneinheitlich.

### Entscheidungen
- Keine Regel legt fest, wie ein String-Retry-Zähler zu lesen ist. E-009 (Replay umgeht den Done-Marker über `x-swarmq-replay-id`) und E-007 (Lock-Wartezeit, Generation über `_as_int`) sind nicht berührt.
- Die Wahl zwischen A und B betrifft das Retry-Budget nach einem manuellen UI-Replay. Das ist eher technisch als Produktfrage, sollte aber im Plan sichtbar gemacht werden.
- Die Uneinheitlichkeit `_as_int` in `lock.wait_generation` (`"3"` → `3:`) bleibt ein kosmetischer Rest. Eine Änderung dort würde die Generation laufender Wartezeiten im Mischbetrieb verschieben. Nicht anfassen.

### Mischbetrieb
Nur Lesen. 0.12.0 schreibt immer `int`, ein 0.12.0-Worker hinge an denselben Nachrichten wie bisher.

### Empfehlung
- Option A.
- **Nachweis-Tests gibt es nicht.** Zuerst rot schreiben:
  - (a) IO: Task scheitert, Header `x-swarmq-retry-count: "1"` → ein Broker-Verb (`retry_later` oder `dead_letter`), kein `delivery_handler_failed`.
  - (b) Rate-Limit gesperrt, `x-swarmq-rate-limit-count: "abc"` → `retry_later` mit Zähler 1.
  - (c) CPU-Executor mit nicht numerischem Retry-Header → Job wird gebaut, Zähler 0.
  - (d) Middleware bzw. `TaskContext.retry_count` ist ein `int`.
- Dateien: `src/swarmq/worker/retry.py`, `src/swarmq/worker/gates/rate_limit.py`, `src/swarmq/worker/pipeline.py`, gegebenenfalls `src/swarmq/worker/compute/executor.py`, Ort des Helfers.
- Aufwand S. Risiko niedrig.

---

## FS-27 (P3): `validate_namespace("SQ")` schlägt das verbotene `'sq'` vor

### Ursache
`src/swarmq/keyspace.py:40-46`: Der Hinweis „write it in lowercase: 'sq'“ wird angehängt, wenn `value.lower()` das Muster erfüllt. Den Ausschluss `value.lower() == "sq"` prüft die Bedingung nicht. Das betrifft auch `Sq` und `sQ`.

### Optionen
1. **Bedingung ergänzen (empfohlen):** `… and value.lower() != "sq"`.
2. Für `SQ` eine eigene Meldung („'sq' is reserved“). Unnötig, die Basismeldung sagt schon „not 'sq'“.

### Folgen
- Aufrufer `config.py:163` und `config_validation.py:141-144` reichen die Meldung als `ConfigurationError` durch (Exit 78).
- E-012: Die Tests `test_keyspace.py::test_invalid_namespace_is_rejected` (prüft nur `match="namespace"`, inklusive `SQ`) und `test_namespace_lowercase.py::*` (`App1`, `APP`, `billing-Prod`, weiter mit Hinweis) bleiben grün.
- Kein Laufzeit- oder Mischbetriebsthema.

### Empfehlung
- Option 1.
- Zuerst ein roter Test (fehlt): `SQ`/`Sq` → `ValueError`, Meldung enthält nicht „write it in lowercase“.
- Datei: `src/swarmq/keyspace.py`, Test in `tests/unit/test_namespace_lowercase.py`.
- Aufwand S. Risiko niedrig.

---

## FS-28 (P3): Meldung „is FIRE_AND_FORGET“ bei AT_LEAST_ONCE mit `result_ttl=0`

### Ursache
- `src/swarmq/worker/results.py:112-145` (`_finish_without_store`) greift bei jedem aufgelösten `result_ttl == 0` (`finish` :296-298).
- Das ist der Fall bei:
  - FIRE_AND_FORGET ohne TTL
  - ausdrücklichem `TaskInfo(result_ttl=0)`
  - globalem `config.result_ttl=0` (über `resolve_result_ttl`)
- Der Text :132 nennt immer „is FIRE_AND_FORGET“.

### Optionen
1. **Text nach Ursache (empfohlen).** FAF behält den heutigen Text. Sonst etwa `f"task {name!r} stores no result (result_ttl=0) and cannot advance a workflow"`. Fehlertyp `WorkflowValidationError` und Verhalten bleiben (E-039).
2. Zusätzlich `result_ttl=0` bei `Task.s()` ablehnen. **Offene Produktfrage** (E-039, „Überdenken“), nicht Teil des Fixes.

### Folgen
- Der E-039-Test `test_results.py::test_result_ttl_zero_stores_nothing_and_fails_faf_workflow_step` prüft `"FIRE_AND_FORGET" in …` für einen FAF-Task. Er bleibt grün, solange der FAF-Text bleibt.
- Andere Tests mit `TailNoStoreTask` (:246-255, :334-340) prüfen den Text nicht.
- Doku `website/guides/workflows.md:238` unterscheidet beide Ursachen schon. Kein Mischbetriebsthema, die Meldung wird nur im Workflow-Zustand gespeichert.
- Nebenbeobachtung, kein Fix nötig: Auch ein **abgebrochener oder gescheiterter** Schritt mit `result_ttl=0` landet hier und meldet `WorkflowValidationError` statt „task was cancelled“ bzw. des Fehlers. Laut E-039 ist das entschieden, der neue Text deckt diesen Fall mit ab.

### Empfehlung
- Option 1.
- Zuerst ein roter Test (fehlt): `TailNoStoreTask` mit Workflow-Header → `fail`-Text enthält `result_ttl=0` und nicht `FIRE_AND_FORGET`. Optional derselbe Fall über globales `config.result_ttl=0`.
- Datei: `src/swarmq/worker/results.py`.
- Aufwand S. Risiko niedrig.

---

## Zusammenfassung

| ID | Empfehlung | Aufwand | Risiko | Abhängigkeiten |
|---|---|---|---|---|
| FS-04 | try/finally-Kaskade im Pipeline-finally (oder AsyncExitStack zusammen mit FS-03) | S (M mit FS-03) | niedrig–mittel | FS-03 (gleiche Stelle); offene Frage „Ack vor DI-Scope-Ende“ (nur Testvorbedingung) |
| FS-05 | Task-`TimeoutError` innerhalb von `wait_for` umhüllen, nur der eigene Ablauf wird `SoftTimeoutError`; IO und Kind | S | niedrig | E-045 (wörtlich eingehalten); mit FS-20 |
| FS-20 | Doku (6 Dateien) auf E-045: CancelledError im Task, `raise` nötig, Verschlucken ergibt Erfolg | S | niedrig | nach bzw. mit FS-05 |
| FS-12 | Cancel-Gate: per-Task-Cancel → `fail_workflow(..., "task was cancelled")` vor `store_result` | S | niedrig | E-040 (erfüllt); FS-13 (gleiche Stelle); FS-32 (Workflow-Cancel) außen vor |
| FS-13 | Merker „Startpunkt erreicht“ im `GateContext`; `maybe_release_unique(delivery, ctx, task_info)` | M | niedrig–mittel | E-034 (Einmal-Freigabe bleibt), E-008 (erfüllt); FS-12, FS-17 |
| FS-17 | Expiry über `dead_letter_copy` (SettleFailedAfterPublish terminal) | S | niedrig | E-043 (erfüllt); FS-10 (gleiche Zeile); FS-13 für start-Modus; neuer roter Test nötig |
| FS-19 | Retry-Zähler über `retry_count_of` normalisieren (inklusive `_build_message_dict` und CPU-Executor), Rate-Limit über `_as_count` | S | niedrig | keine Entscheidung; neuer roter Test nötig |
| FS-27 | Hinweis nur, wenn `value.lower() != "sq"` | S | niedrig | E-012 (unberührt); neuer roter Test |
| FS-28 | Meldung nach Ursache (FAF vs. `result_ttl=0`) | S | niedrig | E-039 (Verhalten bleibt); neuer roter Test |

**Fixes, die eine offene Produktfrage oder eine Entscheidung berühren:**
- **FS-04:** offene Frage „Ack vor DI-Scope-Ende“. Wird sie später entschieden, ändert sich die Ack-Vorbedingung der FS-04-Nachweise. Der Fix selbst ist neutral.
- **FS-05 / FS-20:** E-045. Option C hält den Wortlaut ein. Option A (`asyncio.timeout`) bräuchte eine Präzisierung des Registertexts. FS-20 korrigiert die Doku so, wie E-045 es verlangt. Nach dem Fix den Abschnitt „Überdenken“ in E-045 aktualisieren.
- **FS-12:** E-040, wird durch den Fix erfüllt. Grenzt an FS-32 (Status `cancelled` nach `handle.cancel()`, LÜCKE), der Workflow-Cancel-Zweig bleibt bewusst unverändert.
- **FS-13:** E-034 (Einmal-Freigabe und Lage bleiben, Tests bleiben grün) und E-008 (Unique-Freigabe beim unbekannten Task, wird erfüllt). Abschnitt „Überdenken“ in E-034 nachziehen.
- **FS-17:** E-043, wird erfüllt. Ein eventueller Marker-Test braucht einen Registereintrag.
- **FS-19:** keine Entscheidung. Die Wahl strikt/lenient (Retry-Budget nach UI-Replay) als bewusste technische Festlegung in den Plan schreiben.
- **FS-28:** E-039 (Verhalten bleibt). „`result_ttl=0` bei `Task.s()` ablehnen“ ist eine offene Produktfrage und nicht Teil des Fixes.
- **FS-27:** E-012 nur am Rand (Tests bleiben grün).

**Belege, dass eine Entscheidung nicht funktioniert:** keine gefunden. Ergänzung zu E-045 für die Doku: Ein Task, der die `CancelledError` des Soft-Timeouts verschluckt, liefert in Python 3.14 ein **Erfolgsergebnis** (`wait_for` wirft dann nicht). Das ist dort nicht als Folge genannt.
