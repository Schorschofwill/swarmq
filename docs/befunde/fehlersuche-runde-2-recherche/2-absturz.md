# Fehlersuche 2: Absturz/Abbruch zwischen zwei Schritten (Crash-Konsistenz)

Stand: Worktree `swarmq-fehlersuche` (v0.12.0, ac989a3), 2026-10-13. Nur gelesen. K-1 und K-5 sind zusätzlich mit Wegwerf-Proben im Scratchpad belegt (echte `Worker._handle_message`-Pipeline, Broker und Backend gemockt): `fs/k_shutdown_cancel.py`, `fs/k_run2.py` und `fs/k_sema.py`, Aufruf aus dem Worktree mit `PYTHONPATH=. uv run --extra test python <datei>`. Bekannte BZ-Befunde sind nicht wiederholt.

Übersicht: **3 × P1, 2 × P2.**

---

## K-1 (P1): Ein Task, der den Drain überdauert, wird beim Deploy als Fehler behandelt statt neu zugestellt

**Fundstellen**
- `src/swarmq/worker/execution.py:188-200`: `except asyncio.CancelledError` fängt auch den äußeren Abbruch des Handlers. Ohne `cancel_reason` landet er im Zweig „hard timeout“.
- `src/swarmq/worker/compute/executor.py:199-216`: im CPU-Modus dasselbe, Meldung `compute child cancelled during worker drain`. Absicht laut Plan `2026-09-03-001` KTD2.
- `src/swarmq/worker/shutdown.py:113-118`: `task.cancel()` am Ende des Drains, danach höchstens 1 s Wartezeit.
- `src/swarmq/worker/retry.py:119-120, 169-192, 276-301`: `max_retries` erschöpft → `dead_letter`. Dort kommen erst die Workflow-Fehlmarkierung und das Fehlerergebnis (mit `notify_result`), danach der DLQ-Publish.
- `src/swarmq/task.py:93`: Default `max_retries = 0`.
- Doku, `website/explanation/delivery-semantics.de.md` (Abschnitt Shutdown-Budget): „Ein Task, der länger läuft als der Drain, wird abgebrochen und neu zugestellt“.

**Szenario (normaler Deploy, SIGTERM).** Ein Task läuft länger als `drain_until` (Default rund 25 s).
1. `_drain_in_flight` bricht den Handler ab.
2. `_await_run_result` schluckt den `CancelledError` und baut daraus `RuntimeError("hard timeout after Nones")`. `TASK_FAILED` und `on_error` feuern.
3. Mit dem Default `max_retries=0` folgt `RetryHandler.dead_letter`: `advancer.fail()` markiert den Workflow als **FAILED**, `store_result` schreibt ein **Fehlerergebnis**, `notify_result` weckt wartende `get_result()`-Aufrufer.
4. Der DLQ-Publish läuft nach der Publish-Endfrist (`_set_publish_deadline(drain_until)`) mit Budget 0 und scheitert. Es folgt `redeliver` (`nack`).
5. Die Nachricht läuft auf einem anderen Worker erneut und meist erfolgreich. Das Erfolgsergebnis überschreibt das Fehlerergebnis, `advance()` sieht aber einen terminalen Workflow und tut nichts.

**Folgen**
- Der Workflow bleibt dauerhaft FAILED, obwohl sein Schritt erfolgreich war.
- Ein Client, der im Fenster wartet, bekommt `TaskError("hard timeout after Nones")` für einen Task, der danach erfolgreich läuft.
- Mit `max_retries > 0` wird ein Retry verbraucht (Retry-Zähler +1, sofern der Publish noch durchgeht), oder es gibt ERROR `retry_publish_failed` mit anschließendem `nack`.
- Gelingt der DLQ-Publish doch, etwa mit einem Fremdbroker ohne Frist (so in der Probe), liegt der Task in der DLQ, `TASK_DLQ` feuert und das Unique-Flag wird freigegeben.

Probe-Ausgabe (`fs/k_shutdown_cancel.py`, `max_retries=0`): `dead_letter(... 'x-swarmq-traceback': 'hard timeout after Nones')`, `store_result(t1, is_error=True ...)`, `release_unique(...)`. Mit `max_retries=3`: `retry_later` mit Retry-Zähler 1. Mit scheiterndem DLQ-Publish (`fs/k_run2.py`): Fehlerergebnis gespeichert **und** `redeliver`.

**Warum die Tests es nicht finden.** Die Drain-Tests (`test_lifecycle.py::test_drain_timeout_cancels_remaining_tasks`, `test_befund_bz18_cleanup_after_grace.py`) brechen nackte `asyncio.sleep`- oder `Event.wait`-Tasks ab, nie einen echten `_handle_message`. Die e2e-SIGTERM-Tests (`test_worker_shutdown_load.py`) lassen alle Tasks innerhalb des Drains fertig werden.

**Nachweistest (Unit, deterministisch).** Worker-Fixture mit `shutdown_timeout=0`, ein Task `run()` wartet auf ein `Event`, das nie gesetzt wird. Erst `_on_message`, dann auf ein `started`-Event warten, dann `_request_shutdown()` und `_execute_shutdown()`. Erwartet: weder `store_result` mit `is_error`, noch `dead_letter`, noch `retry_later`, noch `advancer.fail`, aber `redeliver` bzw. keine Settle-Aktion und kein Ergebnis. Variante mit `x-swarmq-workflow-id` und einem Advancer-Mock: `fail` darf nicht aufgerufen werden.

**Vorschlag.** Den äußeren Abbruch erkennen (`asyncio.current_task().cancelling() > 0` bzw. ein Drain-Flag) und als eigenes Outcome „abgebrochen, unbestätigt“ führen. Dabei Schedfire-Claim und Guard nicht-terminal freigeben, das Unique-Flag behalten und kein Ergebnis schreiben. KTD2 wollte genau diesen Terminal-Default vermeiden; heute geht die Nachricht dafür in den Fehlerpfad.

**Konfidenz:** hoch (Probe). Offen ist nur die Produktfrage, ob „Fehlerpfad“ gewollt war. Die Nutzer-Doku verspricht das Gegenteil.

---

## K-2 (P1): Ein Cron- oder delay/eta-Fire geht still verloren, wenn der Worker mitten im Lauf stirbt

**Fundstellen**
- `src/swarmq/worker/gates/schedfire.py:171-199`: Ist der In-Flight-Claim fremd, folgen `ack` und `Terminal.never_release()`.
- `src/swarmq/worker/pipeline.py:429` (Schedfire-Gate vor allem anderen) gegenüber `:633` (Ausführungsschutz erst danach).
- `scheduler/fire.py:124-127`: auch Oneshots (`schedule(delay=/eta=)`) tragen `x-swarmq-schedfire`.

**Szenario**
1. Worker W1 beansprucht den Fire (`sched:inflight:<key>`, TTL `max(300 s, hard_timeout + 30)`) und führt ihn aus.
2. W1 stirbt per SIGKILL: OOM-Killer, Node-Ausfall, SIGKILL des Supervisors bzw. von K8s.
3. RabbitMQ stellt die unbestätigte Nachricht sofort an W2 zu.
4. Das Schedfire-Gate von W2: Completed fehlt, `claim_fire` → 0, weil der Claim W1 gehört. Damit `schedfire_inflight_duplicate_acked` auf INFO und ein **Ack**.
5. Die einzige Kopie des Fires ist weg. Der Docstring („the in-flight key expires and the next redelivery re-runs“) trifft nicht zu, denn es gibt keine nächste Zustellung.

**Folgen**
- Cron-Lauf fällt aus.
- Bei `schedule(delay=...)` läuft `get_result(X)` in den Timeout.
- Ein Unique-Flag des Fires bleibt bis `unique_for` stehen (`never_release`).

Für normale Tasks deckt genau diesen Fall der Ausführungsschutz ab: Er stellt zurück, bis der Running-Marker als Waise abläuft. Das Schedfire-Gate liegt aber davor und bestätigt vorher.

**Warum die Tests es nicht finden.**
- `tests/unit/worker/gates/test_schedfire.py` prüft „fremder Claim → Ack“ als gewolltes Verhalten.
- Der e2e-SIGKILL-Test (`test_worker_shutdown_load.py`, Szenario 2) nutzt normale Tasks ohne `x-swarmq-schedfire`.
- Die Doku (`delivery-semantics.de.md:186-197`) nennt Verlust nur für „Broker nimmt Kopie nicht an“.

**Nachweistest.**
- Integration über die Testports: `claim_fire(key, owner="W1")` direkt im Store setzen (simulierter toter W1, nie finalisiert). Dann dieselbe Fire-Nachricht als Broker-Redelivery (`redelivered=True`) an einen echten Worker geben. Erwartet: zurückgestellt bzw. nach Claim-Ablauf ausgeführt. Ist: Ack ohne Lauf.
- e2e: Variante von Szenario 2 mit Cron-Task und `crash_worker`, dann prüfen, dass ein Ergebnis oder Lauf eintrifft.

**Vorschlag.** Ist der Claim fremd und die Zustellung eine Broker-Redelivery (`message.redelivered` oder `x-delivery-count > 0`), nicht bestätigen, sondern wie beim Guard zurückstellen, bis der Claim abläuft. Ein frisches Doppel-Fire des Leaders kommt nie als Redelivery und bleibt ein Duplikat.

**Konfidenz:** hoch (Code eindeutig, Pfad deterministisch).

---

## K-5 (P1): Beim Deploy gehen Cron-Fires verloren, die am Semaphor warten

**Fundstellen**
- `src/swarmq/worker/pipeline.py:571-575`: `_pre_pipeline_gates` (mit Schedfire-Claim) und `await self._semaphore.acquire()` liegen **vor** dem `try/finally`.
- `src/swarmq/worker/prefetch.py`: K Queues bedeuten K × `concurrency` unbestätigte Nachrichten. Prioritätsbänder zählen als eigene Queues, Handler warten also regulär am Semaphor.

**Szenario**
1. Alle Slots sind mit langen Tasks belegt. Eine Cron-Nachricht hat das Schedfire-Gate passiert (Claim, TTL ≥ 300 s) und wartet am Semaphor.
2. SIGTERM. Der Drain läuft ab, und `_drain_in_flight` bricht auch die wartenden Handler ab.
3. Der `CancelledError` aus `acquire()` verlässt `_process_message` ohne `finally`. Der Claim wird weder freigegeben noch finalisiert, die Nachricht bleibt unbestätigt.
4. Mit dem Kanal-Schluss stellt der Broker sie neu zu. Der nächste Worker sieht den lebenden Claim und bestätigt sie als Duplikat (K-2-Mechanik). Der Fire ist verloren.

Das ist die **deterministische** Variante von K-2 bei jedem Deploy mit vollem Worker.

Probe-Ausgabe (`fs/k_sema.py`, `concurrency=1`, ein Slow-Task, eine Cron-Nachricht dahinter): `claim_fire('cron:x:2', ...)` erfolgt, danach `release_fire_claim []`, `mark_fire_completed []`, Nachricht nicht gesettled.

**Warum die Tests es nicht finden.** Kein Test bricht einen Handler ab, der am Semaphor wartet. Die Drain-Tests nutzen Dummy-Tasks, und die Schedfire-Requeue-Tests (`test_schedfire_requeue.py`) decken nur Gate-Ausgänge ab.

**Nachweistest (Unit).** Genau die Probe als Test:
- Worker mit `concurrency=1` und `shutdown_timeout=0`.
- Task A blockiert, Nachricht B mit `x-swarmq-schedfire` folgt.
- `_execute_shutdown()` aufrufen.
- Erwartet: `release_fire_claim` für B (owner-geprüft) und kein Ack für B.

**Vorschlag.** Den Semaphor-Erwerb in den `try` ziehen (mit Flag „acquired“ für das `release`). Alternativ einen Abbruch im Semaphor-Warten wie den Gate-Abbruch behandeln: `self._detached(self._release_schedfire_claim(ctx))`. K-2 als Wurzelfix deckt es ebenfalls ab.

**Konfidenz:** hoch (Probe).

---

## K-3 (P2): Der Valkey-Client sendet nicht idempotente Befehle nach Verbindungsabbruch erneut

**Fundstellen**
- `src/swarmq/backend/valkey/connection.py:216-226`: `Retry(ExponentialBackoff, retries=5)` mit `retry_on_error = [ConnectionError, TimeoutError, OSError]`. valkey-py (`Valkey.execute_command` → `call_with_retry` → `_send_command_parse_response`) **sendet den Befehl erneut**, wenn die Antwort verloren ging, obwohl der Server ihn schon ausgeführt hat.
- `src/swarmq/backend/valkey/results.py:451-466` (`claim_unique`: `SET NX GET`).
- `src/swarmq/lua/sched_fire_claim.lua:410-424`: Der eigene Owner zählt nicht als übernehmbar.
- `src/swarmq/lua/lock_acquire.lua` (Mutex `SET NX`).
- `guard_claim.lua:519-526` ist dagegen ausdrücklich für den Transport-Retry gebaut („Wiederholung desselben Claims“). Der Fix ist also nur dort angekommen.

**Szenario.** Valkey führt den Befehl aus, dann reißt die Verbindung, bevor die Antwort gelesen ist: Valkey-Neustart oder Failover, Proxy- oder LB-Reset, `CLIENT KILL`. Der Client verbindet neu und sendet den Befehl ein zweites Mal.
- `Client.schedule(unique_for=…)`: Der zweite `SET NX GET` liefert die **eigene** `task_id` als „existing“. `schedule()` gibt sie zurück (`client/schedule.py:237-238`), **publiziert aber nie**. Der Task läuft nicht, `get_result` hängt, und das Flag blockiert gleiche Aufrufe für `unique_for`.
- Leader bei Cron mit `unique_for` (`scheduler/fire.py:175-202`): Ergebnis `dup_detected`, Commit, Fire verloren. Alle Fires bis zum Ablauf von `unique_for` gelten ebenfalls als Duplikat.
- Worker `claim_fire`: Der zweite Lauf liefert 0, also Ack, Fire verloren. Der Claim des eigenen Owners bleibt bis zur TTL.
- Mutex `lock_acquire`: Der zweite Lauf liefert 0, also Konflikt mit dem eigenen verwaisten Lock bis `hard_timeout + 30 s`. Eventuell folgt `LockWaitExceeded` und damit die DLQ.
- Ebenso, aber harmlos: `sliding_window_counter` zählt doppelt, `take_handover_marker` (GETDEL) meldet FAILOVER statt LEADER.

**Warum die Tests es nicht finden.** Unit-Tests mocken die Stores, und die In-Memory-Backends kennen keinen Transport. Integrationstests reißen nie eine Verbindung **nach** der Serverausführung ab. Der Toxiproxy-Spike damals prüfte Verfügbarkeit, keine Idempotenz.

**Nachweistest (Integration, Testports 5699/6399).**
1. `valkey.asyncio.connection.Connection.read_response` per `monkeypatch` so umbauen, dass es beim ersten Aufruf für den Zielbefehl **nach** dem Senden `ValkeyConnectionError` wirft. Der Befehl liegt dann schon auf dem echten Server.
2. Dann `backend.claim_unique(h, "t1", …)` aufrufen. Erwartet `None`, ist `"t1"`.
3. Analog `schedules.claim_fire(key, ttl, owner="o1")` (erwartet `True`, ist `False`) und `LockStore.acquire` (erwartet Token, ist `None`).

**Vorschlag**
- `claim_unique`: `existing == owner` als „geclaimt“ werten.
- `sched_fire_claim.lua`: `current == ARGV[1]` als eigen werten.
- `lock_acquire.lua`: `GET == owner` liefert 1, Semaphor: `ZSCORE owner` vorhanden liefert 1.
- Grundsätzlich alle Lua-Skripte und NX-Befehle gegen Doppelausführung mit gleichem Owner prüfen.

**Konfidenz:** mittel-hoch. Der Mechanismus in valkey-py ist belegt (Quelltext gelesen). Das Fenster ist schmal, trifft aber gerade Valkey-Neustarts und Failover.

---

## K-4 (P2): Zeitüberschreitung nach erfolgreichem Lock- oder Unique-Claim lässt verwaiste Sperren zurück

**Fundstellen**
- `src/swarmq/worker/gates/lock.py:259-291` mit `:539-565`: `bounded_backend_call` (`wait_for`, `backend_op_timeout`, Default 5 s) bricht `acquire` ab. Hat Valkey das Skript schon ausgeführt, hält `owner_token` den Lock. Nichts gibt ihn frei, die Zustellung stellt sich mit `defer_backend_unavailable` zurück und trifft danach auf ihren eigenen Lock.
- `src/swarmq/scheduler/fire.py:174-187`: `claim_unique` läuft in den Timeout, danach `return` **ohne** `_release_own_claim`.

Zum Vergleich: Der Ausführungsschutz löst denselben Fall mit `_release_in_background` (`execution_guard.py:145-150`), der Schedfire-Claim über Tokens, der Client-Pfad mit Rollback auf `BaseException`. Nur Lock-Gate und Leader-Unique fehlen.

**Szenario.** Valkey stockt über 5 s (Fork bei BGSAVE oder AOF-Rewrite, langes Skript, Netz-Jitter) und führt den Befehl danach aus.
- Mutex bzw. Semaphor-Slot sind verwaist bis `hard_timeout + 30 s` (bei 1 h `hard_timeout` eine Stunde Stillstand für alle Tasks dieses Locks). Mit `lock_wait_max_s` darunter folgt `LockWaitExceeded` und damit die DLQ.
- Beim Leader: Das Flag hält eine nie publizierte `task_id`. Der nächste Poll ergibt `dup_detected`, Commit, Fire übersprungen, und so weiter bis zum Ablauf von `unique_for`.

**Warum die Tests es nicht finden.** Die Timeout-Tests der Gates simulieren „hängt und tut nichts“, nicht „tut es und antwortet zu spät“. Der Scheduler-Test zu `scheduler_unique_setnx_failed` prüft nur „kein Commit“.

**Nachweistest (Unit).**
- Fake-`LockStore.acquire`, das den Token in einem Dict einträgt und dann auf ein nie gesetztes `Event` wartet, dazu `backend_op_timeout=0.01`. Nach dem Gate erwartet: `release(token)` wurde (im Hintergrund) aufgerufen. Ist: Der Token bleibt im Dict.
- Scheduler: Fake `claim_unique`, das das Flag setzt und dann über `op_timeout` schläft. Danach `_fire_one` zweimal aufrufen. Erwartet beim zweiten Aufruf ein Publish. Ist: `dedup_detected`.

**Vorschlag.** Wie beim Guard: Nach `BackendConnectionError` im Hintergrund owner-geprüft freigeben, begrenzt und nur einmal (Lock: `release(LockToken(name, owner_token, max_c))`, Leader: `_release_own_claim`).

**Konfidenz:** hoch für den Lock-Pfad, mittel für die Häufigkeit.

---

## Geprüft, ohne Befund (Kurzbegründung)

- **Erfolgspfad** (`store → notify → advance → ack → Done-Marker`). Crash vor dem Ack führt zu Redelivery, der Guard stellt bis zur Waise zurück, dann läuft der Task erneut. Das ist das dokumentierte Redelivery-Szenario 1. Crash nach dem Ack ist folgenlos.
- **`retry_later`** (Kopie → Ack → Superseded). Ein Crash dazwischen führt zur Kopie N+1 plus Redelivery N. Der Guard serialisiert über die gemeinsame Identität: Ein Erfolg setzt `done`, ein Fehler setzt `sup=N+1` und verwirft die ältere Kopie. Es gibt keine parallele Doppelausführung, nur einen möglichen Zusatzlauf.
- **Workflow publish-then-mark.** Deterministische Schritt-IDs und der Guard fangen ein Doppel-Enqueue ab. `chord_complete.lua` (SADD) ist idempotent. `workflow_state_cas.lua` meldet bei einer Wiederholung nur „stale“, danach folgt `_reconcile`.
- **`sched_commit.lua`** nach verlorener Antwort meldet 2 (superseded), es feuert nicht doppelt. Leader-Commit nach Publish mit Lock-Verlust führt zu einem Re-Fire mit demselben Schedfire-Key, die Dedup am Worker greift.
- **DLQ-Replay.** `retry_replay_id` und `generation_id` sind deterministisch, ein Crash zwischen Publish und XQ-Ack ergibt dieselbe Identität und damit eine Guard-Dedup.
- **Lock-Acquire gegen Abbruch.** Durch `asyncio.shield` geschützt, außer bei der Zeitüberschreitung in K-4.
- **Cancel-Gate.** Die Reihenfolge `store → Flag verkürzen → Completed-Marker → Unique-Release → Ack` erzeugt bei einem Crash dazwischen nur Wiederholungen derselben Entscheidung.

## Zusammenfassung

**3 × P1, 2 × P2, 0 × P3.**

Top 3:
1. **K-1:** Beim Deploy wird ein Task, der den Drain überdauert, nicht neu zugestellt, sondern läuft in den Fehlerpfad. Mit dem Default `max_retries=0` heißt das: Workflow dauerhaft FAILED, Fehlerergebnis gemeldet, und der Task läuft danach trotzdem woanders erfolgreich. Per Probe bestätigt.
2. **K-2/K-5:** Das Schedfire-Gate bestätigt Broker-Redeliveries als Duplikat, solange der In-Flight-Claim des toten oder abgebrochenen Vorgängers lebt. Cron- und delay/eta-Fires gehen still verloren: K-2 bei SIGKILL oder OOM mitten im Lauf, K-5 deterministisch bei jedem Deploy, wenn Fires am Semaphor warten (per Probe bestätigt).
3. **K-3:** Der transparente Retry von valkey-py führt `SET NX GET`, `sched_fire_claim.lua` und `lock_acquire.lua` nach verlorener Antwort ein zweites Mal aus. Der eigene Claim gilt dann als fremd: `schedule(unique_for=…)` publiziert nie, Fires gehen verloren, Locks sperren sich selbst. Der Guard ist schon dagegen gebaut, die anderen Skripte nicht.
