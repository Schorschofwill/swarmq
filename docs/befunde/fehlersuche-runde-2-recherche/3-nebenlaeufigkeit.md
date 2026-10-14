# Fehlersuche 3: Nebenläufigkeit, Cancellation, Timeouts (swarmq v0.12.0, Worktree `swarmq-fehlersuche`)

Methode: Code gelesen (worker/, broker/rabbitmq/, scheduler/, client/, compute/, backend/valkey/script_pipeline),
dazu aio-pika 10 / aiormq 7 aus `.venv`. Drei Verhaltensweisen von aiormq habe ich mit broker-freien
Skripten belegt (Fake-Connector statt Socket):

- `fs/repro_aiormq_lock_waiter.py`: Ein Publish, der beim Verbindungsabriss auf die Channel-Sperre wartet, hängt danach ohne Fehler.
- `fs/repro_timeout_zero_publish.py`: Ein Publish unter `asyncio.timeout(0)` landet trotzdem auf der Leitung, der Aufrufer bekommt aber `TimeoutError`.
- `fs/repro_aiormq_rpc_cancel.py`: Ein per `wait_for` abgebrochenes `basic.cancel` schließt den ganzen Channel (Close 504).

Keine Datei im Repo geändert. Bekannte BZ-Befunde sind nicht wiederholt.

## Übersicht

| ID | Prio | Kurz | Konfidenz |
|---|---|---|---|
| N-1 | P2 | Ursache des 30-s-Hängers von `schedule()`: aiormq setzt einen Publish auf einem geschlossenen Channel fort, er endet erst an `publish_timeout_s` (120 s) | hoch (Repro) |
| N-2 | P1 | Scheitert ein Dishka-Finalizer, bleiben Task-Lock (der Keeper verlängert ihn ohne Ende) und ein Semaphor-Platz für immer belegt | hoch |
| N-3 | P1 | Bei verstrichener Shutdown-Endfrist geht ein Publish trotzdem raus, wird aber als gescheitert gemeldet. Folge: Retry- oder DLQ-Kopie plus Requeue des Originals | mittel-hoch (Repro aiormq) |
| N-4 | P1* | Der Recycling-Drain ist vom Budget ausgenommen, die Publish-Endfrist aber nicht: Nach etwa 23 s scheitert jeder Publish der laufenden Tasks bzw. verdoppelt sich | hoch |
| N-5 | P2 | IO-Modus: Der Abbruch am Drain-Ende wird verschluckt und als „hard timeout after None s“ gewertet. Workflow FAILED, Fehlerergebnis, DLQ, danach läuft der Task erneut | hoch (berührt KTD2) |
| N-6 | P2 | Der Consumer-Wächter bricht RPCs nach 2 s per `wait_for` ab. aiormq schließt dann den ganzen Consume-Channel, alle laufenden Zustellungen werden neu zugestellt | mittel |
| N-7 | P3 | Nach dem graceful Shutdown bleiben die SIGTERM/SIGINT-Handler installiert und schlucken bei eingebetteter Nutzung die Signale | hoch |

\* P1, sobald `recycle_max_tasks`/`recycle_rss_mb` gesetzt sind (Recycling ist dann Normalbetrieb).

---

## N-1 (P2): `schedule()` hängt bis zur Publish-Frist nach einem Verbindungsabriss. Ursache der offenen Loadsuite-Frage

**Fundstelle:** `src/swarmq/broker/rabbitmq/delivery.py:156-211` (`_publish_with_retry`, einzige Schranke ist `asyncio.timeout(publish_timeout_s)`), `:257-261` (`_publish_attempts`). Ursache liegt in `aiormq/channel.py:652-691` (`basic_publish`).

**Mechanismus (belegt):** `aiormq.Channel.basic_publish` serialisiert über `self.lock`. Die Property `lock`
prüft `is_closed` nur beim Zugriff. Ein Publish, der schon auf die Sperre wartet, läuft nach dem
Abriss trotzdem weiter. Abriss heißt: `FutureStore.reject_all` lehnt `drain_future` und das Confirm
des Halters ab, der Halter gibt die Sperre frei. Der Wartende bekommt die Sperre auf dem toten Channel
und legt neue Futures im bereits geleerten Store an. Seine Frames gehen in die `write_queue` der alten
Verbindung, deren Writer beendet ist. Danach wartet er auf `await drain_future`, und das Future
wird nie erfüllt und nie abgelehnt. `repro_aiormq_lock_waiter.py` zeigt: Der Halter endet mit
`ConnectionClosed`, der Wartende hängt ohne Fehler. Das „Future exception was never retrieved“ aus dem
rc1-Bericht ist das Confirm-Future des Halters.

Ein zweiter Weg zum selben Ende: `RobustConnection._on_connected` setzt `connected` vor dem
Restore der Channels (`aio_pika/robust_connection.py:107-119`). `RobustChannel.get_underlay_channel`
kann deshalb nach `ready()` noch den alten, geschlossenen aiormq-Channel liefern.

**Szenario:** Die Loadsuite erzeugt etwa 510 Dispatches pro Sekunde über einen Publish-Channel, die Toxiproxy-Trennung kommt um 22:33:45,53.
Die Publishes, die in diesem Moment in der Sperren-Warteschlange stehen (2 Stück), hängen. Alle
späteren Aufrufe sehen `is_closed` und öffnen nach etwa 5 s einen neuen Channel. Die beiden hängenden
enden erst nach 120 s mit `PublishIndeterminate`, obwohl nichts gesendet wurde. Das deckt sich mit
„liefen beide nicht“.

**Bewertung:**
- Kein Vertragsbruch, aber zwei echte Schäden:
  1. Bis zu 120 s Latenz, obwohl der Broker nach etwa 5 s wieder da ist.
  2. `PublishIndeterminate` ist hier falsch, denn der Publish ist sicher nicht angekommen. Ein Aufrufer, der Indeterminate als „vielleicht da, nicht wiederholen“ behandelt, verliert den Task.
- Im Worker trifft es `retry_later`, `dead_letter` und die Workflow-Advances: Der Handler hält 120 s den Semaphor-Platz.
- Im Leader hält ein hängender Fire 120 s den `_fire_lock`. Poll und Übernahme stehen still (`scheduler/loop.py:511`, `:358`), das Leader-Lock wird aber weiter verlängert, also kein Failover.

**Warum Tests es nicht finden:** Unit-Tests mocken den Channel. Die Toxiproxy-Spikes prüfen Publishes
vor bzw. nach dem Abriss, aber nicht den Wartenden in der aiormq-Sperre zum Zeitpunkt des Abrisses.

**Fix-Richtung:**
- Je Versuch den Publish gegen das Schließen des Channels laufen lassen, also `asyncio.wait({publish, underlay.closing}, FIRST_COMPLETED)`. Schließt der Channel zuerst und kein Frame wurde geschrieben (`sent_before_failure`), auf neuem Channel wiederholen.
- Zusätzlich upstream an aiormq melden: `basic_publish` sollte nach dem Erwerb der Sperre `is_closed` prüfen.

**Nachweistest (deterministisch):**
1. Spike-Pin ohne Broker: `repro_aiormq_lock_waiter.py` als Test. Der Halter blockiert über eine `write_queue` ohne Writer, der Wartende kommt per Task dazu, dann folgt `conn.close(...)`. Erwartet wird, dass der Wartende fertig ist (rot heute, `asyncio.wait(..., timeout=1)` als Deckel).
2. swarmq-Unit: Über `_publish_channel_factory` einen Fake-Channel einhängen, dessen `default_exchange.publish` auf ein nie gesetztes Event wartet und dessen `closing`-Future der Test setzt. `publish_timeout_s=60`, `pytest.mark.timeout(5)`. Erwartet wird, dass `publish()` auf dem zweiten Fake-Channel gelingt.

---

## N-2 (P1): Scheiternder DI-Finalizer: Lock wird ewig verlängert, Semaphor-Platz geht verloren

**Fundstelle:** `src/swarmq/worker/pipeline.py:707-715` (finally von `_process_message`), `:318-340` (`_teardown_di_scope`/`_detached`), `src/swarmq/worker/lock_keeper.py:142-168`.

**Mechanismus:**
- Dishka 1.10 wirft beim Schließen des REQUEST-Scopes `ExitError`, wenn ein yield-Finalizer scheitert (`dishka/async_container.py:309-340`).
- `_teardown_di_scope` reicht das über `await asyncio.shield(inner)` durch. Der Rest des `finally` läuft nicht mehr: weder `_detached_lock_release` (Zeile 712-713) noch `self._semaphore.release()` (Zeile 715).
- `contain_delivery_errors` loggt nur `delivery_handler_failed`. Die Nachricht ist zu diesem Zeitpunkt schon bestätigt (Ergebnis-Tail lief), es fällt also niemandem auf.

**Folgen:**
- `LockKeeper.drop` wird nur in `release_acquired_locks` aufgerufen (`gates/lock.py:587`). Der Keeper verlängert den Mutex deshalb so lange, wie der Worker lebt (einmal `task_lock_held_past_hard_timeout`). Alle anderen Tasks mit diesem Lock warten bis `max_age` und landen dann per `LockWaitExceeded` in der DLQ. Clusterweiter Stillstand eines Lock-Namens bis zum Neustart.
- Jeder Fall kostet einen Semaphor-Platz. Nach `concurrency` Fällen bleibt jede Zustellung vor `self._semaphore.acquire()` stehen (pipeline.py:575): Der Worker verarbeitet nichts mehr, `/ready` meldet ihn trotzdem bereit (Consumer existiert). Prefetchte Nachrichten liegen bis zum consumer_timeout.

**Szenario:** Ein üblicher Provider `yield session; await session.commit()`. Der Commit scheitert (Constraint, DB-Verbindung weg), die Task-Klasse hat `lock=…`. Schon ein einziger Fall blockiert den Lock-Namen. Ohne Lock tritt das schleichende Aushungern nach `concurrency` Fällen ein.

**Warum Tests es nicht finden:**
- `tests/unit/worker/test_worker_dishka.py` prüft nur, dass der Finalizer die Exception bekommt, nie einen Finalizer, der selbst wirft.
- `test_semaphore_released_on_error` deckt nur Fehler im Task-Körper ab.

**Fix-Richtung:** Im `finally` jeden Schritt einzeln absichern, z. B. mit `try/finally`-Kaskade oder `contextlib.AsyncExitStack`. Lock-Freigabe und `semaphore.release()` müssen auch bei Ausnahme oder Abbruch der vorigen Schritte laufen. Den DI-Fehler loggen (`di_scope_exit_failed`).

**Nachweistest:**
- Aufbau: Worker mit Mock-LockStore (acquire liefert Token, `release` zeichnet auf), `concurrency=1`. Registrierter Dishka-Container mit REQUEST-Provider, dessen Finalizer nach `yield` `RuntimeError` wirft. Task mit `TaskInfo(lock="x")`.
- Ablauf: `await worker._handle_message(msg1)`, danach `await asyncio.wait_for(worker._handle_message(msg2), 1)`.
- Erwartet:
  - `release` für `x` wurde aufgerufen.
  - `worker._lock_keeper.kept == 0` und `worker._active_handler_locks == {}`.
  - msg2 läuft.

  Heute: Timeout bzw. `kept == 1`.

---

## N-3 (P1): Verstrichene Publish-Endfrist: Publish geht raus, Ergebnis lautet „gescheitert“. Retry- bzw. DLQ-Kopie plus Requeue

**Fundstelle:** `src/swarmq/broker/rabbitmq/connection.py:460-471` (`_effective_publish_timeout` liefert `0.0` mit dem Kommentar „lässt den Publish sofort scheitern“), `delivery.py:171-211`. Aufrufer: `worker/retry.py:157-165` (`retry_publish_failed` führt zu `redeliver_in_place`), `retry.py:311-336` (`dlq_publish_failed` führt zu `redeliver_in_place`), `gates/__init__.py:281-285`.

**Mechanismus (Repro):**
- `asyncio.timeout(0)` plant den Abbruch per `call_soon` beim Eintritt.
- Auf dem Fast-Path suspendiert bis `await drain_future` nichts: Channel offen, nicht blockiert, Sperre frei, `write_queue.put` ohne Wartezeit.
- Die Frames liegen also in der `write_queue`, die Writer-Task schreibt sie im selben Loop-Durchlauf auf den Socket.
- Die Handler-Task bekommt den Timeout: `PublishIndeterminate`, dazu verwirft `_discard_publish_channel` den Channel.
- `repro_timeout_zero_publish.py`: TimeoutError, aber `frames on the wire: 1`.

Swarmq behandelt das wie einen gescheiterten Publish:
- **Retry**: Kopie in DQ (wirklich gesendet) + `nack(requeue)` des Originals. Der Task läuft zweimal, und der Ausführungsschutz erkennt das nicht: `mark_superseded` wird nur nach erfolgreichem Verb gesetzt.
- **DLQ**: Kopie in XQ + Requeue. Der Task liegt in der DLQ und läuft trotzdem erneut. `fail_workflow`, das Fehlerergebnis und die Benachrichtigung liefen schon vorher (`retry.py:276-301`).
- **Scheduler-Leader im Shutdown-Fenster**: Der Fire geht raus, aber kein Commit (`fire.py:208-218`). Er feuert erneut, bis `stop()` kommt. Die Schedfire-Dedup der Worker fängt das meist ab.

**Wann:** Jeder Publish nach `drain_until`:
- alle Handler, die nach dem Drain-Ende noch abschließen (siehe N-5),
- der Leader-Poll zwischen Drain-Ende und `scheduler.stop()`,
- vor allem N-4.

**Warum Tests es nicht finden:** `tests/unit/broker/test_shutdown_deadline.py::test_elapsed_shutdown_deadline_fails_immediately` prüft nur, dass `_effective_publish_timeout() == 0.0` ist, nicht ob gesendet wurde. Der Kanal ist überall gemockt, ohne Writer-Semantik.

**Fix-Richtung:** Bei `remaining <= 0` gar nicht erst publizieren, sondern eine eigene „nicht gesendet“-Ausnahme werfen (z. B. `BrokerConnectionError("shutdown publish deadline passed")`, sicher wiederholbar, kein Verwurf). Allgemein „timeout ≤ kleiner Puffer“ ebenso behandeln.

**Nachweistest:**
- Aufbau: `RabbitMQBroker` mit `_publish_channel_factory`, die einen Fake-Channel liefert. Dessen `default_exchange.publish` hängt den Body synchron an eine Liste `sent` an und wartet dann auf ein Future, das erst die nächste Loop-Runde erfüllt (wie aiormq).
- Ablauf: `broker.set_publish_deadline(loop.time() - 1)`, dann `await broker.retry_later(delivery, delay_ms=1000, headers={})`.
- Erwartet: Fehler ohne Indeterminate und `sent == []`. Heute: `PublishIndeterminate` und `sent == [body]`.
- Zweiter Test auf Worker-Ebene: Retry nach Ablauf der Endfrist, dann `broker.redeliver` und kein Kopie-Publish.

---

## N-4 (P1 bei aktivem Recycling): Recycling-Drain wartet auf Langläufer, die Publish-Endfrist schneidet nach etwa 23 s ab

**Fundstelle:**
- `src/swarmq/worker/recycling.py:113` (`_drain_timeout_override = max(hard_timeout) + 30`)
- `src/swarmq/worker/shutdown.py:103-107` (Override ersetzt die Drain-Frist)
- `shutdown.py:269-272`: `_set_publish_deadline(started_at + budget.drain_until)` gilt unverändert, bei `shutdown_timeout=30` und Supervisor also 23 s nach Beginn (`shutdown_budget.py:53-56`).

**Szenario:**
- Ein IO-Worker mit `recycle_max_tasks` (Normalbetrieb) recycelt und drainiert „ohne erzwungenen Abbruch, damit ein gesunder Langläufer keinen Retry verliert“ (Kommentar Zeile 97-99).
- Jeder Task, der später als 23 s nach Drain-Beginn endet und publizieren muss, scheitert an der Frist.
- Workflow-Schritte: `advance` wirft `PublishIndeterminate` (eine `BrokerConnectionError`), `results.py:258-266` führt zu `redeliver_in_place`. Der schon erfolgreich gelaufene Schritt läuft auf einem anderen Worker noch einmal, und dank N-3 ist der Folgeschritt womöglich schon veröffentlicht.
- Retries/DLQ: wie N-3, also doppelt.
- Ist der recycelnde Worker Leader, feuert er während des gesamten Drains jede Sekunde dieselben fälligen Crons (gesendet, nie committed), bis der BZ-10-Rücktritt greift.

Der Recycling-Drain ist damit genau für die Langläufer kaputt, für die er gebaut wurde.

**Warum Tests es nicht finden:** `tests/unit/worker/test_shutdown_budget_steps.py::test_recycling_drain_is_exempt_from_the_budget` prüft nur, dass der Task nicht abgebrochen wird, nicht ob er danach noch publizieren kann. `test_shutdown_publish_deadline.py` kennt keinen Override.

**Fix-Richtung:** Mit `_drain_timeout_override` die Publish-Endfrist auf `started_at + override + Reserve` setzen (oder keine). Der Supervisor wartet beim Recycling ohnehin auf den Ersatz.

**Nachweistest:** Wie der bestehende Recycling-Test (`shutdown_timeout=0.01`, `_drain_timeout_override=2.0`). Der langsame Task ruft nach `done.wait()` `worker._broker._effective_publish_timeout()` auf (echter `RabbitMQBroker` ohne Verbindung genügt). Erwartet `> 0`, heute `0.0`.

---

## N-5 (P2, berührt KTD2): IO-Modus: Abbruch am Drain-Ende wird verschluckt und als Hard-Timeout gewertet

**Fundstelle:** `src/swarmq/worker/execution.py:183-200` (`except asyncio.CancelledError` in `_await_run_result`), `shutdown.py:113-118`.

**Mechanismus:**
- `_drain_in_flight` bricht Handler nach `drain_until` ab. Der Abbruch der Handler-Task bricht `run_task` ab (wartendes Future). `await run_task` wirft `CancelledError` und wird gefangen.
- `cancel_reason` ist `None`, also läuft der Hard-Timeout-Pfad: `RuntimeError("hard timeout after None s")` bei Tasks ohne `hard_timeout`.
- Kein `uncancel()`, der Abbruch ist verschluckt. Der Handler läuft weiter durch `_resolve_error_path`, mit Default `max_retries=0` und `dead_letter_terminal_failures=True` landet er in `_terminal_failure`.

**Folgen:**
- Ein gesunder Task wird bei jedem Deploy, der länger läuft als der Drain, zum endgültigen Fehler. `fail_workflow` markiert den Workflow FAILED, das Fehlerergebnis plus Notify lässt `get_result()` mit „hard timeout after None s“ scheitern.
- `TASK_FAILED` geht raus. Danach der DLQ-Publish nach der Frist, also N-3: Kopie in XQ und Requeue. Der Task läuft auf einem anderen Worker erneut, der Workflow bleibt FAILED.
- `current_task().cancelling()` bleibt danach `> 0`. In `settle()` und `_publish_attempts` (delivery.py:75-78, :286-289) wird jede interne aio-pika-`CancelledError` deshalb als eigener Abbruch weitergeworfen. Sie entgeht dem `except Exception` in `retry.py` und trifft im `finally` den Terminal-Default: Schedfire-Completed-Marker und Unique-Freigabe auf einer unbestätigten Nachricht. Das ist die Lehre aus `schedfire-retry-dedup-collision-silent-drop`.

**Einordnung:** KTD2 (CPU-Plan) hat „Abbruch am Drain-Ende wird Fehlerergebnis“ für den CPU-Modus bewusst entschieden. Die Annahme, Retry/DLQ liefen danach sauber durch, gilt aber nicht, weil die Publish-Frist zu diesem Zeitpunkt per Konstruktion abgelaufen ist (N-3). Im IO-Modus ist das Verhalten Nebenwirkung, keine Entscheidung (siehe Meldungstext). Produktfrage für Georg: Sollte ein Drain-Abbruch nicht `Redelivered` ohne Fehlerergebnis sein, also eine Neuzustellung ohne Workflow-Fail?

**Warum Tests es nicht finden:**
- `test_drain_timeout_cancels_remaining_tasks` nutzt eine nackte Task, nicht `_handle_message`.
- `test_outer_cancel_during_drain_kills_child_and_returns_error_result` prüft nur das CPU-Ergebnis, nicht Workflow, Ergebnis-Store und Broker-Verben.

**Nachweistest:**
- Aufbau: Worker (IO) mit Mock-Broker/Backend, Task `run` setzt `started` und wartet auf ein nie gesetztes Event, Nachricht mit Workflow-Header.
- Ablauf: Handler-Task starten, `await started.wait()`, `worker._config.shutdown_timeout=0.01`, `worker._request_shutdown()`, `await worker._execute_shutdown()`.
- Erwartet:
  - kein `store_result` mit `is_error`,
  - kein `advancer.fail`,
  - kein `TASK_FAILED`,
  - `broker.redeliver` aufgerufen.

---

## N-6 (P2): Consumer-Wächter bricht RPCs nach 2 s ab, aiormq schließt den ganzen Consume-Channel

**Fundstelle:** `src/swarmq/broker/rabbitmq/consumer_watch.py:244` (`wait_for(queue.cancel(tag), 2 s)`), `:264` (`wait_for(queue.consume(cb), 2 s)`), `_RPC_TIMEOUT_S = 2.0` (Zeile 45).

**Mechanismus (Repro):** `aiormq.Channel.rpc` sendet bei `CancelledError`/`TimeoutError` selbst ein `Channel.Close(504, "RPC timeout …")` (`aiormq/channel.py:189-213`; `repro_aiormq_rpc_cancel.py` zeigt den Close-Frame).

**Folgen:**
- Antwortet der Broker auf das `basic.cancel`/`basic.consume` einer Neuanmeldung langsamer als 2 s, schließt swarmq selbst den Consume-Channel. Darauf liegen alle Consumer des Workers.
- Alle unbestätigten Zustellungen (bis `concurrency × Queues`) gehen an andere Worker, während sie hier noch laufen. Die späten Acks enden in `delivery_settle_failed`.
- Der Ausführungsschutz stellt die Kopien zurück, statt sie doppelt laufen zu lassen. Das kostet aber DQ-Runden, und ohne Schutz (`execution_guard=False`) laufen sie doppelt.
- Neuanmeldungen gibt es gerade in gestörten Lagen: 4.3-consumer_timeout, Channel ersetzt, Broker unter Last.

**Warum Tests es nicht finden:** Die Unit-Tests des Wächters nutzen Fake-Queues, deren `cancel`/`consume` sofort antworten oder werfen. Die aiormq-Semantik „abgebrochenes RPC schließt den Channel“ ist nirgends abgebildet.

**Fix-Richtung:**
- Die RPCs nicht per `wait_for` abbrechen, sondern das aiormq-eigene `timeout=` nutzen; das schließt ebenfalls, ist aber explizit.
- Besser: eine großzügige Frist (z. B. 30 s) und den Takt währenddessen weiterlaufen lassen. Oder in einem eigenen Task ausführen und nur das Warten des Takts begrenzen, ohne das RPC abzubrechen.

**Nachweistest:**
- Spike-Pin (broker-frei): `repro_aiormq_rpc_cancel.py` als Test. Erwartet wird kein `Channel.Close` im `write_queue`, heute rot.
- swarmq-Unit: Fake-Queue, deren `cancel()` auf ein Event wartet und einen Abbruch aufzeichnet (`except CancelledError: cancelled.set(); raise`). `ConsumerWatch(timeout_s=0.01).tick()` mit fehlendem Tag auf offenem Channel. Erwartet: `cancelled` ist nicht gesetzt.

---

## N-7 (P3): Signal-Handler bleiben nach dem graceful Shutdown installiert

**Fundstelle:** `src/swarmq/worker/lifecycle.py:441-442` installiert sie. Entfernt werden sie nur in `_abort_teardown` (`shutdown.py:364-368`), das nach graceful Shutdown wegen `_torn_down=True` nicht läuft (`shutdown.py:293`, `:311`).

**Szenario:** Eingebettete Nutzung, etwa Burst-Modus in einem Skript: `start_worker()` kehrt zurück, das Programm läuft weiter. SIGTERM und Strg-C rufen dann `_request_shutdown` eines gestoppten Workers auf, also ein No-op. Der Prozess lässt sich nicht mehr per Signal beenden.

**Test:** Nach `_execute_shutdown()` und dem Ende von `start()` prüfen, ob `signal.getsignal(SIGTERM)` wieder der vorherige Handler ist. Alternativ `loop.remove_signal_handler` mocken und den Aufruf erwarten.

---

## Geprüft, ohne Befund (Kurzliste)

- `_kill_child_uninterruptible` (pool.py:282): Ein zweiter Cancel geht verloren, aber der Aufrufer wirft den ersten weiter (Zeile 249-252, 405-408). In Ordnung.
- Guard-Claim bei Transport-Retry der `ScriptPipeline` (ganzer Stapel erneut): `guard_claim.lua` ist für denselben Owner idempotent. In Ordnung.
- Leader-Renew-Fehler: Alle Lock-Fehler werden zu `BackendConnectionError`, der Rücktrittszähler greift. Valkey hat `socket_timeout`.
- `create_task`-Referenzen: Alle gehalten (Sets oder Attribute).
- Blockierende Aufrufe: nur am Rand (Forkserver-Warmup beim Start, `Thread.join(2)` beim Pool-Close). Nicht gemeldet.

## Zusammenfassung

- P1: 3 (N-2, N-3, N-4)
- P2: 3 (N-1, N-5, N-6)
- P3: 1 (N-7)

Top 3:
1. **N-3/N-4**: Ein Publish nach verstrichener Endfrist wird trotzdem gesendet und als gescheitert behandelt, was Doppelausführung bzw. DLQ plus erneuten Lauf bedeutet. Beim Recycling-Drain trifft das systematisch jeden Langläufer nach etwa 23 s.
2. **N-2**: Ein scheiternder Dishka-Finalizer hält den Task-Lock für die Lebensdauer des Workers (der Keeper verlängert ihn) und frisst Semaphor-Plätze bis zum stillen Stillstand.
3. **N-1**: Ursache des `schedule()`-30-s-Hängers: aiormq setzt einen Publish, der auf die Channel-Sperre wartet, auf dem geschlossenen Channel fort. Er hängt bis `publish_timeout_s` (120 s) und meldet fälschlich `PublishIndeterminate`, obwohl nichts gesendet wurde.
