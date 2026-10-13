<!-- Befundregister. IDs FS-nn sind die Ticketnummern fuer xfail-Marker (AGENTS.md: kein Skip ohne Ticket). -->
# Befundregister „Fehlersuche Runde 2“ – swarmq v0.12.0 (ac989a3), 2026-10-13

Status: alle 16 Befunde P1/P2 durch Tests bestätigt (2026-10-13), offen. P3 ohne Nachweis-Test.

Nachweis-Tests tragen `@pytest.mark.befund("FS-nn")` und `xfail(strict=True)`. Wird ein Befund behoben, wird sein Test XPASS (rot), und die Markierung muss entfernt werden. Alle Nachweise: `pytest -m befund --runxfail` (Integration nur über die Testports).

Blickwinkel: Regressionen aus 0.12 (R), Absturz zwischen zwei Schritten (K), Nebenläufigkeit und Abbruch (N), Mutation-Testing mit mutmut 3 über die Kernmodule, Sichtung der überlebenden Mutanten (MA = Worker-Kern/Gates, MB = CPU-Modus/Lifecycle, MC = Scheduler/Client/Broker). Die Befunde stammen aus Code-Analyse, ein Teil ist zusätzlich mit Probe-Skripten nachgestellt.

## P1 – Verlust, Doppelausführung oder Hänger im Normalbetrieb (inkl. Deploy und Neustart)

| ID | Befund | Quellen |
|---|---|---|
| FS-01 | Ein Task, der den Drain (Deploy oder Recycling) überdauert, wird abgebrochen und als „hard timeout after None s“ in den Fehlerpfad geschickt statt neu zugestellt. Der Retry wird verbraucht; bei `max_retries=0` folgen DLQ, Fehlerergebnis und Workflow FAILED, und die Nachricht läuft trotzdem erneut. Das gilt für IO und CPU. Recycling ohne `hard_timeout` nutzt `shutdown_timeout` (30 s) als Frist. Abgebrochene CPU-Kinder laufen über die 1-s-Nachlauffrist hinaus. | K-1, N-5, MB-2, MB-5 |
| FS-02 | Nach der Publish-Endfrist geht ein Publish mit 0-s-Deckel trotzdem auf die Leitung, gemeldet wird `PublishIndeterminate`. Retry- bzw. DLQ-Kopie plus Requeue ergibt eine Doppelausführung. Beim Recycling ist das systematisch: Ab etwa 23 s Drain scheitert jeder Publish. | N-3, N-4 |
| FS-03 | Das Schedfire-Gate läuft vor dem Ausführungsschutz und verwirft eine Neuzustellung als Duplikat, solange der In-Flight-Claim dem toten Vorgänger gehört. Cron- und delay/eta-Fires gehen still verloren: bei SIGKILL/OOM und bei jedem Deploy mit am Semaphor wartenden Fires (das Warten liegt außerhalb von try/finally). | K-2, K-5 |
| FS-04 | Scheitert ein Dishka-Finalizer (`ExitError`), überspringt das finally der Pipeline Lock-Freigabe und Semaphor-Rückgabe. Der LockKeeper verlängert den Lock, solange der Worker lebt. Nach `concurrency` Fällen verarbeitet der Worker nichts mehr, `/ready` meldet ihn trotzdem bereit. | N-2 |

## P2 – falsches Verhalten in realistischen Sonderfällen

| ID | Befund | Quellen |
|---|---|---|
| FS-05 | Eine `TimeoutError` aus dem Task selbst wird zu „SoftTimeoutError after None s“, auch ohne `soft_timeout`. `retry_exceptions=[TimeoutError]` greift nicht. Das gilt auch im CPU-Modus. | MA-2 |
| FS-06 | Der Valkey-Client sendet nach verlorener Antwort nicht idempotente Befehle erneut: Unique-Claim, Cron-Fire-Claim, Mutex. Der Leader-Renew läuft ohne `op_timeout` (bis etwa 65 s). | K-3, MC-4 |
| FS-07 | Ein Timeout nach erfolgreichem Lock- bzw. Unique-Claim gibt den Claim nicht frei. | K-4 |
| FS-08 | `schedule()` hängt nach einem Verbindungsabriss bis `publish_timeout_s` auf dem toten Channel. Danach folgt ein falsches `PublishIndeterminate`, obwohl nichts gesendet wurde. | N-1 |
| FS-09 | Der Consumer-Wächter bricht `basic.cancel`/`basic.consume` nach 2 s ab. aiormq schließt daraufhin den ganzen Consume-Channel. | N-6 |
| FS-10 | BZ-08 ist nur teilweise behoben: DLQ-Kopien behalten `x-delivery-count`/`x-acquired-count`. Ein Replay per UI, Shovel oder swarmv geht sofort wieder in die DLQ. | R-1 |
| FS-11 | Die BZ-11-Zurückhaltung legt einen Cron still, dessen Task nur in einem auf 0 skalierten Pool registriert ist. Danach entfernt der Prune ihn. | R-2 |
| FS-12 | Ein Workflow-Schritt, der per `cancel(task_id)` vor dem Start abgebrochen wird, lässt den Workflow bis zur TTL `running`. | MA-1 |
| FS-13 | `unique_until="start"`: Ein terminales Ende vor dem Start gibt das Unique-Flag nicht frei. | MA-4 |
| FS-14 | `schedule(delay=/eta=, unique_for=None)` wird beim Fire trotzdem dedupliziert und ggf. still verworfen. | MC-1 |
| FS-15 | `get_result()` liest Pub/Sub nur in der ersten Sekunde, danach GET alle 5 ms. `polling` wird ignoriert, `subscribe()` hat keine Frist. | MC-2 |
| FS-16 | CPU-Modus: `_kill_child` signalisiert der Prozessgruppe nicht, wenn das Kind schon tot ist. Enkelprozesse überleben (entgegen KTD3/AE13). | MB-1 |

## P3

- FS-17: Expiry-Gate wertet gescheiterten Ack nach liegender DLQ-Kopie als Fehlschlag (R-3).
- FS-18: Expiry-Gate speichert Fehlerergebnis trotz `result_ttl=0`/FIRE_AND_FORGET (MA-3).
- FS-19: Nicht ganzzahliger Header `x-swarmq-rate-limit-count`/`x-swarmq-retry-count` lässt die Nachricht unquittiert (MA-6).
- FS-20: Doku: `SoftTimeoutError` erreicht `run()` nie; der Abstand zwischen soft und hard ist wirkungslos (MA-7).
- FS-21: Backoff am Deckel nicht monoton, gekappte Retries warten höchstens 0,8 × `retry_max_delay` (R-4).
- FS-22: Leader-Rücktritt ohne zweiten Kandidaten kostet bis zu 2 × Lock-TTL zusätzliche Pause (R-5).
- FS-23: Im Hintergrund gesperrter Slot bleibt in der Free-Queue; Sperre wird nie aufgehoben (MB-3).
- FS-24: Gescheiterter Handshake erzeugt asyncio-ERROR und doppelt gezählten Kindtod (MB-4).
- FS-25: Fortschritt aus dem Kind: ein Task je Nachricht, Reihenfolge unbestimmt (MB-6).
- FS-26: DI-Fehler im Kind erscheint nur als „exited without result“ (MB-7).
- FS-27: `validate_namespace("SQ")` schlägt den verbotenen Wert `'sq'` vor (MC-3).
- FS-28: AT_LEAST_ONCE-Schritt mit `result_ttl=0` scheitert mit der irreführenden Meldung „is FIRE_AND_FORGET“ (MA-8).
- FS-29: Signal-Handler bleiben nach graceful Shutdown installiert (N-7).

## Testlücken ohne gefundenen Fehler (Auswahl)

- Kein Unit-Test prüft, dass ein graceful Shutdown laufende Tasks fertig werden lässt (der Mutant „Drain-Frist 0“ überlebt).
- Nicht geprüft: `lock_wait_max_s` über mehrere Kopien, der LockKeeper-Timeout bzw. zwei gleichzeitig gehaltene Locks und `_check_late_cancel` bei Valkey-Ausfall.
- Unique-Header beim Cron-Fire und in `schedule_many` mit gemischtem Batch.
- TTL von Chord-Zähler und Workflow-Ergebnis; Pool-Schließen mit aktivem Kind; fd-Schließen in `_finalize`.
- Das Valkey-Backend wird nur über Integrationstests geprüft (512 Mutanten ohne Unit-Test).

## Geprüft und korrekt

Cron bei Zeitumstellung (Berlin, New York, Lord Howe), sieben Workflow-Kombinationen, Erfolgspfad Speichern → Ack, Retry über die Delay-Queue, Workflow-Weiterschaltung (Lua), DLQ-Replay-Erkennung, Namensraum-Konsistenz, Delay-Stufen, Superseded-Commit, `schedule_many` bei Teilfehlern.

## Offene Produktfragen

1. FS-01: Soll ein Drain-Abbruch eine Neuzustellung sein, ohne Fehlerergebnis und ohne Retry-Verbrauch (entgegen KTD2)?
2. FS-11: Wie verhält sich der Leader bei fehlendem Pool: feuern mit Warnung (wie 0.11) oder zurückhalten mit einer Grenze?
3. FS-14: Sollen `cli`-Oneshots ohne Opt-out dedupliziert werden; wie wird `unique_for=None` gespeichert?
4. FS-21: Ist `retry_max_delay` eine Obergrenze oder ein Mittelwert?
5. FS-02/FS-08: Wie wird `PublishIndeterminate` behandelt, wenn nachweislich nicht gesendet bzw. nach Fristende doch gesendet wurde?

## Nachweis-Tests

| ID | Testdateien | Status |
|---|---|---|
| FS-01 | `tests/unit/worker/compute/test_befund_fs01_cpu_drain_abort.py`, `tests/unit/worker/test_befund_fs01_drain_abort.py`, `tests/unit/worker/test_befund_fs01_recycling_drain.py` | offen |
| FS-02 | `tests/unit/broker/test_befund_fs02_publish_after_deadline.py`, `tests/unit/worker/test_befund_fs02_recycling_publish_deadline.py` | offen |
| FS-03 | `tests/unit/worker/gates/test_befund_fs03_schedfire_redelivery.py` | offen |
| FS-04 | `tests/unit/worker/test_befund_fs04_dishka_finalizer.py` | offen |
| FS-05 | `tests/unit/worker/test_befund_fs05_task_timeout_error.py` | offen |
| FS-06 | `tests/integration/backend/test_befund_fs06_retry_after_lost_reply.py`, `tests/unit/scheduler/test_befund_fs06_leader_renew_timeout.py` | offen |
| FS-07 | `tests/unit/scheduler/test_befund_fs07_unique_claim_after_timeout.py`, `tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py` | offen |
| FS-08 | `tests/unit/broker/test_befund_fs08_publish_lock_waiter.py` | offen |
| FS-09 | `tests/unit/broker/test_befund_fs09_consumer_watch_rpc_abort.py` | offen |
| FS-10 | `tests/integration/broker/test_befund_fs10_dlq_replay_counters.py`, `tests/unit/worker/test_befund_fs10_dlq_copy_counters.py` | offen |
| FS-11 | `tests/unit/scheduler/test_befund_fs11_scaled_to_zero_cron.py` | offen |
| FS-12 | `tests/unit/worker/test_befund_fs12_cancel_before_start.py` | offen |
| FS-13 | `tests/unit/worker/test_befund_fs13_unique_until_start.py` | offen |
| FS-14 | `tests/unit/scheduler/test_befund_fs14_oneshot_unique_none.py` | offen |
| FS-15 | `tests/unit/backend/test_befund_fs15_result_wait_polling.py` | offen |
| FS-16 | `tests/unit/worker/compute/test_befund_fs16_kill_group.py` | offen |

Hinweise zu den Nachweisen:

- FS-01: Die Nachweise prüfen die empfohlene Option der Produktfrage 1 (Neuzustellung ohne Fehlerergebnis). Das widerspricht KTD2 im Plan 2026-09-03-001. `test_recycling.py::test_task_count_threshold_recycles_with_ack` schreibt den Rückfall auf `shutdown_timeout` fest und muss beim Fix angepasst werden. Für MB-5 (1-s-Nachlauffrist nach dem Abbruch) gibt es keinen Nachweis: Er bräuchte echte Wartezeiten über 1 s.
- FS-02: Der Nachweis prüft die Invariante „nie gescheitert und zugleich gesendet“ und gilt damit für beide Antworten auf Produktfrage 5.
- FS-05: Präzisiert: Der Fehler tritt auch auf, wenn ein `soft_timeout` gesetzt ist, aber nicht abläuft. Er gilt im CPU-Kind ebenso.
- FS-10: Auf RabbitMQ 4.3.6 bleibt ein vom Publisher gesetzter `x-delivery-count` erhalten (Quorum und Classic). In einer Quorum-Queue wird nach `nack(requeue)` nur `x-acquired-count` auf 1 überschrieben. RabbitMQ 4.2 ist nicht geprüft.
- FS-11: Der Nachweis prüft die empfohlene Option (weiter feuern, mit Warnung) und widerspricht `test_unowned.py::test_unowned_cron_is_held_instead_of_fired`. Ein Fix muss „entfernter Cron“ von „Pool abwesend“ unterscheiden.
- FS-14: Nur der ausdrückliche Opt-out `unique_for=None` ist abgedeckt. Produktfrage 3 (Dedup ohne Opt-out) ist bewusst nicht festgelegt.
