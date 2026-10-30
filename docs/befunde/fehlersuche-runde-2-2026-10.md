<!-- Befundregister. IDs FS-nn sind die Ticketnummern fuer xfail-Marker (AGENTS.md: kein Skip ohne Ticket). -->
# Befundregister „Fehlersuche Runde 2“ – swarmq v0.12.0 (ac989a3), 2026-10-13

Status (2026-10-13): Gegenprüfung abgeschlossen. 23 Befunde stehen als FEHLER fest: 3 × P1, 10 × P2, 10 × P3. P1 und P2 haben alle einen Nachweis-Test, P3 teilweise. 6 Verdachte sind bewusste ENTSCHEIDUNGEN und keine Befunde mehr, 4 sind LÜCKEN (Produktfragen). Stand 2026-10-14: FS-02, FS-03, FS-04, FS-06 (Fire-Claim) in 0.12.1rc1; auf `fix/fehlersuche-runde-2-p2` behoben: FS-05, FS-06 (Rest), FS-07, FS-08, FS-09, FS-10, FS-12, FS-13, FS-15, FS-16, FS-17, FS-19, FS-20, FS-23, FS-24, FS-27, FS-28, FS-14, FS-30, FS-31 – damit alle 23 FEHLER behoben (Sammelrelease offen).

Nachweis-Tests tragen `@pytest.mark.befund("FS-nn")` und `xfail(strict=True)`. Wird ein Befund behoben, wird sein Test XPASS (rot), und die Markierung muss entfernt werden. Alle Nachweise: `pytest -m befund --runxfail` (Integration nur über die Testports).

Blickwinkel: Regressionen aus 0.12 (R), Absturz zwischen zwei Schritten (K), Nebenläufigkeit und Abbruch (N), Mutation-Testing mit mutmut 3 über die Kernmodule, Sichtung der überlebenden Mutanten (MA = Worker-Kern/Gates, MB = CPU-Modus/Lifecycle, MC = Scheduler/Client/Broker).

**Gegenprüfung:** Drei Prüfer, die die Verdachte nicht gefunden hatten, haben jeden Verdacht zu widerlegen versucht. Geprüft wurden der Entscheidungs-Abgleich (Pläne/KTD, Befundregister, Doku, Changelog, git-Historie, festschreibende Tests), die Quelle der Erwartung und der Realismus der Nachweise. Einstufung: FEHLER, ENTSCHEIDUNG, LÜCKE, NICHT HALTBAR. Ablauf siehe `docs/befunde/README.md` und Entscheidungsregister `compound-packs/swarmq-entscheidungen/` (Branch `chore/entscheidungsregister`).

## P1 – FEHLER: Verlust, Doppelausführung oder Hänger im Normalbetrieb

| ID | Befund | Verletzte Zusage | Quellen |
|---|---|---|---|
| FS-02 | Ist die Publish-Endfrist **vor** dem Publish schon verstrichen, geht er mit 0-s-Deckel trotzdem auf die Leitung; gemeldet wird `PublishIndeterminate`. Retry- bzw. DLQ-Kopie plus Requeue ergibt eine Doppelausführung. Beim Recycling ist das systematisch: Ab etwa 23 s Drain scheitert jeder Publish. | Docstring `connection.py` („verstrichene Endfrist scheitert sofort“); BZ-18-Entscheidung „Recycling-Drain ausgenommen“; Changelog 0.12. Den unbestimmten Ausgang eines laufenden Publishs nimmt Plan 2026-08-28 (AE2/R11) dagegen bewusst hin, das ist nicht Teil des Befunds. | N-3, N-4 |
| FS-03 | Das Schedfire-Gate verwirft eine Neuzustellung als Duplikat, solange der In-Flight-Claim dem toten Vorgänger gehört. Cron- und delay/eta-Fires gehen still verloren: bei SIGKILL/OOM und bei jedem Deploy mit am Semaphor wartenden Fires (das Warten liegt außerhalb von try/finally). | Plan 2026-04-17-003 H-2: at-least-once bei Absturz mit Claim; der geplante Absturztest wurde nie geschrieben. | K-2, K-5 |
| FS-04 | Scheitert ein Dishka-Finalizer (`ExitError`), überspringt das finally der Pipeline Lock-Freigabe und Semaphor-Rückgabe. Der LockKeeper verlängert den Lock, solange der Worker lebt. Nach `concurrency` Fällen verarbeitet der Worker nichts mehr, `/ready` meldet ihn trotzdem bereit. P1 mit Lock, ohne Lock P2. | Semaphor-Freigabe im finally ist durch Tests festgeschrieben; Commit im Finalizer ist das Muster aus dem DI-Plan (R13). | N-2 |

## P2 – FEHLER

| ID | Befund | Verletzte Zusage | Quellen |
|---|---|---|---|
| FS-05 | Eine `TimeoutError` aus dem Task selbst wird zu `SoftTimeoutError`, auch ohne `soft_timeout` oder wenn er nicht abgelaufen ist. `retry_exceptions=[TimeoutError]` greift nicht. Gilt auch im CPU-Kind. | Phase-2-Plan: Umwandlung nur bei gesetztem `soft_timeout`; `errors.md`. | MA-2 |
| FS-06 | Der Valkey-Client sendet nach verlorener Antwort nicht idempotente Befehle erneut: Unique-Claim (`schedule()` gibt eine ID zurück, publiziert nie), Cron-Fire-Claim, Mutex sperrt sich selbst. | `guard_claim.lua` ist für genau diesen Fall bereits idempotent (d7d94c5); `lock_acquire.lua` nimmt „kein Retry mit gleichem Token“ an. | K-3 |
| FS-07 | Ein Timeout nach erfolgreichem Lock- bzw. Unique-Claim gibt den Claim nicht frei: Lock-Gate, Leader-Unique-Claim und `Client.schedule` (Rollback erst nach dem Claim). | Der Ausführungsschutz löst denselben Fall bereits mit Freigabe im Hintergrund (7e6abec). | K-4 |
| FS-08 | `schedule()` hängt nach einem Verbindungsabriss bis `publish_timeout_s` auf dem toten Channel. Danach folgt ein falsches `PublishIndeterminate`, obwohl nichts gesendet wurde. | Changelog 0.12 (B24) und `errors.de.md:485-487`: nicht gesendet → Wiederholung, dann `BrokerConnectionError`. | N-1 |
| FS-10 | DLQ-Kopien behalten `x-delivery-count`/`x-acquired-count`. Ein Replay per UI, Shovel oder swarmv geht sofort wieder in die DLQ (BZ-08 nur teilweise behoben). Auf RabbitMQ 4.3.6 bleibt der vom Publisher gesetzte Zähler erhalten. | Changelog 0.12 (BZ-08): Replay per UI/Shovel funktioniert. | R-1 |
| FS-12 | Ein Workflow-Schritt, der per `cancel(task_id)` vor dem Start abgebrochen wird, lässt den Workflow bis zur TTL `running`. Seltener Auslöser. | Abbruch während des Laufs markiert den Workflow seit 17dac64 als fehlgeschlagen; BZ-13. | MA-1 |
| FS-13 | `unique_until="start"`: Ein terminales Ende vor dem Start (max_age, Cancel, Lock-Wartezeit, unbekannter Task, Delivery-Limit) gibt das Unique-Flag nicht frei. | Doku: Flag wird frei, sobald ein Worker die Nachricht abholt; BZ-22 für den completion-Modus. | MA-4 |
| FS-14 | `schedule(delay=/eta=, unique_for=None)`: Der ausdrückliche Opt-out wird nicht gespeichert, der Leader dedupliziert über die eigene `TaskInfo` und verwirft den Oneshot ggf. still. Der Rückfall auf die eigene `TaskInfo` ohne Opt-out ist entschieden. | `api.md:122`: „Pass None to disable dedup for this call“. | MC-1 |
| FS-16 | CPU-Modus: `_kill_child` signalisiert der Prozessgruppe nicht, wenn das Kind schon tot ist (sauberes Ende in der Gnadenfrist, OOM-Kill). Enkelprozesse überleben. | CPU-Plan R8/R9, KTD3/AE13, `cpu-mode.md:19`. | MB-1 |
| FS-30 | Recycling ohne registrierten `hard_timeout` nutzt `shutdown_timeout` (30 s) als Drain-Frist und bricht längere Tasks bei jedem Recycling ab (abgetrennt von FS-01). | CPU-Plan R22/KTD11, `delivery-semantics.md:260-261`: Recycling verbraucht keinen Retry. | MB-2, N-5 |

## P3 – FEHLER

| ID | Befund | Quellen |
|---|---|---|
| FS-09 | Der Consumer-Wächter bricht `basic.cancel`/`basic.consume` nach 2 s ab; aiormq schließt den ganzen Consume-Channel. Seltener Auslöser, Ausführungsschutz standardmäßig an (Nachweis-Test vorhanden). | N-6 |
| FS-15 | `pubsub.subscribe()` in `get_result()` hat keine Frist; ein hängendes Valkey hält den Aufrufer über `timeout` hinaus fest (Docstring-Zusage). Das 5-ms-Polling selbst ist entschieden (Nachweis-Test vorhanden). | MC-2 |
| FS-17 | Expiry-Gate: Ein gescheiterter Ack nach liegender DLQ-Kopie gilt als Fehlschlag. Mit Ausführungsschutz bleiben nur das Unique-Flag bis `unique_for` und irreführende Logzeilen. | R-3 |
| FS-19 | Nicht ganzzahliger Header `x-swarmq-retry-count` (schon `"3"`) bzw. nicht numerischer `x-swarmq-rate-limit-count` lässt die Nachricht unquittiert; die Regel „kaputter Header zählt wie keiner“ gilt an drei anderen Stellen. | MA-6 |
| FS-20 | Doku: Das Beispiel in `errors.md` mit `except SoftTimeoutError` kann nie greifen (`wait_for` bricht mit CancelledError ab). Eher P2 für Nutzer. | MA-7 |
| FS-23 | Ein im Hintergrund (Recycling) gesperrter Slot wird weiter vergeben. Dass ein gesperrter Slot gesperrt bleibt, ist entschieden (KTD3). | MB-3 |
| FS-24 | Gescheiterter Handshake: asyncio-ERROR „exception in shielded future“ plus doppelt gezählter Kindtod. | MB-4 |
| FS-27 | `validate_namespace("SQ")` schlägt den verbotenen Wert `'sq'` vor. | MC-3 |
| FS-28 | AT_LEAST_ONCE-Schritt mit `result_ttl=0`: Das Verhalten ist dokumentiert, die Meldung nennt aber fälschlich „is FIRE_AND_FORGET“. | MA-8 |
| FS-31 | Leader-Renew ohne `op_timeout` (bis etwa 65 s), Rücktritt erst nach etwa 150 s statt vor Ablauf der Lock-TTL (abgetrennt von FS-06). Doppel-Fires fallen unter das akzeptierte Risiko (Phase-3c-Plan :1391) (Nachweis-Test vorhanden). | MC-4 |

## ENTSCHEIDUNG – kein Befund

| ID | Verdacht | Entscheidung | Neues Argument zum Überdenken |
|---|---|---|---|
| FS-01 | Drain-Abbruch wird zum Fehlerergebnis statt Neuzustellung (IO und CPU). | E-001 / KTD2 im CPU-Plan (user-approved); Tests `compute/test_executor.py`. | Ja: Am Drain-Ende ist die Publish-Endfrist schon verstrichen; zusammen mit FS-02 entstehen Fehlerergebnis, DLQ-Kopie **und** erneuter Lauf. `delivery-semantics.md:267-268` verspricht zudem „cancelled and redelivered“. |
| FS-11 | BZ-11-Zurückhaltung legt einen Cron still, dessen Pool auf 0 skaliert ist. | BZ-11-Entscheidung (zurückhalten, Prune nach 24 h); `test_unowned.py`. | Ja: Ein auf Queue-Länge skalierender Pool (KEDA) wird so nie geweckt. |
| FS-15 (Polling) | GET alle 5 ms nach 1 s, `polling` ignoriert. | Anforderung R3, Plan 2026-04-15-005, Commit c18a144, `api.md:228`. | Ja: Nach 1 s wird Pub/Sub nicht mehr gelesen; für Tasks über 1 s ist das Polling der Normalfall. Gemessen wurde nur mit kurzen Tasks. |
| FS-18 | Expiry-Gate speichert Fehlerergebnis trotz `result_ttl=0`/FIRE_AND_FORGET. | `dead-letter-queues.md:51`, `retry.py:288-292`. Nur der Code-Kommentar im Gate ist falsch. | Nein. |
| FS-21 | Backoff am Deckel nicht monoton (0,6–0,8 × `retry_max_delay`). | BZ-26, Plan, Guide, Changelog. | Nein. |
| FS-22 | Leader-Rücktritt kostet bis zu 2 × Lock-TTL Pause. | Fix-Plan 2026-10-09-2314 :572-573, `configuration.de.md:120` (mit `0` abschaltbar). | Nein. |

## LÜCKE – nie entschieden (Produktfragen)

| ID | Frage |
|---|---|
| FS-25 | Fortschritt aus dem CPU-Kind: Reihenfolge nicht garantiert (Fortschritt ist als best effort dokumentiert; „unbegrenzt“ war nicht haltbar, die 500-ms-Drosselung läuft auch im Kind). |
| FS-26 | Ein scheiternder DI-Finalizer im Kind erscheint nur als „exited without result“ (ein scheiternder Provider wird dagegen normal gemeldet). |
| FS-29 | Signal-Handler bleiben nach graceful Shutdown installiert (nur eingebettete Nutzung). |
| FS-32 | Nach `handle.cancel()` meldet `status()` bis zur TTL `running`, wenn kein Schritt mehr fertig wird; die Doku nennt `cancelled`. |

## Offene Fragen an Georg

1. **E-001 überdenken (FS-01)?** Neues Argument siehe oben. Optionen: Drain-Abbruch als Neuzustellung ohne Fehlerergebnis, oder KTD2 behalten und nur FS-02 beheben (dann gilt „Fehlerergebnis + DLQ“, aber ohne erneuten Lauf). Dazu die widersprechende Doku-Zeile korrigieren.
2. **BZ-11-Entscheidung überdenken (FS-11)?** Für auf 0 skalierte Pools (KEDA).
3. **Polling-Entscheidung überdenken (FS-15)?** Pub/Sub über die ganze Wartezeit lesen.
4. **FS-04, Nebenfrage:** Das Ack kommt vor dem Schließen des DI-Scopes, ein scheiternder Commit im Finalizer also nach dem Ack. Gewollt?
5. **Lücken FS-25, FS-26, FS-29, FS-32:** beheben oder als Grenze dokumentieren?

## Testlücken ohne gefundenen Fehler (Auswahl)

- Kein Unit-Test prüft, dass ein graceful Shutdown laufende Tasks fertig werden lässt (der Mutant „Drain-Frist 0“ überlebt).
- Nicht geprüft: `lock_wait_max_s` über mehrere Kopien, der LockKeeper-Timeout bzw. zwei gleichzeitig gehaltene Locks und `_check_late_cancel` bei Valkey-Ausfall.
- Unique-Header beim Cron-Fire und in `schedule_many` mit gemischtem Batch.
- Für den Zwei-Marker-Absturzfall aus Plan 2026-04-17-003 (H-2) fehlt der Test (siehe FS-03).
- TTL von Chord-Zähler und Workflow-Ergebnis; Pool-Schließen mit aktivem Kind; fd-Schließen in `_finalize`.
- Das Valkey-Backend wird nur über Integrationstests geprüft (512 Mutanten ohne Unit-Test).

## Geprüft und korrekt

Cron bei Zeitumstellung (Berlin, New York, Lord Howe), sieben Workflow-Kombinationen, Erfolgspfad Speichern → Ack, Retry über die Delay-Queue, Workflow-Weiterschaltung (Lua), DLQ-Replay-Erkennung, Namensraum-Konsistenz, Delay-Stufen, Superseded-Commit, `schedule_many` bei Teilfehlern.

## Nachweis-Tests

| ID | Testdateien | Status |
|---|---|---|
| FS-02 | `tests/unit/broker/test_befund_fs02_publish_after_deadline.py`, `tests/unit/worker/test_befund_fs02_recycling_publish_deadline.py` | behoben (U3, 7a2bb19; Review f79fc04, 1f2f1eb) |
| FS-03 | `tests/unit/worker/gates/test_befund_fs03_schedfire_redelivery.py` | behoben (Teil 2: U4, 4fbbd97; Teil 1: U5, de10d90; Review 7ede93e, 4dbc28f) |
| FS-04 | `tests/unit/worker/test_befund_fs04_dishka_finalizer.py` | behoben (U4, 4fbbd97; Review 01cb43a, f21459f) |
| FS-05, FS-20 | `tests/unit/worker/test_befund_fs05_task_timeout_error.py` | behoben (U18, 2632029; Review 06958c8) |
| FS-06 | `tests/integration/backend/test_befund_fs06_retry_after_lost_reply.py` | behoben (Fire-Claim U6, c8f407a; Unique-Claim und Mutex U11, 86798d1; Review 72036b1) |
| FS-07 | `tests/unit/scheduler/test_befund_fs07_unique_claim_after_timeout.py`, `tests/unit/worker/gates/test_befund_fs07_lock_claim_after_timeout.py` `tests/unit/client/test_befund_fs07_client_claim_after_timeout.py` | behoben (U12, 2dc73f8; Review 21a1d46) |
| FS-08 | `tests/integration/broker/test_befund_fs08_lock_waiter_aiormq_7_2.py` (gegen echtes aiormq 7.2; ersetzt den Unit-Nachbau `tests/unit/broker/test_befund_fs08_publish_lock_waiter.py`, Plan 2026-10-29-1945, KTD5) | behoben (U8, 67cb34b; Review 44722f6); Ursache seit aiormq 7.2 (#240) upstream behoben |
| FS-09 | `tests/unit/broker/test_befund_fs09_consumer_watch_rpc_abort.py` | behoben (U10, 7643879; Review 3348b96) |
| FS-10 | `tests/integration/broker/test_befund_fs10_dlq_replay_counters.py` (maßgeblich), `tests/unit/worker/test_befund_fs10_dlq_copy_counters.py` | behoben für Worker-Kopien (U16, b69dd89; Review 7a50258); Grenze: Broker-DLX-Kopien behalten den Zähler (xfail) |
| FS-12 | `tests/unit/worker/test_befund_fs12_cancel_before_start.py` | behoben (U15, 4956358; Review 546cd08) |
| FS-13 | `tests/unit/worker/test_befund_fs13_unique_until_start.py` | behoben (U15, 4956358, 58f069b) |
| FS-14 | `tests/unit/scheduler/test_befund_fs14_oneshot_unique_none.py` | behoben (U13) |
| FS-15 | `tests/unit/backend/test_befund_fs15_result_wait_polling.py` (nur `subscribe()`-Frist) | behoben (U20, 1b05cd9; Review d2aa879) |
| FS-16 | `tests/unit/worker/compute/test_befund_fs16_kill_group.py` | behoben (U19, 13472d9; Review 30e536c) |
| FS-30 | `tests/unit/worker/test_befund_fs30_recycling_drain.py` | behoben (U9, 177b954; Review 38634cf) |
| FS-31 | `tests/unit/scheduler/test_befund_fs31_leader_renew_timeout.py` | behoben (U14, 9c9fe19; Review b8d7a78) |
| FS-17 | `tests/unit/worker/gates/test_befund_fs17_expiry_ack_failed.py` | behoben (U16, b69dd89) |
| FS-19 | `tests/unit/worker/test_befund_fs19_counter_headers.py` | behoben (U17, 29f9a3e; Review 06958c8) |
| FS-23 | `tests/unit/worker/compute/test_befund_fs23_blocked_slot.py` | behoben (U19, 13472d9; Review 30e536c) |
| FS-24 | `tests/unit/worker/compute/test_befund_fs24_handshake.py` | behoben (U19, 13472d9) |
| FS-27 | `tests/unit/test_namespace_lowercase.py` | behoben (U20, 1b05cd9) |
| FS-28 | `tests/unit/worker/test_befund_fs28_no_store_step_message.py` | behoben (U17, 29f9a3e) |

Entfernt nach der Gegenprüfung: die FS-01-Nachweise für IO und CPU (sie prüften die von KTD2 verworfene Option), der FS-11-Nachweis (widersprach der BZ-11-Entscheidung und `test_unowned.py`) sowie zwei FS-15-Nachweise zum Polling-Intervall (entschieden).

Quellberichte der Gegenprüfung (Scratchpad der Sitzung): 5-gegenpruefung-A.md (FS-01–05, 17–21), -B.md (FS-06–11, 22–25), -C.md (FS-12–16, 26–29).
