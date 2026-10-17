# Fehlersuche Runde 4 – Spur 2c: Auswertung der Störungsläufe (U14)

Stand 2026-10-17. Plan `docs/plans/2026-10-17-0941-test-fehlersuche-runde-4-plan.md` (R11, R13, KTD5, KTD9, U14).
Eingaben: Erwartungstabelle `2b-erwartungen.md` (U11), Laufbericht der Loadsuite
`docs/testlauf-stoerung-v0.12.3.md` (U13, Worktree `swarmq-loadsuite-stoerungen`, Branch `feat/stoerungsarten`),
Artefakte `runs/stoerung-smoke-20261017T132823/` und `runs/stoerung-1h-20261017T135831/` (`report.json`,
`dispatch.jsonl`, `events.jsonl`, `publish_indeterminate.json`) samt gesicherten Logs `runs/<id>.logs/`
(Worker, Broker, Valkey, Runner). swarmq-Code am Arbeitsstand 0.12.3 (`src/swarmq/` unverändert).

Ausgewertet wurde je Fenster: Fenstergrenzen und Budgets aus `report.json` (`c_recovery.stoerungen`), Worker-
Zeilen ab WARNING plus `duplicate_delivery_skipped`, `valkey_data_loss_detected` und alle `scheduler_*` je
Fenster bis Ende + Budget (Skript im Scratchpad `…/scratchpad/u14/fenster.py`), Einzelverläufe je `task_id`
für alle 11 Duplikate, die beiden E-041-Belege und den Fall `SettleFailedAfterPublish`, dazu Broker-Log
(Alarmzeitpunkte) und Runner-Log (Zeitpunkt von `connection.blocked`). Keine Reproduktion im Repo gefahren;
die Mechanismen sind aus Log und Code belegt, die Reproduktionsvorschläge stehen je Verdacht.

## Ergebnis in Kürze

- **4 Verdachte** R4I-01 bis R4I-04, alle P3, davon zwei Produktfragen (R4I-01, R4I-02) und zwei Log-/
  Beobachtbarkeitspunkte (R4I-03, R4I-04). Kein Verlust, keine Doppelausführung ohne Erklärung, kein
  Widerspruch zu einer Entscheidung.
- **Alle 11 ausgeführten Duplikate** gehen auf einen Pfad zurück: Erfolg, dann scheitert das Speichern des
  Ergebnisses, `redeliver_in_place`, Running-Marker freigegeben, die Neuzustellung führt den Task vollständig
  erneut aus (R4I-01). Das Verhalten ist dokumentiert (`website/reference/logging.md:676`,
  `website/explanation/architecture.md:216`), aber nicht im Register; die Neuausführung lief jedes Mal erst,
  nachdem Valkey wieder da war.
- **Gate (c) im 1-h-Lauf** ist ein Fensterlage-Artefakt der Suite (S-1), kein swarmq-Verdacht.
- **Tabellenkorrekturen** an `2b-erwartungen.md`: sechs Stellen (Abschnitt „Tabellenkorrekturen“).
- **Ausführungsschutz nach Datenverlust** (offene Frage aus U11): in den Läufen nicht ausgelöst; das Verhalten
  ist in `architecture.md:217` dokumentiert („redeliveries can run twice“), im Register fehlt es.

## Tabelle Fenster × Erwartung × Beobachtung × Einstufung

Erholung als Backlog / e2e-p95 ab Fensterende (Budget). „Worker“ meint Zeilen der swarmq-Worker im Fenster
bis Ende + Budget; die Spitzenfenster des 1-h-Laufs überlappen sich dadurch, die Zuordnung folgt U13.

### stoerung-smoke-20261017T132823 (bestanden)

| Fenster (UTC) | Art | Erwartung (2b) | Beobachtung | Erholung | Einstufung |
|---|---|---|---|---|---|
| 13:30:08, 68,5 s | `broker_alarm` (Grundlast) | erster Publish je Verbindung `PublishIndeterminate`, dann `BrokerPublishBlocked` nach Frist; Acks laufen | Runner: `connection.blocked` 0,6 ms nach Alarmbeginn (Broker 13:30:09.105, Runner 13:30:09.1057), 383 × `BrokerPublishBlocked`, **0** `PublishIndeterminate`. Worker: keine Zeile; zwischen 13:30:10 und 13:31:17 kein einziger `task_succeeded`, weil ohne Backlog nichts zuzustellen war (Grundlast 23/s, Kapazität ≫) | 24,6 / 50,7 s (180) | wie erwartet; Tabelle zu absolut (T-1). Der Smoke prüft im Alarm nur die Produzentenseite (S-2) |
| 13:33:08, 60 s | `toxic_bandwidth` rabbitmq 16 kbit/s | Latenzanstieg, Erholung im Budget | Publish p95 16 → 48,7 ms, keine Fehlerzeile | 5,9 / 29,2 s (120) | wie erwartet |
| 13:35:08, 60 s | `toxic_slicer` valkey 16 B | Latenzanstieg, kein Protokollfehler | keine Fehlerzeile; Wirkung nur über e2e-p95 sichtbar | 13,3 / 44,2 s (120) | wie erwartet |
| 13:38:58, 63,7 s | `queue_max_length` 1 (Spitze) | `PublishRejected` als `publish_failed`, kein Verlust | 5.210 `PublishRejected`, 0 verloren, keine Worker-Zeile | 268,3 / 290,6 s (300) | wie erwartet |
| 13:46:38, 7,7 s | `rabbitmq_restart` | Reconnect, Redelivery, Schutz unterdrückt Zweitzustellungen | 4 `delivery_settle_failed` = 4 `duplicate_delivery_skipped`; 2 `consumer_watch_channel_timeout`; 4 `log_record_unformattable` (B11); Runner 1 `BrokerConnectionError` (nicht gesendet, E-042) | 6,7 / 36,6 s (120) | wie erwartet |
| 13:49:38, 3,5 s | `valkey_restart` mit Persistenz | `task_deferred_backend_unavailable`, Leader-Rücktritt und Rückkehr, kein Verlust | 2 `task_deferred_backend_unavailable`, 3 `backend_connection_error` aus `worker.results`, **3 Duplikate** (Erfolg → Speichern scheitert → zweiter Lauf 1,3–1,4 s später); kein `scheduler_renew_*`, kein Rücktritt | 9,0 / 25,7 s (120) | Duplikate: **Abweichung → R4I-01**; fehlender Rücktritt: wie erwartet nach E-048, Tabelle falsch (T-3) |
| 13:52:08, 4,4 s | `valkey_restart` ohne Persistenz | zusätzlich `valkey_data_loss_detected`, Epoch neu, keine Doppelausführung ohne E-041-Beleg | `dbsize_nach_start` 0; 17 `task_deferred_backend_unavailable`; `scheduler_poll_due_failed` 1, `scheduler_lock_lost` 1; `valkey_data_loss_detected` 13:52:36 (24 s nach Fensterende, je Prozess einmal); **3 Duplikate** im selben Muster (Speichern wartete 3,1 s auf „Error 111 connecting“, zweiter Lauf 4,9 s nach dem ersten) | 5,7 / 39,8 s (120) | Duplikate: **R4I-01** (nicht durch verlorene Done-Marker); Datenverlust-Meldung wie erwartet, Zeitpunkt siehe T-4 |

### stoerung-1h-20261017T135831 (durchgefallen, nur Kriterium c)

| Fenster (UTC) | Art | Erwartung (2b) | Beobachtung | Erholung | Einstufung |
|---|---|---|---|---|---|
| 14:18:57, 5,1 s | `toxic_down` rabbitmq (Spitze; nicht in 2b) | E-042: `PublishIndeterminate` nur nach Senden | Runner 3 `PublishIndeterminate` („lost the connection while waiting for the broker's confirm“), davon 2 ausgeführt, 1 nicht; 1 `BrokerConnectionError`; Worker 37 `delivery_settle_failed`, 40 `duplicate_delivery_skipped`, 1 `retry_publish_failed[SettleFailedAfterPublish]` | **945,2 / 973,7 s (600)** | Reaktion wie erwartet (E-041/E-042); Log-Punkt **R4I-03**; Gate-Riss Suite-Artefakt (S-1) |
| 14:19:17, 68,1 s | `broker_alarm` (Spitze) | wie Smoke; Acks laufen weiter | Alarm im Broker 14:19:18.644. Runner `connection.blocked` 8 ms danach; 3 Publishes waren in diesen 8 ms schon auf dem Socket → `PublishIndeterminate` nach 20 s, alle 3 ausgeführt; 657 `BrokerPublishBlocked`. Worker io-1 blockiert 14:19:18.701 (eigener Publish 14:19:18.700 → `PublishIndeterminate`), io-2 erst 14:19:19.364 beim eigenen ersten Publish (→ `PublishIndeterminate`). 119 `publish_connection_blocked`, 85 `retry_publish_failed[BrokerPublishBlocked]`, 2 `retry_publish_failed[PublishIndeterminate]`, 2 `publish_channel_close_incomplete[TimeoutError]`, 2 aiormq-ERROR „Got frame for closed channel“ bei der Aufhebung. Abschlüsse je 10 s (beide IO-Worker) 1.907 → 595, 128, 591, 242, 569, 323, nach Aufhebung 2.004–2.436 | **862,1 / 890,7 s (600)** | Acks: wie erwartet (kein Stillstand). Durchsatzeinbruch: **Abweichung → R4I-02**. Kanal-Schließen im Alarm: **R4I-04**. Gate-Riss S-1 |
| 14:20:47, 120 s | `toxic_bandwidth` rabbitmq (Spitze) | Latenz, Backlog, Erholung | Publish p95 196 ms → 3,1 s; keine Fehlerzeile | **705,0 / 733,8 s (600)** | wie erwartet; Gate-Riss S-1 |
| 14:23:17, 120 s | `toxic_slicer` valkey (Spitze) | Latenz, kein Protokollfehler | kein Protokollfehler; 5 `task_sent_to_dlq[Retry]` (paginierter Sync, Retries erschöpft durch Mock-Fehlerrate 5 %) | 483,5 / 583,8 s (600) | wie erwartet; DLQ-Einträge sind Szenario-Folge (S-5) |
| 14:37:47, 7,7 s | `rabbitmq_restart` (Soak) | wie Smoke | 7 `delivery_settle_failed` = 7 `duplicate_delivery_skipped`; 8 `consumer_watch_channel_timeout`; 30 `log_record_unformattable` (B11); 4 `metrics_sample_failed`; 1 `scheduler_queue_check_failed` | 6,0 / 36,1 s (120) | wie erwartet |
| 14:42:47, 3,5 s | `valkey_restart` mit Persistenz | wie Smoke | 2 `task_deferred_backend_unavailable`, 3 `backend_connection_error`, 1 `update_progress_hset_failed` (E-044); **3 Duplikate** (zweiter Lauf 1,4–1,5 s später, einer auf dem anderen IO-Worker); kein Rücktritt | 5,2 / 25,2 s (120) | Duplikate **R4I-01**; Rücktritt T-3; Fortschritt wie erwartet (E-044) |
| 14:47:47, 122,1 s | `queue_max_length` 1 (Soak) | `PublishRejected` | Policy wirksam, **0** Ablehnungen (Last 25/s gegen Kapazität ≈ 200/s, Queue hielt nie eine wartende Nachricht) | 9,0 / 26,7 s (120) | **Suite-Artefakt** (S-3) |
| 14:53:47, 4,1 s | `valkey_restart` ohne Persistenz | wie Smoke | `dbsize_nach_start` 0; 32 `task_deferred_backend_unavailable`; `scheduler_poll_due_failed` 14:53:51, `scheduler_lock_lost` 14:53:55; `valkey_data_loss_detected` in allen 4 Workern 14:54:44.96–45.36 (53 s nach Fensterende); **2 Duplikate** im Muster R4I-01 (beide zweiten Läufe scheiterten am Mock und liefen über Retry zu Ende) | 6,0 / 39,6 s (120) | Duplikate **R4I-01**; Datenverlust-Meldung T-4; Leader-Rückkehr nicht beobachtbar (S-4) |

Ledger gesamt: Smoke 0 verloren, 6 Duplikate (0 hingenommen), 0 `publish_failed_unbestimmt_ausgefuehrt`; 1 h 0
verloren, 5 Duplikate (0 hingenommen), 5 `publish_failed_unbestimmt_ausgefuehrt` (2 Trennung, 3 Alarm, beides
E-041/E-042: der Produzent sah einen unbestimmten Ausgang, der Broker hatte die Nachricht). `task_lock_lost` 0
in beiden Läufen (die Suite-Tasks nutzen keine Locks). `task_defer_failed_redelivered`,
`task_expired_dlq_publish_failed`, `dlq_publish_failed`, `execution_guard_lease_lost`,
`execution_guard_finish_failed`: 0.

## Die neun Beobachtungen aus U13

1. **`broker_alarm` im Smoke ohne `PublishIndeterminate`.** Kein Widerspruch zu E-025, die Tabelle ist zu
   absolut. Der Broker schickte dem Runner `connection.blocked` 0,6 ms nach dem Alarm, ohne dass ein Publish
   dazwischen lag (Rate 23/s, nächster Publish erst 53 ms später, sah schon `blocked` und sendete nicht). Im
   1-h-Lauf kamen die drei unbestimmten Runner-Publishes genau aus den 8 ms zwischen Alarm und Notifikation;
   die Worker-Verbindungen (zwischen Publishes ruhend) bekamen die Notifikation erst mit ihrem eigenen ersten
   Publish, der dann unbestimmt endete (io-2: 720 ms nach Alarmbeginn). Unbestimmt enden also die Publishes,
   die vor dem Eintreffen von `connection.blocked` schon gesendet waren: bei einer gerade publizierenden
   Verbindung null bis wenige, bei einer ruhenden der erste. Die Formulierung in E-025 („Grenze“) und
   `website/reference/errors.md:573-575` („the first publish after the alarm starts is already on the wire“)
   beschreibt nur den zweiten Fall; Präzisierung als Doku-Hinweis D-1, kein Verdacht. Dass die Worker im
   Smoke keine Zeile schrieben, folgt aus dem fehlenden Backlog (S-2).
2. **Konsum im Alarm auf ein Viertel.** Mechanismus belegt: Jede Retry-Kopie (Mock-Fehlerrate 5 %) wartet
   auf der blockierten Verbindung bis zur Frist und hält dabei den Slot. 40 IO-Slots, Taskdauer ≈ 0,2 s,
   Frist 20 s: erwartet ≈ 40 / (0,2 + 0,05 × 20) ≈ 33 Abschlüsse/s, gemessen 13–59/s. Acks liefen (kein
   Stillstand, die Queue wurde nach der Aufhebung abgebaut), das „Fehler“-Kriterium der Tabelle („hängende
   Acks“) trifft also nicht. Mit der Vorgabe-Frist 120 s fiele der Durchsatz rechnerisch auf ≈ 6,5/s (rund
   3 %). Eingestuft als Produktfrage **R4I-02**. `publish_channel_close_incomplete` und „Got frame for closed
   channel“: Nach `PublishIndeterminate` verwirft swarmq den Publish-Kanal und schließt ihn entkoppelt mit 5 s
   Frist (`broker/rabbitmq/connection.py:537-548`); im Alarm liest der Broker den Socket nicht, das Close-Ok
   kommt erst bei der Aufhebung, aiormq kennt den Kanal nicht mehr und loggt ERROR. Harmlos im Ausgang,
   laut im Log: **R4I-04**.
3. **`valkey_restart` mit Persistenz ohne Leader-Rücktritt.** Wie erwartet nach E-048: Rücktritt erst nach
   `N = max(1, 30 // 10 − 1) = 2` aufeinanderfolgenden Renew-Fehlern oder bei Erreichen der Zeitgrenze
   (Renew-Start + 30 s − Marge, `scheduler/loop.py:196-199`, E-048 Nachtrag FS-31). Ein Ausfall von 3,5 s
   trifft bei einem Heartbeat von 10 s höchstens einen Renew; im Log steht kein `scheduler_renew_error`, also
   traf er keinen. Tabelle falsch (T-3). Im Fenster ohne Persistenz fand der Renew den Key leer und lieferte
   `False` → sofortiger Standby (`scheduler_lock_lost`, E-048 erster Satz); die Rückkehr ist in Logs und
   Ereignissen nicht sichtbar, weil swarmq den Erwerb nur als Signal meldet und die Suite die
   Scheduler-Signale nicht aufzeichnet (S-4).
4. **Alle 11 Duplikate nach gescheitertem Ergebnis-Speichern.** Nicht durch eine Entscheidung gedeckt, aber
   dokumentiert und im Code ausdrücklich so gebaut: `worker/results.py:313-321` loggt WARNING
   `backend_connection_error` und ruft `redeliver_in_place`; der Ausführungsschutz gibt den Running-Marker vor
   dem `nack` frei (`worker/guarded_broker.py:69-73`), die Neuzustellung erhält einen frischen Claim und läuft
   von vorn. E-046 (at-least-once) gilt nur für Schedfires, E-009 nur für Replays, E-043 nur für die DLQ-Kopie;
   E-041 nur für unbestimmte Kopien. Kein Duplikat folgte aus verlorenen Done-Markern: Der Done-Marker wird
   erst mit dem Ack gesetzt, und dazu kam es nicht. Jeder zweite Lauf startete, nachdem Valkey wieder
   antwortete: Mit Persistenz war Valkey laut eigenem Log nach 0,8 s (Smoke, SIGTERM 13:49:38.593 → „Ready
   to accept connections“ 13:49:39.427) bzw. 1,3 s (1 h) wieder bereit, die zweiten Läufe kamen 0,6 bzw. 0,3 s
   danach; ohne Persistenz nach Wiederanschluss des Proxys am Fensterende. Eine kurze Wiederholung des
   Speicherns hätte alle 11 vermieden. **R4I-01** (Produktfrage, dazu Doku-Lücke in
   `delivery-semantics.md`).
5. **E-041-Belege ohne Duplikat.** Wie entschieden. Ablauf für beide task_ids: Task scheitert (Mock),
   Retry-Kopie bleibt im Alarm hängen, `PublishIndeterminate` nach 20 s, Original per `nack` zurück, läuft
   erneut (das ist der „mindestens ein weiteres Mal“-Lauf aus E-041), endet mit Erfolg; die unbestimmte Kopie
   war beim Broker angekommen, kam nach der Aufhebung mit `attempt 2` (14:20:39/41) und wurde als
   `duplicate_delivery_skipped reason=done` bestätigt. Ein zweiter Erfolg entsteht nur ohne
   Ausführungsschutz oder mit verlorenem Done-Marker. Tabelle anpassen (T-2).
6. **`queue_max_length` im Soak ohne Wirkung.** Suite-Kalibrierung (S-3): Mit `overflow: reject-publish`
   lehnt der Broker erst ab, wenn die Queue schon `max-length` wartende Nachrichten hält; im Soak werden alle
   sofort zugestellt.
7. **`retry_publish_failed[SettleFailedAfterPublish]` im `toxic_down`.** Ausgang korrekt, Log falsch:
   Die Retry-Kopie lag bestätigt in `.DQ`, nur das Ack des Originals scheiterte. `GuardedBroker.retry_later`
   setzt den Superseded-Marker (`guarded_broker.py:85-87`), die Neuzustellung des Originals wurde 14 ms später
   als `superseded` übersprungen, die Kopie lief mit `attempt 2` (14:19:13). `RetryHandler` loggt aber
   ERROR `retry_publish_failed` („Retry could not be published“, `logging.md:660`) und ruft
   `redeliver_in_place` auf einem toten Kanal (`worker/retry.py:163-171`). E-043 regelt genau diesen Fall für
   die DLQ-Kopie (WARNING `dlq_ack_failed_after_publish`), für die Retry-Kopie fehlt das Gegenstück:
   **R4I-03**.
8. **`valkey_data_loss_detected` 53 s nach Fensterende.** Wie gebaut: Der Epoch wird nur im Melde-Takt
   geprüft (`scheduler/loop.py:324-355`, `_report_loop` → `_observe_epoch`, alle
   `scheduler_seen_interval_s` = 60 s, in jeder Rolle). Die Erkennung kommt 0–60 s nach Valkeys Rückkehr,
   in allen Prozessen etwa gleichzeitig, weil die Takte seit dem Start gleich laufen (Smoke 24 s, 1 h 53 s).
   Tabelle ergänzen (T-4). Bis dahin arbeiten Worker und Leader ohne Hinweis; das ist durch E-005 (Grenzen)
   und den Docstring von `scheduler/epoch.py` abgedeckt.
9. **Runner loggt swarmq-Zeilen unformatiert.** Suite-Punkt (S-6): Der Runner konfiguriert keinen
   stdlib-Handler für den Logger `swarmq`, also greift Pythons `lastResort` (nur die Nachricht).
   `website/reference/logging.md:4-13` sagt das so zu („As a library it never configures logging … without
   any, WARNING and above reach Python's `lastResort` handler“).

Weitere Punkte aus dem U13-Bericht, ohne eigene Nummer dort: `toxic_slicer` auf Valkey ohne Protokollfehler
(wie erwartet); 5 `publish_failed_unbestimmt_ausgefuehrt` im 1-h-Lauf (wie erwartet, E-041/E-042); 10
DLQ-Einträge aus erschöpften Retries (Szenario, S-5).

## Offene Fragen aus 2b

- **Ausführungsschutz nach Valkey-Datenverlust.** In beiden Läufen nicht ausgelöst: Alle Duplikate im
  Fenster ohne Persistenz folgen R4I-01 und wären mit Persistenz gleich verlaufen (dort ebenfalls 3 bzw. 3).
  Eine Neuzustellung einer schon bestätigten Ausführung nach dem Flush gab es nicht (sie entsteht nur, wenn
  ein Ack scheitert und die Neuzustellung erst nach dem Flush ankommt). Das Verhalten ist in
  `website/explanation/architecture.md:217` dokumentiert: „Without persistence … the execution-guard and fired
  markers are lost; redeliveries can run twice“; E-005 nennt unter Grenzen nur die Unerkennbarkeit. Einstufung:
  dokumentiert, nicht im Register. Vorschlag: als Regel ins Register (gilt bei Valkey ohne Persistenz, dazu der
  Verweis auf `appendonly yes`), kein Verdacht. Nachweis im Repo wäre ein Integrationstest: Ack einer
  Ausführung scheitern lassen (Kanal schließen nach dem Done-Marker), Valkey-DB leeren, Neuzustellung läuft.
- **Deckt E-041 die Pfade Zurückstellung und Ablauf-DLQ?** In beiden Läufen kein Fall
  (`task_defer_failed_redelivered` und `task_expired_dlq_publish_failed` je 0), also keine Messung. Im Code
  ist die Logik dieselbe (unbestimmte Kopie, Original per `nack` zurück, Kommentar „Duplikat statt Verlust,
  at-least-once“ in `worker/gates/backend_defer.py`). Bleibt Registerfrage für Georg (Nachtrag in E-041 oder
  eigene Regel); bis dahin weist die Loadsuite diese Pfade getrennt aus, wie in 2b beschrieben.

## Verdachtsliste

### R4I-01 Erfolgreicher Task läuft nach gescheitertem Ergebnis-Speichern vollständig erneut

- **Lauf, Fenster:** `stoerung-smoke-20261017T132823` (`valkey_restart` mit Persistenz 13:49:38, ohne
  Persistenz 13:52:08), `stoerung-1h-20261017T135831` (mit Persistenz 14:42:47, ohne 14:53:47). 11 von 11
  Duplikaten beider Läufe; jedes `backend_connection_error` mit task_id führte zu genau einem.
- **Logauszug** (Smoke, `worker-io-1.log`, task `01a14a20-12ce-755e-8b23-7c497c4638a3`):
  ```
  13:49:38.636 task_succeeded            attempt 1
  13:49:38.639 backend_connection_error  BackendConnectionError "Error while reading from toxiproxy:6379 : (104, 'Connection reset by peer')"
  13:49:39.990 task_succeeded            attempt 1
  ```
  Ohne Persistenz (task `01a14a22-5c84-75d4-8fa3-c5ab609f2323`): `task_succeeded` 13:52:08.559,
  `backend_connection_error` „Error 111 connecting“ 13:52:11.674, zweites `task_succeeded` 13:52:13.482.
- **Fundstelle:** `src/swarmq/worker/results.py:313-321` (`ResultTail.finish`: Speichern scheitert →
  `redeliver_in_place`, Kommentar „redelivery will re-run the task from scratch“),
  `worker/guarded_broker.py:69-73` (Running-Marker vor dem `nack` frei), `worker/gates/__init__.py:332-341`
  (Pause, dann `nack`).
- **Quelle der Erwartung:** Kein Registereintrag. Dokumentiert in `website/reference/logging.md:676`
  („WARNING for `AT_LEAST_ONCE`: the message is redelivered and the task runs again“) und
  `website/explanation/architecture.md:216` („a failed result write requeues the message, and the task runs
  again“). Dagegen nennt `website/explanation/delivery-semantics.md:231-265` („Duplicates appear in three
  situations“) diesen Fall nicht. Loadsuite-Backlog B23 sah das Muster schon 2026-10-08 (40 Duplikate bei
  30 s Ausfall) und wertete nur die Logzeile. Erwartungstabelle 2b nennt für den Valkey-Neustart mit
  Persistenz keine Duplikate.
- **Einstufung:** Produktfrage mit Doku-Lücke, kein Fehler gegen eine Entscheidung. Der Task war erfolgreich;
  wiederholt wird nicht das Speichern, sondern die Ausführung samt Nebenwirkung. Alternativen: begrenzte
  Wiederholung von `store_result` im Worker (etwa bis `backend_op_timeout` oder mit dem Backoff der
  Zurückstellung), Zurückstellung einer Kopie, die das Ergebnis trägt, oder Entscheidung „hingenommen“ ins
  Register plus vierter Punkt in `delivery-semantics.md`.
- **Realismus:** Hoch in der Häufigkeit, begrenzt in der Menge. Jeder Valkey-Neustart, Failover oder
  Netzaussetzer trifft die Tasks, die gerade fertig werden (begrenzt durch die Slots, hier 3 je Ausfall bei
  25/s). Die Neuausführung kommt erst, wenn Valkey wieder antwortet (Valkey-Log: mit Persistenz 0,8 bzw. 1,3 s
  Ausfall); eine Wiederholung des Speicherns über wenige Sekunden hätte alle 11 Fälle vermieden.
- **Konfidenz:** hoch (Mechanismus in Log und Code eindeutig). Priorität P3.
- **Reproduktion im Repo:** `tests/chaos` (Chaos-Stack mit Toxiproxy für Valkey): Task-Körper zählt
  Ausführungen und wartet auf ein Event; Test schaltet den Valkey-Proxy ab, setzt das Event, schaltet den
  Proxy nach 1 s wieder an; erwartet laut heutigem Code 2 Ausführungen, gewünscht 1. Deterministische
  Vorstufe in `tests/integration` gegen den Teststack: `backend.store_result` beim ersten Aufruf
  `BackendConnectionError` werfen lassen (Wrapper um die echte Instanz), Zähler im Task. Als Nachweis
  `xfail(strict=True)` bis zur Entscheidung.

### R4I-02 Blockierte Worker-Kopien halten ihren Slot bis zur Publish-Frist; der Konsum bricht im Alarm ein

- **Lauf, Fenster:** `stoerung-1h-20261017T135831`, `broker_alarm` 14:19:17–14:20:25 (Spitze, Backlog
  ≈ 90.000).
- **Logauszug** (beide IO-Worker, je 10 s): Abschlüsse 1.907 (14:19:10) → 595, 128, 591, 242, 569, 323,
  845, nach der Aufhebung 2.004 und 2.436; gleichzeitig je 10 s 6–28 neue `publish_connection_blocked` und
  8–28 `retry_publish_failed[BrokerPublishBlocked]` (gesamt 119 bzw. 85). Verlauf einer Nachricht
  (`01a14a3b-05db-…`, io-1): `task_failed` 14:19:18.696 → `retry_publish_failed[PublishIndeterminate]`
  14:19:38.700 → zweiter Lauf scheitert 14:19:39.054 → `publish_connection_blocked` 14:19:39.057 →
  `retry_publish_failed[BrokerPublishBlocked]` 14:19:59.058 → dritter Lauf `task_succeeded` 14:19:59.495.
  Der Slot war 41 s belegt, der Task lief dreimal.
- **Fundstelle:** `worker/retry.py:163-171` (Retry-Publish im Slot, bei Fehler `redeliver_in_place`),
  Publish-Wartepfad in `broker/rabbitmq/delivery.py` (`publish_connection_blocked`, wartet bis Aufhebung
  oder Frist).
- **Quelle der Erwartung:** E-025 (Publish wartet bis zur Frist; Begründung: Worker sollen im Alarm weiter
  quittieren können), `website/explanation/architecture.md:215` („a retry or dead-letter copy waits up to
  `SWARMQ_PUBLISH_TIMEOUT_S`, then the message is handed back“). Dass der Slot so lange belegt bleibt, steht
  nirgends; 2b zählt nur „hängende Acks“ als Fehler, und die gab es nicht.
- **Einstufung:** Folge von E-025, nicht entschieden in ihrer Wirkung auf den Durchsatz: Produktfrage. Der
  Konsum fällt auf ≈ Slots / (Taskdauer + Fehlerrate × Frist). Gemessen mit Frist 20 s auf ein Viertel; mit
  der Vorgabe 120 s rechnerisch auf rund 3 %. Nach `BrokerPublishBlocked` läuft das Original per `nack` erneut
  (ohne Retry-Zähler, ohne Backoff außer der `nack`-Pause) und belegt den Slot wieder bis zur Frist. Folgefrage
  ohne Messung: Bei langem Alarm zählt jede Runde auf `x-acquired-count`; nach rund 19 Runden (mit 120 s rund
  38 min) erreicht ein Task, der nur einmal gescheitert ist, das Zustelllimit und geht als
  `RedeliveryLimitError` in die DLQ.
- **Realismus:** Mittel. Speicher- oder Plattenalarme sind selten, dauern aber oft Minuten; jede Fehlerrate
  über null reicht, und die Wirkung wächst mit der Frist.
- **Konfidenz:** hoch für den Mechanismus (Rechnung und Messung passen), mittel für die Bewertung.
  Priorität P3.
- **Reproduktion im Repo:** `tests/integration/broker/` auf Basis der Fixture `memory_alarm` aus
  `tests/integration/broker/test_befund_bz16_connection_blocked.py` (Marker `exclusive`): Worker mit
  `concurrency=2`, `SWARMQ_PUBLISH_TIMEOUT_S=10`, zwei Tasks, die mit `Retry` scheitern, danach zehn schnelle
  Tasks; Alarm auslösen. Erwartet heute: die schnellen Tasks starten erst nach Ablauf der Frist. Alternativ
  ohne Broker-Alarm mit `tests/support/rabbitmq_faults.py::hanging_publish` (der hängende Publish belegt den
  Slot ebenso), dann als Unit-naher Integrationstest ohne Sperre des ganzen Brokers.

### R4I-03 `retry_publish_failed` auf ERROR, obwohl die Retry-Kopie bestätigt in der Delay-Queue liegt

- **Lauf, Fenster:** `stoerung-1h-20261017T135831`, `toxic_down` 14:18:57 (Spitze).
- **Logauszug** (task `01a14a3a-dd18-73fe-84a7-b3bb257cb60b`):
  ```
  14:18:57.975 task_failed                Retry        attempt 1   (io-1)
  14:19:02.876 retry_publish_failed       SettleFailedAfterPublish "ack failed, channel or connection lost"   ERROR
  14:19:02.890 duplicate_delivery_skipped reason=superseded attempt 1   (io-1)
  14:19:13.150 task_succeeded                          attempt 2   (io-2)
  ```
- **Fundstelle:** `worker/retry.py:163-171` fängt jede Ausnahme von `retry_later`, auch
  `SettleFailedAfterPublish`, loggt ERROR und ruft `redeliver_in_place`; `worker/guarded_broker.py:85-87`
  behandelt denselben Fall richtig (Superseded-Marker mit Rückstands-TTL).
- **Quelle der Erwartung:** E-043 (für die DLQ-Kopie: WARNING `dlq_ack_failed_after_publish`, terminal);
  `website/reference/logging.md:660` („Retry could not be published; the message is redelivered“) trifft hier
  nicht zu. Log-Vertrag 0.8.0: ERROR nur für Datenverlust oder einen gescheiterten Task.
- **Einstufung:** Abweichung im Log, Ausgang korrekt (keine Doppelausführung, Kopie lief). Ein ERROR je
  Trennung pro Retry im Flug, der „Retry konnte nicht veröffentlicht werden“ behauptet, führt Betreiber in die
  Irre; `task_retrying` fehlt für einen tatsächlich erfolgten Retry.
- **Realismus:** Hoch bei jeder Broker-Trennung unter Last mit Fehlerrate. **Konfidenz:** hoch.
  Priorität P3.
- **Reproduktion im Repo:** Unit in `tests/unit/worker/test_retry.py` (Muster
  `test_result_store_failure_logs_warning_with_error_fields`): `broker.retry_later =
  AsyncMock(side_effect=SettleFailedAfterPublish("ack failed"))`, erwartet kein ERROR `retry_publish_failed`,
  stattdessen WARNING wie bei E-043 und `task_retrying`. Integrationsvariante: Consume-Kanal nach dem
  Confirm der Kopie schließen.

### R4I-04 Publish-Kanal wird im blockierten Zustand geschlossen; Close läuft in die Frist, aiormq meldet bei der Aufhebung ERROR

- **Lauf, Fenster:** `stoerung-1h-20261017T135831`, `broker_alarm` 14:19:17.
- **Logauszug** (io-1):
  ```
  14:19:38.700 retry_publish_failed           PublishIndeterminate (Frist 20 s)
  14:19:43.701 aiormq.channel WARNING         Closing channel <Channel: "2"> because RPC call <Channel.Close> cancelled
  14:19:43.702 publish_channel_close_incomplete TimeoutError
  14:20:25.833 aiormq.connection WARNING      ... was unblocked
  14:20:25.838 aiormq.connection ERROR        Got frame for closed channel 2: <Channel.CloseOk>
  ```
  Gleiches Muster auf io-2 mit Kanal 1.
- **Fundstelle:** `broker/rabbitmq/connection.py:537-548` (`_close_channel_detached`, 5 s Frist
  `_CHANNEL_CLOSE_TIMEOUT_S`), aiormq `connection.py` (Rahmen für unbekannten Kanal → ERROR). Nebenbefund
  aus dem aiormq-Code, nicht gemessen: aiormq vergibt die nächste Kanalnummer als `max(channels) + 1` bzw.
  den alten Wert, wenn die Tabelle leer ist; auf einer Publish-Verbindung mit nur einem Kanal kann die Nummer
  des verworfenen Kanals wiederverwendet werden, bevor dessen Close-Ok eintrifft. Folge wäre ein einzelner
  gescheiterter Kanal-Öffnungsversuch, den E-042 als „nicht gesendet“ wiederholt.
- **Quelle der Erwartung:** E-042 Nachtrag FS-08 (Kanal verwerfen ist gewollt); `publish_channel_close_incomplete`
  ist dokumentiert. Dass im Alarm ein ERROR einer Fremdbibliothek folgt, nicht.
- **Einstufung:** Log-Lärm, kein falscher Ausgang. Ein ERROR je unbestimmtem Publish im Alarm, ohne
  Task-Kontext. Möglicher Ansatz: Close im blockierten Zustand bis zur Aufhebung zurückstellen oder die
  aiormq-Zeile wie B11 abfangen.
- **Realismus:** Mittel (jeder Alarm mit Publish im Flug). **Konfidenz:** hoch für den Ablauf, niedrig für
  die Kanalnummer-Wiederverwendung. Priorität P3.
- **Reproduktion im Repo:** `tests/integration/broker/` mit der Fixture `memory_alarm`: Worker publiziert
  eine Retry-Kopie direkt beim Alarm (ruhende Publish-Verbindung → erster Publish unbestimmt), Frist 2 s;
  nach der Aufhebung im Log nach `Got frame for closed channel` suchen. Ohne Alarm nachstellbar mit
  `tests/support/rabbitmq_faults.py::hanging_publish(close_hangs=True)`.

## Suite-Artefakte (kein swarmq-Verdacht)

- **S-1 Gate-(c)-Riss im 1-h-Lauf (Fensterlage).** Die vier Spitzenfenster messen ihre Erholung ab dem eigenen
  Ende und tragen damit den Rest der Spitze mit. Die Spitze selbst erholte sich in 540 / 569 s (Budget 600).
  Die Überschreitungen folgen genau „Abstand Fensterende → Spitzenende + Erholung der Spitze“:
  `toxic_down` endet 405 s vor Spitzenende → 945 s, Alarm 322 s → 862 s, Bandbreite 180 s → 705 s, Slicer
  30 s → 484 s (im Budget). Kalibrierungsvorschlag, in dieser Reihenfolge: (a) Für Fenster in einer Phase
  mit eigenem Recovery-Budget die Erholung ab `max(Fensterende, Phasenende)` messen und gegen das Budget der
  Phase werten; das Fenster bleibt für die Fehlerzählung zuständig. (b) Ersatzweise Budget je Fenster =
  (Phasenende − Fensterende) + Phasenbudget, hier rund 1.005 / 922 / 780 / 630 s. (c) Spitzenfenster ans Ende
  der Spitze legen. Ein Lauf, keine Streuung; vor der 6-h-Klasse mit (a) oder (b) wiederholen.
- **S-2 Alarm im Smoke ohne Backlog.** In der Grundlast gibt es nichts zuzustellen, solange der Produzent
  blockiert ist; die Worker publizieren keine Kopien, die Ack-Seite von E-025 wird nicht geprüft. Vorschlag:
  Alarm in der Spitze oder mit vorgefülltem Backlog fahren (wie im 1-h-Lauf).
- **S-3 `queue_max_length` im Soak ohne Überlast.** 0 Ablehnungen, weil die Queue nie eine wartende Nachricht
  hielt. Vorschlag: Konsumenten für die Fensterdauer pausieren (Kontrollkanal oder `basic.cancel` über die
  Management-API) oder eine kurze Überlast im Fenster; alternativ `max_length: 0`, falls RabbitMQ damit jede
  Veröffentlichung ablehnt (vorher prüfen).
- **S-4 Leader-Wechsel nicht beobachtbar.** swarmq loggt den Erwerb der Leader-Rolle nicht, nur Signale
  (`SCHEDULER_LEADER`, `SCHEDULER_LEADER_FAILOVER`, `SCHEDULER_STANDBY`); die Lastworker zeichnen keine
  Scheduler-Signale auf und haben keine Crons. Die Zeilen „Leader-Rücktritt und Rückkehr“ und „Split-Brain mit
  doppelt ausgeführtem Fire“ der Tabelle sind damit in diesen Läufen nicht prüfbar. Vorschlag: Signal-Hook für
  die drei Scheduler-Signale ins Ereignis-Log, ein Cron im Szenario (etwa alle 10 s), Duplikatprüfung je Fire.
- **S-5 DLQ-Einträge aus der Mock-Fehlerrate.** 10 `task_sent_to_dlq[Retry]` (paginierter Sync mit vier
  Seiten bei 5 % Fehler je Seite); Ledger und Broker-Tiefe stimmen überein. Szenario-Folge, kein Befund.
- **S-6 Runner-Logging.** Der Runner hat keinen stdlib-Handler für `swarmq`; die 511 bzw. 899 Zeilen kommen
  über `lastResort` nur mit dem Ereignisnamen. Vorschlag: im Runner `logging.basicConfig` mit
  `swarmq.logging.build_formatter(json=True)` (Muster in `website/reference/logging.md:90-100`).
- **S-7 Spitzen-Backlog-Minimum.** Eingetragen 5.000, gemessen 90.563; zu niedrig, um eine fehlende Spitze zu
  erkennen (U13-Kalibrierungsnotiz). Nach einem zweiten Lauf aus der Streuung setzen.

## Tabellenkorrekturen an 2b-erwartungen.md

- **T-1 `broker_alarm`:** statt „Der erste Publish je Verbindung … endet als `PublishIndeterminate`“:
  „Publishes, die vor dem Eintreffen von `connection.blocked` schon gesendet waren, enden als
  `PublishIndeterminate`. Eine Verbindung, die bei Alarmbeginn publiziert, bekommt die Notifikation sofort
  (null bis wenige unbestimmte Publishes); eine ruhende erst mit ihrem ersten Publish (dieser unbestimmt).“
- **T-2 `broker_alarm`/E-041:** „Folgelauf“ präzisieren: Nach unbestimmter Retry-Kopie läuft das
  Original erneut; die spätere Kopie überspringt der Ausführungsschutz (`done`/`superseded`). Ein Duplikat
  im Ledger-Sinn (zweiter Erfolg) entsteht nur ohne Schutz oder nach Markerverlust.
- **T-3 `valkey_restart` mit Persistenz:** „Leader-Rücktritt und Rückkehr“ nur, wenn der Ausfall mindestens
  zwei aufeinanderfolgende Renews trifft (Heartbeat 10 s: je nach Lage ab gut 10 s, sicher ab 20 s) oder die Zeitgrenze erreicht (Renew-Start + 25 s); bei einigen Sekunden kein
  Rücktritt. Ohne Persistenz: Renew liefert `False` → sofort Standby (`scheduler_lock_lost`).
- **T-4 `valkey_restart` ohne Persistenz:** `valkey_data_loss_detected` je Scheduler-Loop einmal, 0 bis
  `scheduler_seen_interval_s` (60 s) nach Valkeys Rückkehr; das Recovery-Budget muss das abdecken.
- **T-5 beide `valkey_restart`:** „Duplikate nach gescheitertem Ergebnis-Speichern (je eins pro
  `backend_connection_error` mit task_id), siehe R4I-01“ als beobachtetes Verhalten aufnehmen; bis zur
  Entscheidung weder als „hingenommen“ noch als Fehler zählen, sondern getrennt ausweisen.
- **T-6 `queue_max_length`:** Wirkungsnachweis nur mit wartenden Nachrichten in der Queue (S-3).

## Doku-Hinweise (ohne Verdachtsnummer)

- **D-1** E-025 „Grenze“ und `website/reference/errors.md:573-575` präzisieren wie T-1 (die Aussage
  stimmt für eine ruhende Verbindung, nicht für eine publizierende). Nachtrag im Register nur nach Georgs Ok.
- **D-2** `delivery-semantics.md` „Redelivery scenarios“ um den Fall aus R4I-01 ergänzen (Teil von R4I-01).

## Abgleich mit der Chaos-Baseline (2a) und TS-01..03

- Keine Überschneidung mit A-1 (Teardown-Leck der TS-02-Klasse in `test_network_chaos.py`); in den
  Loadsuite-Läufen gibt es keine ResourceWarnings, die Suite misst Produktverhalten.
- Lücken der Baseline, die diese Läufe berühren: `test_disk_pressure.py::test_rabbitmq_disk_alarm_publisher_blocks`
  ist ein Stub (unbedingter Skip), einen Chaos-Test für Ressourcenalarme gibt es nicht; der einzige
  Alarm-Test ist `tests/integration/broker/test_befund_bz16_connection_blocked.py` (Ack im Alarm, ein
  Publish). R4I-02 und R4I-04 wären die ersten Tests für Worker-Kopien im Alarm.
  `tests/chaos/test_backend_restart.py` prüft Überleben von Ergebnis, Fortschritt, Pub/Sub und Cancel-Flag
  über einen Valkey-Neustart, nicht die Zahl der Ausführungen; R4I-01 ist dort nicht abgedeckt.
- PF-1 (Chaos-Stack auf 3.13.7): für R4I-01 ohne Belang (Valkey); R4I-02/-04 hängen am Blockierverhalten des
  Brokers, die Läufe liefen auf 4.3.6, die vorgeschlagenen Integrationstests ebenfalls (Teststack 5699).

## Abgleich KTD5 und docs/befunde/README.md

- Erlaubte Abweichungen nach KTD5 und Spalte „hingenommen“: E-025 (`PublishIndeterminate`,
  `BrokerPublishBlocked`), E-041 (Folgelauf), E-042, E-044 (`update_progress_hset_failed`), E-048 (Rücktritt
  bzw. Standby), `valkey_data_loss_detected` sind als wie erwartet eingestuft. Keine der vier Verdachte ändert
  oder widerspricht einer Entscheidung; R4I-01 und R4I-02 sind als Produktfrage formuliert, R4I-03 und R4I-04
  betreffen nur Logs.
- Keine Tests mit `entscheidung`-Marker berührt; keine Datei unter `src/swarmq/` geändert.
- Querverweise: Loadsuite-Backlog B26 (R4I-01), B27 (R4I-02), B28 (R4I-03), B29 (R4I-04), jeweils
  „F4-?? (nach Gegenprüfung)“; Abschnitt „Abgleich v0.12.3 Störungsläufe“ in
  `swarmq-loadsuite-stoerungen/docs/debug-feature-backlog.md`. Die F4-IDs vergibt U17/U18.

## Offene Punkte

- Gegenprüfung (U17, gekreuzt): R4I-01 bis R4I-04, dabei insbesondere, ob R4I-01 als Entscheidung
  („hingenommen“) oder als Änderung (begrenzte Wiederholung des Speicherns) weitergeht.
- Registerfragen für Georg: E-041 für Zurückstellung und Ablauf-DLQ; Ausführungsschutz nach Valkey-Datenverlust
  (dokumentiert, nicht im Register); Präzisierung E-025 (D-1).
- Suite vor dem nächsten Störungslauf: S-1 (Gate-Messung), S-3, S-4, S-6; danach 1-h-Lauf wiederholen, um
  die Erholungsbudgets mit Streuung zu setzen.
- Nicht gemessen: Folgefrage aus R4I-02 (Zustelllimit nach langem Alarm), Kanalnummer-Wiederverwendung aus
  R4I-04.
