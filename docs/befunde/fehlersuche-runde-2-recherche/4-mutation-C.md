# Fehlersuche 4C: Überlebende Mutanten Gruppe C (swarmq v0.12.0, Worktree `swarmq-fehlersuche`)

Grundlage: `mut-gruppe-C.txt` (633 überlebende Mutanten nach String-/Log-Filter) aus scheduler/*,
client/schedule.py, broker/rabbitmq/delivery.py + publish_phase.py, backend/valkey/schedules.py + results.py,
workflow_orchestration.py, backoff.py, keyspace.py. Kompaktansicht: `C-compact.txt`.
Nachweisskripte liegen in `mc/` (nur gelesen und ausgeführt, nichts im Repo geändert).

## Teil 1: Fehlerverdachte

### MC-1 (P2): `schedule(delay=/eta=, unique_for=None)` wird vom Leader trotzdem dedupliziert, und ein so verworfener Oneshot endet ohne Ergebnis

- **Fundstelle:** `src/swarmq/scheduler/fire.py:132-206` (else-Zweig von `_fire_one`), dazu `src/swarmq/client/schedule.py:188-205` und `:611-623` (`source="cli"`, wenn kein Hash) sowie `client/schedule_resolve.py:120-145` (Regel „None → dedup off“).
- **Mutanten:** `_fire_one__mutmut_53` (`and`→`or` in `source == "cli_unique" and dedup_hash`), `_fire_one__mutmut_144` (Unique-Header `None`), `schedule__mutmut_30/56` (`_resolve_unique(None, …)`). Zusammen zeigen sie: Wie die Unique-Entscheidung zwischen Client und Leader beim verzögerten Pfad aufgeteilt ist, prüft kein Test.
- **Szenario:** `TaskInfo(unique_for=3600)`. Der Aufrufer schaltet die Dedup für einen Aufruf ab, wie dokumentiert (`schedule("t", x, delay=60, unique_for=None)`). Der Client nimmt kein Flag und schreibt die Definition mit `source="cli"`, `dedup_hash=None` und `unique_for=None`. Die Definition unterscheidet nicht zwischen „bewusst aus“ und „nicht angegeben“. Beim Fire sieht der Leader `unique_for is None`, nimmt `TaskInfo.unique_for` aus seiner Registry und ruft `claim_unique` auf. Hält ein anderer Lauf mit denselben Argumenten das Flag (etwa ein zweiter `unique_for=None`-Aufruf oder ein normaler Aufruf), gilt das Fire als `dup_detected`. Der Leader publiziert nicht und entfernt den Oneshot per `commit_fire(-1)`. Unter der von `schedule()` gelieferten `task_id` wird **kein Ergebnis** gespeichert, anders als beim Unroutable-Drop (`_store_unroutable_result`). `get_result(task_id)` läuft in den Timeout, und der Task läuft nie.
  Dasselbe Verwerfen ohne Ergebnis trifft jeden `cli`-Oneshot, den der Leader dedupliziert, auch den eines registrylosen Producers (BZ-24-Umfeld).
- **Beleg:** `mc/mc_unique_none_oneshot.py`. Eine Definition wie aus `_schedule_deferred(unique_for=None)` und ein Flag, das ein fremder Lauf hält, ergeben `claim_unique` 1×, `publish` 0×, `commit_fire('oneshot:X', -1, None, …)` und `store_result` 0×.
- **Vorschlag Nachweistest (Unit, `tests/unit/scheduler/test_fire.py`):** Ping mit `unique_for=3600` registrieren. `backend.claim_unique` liefert `"other"`. Definition `source="cli"`, `cron=None`, `task_id="X"`. Erwartet: Publish findet statt, kein `claim_unique`. Zweiter Test: Verwirft der Leader einen Oneshot als Duplikat, steht ein Ergebnis unter `X` (oder ein dokumentiertes Verhalten).
- **Fix-Richtung:** Die Opt-out-Entscheidung in der Definition persistieren, z. B. `unique_for=0` oder ein eigenes Feld für „aus“, und bei `cli`-Oneshots nie aus der Registry nachziehen. Beim Duplikat-Verwerfen eines Oneshots ein Ergebnis schreiben oder die bestehende ID zurückmelden.
- **Konfidenz:** hoch für das Verhalten. Ob die Leader-Dedup bei `cli`-Oneshots ohne Opt-out gewollt ist (Testliste „Claim des Leaders aus der Registry“), ist eine Produktfrage. Das ignorierte explizite `None` widerspricht aber `_resolve_unique` und den Unique-Anforderungen (`docs/brainstorms/2026-04-17-phase3-5-unique-tasks-requirements.md:74`).

### MC-2 (P2): `get_result()` pollt Valkey nach dem ersten Wartesekunde alle 5 ms (rund 200 GET/s je Wartendem)

- **Fundstelle:** `src/swarmq/backend/valkey/results.py:30` (`_FALLBACK_POLL_S = 0.005`), `:33` (`_PUBSUB_WINDOW_S = 1.0`), `:192-196` (Polling-Schleife), `:127-147` (`_pubsub_window`). `client/results.py:69`: Der Parameter `polling` wird ignoriert („nur API-Kompatibilität“), der Aufrufer kann das Intervall also nicht steuern.
- **Mutanten:** `_pubsub_window__mutmut_3/10/12-19`, `wait_for_result__mutmut_6/7/17/18/29` überleben. Die Zeitlogik von Fenster und Polling prüft kein Unit-Test.
- **Szenario:** `await app.get_result(task_id, timeout=300)` auf einen Task, der 5 min läuft. Pub/Sub wird nur in der ersten Sekunde gelesen. Danach bleibt das Abonnement offen, ungelesen. Die Schleife macht 299 s lang alle 5 ms ein `GET`. Mit 100 parallel wartenden Clients sind das rund 20 000 GET/s Dauerlast auf Valkey, ohne dass ein Ergebnis kommt. Eine Benachrichtigung nach der ersten Sekunde wird nicht ausgewertet. Dazu kommt: `pubsub.subscribe()` (`:180`) läuft ohne Deadline. Bei hängendem Valkey kann `wait_for_result` den `timeout` um Socket-Timeout × Retries (10 s × 6) überschreiten, obwohl der Docstring „jeder GET an die Rest-Deadline gebunden“ verspricht.
- **Beleg:** `mc/mc_wait_poll.py` (Fake-PubSub ohne Nachricht, `timeout=3`): 392 `get_result`-Aufrufe in 3,0 s, also rund 196/s nach dem Fenster. `get_results` hat ebenfalls `poll_interval=0.005` als Default (`client/results.py:140`).
- **Vorschlag Nachweistest (Unit):** `ValkeyResults` mit Fake-Pubsub und gezähltem `get_result`, `timeout=2`. Erwartet höchstens etwa 2/(Pollintervall ≥ 0,1 s) + Fenster-GETs. Zweiter Test: Eine Benachrichtigung bei t = 1,5 s weckt sofort (Pub/Sub über die ganze Wartezeit lesen, Polling nur als langsamer Rückfall, z. B. 0,2–0,5 s mit Backoff).
- **Konfidenz:** hoch (Code und Messung). Die Last-Auswirkung ist abgeschätzt, nicht gegen echtes Valkey gemessen.

### MC-3 (P3): Ungültiger Namensraum `SQ` bekommt den Hinweis „write it in lowercase: 'sq'“, also genau den verbotenen Wert

- **Fundstelle:** `src/swarmq/keyspace.py:44-45`.
- **Mutanten:** `x_validate_namespace__mutmut_12` (`and`→`or`) und `__mutmut_14` (`lower`→`upper`) überleben. Die Hinweis-Bedingung ist ungetestet.
- **Szenario/Beleg:** `validate_namespace("SQ")` liefert `invalid namespace 'SQ': … not 'sq'; write it in lowercase: 'sq'`. Die Bedingung prüft nur das Regex der kleingeschriebenen Variante, nicht den Ausschluss `sq`.
- **Vorschlag:** Den Hinweis nur geben, wenn `validate_namespace(value.lower())` gelingt. Test: `SQ` ohne Lowercase-Hinweis, `App1` mit Hinweis `'app1'`.
- **Konfidenz:** hoch (direkt ausgeführt).

### MC-4 (P3): Rücktritt nach Renew-Fehlern greift erst lange nach Ablauf des Leader-Locks

- **Fundstelle:** `src/swarmq/scheduler/loop.py:196-199` (`_renew_failure_limit = max(1, int(ttl/heartbeat) - 1)`), `:388-418` (`_election_tick`, Renew ohne `op_timeout`), `backend/valkey/connection.py:216-227` (Retry mit 5 Wiederholungen, `socket_timeout=10 s`).
- **Mutanten:** `SchedulerLoop.__init__mutmut_49` (Untergrenze 1→2), `_election_tick__mutmut_32/33` (Reset auf 0 bzw. 1 nach Erfolg) überleben. Die Grenze und ihr Zeitbezug sind nicht geprüft.
- **Szenario:** Der Kommentar verspricht den Rücktritt „≈ one heartbeat before the lock TTL is provably expired“. Das stimmt nur, wenn ein fehlschlagender Renew sofort scheitert. Bei halb offener Verbindung oder hängendem Valkey dauert ein Renew bis zu etwa 6 × 10 s plus Backoff, bevor `BackendConnectionError` kommt. Mit den Defaults (TTL 30 s, Heartbeat 10 s, Limit 2) tritt der alte Leader nach rund 2 × (65 s + 10 s) ≈ 150 s zurück, das Lock ist aber nach 30 s frei. In dieser Zeit hält er sich für den Leader und pollt weiter. Der Schaden ist begrenzt: `commit_fire` ist owner-geprüft, und Worker deduplizieren per Schedfire-Marker. Das Split-Brain-Fenster ist also viel länger als dokumentiert, aber nicht unbegrenzt.
- **Vorschlag Nachweistest (Unit):** `lock_manager.renew` als `AsyncMock` mit `side_effect`, das per FakeClock 60 s „verbraucht“ und dann `BackendConnectionError` wirft. Erwartet: Rücktritt, bevor `scheduler_lock_ttl` seit dem letzten erfolgreichen Renew verstrichen ist. Fix-Richtung: Renew mit `op_timeout` ≤ Heartbeat aufrufen oder den Rücktritt an „Zeit seit letztem erfolgreichen Renew ≥ TTL − Heartbeat“ binden statt an eine Fehlerzahl.
- **Konfidenz:** mittel (Code-Analyse). Die Retry-Dauer von valkey-py bei Socket-Timeouts ist aus der Konfiguration abgeleitet, nicht gemessen. Verwandt mit K-3 (gleiche Retry-Konfiguration), aber eine andere Folge.

### Zur Timeout-Frage `keyspace.x_validate_namespace__mutmut_1/3/4/6`

Kein Regex-Backtracking. `[a-z0-9][a-z0-9_.-]{0,63}` hat keine verschachtelten Quantoren. Gemessen: 100 000 Zeichen (gültig, ungültig am Ende, nur `-`, Großbuchstaben) jeweils 0,1–0,2 ms, linear. Die Timeouts sind Artefakte der Mutanten. `validate_namespace` läuft über das modulweite `KeySpace()` schon beim Import (1514 Tests decken die Funktion ab). Mutant 1 (`if value: return None`) und 4 (`fullmatch(None)`) lassen `KeySpace(None)` mit `TypeError` scheitern. Mit `MUTANT_UNDER_TEST` direkt ausgeführt, bricht pytest bereits beim Sammeln ab (geprüft für 1 und 4). mutmut wertet den Abbruch des Laufs offenbar als Timeout. Mutant 3 und 6 lassen jeden gültigen Namensraum werfen. Kein Produktfehler.

### Bestätigungen bekannter Befunde (nicht neu)

- **R-4** (Backoff am Deckel nicht monoton): `backoff.x__jittered_cap__mutmut_1` (`<=`→`<`) überlebt. Der Deckelzweig wird nur stichprobenhaft geprüft. Kein weiterer Befund.
- **K-3** (Retry nicht idempotenter Befehle): Mit `take_handover_marker__mutmut_21` und `claim_fire__mutmut_2` überleben zwei Pfade derselben Store-Schicht ohne Unit-Test. Keine neue Erkenntnis.
- **BZ-28** (Poll-Limit 100): `_poll_tick__mutmut_9/12/14` (Limit `None`/101) überleben. Der Fix sitzt im Lua-Skript und ist dort per Integration abgedeckt.

### Geprüft, ohne Befund

- **cron_time / DST:** `_last_second_of_doubled_phase`-Mutanten (lo-Vorzeichen, Bisektionsende, `/`) sind für ganzstündige Rückstellungen äquivalent. Das gilt, weil der erste Termin im zweiten Durchlauf immer die kleinste Minute des Ausdrucks trägt. Nachgerechnet für Berlin, New York und Lord Howe (30-min-Rückstellung), `30 2`, `0,30 2,3`, `*/20 2`, `0 */2`: alle Termine korrekt, kein doppelter Fire.
- **fire_plan:** Die Grenze `fire_ms > now_ms` in `_count_skipped_fires` ist richtig, denn ein Termin genau bei `now` wird übersprungen, weil `next_fire_ms` strikt nach `now` liefert. Die Kappung 1000/1001 ist belanglos.
- **sched_commit:** Ergebnis 2 (superseded) wird zu `True`. `_fire_one` setzt den Leader also nicht fälschlich auf Standby.
- **Workflow-Weiterschaltung:** Viele `_mark_terminal`-Mutanten überleben, weil die `else`-Zweige (Elternknoten kein Container) toter Code sind. Werte nachgeprüft mit `mc/mc_workflow_values.py` über den Test-Driver: `chain(group, x)`, `chain(s, chord(...), x)`, `chain(chain(a,b), c)`, `group(chain, c)`, `chord(group(chain, c), x)`, `chain(a, group)` und eine geschachtelte Kombination liefern alle das erwartete Endergebnis.
- **schedule_many:** Teilfehler, Rollback, `PublishIndeterminate`-Vorrang korrekt. `_batch_rollback_claims__mutmut_6` (`and`→`or`) ist durch das owner-geprüfte Release faktisch äquivalent.
- **Delivery:** Die Retry-Schleife (`_publish_attempts`), `_wait_while_blocked` (`blocked.append(False)` ist äquivalent, weil eine nicht leere Liste truthy ist) und `retry_later`-Stufenwahl sind korrekt. Bei `dead_letter` muss `at_ms` gleich `x-swarmq-dead-lettered-at` sein (`admin._fresh_generation`). Das Original reicht es korrekt durch. Der Mutant überlebt, weil die Testuhr konstant ist (siehe Teil 2).
- **stepdown / unowned / prune / orphaned_queues / leader_version:** Logik korrekt. Die Reihenfolge Prune vor Report-Prune in `adopt_reported_crons` stimmt. `_release("0.12.0rc1")` = (0, 12) gilt nicht als veraltet, ist aber unkritisch.

## Teil 2: Wichtige Testlücken ohne Fehler (höchstens 10)

1. **`ClientScheduling._batch_claim_flags` mit gemischtem Batch** (`__mutmut_6` `continue`→`break`): Ein Batch, in dem ein Eintrag ohne Dedup vor einem mit Dedup steht, ist ungetestet. Mit `break` gingen spätere Einträge ohne Claim, aber mit Unique-Header raus, und die Dedup wäre still aus. Auch der Unique-Header im Batch (`schedule_many__mutmut_54`) und `post_enqueue` mit dem richtigen Kontext (`__mutmut_46`) werden nicht geprüft.
2. **Unique-Header beim Cron-Fire** (`_fire_one__mutmut_144`): Dass ein Fire mit eigenem Claim `x-swarmq-unique-hash` trägt, prüft kein Test. Ohne den Header gibt der Worker das Flag nie frei, und der Cron blockiert sich bis `unique_for`.
3. **`dead_letter`: `x-swarmq-dlq-gen == x-swarmq-dead-lettered-at`** (`dead_letter__mutmut_22/26`, `_publish_to_dead_letter_queue__mutmut_14`): Die Gleichheit trägt die BZ-08-Replay-Erkennung. Ein Test mit fortlaufender Uhr fehlt.
4. **`_publish_with_retry` mit Deadline vor dem ersten Kanal** (`__mutmut_27`): Läuft die Frist ab, während `_ensure_publish_channel` hängt (`in_flight` leer), ist das ungetestet. Ein Umbau würde zu `IndexError` statt `PublishIndeterminate` führen.
5. **Wahl-Takt-Zähler** (`_election_tick__mutmut_32/33/97/98`, `__init__mutmut_49`): Reset nach Erfolg und Untergrenze des Rücktrittslimits sind ungetestet (siehe MC-4).
6. **Amtszeit-Zähler BZ-11** (`_begin_term__mutmut_1/3`, `_hold_unowned__mutmut_2`): Mehrere Amtszeiten nacheinander fehlen im Test. Mit `_term = 1` würden die Meldungen der alten Amtszeit in der neuen weitergelten. `_held_crons` wird bei mehreren gleichzeitig gehaltenen Crons nicht geprüft.
7. **`update_static_schedule(keep_fire_index=True)`** (`definition…__mutmut_9`, `sync…__mutmut_122-131`): Ein geänderter statischer Cron mit fälligem Score wird nur über Mock-Stores geprüft. Ungetestet ist, dass der Score stehen bleibt und genau ein Fire mit neuem Inhalt folgt (KTD12).
8. **Chord-/Workflow-TTL** (`_on_group_member__mutmut_7` `ttl_ms=None`, `WorkflowAdvancer.__init__mutmut_7`): Ob der Chord-Zähler und das Synthese-Ergebnis eine TTL bekommen, prüft kein Test. Ohne TTL blieben Keys ewig liegen. Dazu prüft `test_chain_with_embedded_chord` nur den Status, nicht den Wert.
9. **`wait_for_result` / `watch_progress` Zeitlogik** (rund 25 Mutanten): Pub/Sub-Fenster, Poll-Rückfall und Resubscribe haben keine Unit-Tests (siehe MC-2).
10. **`RabbitMQDelivery.consume`** (15 Mutanten, z. B. `Delivery(queue=None)`, `handle=None`, `headers=None`): Die Abbildung der aio_pika-Nachricht auf `Delivery` wird nur per Integration geprüft. Ein falsches `queue` würde Retry, DLQ und Delay-Stufe an die falsche Queue lenken.

### Grobe Statistik

- 633 überlebende Mutanten. Nach Modul: workflow_orchestration 96, delivery 81, valkey.results 81, valkey.schedules 69, scheduler.loop 46, client.schedule 44, scheduler.unroutable 42, sync 31, fire 23, stepdown 21, übrige Scheduler-Module 92, publish_phase 3, keyspace 2, backoff 2.
- Grob eingeteilt: etwa 150 Log-, Hint- und Event-Feld-Argumente (`schedule_name=None`, `hint=None`, `_error_fields(None)` usw.). Etwa 30 Operationsnamen in `_run(...)` und Skriptregistrierungen bzw. Konstruktor-Keys des Valkey-Stores (nur per Integration getestet, bekannt). Etwa 40 Mutanten in totem Code oder äquivalent (`else`-Zweige der Workflow-Rekursion, `blocked.append(False)`, Grenzen der DST-Bisektion, Kappung 1000/1001). Etwa 60 Zeit- und Intervallkonstanten (Sleep-Dauern, TTL-Faktoren `*1001`). Der Rest von rund 350 sind echte Verhaltenslücken. Die riskanten sind in Teil 2 gelistet.
- Ergebnis: 2 neue Befunde P2 (MC-1, MC-2) und 2 P3 (MC-3, MC-4). Die Timeout-Mutanten von `validate_namespace` sind Artefakte, keine Regex-Langsamkeit.
