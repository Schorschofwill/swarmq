# Fix-Recherche Scheduler-Store und Client (F3-01, F3-02, F3-07, F3-09, F3-29)

Stand: Worktree `swarmq-fehlersuche3`, Branch `test/fehlersuche-runde-3` (968ff7f). Register `compound-packs/swarmq-entscheidungen/` mit E-055 bis E-071 und den Nachträgen vom 2026-10-15. Ich habe nur gelesen. Bibliotheken im `.venv`: valkey 6.1.1, msgspec 0.21.1.

Ausgeführt: die Unit-Nachweise F3-02 (5), F3-07 (6) und F3-09 (2) mit `--runxfail -p no:randomly`. Alle 13 scheitern aus dem genannten Grund. Den Integrationsnachweis F3-01 habe ich nicht ausgeführt. Laut `scratchpad/fs3/f301-run.txt` scheitert er unter `--runxfail` mit „Oneshot ohne Eintrag im Fire-Index“. Den Mechanismus habe ich im Quelltext von valkey-py geprüft (siehe F3-01).

Eine Probe bestätigt: msgspec 0.21.1 ignoriert beim Dekodieren eines `msgspec.Struct` ohne `forbid_unknown_fields` unbekannte Felder (`{"x":1,"headers":{…}}` → `A(x=1)`).

Größengrenze: `tests/unit/test_architecture_guards.py:37` (`_MAX_MODULE_LINES = 800`, `_LOC_EXCEPTIONS` leer). Die Dateien dieses Pakets liegen weit darunter: `client/schedule.py` 668, `client/schedule_resolve.py` 193, `client/results.py` 248, `scheduler/definition.py` 299 und `backend/valkey/schedules.py` 528. Knapp an der Grenze liegen außerhalb des Pakets `worker/pipeline.py` (797), `logging_file.py` (771), `logging_queue.py` (760) und `agent_spec.py` (744). Kein Fix dieses Pakets berührt sie. Nach F3-02 wächst `client/schedule.py` auf geschätzt 700 bis 715 Zeilen.

---

## F3-01 (P2): HSETNX-Wiederholung in `ScheduleStore.add` – Oneshot fehlt im Fire-Index (E-058)

### Ursache (verifiziert)

- `backend/valkey/connection.py:245-253` baut den Client mit `retry=Retry(ExponentialBackoff(cap=2.0, base=0.05), retries=5)` und `retry_on_error=[ConnectionError, TimeoutError, OSError]` (`:36-40`). Dazu kommen `socket_timeout=10 s` und `socket_connect_timeout=5 s`.
- valkey 6.1.1, `asyncio/client.py:637-642`: `execute_command` wickelt **Senden und Parsen** in `conn.retry.call_with_retry(...)`. `asyncio/retry.py:46-67` wiederholt bei jedem unterstützten Fehler. Geht die Antwort verloren, nachdem der Server den Befehl ausgeführt hat, wird er also erneut gesendet. Das gilt ebenso für EVALSHA (`commands/core.py:5196-5203`; ein `NoScriptError` lädt nach).
- `backend/valkey/schedules.py:120-127`: `add` führt `hsetnx_schedule_def` aus und kehrt bei `False` sofort zurück, **ohne ZADD**. HSETNX und ZADD sind zwei getrennte Befehle, also nicht atomar.
- `scheduler/definition.py:156-161`: `add_schedule` reicht den Wert durch.
- `client/schedule.py:661-668`: `_schedule_deferred` ignoriert den Rückgabewert von `add_schedule` und gibt `task_id` zurück.
- Folge: Die Definition steht in `sq:{sched}:defs`, hat aber keinen Score und wird nie gefeuert. `sched_poll.lua` räumt nur Index-Einträge ohne Definition ab, nicht umgekehrt. Mit `unique_for` hält der Claim bis `unique_for + Verzögerung` und blockiert gleiche Aufrufe.

**Zusatzbefund im selben Pfad: `swarmq schedule add`.** Die CLI läuft über `cli/schedule_cmd.py:254` → `add_schedule` → dasselbe `add`. Geht die Antwort verloren, meldet sie nach E-058 bewusst „already exists“. Die Begründung von E-058 („obwohl der eigene Insert durchging“) setzt aber voraus, dass der Insert vollständig ist. Heute fehlt nach der Wiederholung auch hier der ZADD. Ein CLI-Cron oder `--at`-Termin wird dann nie gefeuert, und es gibt keine Heilung. Die Sync-Heilung `_replan_if_unscored` (`scheduler/sync.py:238-243`) gilt nur für `source="static"`. Der von E-058 genannte Prüfweg `swarmq schedule list` zeigt den Fehler nicht: `NEXT_FIRE` kommt aus `definition.fire_ts_ms`, nicht aus dem Fire-Index (`cli/schedule_cmd.py:85`). Statische Crons aus dem Leader-Sync (`sync.py:206`) heilen beim nächsten Sync über `_replan_if_unscored`. Dasselbe Fenster öffnet sich auch ohne Wiederholung, wenn der Prozess zwischen HSETNX und ZADD stirbt.

### Nachweis-Test

`tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py` verwirft genau eine HSETNX-Antwort nach vollständigem Lesen. Er prüft `fire_ts(name) is not None` für einen Oneshot aus `schedule(delay=60)`. Damit trifft er E-058. Abweichungen gibt es keine. Der Docstring nennt noch „Entscheidung 4“ statt E-058; das ist beim Umstellen auf den Marker anzupassen. Nicht abgedeckt: der Konfliktfall (abweichender Eintrag) und die CLI. Beides sollen Zusatztests abdecken (siehe unten).

### Optionen

**A – Atomares `sched_add.lua` plus Oneshot-Idempotenz in der Definitionsschicht (empfohlen).**

1. Neues Skript `src/swarmq/lua/sched_add.lua` mit `KEYS[1]=defs`, `KEYS[2]=zset`, `ARGV[1]=name`, `ARGV[2]=payload` und `ARGV[3]=fire_ts_ms` (leer = kein Index). Es führt `HSETNX` aus. Bei 1 und gesetztem Fire-Zeitpunkt folgt `ZADD`, Rückgabe 1. Sonst ist die Rückgabe 0.
2. `ValkeyScheduleStore.add` (`schedules.py:120-127`) ruft das Skript. Signatur und Vertrag (`contracts/coordination.py:136`, „Insert-if-absent; False, wenn name existiert“) bleiben gleich. `hsetnx_schedule_def` bleibt als Methode erhalten, weil `tests/unit/test_api_breaks_0_2_0.py:25` den Namen führt.
3. `ScheduleStore.add_schedule` (`scheduler/definition.py:156-161`) bekommt die E-058-Logik. Ein Client-Oneshot ist eindeutig erkennbar: `definition.task_id is not None and definition.cron is None and definition.schedule_name == f"oneshot:{definition.task_id}"`. Die CLI setzt nie `task_id` (`schedule_cmd.py:236-243`), statische Crons haben `cron`. Meldet `add` `False` für einen solchen Oneshot, folgen diese Schritte:
   - `existing = await self._store.get(name)`; aus `str` wird vorher `bytes` (`existing.encode()`), weil ein injizierter Client mit `decode_responses=True` `str` liefert.
   - Ist `existing == payload` (byte-gleich), zählt das als eigener Insert. Fehlt der Index (`await self._store.fire_ts(name) is None` und nicht `disabled`), wird er per `put(name, payload, fire_ts_ms)` ergänzt; das entspricht wörtlich E-058 („Fire-Index wird ergänzt“) und wirkt mit jedem Vertrags-Store. Rückgabe `True`.
   - Sonst bleibt es bei `False`: ein Konflikt.
4. Client (`client/schedule.py:663`): `if not await store.add_schedule(definition): raise BackendResponseError(...)` („one-shot … exists with different content“). Der bestehende `except BaseException` gibt dann den Unique-Claim frei. Bisher wird `False` still verschluckt; das ist der Rest von F3-01. Diese 2 bis 4 Zeilen sollte die Unit mitnehmen, die `client/schedule.py` besitzt (siehe „Schnitt“).

Warum der Byte-Vergleich in Python und nicht in Lua? Ein Lua-Vergleich (`HGET == ARGV[2]`) wäre ebenso byte-genau, bräuchte aber ein Flag „idempotent“ im Aufruf. Damit änderte sich die Signatur des öffentlichen Vertrags `ScheduleStore.add` (dokumentiert in `website/reference/api.md:840ff.`), und ein Fremd-Store bekäme einen `TypeError`. Für einen Patch ist das unpassend. In Python ist der Vergleich genauso exakt: msgspec kodiert deterministisch, und die Wiederholung sendet dieselben Bytes. Nur `str`/`bytes` muss normalisiert werden (siehe oben).

Was das Lua-Skript zusätzlich bringt: Nach einer verlorenen Antwort ist der Eintrag immer vollständig, mit Definition **und** Index. Dadurch wird die bewusste Folge von E-058 für `swarmq schedule add` erst harmlos: „already exists“ wird gemeldet, und der Cron feuert trotzdem. Auch das Absturzfenster zwischen HSETNX und ZADD ist geschlossen.

**B – nur Python (ohne Lua).** Die Schritte 3 und 4 aus A, `add` bleibt nicht atomar. Für Oneshots reicht das; der F3-01-Test wird grün. Der Zusatzbefund bleibt dann offen: Ein CLI-Cron oder `--at` hat nach verlorener Antwort keinen Index und feuert nie. In dem Fall müsste E-058 einen Nachtrag bekommen, oder der Zusatzbefund müsste als eigener Befund ins Register.

**Empfehlung: A.** Die Ursache ist dieselbe K-3-Invariante wie bei FS-06 („alle Lua-Skripte und NX-Befehle gegen Wiederholung prüfen“). Der Aufwand ist klein: ein Skript mit etwa 10 Zeilen, ein Zeilenwechsel in `add` und Testanpassungen.

### Folgefehler-Analyse (Option A)

- **Aufrufer von `add`/`add_schedule`:**
  - Client `_schedule_deferred` (`client/schedule.py:663`): idempotent, Konflikt wirft.
  - CLI `schedule_cmd.py:254`: unverändert „already exists“ bei `False` (E-058); neu ist nur, dass der Eintrag vollständig ist.
  - Leader-Sync `scheduler/sync.py:206`: Statische Crons sind keine Oneshots, also bleibt das Verhalten gleich. Bei verlorener Antwort ist der Eintrag jetzt vollständig.
  - `tests/support/inmemory/backend.py:464`: Der In-Memory-Store ist schon atomar und braucht keine Änderung.
- **Wettlauf mit dem Leader:** Der Leader feuert erst nach dem ZADD. Mit Lua sind Definition und Index zugleich da. Kommt die Wiederholung erst nach dem Fire-Zeitpunkt (kurzes `delay`, Antwort erst nach `socket_timeout` = 10 s verloren, dann Backoff), sind drei Fälle möglich:
  - Der Leader hat gefeuert und den Oneshot gelöscht (HDEL + ZREM): Das zweite HSETNX schreibt neu, und der Oneshot feuert ein zweites Mal mit demselben Schedfire-Key `oneshot:<id>:<fire_ts>`. Der Completed- oder Inflight-Marker des Workers fängt das ab (E-046, at-least-once). Das ist heute schon so und kein neues Risiko.
  - Der Leader hat die Definition umgeschrieben (`unroutable_since` bei unroutbarer Queue, `_defer_unroutable_oneshot`) oder der Bediener hat ihn deaktiviert: Die Bytes weichen ab, also Konflikt, `schedule()` wirft, und der Claim wird freigegeben, obwohl der Oneshot lebt. Das ist extrem selten, weil verlorene Antwort, späte Wiederholung und unroutbare Queue zusammenkommen müssen. Es folgt aber wörtlich aus E-058 („abweichender Eintrag bleibt ein Konflikt“). Dazu siehe die Frage unten „Entscheidung überdenken?“.
  - Eintrag zwischen HSETNX(0) und HGET verschwunden (`existing is None`): Das regelt E-058 nicht. Vorschlag: Konflikt melden, also `False`, konservativ und regelnah.
- **Entscheidungen und ihre Tests:**
  - E-058: bisher `tests: []`. Der Nachweis bekommt den Marker.
  - E-011 und E-022 (verwandte Idempotenz): Sie betreffen den Leader-Claim und Locks; dieser Code wird nicht berührt.
  - E-066 (`TestClientUniqueDeferred::*`): Die Mocks liefern `AsyncMock()` mit einem wahren `MagicMock` als Ergebnis, die Tests bleiben grün. Keiner der Client-Tests setzt `add_schedule` auf `False`. Grep: nur `tests/unit/scheduler/conftest.py:81` mit `True`.
  - E-033 und E-032 bleiben unberührt.
- **Bestehende Unit-Tests, die angepasst werden müssen** (kein `entscheidung`-Marker):
  - `tests/unit/backend/test_valkey_schedules.py:80-95`: `test_add_writes_hsetnx_then_zadd`, `test_add_existing_is_noop_without_zadd` und `test_add_without_fire_time_skips_index` prüfen HSETNX und ZADD einzeln.
  - Die Fixture `_store` (`:50-76`) liefert die Skripte positionsgebunden per `register_script(side_effect=list(scripts.values()))`. Ein neues Skript verschiebt die Reihenfolge, also braucht `scripts` einen Eintrag `"add"` an der richtigen Stelle.
  - `TestErrorWrapping.test_driver_errors_become_backend_connection_error` (`:191-195`) setzt `client.hsetnx` auf einen Fehler. Künftig muss der Skript-Mock den Fehler werfen.
- **Bibliothek:** valkey 6.1.1 `AsyncScript.__call__` nutzt EVALSHA mit NOSCRIPT-Fallback (`commands/core.py:5196-5203`). Die Wiederholung umfasst EVALSHA. Das Skript ist wiederholungssicher: Lauf 1 schreibt beides, Lauf 2 liefert 0, und die Definitionsschicht erkennt den eigenen Insert.
- **Cluster und Namensraum:** Beide Keys tragen den Hash-Tag `{sched}` und liegen im selben Slot. `KeySpace` liefert die Präfixe.
- **Golden:** In `tests/integration/backend/golden/lua_contract.txt` kommen zwei Zeilen hinzu: `sched_add\tKEYS=2\tARGV=3` und `sched_add\tslots=equal\tsq:{sched}:defs sq:{sched}:zset`. Dafür braucht die `multi`-Tabelle in `tests/integration/backend/_golden.py:309-320` den Eintrag `"sched_add"`. Die Änderung ist rein additiv; kein bestehendes Skript ändert Arität oder Bedeutung. `golden/values/schedule_definition.json` bleibt unverändert.
- **Mischbetrieb 0.12.0–0.12.2:** Ältere Producer und CLIs schreiben weiter HSETNX + ZADD in dasselbe Format. Der Leader merkt keinen Unterschied, denn das Format der Definition bleibt gleich. Der Fix wirkt je Producer, sobald dieser aktualisiert ist.
- **Rollback:** Das Skript wird nicht mehr geladen; es bleibt nur im Skript-Cache liegen, was folgenlos ist. Es gibt keine Daten- oder Formatänderung.

### Testszenarien

1. Nachweis F3-01 (Integration): Die HSETNX-Antwort geht verloren. `schedule(delay=60)` liefert eine ID, die Definition existiert, und `fire_ts` ist gesetzt. Bei Option A geht die Antwort von `EVALSHA` verloren, nicht die von `HSETNX`. Der Test injiziert `command == "HSETNX"` und **trifft mit Lua nicht mehr**: `lost.injected` bleibt `False`, die Voraussetzung scheitert. Er muss also auf `EVALSHA` umgestellt werden oder auf beide Befehle prüfen. Das ist die einzige nötige Änderung am Nachweis. Er trägt keinen `entscheidung`-Marker und darf darum angepasst werden. Empfehlung: Den Befehl parametrisieren (`"EVALSHA"`). Zusätzlich sollte ein Unit-Test in der Definitionsschicht E-058 ohne Lua festschreiben.
2. Unit `tests/unit/scheduler/test_definition.py`, Definitionsschicht:
   - Oneshot (`task_id="t1"`, Name `oneshot:t1`, `cron=None`), `add`→`False`, `get`→dieselben Bytes, `fire_ts`→Score: Rückgabe `True`, kein `put`.
   - Wie oben, aber `fire_ts`→`None`: `put(name, payload, fire_ts)` wird einmal aufgerufen, Rückgabe `True`.
   - Wie oben, aber `get` liefert `str` (gleicher Inhalt): `True`.
   - Abweichende Bytes (etwa `unroutable_since` gesetzt): `False`, kein `put`.
   - `get`→`None`: `False`.
   - CLI-artige Definition (`task_id=None`, `--at`) mit `add`→`False`: `False`, kein `get` (E-058: kein Idempotenzfall).
   - Cron-Definition mit `add`→`False`: `False` (der bestehende Test `test_add_schedule_existing_entry_is_noop` bleibt grün).
3. Unit `tests/unit/backend/test_valkey_schedules.py`: `add` ruft das Skript mit `keys=[defs, zset]` und `args=[name, payload, fire_ts]` auf. Ohne Fire-Zeitpunkt wird `""` übergeben. Ergebnis 1 → `True`, 0 → `False`. Ein Skriptfehler wird zu `BackendConnectionError`.
4. Integration (Testport, Valkey-Lua): `sched_add` mit bestehendem Eintrag liefert 0 und lässt Definition und Score unverändert. Ohne `fire_ts` entsteht kein Index-Eintrag. Nach verlorener EVALSHA-Antwort hat eine CLI-Definition (Cron) einen Score, und `add_schedule` meldet `False` („already exists“, E-058).
5. Client-Unit: Liefert `add_schedule` `False`, wirft `schedule(delay=30, unique_for=60)` einen `BackendResponseError`, und `release_unique` wird mit (Hash, eigener `task_id`) aufgerufen.

### Doku, Changelog, Register

- **Doku:** `website/reference/api.md` und `api.de.md`, Abschnitt Contracts. Dort genügt ein Satz: „`ScheduleStore.add` should write the definition and the fire index atomically; swarmq's own `schedule(delay=/eta=)` treats a byte-identical existing one-shot as its own insert (lost reply)“. Ein Vertragsbruch ist das nicht. Optional in `website/guides/scheduling-and-retries.md` und `.de.md`: Nach verlorener Antwort meldet `swarmq schedule add` „already exists“, der Eintrag ist aber vollständig (E-058).
- **Changelog EN:** „`schedule(delay=/eta=)` no longer returns an ID that never fires when Valkey's reply to the store write is lost and the client retries; the one-shot entry and its fire time are written in one step (also for `swarmq schedule add`).“ DE: „`schedule(delay=/eta=)` liefert nach einer verlorenen Valkey-Antwort keine ID mehr, die nie feuert; Eintrag und Fire-Zeitpunkt werden in einem Schritt geschrieben (auch bei `swarmq schedule add`).“
- **Register:**
  - Der Nachweis bekommt `@pytest.mark.entscheidung("E-058")` statt xfail.
  - E-058: `tests:` nennt den Nachweis und die neuen Unit-Tests der Definitionsschicht; `ohne_test` entfällt.
  - Vorschlag für einen Nachtrag zu E-058: „Ein zwischen HSETNX und HGET verschwundener Eintrag gilt als Konflikt“ sowie der Hinweis auf die Atomarität (siehe die Frage unten).

### Entscheidung überdenken? (nur Hinweis)

E-058 bindet die Idempotenz an Byte-Gleichheit. Bei einem Oneshot beweist aber schon der Name `oneshot:<frische uuid7>` die Herkunft. Der Byte-Vergleich ergibt einen falschen Konflikt, wenn der Leader die Definition zwischen verlorener Antwort und Wiederholung umgeschrieben hat (`unroutable_since`). `schedule()` wirft dann, obwohl der Oneshot lebt, und ein Wiederholversuch des Aufrufers erzeugt ein Duplikat. Das neue Argument wäre also: Byte-Gleichheit ist für Oneshots strenger als nötig. Ein Vergleich der dekodierten `task_id` wäre robuster. Das Risiko ist sehr klein; die Empfehlung folgt trotzdem der Regel.

---

## F3-02 (P2): `schedule(delay=/eta=)` umgeht `pre_enqueue` und `TASK_ENQUEUED` (E-059)

### Ursache (verifiziert)

- `client/schedule.py:221-236`: Bei `delay > 0` oder `eta` kehrt `schedule()` über `_schedule_deferred` zurück. `chain.run_pre_enqueue` (`:303`), `run_post_enqueue` (`:328`) und `TASK_ENQUEUED` (`:329-334`) werden nie erreicht.
- `_schedule_deferred` (`:575-668`) erzeugt `task_id` erst in `:618`, setzt den Claim in `:622-640` und speichert in `:663`. Es gibt keinen `MessageContext` und keinen `delivery_mode`.

### Was die Middleware heute ändern kann (Grundlage für „Header-/Message-Änderungen speichern“)

- `MessageContext` (`middleware.py:14-35`) hat **kein Header-Feld**. Er enthält nur `task_id`, `task_name`, `queue`, `args`, `kwargs`, `delivery_mode` und optional `retry_count`, `workflow_id` und `step_path`. Eine Middleware kann heute keine Header setzen, weder im sofortigen noch im verzögerten Pfad.
- Im sofortigen Pfad wirkt nur eine **In-place-Änderung an `kwargs`** auf die Nachricht. `message["kwargs"]` und `message_dict["kwargs"]` sind dasselbe Objekt (`:275, :284`). `args` sind zwei getrennte Kopien (`list(args)` in `:274` und `:283`), deshalb wirken Änderungen an `args` nicht. Neu zugewiesene Schlüssel wie `message["kwargs"] = {...}`, `queue` oder `task_id` wirken ebenfalls nicht.
- `ScheduleDefinition` (`scheduler/definition.py:51-92`) speichert `args` und `kwargs` schon. Es gibt kein Feld `headers`.

**Konsequenz für E-059:** Die Regel ist umsetzbar, ohne das Format zu ändern. „Message-Änderungen“ heißt praktisch dasselbe wie im sofortigen Pfad: In-place-Änderungen an `kwargs` landen in der gespeicherten Definition, und der Leader publiziert sie (`scheduler/fire.py:106-116` kodiert `definition.kwargs`). „Header-Änderungen“ haben **heute keine API**. Wollte man sie speichern, bräuchte man einen neuen `MessageContext`-Schlüssel und ein neues Feld `ScheduleDefinition.headers`. Das wäre ein Formatwechsel:
- Leader 0.12.0–0.12.2 dekodieren ein unbekanntes Feld ohne Fehler (msgspec ignoriert es, Probe oben) und **verwerfen die Header still**.
- Schreibt ein alter Leader die Definition neu (`_defer_unroutable_oneshot`, Commit-Payload), geht das Feld aus dem gespeicherten Eintrag verloren.
- `golden/values/schedule_definition.json` und `test_schedule_definition_is_field_equal` (Feldliste `test_golden_fixtures.py:72-89`) ändern sich.

Für einen rolling-deploy-sicheren Patch ist das ungeeignet. **Empfehlung:** Für 0.12.3 kein Header-Feld. E-059 bekommt einen Nachtrag: Die Middleware kann heute keine Header setzen. Gespeichert werden die In-place-Änderungen an `kwargs` wie im sofortigen Pfad. Header kämen erst mit einer neuen Minor-Version und einer `MessageContext`-Erweiterung.

### Nachweis-Test

`tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py` prüft vier Dinge:
- Veto bei `delay` und `eta`: Die Ausnahme propagiert, kein `add_schedule`, kein Dispatch.
- Veto vor dem Claim: kein `claim_unique`.
- Reihenfolge `["store", "TASK_ENQUEUED"]` mit (id, name, queue).
- Eine `kwargs`-Ergänzung steht in der Definition.

Das entspricht E-059. Nicht geprüft werden `post_enqueue`, der Dedup-Hash (siehe unten) und der Duplikatpfad. Der Test greift auf `add_schedule.await_args.args[0]` zu, und die `side_effect`-Funktion `_stored(_definition)` nimmt genau ein Positionsargument. Der Fix muss darum **`store.add_schedule(definition)` unverändert aufrufen**: keine neuen Keyword-Argumente, keine andere Methode. Das ist eine harte Randbedingung, auch für F3-01; der Nachweis F3-07 (`schedule_store.add_schedule.assert_not_awaited()`) und `TestClientScheduleDelayEta` hängen ebenfalls daran.

### Fix (empfohlen, Option A)

Reihenfolge in `schedule()` und `_schedule_deferred` (`client/schedule.py`):
1. Validierung wie heute: `delay` und `eta` schließen sich aus, `delay >= 0`; dazu `_resolve_unique` (Validierung nach F3-07), dann `eta` in der Vergangenheit → `ValueError`. Alles **vor** der Middleware.
2. `task_id = uuid7()` früher erzeugen, damit `pre_enqueue` und Rückgabe dieselbe ID sehen.
3. Bei `unique_for is not None` den `dedup_hash` **vor** `pre_enqueue` aus den Original-`args`/`kwargs` berechnen (`compute_dedup_hash`, eine reine Funktion). Grund: Im sofortigen Pfad stammt der Hash ebenfalls aus den Werten vor der Middleware (Claim in `:247` vor `pre_enqueue` in `:303`). Ergänzt eine Middleware je Aufruf etwa eine `correlation_id` in `kwargs`, wäre ein Hash danach je Aufruf verschieden. Die Dedup verzögerter Aufrufe wäre dann wirkungslos, und gleiche sofortige und verzögerte Aufrufe kollidierten nicht mehr. Umsetzung: Parameter `dedup_hash: str | None = None` an `_claim_unique_flag` (`:75-139`). Ist er gesetzt, ersetzt er die Berechnung in `:105`.
4. `message_dict` wie im sofortigen Pfad bauen: `args=list(args)` als Kopie, `kwargs=kwargs` als dasselbe Objekt und `delivery_mode` aus `schedule()`. `_schedule_deferred` braucht dafür den neuen Parameter `delivery_mode`.
5. `await chain.run_pre_enqueue(message_dict)`. Ein Veto propagiert. Es gibt noch nichts zurückzurollen: weder Claim noch Store.
6. Claim mit dem vorab berechneten Hash. Bei einem Duplikat wird die bestehende ID zurückgegeben, ohne `post_enqueue` und ohne `TASK_ENQUEUED`, wie im sofortigen Pfad.
7. Die Definition wird aus `args` (Original, wie die publizierte Nachricht im sofortigen Pfad) und `kwargs` (dasselbe Objekt, also mit den In-place-Änderungen) gebaut. Dann `add_schedule` und bei `False` ein Fehler (F3-01, Schritt 4). Der Rollback des Claims bleibt wie heute (`:664-667`).
8. Danach `await chain.run_post_enqueue(message_dict)` und `TASK_ENQUEUED(task_id, task_name, queue=resolved_queue)`.

`post_enqueue`: E-059 nennt nur `TASK_ENQUEUED`. Die Doku verbindet `post_enqueue` aber mit dem erfolgreichen Enqueue (`middleware.md:28`). Eine Middleware mit gemeinsamem Zustand vor und nach dem Enqueue (Startzeit → Dauer) bliebe sonst offen. Empfehlung: `post_enqueue` nach dem Speichern aufrufen und das in einem Nachtrag zu E-059 festhalten.

Option B wäre, `pre_enqueue` nach dem Claim aufzurufen, mit Rollback beim Veto wie im sofortigen Pfad. Das widerspricht E-059 („vor dem Unique-Claim“) und scheidet aus.

### Folgefehler-Analyse

- **Nachbarpfade:**
  - `schedule_many` hat keinen `delay`/`eta`-Pfad (Signatur `:466-473`) und ruft `pre_enqueue` schon heute auf. Keine Änderung.
  - Workflow-Schritte und `app.apply()` laufen nicht über `_schedule_deferred` (Grep: keine Aufrufer außerhalb `client/schedule.py`). Sie sind durch E-059 und `test_workflow_step_publisher_runs_middleware_then_signals` bereits abgedeckt und bleiben unberührt.
  - `app.schedule` (`app.py:192`) und `Client.run` (`client/results.py:179`) reichen nur durch.
  - Cron-Fires des Leaders (F3-36) bleiben bewusst ohne Middleware.
- **Asymmetrie zum sofortigen Pfad, bleibt bestehen:**
  - Sofortig: Claim → `pre_enqueue` → Veto mit Rollback.
  - Verzögert: `pre_enqueue` → Claim.
  - Folge: Bei einem Duplikat sieht die Middleware im verzögerten Pfad ein `pre_enqueue` ohne `post_enqueue`, im sofortigen gar keins. Das ist kein Fehler gegen E-059. Ein Satz in der Doku genügt.
- **Leader-Rückfall E-032 (`source="cli"`, Producer ohne Registry und ohne `unique_for`):** Der Leader berechnet den Hash beim Fire aus `definition.kwargs`, also nach der Middleware. Ändert die Middleware `kwargs`, weicht dieser Hash vom Hash eines gleichen sofortigen Aufrufs ab. Das ist ein Randfall ohne Regel; ich empfehle einen Satz in der Doku, keinen Fix.
- **Entscheidungen und ihre Tests:**
  - E-066 (`TestClientUniqueDeferred::test_deferred_with_unique_for_sets_flag_and_persists_hash`, `test_deferred_duplicate_returns_existing_and_skips_store` und `test_deferred_with_eta_ttl_covers_fire_plus_unique_for`): Der Claim kommt weiter vor dem Store, das Duplikat überspringt den Store, und die TTL-Formel bleibt. Die Tests bleiben grün.
  - Der Test `:1220ff.` („SETNX must happen BEFORE store.add_schedule“) bleibt grün.
  - E-033 (`test_deferred_explicit_none_persists_the_opt_out_as_cli_unique_without_hash`) und E-032 (`test_deferred_without_registry_and_unique_for_stays_cli`) betreffen `source` und `dedup_hash` der Definition; beide bleiben unverändert.
  - `test_deferred_duplicate_emits_signal` hört nur auf `TASK_DUPLICATE_DETECTED` und ist nicht betroffen.
  - Zu prüfen sind Tests, die im verzögerten Pfad `dispatch.assert_not_awaited()` oder eine genaue Zahl von Dispatches erwarten. In `test_schedule.py` fand ich keine; die Treffer in `:280` und `:420` gehören zum sofortigen Pfad bzw. zum Veto.
- **Mischbetrieb:**
  - Nur der Producer ändert sich. Format und Leader bleiben gleich.
  - Alte Leader publizieren `definition.kwargs` samt Middleware-Ergänzung, weil das Feld schon existiert.
  - Producer 0.12.0–0.12.2 umgehen die Middleware weiter, bis sie aktualisiert sind.
  - Für die Upgrade-Notiz: Eine Middleware mit Veto blockiert ab jetzt auch `delay`/`eta`-Aufrufe. Wer sich, auch unbewusst, auf den Umweg verlassen hat, sieht neue Ausnahmen.
- **Rollback:** Gespeicherte Definitionen bleiben gültig. Die Middleware wird wieder umgangen.

### Testszenarien

1. Die vier Nachweis-Tests (Marker `E-059` statt xfail).
2. `post_enqueue` läuft nach dem Speichern genau einmal, mit demselben `message_dict`. Im Duplikatpfad und beim Veto läuft es nicht.
3. Der Dedup-Hash ist stabil gegen Middleware-Änderungen:
   - Eine Middleware ergänzt `kwargs["corr"] = uuid4()`. Zwei Aufrufe `schedule(…, delay=30, unique_for=60)` mit gleichen Argumenten: Der zweite liefert die erste ID. `claim_unique` wird zweimal mit **demselben** Hash aufgerufen, und zwar dem Hash der Original-Argumente.
   - Ein sofortiger Aufruf mit gleichen Argumenten nutzt denselben Hash.
4. `message_dict` enthält `task_id` (gleich der Rückgabe), `queue` (aufgelöste Queue samt Prioritätssuffix) und `delivery_mode` (aus der `TaskInfo`).
5. `eta` in der Vergangenheit führt zu `ValueError`, ohne dass `pre_enqueue` aufgerufen wird. Das gilt ebenso für `unique_for=0` (F3-07).
6. Wirft `add_schedule`, gibt es weder `post_enqueue` noch `TASK_ENQUEUED`, und der Claim wird freigegeben (wie bisher `:1247ff.`).
7. Eine Änderung an `args` durch die Middleware wirkt nicht, wie im sofortigen Pfad. Optional, aber gut als Festschreibung der Parität.

### Doku, Changelog, Register

- `website/guides/middleware.md:27-28` (DE `middleware.de.md:27-28`), Tabelle:
  - `pre_enqueue` „Before `broker.publish`, or before a `delay`/`eta` task is stored“.
  - `post_enqueue` „After the broker confirms, or after a `delay`/`eta` task is stored“.
  - Dazu ein kurzer Absatz: Das Veto wirkt beim `schedule()`-Aufruf, nicht beim Fire. `kwargs`-Änderungen werden gespeichert und vom Leader publiziert. Cron-Fires laufen ohne Enqueue-Middleware (F3-36).
- `website/guides/signals.md:62` (DE `signals.de.md:66`): `TASK_ENQUEUED`, „after a successful `schedule()`; with `delay`/`eta` once the task is stored, not at fire time“.
- `website/reference/api.md:646` (DE `api.de.md:666`): „After a publish … or, for `delay`/`eta`, after the one-shot is stored.“
- **Changelog EN:** „`schedule(delay=…/eta=…)` now runs `pre_enqueue` before storing the task (a veto aborts it, no dedup flag is claimed) and fires `post_enqueue` and `TASK_ENQUEUED` once it is stored. Changes a middleware makes to `kwargs` are stored and published at fire time.“ Dazu als Upgrade-Notiz: Ein Veto blockiert jetzt auch verzögerte Aufrufe. DE entsprechend.
- **Register:**
  - Nachweis-Tests mit `E-059`.
  - E-059 `tests:` ergänzen, `ohne_test` entfernen.
  - Nachtrag zu E-059: Header gibt es mangels API nicht; `kwargs` werden wie im sofortigen Pfad übernommen; `post_enqueue` läuft nach dem Speichern; der Dedup-Hash stammt aus den Argumenten vor der Middleware.

---

## F3-07 (P3): `unique_for <= 0` an `schedule`/`schedule_many` (E-056, mit E-055)

### Ursache (verifiziert)

- `client/schedule_resolve.py:137-138`: Ein ausdrückliches `unique_for` wird ungeprüft übernommen (`final_for = unique_for`).
- Aufrufer:
  - `schedule()` sofort (`client/schedule.py:242`, TTL `:252`): `SET PX 0` bzw. ein negativer Wert führt zu einer Valkey-Fehlerantwort, also `BackendResponseError`.
  - `schedule()` verzögert (`:225`, TTL `:630`): `unique_for + fire_offset` ist positiv. Der Claim wird still gesetzt und lebt nur bis etwa zum Fire.
  - `schedule_many` über `_batch_prepare` (`schedule_resolve.py:165-176`).
- `TaskInfo._validate_unique` (`task.py:253-267`) und die CLI (`parser_ops.py:97-100`, `_positive_int`) prüfen schon.

### Nachweis-Test

`tests/unit/client/test_befund_f3_07_call_unique_for_bounds.py`: `0` und `-5` für sofort, verzögert und `schedule_many`. Erwartet wird `ValueError` mit „unique_for“, ohne Claim, Publish oder Store. Das entspricht E-056. Nicht abgedeckt sind die Fälle aus E-055 und E-056: `0.0` (Fehler), `-0.5` (Fehler) und `0.2` (wird 1). Die Falle bei `-0.5`: `ceil(-0.5) == 0`. Wird zuerst aufgerundet und dann geprüft, wäre das Ergebnis 0. Die Prüfung `<= 0` muss deshalb **vor** `ceil` auf dem Rohwert laufen; danach liefert jeder Rest > 0 mindestens 1.

### Fix (empfohlen)

- Eine einzige Stelle: der ausdrückliche Zweig von `_resolve_unique` (`schedule_resolve.py:137-138`). `final_for = coerce_unique_for(unique_for)` deckt alle drei Pfade ab, `schedule` sofort, `schedule` verzögert und `schedule_many`. Die Validierung läuft dort jeweils vor jedem Netzwerkaufruf (`schedule.py:225` vor `_schedule_deferred`, `:242` vor dem Claim in `:247`, `_batch_prepare` vor `_batch_claim_flags`).
- `coerce_unique_for(value) -> int` ist ein gemeinsamer Helfer für E-055 und E-056:
  - `bool` und nicht-numerische Werte → `TypeError` oder `ValueError`. Optional; die Typannotation deckt es ab.
  - Nicht endlich (`inf`, `nan`) → `ValueError("unique_for must be finite")`. Das folgt sinngemäß E-063: `nan` besteht `<= 0` nicht, und `ceil(nan)` würde ohne Hinweis auf „unique_for“ fehlschlagen.
  - `value == 0` → `ValueError("unique_for=0 is invalid; use None to disable dedup")` und `value < 0` → `ValueError(f"unique_for must be > 0 (got …)")`, beides wörtlich wie in `TaskInfo`.
  - Danach `int(math.ceil(value))`.
- Ort des Helfers: neben `_validate_unique` in `src/swarmq/task.py`. `TaskInfo` (F3-31) und der Client nutzen ihn dann gemeinsam.
- `unique_for=None` (E-033) und `_MISSING` (Rückfall auf die `TaskInfo`) bleiben unberührt. Der `TaskInfo`-Default ist bei der Konstruktion schon geprüft und wird nach F3-31 normalisiert.

### Zusammenspiel mit F3-31 (E-055, anderes Paket)

E-055 nennt ausdrücklich den Aufrufparameter `unique_for` von `schedule`/`schedule_many`. Das ist **dieselbe Zeile** wie bei F3-07 (`schedule_resolve.py:137-138`). Vorschlag:
- **Variante 1 (bevorzugt):** Die Unit F3-31 legt den Helfer in `task.py` an und nutzt ihn in `TaskInfo` und `SwarmQConfig`. Die Unit F3-07 übernimmt die Aufrufstelle vollständig: Bereich nach E-056 und `float`-Normalisierung nach E-055. Reihenfolge: F3-31 vor F3-07. Dateien: F3-31 bearbeitet `task.py` und `config.py`, F3-07 nur `client/schedule_resolve.py`.
- **Variante 2 (parallel):** F3-07 schreibt die Prüfung lokal in `schedule_resolve.py`. Wer zuletzt merged, stellt auf den gemeinsamen Helfer um, als Refactor ohne neues Verhalten.

Die TTL-Rechnungen `resolved_unique_for * 1000` (`schedule.py:252`, `schedule_resolve.py:176`) und `(unique_for + int(fire_offset_s)) * 1000` (`schedule.py:630`) bekommen danach immer `int`. Der `DataError` aus F3-31 entfällt dann auch am Aufruf.

### Folgefehler-Analyse

- **Entscheidungen und ihre Tests:**
  - E-056 (`tests/unit/test_task.py::test_taskinfo_unique_for_zero_raises`) bleibt unberührt.
  - E-033 (`test_schedule_per_call_override_none_disables_dedup`, `test_schedule_many_batch_override_none_disables_dedup` und `test_deferred_explicit_none_…`): `None` wird nicht validiert. Die Tests bleiben grün.
  - E-066: Die TTL-Tests nutzen positive `int`-Werte und bleiben grün.
- **Verhaltensänderung:** Ein Aufrufer mit `unique_for=0` bekommt jetzt `ValueError` statt `BackendResponseError` (sofort) bzw. statt stiller Annahme (verzögert). Das gehört in den Changelog, als Upgrade-Hinweis für verzögerte Aufrufe.
- **Mischbetrieb und Rollback:** nur im Client, keine Format- oder Key-Änderung.
- **CLI:** `--unique-for` ist bereits `_positive_int` und bleibt unverändert.

### Testszenarien

1. Die Nachweis-Tests mit `0` und `-5` (Marker `E-056`).
2. `0.0` und `-0.5` → `ValueError` in allen drei Pfaden, ohne Claim. Das ist die `ceil`-Falle.
3. E-055 (falls F3-07 die Aufrufstelle besitzt): `unique_for=0.2` mit sofortigem Aufruf führt zu `claim_unique(…, ttl_ms=1000)`. `unique_for=3600.0` führt zu `ttl_ms=3_600_000` als `int`. Verzögert mit `delay=10`, `unique_for=1.5` ergibt `ttl_ms=(2+10)*1000`.
4. `float("inf")` und `float("nan")` → `ValueError` mit „unique_for“.
5. `schedule_many` mit `unique_for=0` und zwei Elementen: Der Fehler kommt vor jedem Claim (`_batch_prepare`).

### Doku, Changelog, Register

- `website/reference/api.md:123` (DE `api.de.md:125`), Zeile `unique_for`: „Must be > 0 (`ValueError` otherwise; a `float` is rounded up to whole seconds). Pass `None` to disable dedup for this call.“ Für `schedule_many` gilt dasselbe in dessen Parameterabschnitt.
- **Changelog EN:** „`schedule()` and `schedule_many()` reject `unique_for <= 0` with `ValueError`, like `TaskInfo` (it used to fail in Valkey or, with `delay`/`eta`, be accepted silently).“ DE entsprechend.
- **Register:**
  - E-056 `tests:` um die Nachweis-Tests ergänzen; `ohne_test` entfällt.
  - E-055 bekommt den Test für die Aufrufstelle (Szenario 3), falls F3-07 ihn liefert; sonst bleibt er bei F3-31.

---

## F3-09 (P3): `get_results(timeout=0)` und letzter MGET nach Ablauf der Frist (E-030, Nachtrag)

### Ursache (verifiziert)

`client/results.py:150-167`:

```python
deadline = loop.time() + timeout
while loop.time() < deadline:
    raw_list = await self._backend.get_results(task_ids)
    ...
    await asyncio.sleep(poll_interval)
```

- Bei `timeout=0` ist die Bedingung sofort falsch. Es gibt keinen MGET, und `raw_list` bleibt `[None]*n`; also kommt `ResultNotReadyError` mit allen IDs.
- Läuft die Frist während `sleep(poll_interval)` ab, beendet die Schleife ohne letzten MGET. Zudem überzieht `sleep(poll_interval)` die Frist um bis zu `poll_interval`.

### Nachweis-Test

`tests/unit/client/test_befund_f3_09_get_results_timeout_zero.py` nutzt eine gefälschte Uhr in `results.asyncio` und prüft zwei Fälle:
- `timeout=0` → genau ein MGET, Ergebnisse da.
- `timeout=0.05`, `poll_interval=0.1`, zweiter MGET bringt das Ergebnis → genau 2 MGETs.

Das entspricht wörtlich dem Nachtrag zu E-030.

### Fix (empfohlen)

Muster wie `WorkflowHandle.get` (`workflow_orchestration.py:166-185`): erst lesen, dann die Restfrist prüfen.

```python
deadline = loop.time() + timeout
while True:
    raw_list = await self._backend.get_results(task_ids)
    if all(r is not None for r in raw_list):
        return [...]
    remaining = deadline - loop.time()
    if remaining <= 0:
        break
    await asyncio.sleep(min(poll_interval, remaining))
```

Mit `min(...)` liegt der letzte MGET genau an der Frist. Im zweiten Nachweis schläft der Client 0,05 s, liest erneut und hat das Ergebnis. Ohne `min` wäre der Test auch grün, er überzöge die Frist aber um bis zu `poll_interval`. Als Alternative käme ein separater MGET nach der Schleife infrage; das wäre doppelter Code.

### Folgefehler-Analyse

- **Aufrufer:**
  - `app.get_results` reicht durch.
  - `workflow_failure.py:41` und `workflow_orchestration.py:127` rufen `backend.get_results` direkt auf, ohne Frist. Sie sind nicht betroffen.
  - Die e2e-Tests (`test_bulk_mixed.py:314`, `test_worker_shutdown_load.py:227/306`, `test_schedfire_worker_crash.py:131`) haben großzügige Fristen und gewinnen höchstens einen letzten MGET.
- **Bestehende Unit-Tests:**
  - `tests/unit/client/test_results.py:243-247` (`test_get_results_timeout_raises`) bekommt immer `[None, None]` und läuft jetzt mit einem MGET mehr. Er bleibt grün und zählt keine Aufrufe.
  - `test_get_results_returns_all_in_order` bleibt grün.
- **Entscheidungen:** Die `entscheidung`-Tests von E-030 prüfen `wait_for_result` und die FS-15-Tests, nicht `get_results`. Sie bleiben unberührt. Pub/Sub kommt nicht hinzu, was dem Nachtrag entspricht.
- **Randfälle:** Ein negatives `timeout` führt zu einem MGET und dann zum Fehler, wie `timeout=0`. Bei `poll_interval <= 0` ergibt sich wie heute eine Schleife mit `sleep(0)`. Das ist nicht Teil des Befunds.
- **Mischbetrieb und Rollback:** nur im Client.

### Testszenarien

1. Die beiden Nachweis-Tests (Marker `E-030`).
2. `timeout=0` und das Ergebnis fehlt: genau ein MGET, dann `ResultNotReadyError` mit den fehlenden IDs.
3. Der letzte Schlaf wird auf die Restfrist gekürzt: `timeout=0.05`, `poll_interval=1.0`, die gefälschte Uhr protokolliert `sleep(0.05)`, nicht `sleep(1.0)`.
4. Teilergebnis zum Schluss (`[x, None]`): Die Fehlermeldung nennt nur die fehlende ID.

### Doku, Changelog, Register

- `website/reference/api.md:263-265` (DE `api.de.md:269-271`): „With `timeout=0` it reads once (one `MGET`); when the timeout expires while waiting, a final `MGET` runs before `ResultNotReadyError`.“
- **Changelog EN:** „`get_results(timeout=0)` reads once instead of reporting every task as not ready, and a timeout that expires while waiting ends after one last read.“ DE entsprechend.
- **Register:** Die Nachweis-Tests in `tests:` von E-030 eintragen. Im Nachtrag ist der Test schon genannt.

---

## F3-29 (P3, nur Doku): Dedup verzögerter Tasks ohne `unique_for` (E-066)

### Befund (verifiziert)

`website/guides/scheduling-and-retries.md:52-54` (DE `scheduling-and-retries.de.md:55-57`): „If you leave out `unique_for`, the leader resolves it from its own `TaskInfo` at fire time and skips the task …“. Das stimmt nur für einen Producer **ohne** Registry. Mit Registry löst der Producer selbst den Default aus der `TaskInfo` auf. Das geschieht in `_resolve_unique` (`schedule_resolve.py:131-136`); danach folgt der Claim in `_schedule_deferred` (`schedule.py:622-640`) mit TTL `(unique_for + Verzögerung)` und `source="cli_unique"`. So steht es auch in E-066 und wird von `TestClientUniqueDeferred` festgeschrieben. Der Absatz widerspricht außerdem seinem eigenen ersten Satz („Dedup … is settled when you call `schedule()`“).

### Neuer Text (Vorschlag)

**EN** (ersetzt „If you leave out `unique_for`, … holds the flag.“):

> If you leave out `unique_for`, a producer whose registry knows the task applies `TaskInfo.unique_for` right away: it claims the flag when you call `schedule()`, for `unique_for` plus the delay, and a call with the same arguments in that time is a duplicate. Only a producer without a registry (or one that does not know the task) leaves it to the leader, which resolves `unique_for` from its own `TaskInfo` at fire time and skips the task if another run with the same arguments holds the flag.

**DE** (ersetzt „Fehlt `unique_for`, … das Flag hält.“):

> Fehlt `unique_for` und kennt die Registry des Producers den Task, gilt sofort `TaskInfo.unique_for`: Der Producer setzt das Flag beim Aufruf von `schedule()`, für `unique_for` plus Verzögerung, und ein gleicher Aufruf in dieser Zeit ist ein Duplikat. Nur ein Producer ohne Registry (oder ohne diesen Task) überlässt es dem Leader. Der löst `unique_for` beim Fire aus seiner eigenen `TaskInfo` auf und überspringt den Task, solange ein anderer Lauf mit denselben Argumenten das Flag hält.

Der Rest des Absatzes (Opt-out ab 0.12.1) bleibt unverändert.

### Folgen

- Kein Code und kein Test. Doku-Tests: Falls ein Sprachpaar- oder Abschnitts-Guard Absätze zählt, behalten EN und DE dieselbe Struktur.
- **Changelog:** Ein Eintrag unter „Documentation“ ist optional; EN: „Corrected how a delayed task without `unique_for` is deduplicated.“
- **Register:** In E-066 kann der Satz „`scheduling-and-retries.md:52-54` (+ DE) widerspricht sich … und wird korrigiert (F3-29, Doku-Fix)“ nach dem Fix zu „korrigiert in 0.12.3“ werden. `tests:` bleiben.

---

## Schnitt für die Umsetzung (Dateikonflikte minimieren)

`client/schedule.py` berühren nach diesem Schnitt nur **F3-02** und der kleine Client-Anteil von **F3-01**. F3-07 und F3-31 arbeiten an der Aufrufstelle in `client/schedule_resolve.py`, nicht in `schedule.py`. F3-09 berührt nur `client/results.py`.

| Unit | Befund | Produktionsdateien | Reihenfolge |
|---|---|---|---|
| S1 | F3-01 | `src/swarmq/lua/sched_add.lua` (neu), `src/swarmq/backend/valkey/schedules.py`, `src/swarmq/scheduler/definition.py` | unabhängig. Der Nachweis wird ohne Client-Änderung grün |
| S2 | F3-02 und der Client-Anteil von F3-01 (`False` aus `add_schedule` → Fehler) | `src/swarmq/client/schedule.py` | nach S1 (der Konfliktfall braucht S1, damit `False` nur noch einen echten Konflikt bedeutet) |
| S3 | F3-07 und F3-31 an der Aufrufstelle | `src/swarmq/client/schedule_resolve.py` (Helfer aus `src/swarmq/task.py` von F3-31) | nach F3-31 (Variante 1) oder parallel (Variante 2) |
| S4 | F3-09 | `src/swarmq/client/results.py` | unabhängig |
| S5 | F3-29 | `website/guides/scheduling-and-retries.md`, `.de.md` | unabhängig |

Gemeinsame Hotspots:
- `website/reference/changelog.md` und `changelog.de.md` (alle Units): Ein Sammel-Commit am Ende oder je Unit nur angehängte Einträge im Abschnitt des nächsten Patch-Release. Achtung: `changelog.md:7` führt noch „[0.12.2] — unreleased“. Der Koordinator klärt, ob 0.12.3 einen eigenen Abschnitt bekommt.
- `website/reference/api.md` und `api.de.md` (S1 Contracts, S2 Signals, S3 `unique_for`, S4 `get_results`): Es sind verschiedene Abschnitte, aber dieselbe Datei. Nacheinander mergen.
- `tests/unit/client/test_schedule.py`: Neue Tests von S2 kommen dorthin. S3 legt seine Zusatztests in die Nachweis-Datei F3-07 oder in eine neue Datei `tests/unit/client/test_schedule_resolve.py` und vermeidet so den Konflikt.
- Register: `e056-…`, `e058-…`, `e059-…` und `e030-…` je von genau einer Unit (S3, S1, S2, S4). `e055-…` ändert F3-31 oder S3, nicht beide.

## Dateien je Befund

| Befund | Produktion | Tests | Doku | Register |
|---|---|---|---|---|
| F3-01 | `src/swarmq/lua/sched_add.lua` (neu), `src/swarmq/backend/valkey/schedules.py`, `src/swarmq/scheduler/definition.py`, `src/swarmq/client/schedule.py` (2–4 Zeilen, in S2) | `tests/integration/backend/test_befund_f3_01_schedule_add_lost_reply.py` (Befehl `EVALSHA`, Marker), `tests/unit/backend/test_valkey_schedules.py`, `tests/unit/scheduler/test_definition.py`, `tests/integration/backend/golden/lua_contract.txt`, `tests/integration/backend/_golden.py`, optional ein neuer Integrationstest für `sched_add` | `website/reference/api.md`, `api.de.md` (Contracts), optional `website/guides/scheduling-and-retries.md`/`.de.md`, Changelog EN/DE | `compound-packs/swarmq-entscheidungen/e058-oneshot-speichern-idempotent.md` |
| F3-02 | `src/swarmq/client/schedule.py` | `tests/unit/client/test_befund_f3_02_deferred_pre_enqueue.py` (Marker), `tests/unit/client/test_schedule.py` (neue Tests) | `website/guides/middleware.md`/`.de.md`, `website/guides/signals.md`/`.de.md`, `website/reference/api.md`/`.de.md`, Changelog EN/DE | `e059-enqueue-middleware-ohne-umweg.md` |
| F3-07 | `src/swarmq/client/schedule_resolve.py` (dazu der Helfer in `src/swarmq/task.py` aus F3-31) | `tests/unit/client/test_befund_f3_07_call_unique_for_bounds.py` (Marker und Zusatzfälle) oder `tests/unit/client/test_schedule_resolve.py` (neu) | `website/reference/api.md`/`.de.md` (`unique_for`-Zeile), Changelog EN/DE | `e056-unique-for-fail-fast.md`, ggf. `e055-sekundenfelder-float-normalisieren.md` |
| F3-09 | `src/swarmq/client/results.py` | `tests/unit/client/test_befund_f3_09_get_results_timeout_zero.py` (Marker), optional `tests/unit/client/test_results.py` | `website/reference/api.md`/`.de.md` (`get_results`), Changelog EN/DE | `e030-get-result-polling.md` |
| F3-29 | – | – | `website/guides/scheduling-and-retries.md`, `scheduling-and-retries.de.md`, optional Changelog | `e066-deferred-dedup-beim-producer.md` (Formulierung) |

## Wechselwirkungen mit anderen Befunden

- **F3-01 ↔ F3-02:** Beide betreffen `_schedule_deferred`. Daraus folgt eine harte Bedingung für beide: Der Client ruft weiter genau `store.add_schedule(definition)` auf, ohne Keyword-Argumente. Davon hängen die Nachweise F3-02/F3-07 und `TestClientScheduleDelayEta` ab, sowohl über die `side_effect`-Signatur als auch über `assert_not_awaited`. Die Oneshot-Idempotenz gehört darum in `ScheduleStore.add_schedule`, nicht in eine neue Client-Methode.
- **F3-01 ↔ F3-02, Pfad zum Fehlerfall:** Wirft `schedule()` bei einem Konflikt, gibt es weder `post_enqueue` noch `TASK_ENQUEUED`. Das ist Schritt 8 von F3-02 und läuft ohnehin nach dem Speichern.
- **F3-02 ↔ F3-07/F3-31:** Die Validierung von `unique_for` und `eta` muss **vor** `pre_enqueue` bleiben (`_resolve_unique` in `schedule()` vor `_schedule_deferred`). S2 darf `_resolve_unique` beim Umbau nicht hinter die Middleware verschieben.
- **F3-02 ↔ F3-36:** Cron-Fires bleiben ohne Middleware (E-059, bewusste Folge). Der verzögerte Pfad speichert mit S2 die von der Middleware geänderten `kwargs`. Löst F3-36 später ein Leader-`pre_enqueue` für Oneshots aus, liefe die Middleware für denselben Task zweimal. Bei F3-36 ist das zu beachten.
- **F3-02 ↔ E-032 (Leader-Rückfall):** Bei `source="cli"` hasht der Leader die gespeicherten, von der Middleware geänderten `kwargs`. Siehe F3-02; dazu gehört ein Satz in der Doku.
- **F3-07 ↔ F3-31:** gleiche Zeile in `schedule_resolve.py`. Ein gemeinsamer Helfer und die Prüfung `<= 0` vor `ceil` sind nötig (Falle `-0.5`). Zur Reihenfolge siehe S3.
- **F3-07 ↔ F3-04/E-063:** `inf`/`nan` am Aufruf sind kein Konfigurationsfeld. Die Ablehnung mit `ValueError` folgt aber dem Sinn von E-063. Keine Regeländerung nötig, nur ein konsistenter Text.
- **F3-09:** keine Überschneidung im Code. Mit E-030/FS-15 nur über die Doku (`api.md`, Abschnitt Ergebnisse).
- **F3-29 ↔ F3-02:** Der neue Doku-Absatz zur Dedup bei `delay`/`eta` und der Middleware-Absatz (Veto beim Aufruf) gehören thematisch zusammen. Werden beide in `scheduling-and-retries.md` ergänzt, nacheinander mergen.

## Hinweise zu Register-Entscheidungen (nicht umsetzbar oder fraglich)

1. **E-059, „Header-Änderungen speichern“:** `MessageContext` hat kein Header-Feld, eine Middleware kann heute keine Header setzen. Umsetzbar ist nur die Übernahme von `kwargs` wie im sofortigen Pfad. Ein Header-Feld in `ScheduleDefinition` wäre ein Formatwechsel, den Leader 0.12.0–0.12.2 still verwerfen (msgspec ignoriert unbekannte Felder; ihre Commits schreiben es weg). Deshalb nicht in 0.12.3. Ein Nachtrag zu E-059 ist nötig.
2. **E-059, `post_enqueue`:** nicht ausdrücklich entschieden. Empfohlen: nach dem Speichern aufrufen, mit Nachtrag.
3. **E-058, CLI:** Die bewusste Folge („already exists“, obwohl der eigene Insert durchging) ist nur harmlos, wenn `add` atomar ist. Heute fehlt nach der Wiederholung der Index, und CLI-Crons feuern nie. Option A (Lua) behebt das. Bei Option B braucht E-058 einen Nachtrag oder es entsteht ein eigener Befund.
4. **E-058, Byte-Gleichheit:** Ein falscher Konflikt ist möglich, wenn der Leader den Oneshot zwischendurch umschreibt (`unroutable_since`). „Entscheidung überdenken?“ mit dem Argument: Bei Oneshots beweist schon die frische uuid7 im Namen die Herkunft. Kein Blocker.
