# Fix-Recherche Scheduler, Schedfire, Valkey-Claims (FS-03, FS-06, FS-07, FS-14, FS-15, FS-31)

Stand: Worktree `swarmq-fehlersuche` (906afac, v0.12.0 + Nachweise), Register `swarmq-entscheidungen` (45c6336). Nur gelesen. Bibliotheken: valkey 6.1.1, msgspec aus `.venv`.

Ausgeführt: alle Unit-Nachweise dieser Gruppe mit `--runxfail` (FS-03 ×2, FS-07 ×2, FS-14, FS-15, FS-31). Jeder scheitert aus dem genannten Grund. Den Integrationsnachweis FS-06 habe ich nicht erneut ausgeführt; den Mechanismus habe ich im Quelltext von valkey-py geprüft (siehe FS-06). Per Probe geprüft: msgspec ignoriert unbekannte Felder einer `ScheduleDefinition` beim Dekodieren, lehnt aber einen unbekannten `source`-Wert ab (`Invalid enum value 'cli_nodedup' - at $.source`). Ohne `omit_defaults` kodiert msgspec alle Felder mit, auch die mit Defaultwert.

Gemeinsame Mischbetriebs-Fakten (gelten für alle Befunde):
- Lua-Skripte laufen über `register_script` → EVALSHA. Ein geänderter Skripttext bekommt eine neue SHA; alte und neue Fassung liegen nebeneinander im Skript-Cache. Es gibt keine Namenskollision. Kompatibel bleibt ein geändertes Skript, solange Key-Format, ARGV-Bedeutung und Rückgabewerte gleich bleiben. Die Golden-Datei `tests/integration/backend/golden/lua_contract.txt` prüft die Arität (`lock_acquire ARGV=4`, `sched_fire_claim ARGV=3`). Eine Änderung dort zeigt einen Vertragsbruch an.
- Neue Header-Namen ändern `golden/headers.txt`. Ein 0.12.0-Worker ignoriert unbekannte Header und kopiert sie weiter (`republish_headers` nimmt nur `_NOT_COPIED` aus).
- Die Mischbetriebs-Suite `tests/compat` hat als Basis nur 0.11.0/0.11.1 (`constraints-0.11.*.txt`). Für 0.12.1 neben 0.12.0 fehlt eine Basis (`constraints-0.12.0.txt` + Fixture). Ohne sie ist „rolling-deploy-sicher“ für keinen dieser Fixes automatisch belegt.

---

## FS-03 (P1): Schedfire-Gate bestätigt Neuzustellung mit fremdem Claim; Semaphor-Warten außerhalb try/finally

### Ursache

**Teil 1 (K-2): Verlust nach Worker-Tod.**
- `worker/gates/schedfire.py:171-199`: Liefert `claim_fire` 0, folgen `ack` und `Terminal.never_release()`. Der Docstring `:105-107` („if that worker crashes … the in-flight key expires and the next redelivery re-runs“) trifft nicht zu, denn nach dem Ack gibt es keine nächste Zustellung.
- Ablauf: W1 hält `sq:sched:inflight:<key>` mit TTL `max(scheduler_inflight_ttl=300 s, hard_timeout+30)` (`:144-154`), dann SIGKILL/OOM. Der Broker stellt sofort an W2 zu. `sched_fire_claim.lua:24-38` sieht einen fremden Wert, Ergebnis 0, das Gate bestätigt. Der Fire ist weg.
- Der Ausführungsschutz würde genau das abfangen (`execution_guard.py:165-166`, „running“ → `_defer`). Er liegt aber im `try` (`pipeline.py:633`), also hinter dem Schedfire-Gate (`pipeline.py:429`).

**Teil 2 (K-5): Abbruch am Semaphor.**
- `pipeline.py:571-575`: `_pre_pipeline_gates` (Claim gewonnen) und `await self._semaphore.acquire()` liegen vor dem `try` (`:599`).
- Bricht `_execute_shutdown` den wartenden Handler ab, verlässt der `CancelledError` `_process_message` ohne `finally`. Der Claim bleibt stehen, die Nachricht ist unbestätigt, der Broker stellt sie neu zu, und Teil 1 bestätigt sie als Duplikat.
- Für Gate-Ausnahmen gibt es die Freigabe schon (`pipeline.py:430-439`, `self._detached(self._release_schedfire_claim(ctx))`). Für `acquire()` fehlt sie.

### Lösungsoptionen Teil 1

**A – Broker-Neuzustellung mit fremdem Claim zurückstellen statt bestätigen (empfohlen).**

Ablauf im Gate:
- Ist `claimed == False` und die Zustellung eine Broker-Neuzustellung (`broker_redelivered(headers)`, also `x-delivery-count`/`x-acquired-count` > 0, `gates/__init__.py:227-231`) oder eine schon zurückgestellte Schedfire-Kopie (neuer Header, z. B. `x-swarmq-schedfire-defer-count`), dann `retry_later` mit wachsender Wartezeit wie `ExecutionGuard._defer` (`execution_guard.py:197-228`, `backend_defer_delay`) und `Redelivered` zurückgeben.
- Fällt `retry_later` aus, folgt `redeliver_in_place` (Claim gehört nicht uns, `ctx.schedfire_claim = None`, also nur `nack` nach `redeliver_pause_s`).
- Ein frisches Doppel-Fire des Leaders (ohne Broker-Zähler, ohne Defer-Header) bleibt ein Duplikat und wird bestätigt.

Der Defer-Header muss **mitkopiert** werden, also nicht in `_NOT_COPIED`. Sonst verliert ihn eine Backend-Ausfall-Kopie (`defer_backend_unavailable` beim Completed-Check), sie kommt ohne Zähler zurück und wird bei noch fremdem Claim bestätigt. Ein klebender Header schadet nicht: Er macht aus „bestätigen“ nur „warten, bis Completed oder Ablauf“.

Ausgang einer zurückgestellten Kopie:
- Completed-Marker gesetzt (Original oder Vorgänger fertig): Ack.
- Claim abgelaufen (Vorgänger tot): Claim, Lauf.
- Claim lebt noch (Original läuft weiter, z. B. nur sein Kanal ist gerissen): erneut zurückstellen.

Folgefehler-Analyse:
- **Aufrufer/Nachbarpfade:**
  - Nur das Gate ist betroffen; `_pre_pipeline_gates` behandelt `Redelivered` schon (`finalize_schedfire(terminal=False)` ist No-op, weil `ctx.schedfire_claim` None ist).
  - `republish_headers` übernimmt bestehende Claim-Tokens unverändert.
  - Der Ausführungsschutz bleibt die zweite Linie: Die Kopie trägt dieselbe `task_id`. Läuft das Original noch (Kanalverlust), meldet `guard_claim.lua` „running“, sobald der Fire-Claim abgelaufen ist, und stellt zurück. Ein Absturzloop endet über `execution_guard_max_orphans` (Default 3) in der DLQ, weil die Zurückstellung das Broker-`x-delivery-limit` umgeht.
- **Entscheidungen:**
  - E-046 verlangt genau das („muss … erhalten (Zurückstellen statt Bestätigen)“).
  - E-047: Doppel-Publishes zweier Leader bleiben Duplikate (frisch, keine Zähler), unverändert.
  - E-009: Der Schedfire-Key mit Replay-Suffix bleibt unverändert, `test_a_plain_duplicate_fire_keeps_the_key_and_is_skipped` arbeitet mit Completed-Marker und bleibt grün.
  - E-010: Zurückgestellte Kopien treffen nach dem Lauf auf den Completed-Marker (TTL ≥ `scheduler_dedup_ttl`, mit `max_age` ≥ `max_age+300`), unberührt.
  - Keine `entscheidung`-Tests betroffen (Marker-Suche E-046: keine festschreibenden Tests vorhanden, `ohne_test: verletzt`).
- **Bibliothek:** RabbitMQ setzt bei Quorum-Queues (Default `queue_type="quorum"`, `config.py:227`) nach Kanal- bzw. Consumer-Ende einen Zähler. 4.x zählt `x-acquired-count`, das prüft `broker_redelivered` mit. **Klassische Queues tragen keine Zähler.** Dort wirkt Option A nicht, das Verhalten bleibt wie 0.12.0 (Doku-Grenze). `Delivery` hat kein `redelivered`-Feld (`contracts/message.py:15-29`). Das AMQP-Flag zu nutzen hieße, den Vertrag zu ändern.
- **Mischbetrieb:**
  - Keine Lua-, Key- oder Wert-Änderung.
  - Eine zurückgestellte Kopie, die an einen 0.12.0-Worker geht, wird dort bei fremdem Claim bestätigt, wie heute. Ist der Claim schon frei, läuft sie. Der Fix wirkt also erst voll, wenn alle Worker auf 0.12.1 sind, ist aber nie schlechter als 0.12.0.
  - Rollback 0.12.1 → 0.12.0: Kopien mit dem neuen Header laufen auf 0.12.0 wie normale Zustellungen.
  - `golden/headers.txt` bekommt den neuen Namen.
- **Wechselwirkungen:**
  - FS-06: Nach verlorener Antwort hält eine *frische* Zustellung ihren eigenen Claim für fremd und wird bestätigt. Option A deckt das nicht ab, weil die Zustellung keine Neuzustellung ist. FS-06 braucht den eigenen Fix.
  - FS-08/FS-02 (Publish-Hänger bzw. -Deadline) treffen das `retry_later` der Zurückstellung wie jede andere Kopie.
  - FS-13: Ein Fire mit Unique-Flag hält es heute nach dem Duplikat-Ack bis `unique_for` (`never_release`). Mit A behält die Kopie den Unique-Header und gibt das Flag beim Lauf frei. Das verbessert FS-13 nebenbei.
- **Neue Fehlermodi:**
  - Zusätzliche Kopien während der Claim-TTL: etwa 9–10 bei 300 s mit Backoff 1→60 s, bei langem `hard_timeout` mehr. Sinnvoll: ein Stall-Log wie `execution_guard_defer_stalled`.
  - Schedfire-Fires laufen nach Worker-Tod mit bis zu `scheduler_inflight_ttl` (bzw. `hard_timeout+30`) Verspätung. Das deckt E-046 („nach Ablauf bzw. Übernahme des Claims“).

**B – Jede Zustellung mit fremdem Claim zurückstellen (auch frische Duplikate).**
- Deckt klassische Queues, FS-06 und K-5 als Rückfall ab und braucht keinen Header.
- **Neuer Doppellauf-Fall:** Ein frisches Leader-Duplikat (PublishIndeterminate- oder Commit-Fehler-Refire, Split-Brain) hat eine andere `task_id` (`_fire_task_id`, uuid7 je Poll). Läuft das Original ohne `hard_timeout` länger als 300 s, läuft der In-Flight-Claim ab (er wird nicht verlängert). Die wartende Kopie läuft dann parallel, und der Ausführungsschutz greift nicht (andere Identität). Heute wird dieses Duplikat bestätigt.
- Abgelehnt, solange der Fire-Claim nicht wie der Guard-Marker verlängert wird.

**C – Bei Broker-Neuzustellung den Claim übernehmen und dem Ausführungsschutz die Parallelität überlassen.**
- Schneller: Fire nach ~60 s statt nach 300 s.
- Hängt aber an `execution_guard=True` und gilt nicht für FIRE_AND_FORGET-Tasks (`execution_guard.py:116`).
- Ohne Schutz entsteht wieder die Parallelausführung bei Kanalverlust, gegen die d93d3ab baute. Bräuchte eine neue Entscheidung. Nicht empfohlen.

### Lösungsoptionen Teil 2 (K-5)

**A – `acquire()` absichern wie die Gate-Ausnahme (empfohlen):**
```python
try:
    await self._semaphore.acquire()
except BaseException:
    await self._detached(self._release_schedfire_claim(ctx))
    raise
```
- Geshieldet und getrackt (`_detached`). `_execute_shutdown` wartet vor dem Backend-Schließen auf `_detached_releases` (`shutdown.py:124-129`).
- Sonst hält die Zustellung an dieser Stelle nichts: Guard, Lock und Unique-Start-Release liegen im `try`.

**B – `acquire()` in den `try` ziehen (Flag „acquired“):**
- Der `finally` liefe mit dem Default-Outcome `Terminal.release_in_finally()` (`pipeline.py:587`). Er schriebe beim Abbruch den **Completed-Marker** (terminal) und verlöre den Fire endgültig.
- Bräuchte ein eigenes Outcome und ändert die U19-Logik. Gefährlicher, abgelehnt.

Wechselwirkung: FS-04 (Dishka-`ExitError` überspringt Semaphor- und Lock-Freigabe) ändert denselben `finally`/Semaphor-Bereich. Beide Fixes nacheinander und im selben MR planen, Option A berührt den `finally` nicht.

Test-Hinweis aus der Gegenprüfung: `test_worker.py:2509-2516` („Only one release call in start-mode“) bleibt mit Option A unberührt (keine Unique-Freigabe vor dem `try`).

### Empfehlung
- Teil 1 Option A plus Teil 2 Option A, dazu FS-06 (claim_fire).
- Den Docstring des Gates und von `tests/unit/worker/gates/test_schedfire.py::test_duplicate_when_inflight_claim_lost` korrigieren. Der Test bleibt grün, weil er eine frische Zustellung prüft.
- `tests/integration/backend/test_schedfire_claim_handover.py`: Die Tests `test_delivery_without_token_stays_a_duplicate` und `test_redelivery_of_the_copy_after_takeover_stays_a_duplicate` bleiben grün (ihre Kopien tragen keine Broker-Zähler). Ihr Docstring („Broker-Redelivery … bleibt ein Duplikat“) muss auf „wird zurückgestellt, läuft nicht parallel“ korrigiert werden. Das sind keine `entscheidung`-Tests.

Zusätzliche Tests:
1. Unit: Neuzustellung mit Zähler und fremdem Claim → `retry_later` mit Defer-Header, kein Ack, kein Lauf.
2. Unit: Kopie mit Defer-Header ohne Zähler, Claim weiter fremd → erneut zurückgestellt (Backoff wächst).
3. Unit: Kopie mit Defer-Header, Completed-Marker gesetzt → Ack.
4. Unit: Backend-Ausfall-Kopie einer zurückgestellten Kopie behält den Defer-Header.
5. Unit: Frisches Duplikat ohne Zähler → Ack (Regression E-047).
6. Unit: `retry_later` scheitert → `nack` ohne Claim-Freigabe fremder Claims.
7. Integration (Testports): Claim mit Owner „W1“ direkt setzen, Neuzustellung → zurückgestellt; nach Ablauf der Claim-TTL läuft die Kopie genau einmal.
8. Integration: Original läuft (Claim und Guard-Marker leben), Neuzustellung mit Zähler → kein Parallellauf, nach Abschluss Ack.
9. e2e: der in Plan 2026-04-17-003 H-2 geforderte SIGKILL-Test mit Cron-Task (E-046 `ohne_test`).
10. Compat: 0.12.0-Worker neben 0.12.1 (zurückgestellte Kopie bei 0.12.0 → Verhalten wie heute, kein Fehler).
11. K-5: Nachweis vorhanden. Zusätzlich: Abbruch am Semaphor ohne Schedfire-Header → keine Freigabe-Aufrufe.

Dateien:
- `src/swarmq/worker/gates/schedfire.py`
- `src/swarmq/worker/pipeline.py`
- `src/swarmq/headers.py`
- `tests/integration/backend/golden/headers.txt`
- Doku `website/explanation/delivery-semantics*.md` (klassische Queues als Grenze)

Aufwand M, Risiko mittel (Lieferpfad jedes Fires; mehr Kopien; klassische Queues unverändert).

---

## FS-06 (P2): valkey-py wiederholt nicht idempotente Befehle

### Ursache
- `backend/valkey/connection.py:216-226`: `Retry(ExponentialBackoff(cap=2, base=0.05), retries=5)` mit `retry_on_error=[ConnectionError, TimeoutError, OSError]` (`:36-40`), dazu `socket_timeout=10 s`.
- valkey 6.1.1 `asyncio/client.py:622-651`: `execute_command` → `conn.retry.call_with_retry(lambda: _send_command_parse_response(...), _disconnect_raise)`. Ein Lesefehler *nach* der Ausführung auf dem Server sendet den ganzen Befehl erneut, auch EVALSHA.

Folgen:
- `claim_unique` (`results.py:451-466`, `SET NX GET PX`): Die Wiederholung liefert die eigene `task_id` als „existing“.
  - `Client.schedule` gibt sie zurück und publiziert nie (`client/schedule.py:237-238`).
  - Gleiches gilt für `_schedule_deferred` (`:609-610`) und `schedule_many` (`:502-507`).
  - Beim Leader-Cron (`scheduler/fire.py:175-202`) folgt `dup_detected`, Commit, Fire verloren.
- `claim_fire` (`sched_fire_claim.lua`): Der eigene Owner zählt nicht als übernehmbar, Ergebnis 0, das Gate bestätigt als Duplikat.
- `lock_acquire.lua:43-50` (Mutex `SET NX`): Ergebnis 0, eigener verwaister Lock bis `hard_timeout+30`. Der Semaphor (`:52-71`) scheitert nur, wenn die Menge samt eigenem Token voll ist.

### Optionen

**A – Gezielt idempotent machen, je Befehl (empfohlen).**

1. **claim_fire ohne Lua-Änderung:**
   - `ValkeyScheduleStore.claim_fire` (`schedules.py:228-246`) hängt bei gesetztem `owner` den eigenen Owner an die Übernahme-Tokens an (`args=[owner, ttl, *offered, owner]`). `sched_fire_claim.lua:27-31` wertet `current == ARGV[i]` dann als eigen und schreibt denselben Wert mit neuer TTL (idempotent).
   - Bei `owner=None` (Altvertrag „1“) nichts anhängen, sonst gewännen zwei Aufrufe ohne Owner beide. Der Contract-Test `tests/contracts/coordination.py:607-608` bleibt so grün.
   - Gleiche Logik im In-Memory-Backend (`tests/support/inmemory/backend.py:547`).
   - Sicherheit: `owner` ist je Zustellung ein frisches `uuid4` (`schedfire.py:159`). Ein Treffer ist nur durch die Wiederholung desselben Aufrufs möglich. Die Zusage „Broker-Redelivery derselben Kopie bleibt Duplikat“ (`sched_fire_claim.lua:9-11`) bleibt, weil eine Neuzustellung einen neuen Owner hat.
2. **lock_acquire.lua:**
   - Mutex: `if GET(key) == owner then return 1`.
   - Semaphor: `if ZSCORE(key, owner) then return 1`, vor der Zählung.
   - ARGV-Anzahl bleibt 4, die Golden-Arität bleibt gleich. Der Kommentar `:28-31` („caller should not retry with the same token“) wird ersetzt.
   - Die Wiederholung eines Semaphor-Aufrufs hat dasselbe `now_ms` (ARGV aus demselben Aufruf), der Score bleibt also gleich.
3. **claim_unique an den Aufrufstellen, nicht im Backend:**
   - Wo die `task_id` in genau diesem Aufruf frisch erzeugt wurde, gilt `existing == task_id` als „geclaimt“: `_claim_unique_flag` (`client/schedule.py:100-108`, für `schedule` und `_schedule_deferred`), die Auswertung in `_batch_claim_flags`/`schedule_many` (`:338-347`, `:502`), Leader-Cron in `_fire_one` (`fire.py:189`, nur wenn `definition.cron is not None`).
   - **Nicht** zentral in `ValkeyResults.claim_unique`, aus zwei Gründen:
     - `swarmq schedule add --unique-for` (`cli/schedule_cmd.py:228`) nutzt als Owner den stabilen Schedule-Namen. Ein zweites `add` würde dann nicht mehr als Duplikat gemeldet.
     - Der Leader-Oneshot (stabile `task_id`, `fire.py:377-386`) würde nach `PublishIndeterminate` beim nächsten Poll erneut publizieren. Das widerspricht E-011 („nicht bei `PublishIndeterminate`“, der Wiederholungsversuch gilt als Duplikat).

**B – Transport-Retry für nicht idempotente Befehle abschalten** (zweiter Client bzw. Pool mit `retries=0`):
- Ein Fehler kommt dann als `BackendConnectionError` an, und der Aufrufer muss mit unklarem Ausgang umgehen. Das ist genau FS-07, also doppelt Arbeit.
- Zweiter Verbindungspool; die Verfügbarkeit bei kurzen Abrissen sinkt (der Toxiproxy-Spike maß diese Transparenz). Abgelehnt.

**C – Wert mit Nonce** (`task_id|nonce`) im Unique-Flag:
- 0.12.0-Worker geben das Flag nicht mehr frei (`unique_release.lua` vergleicht exakt mit `task_id`).
- 0.12.0-Clients bekämen `task_id|nonce` als bestehende ID. **Mischbetrieb bricht.** Abgelehnt.

### Folgefehler-Analyse (Option A)
- **Aufrufer:**
  - `claim_fire` hat nur einen Produktionsaufrufer (`schedfire.py:174`).
  - `lock_acquire.lua` nutzen das Lock-Gate (`gates/lock.py:251`, `uuid4` je Zustellung) **und die Leader-Wahl** (`scheduler/loop.py:438-446`, stabiler `owner_token` je `SchedulerLoop`). Für die Leader-Wahl ändert sich: Hält der Key nach einem Renew-Fehler-Rücktritt (E-048) noch den eigenen Token, gewinnt der eigene nächste `acquire` sofort, statt bis zum TTL-Ablauf zu warten.
  - Das ist ungefährlich: Kein anderer kann den Key haben, also gibt es kein Split-Brain. Der E-013-Rücktritt löscht den Key ohnehin, die Abkühlzeit verhindert die Kandidatur (`_in_stepdown_cooldown`). E-048-Tests mocken den LockManager und bleiben grün.
- **Entscheidungen:**
  - E-022: `lock_acquire.lua` setzt PEXPIRE nur verlängernd; der Selbsttreffer ändert die TTL nicht.
  - E-011: Mit der Einschränkung „nur frische `task_id`“ bleibt `test_indeterminate_publish_keeps_the_claim` grün. Der Cron bekommt je Poll eine neue uuid7, der Selbsttreffer trifft den alten Poll nicht.
  - E-047: unberührt.
- **Bibliothek:** valkey-py wiederholt mit identischen Argumenten (Lambda über dieselben `args`). Darauf beruht die Idempotenz durch Selbsttreffer.
- **Mischbetrieb:**
  - claim_fire: keine Skriptänderung.
  - lock_acquire: neue SHA. 0.12.0-Worker behalten das alte Skript und den Fehler; Key, Werte und Rückgaben sind gleich, also kompatibel in beide Richtungen.
  - claim_unique: nur Python.
- **Wechselwirkungen:**
  - FS-07 bleibt nötig. Ein Timeout ohne Wiederholung lässt den Claim mit unbekanntem Ausgang stehen; der nächste Versuch nutzt eine *neue* `task_id` bzw. einen neuen Owner, und der Selbsttreffer hilft nicht.
  - FS-03 (siehe oben).
  - FS-31 nutzt dieselbe Retry-Konfiguration; Renew ist idempotent, kein Handlungsbedarf.
- **Restlücke:** Der Leader-Oneshot mit Rückfall-Claim (E-032, nur `source="cli"` ohne Opt-out, nach FS-14 nur noch Producer ohne Registry/`_MISSING`) bleibt bei verlorener Antwort ein Duplikat. Er wird mit `commit_fire(-1)` verworfen.

### Tests
- Der Integrationsnachweis (3 Fälle) wird grün.
- Zusätzlich Contract-Tests in `tests/contracts/coordination.py`, analog `tests/contracts/execution_guard.py:76`:
  - wiederholter `claim_fire` mit demselben Owner → True
  - wiederholter Mutex- bzw. Semaphor-`acquire` mit demselben Owner → Token
- Unit: `Client.schedule` mit `claim_unique` → eigene `task_id` publiziert; `schedule_many` dito.
- Unit: Leader-Cron mit `existing == task_id` → Publish.
- Unit: Leader-Oneshot mit `existing == task_id` → weiter Duplikat (E-011-Schutz).
- Integration: Semaphor voll inklusive eigenem Token, verlorene Antwort → Token.
- Leader-Wahl: Ein Standby mit eigenem Token im Key erwirbt (bewusst dokumentiert).

Dateien:
- `src/swarmq/backend/valkey/schedules.py`
- `src/swarmq/lua/lock_acquire.lua`
- `src/swarmq/client/schedule.py`
- `src/swarmq/scheduler/fire.py`
- `tests/support/inmemory/backend.py`
- `tests/contracts/coordination.py`

Aufwand S–M, Risiko niedrig bis mittel (Leader-Wahl-Nebenwirkung von `lock_acquire`).

---

## FS-07 (P2): Claim nach Timeout nicht freigegeben

### Ursache
`bounded_backend_call`/`_run` nutzen `asyncio.wait_for` (`connection.py:279-283`). valkey-py trennt beim Abbruch die Verbindung (`asyncio/connection.py:533-538`, `disconnect(nowait=True)`), ein schon gesendeter Befehl läuft auf dem Server trotzdem.

Betroffen sind drei Stellen:
- **Lock-Gate** (`gates/lock.py:259-283`, `:539-560`): Läuft das Zeitlimit innerhalb des Shields ab, wird nichts registriert. `defer_backend_unavailable` stellt zurück. Der Lock mit `owner_token` bleibt bis `hard_timeout+30` stehen, und die Kopie trifft auf ihn.
- **Leader** (`scheduler/fire.py:174-187`): `return` ohne `_release_own_claim`. Das Flag hält eine nie publizierte uuid7, der nächste Poll endet in `dup_detected` + Commit, bis `unique_for` abläuft.
- **Client:** `schedule`, `_schedule_deferred` und `schedule_many` claimen vor ihrem `try` (`client/schedule.py:229`, `:601`, `:474`). Der Rollback (`:304-307`, `:634-637`, `:544-546`) greift erst danach. `_batch_rollback_claims` (`:358-366`) gibt nur erfolgreiche Claims frei, nicht die mit Fehler.

### Optionen

**A – Nach Timeout bzw. Transportfehler owner-geprüft freigeben (empfohlen), wie 7e6abec (`execution_guard.py:145-150`).**
- **Lock-Gate:**
  - Im `is_backend_outage`-Zweig `LockToken(lock_name, owner_token, max_concurrency)` **im Hintergrund** freigeben.
  - Getrackt, z. B. über eine neue `GateDeps`-Funktion auf `_detached_releases`, ohne `await asyncio.shield`, oder mit eigenem Set plus `drain()` wie im Ausführungsschutz. Der Shutdown wartet vor `backend.close` darauf (`shutdown.py:124-129`).
  - `lock_release.lua` ist owner-geprüft, eine Freigabe ohne bestehenden Lock ist harmlos.
  - Nicht inline: Bei hängendem Valkey verzögerte das die Zurückstellung um ein weiteres `backend_op_timeout`. Der Nachweistest wartet 50 ms auf die Hintergrundfreigabe.
- **Leader:**
  - Im `except BackendConnectionError` nach `claim_unique` `await self._release_own_claim(schedule_name, dedup_hash, task_id)` aufrufen. Inline, begrenzt durch `op_timeout`, best effort.
  - **Nur für Crons** (`definition.cron is not None`, `task_id` frisch je Poll). Ein Oneshot hat eine stabile `task_id`: Hat ein früherer Poll das Flag gesetzt und danach `PublishIndeterminate` bekommen, gäbe die owner-geprüfte Freigabe genau dieses Flag frei. Das verletzt E-011 („nicht bei `PublishIndeterminate`“).
  - Der Nachweis nutzt einen Cron und wird damit grün.
- **Client:**
  - Claim in `try/except BackendConnectionError` (bzw. `Exception`), dann `_rollback_unique_claim` und erneut werfen.
  - `schedule_many`: Auch Einträge in `setnx_errors` freigeben (owner-geprüft).
  - Inline, denn der Client hat keine Hintergrund-Infrastruktur und keinen Drain.
  - Abbruch (`CancelledError`) während des Claims bleibt Restlücke (Flag bis TTL). Eine geshieldete Freigabe im Client wäre möglich, kostet aber eine Hintergrund-Task ohne Besitzer.

**B – Nur Selbstheilung über die TTL, dokumentiert.**
- Widerspricht der Invariante aus 7e6abec und lässt bei langem `hard_timeout` stundenlangen Stillstand bzw. Cron-Verlust zu. Abgelehnt.

**C – Lock-Gate: statt Freigabe die Kopie den Lock mit demselben Owner-Token übernehmen lassen** (Token im Header wie beim Schedfire-Claim):
- Braucht einen neuen Header und den Selbsttreffer aus FS-06 über Zustellungsgrenzen hinweg. Ein 0.12.0-Worker kennt den Header nicht.
- Mehr Aufwand ohne Gewinn gegenüber A.

### Folgefehler-Analyse (Option A)
- **Entscheidungen:**
  - E-022: Der Lock-Keeper ist nicht beteiligt (nichts registriert); `task_lock_lost`-Logik unberührt.
  - E-007: Die Lock-Wartezeit der Kopie beginnt wie heute.
  - E-011: Durch die Cron-Beschränkung bleiben `test_indeterminate_publish_keeps_the_claim` und `test_publish_failure_keeps_a_cli_unique_claim_of_the_producer` grün.
  - E-033: Die Client-Opt-out-Tests sind unberührt.
- **Mischbetrieb:** Nur Python, bestehende Skripte (`lock_release.lua`, `unique_release.lua`). Kompatibel.
- **Wechselwirkungen:**
  - FS-06 (unabhängig, beide nötig).
  - FS-31: Eine Inline-Freigabe beim Leader kostet unter `_fire_lock` bis zu einem weiteren `op_timeout` bei hängendem Valkey. Hinnehmbar, der Poll scheitert dann ohnehin.
- **Neue Fehlermodi:**
  - Eine Hintergrund-Freigabe läuft nach dem Shutdown-Drain weiter, wenn der Drain-Schritt sein Zeitlimit reißt. Sie scheitert dann am geschlossenen Backend; nur ein Log, die TTL heilt.
  - Die Freigabe trifft nie einen fremden Lock (Owner-Prüfung).

### Tests
- Beide Nachweise werden grün.
- Neu: Client-Nachweis (`claim_unique` setzt das Flag und wirft dann `BackendConnectionError` → Flag frei, Wiederholung publiziert), für `schedule`, `_schedule_deferred` und `schedule_many`.
- Lock-Gate: Shutdown wartet auf die Hintergrundfreigabe.
- Leader-Oneshot nach Timeout → keine Freigabe (E-011).

Dateien:
- `src/swarmq/worker/gates/lock.py` (und `GateDeps` in `worker/gates/__init__.py`)
- `src/swarmq/worker/pipeline.py` (Verdrahtung)
- `src/swarmq/scheduler/fire.py`
- `src/swarmq/client/schedule.py`

Aufwand S–M, Risiko niedrig.

---

## FS-14 (P2): `unique_for=None`-Opt-out bei delay/eta nicht gespeichert

### Ursache
- `client/schedule.py:208` löst `_resolve_unique` auf: explizites `None` ergibt `None`. `_schedule_deferred` (`:588-625`) schreibt `source="cli"`, `dedup_hash=None` und `unique_for` leer. Damit sind „bewusst aus“ und „nicht angegeben bzw. Producer ohne Registry“ nicht unterscheidbar.
- Der Leader (`fire.py:140-167`) fällt nach E-032 auf `task_info.unique_for` zurück, `claim_unique` findet ein fremdes Flag, und es folgt `dup_detected` + `commit_fire(-1)`. Der Oneshot ist ohne Ergebnis weg.

### Optionen

**A – Opt-out als `source="cli_unique"` mit `dedup_hash=None` persistieren (empfohlen).**
- Bedeutung: „Der Producer hat über die Dedup entschieden.“ Mit Hash heißt das geclaimt, ohne Hash bewusst aus.
- Client: `_schedule_deferred` bekommt aus `schedule()` die Information „explizites None“ (`unique_for is None`, nicht `_MISSING`) und schreibt dann `cli_unique` ohne Hash.
- Leader: `if definition.source == "cli_unique":` Header nur bei Hash setzen, **nie** auf die Registry zurückfallen.
- `unroutable.py:182` prüft schon `cli_unique and dedup_hash`, bleibt korrekt.
- Folgefehler-Analyse:
  - **Mischbetrieb:**
    - Der Wert ist im 0.12.0-`Literal` erlaubt, die Dekodierung klappt in beiden Richtungen.
    - Ein 0.12.0-Leader liest `cli_unique` ohne Hash, geht in den `else`-Zweig und fällt auf die Registry zurück. Der Fehler bleibt dort bestehen, aber kein Absturz.
    - Ein 0.12.0-Producer schreibt weiter `cli`, also bleibt der Fehler für dessen Aufrufe.
    - Rollback ist sicher.
    - Wirksam erst mit Leader ≥ 0.12.1. Rollout-Hinweis analog E-011 („wirkt erst mit Leader ≥ 0.12“).
  - **Wire-Format:** unverändert. Kein Golden-Update, Fingerabdruck unberührt (`reports.py:40-49` nur `code_fields`).
  - **Entscheidungen:**
    - E-033 wird eingelöst.
    - E-032 bleibt für `cli` ohne Opt-out unverändert. `test_old_definition_takes_both_unique_values_from_the_registry` nutzt einen statischen Cron.
    - Die offene Produktfrage (Rückfall für CLI-Definitionen) wird nicht berührt.
  - **Nachbarn:**
    - `cli/schedule_cmd.py:242` (Operator-CLI ohne `--unique-for` schreibt `cli`) bleibt unberührt.
    - Das Dashboard-Repo swarmv zeigt `source` an. Prüfen, ob es `cli_unique` mit vorhandenem Hash gleichsetzt (Kernänderungen nur nach Absprache).
    - Docstring von `ScheduleDefinition` (`definition.py:33-43`) anpassen.

**B – Neues Feld (z. B. `dedup_off: bool = False`) am Ende der Struct.**
- Explizit, und 0.12.0 ignoriert das unbekannte Feld (geprüft).
- msgspec kodiert ohne `omit_defaults` aber alle Felder. Jede neu geschriebene Definition ändert sich in den Bytes, die Golden-Fixture `values/schedule_definition.json` (`test_golden_fixtures.py:69-72`) muss neu erzeugt werden.
- Mehr Fläche, gleicher Mischbetriebs-Effekt. Gute Alternative, wenn Georg das Überladen von `cli_unique` nicht will.

**C – Sentinel `unique_for=0`.**
- Ein 0.12.0-Leader rechnet `0 * 1000` und schickt `SET … PX 0`. Valkey antwortet mit einem ResponseError (ungültige Ablaufzeit), keinem `BackendConnectionError`. `_fire_one` wirft bei jedem Poll erneut. **Mischbetrieb bricht.**
- Ein neuer `source`-Wert scheitert in 0.12.0 an der `Literal`-Validierung (Probe). Beides abgelehnt.

Wechselwirkung FS-06/FS-07: Nach dem Fix claimt der Leader für Opt-out-Oneshots nicht mehr. Die E-011-Einschränkung betrifft nur noch `cli`-Oneshots aus Producern ohne Registry.

Nicht im Umfang (MC-1, zweiter Teil): Ein vom Leader als Duplikat verworfener Oneshot schreibt kein Ergebnis (`get_result` läuft in den Timeout; Nachbarinvariante `_store_unroutable_result`). Kein Nachweis im Register, eventuell eigener Befund.

### Tests
- Nachweis grün.
- Neu:
  - `_MISSING` ohne Registry → `source="cli"` (Rückfall bleibt, E-032).
  - explizites `None` mit Registry ohne `unique_for` → `cli_unique` ohne Hash, ohne Claim
  - `unique_for=60` → unverändert `cli_unique` mit Hash
  - Leader mit `cli_unique` ohne Hash: kein `claim_unique`, keine Unique-Header
  - Unroutable-Oneshot `cli_unique` ohne Hash → keine Freigabe
- E-033-Tests bleiben unverändert grün.

Dateien:
- `src/swarmq/client/schedule.py`
- `src/swarmq/scheduler/fire.py`
- `src/swarmq/scheduler/definition.py` (Docstring)
- `website/reference/api.md`, `website/guides/scheduling-and-retries.md` (Rollout-Hinweis)

Aufwand S, Risiko niedrig.

---

## FS-15 (P3): `pubsub.subscribe()` ohne Frist in `get_result`

### Ursache
- `backend/valkey/results.py:178-183`: `await pubsub.subscribe(channel)` ohne Deadline.
- valkey 6.1.1 `PubSub.execute_command` (`client.py:891-901`) → `connect()` (`:903-916`, Pool `get_connection` mit `socket_connect_timeout=5`) → `_execute` mit `call_with_retry` (`:934-944`, 5 Wiederholungen, reconnect).
- Bei unerreichbarem bzw. hängendem Valkey sind das real 30 s oder mehr über `timeout`. Der Docstring `:155-161` sagt das Gegenteil zu.

### Optionen

**A – `subscribe` auf die Rest-Deadline begrenzen (empfohlen).**
- `asyncio.wait_for(pubsub.subscribe(channel), timeout=deadline - now)`. Bei `TimeoutError` oder einem anderen Fehler wird Pub/Sub verworfen und es geht mit dem GET- bzw. Polling-Pfad weiter, der schon an die Deadline gebunden ist.
- Den verworfenen PubSub **nicht blockierend** schließen, über eine Hintergrund-Task oder `connection.disconnect(nowait=True)`. `aclose()` (`client.py:841-856`) ruft `disconnect()` mit bis zu `socket_connect_timeout` (`connection.py:452-473`), und `unsubscribe` sendet einen Befehl samt Retry.
- Abbruch-Sicherheit der Bibliothek:
  - Der Pool gibt beim Abbruch in `get_connection` die Verbindung zurück (`connection.py:1070-1077`, `except BaseException: release`).
  - Wurde `SUBSCRIBE` schon gesendet, ist `channels` noch leer (Update erst nach dem Befehl, `client.py:1049-1055`). Darum muss die Verbindung geschlossen und nicht wiederverwendet werden; `aclose()` tut das.
- Zusätzlich den Teardown im normalen `finally` (`_close_pubsub`, `:149-155`, `:198-200`) begrenzen. Ein `unsubscribe` auf hängendem Valkey verlängert den Aufruf sonst ebenfalls über `timeout` (gleiche Docstring-Zusage, im Nachweis nicht geprüft).

**B – Ganze Funktion in `asyncio.timeout(timeout)` legen.**
- Einfach, aber der `finally`-Teardown liefe dann unter Abbruch und könnte wieder hängen.
- `None` statt Ergebnis bei knappem Treffer am Fristende. Gleiches Ergebnis wie heute, aber weniger kontrolliert. Nicht empfohlen.

Variante zu A: die Frist auf `min(Rest, _PUBSUB_WINDOW_S)` legen, damit ein langsames `subscribe` nicht die ganze Zeit vor dem ersten GET verbraucht. Beides erfüllt den Nachweis.

### Folgefehler-Analyse
- **Entscheidungen:** E-030 (Fenster 1 s, 5-ms-Polling, `polling` wirkungslos) bleibt unverändert. `test_pubsub_miss_falls_back_to_polling` (`entscheidung("E-030")`) ist unberührt.
- **Nachbarpfade mit gleichem Muster** (nicht im Befund, beim Fix mitprüfen):
  - `wait_for_workflow` (`results.py:413-420`, Deadline erst *nach* `subscribe`)
  - `watch_progress` (`:266`, endlos, also eher unkritisch)
- **Mischbetrieb:** nur Client, keine Wire-Änderung.
- **Neue Fehlermodi:** Hintergrund-Schließen ohne Besitzer (Task-Referenz halten, sonst GC-Warnung). Bei aktivem `-W error` (U3) unbedingt einen Test auf „Task was destroyed but it is pending“.

### Tests
- Nachweis grün.
- Neu:
  - `subscribe` scheitert schnell → Polling, Ergebnis wird gefunden
  - hängendes `unsubscribe` im Teardown → Rückkehr ≤ `timeout` + ε
  - `wait_for_workflow` mit hängendem `subscribe` (falls mit behoben)

Dateien: `src/swarmq/backend/valkey/results.py`. Aufwand S, Risiko niedrig.

---

## FS-31 (P3): Leader-Renew ohne `op_timeout`

### Ursache
- `scheduler/loop.py:388-394`: `lock_manager.renew(...)` ohne Zeitlimit. Mit `socket_timeout` 10 s × (1+5) plus Backoff dauert ein Fehler bis etwa 65 s.
- Der Rücktritt zählt nur Fehler (`:196-199`, `N = max(1, ttl//hb - 1)`, Default 2), also etwa 150 s gegenüber 30 s Lock-TTL.
- Ebenfalls ohne Grenze: `acquire` (`:438-446`), `_renew_after_takeover` (`:493-498`) und `take_handover_marker`.

### Optionen

**A – Nur den Renew mit `backend_op_timeout` begrenzen** (`asyncio.wait_for`, `TimeoutError` zählt als Fehler).
- Erfüllt den Nachweis: Der erste Tick beginnt bei t=0, Fehler bei 5 und 20 s, Rücktritt bei 20 s.
- Rechnung ab dem letzten Erfolg t0 (Ticks im Abstand `hb + Dauer`): Fehler bei t0+15 und t0+30. Der Rücktritt fällt also mit Defaults (TTL 30, hb 10, `op_timeout` 5) **genau auf den TTL-Ablauf**.
- Allgemein gilt die Bedingung `(ttl/hb − 1) · timeout < hb`. Die E-048-Zusage „einen Heartbeat vorher“ hält A allein nicht ein.

**B – Zeitbasierte Grenze zusätzlich zur Fehlerzahl (empfohlen, zusammen mit A).**
- Startzeit des letzten erfolgreichen Renews bzw. Acquires mit `loop.time()` merken. Daraus ergibt sich `stepdown_at = start + ttl − hb`.
- Jeden Renew auf `min(backend_op_timeout, stepdown_at − now)` begrenzen. Ist `stepdown_at` erreicht, sofort zurücktreten, unabhängig vom Zähler.
- Die Fehlerzahl-Regel bleibt, damit bleiben `test_renew_exception_steps_down_after_threshold` und `test_renew_failure_limit_derived_from_ttl_and_heartbeat` (E-048) grün. Die Ticks dort folgen ohne Wartezeit, die Zeitgrenze greift nicht.
- Als Start den Zeitpunkt *vor* dem Aufruf nehmen. Valkey verlängert frühestens dann, das ist konservativ.

**C – Election-Tick insgesamt begrenzen** (auch `acquire`/`take_handover_marker`).
- Sinnvoll als Ergänzung.
- `acquire` mit Timeout kann den Leader-Lock mit eigenem Token verwaist zurücklassen (FS-07-Muster). Mit dem `lock_acquire`-Selbsttreffer aus FS-06 erwirbt der nächste Tick ihn sofort, ohne ihn erst nach der TTL (30 s, harmlos).

### Folgefehler-Analyse
- **Entscheidungen:**
  - E-048: B setzt die Zeitgrenze der Regel durch und ergänzt einen zweiten Auslöser (Zeit). Die Zählregel bleibt. Im Register festhalten (Ergänzung, keine Umkehr).
  - E-047: unverändert, der Schaden war dort ohnehin akzeptiert.
  - E-013 (Abkühlzeit nach Publish-Fehlern): unberührt; der Renew-Rücktritt setzt keine Abkühlzeit.
- **Bibliothek:**
  - Abbruch mitten in `lock_renew.lua`: valkey-py trennt die Verbindung (`connection.py:533-538`). Der Renew kann serverseitig trotzdem gelungen sein.
  - Dann tritt der Leader zurück, obwohl er den Lock hält. Ohne den FS-06-Selbsttreffer bewirbt er sich nach TTL-Ablauf neu; mit ihm sofort. Kein Split-Brain, weil der Key dann seiner ist.
- **Mischbetrieb:** nur Python, keine Keys oder Skripte.
- **Wechselwirkungen:** FS-06 (`lock_acquire`-Selbsttreffer verkürzt die Rückkehr nach Fehlalarm). FS-07 (Inline-Freigabe im Fire-Pfad) betrifft den Poll-Loop, nicht die Wahl.
- **Neue Fehlermodi:** Ein Valkey, das langsam, aber unter `op_timeout` antwortet, löst keinen Rücktritt aus. Eine Folge langsamer Erfolge setzt die Zeit zurück. Kein Flattern, solange `op_timeout < ttl − hb`. Sonst greift `min(...)`: Bei `backend_op_timeout ≥ ttl−hb` begrenzt die Zeitgrenze.

### Tests
- Nachweis grün.
- Neu (virtuelle Uhr wie im Nachweis):
  - Renew-Erfolg bei t0, danach hängende Renews → Rücktritt vor t0+ttl−hb+ε
  - Langsame, aber erfolgreiche Renews (4 s) → bleibt Leader
  - Renew-Timeout zählt als Fehler (Log `scheduler_renew_error`)
- E-048-Tests unverändert.

Dateien: `src/swarmq/scheduler/loop.py`. Aufwand S, Risiko niedrig.

---

## Wechselwirkungen und Reihenfolge

1. **FS-06 vor FS-03:** Der `claim_fire`-Selbsttreffer ist klein und schließt den Fall der frischen Zustellung, den FS-03/A bewusst nicht anfasst.
2. **FS-03 (Teil 2) und FS-04** ändern `_process_message` rund um Semaphor und `finally`: im selben Strang, FS-03/K-5 zuerst (berührt den `finally` nicht).
3. **FS-07 nach FS-06:** Beide ändern `client/schedule.py` (`_claim_unique_flag`, `_batch_claim_flags`) und `fire.py` an derselben Stelle. Gemeinsam umsetzen, beide mit derselben Regel „nur frische `task_id`“ (E-011).
4. **FS-14** ändert ebenfalls `fire.py` (else-Zweig) und `_schedule_deferred`. Nach FS-06/07 oder zusammen damit.
5. **FS-31** unabhängig. Profitiert vom `lock_acquire`-Selbsttreffer (FS-06).
6. **FS-15** unabhängig.
7. Release-Wellen laut Kontext: FS-03 gehört zu P1 (0.12.1). FS-06 ist für FS-03 nicht zwingend, schließt aber eine zweite Verlustquelle desselben Gates; ihn mit in 0.12.1 zu nehmen, ist erwägenswert (S, rein additiv).
8. **Mischbetrieb allgemein:** Für 0.12.1 eine 0.12.0-Basis in `tests/compat` anlegen. Ohne sie ist kein Fix dieser Gruppe automatisch neben 0.12.0 geprüft.

## Befunde, deren Fix eine Entscheidung oder offene Produktfrage berührt

- **FS-03:**
  - Setzt E-046 um.
  - Ändert die 0.9.0-Zusage (`sched_fire_claim.lua:9-11`, d93d3ab) von „Broker-Redelivery bleibt Duplikat“ zu „wird zurückgestellt“, wie E-046 verlangt. Betroffene Tests sind keine `entscheidung`-Tests (nur Docstrings).
  - Offen bzw. Doku: Auf klassischen Queues wirkt Option A nicht (keine Broker-Zähler). Ob das hingenommen oder per Vertragsänderung (`Delivery.redelivered`) geschlossen wird, ist eine Produktentscheidung.
  - Option C bräuchte eine neue Entscheidung (Parallelität nur über den Ausführungsschutz).
- **FS-06/FS-07 (Leader-Oneshot):** E-011 („nicht bei `PublishIndeterminate`“) zwingt zur Einschränkung auf frische `task_id`s. Ob Oneshot-Fires mit stabiler `task_id` ebenfalls gegen verlorene Antworten und Timeouts geschützt werden sollen, braucht eine Klärung, was bei Oneshots „genau dieses Fire“ heißt. Bis dahin bleibt die Restlücke dokumentiert.
- **FS-06 (`lock_acquire`):** Ändert nebenbei die Leader-Wiederwahl nach einem E-048-Rücktritt (sofort, wenn der Key noch den eigenen Token trägt). Kein Konflikt mit E-047/E-048, aber im Register erwähnen.
- **FS-14:** Berührt E-032/E-033. Der Fix löst E-033 ein, ohne den E-032-Rückfall anzufassen. Die offene Produktfrage (Rückfall für CLI-Definitionen) bleibt offen. Wahl zwischen Option A (`cli_unique` ohne Hash) und B (neues Feld) ist eine kleine Formatentscheidung. Wirkt erst mit Leader ≥ 0.12.1.
- **FS-15:** E-030 bleibt unberührt. Die offene Polling-Frage (Pub/Sub nur 1 s) ist nicht Teil des Fixes.
- **FS-31:** Ergänzt E-048 um einen zeitbasierten Auslöser, der die Regel erst einhält. Im Register als Präzisierung nachtragen; keine Umkehr.
