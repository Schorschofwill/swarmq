# Fluss-Analyse: Zusammenspiel der empfohlenen Fixes (Fehlersuche Runde 2)

Stand: Worktree `swarmq-fehlersuche` @ 906afac (v0.12.0 + Nachweise), Register @ 45c6336. Nur gelesen.
Methodik: spec-flow-analyzer (Abläufe → Lücken → Fragen → nächste Schritte). Grundlage sind die Empfehlungen aus r-broker, r-scheduler, r-worker, r-cpu und r-learnings. Die Einzelfixes bewerte ich nicht neu. Gesucht sind Zustände, die erst durch die Kombination entstehen.

Im Code gegengeprüft: `worker/pipeline.py:568-715`, `worker/gates/__init__.py:150-315`, `worker/retry.py:95-340`, `worker/guarded_broker.py`, `worker/shutdown.py:83-300, 370-378`, `worker/recycling.py:77-114`, `shutdown_budget.py:45-63`, `worker/execution.py:183-205`, `worker/compute/executor.py:199-215`, `broker/rabbitmq/delivery.py:64-80, 156-213, 270-300`, `broker/rabbitmq/consumer_watch.py:100-150`, `worker/gates/schedfire.py:100-200`, `lua/lock_acquire.lua`, `workflow_orchestration.py:265-290`, `process_manager/entry.py:160-180`.

---

## Kurzfassung der wichtigsten Befunde

1. **Widerspruch FS-02b ↔ FS-30.** r-broker (b1) setzt die Publish-Endfrist im Recycling auf das tatsächliche Drain-Ende. r-cpu will im Recycling-Drain *keine* Endfrist und setzt sie nach dem Drain-Ende neu aus `shutdown_budget()`. Die Varianten führen zu verschiedenem Verhalten abgebrochener Handler: b1 bedeutet Neuzustellung ohne Retry-Verbrauch, die r-cpu-Variante Retry- bzw. DLQ-Kopie. Die r-cpu-Variante bricht außerdem die Vorbedingung des FS-02b-Nachweises (`_publish_deadline_at is not None`). Sie öffnet auch das Fenster aus Punkt 2 wieder. **Empfehlung: b1 („Endfrist = tatsächliches Drain-Ende“) als einzige Regel. FS-30 setzt nur den Anker beim ersten SIGTERM neu.**
2. **Neuer, kritischer Pfad: Publish in einer vom Drain abgebrochenen Handler-Task.** E-001 schluckt den `CancelledError` (`execution.py:188`, `compute/executor.py:199`), ruft aber nie `uncancel()` auf. Danach gilt in der Handler-Task dauerhaft `cancelling() > 0`. `_publish_attempts` (`delivery.py:288`) und `settle()` (`delivery.py:77`) halten deshalb jede **interne** aio-pika-`CancelledError` für einen eigenen Abbruch und werfen sie weiter. `retry.py` fängt nur `Exception`. Das `finally` läuft dann mit dem Default `Terminal.release_in_finally()`: Completed-Marker geschrieben, Unique freigegeben, kein Broker-Verb. Die Neuzustellung wird als „completed“ bestätigt, **der Fire ist weg.** Heute verdeckt FS-02a das (Publish nach Fristablauf). Nach a1 + b1 ist es praktisch unerreichbar. Jede Variante, die abgebrochenen Handlern wieder Publish-Zeit gibt (r-cpu zu FS-30, a2 zu FS-02), macht es wieder erreichbar. FS-08 Option A (Kind-Task, Unterscheidung über `cancelling()` der Eltern-Task) ist davon direkt betroffen.
3. **FS-30-SIGTERM-Umschaltung muss genau einmal ankern.** Unter dem Supervisor kommen zwei SIGTERM an. Das erste kommt über den Watcher (`entry.py:175-177`, ≤ 0,3 s nach dem Supervisor-Abbau), das zweite als Eskalation bei `sigterm_at`, etwa 25 s später. Würde jedes SIGTERM das Budget neu ansetzen, läge `finish_by` hinter dem SIGKILL des Supervisors. Außerdem ruft `request_recycle` selbst `_request_shutdown()` auf (`recycling.py:114`). Die „Event setzen, wenn Override gesetzt“-Variante aus r-cpu muss den Zustand prüfen (`DRAINING` **und** Override), sonst bricht das Recycling sich selbst ab.
4. **FS-09 (abkoppeln statt abbrechen) erzeugt einen neuen Zustand „späte Neuanmeldung nach `cancel_consume`“.** `cancel_all` ruft `stop()` und leert danach `_entries` (`consumer_watch.py:140-152`). Ein abgekoppelter Takt, dessen `basic.consume` danach noch durchkommt, meldet einen Consumer an, den niemand mehr kennt und niemand abmeldet. Während des Drains kommen dann neue Zustellungen herein, die `tasks_snapshot` nicht abwartet.
5. **FS-03 und Mischbetrieb.** FS-03 wirkt nur, wenn der **empfangende** Worker 0.12.1 ist. Gerade der Rolling Deploy erzeugt die Neuzustellungen (SIGTERM-Abbrüche, SIGKILL nach Grace), die FS-03 retten soll. Sie landen anfangs überwiegend auf 0.12.0-Workern. Abhilfe: Surge-Deploy (erst neue Worker starten, dann alte stoppen). Das gehört in den Rollout-Hinweis.
6. **FS-03 + nicht verlängerter In-Flight-Claim:** Bei `SWARMQ_EXECUTION_GUARD=false` wird ein Cron-Task ohne `hard_timeout`, der länger als `scheduler_inflight_ttl` (300 s) läuft und dabei seinen Kanal verliert, nach Ablauf des Claims **parallel** ausgeführt. Heute wird er bestätigt und geht verloren. Mit Schutz (Default) fängt `guard_claim.lua` das ab. E-046 verlangt beides (kein Verlust und kein Parallellauf). Die Kombination ohne Schutz ist nicht geregelt.
7. **Reihenfolge:** P1 als zwei MRs (Publish-Endfrist; Pipeline-finally + Schedfire). FS-06/claim_fire (nur Python) sollte nach P1 vorgezogen werden. Vor P1 muss die compat-Basis 0.12.0 existieren, sonst ist „rolling-deploy-sicher“ für keinen Fix belegt (heute nur `constraints-0.11.*.txt`).

---

## 1. Abläufe

### Ablauf 1: Lebenszyklus einer Nachricht (mit allen Fixes)

```mermaid
flowchart TD
  P[Publish Client/Leader/Worker] -->|FS-02a: Frist vorbei → BrokerConnectionError, nichts gesendet| PX[Aufrufer: Rollback/redeliver]
  P -->|FS-08: Abriss in Kanal-Sperre → neuer Kanal / BrokerConnectionError| P
  P --> Z[Zustellung]
  Z --> SF{Schedfire-Gate}
  SF -->|Completed| ACK1[ack, never_release]
  SF -->|Claim fremd + Broker-Zähler oder Defer-Header (FS-03)| DEF[retry_later + Defer-Header → Redelivered]
  SF -->|Claim fremd, frisch (E-047)| ACK2[ack Duplikat]
  SF -->|Claim gewonnen / FS-06 Selbsttreffer| CG{Cancel-Gate}
  CG -->|per-Task-Flag: FS-12 fail_workflow → store → FS-13 release(ctx) → ack| T1[Terminal already_released]
  CG --> DC{DeliveryCount} --> SEM[Semaphor: FS-03 K-5 Freigabe bei Abbruch]
  SEM --> TRY[try: Shutdown-Check → Lookup → Guard → Expiry FS-17 → RateLimit → Lock FS-06/07]
  TRY --> START[Unique-Start-Release + FS-13-Merker]
  START --> EX[Ausführung: FS-05 Soft-Timeout-Hülle; E-001 Drain-Abbruch → Fehlerergebnis]
  EX -->|Erfolg| TAIL[Ergebnis-Tail: store → advance → ack]
  EX -->|Fehler| RT{Retry-Budget FS-19}
  RT -->|Retry| RL[retry_later]
  RT -->|erschöpft| DLQ[fail_workflow → store → DLQ-Kopie FS-10 ohne Zähler → ack → release]
  RL -->|BrokerConnectionError (FS-02a/FS-08)| RIP[redeliver_in_place: Claim frei → nack]
  DLQ -->|BrokerConnectionError| RIP
  TAIL --> FIN[finally FS-04-Kaskade: guard.settle → finalize → DI → Locks → Semaphor]
  RIP --> FIN
  DLQ --> FIN
  FIN --> REPLAY[DLQ-Replay: UI/Shovel nur für 0.12.1-Kopien; dlq retry immer]
```

Endzustände: ack (Erfolg/Duplikat/Cancel), DLQ, Neuzustellung (nack/Kopie), verloren (nur noch in den Restlücken unter „Lücken“).

### Ablauf 2: Cron-/delay-Fire

Leader-Poll → Unique-Claim (FS-06 Selbsttreffer nur bei frischer `task_id`, FS-07 Freigabe nach Timeout nur bei Crons, FS-14 `cli_unique` ohne Hash = kein Claim) → Publish (FS-02a/FS-08: „nicht gesendet“ heißt `_release_own_claim`, Indeterminate heißt Claim bleibt, E-011) → Commit. Danach Zustellung → Schedfire-Gate (FS-03/FS-06) → Lauf → `finalize_schedfire` (Completed bzw. In-Flight löschen). Daneben läuft die Wahl: Renew mit Zeitgrenze (FS-31), Acquire-Selbsttreffer (FS-06), Rücktritt (E-013/E-048).

### Ablauf 3: Worker-Prozess

Start → RUNNING → (a) SIGTERM: Budget ab `started_at`, Endfrist = `drain_until`, Drain, Abbruch (E-001), Aufräumschritte im Budget. (b) Recycling: Override (FS-30: `max(hard_timeout)+30` bzw. `consumer_timeout_s`), Endfrist = Drain-Ende (FS-02b), bei SIGTERM Umschaltung auf (a) ab erstem SIGTERM (FS-30), danach Aufräumen. (c) SIGKILL: kein `finally`, Erholung über Claim-TTL (FS-03), Lock-TTL, Guard-Orphans.

### Ablauf 4: Workflow

Schritt läuft → advance (publish-then-mark). Cancel per `cancel(task_id)` vor dem Start → FS-12 `failed`. Während des Laufs → `failed` (bestehend). Expiry → `failed` + DLQ (FS-17). Retries erschöpft → `failed` vor DLQ. Workflow-Cancel → `cancelled` (FS-32, außen vor). Drain-Abbruch bei `max_retries=0` → `failed`, dann (neu mit FS-02a) Neuzustellung statt DLQ.

---

## 2. Kombinierte Zustandsübergänge (Fokus 1)

### 2.1 FS-02a + E-001: Drain-Abbruch

Ablauf nach a1 (SIGTERM, Handler läuft über `drain_until`):
1. Bei `drain_until` ruft `_drain_in_flight` `task.cancel()` auf (`shutdown.py:116-118`). E-001 bildet das als Fehlerergebnis ab („hard timeout after …s“ bzw. „compute child cancelled during worker drain“).
2. Retry übrig: `retry_later` → a1 → `BrokerConnectionError` → `redeliver_in_place` → `GuardedBroker.redeliver` gibt Running frei und macht `nack`. Neuer Lauf **ohne** Retry-Verbrauch. Für Schedfire wird der Claim vorher freigegeben, die Neuzustellung gewinnt den Claim.
3. Retries erschöpft (`max_retries=0`): `dead_letter` markiert den Workflow **zuerst** als `failed` (`retry.py:271`) und speichert dann das Fehlerergebnis. Erst danach kommt der DLQ-Publish, der an a1 scheitert → `redeliver_in_place`. Der Schritt läuft erneut. Gelingt er, überschreibt er das Fehlerergebnis mit Erfolg. `advance` endet stumm, weil der Workflow terminal ist (`workflow_orchestration.py:278-279`).
   - **Neuer sichtbarer Zustand:** Workflow `failed`, Schrittergebnis `success`. Das gab es in 0.12.0 auch (DLQ-Kopie ging trotz Frist raus, dazu Requeue), dort aber zusätzlich mit einem DLQ-Eintrag. Mit a1 fehlt dieser Eintrag. Dem Operator bleibt kein Artefakt, das den Widerspruch erklärt.
4. FIRE_AND_FORGET: vor der Ausführung bestätigt (`acked_before_execute`). Den Fehlerpfad eines abgebrochenen FAF-Tasks nach a1 habe ich nicht nachverfolgt. Zu prüfen ist, ob dort ein Publish versucht wird. Falls ja, entfällt der Lauf ersatzlos, was für FAF zulässig ist.

Bewertung: a1 ist richtig (E-042). E-001 bleibt formal eingehalten. Der E-001-Folgentext („verbraucht einen Retry / DLQ“) stimmt aber nicht mehr. Das haben r-broker und r-learnings schon gemeldet. Neu ist hier der Workflow-Widerspruch aus Schritt 3.

### 2.2 Abgebrochene Handler-Task und `cancelling()` (E-001 + FS-08 + FS-02b-Variante)

- Belegt: In `worker/` gibt es keinen `uncancel()`-Aufruf (`grep`). `delivery.py:77` (`settle`) und `:288` (`_publish_attempts`) werfen eine `CancelledError` weiter, wenn `current_task().cancelling() > 0`.
- In einer per Drain abgebrochenen Handler-Task ist das nach E-001 dauerhaft so. Jede aio-pika-interne `CancelledError` (Kanalverlust: `drain_future` abgebrochen) wird dann zu einem durchgereichten `CancelledError`. `retry.py:157` und `:323` fangen nur `Exception`. Das `finally` sieht das Default-Outcome `Terminal.release_in_finally()`: Completed-Marker geschrieben (`finalize_schedfire(terminal)`), Unique-Flag freigegeben, die Nachricht bleibt unbestätigt. Nach dem Verbindungsschluss stellt der Broker sie neu zu, das Schedfire-Gate bestätigt sie als `completed`, und der Fire ist verloren. Das ist genau das Muster aus dem Learning `schedfire-retry-dedup-collision-silent-drop`.
- Erreichbarkeit je Variante:

| Variante | Publish nach Drain-Abbruch? | Fenster offen? |
|---|---|---|
| 0.12.0 | ja (Frist 0, FS-02a) | ja, wird aber durch `asyncio.timeout(0)` meist zu Indeterminate |
| a1 + b1 (empfohlen) | nein, `BrokerConnectionError` vor jedem `await` | praktisch zu; nur `nack` in `settle()` bleibt |
| a2 (Reserve für Fehlerpfade) | ja | **offen** |
| r-cpu zu FS-30 (neue Endfrist nach Drain-Ende) | ja, und parallel zu `broker_close` | **offen** |
| FS-08 Option A | Kind-Task; Eltern-`cancelling()` > 0 | **falsch klassifiziert**, wenn „eigener Abbruch“ über die Eltern-Task bestimmt wird |

- Folge für den Plan: FS-08 darf „eigener Abbruch“ nicht über `cancelling() > 0` bestimmen. Es muss den Zählerstand **beim Eintritt** in `_publish_with_retry` merken und nur einen Anstieg werten. Alternativ ruft der E-001-Pfad nach dem Schlucken `uncancel()` auf. Dann ist aber zu prüfen, ob `_drain_in_flight` den zweiten `asyncio.wait(pending, 1 s)` korrekt beendet. `uncancel` ändert keine Entscheidung, ist aber eine Verhaltensänderung außerhalb der 23 Befunde.

### 2.3 FS-02b + FS-30 + SIGTERM im Recycling + E-018

- Widerspruch zwischen r-broker und r-cpu: siehe Kurzfassung Punkt 1. Argumente für b1:
  1. Der FS-02b-Nachweis setzt `deadline is not None` voraus (`test_befund_fs02_recycling_publish_deadline.py:79-80`).
  2. Die Regel stimmt dann mit dem SIGTERM-Pfad überein: Endfrist = Drain-Ende, abgebrochene Handler werden nicht gesendet, die Neuzustellung ist sauber.
  3. Es entspricht E-038 sogar wörtlicher („ein laufender Task verliert durch das Recycling keinen Retry“): Der beim Fristablauf abgebrochene Recycling-Handler wird neu zugestellt und nicht mit Retry-Verbrauch kopiert.
  4. Es gibt kein Rennen zwischen Fehlerpfad-Publishes und `broker_close`. Nach dem Abbruch wartet `_drain_in_flight` nur `min(1 s, step)`.
- Was r-cpu zu Recht verlangt, gilt trotzdem: `_shutdown_end` nach dem Drain-Ende neu ansetzen (`drain_end + (finish_by − drain_until)`), sonst bekommt jeder Aufräumschritt 1 s.
- **SIGTERM im Recycling-Drain:** Neuer Anker = Zeitpunkt des **ersten** SIGTERM. Die Endfrist wird `min(aktuelle Frist, sigterm + drain_until)` und darf nie später werden. `_set_publish_deadline` zieht laufende Publishes nach (`_active_publish_timeouts`).
  - Zweites SIGTERM (Supervisor-Eskalation bei `sigterm_at`): darf **nicht** neu ankern.
  - `request_recycle` ruft `_request_shutdown()` im Zustand `RUNNING` auf. Das darf das Unterbrechungs-Event nicht setzen.
  - Der Watcher in `entry.py` hört nach dem Senden des SIGTERM auf zu beaten (`return` in `_watch_shutdown`). Im Abbau ist das Bestand, für den Fix aber wichtig: Nach dem SIGTERM gibt es keine Lebenszeichen mehr. Die Frist darf also nicht auf Staleness bauen.
- **Recycling + eingebetteter Leader:** Mit FS-30 hält ein recycelnder Prozess den Leader-Lock bis zu `consumer_timeout_s` (30 min). Mit b1 feuert er normal weiter. Ohne b1 (wenn FS-30 vor FS-02b käme) bekäme jeder Fire nach 23 s die Frist 0, also Indeterminate und einen stehenbleibenden Unique-Claim (E-011), 30 min lang. **FS-30 darf nie ohne FS-02b ausgeliefert werden** (r-cpu sagt das ebenfalls). In den Wellen ist das erfüllt (P1 vor P2).
- **Recycling-Drain bis `consumer_timeout_s`:** Erreicht ein Handler das Broker-Consumer-Timeout, schließt RabbitMQ den **gemeinsamen** Consume-Kanal (E-019). Alle anderen noch laufenden Handler dieses Workers verlieren ihre Zustellung. Der Guard setzt bei gescheitertem Ack `done (extended)`. Das ist Bestand, wird aber mit FS-30 wahrscheinlicher, weil der Drain so lange läuft wie das Timeout. Der Wert `consumer_timeout_s` als Drain-Grenze garantiert also nicht „keinen Retry verloren“ (siehe Frage F3).

### 2.4 FS-03 (Defer-Header) + FS-10 (Header in `copy_headers`)

- Kein direkter Konflikt: `republish_headers` filtert die Broker-Zähler schon heute (`gates/__init__.py:171`). FS-10 zieht die Liste nach `headers.py` und filtert zusätzlich in `copy_headers`. FS-03 erkennt die Neuzustellung am **Original** (`broker_redelivered(delivery.headers)`), nicht an der Kopie.
- Zwei Bedingungen für den Plan:
  1. Der neue Header (`x-swarmq-schedfire-defer-count`) darf weder in `_NOT_COPIED` noch im FS-10-Filter landen. Sonst verliert eine Backend-Ausfall-Kopie der zurückgestellten Kopie die Markierung, kommt ohne Zähler zurück und wird bei noch fremdem Claim **bestätigt**.
  2. Der Backoff-Zähler sollte `prior_deferrals(headers, HEADER_SCHEDFIRE_DEFER)` nutzen. Es setzt bei `broker_redelivered` auf 0 zurück. Die erste Zurückstellung (ausgelöst gerade durch den Broker-Zähler) beginnt dann korrekt bei 1.
- **DLQ-Kopie mit klebendem Defer-Header:** Geht eine zurückgestellte Schedfire-Kopie später in die DLQ (Guard-Orphans, Delivery-Limit), trägt sie den Header weiter. Beim Replay entsteht ein neuer Schedfire-Key (`K|<Identität>`, E-009), der Claim ist frei, die Kopie läuft. Harmlos, aber `dlq retry` sollte den Header wie `x-swarmq-retry-count` zurücksetzen, sonst wächst der Backoff weiter. Kleine Ergänzung.
- **FS-10 im Mischbetrieb für FS-03:** Eine 0.12.0-DLQ-Kopie mit Zählern, per UI wieder eingespielt, gilt in 0.12.1 als `broker_redelivered`. Bei fremdem Claim (gleicher Replay-Key wie eine laufende Kopie) wird sie zurückgestellt statt bestätigt. Korrekt.

### 2.5 FS-13 (Freigabe) + FS-12 (Workflow-Fail) + FS-17 (Expiry)

- Gemeinsame Reihenfolge im Cancel-Gate nach beiden Fixes: `fail_workflow` → `store_result` (Cancel) → `clear_cancel_flag` (PEXPIRE LT, E-020) → `finalize_schedfire(terminal)` → `maybe_release_unique(delivery, ctx, info)` (FS-13: im start-Modus frei, weil der Merker fehlt) → `ack`.
- Kombinationsfälle:
  - Fail-Markierung scheitert transient → `Redelivered`. FS-13 gibt nicht frei (nicht terminal), das Flag wird nicht verkürzt. Konsistent.
  - **Ack scheitert danach** (Kanalverlust): Der Workflow ist schon `failed`, das Flag schon frei. Die Neuzustellung durchläuft das Cancel-Gate erneut: Markierung als No-op, Ergebnis wird überschrieben, Freigabe owner-geprüft als No-op. Konsistent. **Grenzfall:** Kommt die Neuzustellung erst, nachdem das verkürzte Cancel-Flag (≤ 1 h) abgelaufen ist (Rückstand, Consumer-Timeout), **läuft der Schritt**. Das Unique-Flag ist dann womöglich schon von einem neuen `schedule()` belegt, und `advance` auf dem `failed`-Workflow endet stumm. Bestand von E-020, durch FS-12 aber sichtbarer (der Workflow ist jetzt `failed`).
  - Das Schedfire-Gate liegt **vor** dem Cancel-Gate. Ein abgebrochener Cron-Fire mit fremdem Claim wird mit FS-03 bis zum Claim-Ablauf zurückgestellt, bevor das Cancel-Gate ihn sieht. Nur eine Verzögerung.
  - **Expiry** (FS-17) liegt im `try` nach dem Guard: DLQ-Kopie über `dead_letter_copy`. Bei `SettleFailedAfterPublish` folgt `release_only`. FS-13 gibt jetzt auch im start-Modus frei, und `GuardedBroker.dead_letter` setzt `done (extended)`. Die Neuzustellung überspringt der Guard. Ohne Guard laufen zweite Kopie und zweite Freigabe owner-geprüft (E-043 akzeptiert). Konsistent.
  - Expiry mit FS-10: Die Kopie geht über `broker.dead_letter` → `dead_letter_generation` → `copy_headers`. Mit dem FS-10-Filter in `copy_headers` ist sie ohne Zähler, egal ob FS-17 vorher kommt oder nicht. Die Reihenfolgeabhängigkeit aus r-worker („FS-17 vor FS-10“) gilt nur, wenn FS-10 in `RetryHandler.dead_letter` filtern würde. Mit Option A (zentral) entfällt sie.
  - FS-13-Merker + E-001-Drain-Abbruch: Der Abbruch kommt nach dem Start, der Merker ist gesetzt, es gibt keine zweite Freigabe. FS-13-Merker + K-5 (Abbruch am Semaphor): Das liegt vor dem `try`, es gibt kein `finally` und keine Freigabe. Richtig, das Flag bleibt für die Neuzustellung.

### 2.6 FS-06 (Idempotenz) + FS-07 (Freigabe nach Timeout) + LockKeeper/E-022

- Verlorene Antwort mit Transport-Retry **innerhalb** von `op_timeout`: Der Selbsttreffer liefert `1`, der Lock wird registriert, der Keeper übernimmt. Korrekt.
- Timeout (`wait_for`) während des Retries: FS-07 gibt im Hintergrund owner-geprüft frei, der Keeper kennt den Lock nicht. **Restrennen:** valkey-py trennt beim Abbruch mit `disconnect(nowait=True)`. Liegt das `EVALSHA lock_acquire` schon im Socket-Puffer des Servers, kann die Freigabe über eine **andere** Verbindung vor dem Acquire ausgeführt werden. Dann bleibt ein verwaister Lock bis `hard_timeout+30`. Das ist nicht schlechter als 0.12.0, aber nicht „geschlossen“. Eine zweite, verzögerte Freigabe (z. B. nach `socket_timeout`) würde es schließen. Siehe Frage F6.
- Semaphor-Selbsttreffer (`ZSCORE`): Die Prüfung muss **vor** `ZCARD` laufen (sonst scheitert ein voller Satz inklusive eigenem Token) und **nach** `ZREMRANGEBYSCORE`. Der Score bleibt gleich (dasselbe `now_ms` im Retry). E-022 („PEXPIRE nur verlängernd“) muss der Selbsttreffer-Zweig ebenfalls einhalten: entweder kein PEXPIRE oder dieselbe `PTTL < ttl`-Prüfung.
- Leader-Wahl nutzt dasselbe Skript mit stabilem Token. FS-31 (Rücktritt per Zeitgrenze) + FS-06 (Selbsttreffer): Ein Renew-Timeout, das serverseitig gelang, führt zu Rücktritt und dann zu sofortiger Wiederwahl, also LEADER-/FAILOVER-Signalen im Takt eines Heartbeats („Flattern“). Kein Split-Brain, aber ein neues, beobachtbares Muster. Der Plan sollte festlegen, ob nach einem zeitbasierten Rücktritt eine Abkühlzeit gilt (wie E-013) oder ob das Flattern hingenommen wird (Frage F7).
- FS-07 im Leader inline unter `_fire_lock`: Bei hängendem Valkey kommt bis zu ein weiteres `op_timeout` hinzu. Gleichzeitig tritt die Wahl-Task über FS-31 nach Zeit zurück. Der laufende Fire endet danach mit `release`/`commit` auf einen Leader, der keiner mehr ist. Das ist schon heute so (E-047 akzeptiert).

### 2.7 FS-04 (finally-Umbau) + FS-03 (Semaphor-Teil)

- K-5-Option A (`try: acquire() except BaseException: release_claim; raise`) und die FS-04-Kaskade berühren verschiedene Blöcke. Wichtig ist nur, dass **beide** Varianten den Semaphor-Erwerb **außerhalb** des `try` mit dem `Terminal`-Default lassen. FS-03 Option B (Erwerb im `try`) und FS-04 Option B (`AsyncExitStack`, der den Erwerb in den geschützten Bereich zieht) laufen beide auf denselben Fehler hinaus: Ein Abbruch am Semaphor erreicht das `finally` mit `release_in_finally()` und schreibt den Completed-Marker. **FS-04 Option B ist deshalb nur zulässig, wenn der Stack erst nach dem erfolgreichen `acquire()` betreten wird.** r-worker nennt B „passend, falls FS-03 den Erwerb umbaut“. Das ist genau die verbotene Variante.
- Kaskade + Drain-Abbruch: Ein äußerer Abbruch in Schritt 1–3 lässt jetzt Lock-Freigabe und Semaphor laufen. Gleichzeitig räumt `_release_leftover_locks` die Registry. Das Doppel-Release-Rennen (Fehlalarm `task_lock_lost stage=release`) wird häufiger, weil die Kaskade jetzt öfter bis zur Freigabe kommt. Den Registry-Eintrag vor dem Release entnehmen (r-worker optional) wird damit **empfohlen**, nicht optional.
- Kaskade + 2.2: Die Kaskade läuft in einer Task mit `cancelling() > 0`. `_detached` nutzt `asyncio.shield`, ohne erneutes `cancel()` läuft das normal. Kommt ein zweites `cancel()` (Teardown im Fehlerpfad, `_teardown_on_exit`), bricht jede spätere Stufe ab. Der Semaphor-`release()` ist synchron und läuft trotzdem.

### 2.8 Weitere Paare

- **FS-02a + FS-03-Zurückstellung im Shutdown:** `retry_later` der Zurückstellung → a1 → `redeliver_in_place`, ohne eigenen Claim (`ctx.schedfire_claim = None`), also nur `nack`. Korrekt.
- **FS-03 + PublishIndeterminate der Zurückstellung (FS-08 noch nicht da):** r-scheduler sagt „Fällt `retry_later` aus, folgt `redeliver_in_place`“. Bei Indeterminate entstehen eventuell zwei Kopien. Beide tragen den Defer-Header. Die erste übernimmt den Claim nach Ablauf, die zweite sieht ihn fremd und wird zurückgestellt, danach bestätigt sie über den Completed-Marker. Die Marker machen das harmlos, aber es muss so festgeschrieben werden: Bei Indeterminate wird das Original **nicht** bestätigt.
- **FS-03-Zurückstellungsdauer gegen die Completed-Marker-TTL (E-010):** Die Zurückstellung endet spätestens mit dem Claim-Ablauf (`max(300, hard_timeout+30)`). Danach entscheidet der Completed-Marker. Dessen TTL ist `completed_marker_ttl_ms` (≥ `scheduler_dedup_ttl`, mit `max_age` ≥ `max_age+300`). Läuft der Marker vor dem letzten Takt einer Kopie ab (Backoff bis 60 s), würde die Kopie einen schon erledigten Fire erneut ausführen. Prüfen: Ist `completed_ttl ≥ inflight_ttl + backend_defer_max_delay` für alle Konfigurationen garantiert? `scheduler_dedup_ttl=None` heißt Default. Ich habe die Formel nicht vollständig nachgerechnet (Frage F5).
- **FS-09 + FS-30:** Beide ändern `_drain_in_flight`. FS-09 begrenzt `cancel_consume` (shield). FS-30 lässt den Drain zusätzlich auf das SIGTERM-Event warten. Zusammen mit Punkt 4 der Kurzfassung muss die Reihenfolge so sein: Erst FS-30, dann FS-09, und FS-09 braucht „Takt meldet nach `_stopping` nichts mehr an bzw. meldet sofort wieder ab“.
- **FS-05 + E-001:** Der Drain-Abbruch während eines Soft-Timeout-`wait_for` bleibt eine `CancelledError` (der Zähler in `Timeout.__aexit__` ist höher als beim Eintritt). Die FS-05-Hülle ändert daran nichts. Ein Testfall „Drain-Abbruch und Soft-Timeout im selben Loop-Durchlauf → Fehlerergebnis nach E-001, nicht `SoftTimeoutError`“ sichert die Kombination ab.
- **FS-16-Sweep + Drain-Abbruch im CPU-Modus:** Der Sweep ist synchron und verlängert den Abbruch nicht. `SlotPool.run` wartet beim Abbruch weiter bis `kill_grace_s` plus Tod, also länger als `_drain_in_flight`s `min(1 s, step)`. Bestand.
- **FS-12 + Delivery-Limit:** Die Rückstellschleife einer dauerhaft scheiternden Fail-Markierung liegt vor dem DeliveryCount-Gate (r-worker). Mit FS-03 kommt eine zweite solche Schleife vor dem Gate dazu (Schedfire-Zurückstellung), begrenzt durch die Claim-TTL. Beide umgehen `x-delivery-limit`, weil Kopien keine Zähler tragen. Zusammen nicht schlimmer als einzeln, aber im Betrieb nur über die Logs sichtbar (Stall-Log für FS-03 empfohlen).

---

## 3. Rolling Deploy über die Wellen (Fokus 2)

### Welle P1: 0.12.0 → 0.12.1 (FS-02, FS-03, FS-04)

| Rolle gemischt | Szenario | Ergebnis |
|---|---|---|
| Worker 0.12.0 wird per SIGTERM gestoppt | Drain-Abbruch, Retry/DLQ mit Frist 0 | wie heute: Kopie geht raus und es wird requeuet (Doppellauf). Der Deploy selbst löst FS-02a an jedem alten Worker **ein letztes Mal** aus. |
| Neuzustellung eines Schedfire vom gestoppten 0.12.0-Worker (Claim noch gehalten, z. B. nach SIGKILL am Ende der Grace) | empfängt 0.12.0 | Ack, Fire verloren (wie heute) |
| | empfängt 0.12.1 | Zurückstellung bis Claim-Ablauf, dann Lauf (E-046) |
| 0.12.1 stellt zurück, Kopie (ohne Zähler, mit Defer-Header) geht an 0.12.0 | | 0.12.0 bestätigt bei fremdem Claim, also Verlust. Nicht schlechter als 0.12.0, verschiebt den Verlust aber nur. |
| Rollback 0.12.1 → 0.12.0 mit Kopien in der Delay-Queue | | wie Zeile davor |
| Leader 0.12.0, Worker 0.12.1 | Fire-Publishes des Leaders im Shutdown | Leader-Seite unverändert (FS-02a fehlt dort). Wechselt der Leader auf 0.12.1, gilt FS-02a für Fires. |
| Producer 0.12.0 | Client-Publish nach Endfrist | betrifft nur eingebettete Producer im Worker-Prozess, nicht externe |

Empfehlungen P1:
- **Surge-Deploy** (neue Worker hochfahren, bevor alte stoppen). Sonst nehmen anfangs vor allem 0.12.0-Worker die Neuzustellungen an, also genau die Fälle, die FS-03 retten soll. Gehört in den Upgrade-Hinweis.
- Die Grace des Orchestrators (`terminationGracePeriodSeconds` o. ä.) ≥ `shutdown_timeout`, damit kein SIGKILL vor `finish_by` kommt. Sonst fallen Claim-Freigaben und Guard-Abschluss aus.
- compat-Basis 0.12.0 anlegen, mit Szenario „Schedfire-Neuzustellung zwischen den Versionen“.

### Welle P2/P3: 0.12.1 → 0.12.2/0.13 (alle übrigen)

| Fix | Mischfall | Ergebnis |
|---|---|---|
| FS-06 `lock_acquire.lua` | alte und neue SHA auf demselben Lock-Key | gleiche Keys, Werte und Rückgaben. Wechselseitiger Ausschluss bleibt; nur der neue Worker hat den Selbsttreffer. **Test fehlt:** Mutex und Semaphor gleichzeitig mit beiden Skriptfassungen. |
| FS-06 Leader-Wahl | 0.12.1-Leader und P2-Standby | P2-Standby bekommt nie den Selbsttreffer auf einen fremden Token. Unkritisch. |
| FS-07 Leader | nur P2-Leader gibt nach Timeout frei | Die Wirkung hängt davon ab, welche Version gerade führt. |
| FS-14 `cli_unique` ohne Hash | Producer P2 + Leader alt **oder** Producer alt + Leader P2 | Der Fehler bleibt in beiden Mischfällen. Er wirkt erst, wenn **Producer und Leader** neu sind. Producer sind oft andere Dienste (Phoenix). Der Rollout liegt also außerhalb der Wellensteuerung und gehört in den Hinweis. |
| FS-10 DLQ-Kopien | alter Bestand in der XQ | per UI/Shovel weiter nicht replaybar; `dlq retry` nutzen |
| FS-13/FS-12/FS-17 | je Zustellung alt oder neu | Flags bzw. Workflows hängen nur bei Zustellungen an alten Workern |
| FS-08 | Producer-seitig | weniger Indeterminate heißt mehr Unique-Rollback (E-041 nur für Indeterminate). Mit alten Producern gemischt unkritisch. |
| FS-30 + FS-02b | Supervisor und Worker gleiche Version | kein Mischfall |
| FS-03 (aus P1) + FS-06 `claim_fire` (P2) | 0.12.1-Worker ohne Selbsttreffer | Eine verlorene Antwort bei frischer Zustellung wird bestätigt (FS-06-Restfall), bis alle Worker auf P2 sind |

---

## 4. Lücken nach Schwere

### Kritisch

1. **Abgebrochene Handler-Task publiziert mit `cancelling() > 0`** (2.2). Fehlt: eine Regel, dass Fehlerpfade nach einem E-001-Abbruch entweder nicht publizieren (a1+b1) oder ihre Abbruch-Erkennung relativ zum Eintrittsstand führen. Ohne diese Regel schreibt das `finally` bei einem Kanalverlust den Completed-Marker auf eine unbestätigte Nachricht (stiller Fire-Verlust). Code-Muster: `Timeout.__aexit__` von asyncio arbeitet selbst mit dem Eintrittsstand (`_cancelling`). Dasselbe Muster gehört in `_publish_with_retry`/`settle`.
2. **Widerspruch FS-02b (b1) ↔ FS-30 (r-cpu, „keine Endfrist im Recycling, danach neu“)** (2.3). Ohne Festlegung bauen zwei MRs gegeneinander, und der Nachweis bricht.
3. **Mehrfaches Anker-Setzen bei SIGTERM im Recycling-Drain** (2.3). Die zweite SIGTERM-Welle des Supervisors bzw. der Selbstaufruf aus `request_recycle` verschiebt `finish_by` hinter das SIGKILL bzw. bricht das Recycling ab.

### Wichtig

4. **Späte Neuanmeldung nach `cancel_consume`** (FS-09 O3 + `cancel_all`).
5. **Workflow `failed`, Schritt danach erfolgreich, ohne DLQ-Artefakt** (FS-02a + E-001 + BZ-13-Reihenfolge, 2.1).
6. **FS-03 bei abgeschaltetem Ausführungsschutz:** Parallellauf nach Claim-Ablauf für Cron-Tasks ohne `hard_timeout` (Kurzfassung Punkt 6). Wohin gehört es: Doku-Grenze, `inflight_ttl` an `consumer_timeout_s` koppeln, oder Claim-Verlängerung?
7. **FS-04 Option B mit FS-03 ist die verbotene Variante** (2.7). Der Plan muss A (Kaskade) festschreiben oder B mit „Stack erst nach `acquire()`“.
8. **Doppel-Release-Rennen nach FS-04** wird häufiger (2.7). Den Registry-Pop vor dem Release zum Pflichtteil machen.
9. **compat-Basis 0.12.0 fehlt**. Die Aussage „rolling-deploy-sicher“ ist für keinen Fix automatisch belegt.
10. **Surge-Deploy/Grace-Hinweis für P1** fehlt im Rollout (Abschnitt 3).

### Gering

11. Defer-Header in DLQ-Kopien und `dlq retry` (2.4).
12. Restrennen FS-07 bei Freigabe über eine andere Verbindung (2.6).
13. Flattern der Leader-Wahl nach zeitbasiertem Rücktritt + Selbsttreffer (2.6).
14. Abgelaufenes Cancel-Flag vor der Neuzustellung eines FS-12-Schritts (2.5).
15. Completed-Marker-TTL gegen Zurückstellungsdauer (2.8, F5).

---

## 5. Fragen (nach Priorität)

**F1. Welche Regel gilt für die Publish-Endfrist im Recycling: b1 (Endfrist = tatsächliches Drain-Ende, beim ersten SIGTERM auf `min(…, sigterm+drain_until)` vorziehen) oder r-cpu (keine Endfrist im Drain, danach Reserve)?**
Es geht darum, ob abgebrochene Recycling-Handler neu zugestellt oder per Retry/DLQ kopiert werden, um den Nachweis FS-02b und um das Fenster aus 2.2.
Default: b1. `_shutdown_end` wird nach dem Drain-Ende neu angesetzt.

**F2. Soll die Abbruch-Erkennung in `_publish_with_retry`/`settle` relativ zum Eintrittsstand von `cancelling()` arbeiten, oder soll der E-001-Pfad nach dem Schlucken `uncancel()` aufrufen?**
Ohne eines von beiden darf kein künftiger Fix abgebrochenen Handlern Publish-Zeit geben, und FS-08 klassifiziert falsch.
Default: Eintrittsstand in FS-08 (ohnehin derselbe Code). Dazu kommt ein Regressionstest für den Fall, dass eine vom Drain abgebrochene Task bei einem Kanalverlust kein Terminal-`finally` bekommt. `uncancel` nur bei Bedarf und mit eigenem Befund.

**F3. Reicht `consumer_timeout_s` als Recycling-Grenze ohne `hard_timeout` (FS-30 A), obwohl das Broker-Consumer-Timeout dann den gemeinsamen Kanal schließt und andere laufende Handler mitnimmt?**
Wortlaut von E-038 und Speicher des alten Prozesses (bis 30 min).
Default: A wie empfohlen, Doku-Grenze, E-038-Präzisierung durch Georg bestätigen (r-cpu verlangt das ebenfalls).

**F4. Wie ist der Rollout für P1 vorzuschreiben (Surge, Orchestrator-Grace ≥ `shutdown_timeout`)?**
FS-03 wirkt sonst gerade im Deploy kaum.
Default: als Pflichthinweis in Changelog und Upgrade-Doku, ohne Codeänderung.

**F5. Ist garantiert, dass `completed_ttl ≥ inflight_ttl + backend_defer_max_delay` in jeder Konfiguration gilt?**
Sonst kann eine zurückgestellte Kopie einen erledigten Fire erneut ausführen.
Default: Beim Bau der FS-03-Zurückstellung prüfen und bei Bedarf die Zurückstellung auf `completed_ttl` deckeln (Log statt Lauf).

**F6. Soll FS-07 im Lock-Gate eine zweite, verzögerte Freigabe bekommen (gegen das Rennen „Freigabe vor dem Acquire“)?**
Ein seltener verwaister Lock bleibt bis `hard_timeout+30`.
Default: nein, als Restlücke dokumentieren.

**F7. Gilt nach einem zeitbasierten Leader-Rücktritt (FS-31) eine Abkühlzeit, damit der FS-06-Selbsttreffer nicht flattert?**
Default: keine Abkühlzeit, Flattern hinnehmen und in E-048 als Präzisierung nennen.

**F8. FS-03 ohne Ausführungsschutz: Doku-Grenze, Kopplung `inflight_ttl ≥ consumer_timeout_s` für Tasks ohne `hard_timeout`, oder Verlängerung des Claims?**
Default: Doku-Grenze. Der Schutz ist Default an.

**F9. Soll `dlq retry` den Defer-Header von FS-03 zurücksetzen?**
Default: ja, analog zum Retry-Zähler.

---

## 6. Fehlende Testszenarien (Fokus 3)

### Kombinationstests, Unit (< 100 ms, Events statt Sleeps)
1. **Drain-Abbruch + Kanalverlust im Retry-Publish** (F2): Handler per `task.cancel()` im Drain abgebrochen, `retry_later` wirft eine aio-pika-interne `CancelledError`. Erwartet: kein Completed-Marker, keine Unique-Freigabe, Outcome nicht terminal. Heute rot (das ist Absicht). Gehört zu FS-08.
2. **Drain-Abbruch bei `max_retries=0` im Workflow-Schritt:** `fail_workflow` gerufen, DLQ-Publish nicht gesendet (a1), Neuzustellung. Schreibt den Zustand aus 2.1 fest. Kein `entscheidung`-Marker ohne Registereintrag.
3. **Recycling: erstes SIGTERM ankert, zweites nicht.** Die Endfrist wird nie später. `request_recycle` setzt das Unterbrechungs-Event nicht.
4. **Recycling ohne SIGTERM:** Publish nach der normalen Frist gelingt (angepasster FS-02b-Nachweis). Nach dem Drain-Ende bekommt der Aufräumschritt das Budget ab dem Drain-Ende.
5. **K-5 + FS-04 im selben Lauf:** Abbruch am Semaphor (Claim frei, keine `finally`-Schritte), danach eine zweite Zustellung mit scheiterndem Finalizer (`concurrency=1`). Die zweite läuft durch.
6. **FS-04 + Shutdown-Registry:** Kaskade und `_release_leftover_locks` gleichzeitig. Kein `task_lock_lost`.
7. **FS-12 + FS-13 im start-Modus:** Reihenfolge `fail → store → clear → finalize → release → ack`. Ack scheitert, die Neuzustellung ist idempotent (genau ein `failed`, Freigabe als No-op).
8. **FS-17 + FS-10 + FS-13:** Expiry im start-Modus, `SettleFailedAfterPublish`. Kopie ohne Broker-Zähler, Freigabe genau einmal, Done-Marker gesetzt.
9. **FS-03-Defer-Header überlebt Backend-Ausfall-Kopie und FS-10-Filter** (r-scheduler Nr. 4 erweitert um `copy_headers`).
10. **FS-09: Takt bekommt die `consume`-Antwort nach `cancel_all`.** Der Tag wird sofort abgemeldet bzw. nicht übernommen.
11. **FS-05 + Drain:** Soft-Timeout und Drain-Abbruch im selben Durchlauf ergeben das E-001-Ergebnis.
12. **FS-06 Semaphor-Selbsttreffer bei vollem Satz** inklusive eigenem Token, mit PTTL unverändert (E-022). Contract-Test für Lua und In-Memory.

### Integration (Testports 5699/6399, nur über `itest.sh`)
13. Lock-Key mit **beiden** `lock_acquire`-Fassungen (alte SHA per `SCRIPT LOAD` des 0.12.0-Texts): Mutex und Semaphor schließen sich gegenseitig aus.
14. FS-03: Claim mit Owner „W1“ setzen, Neuzustellung mit Zähler. Zurückgestellt, nach Claim-Ablauf genau ein Lauf, Completed-Marker blockiert die zweite Kopie (r-scheduler 7/8, ergänzt um die Doppelkopie bei Indeterminate).
15. FS-10-Variante `route_via_dlx` (Broker-Verhalten festhalten).

### e2e/Chaos
16. **SIGKILL eines Workers mitten in einem Cron-Task** (E-046 `ohne_test`, H-2), mit skalierter `scheduler_inflight_ttl`. Genau ein Lauf.
17. **SIGTERM-Drain-Abbruch:** Langläufer > `shutdown_timeout`. Kein `publish_deadline_exceeded`, keine DLQ-Kopie, genau ein erneuter Lauf.
18. **Supervisor-Abbau während eines Recycling-Drains** (FS-30): Worker endet vor `sigkill_at`, Locks frei, Leader-Übergabe mit Marker (kein FAILOVER).
19. **Toxiproxy Valkey:** Antwort verloren bei `lock_acquire`/`claim_fire`/`claim_unique` innerhalb und außerhalb von `op_timeout`. Kein verwaister Lock, kein verlorener Cron (FS-06 + FS-07).
20. **Toxiproxy RabbitMQ:** Abriss bei laufendem Publish-Strom mehrerer Handler, einer davon im Drain-Abbruch (FS-08 + F2). Smoke ohne Timing-Annahmen.

### compat (Basis 0.12.0 neu anlegen)
21. Schedfire-Neuzustellung und Zurückstellungskopie zwischen 0.12.0 und 0.12.1 in beiden Richtungen (kein Fehler, Verlust nur bei 0.12.0 als Empfänger, so dokumentiert).
22. DLQ-Replay einer 0.12.1-Kopie an einen 0.12.0-Worker.
23. FS-14-Matrix Producer×Leader (alt/neu).
24. Rollback mit Kopien in der Delay-Queue.

### Loadsuite (1 h / 6 h)
25. Messgröße „Zurückstellungen je Minute“ (FS-03) und Publish-Latenz p99 mit Kind-Task (FS-08). Im 6-h-Lauf mindestens ein Recycling mit Langläufer (FS-30/FS-02b).

### Anzupassende Nachweis- und Bestandstests
- `tests/unit/worker/test_befund_fs02_recycling_publish_deadline.py`: Normalfrist aus `shutdown_budget` berechnen statt aus dem Broker lesen (nur unter b1 gültig, siehe F1).
- `tests/unit/broker/test_befund_fs09_consumer_watch_rpc_abort.py`: Fake-Signaturen um `timeout=` ergänzen (sonst falsch grün).
- `tests/unit/worker/test_befund_fs04_dishka_finalizer.py`: Ack-Vorbedingung bleibt, solange „Ack vor DI-Scope-Ende“ offen ist.
- `tests/unit/worker/gates/test_befund_fs03_schedfire_redelivery.py`: Fall 1 nutzt `x-delivery-count: 1`, wird mit Option A also grün. Einen Fall „Kopie mit Defer-Header ohne Zähler“ ergänzen.
- Ohne Marker, anpassen: `test_recycling.py:84`, `test_pool.py::test_cooperative_child_needs_only_sigterm`, `test_consumer_watch.py::TestHangingRpc::*`, `test_pool.py::test_handshake_timeout_is_slot_unavailable_not_child_death` (Rennen), Docstrings in `test_schedfire.py` und `test_schedfire_claim_handover.py`.
- Kein `entscheidung`-Test muss geändert werden. Neue Marker-Tests (E-043, E-045, E-046) nur zusammen mit der `tests:`-Liste im Register.

---

## 7. Reihenfolge und Bündelung (Fokus 4)

**Vorbedingungen (vor P1):** Register-Branch nach main. compat-Basis `constraints-0.12.0.txt` mit Fixture. Antwort auf F1 und F2.

**P1 = 0.12.1**
1. **MR P1-a „Publish-Endfrist“:** FS-02 a1 + b1. Eine Funktion „Drain-Ende“, die schon die Schnittstelle für FS-30 hat (Anker neu setzen, nur vorziehen). `_shutdown_end` nach dem Drain-Ende neu ansetzen, gehört hierher, weil b1 den Recycling-Drain sonst ohne Aufräumbudget lässt. E-001-Nachtrag.
2. **MR P1-b „Pipeline-finally + Schedfire“:** FS-03 Teil 2 (K-5) → FS-04 (Kaskade A, Registry-Pop vor Release) → FS-03 Teil 1 (Zurückstellung, Defer-Header, Stall-Log). Ein MR, weil alle drei `_process_message` und die `Terminal`-Default-Semantik betreffen.
3. **Optional in P1 (empfohlen):** FS-06, nur der `claim_fire`-Teil (Owner an Tokens anhängen, nur Python plus In-Memory). Schließt die zweite Verlustquelle desselben Gates ohne Lua-Änderung.

**P2/P3 (gesammelt)**
4. **MR „Broker-Phase“:** FS-08 + a3 + `publish_phase`, mit Eintrittsstand-Regel aus F2 und Test 1.
5. **MR „Recycling“:** FS-30 (A + SIGTERM-Anker einmalig) auf P1-a aufbauend. **Danach** FS-09 (P3), weil beide `_drain_in_flight` ändern und FS-09 die Neuanmeldungs-Sperre braucht.
6. **MR „Valkey-Idempotenz“:** FS-06 Rest (`lock_acquire.lua`, `claim_unique`) → FS-07 → FS-14 (dieselben Stellen in `client/schedule.py`/`fire.py`) → FS-31 (nutzt den Selbsttreffer; F7 klären).
7. **MR „Gates“:** FS-13 (Signatur `maybe_release_unique(delivery, ctx, info)` zuerst, berührt sieben Aufrufer) → FS-12 → FS-17 → FS-10 (zentral in `copy_headers`; damit entfällt die Reihenfolgefrage FS-17/FS-10). FS-19 und FS-28 können folgen (gleiche Dateien `retry.py`/`results.py`).
8. **MR „Timeouts“:** FS-05 + FS-20 (Doku).
9. **MR „Pool“:** FS-24 → FS-16 → FS-23.
10. **Kleinkram:** FS-15, FS-27.

Begründung der Reihenfolge:
- FS-30 nie vor FS-02b (Leader im Recycling, 2.3).
- FS-08 erst mit der Abbruch-Regel (2.2).
- FS-09 nach FS-30 (gleicher Code, neuer Zustand).
- FS-13 vor FS-12 und FS-17 (Signatur und Merker), FS-10 zuletzt im Gate-MR.
- FS-06 vor FS-07, FS-14 und FS-31 (gleiche Stellen und Selbsttreffer).

---

## 8. Belege, dass eine Entscheidung so nicht trägt (gemeldet, nicht unterdrückt)

- **E-001 (Folgentext)** trägt nach FS-02a nicht mehr. Drain-Abbruch heißt Neuzustellung statt Retry/DLQ. Zusätzlich gibt es den Workflow-Widerspruch aus 2.1. Außerdem hinterlässt E-001 eine Task mit `cancelling() > 0`. Für die weitere Verarbeitung in derselben Task trägt die Zusage „nie als `CancelledError` propagiert“ nur, solange keine Bibliotheksstelle `cancelling()` auswertet. Genau das tun `settle()` und `_publish_attempts` (2.2).
- **E-038 („verliert keinen Retry“)** ist mit einer endlichen Grenze (FS-30 A) nicht wörtlich zu halten, und auch `consumer_timeout_s` schützt andere Handler nicht vor dem Kanalschluss (2.3, F3).
- **E-046 (kein Verlust und kein Parallellauf)** lässt sich mit FS-03 Option A nur bei eingeschaltetem Ausführungsschutz für beide Hälften erfüllen (F8).
