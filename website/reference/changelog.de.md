# Changelog

Alle nennenswerten Änderungen an SwarmQ. Dieses Projekt folgt
[Keep a Changelog](https://keepachangelog.com/) und
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Hinzugefügt

- **`swarmq.MessageHeadersTooLarge`** (Unterklasse von
  `BrokerConnectionError`): Ein Publish, dessen Header auch mit
  gekürzten Texten nicht in die Hälfte des ausgehandelten `frame_max`
  passen, wirft sie, ohne etwas zu senden
  ([Fehlerkatalog](errors.md#messageheaderstoolarge)).
- **`swarmq.RedeliveryLimitError`**: wird nie geworfen; der
  `error_type` einer DLQ-Kopie, deren Zustellungen der Worker
  zurückgegeben hat, bis `x-acquired-count` das Zustelllimit erreichte
  ([Fehlerkatalog](errors.md#redeliverylimiterror)).

### Behoben

- **Tote Einträge im Fire-Index halten nicht mehr alle Crons an.** Der
  Leader liest pro Takt höchstens 100 fällige Einträge. Index-Einträge
  ohne Definition und deaktivierte Definitionen, die noch einen Termin
  hatten (geschrieben vor 0.10), füllten dieses Limit, sodass 100 davon
  alle Crons blockierten. Der Poll nimmt sie jetzt aus dem Fire-Index,
  bevor das Limit greift, höchstens 1000 pro Takt; die Definitionen
  bleiben. Eine Definition, die der Leader nicht dekodieren kann, lässt
  auch nicht mehr den ganzen Takt scheitern: Sie wird als
  `schedule_def_malformed_parked` geloggt und aus dem Fire-Index
  genommen ([Fire-Index](../guides/scheduling-and-retries.md#fire-index)).
  `sched_poll.lua` behält Keys und Argumente, schreibt aber jetzt
  (`ZREM`).
- **Ein Unique-Task, der über `max_age` abläuft, gibt sein Dedup-Flag
  frei.** Bisher blieb das Flag bis zum Ende von `unique_for` stehen,
  und jedes neue `schedule()` mit denselben Argumenten lieferte still
  die ID des abgelaufenen Tasks. Das Flag wird freigegeben, sobald die
  abgelaufene Nachricht in der DLQ liegt, geprüft gegen ihre Task-ID
  ([Wann das Flag freigeben](../guides/locking-and-dedup.md#wann-das-flag-freigeben)).
- **Workflows hängen nicht mehr bis zu ihrer TTL, wenn ein Schritt
  außerhalb des Retry-Pfads endet.** Ein Schritt, der abläuft
  (`max_age`), am Lock-Template scheitert, die Lock-Konflikt-Grenze
  erreicht oder im Worker nicht registriert ist, markiert seinen
  Workflow jetzt als `failed`; `get()` wirft `TaskError`, statt in den
  Timeout zu laufen. Lock-Konflikt-Grenze und Lock-Template-Fehler
  speichern jetzt auch ein Fehlerergebnis und feuern `TASK_DLQ`
  (`error_type=LockConflictLimitError` an der Grenze). Ein spätes
  Duplikat eines Schritts, der schon erfolgreich lief, markiert den
  Workflow nicht als gescheitert, und ein Workflow mit abgelaufenem
  Zustand loggt `workflow_state_missing`, statt still zu enden. Am
  Zustelllimit gibt eine Workflow-Markierung, die an einem nicht
  erreichbaren Valkey scheitert, die Zustellung nicht mehr zurück; sie
  geht trotzdem in die DLQ
  ([Fehlerfälle](../guides/workflows.md#fehlerfalle)).
- **Eine Zustellung, die der Worker immer wieder zurückgibt, kreist
  auf RabbitMQ 4.3 nicht mehr endlos und verbraucht ihr Zustellbudget
  nicht mehr in einem Augenblick.** RabbitMQ 4.3 zählt
  `nack(requeue=True)` nicht in `x-delivery-count`, nur in
  `x-acquired-count`. Die Zustellzähler-Prüfung zählt jetzt beide und
  schickt eine solche Zustellung eine Runde vor `SWARMQ_DELIVERY_LIMIT`
  als `RedeliveryLimitError` in die DLQ; bisher kreiste eine
  Zustellung, die eine Prüfung jedes Mal zurückgab (Fehlerantwort von
  Valkey, gescheiterter Dead-Letter-Publish, kein Rechen-Slot), auf 4.3
  ohne Ende und verbrauchte auf 4.2 ihre rund 20 Zustellungen in einem
  Augenblick, sodass schon eine kurze Störung sie in die DLQ schickte.
  Vor jeder Rückgabe wartet der Worker jetzt
  `min(0,1 s × 2^k, 5 s)` (k = bisherige Zustellungen) und hält dabei
  den Slot; ein Shutdown beendet die Wartezeit
  ([Zustellsemantik](../explanation/delivery-semantics.md)).

- **Ein Producer ohne Registry verliert Dedup und Priorität nicht mehr
  still.** Ein Client ohne Registry kann `unique_for`, `priority` und den
  Liefermodus aus der `TaskInfo` des Tasks nicht anwenden; er loggt jetzt
  einmal je Task-Name und Client-Instanz (für höchstens 1000 Namen) eine
  WARNING `client_task_defaults_unresolved`
  ([Client-Only-Modus](api.md#schedule)).
- **Ein Task, der mit einem riesigen Fehlertext scheitert, schickt
  seine Nachbarn nicht mehr in die Queue zurück.** `x-swarmq-traceback`
  und `x-swarmq-lock-render-error` sind auf 8 KiB begrenzt (die ersten
  6 KiB und die letzten 2 KiB); bisher schloss der Broker bei einem
  Fehlertext über `frame_max` (128 KiB) die Worker-Verbindung, und jede
  Nachricht, die der Worker hielt, wurde neu zugestellt. Jeder Publish
  prüft seine Header außerdem gegen die Hälfte von `frame_max`
  ([Eine Nachricht inspizieren](../guides/dead-letter-queues.md#eine-nachricht-inspizieren)).

- **Retries am Delay-Cap kommen nicht mehr alle gleichzeitig zurück.**
  Oberhalb von `retry_max_delay` wartet ein `exponential`- oder
  `linear`-Retry jetzt zwischen 0,6 und 0,8 × `retry_max_delay` statt
  genau den Cap, und eine sehr hohe Versuchszahl (ab etwa 1024) wirft
  kein `OverflowError` mehr. `fixed` bleibt ohne Jitter
  ([Backoff-Formeln](../guides/scheduling-and-retries.md#backoff-formeln)).

## [0.11.0] — 2026-10-09

Ein Worker konsumiert nach einem Consumer-Timeout des Brokers weiter,
und eine erneut zugestellte Nachricht läuft nicht mehr neben ihrem
Original. Dead-gelettete Nachrichten verfallen nach 7 Tagen, das
Dead-Lettering von Task-Fehlern lässt sich abschalten, und statische
Crons, die keine App mehr meldet, werden entfernt. **Bevor Worker mit
0.11.0 Scheduler-Leader werden, muss jede App auf demselben Valkey
mindestens 0.10.0 fahren**, sonst können Crons einer älteren App
entfernt werden; siehe die
[Upgrade-Hinweise](#upgrade-von-010x).

### Geändert (breaking)

- **`HealthChecker(compute_check=...)` heißt jetzt
  `HealthChecker(checks={...})`.** Der Readiness-Hook nimmt beliebig
  viele benannte Prüfungen, jede eine async Funktion, die `(ok, error)`
  liefert und in `/ready` als eigener Eintrag erscheint.
  `compute_check=fn` durch `checks={"compute": fn}` ersetzen. Betroffen
  ist nur Code, der selbst einen `HealthChecker` baut; `swarmq worker`
  und `start_worker()` verdrahten ihn selbst
  ([API](api.md#health-und-metriken)).
- **Statische Crons, die kein Worker mehr meldet, werden entfernt.**
  Der Leader entfernt einen statischen Cron, der mindestens einmal
  gemeldet wurde und den seit `SWARMQ_SCHEDULER_PRUNE_AFTER_S`
  (Standard 24 h) kein Worker mehr gemeldet hat; bisher feuerte ein aus
  dem Code entfernter Cron weiter. Das setzt voraus, dass jede App auf
  demselben Valkey mindestens 0.10.0 fährt, denn nur diese melden ihre
  Crons: Ein Cron, der einmal gemeldet wurde und jetzt nur noch von
  einer älteren App bedient wird (etwa nach einem Rollback), würde
  entfernt, obwohl die App noch läuft. Notbehelf:
  `SWARMQ_SCHEDULER_PRUNE=log` in den 0.11.0-Apps, bis die letzte App
  umgestellt ist. Nie gemeldete Crons (Altbestand, Apps vor 0.10.0)
  werden nie automatisch entfernt. Siehe
  [Crons entfernen](../guides/scheduling-and-retries.md#crons-entfernen-die-keine-app-mehr-meldet).
- **Dead-gelettete Nachrichten verfallen standardmäßig nach 7 Tagen.**
  Jede Nachricht, die der Worker dead-lettert, trägt eine `expiration`
  pro Nachricht von `SWARMQ_DEAD_LETTER_TTL_S` (Standard `604800`) und
  die Header `x-swarmq-dead-lettered-at` und
  `x-swarmq-dead-letter-ttl-ms`. Mit
  `SWARMQ_DEAD_LETTER_TTL_S=none` bleiben sie wie bisher unbegrenzt.
  Siehe
  [Aufbewahrungsfrist](../guides/dead-letter-queues.md#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen).
- **Ein eigener `ScheduleStore` braucht neue Methoden** für das
  Aufräumen von Crons: `prune_cron`, `pruned_disabled` und
  `forget_pruned_disabled`. Ein eigenes Backend läuft ohne
  Ausführungsschutz, solange es keinen `ExecutionGuardStore` als
  `execution_guard` anbietet. Neue Typen in `swarmq.contracts`:
  `ExecutionGuardStore`, `GuardClaim`
  ([API-Referenz](api.md#protokolle-der-koordinations-stores)).

### Geändert

- **Doppelte Zustellungen werden übersprungen.** Der neue
  [Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
  (standardmäßig an, `SWARMQ_EXECUTION_GUARD`) verhindert, dass eine
  erneut zugestellte Nachricht läuft, solange dieselbe Ausführung
  anderswo läuft, nachdem sie abgeschlossen ist oder nachdem ein Retry
  sie überholt hat. Die Neuzustellung eines abgeschlossenen Tasks wird
  bestätigt und übersprungen (`duplicate_delivery_skipped`); eine,
  deren Original noch läuft, geht mit Backoff über die Delay-Queue
  zurück (`execution_guard_deferred`) und zählt nicht als Retry.
  **Langsamere Erholung nach einem Absturz:** Die Neuzustellungen eines
  per SIGKILL oder OOM-Killer beendeten Workers laufen erst, wenn seine
  Running-Marker abgelaufen sind -- mit den Standardwerten etwa ein bis
  zwei Minuten nach dem Absturz statt sofort
  ([Details](../explanation/delivery-semantics.md#ausfuhrungsschutz));
  `SWARMQ_EXECUTION_GUARD_TTL_S` verkürzt das. Einen Marker, den ein
  laufender Worker während eines kurzen Valkey-Ausfalls nicht setzen
  konnte (Done, Superseded, Freigabe), holt er im Hintergrund etwa
  diese TTL lang nach und beim Shutdown sofort ein letztes Mal -- die
  Neuzustellung wartet nicht auf die TTL, und es zählt keine Waise.
  Kosten: zwei Lua-Aufrufe je Task plus eine gesammelte Verlängerung;
  in der Loadsuite unter realistischer Last kein messbarer
  Durchsatzverlust, im ungünstigsten Fall (ein Worker, nur Tasks von
  etwa 0,3 ms, CPU-gesättigt) −7,4 %, +11–12 % CPU je Task und
  +0,15–0,2 ms Latenz bei schnellen Tasks. Worker vor 0.11.0 im selben
  Deployment schützen die Zustellungen nicht, die sie bearbeiten.
- **`/ready` prüft die Consumer.** Es meldet einen Worker als nicht
  bereit vor seinem ersten `consume` und sobald eine konsumierte Queue
  (einschließlich Prioritäts-Queues) `SWARMQ_CONSUMER_READY_GRACE_S`
  (Standard 30 s) keinen aktiven Consumer hatte. `/ready` nie als
  Liveness-Probe nutzen; dafür `/health`.
- **`swarmq dlq retry` setzt eine neue `x-swarmq-replay-id`** auf jede
  Nachricht, die es bewegt, damit der Ausführungsschutz die
  Wiederholung laufen lässt, und entfernt `x-swarmq-dead-lettered-at`
  und die Frist. `dlq list` und `dlq inspect` legen Nachrichten mit
  ihrer Restfrist zurück und verwerfen abgelaufene; Nachrichten ohne
  den Header (Altbestand, vom Dead-Letter-Exchange des Brokers
  geroutete) bekommen beim ersten Lesen die volle Frist. Die Frist kommt
  aus `x-swarmq-dead-letter-ttl-ms` der Nachricht; ihre eigene
  `SWARMQ_DEAD_LETTER_TTL_S` nutzt die CLI nur für Nachrichten ohne
  diesen Header.
- **Der Dead-Letter-Pfad speichert das Fehlerergebnis mit dem
  `result_ttl` des Tasks**, wie jedes andere Ergebnis des Tasks
  (bisher: immer `SWARMQ_RESULT_TTL`).
- **`task_deferred_backend_unavailable`** kann `gate="execution_guard"`
  tragen, wenn Valkey für den Claim des Ausführungsschutzes nicht
  erreichbar ist.
- **Neue Valkey-Keys:** `sq:guard:{<identity>}:run` und
  `sq:guard:{<identity>}:state` (Identität = Task-ID, nach einer
  DLQ-Wiederholung plus `|Replay-ID`) und das Set
  `sq:{sched}:disabled`. Cron-Meldungen werden doppelt so lange wie
  `SWARMQ_SCHEDULER_PRUNE_AFTER_S` aufbewahrt, wenn das länger als
  7 Tage ist. Neue Lua-Skripte `guard_claim`, `guard_finish`,
  `sched_prune`. Die Golden-Fixtures (`keys.json`, `lua_contract.txt`,
  `headers.txt`) sind neu erzeugt; die Queue-Argumente sind unverändert,
  bestehende Deployments starten also ohne `PRECONDITION_FAILED`.
- Das Dashboard swarmv zeigt die neuen Header von DLQ-Nachrichten
  (`x-swarmq-dead-lettered-at`, `x-swarmq-replay-id`).
- `swarmq schedule --help` sagt nicht mehr, dass `prune` nicht in v1
  enthalten ist.
- **Tests:** Integrations- und e2e-Suiten zielen standardmäßig auf die
  swarmq-Testcontainer auf 5699/15699 (RabbitMQ) und 6399 (Valkey) und
  verweigern das Flushen eines lokalen Valkey oder RabbitMQ auf den
  Standardports 6379/5672, außer `SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1`
  ist gesetzt.

### Hinzugefügt

- **Consumer-Wächter** (`SWARMQ_CONSUMER_WATCH_INTERVAL_S`, Standard
  5 s): meldet einen vom Broker beendeten Consumer ab und binnen zwei
  Takten neu an, ohne Neustart und ohne einen Zombie-Consumer am Broker
  zu hinterlassen. Log `consumer_reregistered` (Grund `broker_cancel`
  oder `channel_replaced`), Metrik
  `swarmq_consumer_reregistrations_total{queue}`; weitere Events
  `consumer_cancel_failed`, `consumer_reregister_failed`,
  `consumer_watch_channel_timeout`, `consumer_watch_tick_failed`. Siehe
  [Consumer-Timeout](../explanation/delivery-semantics.md#consumer-timeout).
- **`SWARMQ_CONSUMER_READY_GRACE_S`** (Standard `30`): Gnadenfrist der
  Consumer-Prüfung in `/ready`.
- **Einstellungen des Ausführungsschutzes:** `SWARMQ_EXECUTION_GUARD`
  (Standard `true`), `SWARMQ_EXECUTION_GUARD_TTL_S` (`60`),
  `SWARMQ_EXECUTION_GUARD_DONE_TTL_S` (Standard
  `max(retry_max_delay, consumer_timeout_s) + 300`, mindestens die
  TTL), `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` (`3`; läuft dieselbe
  Ausführung so oft ohne Freigabe ab, geht sie als `WorkerHungError` in
  die DLQ). Metrik `swarmq_execution_guard_events_total{event}` mit
  `event` aus `duplicate_skipped`, `deferred`, `orphan_limit`,
  `lease_lost`. Log-Events `duplicate_delivery_skipped`,
  `execution_guard_deferred`, `execution_guard_defer_failed_redelivered`,
  `execution_guard_orphan_limit`, `execution_guard_lease_lost`,
  `execution_guard_renew_failed`, `execution_guard_finish_failed`,
  `execution_guard_finish_recovered`, `execution_guard_finish_gave_up`.
- **`SWARMQ_DEAD_LETTER_TTL_S`** (Standard `604800`, `none` =
  unbegrenzt) und **`SWARMQ_DEAD_LETTER_TERMINAL_FAILURES`** (Standard
  `true`) mit **`TaskInfo(dead_letter=...)`**, das über die globale
  Einstellung gewinnt. Abgeschaltet speichern `NoRetry`, eine Exception
  außerhalb von `retry_exceptions` und erschöpfte Retries das
  Fehlerergebnis und bestätigen die Nachricht, wie es Tasks mit `max_retries=0` vor 0.1.2 taten. Ein Alarm
  auf `swarmq_tasks_total{outcome="dlq"}` deckt diese Tasks dann nicht
  mehr ab; `TASK_FAILED` und `outcome="failed"` feuern weiter je
  Versuch. Zustellzähler-Prüfung, Waisen-Grenze, nicht dekodierbare
  Nachrichten, `max_age` und Lock-Fehler gehen weiter in die `.XQ`.
  Siehe
  [Abschalten](../guides/dead-letter-queues.md#dead-lettering-von-task-fehlern-abschalten).
- **Einstellungen für das Aufräumen von Crons:** `SWARMQ_SCHEDULER_PRUNE`
  (`on`, `log`, `off`; Standard `on`) und
  `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (Standard `86400`, mindestens
  3 × `SWARMQ_SCHEDULER_SEEN_INTERVAL_S`). Log-Events
  `scheduler_cron_pruned`, `scheduler_cron_prune_candidate`,
  `scheduler_cron_prune_skipped`, `scheduler_cron_prune_failed`. Ein
  Cron, der beim Entfernen deaktiviert war, kommt deaktiviert zurück.
- **`swarmq schedule prune --orphans [--format]`** entfernt statische
  Crons, die nie ein Worker gemeldet hat; JSON-Ausgabe
  `{"removed": [...]}`. **`swarmq schedule list`** zeigt die jüngste
  Meldung je Cron in der Spalte `LAST_SEEN` (`never` für einen
  statischen Cron ohne jede Meldung, `-` für CLI-Schedules), JSON
  `last_seen_ms` ([CLI](cli.md#swarmq-schedule-prune)).
- **Header** `x-swarmq-replay-id`, `x-swarmq-guard-defer-count` und
  `x-swarmq-dead-lettered-at`
  ([Nachrichtenformate](agent/message-formats.md)).
- **Settle-Regeln für RabbitMQ 4.3:** Ein Ack oder Nack für eine
  Zustellung, deren Consumer der Broker beendet hat, wartet auf das
  eigene Cancel des Wächters (`delivery_settle_deferred`) und wird
  verworfen, wenn dieses Cancel nicht binnen 10 s kommt
  (`delivery_settle_withheld`), damit ein spätes Settle
  den Zombie-Consumer nicht weckt.
- **`swarmq.exceptions.SettleFailedAfterPublish`** (Unterklasse von
  `BrokerConnectionError`, Worker-intern): Eine Retry- oder
  Dead-Letter-Kopie ist veröffentlicht, nur das Ack des Originals
  scheiterte ([Fehlerkatalog](errors.md#settlefailedafterpublish)).
- Dokumentation:
  [Ausführungsschutz und Consumer-Timeout](../explanation/delivery-semantics.md#ausfuhrungsschutz),
  DLQ-Aufbewahrungsfrist und die Trigger-Matrix mit abgeschaltetem
  Dead-Lettering
  ([Dead-Letter-Queues](../guides/dead-letter-queues.md#wann-nachrichten-in-der-dlq-landen)).

### Behoben

- **Ein Worker hört nach einem Consumer-Timeout auf RabbitMQ 4.3 nicht
  mehr auf zu konsumieren, und ein Task läuft danach auf 4.2 und älter
  nicht mehr doppelt.** Auf 4.3 beendet die Quorum-Queue den Consumer
  und lässt den Channel offen; der Consumer blieb als Zombie am Broker,
  ein spätes Ack weckte ihn, und seine Zustellungen wurden still
  verworfen. Bis 4.2 schließt der Broker den Channel, und aio-pikas
  Restore bekam die Nachricht erneut, während das Original noch lief.
  Jetzt meldet der Consumer-Wächter den Consumer ab und neu an, Settles
  warten auf dieses Cancel, und der Ausführungsschutz stellt die
  Neuzustellung zurück oder überspringt sie.
- **Die Dead-Letter-Queue wächst nicht mehr unbegrenzt:**
  Dead-gelettete Nachrichten verfallen, Lesen der DLQ verlängert ihre
  Frist nicht, und das Dead-Lettering terminaler Task-Fehler lässt sich
  abschalten.
- **Ein aus dem Code entfernter Cron hört auf zu feuern**, sobald ihn
  `SWARMQ_SCHEDULER_PRUNE_AFTER_S` lang kein Worker gemeldet hat.
- `log_records_dropped` zählt nicht mehr das Stopp-Signal einer
  Log-Queue, deren Pipe multiprocessing beim Prozessende schon
  geschlossen hatte (unter CPU-Last meldete es 2 statt 1).
- Liegt die Kopie eines fehlgeschlagenen Tasks in der DLQ und scheitert
  nur das Ack des Originals (Channel weg), gilt der Fehlschlag jetzt als
  dead-gelettert: Das Unique-Flag wird freigegeben, `TASK_DLQ` feuert,
  und der Worker loggt `dlq_ack_failed_after_publish` (WARNING) statt
  `dlq_publish_failed`. Der Ausführungsschutz überspringt die
  Neuzustellung. Das Lock-Gate (Konfliktgrenze, Render-Fehler) versucht
  in diesem Fall nicht mehr zusätzlich den DLX-Weg.
- **`dlq list`/`inspect`/`retry` behalten die Frist, mit der eine
  Nachricht dead-gelettert wurde.** Der Worker schreibt sie in
  `x-swarmq-dead-letter-ttl-ms` (Millisekunden oder `"none"`); eine CLI
  mit kürzerer oder Standard-`SWARMQ_DEAD_LETTER_TTL_S` verwirft keine
  Nachrichten mehr, die der Worker länger oder unbegrenzt halten wollte.
- Auf RabbitMQ 4.3 scheitert ein Dead-Letter-`nack`, der vergeblich auf
  das eigene Cancel des Consumer-Wächters wartet, jetzt, statt still
  zurückzukehren; der Ausführungsschutz gibt die Nachricht frei, damit
  die Neuzustellung des Brokers läuft, statt sie als erledigt zu
  überspringen.
- Ein Claim des Ausführungsschutzes, der beim Client in den Timeout lief,
  in Valkey aber ausgeführt wurde, wird im Hintergrund freigegeben (nur
  der eigene Marker) und zählt später nicht mehr als Waise.
- Der Ausführungsschutz gibt seinen Running-Marker vor der Schedfire- und
  Unique-Finalisierung frei; ein Shutdown-Abbruch dort hält ihn nicht
  mehr bis zum Prozessende am Leben.
- Ein Cron, der beim Entfernen deaktiviert war und wieder gemeldet wird,
  wird deaktiviert angelegt, mit WARNING
  `scheduler_cron_revived_disabled`; `schedule remove --permanent`
  löscht jetzt auch diese Markierung.
- Summieren sich die Zurückstellungen einer Neuzustellung auf mehr als
  `consumer_timeout_s`, weil das Original noch läuft, loggt der Worker
  einmal je Nachricht `execution_guard_defer_stalled` (WARNING) und
  zählt `event="defer_stalled"`; er stellt weiter zurück.

### Upgrade von 0.10.x

1. **Jede App auf dem Valkey muss mindestens 0.10.0 fahren**, bevor
   Worker mit 0.11.0 Scheduler-Leader werden können. Mit
   `swarmq schedule status` prüfen, dass `LEADER_VERSION` und alle
   `WORKERS` 0.10.0 oder neuer zeigen. Lässt sich eine App noch nicht
   umstellen, in jeder 0.11.0-App `SWARMQ_SCHEDULER_PRUNE=log` setzen
   und `scheduler_cron_prune_skipped` beobachten; zurück auf `on`,
   sobald alle Apps melden.
2. **Crons ohne jede Meldung ansehen:** `swarmq schedule list` zeigt sie
   mit `LAST_SEEN` `never`, und der Leader loggt
   `scheduler_cron_prune_candidate`. Nicht mehr benötigte mit
   `swarmq schedule prune --orphans` entfernen, aber erst, wenn jede App
   mindestens 0.10.0 fährt.
3. **Liveness-Probes:** Zeigt eine Probe auf `/ready`, auf `/health`
   umstellen. `/ready` kann jetzt nach 30 s ohne Consumer rot werden,
   und ein Neustart hilft nicht.
4. **Eigener `HealthChecker` oder `ScheduleStore`:** auf
   `checks={"compute": ...}` umstellen und die drei
   `ScheduleStore`-Methoden ergänzen.
5. **Dead-Letter-Queues:** über `SWARMQ_DEAD_LETTER_TTL_S` entscheiden
   (standardmäßig 7 Tage; `none` behält das alte Verhalten). Die
   `swarmq dlq`-CLI behält die Frist, mit der jede Nachricht
   dead-gelettert wurde. Alte Nachrichten
   bekommen die Frist beim ersten Lesen. Nachrichten, die der
   Dead-Letter-Exchange des Brokers routet, tragen keine Frist; die
   [Broker-Policy auf `\.XQ$`](../guides/dead-letter-queues.md#die-xq-per-broker-policy-begrenzen)
   aus 0.10.0 bleibt deshalb der Notbehelf, der jeden Weg und die Länge
   begrenzt; dabei beachten, dass pro Queue nur eine Policy gilt und
   Überlauf gelöscht wird.
6. **Dead-Lettering abschalten** (`dead_letter=False`): Alarme für diese
   Tasks von `outcome="dlq"` auf `outcome="failed"` umstellen.
7. **Ausführungsschutz während des Rolling Upgrades:** Er schützt nur
   Zustellungen, die ein 0.11.0-Worker bearbeitet. Eingeschaltet
   lassen; nur mit `SWARMQ_EXECUTION_GUARD=false` abschalten, wenn ein
   Problem gemessen wurde.

## [0.10.0] — 2026-10-09

Crons bekommen eine Zeitzone mit fester Regel für die Zeitumstellung,
und jeder Worker meldet seine Crons, sodass auch Crons von Apps
ankommen, deren Worker nie Leader werden. **Alle Apps auf einem
gemeinsamen Valkey auf 0.10.0 bringen, bevor ein Cron eine Zeitzone
bekommt**, und von Hand umgerechnete UTC-Crons vor dem 25. Oktober
2026, 01:00 UTC umstellen:
[Migrationsanleitung](../guides/scheduling-and-retries.md#von-hand-umgerechnete-utc-crons-umstellen).

### Geändert (breaking)

- **Upgrade in zwei Schritten, wenn Crons Zeitzonen bekommen.** Ein
  Scheduler-Leader vor 0.10.0 kennt das neue Feld `timezone` nicht: Er
  verwirft die Zone jedes Crons, den er feuert, und feuert ihn danach
  wieder in UTC. Zuerst alle Apps auf demselben Valkey upgraden, ohne
  Crons zu ändern, und mit `swarmq schedule status` prüfen, dass
  `LEADER_VERSION` und alle `WORKERS` 0.10.0 zeigen; erst dann Zonen
  setzen und die Ausdrücke auf Ortszeit zurückführen, beides im selben
  Commit. Details und Rezept für die Äquivalenzprüfung:
  [Migrationsanleitung](../guides/scheduling-and-retries.md#von-hand-umgerechnete-utc-crons-umstellen).
- **Ein eigener `ScheduleStore` braucht neue Methoden.**
  `fire_ts(name)`; `put(..., fire_ts_ms=None)` muss den Fire-Index
  unberührt lassen; `commit_fire` nimmt das neue Schlüsselwort
  `fired_payload`; `replan_if_unchanged` plant einen Eintrag ohne Score
  atomar neu ein; für Cron-Meldungen `report_cron`, `cron_reports` und
  `prune_cron_reports`; für die Leader-Version `report_worker_version`,
  `worker_versions`, `set_leader_version` und `leader_info`. Neue Typen
  in `swarmq.contracts`: `CronReport`, `CronReports`, `LeaderInfo`.
  Signaturen und Semantik:
  [API-Referenz](api.md#protokolle-der-koordinations-stores).

### Geändert

- **Statische Crons kommen über Cron-Meldungen statt über die Registry
  des Leaders.** Bisher schrieb der Leader die Crons seiner eigenen
  Task-Registry, wenn er die Leader-Rolle übernahm, und fügte nur ein
  oder aktualisierte. Crons einer App, deren Worker nie Leader wurden,
  kamen nie an, und eine Änderung wartete auf den nächsten
  Leader-Wechsel. Jetzt meldet jeder Worker seine statischen Crons beim
  Start und alle `SWARMQ_SCHEDULER_SEEN_INTERVAL_S` (Standard 60 s);
  der Leader übernimmt pro Name die lebende Definition, die zuletzt neu
  aufgetaucht ist. Eine Änderung im Code einer beliebigen App gilt
  binnen zwei Takten nach dem Start des ersten Workers mit dem neuen
  Code; im Rolling Deploy gewinnt der neuere Code-Stand, und ein später
  neu startender alter Worker setzt ihn nicht zurück. Siehe
  [Cron-Meldungen](../guides/scheduling-and-retries.md#cron-meldungen-und-leader).
- **`swarmq schedule remove --permanent` auf einen statischen Cron, den
  ein lebender Worker meldet:** Der Cron ist binnen eines Takts wieder
  da (bisher: beim nächsten Leader-Wechsel). Dauerhaft weg ist er, wenn
  ihn kein Code mehr meldet. Ein ohne `--permanent` deaktivierter Cron
  bleibt deaktiviert.
- **Log-Events des Cron-Syncs.** `scheduler_sync_taskinfo_crons_failed`
  entfällt. Neu: `scheduler_cron_report_failed`,
  `scheduler_cron_reports_read_failed`,
  `scheduler_cron_reports_prune_failed`, `scheduler_cron_adopt_failed`
  (WARNING) und `scheduler_cron_report_invalid` (ERROR). Ein Cron, den
  ein lebender Worker meldet, loggt beim Fire auf dem Leader einer
  anderen App kein `scheduler_task_not_in_local_registry` mehr. Siehe
  [Event-Katalog](logging.md#event-katalog).
- **Neue Valkey-Keys**, alle unter dem Hash-Tag `{sched}`:
  `sq:{sched}:report:first_seen`, `sq:{sched}:report:last_seen`,
  `sq:{sched}:report:def` (Feld `name|fingerprint`),
  `sq:{sched}:leader_version` (mit der TTL des Locks) und
  `sq:{sched}:worker_version:<worker_id>` (TTL drei Meldetakte). Der
  Leader löscht Meldungen, die 7 Tage nicht erneuert wurden. Die
  Golden-Fixtures des Backend-Vertrags (`keys.json`,
  `lua_contract.txt`, Schedule-Definition) sind für die neuen Keys,
  Skripte und das Feld `timezone` neu erzeugt; die Queue-Argumente sind
  unverändert.
- **Abhängigkeiten:** `croniter>=6.2` (bisher `>=2`; sein Verhalten in
  der doppelten Stunde ist nur für diese Versionen belegt) und neu
  `tzdata`, damit Zonen auch in Images ohne `/usr/share/zoneinfo`
  aufgelöst werden.
- **Tests und CI pinnen RabbitMQ exakt:** `4.3.6` als Hauptbroker, ein
  zweiter `4.2.9` für die Charakterisierung des Consumer-Timeouts,
  `3.13.7` im Chaos-Stack. Das Verhalten nach einem Consumer-Timeout
  unterscheidet sich zwischen 4.2 und 4.3; die Tests halten es pro
  Version fest.

### Hinzugefügt

- **Zeitzonen für Crons.** `TaskInfo(timezone="Europe/Berlin")`
  (IANA-Name, bei der Klassendefinition geprüft) oder der Standard
  `SWARMQ_SCHEDULER_TIMEZONE` / `SwarmQConfig(scheduler_timezone=...)`.
  Eine Zone am Task schlägt den Standard, auch ein ausdrückliches
  `"UTC"`. Ohne Zone ändert sich nichts: `"UTC"`, `"Etc/UTC"` und keine
  Angabe werden gleich gespeichert, bestehende Schedules behalten ihren
  Fingerabdruck. [Zeitzonen](../guides/scheduling-and-retries.md#zeitzonen).
- **Regel für die Zeitumstellung.** In der doppelten Stunde im Herbst
  feuert ein Cron ohne `*` im Stundenfeld nur im ersten Durchlauf; mit
  `*` (auch `*/n`, `@hourly`) folgt er der echten Zeit. In der Lücke im
  Frühjahr feuert eine feste Stunde einmal zur nächsten existierenden
  Uhrzeit. Tabelle und Beispiele:
  [Zeitumstellung](../guides/scheduling-and-retries.md#zeitumstellung).
- **`swarmq.scheduler.next_fire_times(cron, start, *, timezone=None,
  count=1)`** berechnet Termine mit der Regel des Schedulers, etwa um
  vor der Umstellung zu prüfen, dass ein umgerechneter Ausdruck seine
  Termine behält ([API](api.md#next_fire_times)).
- **`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`** (Standard `60`, `> 0`): Takt
  der Cron-Meldungen und der Übernahme durch den Leader. Eine Meldung
  lebt drei Takte; alle Apps auf einem Valkey sollten denselben Wert
  nutzen.
- **Konflikte:** Leben mehrere Definitionen eines Crons länger als 10
  Minuten (typisch: gleichnamige Tasks in zwei Apps), loggt der Leader
  `scheduler_cron_conflict`, sendet `Signal.SCHEDULER_CRON_CONFLICT`
  (`worker_id`, `schedule_name`, `fingerprints`) und zählt
  `swarmq_scheduler_cron_conflicts_total{schedule_name}`.
- **Leader-Version.** Der Leader schreibt seine swarmq-Version neben das
  Lock, jeder Worker meldet seine eigene. Ein Worker, der einen Leader
  ohne Version oder vor 0.10.0 sieht, loggt `scheduler_leader_outdated`
  (Logger `swarmq.scheduler.leader_version`, einmal pro Leader), sendet
  `Signal.SCHEDULER_LEADER_OUTDATED` (`worker_id`, `leader`,
  `leader_version`, `outdated`) und setzt die Gauge
  `swarmq_scheduler_leader_outdated{worker_id}`.
  `swarmq schedule status` zeigt `LEADER_VERSION` und einen Block
  `WORKERS`; `--format json` ergänzt `leader_version` und `workers`.
  Schreib- oder Lesefehler loggen `scheduler_leader_version_write_failed`
  und `scheduler_leader_version_check_failed`.
- `SchedulerLoop(static_crons=..., version=...)` für Code, der die
  Scheduler-Schleife selbst betreibt.
- **Guide: `{queue}.XQ` per Broker-Policy begrenzen.** Die DLQ hat
  weder TTL noch Längengrenze, und jeder terminale Fehlschlag landet
  dort, auch mit `max_retries=0`. Als sofortiger Notbehelf begrenzt eine
  RabbitMQ-Policy auf `\.XQ$` mit `message-ttl` und `max-length` sie.
  Pro Queue gilt nur eine Policy, und Überlauf wird gelöscht, nicht
  weiter dead-gelettert:
  [Dead-Letter-Queues](../guides/dead-letter-queues.md#die-xq-per-broker-policy-begrenzen).

### Behoben

- **Ein fälliger Termin geht nicht mehr verloren, wenn sich ein
  statischer Cron ändert.** Ist der alte Termin schon fällig, bleibt er
  und feuert einmal mit dem neuen Inhalt; sonst wird der nächste Termin
  neu berechnet.
- **Eine geänderte Queue, Zeitzone oder ein geänderter Task-Name eines
  statischen Crons wird übernommen.** Bisher ersetzte nur ein geänderter
  Ausdruck die gespeicherte Definition.
- **Ein Cron, dessen nächster Termin sich nicht berechnen lässt,
  blockiert die Poll-Schleife nicht mehr.** Bisher wurde der nächste
  Termin nach dem Publish berechnet, und ein Fehler beendete den ganzen
  Poll-Tick: Die folgenden Einträge des Batches feuerten nicht, und der
  Eintrag blieb für den nächsten Tick fällig. Jetzt wird der nächste
  Termin vor dem Publish berechnet; bei einem Fehler wird nur dieser
  Eintrag geloggt (`scheduler_next_fire_failed`, ERROR) und verlässt
  den Fire-Index, und der Leader plant ihn wieder ein, sobald ein
  lebender Worker ihn meldet und sich der Termin berechnen lässt. Dieses
  Wiedereinplanen ist atomar: Ein `swarmq schedule remove`, das
  dazwischen landet, bleibt wirksam. Lässt sich der Eintrag nicht aus
  dem Fire-Index nehmen, loggt der Leader `scheduler_park_failed`
  (WARNING, Logger `swarmq.scheduler.fire_plan`) und versucht es im
  nächsten Takt erneut; `scheduler_park_owner_mismatch` (WARNING) heißt,
  dass er den Leader-Lock inzwischen verloren hat.
- **Deaktivierte Definitionen bekommen keinen Eintrag im Fire-Index.**
  Einträge, die ältere Versionen dort hinterlassen haben, werden
  weiterhin übersprungen.
- **Ein Fire setzt eine zwischenzeitlich übernommene Definition nicht
  mehr zurück.** Der Commit nach einem Fire schreibt die Definition nur
  zurück, wenn sie sich seit dem Poll nicht geändert hat.
- **Ein verlorener Broker-Kanal beim abschließenden Ack endet nicht
  mehr als „Task exception was never retrieved“.** Fiel RabbitMQ weg,
  während der Worker eine Zustellung abschloss, entwich aiormqs
  `ChannelInvalidStateError` aus der Nachrichten-Task, und asyncio
  loggte ihn auf ERROR ohne Task-Kontext. Der Ausgang stimmte schon
  (RabbitMQ stellt unbestätigte Nachrichten neu zu), nur das Log war
  falsch. `ack`, `redeliver` und `route_via_dlx` werfen bei verlorenem
  Kanal oder verlorener Verbindung jetzt `BrokerConnectionError`, wie
  es der Broker-Vertrag beschreibt; der Abbruch der aufrufenden Task
  wirft weiterhin `CancelledError`. Der Worker loggt eine WARNING
  `delivery_settle_failed` mit Task-Kontext, `error_type` und `error`.
  Jeder andere unerwartete Fehler, der eine Zustellung beendet, wird als
  ERROR `delivery_handler_failed` mit Task-Kontext und Traceback
  geloggt, statt aus der Task zu entweichen.

- **Fehler-Logs im Worker tragen `error_type` und `error`.**
  `backend_connection_error` (Ergebnis ließ sich nicht speichern) hatte
  beides nicht und war immer ERROR; jetzt WARNING bei `AT_LEAST_ONCE`,
  wo die Neuzustellung heilt, und weiter ERROR bei `FIRE_AND_FORGET`, wo
  das Ergebnis verloren ist. `workflow_advance_backend_error` und
  `workflow_advance_broker_error` folgen derselben Regel. Ebenfalls mit
  Fehlerfeldern: `task_expired_result_store_failed`,
  `task_expired_dlq_publish_failed`, `task_expired_nack_also_failed`,
  `cancel_consume_partial_failure` und
  `compute_recycle_replacement_failed`.

## [0.9.0] — 2026-10-07

### Geändert (breaking)

- **CPU-Pool: `logging.basicConfig()` in einem Task-Modul wirkt nicht
  mehr, und `--log-format`, `--log-level` und `--log-config` gelten in
  den Rechenkindern.** Beide Pools folgen jetzt derselben Regel „App
  gewinnt“. Bisher importierte der Forkserver deine Task-Module mit
  unkonfiguriertem Root-Logger; ein `basicConfig()` beim Import gewann
  deshalb in jedem Rechenkind: gegen `--log-format json` (Textzeilen
  mitten im JSON-Strom), gegen `--log-config` (Task-Zeilen im Format der
  App) und gegen `--log-level` (INFO-Zeilen gingen verloren, wenn
  `basicConfig` kein Level setzte). Jetzt hängt der Forkserver vor dem
  Import deiner Module einen Platzhalter-Handler an den Root-Logger (er
  startet keinen Thread und gibt WARNING und höher an `lastResort`
  weiter, wenn kein anderer Handler den Record bekommt), und jedes
  Rechenkind wendet die Regel des IO-Workers an:
  `basicConfig` wirkt nicht, ein Modul, das die Root-Handler ersetzt
  (etwa mit `logging.config.dictConfig`), gewinnt, und hinzugefügte
  Handler schreiben neben denen von SwarmQ. `logging_reconfigured_by_app`
  und `logging_handlers_added_by_app` erscheinen einmal pro Pool, vom
  Logger `swarmq.worker.compute.pool`. Jeder vom Forkserver per Fork
  gestartete Prozess beginnt ohne den Platzhalter; in Prozessen, die
  deine App selbst über den gemeinsamen Forkserver startet (etwa ein
  `ProcessPoolExecutor` mit der Standard-Startmethode `forkserver` unter
  Python 3.14), wirkt `basicConfig` also. Unter `--no-configure-logging`
  (`configure_logging=False`) wirkt `basicConfig` in beiden Pools. Das
  gilt ebenso für die Rechenkinder eines eingebetteten CPU-Pools. Ein per
  Pfad gestartetes Hauptmodul (`python app.py`) importiert der
  Forkserver vor SwarmQs Vorladen; dessen Logging beim Import gewinnt
  dort weiterhin. Garantiert gilt die Regel mit dem Befehl `swarmq` und
  mit `python -m`. **Migration:** Das Format, das du mit `basicConfig`
  setzt, gehört in eine `--log-config`-Datei; soll dein Modul das Logging
  in jedem Prozess besitzen, nimm `--no-configure-logging`. Loggt deine
  App über einen eigenen `QueueHandler`, starte dessen `QueueListener` im
  `--configure`-Hook statt beim Import; SwarmQ startet ihn dann in jedem
  Rechenkind. Details:
  [Logging](logging.md#wenn-ein-task-modul-das-logging-konfiguriert).
- **Eine Logzeile je Zurückstellung, wenn das Backend vor der Ausführung
  nicht erreichbar ist.** Die Gate-Warnungen
  `cancel_check_backend_error`, `workflow_cancel_check_backend_error`,
  `cancel_store_result_failed` und `schedfire_dedup_backend_error`
  entfallen; `rate_limit_check_failed` und `lock_acquire_failed` (ERROR)
  erscheinen nur noch bei anderen Fehlern als einem unerreichbaren
  Backend. Ihre Angaben stehen jetzt in
  `task_deferred_backend_unavailable`: im neuen Feld `stage`
  (`completed_check`, `inflight_claim`, `cancel_check`,
  `workflow_cancel_check`, `cancel_store_result`, `check`, `acquire`)
  sowie, wo zutreffend, in `schedfire_key`, `workflow_id` oder
  `lock_name`. `task_defer_failed_redelivered` trägt dieselben Felder
  und den Backend-Fehler als `backend_error_type` und `backend_error`.
  Alarme auf die entfallenen Events sollten stattdessen auf
  `task_deferred_backend_unavailable` prüfen. Antwortet Valkey einer
  solchen Prüfung dagegen mit einem Fehler (`WRONGTYPE`, Skriptfehler),
  loggen die Schedule-Fire- und die Cancel-Prüfung das neue ERROR-Event
  `task_gate_backend_error` und reihen an derselben Stelle neu ein;
  `error_type` lautet in diesen und anderen Backend-Logzeilen dann
  `BackendResponseError` (siehe Hinzugefügt).

### Geändert

- **Der Listener-Thread heißt jetzt `swarmq-log-lsnr`** (bisher
  `swarmq-log-listener`): Linux behält 15 Zeichen eines Thread-Namens,
  und die Forkserver-Prüfung erkennt den Thread an diesem Namen.
  Betroffen ist nur Code, der den Thread über seinen Namen sucht.
- **`log_records_dropped` ist ein gewöhnlicher Record aus dem
  Listener-Thread** (Logger `swarmq.logging`, Level WARNING, Feld
  `count`), geschrieben über die eigenen Handler des Listeners,
  höchstens einmal pro Sekunde und noch einmal beim Stoppen. Bisher hat
  der loggende Code die Meldung selbst nach stderr geschrieben. Bei der
  Opt-in-Log-Queue formatieren deine Handler sie; `log_json` wählt dort
  nur das Format der neuen Zeile `log_drain_timeout_exceeded`.
- **`log_config_reload_failed` kommt jetzt vom Logger
  `swarmq.process_manager.reload`** (bisher `swarmq.process_manager.run`).
  Eine Prüfung der `--log-config`-Datei, die in ihre Frist von 30
  Sekunden läuft, wird stattdessen als `log_config_reload_probe_timeout`
  gemeldet. Die übrigen Reload-Events `process_manager_reload_execvp`,
  `reload_aborted_alive_children` und `reload_execvp_failed` kommen vom
  selben Logger (bisher `swarmq.process_manager.supervisor`).
- **Der Befehl `swarmq` startet über `swarmq.cli:console_main`** (bisher
  `swarmq.cli:main`), wie `python -m swarmq.cli`. Er beendet den Prozess
  hart, wenn nach dem letzten Leeren noch ein Log-Thread feststeckt
  (siehe Hinzugefügt). `swarmq.cli.main()` bleibt unverändert und endet
  nie hart.

### Hinzugefügt

- **`BackendResponseError`** (`from swarmq import BackendResponseError`),
  eine Unterklasse von `BackendConnectionError`: Valkey war erreichbar,
  hat eine Operation aber mit einem Fehler beantwortet (`WRONGTYPE`,
  Skriptfehler). Jedes `except BackendConnectionError` fängt sie weiter.
  Die Prüfungen vor der Ausführung stellen eine Nachricht dafür nicht
  zurück (Warten hilft nicht), sondern reihen sie an derselben Stelle
  neu ein; ein bleibender Fehler landet so über das Zustelllimit in
  `{queue}.XQ`. Fehlerantworten, die durch Warten heilen, gelten als
  Ausfall und bleiben ein einfacher `BackendConnectionError`: `OOM`
  (maxmemory erreicht), `READONLY` (Failover), `LOADING`, `TRYAGAIN`,
  `CLUSTERDOWN`, `MASTERDOWN` und `MISCONF`. Die Schedule-Fire- und die
  Cancel-Prüfung loggen dabei
  das neue ERROR-Event `task_gate_backend_error` (Felder `gate`,
  `stage`, `error_type`, `error`, dazu `schedfire_key` oder
  `workflow_id`).
- **`log_drain_timeout`** (`--log-drain-timeout SECONDS`,
  `SWARMQ_LOG_DRAIN_TIMEOUT`, `SwarmQConfig(log_drain_timeout=...)`,
  `LoggingSettings.drain_timeout`; Vorgabe 5; Priorität CLI > Env >
  Config > Vorgabe): Obergrenze für jedes Leeren der Log-Queues — beim
  Prozessende, am Ende eines Worker-Subprozesses, bevor sich der
  Supervisor für einen Reload neu ausführt, in einem Rechenkind vor
  jedem Ergebnis, für die Listener einer `--log-config`-Datei und in
  `SwarmQ.__aexit__`. Alle Listener eines Prozesses teilen sich die Frist
  und werden parallel gestoppt; was bis dahin nicht geschrieben ist, gilt
  als verworfen. Ein Wert, der keine endliche Zahl größer als 0 und
  höchstens `threading.TIMEOUT_MAX` ist, beendet `swarmq worker` mit
  Exit `78` (bei `SwarmQConfig` ein `ConfigurationError`).
  `shutdown_logging()` nimmt ein optionales `timeout`.
- **Hartes Ende der eigenen Prozesse von SwarmQ.** Steckt nach dem
  Leeren noch ein aufgegebener Log-Thread fest, enden der Befehl
  `swarmq`, die Worker-Subprozesse und die Rechenkinder per `os._exit`
  mit dem Exit-Code, mit dem sie ohnehin geendet hätten, statt mit
  `Fatal Python error` abzubrechen (Exit `134`) oder zu hängen.
  Registrierte `atexit`-Funktionen, die noch nicht gelaufen sind, laufen
  dann nicht mehr; stdout wird nicht geleert, wenn es sich die Datei oder
  Pipe mit dem hängenden Stream teilt (`2>&1`, journald), und stdout und
  stderr werden höchstens 0,2 Sekunden lang geleert. Endet der Befehl
  `swarmq`, ein Worker-Subprozess oder ein Rechenkind mit einer
  Ausnahme, geht ihr Traceback (oder die Meldung eines `SystemExit`)
  vorher auf stderr, gekürzt auf 4096 Bytes und nur, wenn stderr nicht
  blockiert; Strg-C beendet den Befehl `swarmq` dann mit `130`. Apps,
  die SwarmQ einbetten, werden nie hart beendet.
- **Neue Events:** `log_handler_errors` (ein Handler, Filter oder
  Formatter hat geworfen; Feld `count`), `log_drain_timeout_exceeded`
  (eine Zeile direkt auf stderr, wenn ein Leeren die Frist überschritten
  hat, nur wenn stderr nicht blockiert; Felder `count`, `timeout_s`),
  `log_config_reload_probe_timeout` (Felder `path`, `timeout_s`, `hint`)
  `forkserver_restarted_multithreaded` (ERROR; Felder `pid`, `threads`,
  `thread_names`, `hint`), `compute_fork_source_rejected` (ERROR; Felder
  `reason`, `hint`) und `worker_fork_source_rejected` (Supervisor,
  WARNING; Felder `worker_id`, `pid`, `exitcode`).
- **Die Thread-Prüfung des Forkservers nennt die gefundenen Threads**
  und die wahrscheinliche Ursache: die Opt-in-Log-Queue, einen beim
  Import gestarteten logging-`QueueListener` deiner App oder sonst eine
  BLAS-/OpenMP-Rechnung. Sie läuft auch nach einem stillen Neustart des
  Forkservers, bevor das nächste Rechenkind Arbeit bekommt. Ein neu
  gestarteter Forkserver mit mehr als einem Thread wird verworfen:
  `/ready` meldet den Pool als ungesund, der Worker loggt
  `compute_fork_source_rejected`, hört auf zu konsumieren und fährt
  geordnet herunter (die laufende Nachricht geht ohne Retry-Zählung
  zurück in die Queue). Ein Worker-Subprozess endet dann mit Code `71`,
  und der Supervisor startet ihn mit frischem Forkserver neu (im
  Neustart-Budget). Mit eingebettetem CPU-Pool wirft `Worker.start()`
  nach dem Herunterfahren `swarmq.supervision.ForkSourceRejected`; ob
  neu gestartet wird, entscheidet deine App.

### Behoben

- **Agent-Spec: Exit-Codes.** `errors.json` nannte für
  `ConfigurationError` den CLI-Exit-Code `1`; `swarmq worker` endet mit
  `78`. `cli.json` listet jetzt auch `78` und `130`.

- **Ein Scheduler-Fire geht nicht mehr verloren, wenn Valkey direkt
  nach seinem Claim ausfällt.** Der Worker beansprucht einen Fire in
  Valkey, bevor er den Task ausführt. Beendete eine spätere Prüfung
  (Cancel-Check, Zustellzähler) die Zustellung, wurde der Claim nie
  freigegeben; scheiterte die Freigabe selbst (Valkey weg), sah die
  zurückgestellte, wiederholte oder neu eingereihte Kopie den Claim,
  hielt sich für ein Duplikat und bestätigte sich, der Fire lief also
  nie (der Claim lebt bis zu `SWARMQ_SCHEDULER_INFLIGHT_TTL`, Standard
  300 s). Jetzt speichert der Claim ein Owner-Token, jede Kopie
  trägt es im neuen Header `x-swarmq-schedfire-claim`, und eine Kopie,
  die ihr eigenes Token findet, übernimmt den Claim (das Token rotiert
  dabei, eine Broker-Redelivery dieser Kopie bleibt also ein Duplikat).
  Echte Duplikate (ein zweites Publish des Fires, eine Redelivery des
  Originals) werden weiter verworfen. Prüfungen, die die Zustellung vor
  dem Task beenden oder mit einer Ausnahme abbrechen, geben den Claim
  jetzt ebenfalls frei, mit Owner-Prüfung. Das Token wird vor dem
  Claim-Aufruf festgelegt: Geht dessen Antwort verloren (Timeout,
  Reset), kann Valkey den Claim geschrieben haben oder nicht; die Kopie
  trägt dann das neue Token und die angebotenen (kommagetrennt, neuestes
  zuerst, höchstens 16) und übernimmt den Claim in beiden Fällen. Ein
  Neueinreihen an derselben Stelle (`nack(requeue=True)`: Shutdown, ein
  Publish-Fallback, ein anderer Fehler als ein Ausfall) kann kein Token
  tragen, deshalb wird der Claim vorher freigegeben. Scheitert diese
  Freigabe (Valkey weg oder ein Store von Dritten wirft eine beliebige
  Ausnahme), veröffentlicht der Worker stattdessen eine Kopie mit dem
  Token in die Delay-Queue, mit dem Backoff einer Zurückstellung, und
  bestätigt das Original, sobald der Broker die Kopie bestätigt hat
  (WARNING `task_requeued_as_copy`); nur wenn auch dieser Publish
  scheitert, reiht er an derselben Stelle neu ein
  (`task_copy_failed_redelivered`), und der Fire kann dann weiter als
  Duplikat verworfen werden, solange der Claim lebt. Den Claim einer
  Zustellung, deren Prüfungen abgebrochen werden (Shutdown), gibt der
  Worker auch dann frei, wenn während der Freigabe ein zweiter Abbruch
  kommt. Aus dem Header bietet der Worker dem Claim-Skript höchstens 16
  Tokens an. Neues
  Lua-Skript `sched_fire_claim.lua`; `ScheduleStore.claim_fire` hat die
  neuen optionalen Keywords `owner` und `takeover` (ein Token oder eine
  Folge von Tokens), `ScheduleStore.release_fire_claim` das neue
  optionale Keyword `owner` und `ScheduleStore.commit_fire` das neue
  Keyword `fired_ts_ms` (siehe unten). Worker und Scheduler übergeben
  sie immer: Ein Store von Dritten, der sie nicht annimmt, wirft bei
  jedem Fire `TypeError`. **Rollendes Upgrade:** Der Schutz setzt
  voraus, dass alle Worker 0.9.0 laufen; ein Worker älterer Version
  schreibt das Token weder noch erkennt er es, eine Kopie, die er
  bearbeitet, kann also weiter als Duplikat verworfen werden, bis alle
  Worker 0.9.0 laufen.
- **Retry-Kopien erben die Zustellzähler des Brokers nicht mehr.** Die
  Kopie für einen Retry, einen Rate-Limit-Treffer oder einen
  Lock-Konflikt übernahm `x-delivery-count` und `x-acquired-count` vom
  Original. RabbitMQ 4 behält einen vom Publisher gesetzten
  `x-delivery-count` bei der Zustellung bei, RabbitMQ 3.13 zählt
  zusätzlich Requeues; eine Kopie, deren geerbter Zähler
  `SWARMQ_DELIVERY_LIMIT - 1` erreichte, landete als `WorkerHungError`
  in `{queue}.XQ`, obwohl der Task noch Retries hatte. Diese Kopien sind
  jetzt neue Nachrichten mit eigenem Zustellbudget, wie die Kopien der
  Zurückstellung.
- **Ein Backend-Ausfall vor der Ausführung verbraucht das Zustellbudget
  nicht mehr.** Erreichte eine Prüfung vor dem Task (Schedule-Fire-Dedup,
  Cancel-Check, Rate-Limit, Lock) Valkey nicht, reihte der Worker die
  Nachricht an derselben Stelle neu ein (`nack(requeue=True)`), etwa alle
  1,5 s (abgewiesene Verbindung) bis 5 s (Blackhole). Auf einem Broker,
  der Requeues auf `x-delivery-limit` anrechnet (RabbitMQ 3.13),
  verschob der Broker die Nachrichten nach etwa 20 Runden nach
  `{queue}.XQ`, ohne `TASK_DLQ`, Fehlerergebnis oder Logzeile; auf
  RabbitMQ 4 kreiste die Nachricht ohne Backoff. Jetzt publiziert der
  Worker eine Kopie in die Delay-Queue und bestätigt das Original erst,
  wenn der Broker die Kopie bestätigt hat. Die Wartezeit beginnt bei
  `SWARMQ_BACKEND_DEFER_BASE_DELAY` (neu, Standard 1 s) und verdoppelt
  sich je Zurückstellung bis `SWARMQ_BACKEND_DEFER_MAX_DELAY` (neu,
  Standard 60 s), um ±20 % gestreut, damit gemeinsam zurückgestellte
  Nachrichten nicht gemeinsam wiederkommen; die Kopie behält alle SwarmQ-Header, aber nicht die
  Zustellzähler des Brokers (neuer Header
  `x-swarmq-backend-defer-count`). Die Zählung beginnt neu, sobald das
  Backend geantwortet hat: Kopien für einen Retry, einen
  Rate-Limit-Treffer oder einen Lock-Konflikt tragen den Header nicht,
  und eine Nachricht, die der Broker nach einer Zustellung ohne
  Zurückstellung zurückgibt (Requeue nach den Prüfungen, Absturz;
  erkannt an `x-delivery-count` oder `x-acquired-count` auf
  Quorum-Queues), wartet wieder die Basiswartezeit. Das ist kein Retry: `max_retries`
  bleibt unberührt, `TASK_RETRYING` feuert nicht. Jede Zurückstellung
  loggt das neue WARNING-Event `task_deferred_backend_unavailable`
  (Felder `gate`, `stage`, `defer_count`, `delay_ms`, `error_type`,
  `error`; siehe Geändert).
  Nimmt auch der Broker die Kopie nicht an, reiht der Worker wie bisher
  an derselben Stelle neu ein und loggt `task_defer_failed_redelivered`.
  Zurückgestellt wird nur bei einem Ausfall: Eine Fehlerantwort von
  Valkey (`BackendResponseError`, siehe Hinzugefügt) wird wie bisher an
  derselben Stelle neu eingereiht, ein bleibender Fehler landet also
  weiter über das Zustelllimit in `{queue}.XQ`, statt endlos
  zurückgestellt zu werden. `ValkeyRateLimitStore.check` wirft bei
  Verbindungs- und Timeout-Fehlern und bei Fehlerantworten, die durch
  Warten heilen (siehe `BackendResponseError` unter Hinzugefügt), jetzt
  `BackendConnectionError`, bei anderen Fehlerantworten
  `BackendResponseError` (vorher den rohen
  `valkey.exceptions.ResponseError`). Ein
  `RateLimitStore` von Dritten bekommt die Zurückstellung nur, wenn sein
  `check` bei Transportfehlern `BackendConnectionError` wirft; jede
  andere Ausnahme behält das Neueinreihen an derselben Stelle.
- **Prüfungen vor der Ausführung hängen nicht mehr an einem Backend,
  das nicht antwortet.** Die Rate-Limit-Prüfung, das Lock-Acquire, die
  Dedup-Prüfung und der Claim eines Schedule-Fires sowie das Lesen des
  Ergebnisses eines abgebrochenen Workflow-Schritts hatten kein eigenes
  Zeitlimit. Antwortete Valkey nicht mehr, ohne die Verbindung zu
  schließen (ein Blackhole, etwa ein verlorener Netzpfad), wartete die
  Zustellung auf den Socket-Timeout des Valkey-Clients (10 s je
  Versuch, dazu dessen Wiederholungen). Diese Aufrufe sind jetzt durch
  `SWARMQ_BACKEND_OP_TIMEOUT` (Standard 5 s) begrenzt; ein Timeout gilt
  als nicht erreichbares Backend, die Nachricht wird wie oben
  beschrieben zurückgestellt. Die Ergebnis-Benachrichtigung nach einem
  Cancel vor dem Start und das Abschließen oder Freigeben eines
  Schedule-Fire-Claims sind genauso begrenzt; ein Timeout dort loggt
  `publish_notification_failed` bzw. `schedfire_finalize_failed`, und
  der Worker macht weiter. Läuft ein Lock-Acquire in den Timeout,
  nachdem Valkey es ausgeführt hat, bleibt der Lock bis zu seiner TTL
  gehalten, wie zuvor bei einem Socket-Timeout.
- **Ein Scheduler-Fire überschreibt keine Zeitplanänderung mehr, die
  während des Fires passiert.** Ein `swarmq schedule remove`
  (deaktivieren oder `--permanent`) oder ein Redeploy mit neuer Cron
  zwischen Poll und Commit wurde rückgängig gemacht: Der Zeitplan war
  wieder aktiv, wieder angelegt oder hatte wieder die alte Cron. Der
  Commit greift jetzt nur, solange der Zeitplan noch den gefeuerten
  Score hat; `sched_commit.lua` prüft das atomar (neues optionales
  `ARGV[5]`, abwärtskompatibel), und `ScheduleStore.commit_fire` hat das
  neue optionale Keyword `fired_ts_ms`. Ein Zeitplan, der zwischen Poll
  und Commit entfernt und mit demselben nächsten Fire-Zeitpunkt neu
  angelegt wurde, ist vom gefeuerten nicht zu unterscheiden; dort greift
  der Commit.
- **Ein abgebrochener Worker-Start lässt den eingebetteten Scheduler
  nicht mehr weiterlaufen.** Ein Abbruch von `start_worker()` oder ein
  Fehler nach dem Start des Schedulers (Executor-Start,
  Queue-Deklaration, `WORKER_ONLINE`) ließ dessen Election- und
  Poll-Tasks weiterlaufen; der Prozess erneuerte den
  Scheduler-Leader-Lock, und kein anderer Worker wurde Leader.
- **Eine volle Log-Queue hält den loggenden Code nicht mehr an.** Bei
  blockiertem stderr schrieb der Aufrufer — im Event-Loop — die
  Drop-Meldung selbst, und der Loop stand sekundenlang still (2,9 s
  gemessen; ohne Leser von stderr unbegrenzt). Bei geschlossenem stderr
  warf ein Logaufruf `ValueError` in den Anwendungscode. Der Aufrufer
  zählt jetzt nur noch.
- **Ein werfender Handler, Filter oder Formatter tötet den
  Listener-Thread nicht mehr.** Danach wurde jeder Record still
  verworfen, und das Leeren beim Prozessende hing für immer. Dasselbe
  galt für die Queue-Handler einer `--log-config`-Datei, wo eine volle
  begrenzte Queue zusätzlich einen Traceback im Aufrufer ausgab. Fehler
  werden jetzt gefangen und gezählt (`log_handler_errors`), und ein
  toter Listener startet mit dem nächsten Record neu.
- **Das Leeren der Log-Queues kann keinen Prozess mehr hängen lassen.**
  Ein blockiertes stderr oder ein langsamer Handler ließ jedes Leeren so
  lange dauern wie das Ziel: das Ende jedes SwarmQ-Prozesses, den
  Supervisor vor einem Reload (seine Worker waren schon gestoppt) und
  ein Rechenkind, dessen Ergebnis dann nie ankam. Jedes Leeren ist jetzt
  durch `log_drain_timeout` begrenzt. Records in einer Queue, deren
  Listener nicht läuft, werden innerhalb der Frist geschrieben oder
  danach als verworfen gezählt; bis das Leeren zurückkehrt, startet kein
  Record mehr einen Listener, den das Leeren sofort wieder aufgäbe.
- **`SwarmQ.__aexit__` mit `log_queue=True` hält den Event-Loop nicht
  mehr an**, während die Opt-in-Queue geleert wird (bis zu 12 s gemessen
  mit einem langsamen Handler). Wird es beim Warten abgebrochen, geht
  der Abbruch sofort weiter, und das Leeren läuft im Hintergrund bis zur
  Frist weiter.
- **`SwarmQ.__aexit__` mit `log_queue=True` wirft nicht mehr, wenn der
  Default-Executor des Event-Loops schon heruntergefahren ist**
  (`loop.shutdown_default_executor()`, wie `asyncio.run` es am Ende
  tut). Das Leeren warf `RuntimeError: Executor shutdown has been
  called` in deinen Code; jetzt wartet es auf einem eigenen Thread,
  wie bisher begrenzt durch `log_drain_timeout`.
- **CPU-Pool mit Opt-in-Log-Queue:** Ein `swarmq.*`-Record, der beim
  Import geloggt wurde, während `SwarmQ(log_queue=True)` oder
  `SWARMQ_LOG_QUEUE=1` aktiv war (auch durch zweimaliges
  `setup_dishka()`), startete im Forkserver einen Listener-Thread. Der
  Worker verweigerte dann den Start mit Exit `78` und einem Hinweis, der
  eine BLAS-Rechnung beschuldigte. Der Forkserver schreibt solche
  Records jetzt synchron.
- **CPU-Pool mit eigenem logging-`QueueHandler`:** Ein beim Import
  gestarteter `QueueListener` ließ den Worker mit demselben irreführenden
  Hinweis nicht starten; einer, der im `--configure`-Hook startete, lief
  in den Rechenkindern nie, sodass alle ihre Zeilen verloren gingen,
  obwohl `logging_reconfigured_by_app` gemeldet wurde. SwarmQ startet ihn
  jetzt in jedem Kind, und die Startmeldung nennt den Listener der App.
- **Das Logging im CPU-Pool entsprach weder dem IO-Pool noch der
  Dokumentation** (`basicConfig` gewann gegen `--log-format json`,
  `--log-config` und `--log-level`); siehe Geändert (breaking).
- **Ein still neu gestarteter Forkserver wurde nie auf Threads
  geprüft.**
- **Hot-Reload mit `--log-config`:** Die Datei wurde im
  `SIGHUP`-Signal-Handler geprüft, was die Überwachung bis zu 30 Sekunden
  einfror (tote Worker wurden nicht ersetzt, Autoskalierung und
  Kontrollkanal pausierten). Ein `SIGTERM` während der Prüfung wurde zu
  einem Neustart statt zum Herunterfahren, ein zweites `SIGHUP` startete
  eine zweite Prüfung parallel, und der Datei-Watcher löste während der
  Prüfung erneut aus. Eine Prüfung, die in ihre Frist lief oder per
  Ctrl-C abbrach, wurde als ungültige Konfiguration gemeldet. Die Prüfung
  läuft jetzt außerhalb des Signal-Handlers in einer eigenen
  Prozessgruppe; mehrere `SIGHUP`s führen zu einer Prüfung.
- **`SIGTERM` beendet den Supervisor immer.** Ein `SIGHUP`, das kurz
  nach einem `SIGTERM` eintraf, startete den Supervisor neu, statt ihn
  herunterzufahren — in jedem Deployment, mit oder ohne `--log-config`.
  Auch ein `SIGTERM` während des Worker-Stopps für einen Reload wird
  jetzt beachtet, und eines, das ein anderer Thread des Supervisors kurz
  vor dem Neuausführen annimmt, bleibt anhängig und beendet den neuen
  Supervisor.
- **Fork aus Task-Code:** Nach `os.fork()` verloren die Queue-Handler
  einer `--log-config`-Datei im Kind jeden Record (mit begrenzter Queue
  gab jeder Record einen `Logging error` aus). Ein Fork, während ein
  Listener schrieb, konnte im Kind bei jedem Record
  `RuntimeError: reentrant call` hinterlassen. Das Kind bekommt jetzt
  frische Queues, und der Fork wartet bis zu 50 ms auf laufende
  Schreibvorgänge. Eine `multiprocessing`-Queue (oder ein Manager-Proxy)
  bleibt mit dem Elternprozess geteilt: Das Kind startet keinen eigenen
  Listener, die Records des Kindes schreibt der Listener des
  Elternprozesses. Unter rohem `os.fork()` startet SwarmQ den
  Feeder-Thread der Queue im Kind neu, und ein Manager-Proxy baut eine
  eigene Verbindung zum Manager auf, statt die des Elternprozesses
  mitzubenutzen. Auf ein Ziel, das belegt bleibt, wartet nur der erste
  Fork, nicht jeder weitere erneut.
- **Records, die beim Herunterfahren des Interpreters entstehen** (etwa
  „Future exception was never retrieved“ oder aus einem `__del__`),
  lösten einen `--- Logging error ---` aus und gingen verloren; sie
  werden jetzt synchron geschrieben.
- **Eine `multiprocessing`-Queue in einer `--log-config`-Datei beim
  Prozessende:** Die Exit-Funktion von `multiprocessing` läuft meist vor
  der von SwarmQ und schloss die Pipe der Queue, während der Listener
  noch aus ihr las. Noch nicht gelesene Records gingen verloren, und der
  Listener starb mit einem `EOFError`/`OSError`-Traceback — immer wieder
  neu gestartet, oft Hunderte Tracebacks. SwarmQ leert diese Listener
  jetzt, bevor `multiprocessing` die Queue schließt. Ein Record, der
  danach noch kommt (etwa aus einer `atexit`-Funktion, die vor dem
  Import von `multiprocessing` registriert wurde), gilt ohne Traceback
  als verworfen, und eine Queue, die schon geschlossen ist, wenn das
  Leeren ihre Records nach Ablauf der Frist verwirft, wirft kein
  `EOFError`/`OSError` mehr aus `shutdown_logging()`.

## [0.8.0] — 2026-10-06

### Geändert (breaking)

- **`SwarmQ()` konfiguriert kein Logging mehr.** Eine `SwarmQ`-App lässt
  Root-Handler, Root-Level und die structlog-Konfiguration unberührt
  (bisher installierte sie einen eigenen Root-Handler und rief
  `structlog.configure` auf). SwarmQ loggt über benannte Logger
  `swarmq.<modul>` in das stdlib-`logging`: Der Event-Name ist die
  Nachricht des Records, die Felder sind `LogRecord`-Attribute. Ganz ohne
  Logging-Konfiguration erreichen WARNING und höher weiterhin Pythons
  `lastResort`-Handler — SwarmQ installiert absichtlich keinen
  `NullHandler`. Eine App mit Root-Level WARNING, die sich für ihre
  Logausgabe auf `SwarmQ()` verlassen hat, sieht SwarmQs INFO-Events
  nicht mehr. **Migration:**
    - Reine stdlib-App: Logging selbst konfigurieren, z. B.
      `logging.basicConfig(level=logging.INFO)`, mit einem Formatter, der
      die Felder zeigt — ein JSON-Formatter, der die Attribute des Records
      liest, oder `swarmq.logging.build_formatter(json=True, colors=False)`.
    - structlog-App: `structlog.stdlib.ExtraAdder()` in die
      `foreign_pre_chain` deines `ProcessorFormatter` aufnehmen. Ohne ihn
      gehen SwarmQs Felder stillschweigend verloren.
    - `log_level` und `log_json` von `SwarmQConfig` bleiben als Felder
      erhalten, steuern aber nur noch die neue Opt-in-Log-Queue
      (`log_level` setzt das Level des Loggers `swarmq`, `log_json` das
      Format der Drop-Meldung).

    Rezepte: [Logging-Referenz](logging.md#einbindung-in-deine-app).

- **Vertrag der Logausgabe.** Jede Zeile aus Prozessen, die
  `swarmq worker` startet, trägt neben `event`, `level` und `timestamp`
  den neuen Schlüssel `logger`. Umbenannte Events:
  `swarmq.dedup.detected` → `dedup_detected`,
  `swarmq.dedup.rollback_failed` → `dedup_rollback_failed`,
  `publish_notification failed` → `publish_notification_failed`,
  `publish_workflow_notification failed` →
  `publish_workflow_notification_failed` (Feld `wf_id` → `workflow_id`),
  `health_server_started`/`_stopped`/`_request_failed` und
  `metrics_server_started`/`_stopped`/`_request_failed` →
  `http_server_started`/`http_server_stopped`/`http_server_request_failed`
  mit dem Feld `server` (`health_server`/`metrics_server`). In
  `watch_dir_inside_venv_or_stdlib` heißt das Feld `module` jetzt
  `module_name`; allgemein bekommt ein Feld, dessen Name mit einem
  `LogRecord`-Attribut kollidiert, das Präfix `swarmq_` (`name` →
  `swarmq_name`). Events, die eine Exception melden, tragen `error_type`
  und `error`; einen Traceback gibt es nur bei unerwarteten Fehlern.
  Das gilt jetzt auch für `task_sent_to_dlq` (`error_type` ist der
  Klassenname der Exception, die den Task in die DLQ geschickt hat,
  `WorkerHungError` beim Erreichen des Delivery-Limits).
  Alarme und Dashboards, die Logs per grep auswerten, müssen angepasst
  werden.
- **Log-Level folgen einer Regel** (ERROR: Datenverlust oder ein
  fehlgeschlagener Task bzw. Prozess; WARNING: Degradierung, die sich
  selbst heilt; INFO: Lebenszyklus von Prozess und Task; DEBUG:
  Diagnose). `check_backend_failed` und `check_broker_failed` INFO →
  WARNING; `task_sent_to_dlq` WARNING → ERROR; Fälle, die eine TTL heilt,
  ERROR → WARNING: `shutdown_lock_release_failed`,
  `scheduler_stop_failed`, `lock_release_failed`,
  `cancel_check_backend_error`, `workflow_cancel_check_backend_error`,
  `cancel_store_result_failed`, `schedfire_dedup_backend_error`.
  `worker_died` ist INFO bei sauberem Ende oder erwartetem
  Herunterfahren, sonst WARNING.
- **Logging-Einstellungen werden einmal im Supervisor aufgelöst.**
  Priorität: CLI-Flag > Env-Var > `SwarmQConfig` > Default. Der
  Supervisor gibt die aufgelösten Einstellungen an seine
  Worker-Subprozesse und Rechenkinder weiter, die das Environment nicht
  mehr lesen; ein Flag gewinnt jetzt in jedem Prozess.

- **CLI-Optionen schlagen jetzt in allen Prozessen die `SWARMQ_*`-Umgebung,
  für alle Optionen von `swarmq worker`.** Bisher gewann in den
  Worker-Prozessen die Umgebung bei `--queues`, `--concurrency`, `--pool`,
  `--burst`, `--burst-idle-timeout`, `--priorities`, `--metrics-port` und
  `--health-port` (z. B. schlug `SWARMQ_CONCURRENCY=5` die Option
  `--concurrency 20`), während der Supervisor die Option nutzte. Jetzt wird
  jede Option einmal als CLI > Umgebung > Default aufgelöst, und Supervisor,
  Worker-Prozesse, Rechenkinder und `--check` sehen dieselben Werte. Ohne
  Flag gilt die Umgebungsvariable weiterhin.

### Hinzugefügt

- **Logging-Optionen für `swarmq worker`:** `--log-config PATH`
  (`SWARMQ_LOG_CONFIG`; eine `dictConfig`-Datei als JSON, YAML mit
  PyYAML; `disable_existing_loggers` ist ohne Angabe `false`; einmal vom
  Supervisor geprüft und vor einem Reload erneut, der mit
  `log_config_reload_failed` abbricht, wenn die Datei inzwischen ungültig
  ist), `--no-configure-logging` (`SWARMQ_CONFIGURE_LOGGING=false`),
  `--logger-level NAME=LEVEL` (wiederholbar; `SWARMQ_LOGGER_LEVELS`),
  `--log-queue-size N` (`SWARMQ_LOG_QUEUE_SIZE`, Vorgabe 10000),
  `--log-lifecycle-level LEVEL` (`SWARMQ_LOG_LIFECYCLE_LEVEL`, Vorgabe
  DEBUG) und `SWARMQ_LOG_FORMAT` (gewinnt gegen den Alias
  `SWARMQ_LOG_JSON`). `CRITICAL` ist für jedes Level erlaubt. Ungültige
  Werte oder widersprüchliche Kombinationen (`--log-config` mit einem
  ausdrücklichen Format oder mit `--no-configure-logging`) beenden den
  Lauf mit Exit 78 und genau einer Zeile `worker_config_error`; eine
  fehlerhafte Konfigurationsdatei wirft den neuen `LoggingConfigError`.
- **Lebenszyklus-Events der Tasks** über den Logger `swarmq.task`:
  `task_received` und `task_started` (Lebenszyklus-Level, Vorgabe DEBUG),
  `task_succeeded`, `task_failed` und `task_cancelled` (INFO, mit
  `duration_ms`; `task_failed` mit `error_type`/`error`; `compute_child`
  bei Ergebnissen aus dem CPU-Pool). Abschalten mit
  `--logger-level swarmq.task=WARNING`.
- **Task-Kontext an jedem Event innerhalb eines Tasks:** `task_id`,
  `task_name`, `queue`, `worker_id`, dazu neu `workflow_id` (wenn gültig)
  und `attempt` (`retry_count + 1`) sowie `compute_child` in
  Rechenkindern. Der Kontext wird pro Nachricht gebunden und danach
  wiederhergestellt; Kontextvariablen deiner App werden nicht mehr
  gelöscht.
- **Opt-in-Log-Queue für eingebettete Apps:**
  `SwarmQConfig(log_queue=True)` / `SWARMQ_LOG_QUEUE=true` leitet
  `swarmq.*`-Events über den Hintergrund-Thread `swarmq-log-listener` an
  die Root-Handler der App, sodass langsame Handler den Event-Loop nicht
  mehr blockieren. Standardmäßig aus.
- **Regel „App gewinnt“:** Ein Task-Modul oder `--configure`-Hook, das
  die Root-Handler ersetzt, behält sie; SwarmQ leitet seine eigenen
  Events über seine Queue dorthin und loggt `logging_reconfigured_by_app`
  (und schaltet `swarmq.*`-Logger wieder ein, die die `dictConfig` der
  App deaktiviert hat). Hinzugefügte Handler meldet es als WARNING
  `logging_handlers_added_by_app`.
- **Verworfene Logeinträge werden sichtbar:** Eine volle Log-Queue
  verwirft, statt zu blockieren (WARNING und höher behalten 10 %
  Reserve), meldet `log_records_dropped` höchstens einmal pro Sekunde
  (im JSON-Modus als JSON-Zeile) und zählt die Verluste in der Metrik
  `swarmq_log_records_dropped_total`.
- **Neue Events** `worker_connection_error`, `worker_recycle_ack_timeout`
  (Exit 75), `log_config_reload_failed`, `logging_reconfigured_by_app`,
  `logging_handlers_added_by_app`, `log_records_dropped`, jeweils als
  strukturierte Zeile im konfigurierten Format.
- **Öffentliche Logging-API** in `swarmq.logging`: `get_logger`,
  `build_formatter`, `configure_logging`, `shutdown_logging`,
  `load_log_config` und `LoggingConfigError`. Neue Referenzseite:
  [Logging](logging.md).
- **Weitere Exceptions direkt aus `swarmq` importierbar:**
  `LoggingConfigError`, `SoftTimeoutError` und `PublishIndeterminate`
  (dieselben Klassen wie in `swarmq.exceptions`).

### Deprecated

- `swarmq.logging.setup_logging` (Einstiegspunkte nutzen
  `configure_logging`) und `swarmq.logging.flush_logging` (jetzt ein
  Alias für `shutdown_logging`).

### Behoben

- **Hot-Reload funktioniert nach `python -m swarmq.cli`.** Der Supervisor
  startete sich mit `sys.argv` neu; nach `-m` steht dort der Pfad von
  `swarmq/cli/__main__.py`, und das neue Abbild führte die Datei als Skript
  aus — ohne Arbeitsverzeichnis in `sys.path` (`--module ... cannot be
  imported`) und mit Workern, die per spawn in der Bootstrapping-Phase
  scheiterten. Der Neustart nutzt jetzt `sys.orig_argv`, das `-m` und
  Interpreter-Flags behält; `swarmq/cli/__main__.py` ruft `main()` nur noch
  als Hauptmodul auf. Der Start über das Kommando `swarmq` ist unverändert.

### Abhängigkeiten

- `structlog` ist jetzt auf `>=24,<27` begrenzt: `swarmq.logging.get_logger`
  hängt von der Aufruftiefe von `structlog.stdlib.BoundLogger` ab, die die
  Testsuite für 26.x festhält.

## [0.7.0] — 2026-09-22

### Hinzugefügt

- **Kontrollkanal für Worker-Gruppen.** `swarmq worker --max-processes N`
  schaltet ihn ein und erklärt den Spielraum; `--group LABEL` benennt die
  Gruppe (Vorgabe: die sortierte Queue-Menge). Ein Sollwert
  `{"min": M, "max": K}` unter `sq:control:setpoint:<gruppe>` wird vor dem
  ersten Kindprozess und danach alle 5 Sekunden gelesen: Eine höhere
  Untergrenze startet Worker sofort, eine niedrigere Obergrenze beendet die
  überzähligen sanft, ein gelöschter Schlüssel stellt die Startargumente
  wieder her, die feste Form `N..N` schaltet die Autoskalierung ab.
  Sollwerte über dem Spielraum, mit verdrehten Grenzen, unlesbare und
  Bereiche auf Gruppen ohne Autoskalierung werden mit Grund abgelehnt; ein
  Sollwert lässt den Start nie scheitern. Jeder Supervisor meldet unter
  `sq:control:status:<gruppe>:<supervisor>` mit Verfallsdatum seine
  wirksamen Grenzen samt Herkunft, den zuletzt gelesenen Sollwert, einen
  Ablehnungsgrund und seine Slot-Zahlen, sodass ein steuernder Dienst
  „abgelehnt", „noch nicht angekommen" und „Backend nicht erreichbar"
  unterscheiden kann. Slot-Tabelle, Beobachtungs-Portbereiche und (mit
  `--pool cpu`) die Voreinstellung der Rechenslots richten sich beim Start
  nach dem Spielraum; die Vorflugprüfung lehnt einen Spielraum unter
  `--processes` oder der `--autoscale`-Obergrenze ab, ebenso einen, der die
  zugeteilten CPUs überbuchen würde. Ohne `--max-processes` ändert sich
  nichts. Siehe [CLI-Referenz](cli.de.md#kontrollkanal).

## [0.6.2] — 2026-09-15

### Geändert

- **README aktualisiert.** Die Paketbeschreibung auf PyPI führte unter „What's
  planned next“ Features, die längst ausgeliefert sind (Priorities, Expiry,
  Burst, Supervisor, Autoscaling, Metriken, Health, Hot Reload, Agent-Spec).
  Sie beschreibt jetzt den aktuellen Funktionsumfang, den CPU-Worker-Modus,
  die Supervisor-Vorgaben und die zwei verbliebenen Roadmap-Punkte.
- **Abhängigkeiten aktualisiert (2026-09-15).** Geprüft gegen aio-pika 10.0.1
  und aiormq 7.0.0 (pamqp 4), structlog 26.1, valkey mit libvalkey 4.1,
  msgpack 1.2, prometheus-client 0.26, watchfiles 1.2; die deklarierten
  Untergrenzen bleiben unverändert, bestehende Installationen laufen weiter.
  Entwicklungswerkzeuge: mypy 2.3, ruff 0.16 (neue Regel `RUF036`, `None`
  am Ende von Typ-Unions), pytest 9.1, hypothesis 6.168. Unit-,
  Integrations-, e2e-, Property- und Contract-Suiten sind mit dem neuen
  Stand grün.

### Behoben

- **Worker-Subprozesse sterben mit ihrem Supervisor, auch bei SIGKILL.** Ein
  hart beendeter Supervisor (Test-Teardown, OOM-Killer, `docker kill`) ließ
  seine Worker als Waisen weiterlaufen; sie erneuerten den Leader-Lock des
  Schedulers per Heartbeat und blockierten jeden späteren Leader in derselben
  Datenbank. Das Ziel des Kindprozesses setzt jetzt `PR_SET_PDEATHSIG` auf
  SIGTERM (Linux), sodass der Tod des Supervisors denselben Graceful Shutdown
  auslöst wie ein geordneter Stopp; ein Kind, dessen Supervisor zwischen
  Spawn und Setzen starb, beendet sich, statt als Waise anzulaufen. Ohne
  `prctl` bleibt das Verhalten unverändert.

## [0.6.1] — 2026-09-15

### Behoben

- **`TASK_REJECTED` trägt die Task-Identität, sobald der Body dekodiert
  wurde.** Für `reason="unregistered_task"` und
  `reason="middleware_chain_factory"` enthält das Signal jetzt `task_id` und
  `task_name`; bisher war `task_id` auf beiden Wegen `None`, obwohl der
  Worker die Nachricht schon dekodiert und die Kennung geloggt hatte. Ohne
  sie ließ sich eine abgewiesene Nachricht mit `swarmq dlq inspect` nicht im
  `.XQ` wiederfinden. `body_too_large` und `undecodable` melden weiterhin
  `None`, wie dokumentiert. Gefunden von der Lasttest-Suite am Pin v0.6.0
  (dort Backlog-Eintrag B14).
- **Der Supervisor hebt die geerbte Signalmaske auf, bevor er irgendeinen
  Worker-Entry ausführt.** `ProcessManager` blockiert SIGINT/SIGTERM/SIGHUP,
  während er Kinder startet, und `spawn` gibt diese Maske weiter. swarmqs
  eigener Kind-Entry hob sie auf; ein eigener `worker_entry`, den ein
  Betreiber dem `ProcessManager` übergibt, tat das nicht und sah das SIGTERM
  seines Shutdown-Watchers nie. Ein Graceful Stop endete deshalb erst mit
  dem SIGKILL der Orchestrierung nach dem vollen Stop-Timeout. Das Ziel des
  Kindprozesses hebt die Maske jetzt selbst auf, bevor der Entry läuft.
  Gefunden von der Lasttest-Suite (Backlog-Eintrag B15).

## [0.6.0] — 2026-09-14

### Geändert (breaking)

- **`worker_id` hat eine feste Form.** In jedem Signal, Metrik-Label und
  Logkontext ist die Worker-Identität `<basis>:<slot>` unter dem Supervisor
  und `<basis>:p<pid>` im eingebetteten `start_worker()`. `<basis>` ist
  `SWARMQ_WORKER_ID`, sonst der Hostname. Bisher war es eine undurchsichtige
  Objekt-ID, die sich mit jedem Neustart änderte. Gilt für `WORKER_*`,
  `SCHEDULER_*`, `WORKER_RECYCLING` (auch `kind="compute_child"`) und die
  Labels von `swarmq_worker_uptime_seconds` und `swarmq_scheduler_leader`.
  Dashboards können den Slot lesen; die Uptime beginnt beim Recycling eines
  Slots von vorn.
- **`--metrics-port` und `--health-port` sind Basisports.** Der Worker in
  Slot `s` bindet `basis + s`; Mehrprozess-Betrieb mit festem Port wird nicht
  mehr abgelehnt. Abgelehnt wird stattdessen ein Bereich `[basis, basis + N)`
  über 65535 hinaus oder eine Überschneidung von Metrik- und Health-Bereich.
  `N` ist `--processes` oder die Obergrenze von `--autoscale`. Prometheus
  braucht ein Target je Slot; `swarmq worker --check` gibt beide Bereiche
  aus (`metrics_ports`, `health_ports`).
- **Exit-Code 78 für Konfigurationsfehler, auf jeder Ebene.** Ein
  `ConfigurationError` oder `ModuleNotFoundError` im Elternprozess von
  `swarmq worker` erzeugt den strukturierten Logsatz `worker_config_error`
  und endet mit 78 (bisher 1). Endet ein Worker-Kind mit 78 — auch bei einem
  `ImportError` beim Import des Task-Moduls — endet der Supervisor ebenfalls
  mit 78 (bisher 1). Verbindungsfehler zu Broker oder Backend bleiben bei 1.
  Eine Orchestrierung kann jetzt „nicht wieder anfassen" von „gleich
  nochmal" unterscheiden.
- **Workflow-Schritte laufen durch die Enqueue-Middleware.** Jeder Schritt,
  den der `WorkflowAdvancer` publiziert, und jeder Schritt der Anfangsfront
  in `Client.apply` durchläuft vor dem Publish `pre_enqueue` und danach
  `post_enqueue`, in derselben Kette wie `Client.schedule`, mit
  `delivery_mode="at_least_once"` und den zusätzlichen Kontextschlüsseln
  `workflow_id` und `step_path`. `pre_enqueue` läuft für alle Schritte einer
  Front, bevor der erste publiziert wird; wirft es, wird kein Schritt dieser
  Front publiziert, der Workflow als gescheitert markiert (`error.type` =
  Name der Ausnahme) und die Nachricht bestätigt. Eine `pre_enqueue`, die
  Client-Kontext voraussetzt, kann jetzt Workflow-Schritte ablehnen.
- **Das Pseudo-Ereignis `<workflow>` entfällt.** `Client.apply` löst kein
  `TASK_ENQUEUED` mit `task_name="<workflow>"` mehr aus. Jeder Schritt löst
  `TASK_ENQUEUED` mit seiner echten Identität aus, gefolgt von
  `WORKFLOW_STEP_ENQUEUED`.

### Hinzugefügt

- **`swarmq worker --configure MODULE:CALLABLE`**: Das Callable erhält die
  `SwarmQ`-App nach ihrer Konstruktion und vor dem Start des Workers; ein
  Awaitable als Rückgabe wird abgewartet. Middleware und Signal-Handler
  registrieren, ohne einen eigenen Worker-Einstieg zu schreiben. Läuft genau
  einmal je Worker-Prozess, nie im Forkserver und nie in `schedule`, `dlq`
  oder `inspect`. Ein scheiternder Hook ist ein Konfigurationsfehler
  (Exit 78). `--check` prüft die Syntax `MODULE:CALLABLE` und die
  Auffindbarkeit des Moduls, ohne es zu importieren.
- **Signal `TASK_REJECTED`** mit `queue`, `reason`, `task_id`, `task_name`,
  `error` für jede Nachricht, die der Worker ohne Diagnose-Header an den
  Dead-Letter-Exchange des Brokers gibt. `reason` ist einer von
  `body_too_large`, `undecodable`, `unregistered_task`,
  `middleware_chain_factory`, `dlq_publish_failed`; `task_id` und
  `task_name` sind `None`, wenn der Body nie dekodiert wurde. Zähler
  `swarmq_tasks_rejected_total{queue,reason}`.
  `swarmq_tasks_total{outcome="dlq"}` zählt weiterhin nur `TASK_DLQ`. Der
  [Dead-Letter-Guide](../guides/dead-letter-queues.de.md#drei-wege-in-die-xq)
  benennt die drei Wege ins `.XQ`.
- **Signal `WORKFLOW_STEP_ENQUEUED`** mit `task_id`, `task_name`, `queue`,
  `workflow_id`, `step_path` nach jedem publizierten Workflow-Schritt,
  zusätzlich zu `TASK_ENQUEUED`.
- **`SWARMQ_WORKER_ID`** (auch `SwarmQ(worker_id=...)`) setzt die Basis der
  Worker-Identität; Standard ist der Hostname.
- **Slot-Tabelle im Supervisor.** Slots sind `0..N-1`. Der Ersatz für einen
  gestorbenen, hängenden oder recycelten Worker erhält denselben Slot und
  damit dieselben Ports; ein Scale-Up nimmt den kleinsten freien Slot, ein
  Scale-Down entfernt den jüngsten Worker nach Startzeit. Ein Slot mit
  erschöpftem Neustart-Budget wird gesperrt und nie nachgefüllt. Während
  eines rollierenden Ersatzes teilen Alt- und Neu-Prozess den Slot; der
  Autoscaler zählt ihn einmal. Der Ersatz bindet seinen Port mit
  Wiederholung; das Budget deckt das SIGKILL eines hängenden und die
  Portfreigabe eines recycelnden Vorgängers.

### Behoben

- **Verlorene Zustands-Updates von Workflows bei parallelen Zweigen.** Jede
  Schreibung des Workflow-Zustands durch den Advancer ist ein
  Compare-and-Set gegen die gelesene Version (Feld `version`; Dokumente aus
  0.5.x gelten als 0). Bei einem Konflikt liest der Advancer neu, übernimmt
  seine lokalen Änderungen (`step_tasks` additiv; `status`,
  `final_result_task_id` und `error` bei lokalem Terminalstatus) und
  schreibt erneut, bis zu 25 Runden; ein frischer Terminalstatus beendet
  den Advance ohne Schreibung. Oberhalb der Grenze wird die Nachricht neu
  zugestellt. Der bisher als `xfail` geführte Test zum verlorenen
  Schritt-Marker ist jetzt ein Regressionstest.
- **Fehlermarkierung unterscheidet Fehlertypen.** Scheitert die
  Fehlermarkierung eines Workflows an einem `BackendConnectionError`, wird
  die Nachricht (at-least-once) neu zugestellt statt bestätigt; jeder andere
  Fehler bestätigt und loggt mit Workflow-Kennung.
- **Konfigurationsfehler im Kind sind strukturierte Logsätze.** Der
  Fehlertext erscheint im konfigurierten Format (`human` oder `json`), nicht
  als rohe stderr-Zeile; der Supervisor loggt `worker_config_error`. Ein
  Kind, das mit 75 endet (Recycling ohne Bestätigung), wird als
  `worker_recycle_noack` geloggt und im Budget neu gestartet.
- **Der Log-Formatter druckt keine Tracebacks mehr.** Scheitert die
  Formatierung eines Records (etwa an einem werfenden `__repr__` in
  `record.args`), gibt der Formatter eine Zeile im konfigurierten Format mit
  `event="log_record_unformattable"`, Logger-Name, Level und Fehlertext aus.
- **Keine `RuntimeWarning` je Rechenkind.** Ein per
  `python -m paket.modul` gestarteter Worker im CPU-Modus importierte sein
  Hauptmodul in jedem Rechenkind erneut, jeweils mit einer `runpy`-Warnung.
  Der Forkserver-Preload registriert es jetzt einmal als `__main__`; der
  Kindstart bleibt bei etwa 5 ms.

## [0.5.1] — 2026-09-06

### Behoben

- Ein Fehler beim *Aufruf* von `run()` eines Tasks — falsche Argumente
  (`TypeError`) oder ein `run()`, das keine Coroutine ist — ist jetzt ein
  gewöhnlicher Task-Fehler und folgt dem Retry-/DLQ-Pfad. Bisher entkam die
  Exception dem Nachrichten-Handler, die Zustellung blieb unbestätigt, bis der
  Kanal geschlossen wurde.
- CPU-Modus: Der Forkserver-Preload enthält jetzt das Hauptmodul des
  Loop-Prozesses und die Laufzeit des Rechenkinds. Ohne sie importierte jedes
  frische Kind beides neu (etwa 60 ms je Task); der Kindstart liegt wieder bei
  etwa 5 ms wie in der Machbarkeitsstudie gemessen. `SlotPool.recent_spawn_ms`
  zeichnet Spawn- und Handshake-Zeiten zur Diagnose auf.

## [0.5.0] — 2026-09-03

### Geändert (breaking)

- **Der Supervisor ist Standard.** `swarmq worker` startet immer einen
  `ProcessManager` mit dem Worker als Subprozess, auch bei `--processes 1`.
  Zwei Prozesse statt einem; `SIGHUP` bedeutet jetzt Reload (wie bisher mit
  `--processes`); ein abstürzender Worker wird innerhalb des Budgets (5 pro
  Stunde) intern neu gestartet, bevor der Prozess endet — Alarme daher auf das
  Log-Ereignis `worker_died` statt auf Container-Neustarts. `--burst` endet
  weiterhin mit 0, sobald alle Worker leergelaufen sind.
- **Startprüfung für wirkungslose `FromDishka`-Annotationen.** Ein Parameter
  wie `db: FromDishka[Db] | None = None` hat nie injiziert und blieb still
  `None`; der Worker verweigert jetzt den Start mit einem `ConfigurationError`,
  der Task und Parameter nennt — in jedem Pool-Modus. `db: FromDishka[Db]` ohne
  `| None` und ohne Default deklarieren.
- **Konfigurationsfehler beenden den Lauf nach einem Versuch.** Ein
  Worker-Subprozess, der an seinen Startprüfungen scheitert, endet mit Code 78;
  der Supervisor startet ihn nicht neu und endet mit 1, statt das
  Neustart-Budget zu verbrennen.

### Hinzugefügt

- **CPU-Modus** (`--pool cpu`, `SWARMQ_POOL`): Task-Körper laufen in
  Rechenkindern aus einem vorgewärmten `forkserver`; der Loop-Prozess rechnet
  nie. `hard_timeout` und Cancel töten die Prozessgruppe des Kindes;
  `--concurrency` bedeutet Rechenslots und ist voreingestellt auf die dem
  Worker zugeteilten CPUs (Affinität und cgroup-Quote) geteilt durch
  `--processes`. Frisches Kind je Task ohne DI, wiederverwendetes Kind mit
  RSS-/Taskzahl-Recycling für `FromDishka`-Tasks. Siehe
  [CPU-Modus](../guides/cpu-mode.md).
- **Lebenszeichen zum Supervisor**: Ein Worker, dessen Event-Loop seinen
  Zeitstempel `SWARMQ_HEARTBEAT_TIMEOUT_S` lang (Standard 60 s) nicht mehr
  schreibt, wird ersetzt (SIGTERM, dann SIGKILL), mit eigenem Budget von
  2 × Delivery-Limit pro Stunde. Eine Nachricht, die Worker zum Hängen bringt,
  wird ab dem vorletzten Zustellversuch (`x-delivery-count`) vom Worker in
  die DLQ geführt — mit Fehlerergebnis, Workflow-Fail, Unique-Freigabe und
  `TASK_DLQ`, nicht stumm vom Broker.
- **Selbst-Recycling des IO-Workers** (`SWARMQ_RECYCLE_RSS_MB`,
  `SWARMQ_RECYCLE_MAX_TASKS`, Opt-in): Nach einem Task bittet der Worker den
  Supervisor um Ersatz, wartet auf die Bestätigung und drainiert ohne
  erzwungenen Abbruch.
- Signal `WORKER_RECYCLING` und Metrik `swarmq_recyclings_total{kind,reason}`.
- Neue Einstellungen: `SWARMQ_HEARTBEAT_INTERVAL_S`, `SWARMQ_HEARTBEAT_TIMEOUT_S`,
  `SWARMQ_CONSUMER_TIMEOUT_S` (Startwarnung, wenn ein `hard_timeout` darüber
  liegt), `SWARMQ_MALLOC_TRIM_THRESHOLD`, `SWARMQ_MALLOC_ARENA_MAX`,
  `SWARMQ_COMPUTE_THREAD_LIMIT`. `/ready` meldet im CPU-Modus den Forkserver;
  der Start warnt, wenn `queue_type` nicht `quorum` ist (kein Delivery-Limit).

## [0.4.1] — 2026-08-30

### Hinzugefügt

- **`SWARMQ_DELIVERY_LIMIT` / `SwarmQConfig.delivery_limit`** (Default `20`,
  unverändertes Verhalten) — das Zustellbudget, mit dem Quorum-Queues
  deklariert werden (`x-delivery-limit`). Ist eine Nachricht so oft zugestellt
  worden, wirft der Broker sie selbst in die Dead-Letter-Queue; das Budget ist
  das Sicherheitsnetz gegen einen Task, der den Prozess zuverlässig abschießt
  (SIGSEGV/OOM) und deshalb ewig neu zugestellt würde, ohne je `max_retries`
  zu erreichen.

  Der Default deckt drei bis vier fachliche Wiederholungen je Zustellung plus
  Puffer für Broker-Schluckauf. Dieser Puffer ist mit dem Publish-Deckel aus
  0.3.0 schmaler geworden: Bricht ein Publish im Worker an seiner Frist ab,
  wird die bearbeitete Nachricht neu zugestellt statt bestätigt — eine längere
  Broker-Störung verbraucht damit Zustellungen, die sie vorher nicht
  verbraucht hat. Höher setzen, wer längere Störungen aussitzen muss; niedriger,
  wer ein engeres Zustellfenster erzwingen will. Klassische Queues ignorieren
  die Einstellung, sie kennen das Argument nicht.

## [0.4.0] — 2026-08-30

### Geändert (breaking)

- **Eine `group`, deren Glied selbst eine `chain`, `group` oder ein `chord`
  ist, liefert keine gekürzte Ergebnisliste mehr.** Seit 0.2.0 fiel ein
  solches Glied ohne jede Fehlermeldung aus der Liste — `group(a, chain(b,
  c))` gab eine einelementige Liste zurück, während `handle.children()`
  gleichzeitig alle Schritte als abgeschlossen meldete. Derselbe Code baut
  die Eingabe eines Chord-Bodys, ein Chord über einem solchen Header hat
  also auf einer gekürzten Liste gerechnet, statt eine zurückzugeben.

  **Betroffene Versionen: 0.2.0 und 0.3.0.** Eine gekürzte Liste ist im
  Nachhinein von einer vollständigen nicht zu unterscheiden — die
  gespeicherten Ergebnisse geben das nicht her. Beantwortbar ist nur die
  Frage nach der Workflow-*Definition*: **betroffen ist jeder Lauf, dessen
  Topologie eine `group` oder einen Chord-Header mit einem Glied enthält,
  das selbst `chain`, `group` oder `chord` ist.** Wer solche Workflows
  einsetzt, muss davon ausgehen, dass frühere Ergebnisse falsch waren — und
  alles, was daraus gerechnet wurde.

- **Eine unvollständige Gliederliste ist jetzt ein Fehler, für jede Gruppe.**
  Wo die Zusammenstellung ein fehlendes Glied bisher wortlos übersprang,
  wirft sie nun `WorkflowValidationError` mit Gruppenpfad und beiden Zahlen.
  Der Guard sitzt am gemeinsamen Fan-in **aller** Gruppen und Chord-Header,
  auch der flachen — ein aus anderem Grund verlorener Schritt-Marker (etwa
  im publish-then-mark-Fenster) scheitert damit laut, wo er vorher ein
  Ergebnis lieferte. Genau das ist die Absicht: Ein falsches Ergebnis, das
  man nicht erkennen kann, ist teurer als ein sichtbarer Fehler.

- **Verschachtelte Workflows vor dem Upgrade leerfahren.** Ein Workflow, der
  über das Upgrade hinweg läuft, trifft auf den neuen Guard und scheitert,
  statt sein (falsches) gekürztes Ergebnis zu liefern. Das gilt je Fleet,
  nicht je Prozess: Solange nicht jeder Worker die neue Version fährt, kann
  ein alter Worker weiterhin still kürzen.


## [0.3.0] — 2026-08-30

**Nie auf PyPI veröffentlicht.** Getaggt, trug aber
noch die stillschweigend gekürzte `group`-Ergebnisliste, die 0.4.0 behebt —
die Veröffentlichung hätte einen wissentlich falsch rechnenden Stand
ausgeliefert. Der Eintrag bleibt fürs Protokoll; von 0.2.0 direkt auf 0.4.0
gehen und beide Abschnitte lesen.

### Geändert (breaking)

- **`swarmq worker` verweigert Mehrprozess-Betrieb mit festem
  Beobachtungs-Port.** Werden mehr als ein Worker-Prozess angefordert
  (`--processes N` mit N > 1 oder `--autoscale MIN,MAX` mit MAX > 1) und ist
  zugleich ein Metrik- oder Health-Port wirksam, scheitert jetzt der Start,
  statt still unterbesetzt weiterzulaufen. Jedes Kind band denselben Port,
  sodass nur das erste überlebte und die übrigen ihr Restart-Budget
  aufbrauchten. Geprüft wird der aufgelöste Wert: `SWARMQ_METRICS_PORT` und
  `SWARMQ_HEALTH_PORT` werden genauso abgelehnt wie die Optionen, und
  `swarmq worker --check` meldet denselben Konflikt. Entweder den festen
  Port im Mehrprozess-Betrieb weglassen oder einen einzelnen Prozess damit
  fahren — einen Port je Kindprozess gibt es noch nicht.
- **Neue Ausnahme `PublishIndeterminate`** (Unterklasse von
  `BrokerConnectionError`, bestehende Handler bleiben unberührt) für einen
  Publish, dessen Frist beim Warten auf die Broker-Bestätigung riss. In genau
  diesem Fall bleibt eine `unique_for`-Dedup-Sperre **stehen**, statt
  freigegeben zu werden: Ein Wiederholversuch wird abgewiesen, bis die Sperre
  abläuft, statt den Task ein zweites Mal auszuführen.
- **`Publisher` hat `set_publish_deadline` bekommen.** Eigene Broker, die
  `swarmq.contracts.Publisher` implementieren, sollten die Methode
  ergänzen; der Worker ruft sie über eine Schutzabfrage, eine
  Implementierung ohne sie fährt also weiterhin sauber herunter, bekommt
  aber die an das Herunterfahren gekoppelte Publish-Frist nicht.

### Hinzugefügt

- **`SWARMQ_PUBLISH_TIMEOUT_S` / `SwarmQConfig.publish_timeout_s`**
  (Default `120.0`) — Obergrenze für einen kompletten Publish inklusive
  interner Wiederholversuche. Ab Werk aktiv. Wird sie überschritten,
  scheitert der Publish mit `BrokerConnectionError` und dem ausdrücklichen
  Vorbehalt, dass die Zustellung unbestimmt ist, statt endlos auf eine
  Bestätigung zu warten, die der Transport nie liefert. Beim Herunterfahren
  gilt die kürzere aus diesem Wert und der verbleibenden Gnadenfrist.

### Behoben

- **Ein Publish konnte auf einem still toten Transport unbegrenzt hängen.**
  Es fliegt keine Ausnahme und der Kanal gilt weiter als offen, der
  bestehende Wiederholpfad griff also nie. In einem Lasttestlauf wurden aus
  einer fünfsekündigen Broker-Störung 62 Minuten Stillstand, die sich per
  SIGTERM nicht beenden ließen.
- **Jede JSON-Logzeile trug ihren Inhalt zweimal.** Die formatter-internen
  Arbeitsschlüssel (`_record`, `_from_structlog`) wurden gerendert statt
  entfernt — doppelte Logmenge und von Hand kaum lesbar. Die Textausgabe
  trug sie ebenfalls und verliert sie mit.

## [0.2.0] — 2026-08-27

### Geändert (Breaking — 0.2.0)

- **`ValkeyBackend`-Legacy-Methodenaliase entfernt.** Es gelten die
  Vertragsnamen — z. B. `setnx_unique` → `claim_unique`,
  `get_results_batch` → `get_results`, `publish_notification` →
  `notify_result`, `subscribe_and_wait` →
  `wait_for_result`/`wait_for_workflow`; Schedule-/Schedfire-Primitive
  liegen im Store `backend.schedules` (`zrangebyscore_schedules` →
  `schedules.poll_due`, …). Semantik, Key-Layout und Lua-Verträge sind
  unverändert (Golden-Fixtures). Migrationstabelle im
  0.2.0-Bruch-Inventar.
- **`RabbitMQBroker`-Admin-/Verb-Aliase entfernt.** `get_queue_depth` →
  `queue_depth`, `get_dlq_depth` → `dead_letter_depth`,
  `list_dead_letter_queue` → `list_dead_letters`, `inspect_dead_letter_message` →
  `inspect_dead_letter`, `retry_dead_letter_message` → `retry_dead_letter`,
  `retry_all_dead_letter_messages` → `retry_all_dead_letters`,
  `purge_dead_letter_queue` → `purge_dead_letters`; die Zustellungs-Verben
  heißen `redeliver`, `route_via_dlx`, `retry_later` und `dead_letter`
  (die letzten beiden bestätigen das Original selbst — separates `ack`
  entfällt).
- **Konstruktoren sind gegen die Verträge** in `swarmq.contracts`
  typisiert (`Worker`, `Client`, `SwarmQ`). `RabbitMQBroker`/
  `ValkeyBackend` implementieren sie; bestehender Code läuft unverändert.
- **Interne Modulstruktur:** `worker`, `client`, `scheduler` und
  `process_manager` sind jetzt Pakete; Klassen-Re-Exports wie
  `swarmq.locking.LockManager`, `swarmq.limits.RateLimiter` und
  `swarmq.workflow_orchestration.ChordManager` liegen kanonisch unter
  `swarmq.backend.valkey.*` (nicht Teil der Public-Oberfläche; hier
  gelistet, weil die CLI-Surface-Whitelist sie pinnt).

### Entfernt (Breaking — 0.2.0)

- **`ValkeyBackend.create_pubsub()`.** Stattdessen `wait_for_result` /
  `watch_progress` / `wait_for_workflow`; die Kanalnamen (`sq:notify:…`)
  bleiben für eigene Clients stabil.

### Behoben (0.2.0)

- **Band-Erhalt beim Retry.** Retries, Rate-Limit- und Lock-
  Rückstellungen zielen jetzt auf das Verzögerungs-/Dead-Letter-Ziel der
  **Zustellungs-Queue** — ein Retry aus `tasks.high` bleibt in
  `tasks.high`, auch wenn der Nachrichten-Body die Basis-Queue nennt
  (bisher ging das Band bei Fremd-Producer-Nachrichten verloren).
  Abgesichert durch `tests/e2e/test_retry_band.py`. Dies ist die einzige
  Verhaltensänderung der 0.2.0-Restrukturierung.

### Intern (0.2.0)

- **Vertrags-Rückgrat.** Explizite Protokolle in `swarmq.contracts`
  (Publisher/Consumer/Broker, BrokerAdmin, ResultBackend, LockStore,
  RateLimitStore, ChordStore, ScheduleStore) mit Batch-Publish-
  Andockstellen für den geplanten Bulk-Fast-Path; ein In-Memory-Stub
  in der Test-Suite implementiert dieselben Verträge, und eine
  Contract-Test-Suite läuft identisch gegen Stub, RabbitMQ und Valkey.
- **Paket-Zerlegung.** Die vier früheren Monolith-Module sind Pakete
  (`worker/`, `client/`, `scheduler/`, `process_manager/`), mit
  CI-erzwungenen Struktur-Guards (Modulgröße, Import-Zyklen,
  Import-Reinheit der Kernpakete).
- **Projektweites mypy strict.** Beide Legacy-Override-Blöcke sind
  weg; jede verbleibende `# type: ignore` trägt einen Fehlercode.

### Behoben (0.2.0, nach dem Inventar)

- **Fehlerergebnisse abgelaufener Tasks werden jetzt wirklich
  gespeichert.** Das Expiry-Gate übergab dem Result-Store ein dict
  statt kodierter Bytes; der SET scheiterte gegen echtes Valkey still
  und `get_result()` eines abgelaufenen Tasks hing bis zum Timeout
  statt `TaskError("expired")` zu liefern.

### Rollout (0.2.0)

- Gleiche Version im gesamten Worker-Fleet ausrollen.
- Keine Valkey-Migration: Key-Layout, Lua-Verträge, Channels, Header
  und Queue-Argumente sind unverändert (Golden-Fixtures).
- In-flight-0.1.2-Nachrichten bleiben gültig; ein 0.2.0-Worker
  konsumiert von 0.1.2 deklarierte Queues ohne `PRECONDITION_FAILED`.
- Rollback ist ein Redeploy von 0.1.2.

## [0.1.2] — 2026-06-07

- `py.typed`-Marker ausgeliefert, damit Type-Checker das Paket als typisiert sehen.

## [0.1.1] — 2026-06-07

- sdist per Whitelist auf `src/` getrimmt; `libvalkey` ins `fast`-Extra; Readme deklariert.

## [0.1.0] — 2026-06-07

Erstes PyPI-Release. Alles Folgende war darin enthalten.

### Performance

- **`import swarmq.cli` lädt `valkey` und `aio_pika` nicht mehr eager.**
  Der Package-Init in `swarmq/__init__.py` und die CLI-Fassade
  auflösen Public-Surfaces (`SwarmQ`, `ValkeyBackend`,
  `RabbitMQBroker`, `Metrics`, `HealthChecker`, `Middleware`,
  `Signal`, `Task` u.a.) jetzt lazy via PEP-562 `__getattr__`. Die
  Identität bleibt beim ersten Zugriff erhalten — `from swarmq
  import ValkeyBackend` liefert weiterhin das kanonische Klassen-
  Objekt, keine API-Änderung für Konsumenten. Median-`importtime`-
  Cumulative-Latency für `import swarmq.cli` sinkt von ~188 ms auf
  ~100 ms (Best Case ~78 ms). Betrifft `swarmq --help`,
  `swarmq worker --check` und jede Agent-Introspektion, die den
  CLI-Namespace ohne die schweren Backends importiert.

### Verhaltensänderungen

- **`--format`-Default ist jetzt TTY-bewusst (CLI-04).** Für
  Subcommands mit `--format`-Flag (`dlq list`, `dlq inspect`,
  `dlq retry`, `dlq purge`, `schedule list`, `schedule add`,
  `schedule remove`, `schedule status`, `inspect`) ist der
  Standard-Output jetzt `table` bei einem TTY und `json` bei
  einer Pipe/Umleitung. Vorher war der Default stets `table`.

  **Skripte, die auf dem alten Piped-Table-Default aufbauten**
  können das Altverhalten ohne Code-Änderung mit
  `SWARMQ_DEFAULT_FORMAT=table` wiederherstellen. Explizite
  `--format=table`-Flags funktionieren unverändert. Ein
  explizites `--format=…` auf der Kommandozeile schlägt die
  Env-Var immer.

  Ungültige `SWARMQ_DEFAULT_FORMAT`-Werte (Tippfehler, unbekanntes
  Format, leerer String) werden stillschweigend ignoriert — der
  CLI fällt auf den TTY-basierten Default zurück statt zu
  fehlen. Siehe [CLI-Output](configuration.md#cli-output).

### Hinzugefügt — Phase 5 Welle 2 (Dev Experience)

- **R5.3 Hot Reload** (`pip install swarmq[reload]`). `--reload`
  startet einen `watchfiles`-gestützten Daemon-Thread, der bei
  `.py`-Änderungen unter dem Task-Modul-Verzeichnis `SIGHUP` an
  den Supervisor feuert, was die bestehende R4.6-Reload-Pipeline
  (Drain → execvpe) triggert. Neue Flags: `--reload`, `--watch-dir
  PATH` (wiederholbar). Filter ignoriert `__pycache__`, `*.pyc`,
  Editor-Noise (`4913`, `*.swp`, `*.bak`, `*.tmp`). Loggt
  `hot_reload_enabled`-WARNING bei jedem Start — Developer-Only-
  Feature. Siehe [guides/hot-reload](../guides/hot-reload.md).

### Hinzugefügt — Phase 5 Welle 1 (Operations)

- **R5.1 Prometheus-Metriken** (`pip install swarmq[metrics]`).
  Sieben Metriken mit `task_name`/`queue`/`outcome`-Labels:
  `swarmq_tasks_total`, `swarmq_task_duration_seconds`,
  `swarmq_tasks_active`, `swarmq_queue_depth`,
  `swarmq_worker_uptime_seconds`, `swarmq_retries_total`,
  `swarmq_dlq_messages`. Stdlib-only HTTP-`/metrics`-Endpoint.
- **R5.2 Health- und Ready-Endpoints.** `GET /health` (Liveness,
  immer 200 solange Event-Loop lebt), `GET /ready` (200, wenn
  Broker + Backend erreichbar, 503 mit sanitisierter JSON-Body
  andernfalls). `HealthChecker`, `HealthServer`, `serve_health`
  als öffentliche API.
- **Auto-Wire.** `SwarmQConfig.metrics_port` / `health_port` (oder
  `--metrics-port` / `--health-port` auf `swarmq worker`) aktivieren
  beide Server + Signal-basierte Instrumentierung ohne Python-
  Entrypoint-Boilerplate.

### Hinzugefügt — Phase 4 (Operator-Features)

- **R4.3 Task-Priorities.** `TaskInfo(priority="high"|"normal"|"low")`
  routet zu `{queue}.{priority}`-Suffixen. Worker deklariert das
  Triplet automatisch, wenn `priorities=True` (oder `--priorities`).
  Warnt beim Start, wenn Tasks Nicht-Normal-Priority deklarieren,
  die Worker-Config aber Priorities deaktiviert hat.
- **R4.4 Task-Expiry.** `TaskInfo(max_age=seconds)` + der
  `x-swarmq-enqueued-at`-Header wird beim Publish gestempelt
  (Client + Scheduler). Worker routet abgelaufene Nachrichten
  vor Rate-Limit-/Lock-/pre_execute-Stages mit `TASK_EXPIRED`-
  Signal + `TaskError("task expired: …")`-Ergebnis in die DLQ.
- **R4.5 Burst-Mode.** `swarmq worker --burst` exited mit 0,
  sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren.
  `SwarmQConfig.burst_mode` / `burst_idle_timeout` +
  `SWARMQ_BURST_MODE` / `SWARMQ_BURST_IDLE_TIMEOUT`-Env-Vars.
- **R4.6 ProcessManager.** `swarmq worker --processes N` (Alias
  `-p`) lässt N Worker-Subprozesse unter einem Supervisor laufen,
  der gecrashte Worker automatisch neu startet (Rolling-Window-
  Limit: 5 Restarts pro Stunde pro Worker). `spawn`-Context hält
  aio-pika- + Valkey-Verbindungen subprozess-scoped. SIGHUP
  triggert `os.execvp()`-basierten Reload nach graceful Drain;
  SIGTERM / SIGINT triggern denselben 3-Phasen-Shutdown
  (kooperatives Event → SIGTERM → SIGKILL) wie ein Single-
  Process-Worker.
- **R4.7 Autoscaling.** `swarmq worker --autoscale=MIN,MAX`
  skaliert die Worker-Anzahl auf Basis der Queue-Tiefe. Scale-Up,
  wenn Pending-Tasks `50 * active` übersteigen; Scale-Down nach
  60s sustained Idle; 30s Cooldown zwischen Entscheidungen.
  Queue-Tiefe per neuer `RabbitMQBroker.get_queue_depth()`
  (passives `declare(..., passive=True)` auf Main + `.DQ`).

### Hinzugefügt — Phase 3 (Komfort-Features)

- **R3.5 Unique Tasks.** `TaskInfo(unique_for=seconds,
  unique_until="start"|"completion")` dedupt auf kanonisch
  kodierten Args. Owner-Check via Lua für TTL-Expiry-Race-Schutz.
  Sowohl `Client.schedule`- als auch `Client.schedule_many`-Pfade.
- **R3.6 Task-Cancellation.** `Client.cancel(task_id)` setzt ein
  Valkey-Flag, das der Worker pre-pickup und während der
  Ausführung checkt. Dediziertes `TASK_CANCELLED`-Signal. Cancels
  propagieren als `TaskCancelledError` via `get_result`.
- **R3.7 Progress Tracking.** `Task.update_progress(current,
  total, message)` auf 500ms pro Instanz gedrosselt mit
  Force-Flush bei `current == total`. `Client.get_progress` +
  `Client.watch_progress` AsyncIterator streamt Updates via
  Pub/Sub mit HASH-Fallback bei Disconnect. ETA via linearer
  Extrapolation.
- **R3.10 DLQ-CLI.** `swarmq dlq list|inspect|retry|purge` für
  Operator-Recovery. Drain-and-Republish-Scan-Pattern, Exit-Codes
  0 (Success), 2 (Config), 3 (task_id nicht gefunden).
- **Embedded Scheduler.** Cron + Delayed (`delay=` / `eta=`) +
  Recurring mit Leader-Election-Failover. Graceful Handover ohne
  TTL-Wartezeit.
- **Full CLI.** `swarmq worker`, `swarmq schedule`, `swarmq inspect`
  — kein Python-Entrypoint für typische Deployments nötig.

### Hinzugefügt — E2E-Hardening (separate Initiative)

- **Chaos-Suite** (`tests/chaos/`): toxiproxy + Container-Restart-
  Abdeckung für Worker-SIGKILL, Broker/Backend-Restart, Netzwerk-
  Chaos (Latency/Bandwidth/Packet-Loss/Full-Down), Clock-Skew,
  Message Corruption, Byzantine Workers.
- **Property-based Tests** (`tests/property/`): Hypothesis gegen
  Dedup-Hash, Progress, Serialization, Backoff, Rate-Limiter.
- **Fuzz** (`tests/fuzz/`): Byte-Fuzz auf Message-Parser +
  AMQP-Header- / Pub/Sub-External-Publisher-Fuzz.
- **Stress** (`tests/stress/`): 10-Worker × 1000-Task exact-once,
  50-Parallel-Cancel-Winner, 200-Parallel-Progress-Throttle.
- **Long-Soak** (`tests/soak/`): 1h-Opt-in-Leak-Detection-Harness
  mit psutil-RSS- / FD- / Valkey-Conn- / Rabbit-Channel-Zählern.
- **Drei-Tier-CI** (GitHub Actions): pre-merge ≤ 5 min, nightly
  Chaos+Stress+Fuzz ≤ 30 min, weekly Mutation+Soak ≤ 4h.

### Reliability-Hardening (ce-review Follow-ups)

- **RabbitMQBroker** Channel-Recreate nach Connection-Reset;
  `_publish_with_retry`-Bounded-Retry-Wrapper erhält asyncio-
  Task-Level-Cancellation.
- **Quorum-Queue `x-delivery-limit=20`** auf Main-Queue — pathologische
  SIGSEGV-Loops terminieren jetzt in der DLQ statt in endloser
  Redelivery.
- **ValkeyBackend** Retry-Policy + `health_check_interval` auf dem
  darunterliegenden valkey-py-Client.
- **Client.watch_progress** re-subscribed bei Pub/Sub-Disconnect.
  valkey-py re-subscribed nicht automatisch.
- **Task.update_progress** asyncio.Lock um den 500ms-Throttle-
  Read-Compare-Write — raste zuvor unter `asyncio.gather`.
- **DLQ-Drain-Republish** routet durch `_publish_with_retry` —
  transiente Channel-Fehler lassen gedrainte Nachrichten nicht
  mehr stumm fallen.
- **HTTP-Oberflächen** (`/metrics`, `/health`, `/ready`) durch
  `asyncio.timeout(5.0)` begrenzt, abdeckend Request-Line +
  Header-Drain + 200-Line-Cap. Binäre Request-Lines geben 400
  zurück, keinen Log-Spam. Default-Bind `127.0.0.1`. `/ready`-
  Error-Body sanitisiert, um Host:Port-Leak zu verhindern.

## [0.x] — frühere Phasen

Phase 1 (minimaler Roundtrip) + Phase 2 (Retries, DLQ, Logging,
Graceful Shutdown) + Phase 3 Core-Komfort-Features (Middleware,
Signals, Rate-Limit, Locking) landeten im selben Branch wie die
Punkte oben. Einzelne Commit-History in git.
