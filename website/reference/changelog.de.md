# Changelog

Alle nennenswerten Änderungen an SwarmQ. Dieses Projekt folgt
[Keep a Changelog](https://keepachangelog.com/) und
[Semantic Versioning](https://semver.org/).

## [0.12.2] — unveröffentlicht

Sammelrelease für die P2- und P3-Befunde der zweiten Fehlersuche;
zusammen mit 0.12.1rc1 sind damit alle 23 Befunde behoben. Es behebt
die übrigen 19 und den Rest von FS-06, darunter eine eigene
`TimeoutError` des Tasks, die zu `SoftTimeoutError` wurde (FS-05,
FS-20), Locks und Unique-Flags, die eine verlorene Valkey-Antwort oder
ein Timeout zurückließ (FS-06, FS-07), einen Publish, der auf einem
toten Kanal hing (FS-08), DLQ-Kopien, die bei einem Replay per UI
sofort wieder in der DLQ landeten (FS-10), Unique-Flags, die
`unique_until="start"` und `unique_for=None` blockierten (FS-13,
FS-14), Enkelprozesse, die ein CPU-Kind überlebten (FS-16), und einen
Recycling-Drain, der Langläufer abbrach (FS-30). Keine
Konfigurationsänderungen und keine neuen Nachrichten-Header; 0.12.2
läuft neben 0.12.0 und 0.12.1 auf denselben Queues und Keys. Die
Upgrade-Hinweise unten lesen: Recycling, Scheduler-Leader und DLQ
verhalten sich anders.

### Upgrade-Hinweise

1. **Längerer Recycling-Drain.** Kann auf einem recycelnden Worker ein
   Task ohne `hard_timeout` laufen (deklariert auf einer Queue, die er
   konsumiert, oder dort bereits gestartet), wartet der Recycling-Drain
   jetzt bis zu `max(größter hard_timeout + 30 s,
   SWARMQ_CONSUMER_TIMEOUT_S)`, im Default 30 min, statt
   `SWARMQ_SHUTDOWN_TIMEOUT`. So lange behält der alte Prozess seinen
   Slot und seinen Speicher. `SWARMQ_CONSUMER_TIMEOUT_S` gleich dem
   `consumer_timeout` des Brokers halten; bei einem Broker ohne
   Consumer-Timeout wird auch ein gesunder Langläufer nach
   `SWARMQ_CONSUMER_TIMEOUT_S` abgebrochen. Ein `SIGTERM` während eines
   Recycling-Drains setzt jetzt ab diesem Signal das Shutdown-Budget in
   Kraft; ein Deploy wartet also nicht auf den Drain
   ([Shutdown-Budget](../explanation/delivery-semantics.md#shutdown-budget)).
2. **`unique_for=None` mit `delay`/`eta` braucht Producer und Leader
   auf 0.12.2.** Ein Producer bis 0.12.1 speichert den Opt-out nicht,
   und ein Leader bis 0.12.1 fällt auf seine eigene `TaskInfo` zurück;
   in beiden Fällen kann der Oneshot beim Fire weiter als Duplikat
   verworfen werden. Producer laufen oft in anderen Diensten: auch diese
   aktualisieren ([`schedule()`](api.md#schedule)).
3. **DLQ-Einträge älterer Worker.** DLQ-Kopien von 0.12.2-Workern
   tragen kein `x-delivery-count`/`x-acquired-count` und lassen sich per
   "Move messages" in der RabbitMQ-UI oder per Shovel zurückspielen.
   DLQ-Einträge von Workern bis 0.12.1 und Nachrichten, die der Broker
   selbst über den Dead-Letter-Exchange ablegt (`route_via_dlx`), können
   die Zähler noch tragen; für sie `swarmq dlq retry` verwenden, es
   entfernt sie
   ([was den Ack abbricht](../explanation/delivery-semantics.md#was-den-ack-abbricht)).
4. **Der Scheduler-Leader tritt vor Ablauf seines Locks zurück.** Hängt
   Valkey, tritt der Leader jetzt vor Ablauf seiner Lock-TTL zurück
   (neue Warnung `scheduler_renew_deadline_stepdown`, Signal
   `SCHEDULER_STANDBY`), statt erst nach etwa 150 s. Ein Renew ist jetzt
   auf `SWARMQ_BACKEND_OP_TIMEOUT` begrenzt, und ein Renew-Timeout zählt
   als Renew-Fehler (`scheduler_renew_error`, `error_type=TimeoutError`).
   Nebenwirkung: Handler für `SCHEDULER_HEARTBEAT` laufen im selben
   Wahl-Takt ohne Zeitlimit. Braucht ein Handler länger als etwa 10 bis
   15 s (Defaults), tritt ein gesunder Leader zurück und holt die Rolle
   im nächsten Takt zurück (`SCHEDULER_STANDBY`, danach
   `SCHEDULER_LEADER_FAILOVER` ohne echten Wechsel). Diese Handler kurz
   halten.
5. **Neue und geänderte Log-Events.** Neu:
   `scheduler_renew_deadline_stepdown`, `unique_release_backend_error`,
   `consumer_watch_detached`, `publish_channel_closing_unreadable`,
   `compute_signal_denied` und `worker_recycling_superseded_by_shutdown`.
   `lock_release_failed` kommt jetzt auch aus dem Lock-Gate, mit dem
   Feld `stage="acquire_timeout"`; `task_deferred_backend_unavailable`
   kann `stage="cancel_fail_workflow"` tragen, `shutdown_step_timeout`
   kann `step="cancel_consume"` tragen und
   `cancel_consume_partial_failure` kann `late=true` tragen.
   `scheduler_unique_release_failed` und `dedup_rollback_failed`
   decken jetzt auch die Freigabe nach einem Claim-Timeout ab. Alerts,
   die auf diese Namen prüfen, ggf. anpassen
   ([Event-Katalog](logging.md#event-katalog)).
6. **Mischbetrieb mit 0.12.0 und 0.12.1.** Das Lock-Skript
   `lock_acquire.lua` wird unter einer neuen SHA geladen; alte und neue
   Worker schließen sich auf demselben Mutex oder Semaphor weiter
   gegenseitig aus. Nachrichten und Schedule-Einträge älterer Versionen
   laufen unverändert auf 0.12.2 und umgekehrt.

### Behoben

- **FS-05, FS-20: Eine `TimeoutError` des Tasks bleibt eine
  `TimeoutError`.** Bisher wurde jede `TimeoutError` aus `run()` zu
  `SoftTimeoutError`, auch ohne `soft_timeout` (Meldung „soft timeout
  after None s“), und `retry_exceptions=[TimeoutError]` griff nie; das
  CPU-Kind machte es genauso. Jetzt wird nur der Ablauf von
  `soft_timeout` selbst zu `SoftTimeoutError`; der eigene Timeout des
  Tasks behält Klasse, Meldung und `error_class`. Die Doku versprach
  einen `SoftTimeoutError`, den `run()` fangen kann; im Task kommt der
  Soft Timeout aber als `asyncio.CancelledError` an. In `finally` oder
  in `except asyncio.CancelledError:` aufräumen und erneut werfen; ein
  Task, der den Abbruch verschluckt, liefert ein Erfolgsergebnis
  ([`SoftTimeoutError`](errors.md#softtimeouterror)).
- **FS-06 (Unique-Claim, Mutex): Ein Lock oder Unique-Claim übersteht
  die Wiederholung des Valkey-Clients nach einer verlorenen Antwort.**
  Der wiederholte Aufruf hielt den eigenen Lock bzw. das eigene Flag für
  fremd: Ein Mutex sperrte sich bis zu seiner TTL selbst aus, und
  `schedule()` gab eine ID zurück, ohne zu publizieren. Jetzt wertet
  `lock_acquire.lua` den eigenen Token als erworben (Mutex und
  Semaphor, TTL unverändert), und `schedule()`, `schedule_many()` und
  das Cron-Fire des Leaders werten ein Flag mit ihrer frisch erzeugten
  `task_id` als eigenes. Ein Oneshot aus `schedule(delay=/eta=)` behält
  seine stabile ID; dort bleibt der Selbsttreffer ein Duplikat, weil er
  sich nicht vom Folge-Poll nach `PublishIndeterminate` unterscheiden
  lässt.
- **FS-07: Ein Claim wird nach einem Timeout freigegeben.** Ein
  Valkey-Timeout nach erfolgreichem Lock-Erwerb oder Unique-Claim ließ
  Lock bzw. Flag bis zur TTL stehen. Jetzt gibt das Lock-Gate den Lock
  im Hintergrund frei (owner-geprüft; der Shutdown wartet darauf), der
  Leader gibt das Unique-Flag eines Crons frei, und
  `schedule()`/`schedule_many()` geben das Flag frei, bevor der Fehler
  beim Aufrufer ankommt. Scheitert die Freigabe, loggt swarmq
  `lock_release_failed` (`stage="acquire_timeout"`) bzw.
  `unique_release_backend_error`. Ein Oneshot unter stabiler ID behält
  sein Flag, wie bei FS-06.
- **FS-08: Ein Publish, der in der Kanal-Sperre wartet, endet beim
  Verbindungsabriss.** Bisher hing er bis `publish_timeout_s` auf dem
  toten Kanal und meldete dann `PublishIndeterminate`, obwohl nichts
  gesendet war. Jetzt läuft jeder Versuch gegen das Schließen seines
  Kanals: Nicht gesendete Versuche gehen auf einen neuen Kanal oder
  enden als `BrokerConnectionError`. Reißt die Frist, bevor etwas
  gesendet sein kann (Warten auf die Verbindung, Backoff nach einem
  nachweislich nicht gesendeten Versuch, Warten in der Kanal-Sperre),
  folgt ebenfalls `BrokerConnectionError` („nicht gesendet“). Beim
  Leader zählt das wie jeder Verbindungsfehler für den Rücktritt
  ([`BrokerConnectionError`](errors.md#brokerconnectionerror)).
- **FS-09: Der Consumer-Wächter bricht `basic.cancel` und
  `basic.consume` nicht mehr ab.** Er brach sie nach 2 s ab, und
  aiormq schloss daraufhin den ganzen Consume-Kanal mit allen
  unbestätigten Zustellungen des Workers. Jetzt ist nur der Kanalbezug
  begrenzt; das RPC selbst hat in aiormq eine Notbremse von 60 s. Beim
  Stoppen wird ein laufender Takt abgekoppelt statt abgebrochen
  (`consumer_watch_detached`), und ein danach eintreffender
  Consumer-Tag wird sofort wieder abgemeldet. `cancel_consume` im
  Shutdown ist begrenzt, ohne das RPC abzubrechen.
- **FS-10: DLQ-Kopien tragen keine Zustellzähler des Brokers mehr.**
  Eine Kopie mit `x-delivery-count`/`x-acquired-count` landete nach
  einem Replay per RabbitMQ-UI, Shovel oder swarmv sofort wieder in der
  DLQ. Jetzt entfernt jede Kopie, die swarmq publiziert, die Zähler;
  Replay-ID und Generation bleiben gleich. Nachrichten, die der Broker
  selbst dead-lettert (`route_via_dlx`), liegen außerhalb von swarmq
  (Upgrade-Hinweis 3).
- **FS-12: Ein Workflow-Schritt, der vor dem Start abgebrochen wird,
  beendet den Workflow.** `cancel(task_id)` auf einen noch nicht
  gestarteten Schritt ließ den Workflow bis zur TTL `running`. Jetzt
  ist der Workflow `failed`, und `get()` wirft `TaskError` („task was
  cancelled“). Scheitert die Markierung vorübergehend, wird die
  Nachricht über die Delay-Queue zurückgestellt
  (`task_deferred_backend_unavailable`,
  `stage="cancel_fail_workflow"`)
  ([Fehlerfälle](../guides/workflows.md#fehlerfalle)).
- **FS-13: `unique_until="start"` gibt das Flag auf jedem terminalen
  Pfad vor dem Start frei.** Eine Nachricht, die vor dem Start ihres
  Tasks endete (Cancel, Ablauf über `max_age`, Ende einer
  Lock-Wartezeit, Delivery-Limit, Task nicht registriert), hielt das
  Flag bis `unique_for`. Jetzt wird es auch dort freigegeben, weiterhin
  genau einmal; eine Zurückstellung (Rate-Limit, Lock-Wartezeit) behält
  das Flag
  ([Wann das Flag freigeben](../guides/locking-and-dedup.md#wann-das-flag-freigeben)).
- **FS-14: `schedule(delay=/eta=, unique_for=None)` schaltet die Dedup
  ab.** Der Opt-out wurde nicht gespeichert, der Leader deduplizierte
  mit seiner eigenen `TaskInfo` und konnte den Oneshot still verwerfen.
  Jetzt hält der Oneshot-Eintrag den Opt-out fest, und der Leader
  feuert ohne Dedup-Claim (Upgrade-Hinweis 2;
  [Immediate vs Deferred](../guides/scheduling-and-retries.md#immediate-vs-deferred)).
- **FS-15: `get_result()` hängt nicht mehr in Pub/Sub.** `subscribe()`
  und der Teardown hatten keine Frist; ein hängendes Valkey hielt den
  Aufrufer über `timeout` hinaus fest. Jetzt sind beide an die
  Restfrist gebunden; ohne Abonnement pollt der Aufruf weiter.
  `WorkflowHandle.get()` wartet genauso. Am Polling selbst ändert sich
  nichts.
- **FS-16, FS-23, FS-24: CPU-Pool.** Nach dem Tod eines Kindes (Ende in
  der `SIGTERM`-Gnadenfrist, Tod ohne Ergebnis, Tod eines warmen
  DI-Kinds zwischen zwei Jobs) geht ein `SIGKILL` an seine
  Prozessgruppe; Enkelprozesse überleben nicht mehr. Lässt sich die
  Gruppe nicht signalisieren, loggt der Pool `compute_signal_denied`.
  Ein im Hintergrund (Recycling) gesperrter Slot wird nicht mehr
  vergeben. Ein gescheiterter Handshake loggt keinen asyncio-Fehler
  („exception in shielded future“) mehr und zählt nicht mehr als
  zweiter Kindtod.
- **FS-17: Ein abgelaufener Task, dessen Ack nach der DLQ-Kopie
  scheitert, ist terminal.** Der `max_age`-Pfad wertete
  `SettleFailedAfterPublish` als Fehlschlag und stellte das Original
  neu zu; mit Ausführungsschutz blieben irreführende Logzeilen und ein
  Unique-Flag bis `unique_for`. Jetzt verhält er sich wie jeder andere
  Dead-Letter-Pfad: Warnung `dlq_ack_failed_after_publish`,
  Unique-Freigabe, kein `nack`
  ([Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)).
- **FS-19: Kaputte Zähler-Header blockieren keine Nachricht mehr.** Ein
  nicht ganzzahliger `x-swarmq-retry-count` (schon `"3"`) oder ein nicht
  numerischer `x-swarmq-rate-limit-count` ließ die Nachricht
  unbestätigt. Ein nicht ganzzahliger Retry-Zähler zählt jetzt wie kein
  Header (ein UI-Replay mit `"3"` bekommt wieder das volle
  Retry-Budget); der Rate-Limit-Zähler wird nachsichtig gelesen.
- **FS-27: `validate_namespace("SQ")` schlägt nicht mehr den verbotenen
  Wert `'sq'` vor.**
- **FS-28: Die Meldung für einen Workflow-Schritt mit `result_ttl=0`
  nennt die Ursache.** Sie lautete „is FIRE_AND_FORGET“; jetzt „stores
  no result (result_ttl=0)“.
- **FS-30: Recycling bricht Langläufer nicht mehr nach
  `SWARMQ_SHUTDOWN_TIMEOUT` ab.** Ohne registrierten `hard_timeout` fiel
  der Recycling-Drain auf das Shutdown-Timeout (30 s) zurück und brach
  bei jedem Recycling jeden längeren Task ab. Jetzt wartet der Drain bis
  zu `SWARMQ_CONSUMER_TIMEOUT_S` (Upgrade-Hinweis 1). Ein `SIGTERM`
  während des Recycling-Drains oder während der Worker auf die
  Bestätigung des Supervisors wartet
  (`worker_recycling_superseded_by_shutdown`) setzt das Shutdown-Budget
  in Kraft; ein zweites `SIGTERM` startet es nicht neu.
- **FS-31: Der Scheduler-Leader tritt vor Ablauf seiner Lock-TTL
  zurück.** Ein Renew lief ohne `op_timeout` (bei hängendem Valkey bis
  etwa 65 s), und der Rücktritt kam erst nach etwa 150 s. Jetzt ist
  jeder Renew begrenzt, und der Leader tritt zurück, sobald die
  Zeitgrenze ohne erfolgreichen Renew erreicht ist (Upgrade-Hinweis 4;
  [Leader-Übernahme](../guides/scheduling-and-retries.md#leader-ubernahme)).
- **Nachtrag zu FS-03:** Eine Retry-Kopie eines Cron- oder
  `delay`/`eta`-Fires trägt `x-swarmq-schedfire-defer-count` nicht mehr.
  Mit dem Zähler wurde die Kopie bei fremdem Claim zurückgestellt statt
  als Duplikat bestätigt, und ihr Zurückstellungsdeckel begann mit dem
  alten Stand.

## [0.12.1rc1] — 2026-10-14

Release-Candidate für 0.12.1 (Installation mit `pip install --pre`).
Er behebt die drei P1-Befunde der zweiten Fehlersuche und einen Teil
eines vierten: einen Publish nach verstrichener Shutdown-Endfrist, der
einen Task doppelt ausführte (FS-02), verlorene Cron- und
`delay`/`eta`-Fires nach einem Worker-Absturz (FS-03), Locks und
Concurrency-Slots, die ein scheiternder DI-Finalizer blockierte
(FS-04), und einen Fire-Claim, den eine Client-Wiederholung verlor
(FS-06, Teil Fire-Claim). Keine Konfigurationsänderungen; 0.12.1 läuft
neben 0.12.0 auf denselben Queues. Die Upgrade-Hinweise unten befolgen.

### Upgrade-Hinweise

1. **Surge-Deploy.** Die neuen Worker starten, bevor die alten
   stoppen (Kubernetes `maxSurge` ≥ 1 mit `maxUnavailable: 0`, oder die
   neuen Container zuerst starten). Die Korrektur von FS-03 wirkt nur
   auf dem Worker, der die Neuzustellung empfängt: Ein 0.12.0-Worker
   bestätigt eine zurückgestellte Fire-Kopie weiter als Duplikat,
   solange der Claim gehalten wird, und das Fire geht wie bisher
   verloren.
2. **Grace-Period des Orchestrators mindestens
   `SWARMQ_SHUTDOWN_TIMEOUT`** (empfohlen
   `SWARMQ_SHUTDOWN_TIMEOUT + 15 s` wie in
   [Schritt 3 der Deployment-Anleitung](../guides/deployment.md#schritt-3-grace-period-des-orchestrators)).
   Ein `SIGKILL` vor Ende des Budgets schneidet die Lock-Freigabe und
   die letzten Publishes ab, und ein gerade laufendes Cron-Fire wird
   zum Absturzfall aus FS-03.
3. **Mischbetrieb mit 0.12.0.** Zurückgestellte Fire-Kopien tragen den
   neuen Header `x-swarmq-schedfire-defer-count`; 0.12.0 ignoriert ihn.
   Nach einem Rollback bestätigt ein 0.12.0-Worker eine solche Kopie als
   Duplikat, solange der Claim gehalten wird, und führt sie aus, sobald
   der Claim abgelaufen ist. Nachrichten von 0.12.0 laufen unverändert
   auf 0.12.1.
4. **Klassische Queues:** Die Korrektur von FS-03 erkennt eine
   Neuzustellung an den Zustellzählern des Brokers, die nur
   Quorum-Queues (der Default) führen. Auf einer klassischen Queue wird
   die erste Neuzustellung nach einem Absturz weiter als Duplikat
   bestätigt, das Fire geht dort wie bisher verloren.
5. **Dead-Letter-Queue:** Eine zurückgestellte Fire-Kopie, die in der
   DLQ landet, behält den Defer-Zähler; `swarmq dlq retry` entfernt
   ihn, die Wiederholung beginnt also mit frischem Backoff.
   DLQ-Einträge von 0.12.0 tragen keinen Zähler und laufen wie bisher.

### Behoben

- **FS-02: Ein Publish, dessen Shutdown-Endfrist schon verstrichen ist,
  sendet nichts.** Bisher sendete swarmq trotzdem und meldete
  `PublishIndeterminate`; Retry- bzw. DLQ-Kopie plus Requeue des
  Originals führten den Task doppelt aus. Das geschah am Ende eines
  Drains, beim Recycling systematisch nach etwa 23 s Drain. Jetzt
  scheitert der Publish mit `BrokerConnectionError` („nicht gesendet“),
  ohne den Kanal anzufassen. Ein vom Drain abgebrochener Task endet
  weiter mit einem Fehlerergebnis, aber Retry- und DLQ-Kopie gehen
  nicht mehr raus: Das Original wird neu zugestellt, ohne einen Retry
  zu verbrauchen und ohne DLQ-Eintrag. Beim Recycling kann sich das bei
  jedem Recycling wiederholen, begrenzt durch das Delivery-Limit. Die
  Publish-Endfrist eines Recyclings folgt jetzt seinem tatsächlichen
  Drain-Ende, das Aufräumbudget beginnt danach neu. Reißt die Frist
  erst während des Backoffs oder des Kanalaufbaus eines späteren
  Versuchs, bleibt es bei `PublishIndeterminate`. Ein Fire, das der
  Leader nach der Endfrist nicht mehr senden kann, zählt für seinen
  Rücktritt
  ([was den Ack abbricht](../explanation/delivery-semantics.md#was-den-ack-abbricht)).
- **FS-03: Ein Cron- oder `delay`/`eta`-Fire geht nicht mehr verloren,
  wenn sein Worker nach dem Claim stirbt (SIGKILL, OOM).** Bisher fand
  die Neuzustellung des Brokers den In-Flight-Claim des toten Workers
  und wurde als Duplikat bestätigt. Jetzt stellt der Worker eine solche
  Neuzustellung mit wachsendem Backoff in die Delay-Queue zurück
  (`schedfire_redelivery_deferred`) und markiert die Kopie mit
  `x-swarmq-schedfire-defer-count`. Die Kopie wird bestätigt, sobald
  das Fire abgeschlossen ist, und läuft, sobald der Claim abgelaufen ist
  (`SWARMQ_SCHEDULER_INFLIGHT_TTL`, oder `hard_timeout + 30` s, falls
  größer). Läuft das Original noch (nur sein Kanal ging verloren),
  wartet die Kopie, statt daneben zu laufen. Die Zurückstellung ist
  gedeckelt; danach loggt der Worker `schedfire_defer_stalled` und
  bestätigt die Kopie. Ein zweiter Publish desselben Fires durch den
  Leader bleibt ein Duplikat. Ein Worker, der herunterfährt, während
  Fires auf einen freien Concurrency-Slot warten, gibt jetzt deren
  Claims frei, sodass die Neuzustellung sie ausführt. Cron-Tasks
  sollten einen `hard_timeout` haben: Der Claim überdauert dann jeden
  Lauf, das schließt den einen verbleibenden Fall zweier gleichzeitiger
  Läufe
  ([was den Ack abbricht](../explanation/delivery-semantics.md#was-den-ack-abbricht)).
- **FS-04: Locks und Concurrency-Slots werden auch frei, wenn ein
  DI-Finalizer scheitert.** Bisher übersprang ein scheiternder
  Dishka-Finalizer (`ExitError`) Lock-Freigabe und Slot-Rückgabe. Der
  Lock-Keeper verlängerte den Lock, solange der Worker lebte; nach
  `concurrency` solchen Fehlern verarbeitete der Worker nichts mehr, und
  `/ready` meldete ihn trotzdem bereit. Jetzt werden Lock, Slot und
  Fire-Claim in jedem Fehler- und Abbruchpfad frei. Bricht der Shutdown
  einen Task während des DI-Teardowns ab, werden Lock und Slot erst
  frei, wenn der Finalizer (etwa ein Commit) fertig ist; der Commit
  läuft also nicht ohne seinen Mutex.
- **FS-06 (Fire-Claim): Ein Fire-Claim geht nicht mehr verloren, wenn
  der Valkey-Client den Claim nach einer verlorenen Antwort
  wiederholt.** Der wiederholte Aufruf hielt den eigenen Claim des
  Workers für fremd, und das Fire wurde als Duplikat bestätigt.
  Unique-Claim und Mutex haben denselben Fehler vorerst weiter; ein
  späteres Release behebt sie.

## [0.12.0] — 2026-10-13

0.12.0 ist 0.12.0rc2 plus die Korrektur unten. Die beiden
Release-Candidates beschreiben alle Änderungen seit 0.11.1: siehe
[0.12.0rc1](#0120rc1-2026-10-11) (Upgrade-Hinweise, zuerst 0.11.1
ausrollen) und [0.12.0rc2](#0120rc2-2026-10-12). Installation mit
`pip install 'swarmq==0.12.*'`; `--pre` ist nicht mehr nötig. Von der
Lasttest-Suite an 0.12.0rc2 abgenommen (Konformität, Smokes, Läufe über
1 h und 6 h, kein Verlust).

### Behoben

- **B25: `swarmq inspect` nennt `--app-dir` und das zuerst durchsuchte
  Verzeichnis, wenn `--module` nicht gefunden wird**, wie schon
  `swarmq worker` und `swarmq worker --check`. Fehlt nur eine
  Abhängigkeit eines gefundenen Moduls, bleibt die Meldung unverändert
  ([das Task-Modul finden](cli.md#das-task-modul-finden)).

## [0.12.0rc2] — 2026-10-12

Zweiter Release-Candidate für 0.12.0 (Installation mit `pip install --pre`).
Er bringt die Korrektur für einen Verbindungsabriss beim Publish (B24)
und die Verbesserungen aus der Phoenix-Migrationsanalyse unten. Die
Upgrade-Hinweise von [0.12.0rc1](#0120rc1-2026-10-11) gelten unverändert.

### Hinzugefügt

- **Fehlerergebnisse tragen `error_class` (voll qualifizierte
  Exception-Klasse), und `TaskError.error_class` stellt sie bereit; im
  CPU-Modus ist es die Klasse, die das Kind meldet.** Die Form ist
  `modul.qualname`, eingebaute Klassen als `builtins.ValueError`. Pfade
  ohne Task-Exception nennen die öffentliche swarmq-Klasse, wo es eine
  gibt (`TaskNotFoundError`, `LockWaitExceeded`, `RedeliveryLimitError`,
  `UnroutableMessage`, `ResultSerializationError`). Ein gescheiterter
  Workflow speichert sie als `error.class`, und `handle.get()` wirft
  `TaskError` damit. Fehler, die der Worker selbst erzeugt (harter
  Timeout, Drain, verlorenes Rechenkind, Ablauf), tragen keine Klasse;
  der Schlüssel fehlt dann, wie bei Ergebnissen älterer Worker.
  `error_message` bleibt unverändert: Eine Exception ohne Text ergibt
  weiter `""`. Ältere Clients ignorieren das neue Feld
  ([Nachrichtenformate](agent/message-formats.md#task-ergebnis-valkey),
  [`TaskError`](errors.md#taskerror)).

- **`SWARMQ_SCHEDULER=on|off` und `swarmq worker --no-scheduler` halten
  einen Worker aus der Wahl des Scheduler-Leaders heraus.** Er meldet
  seine Crons weiter, der Leader räumt sie also nicht ab. Läuft auf
  keinem Worker der Scheduler, feuern Crons, Oneshots und
  `delay`/`eta`-Tasks nicht. `scheduler_disabled` (WARNING) erscheint
  einmal je Worker-Prozess; `swarmq check` hat einen neuen Abschnitt
  `scheduler` mit den Befunden `scheduler_disabled` (Warnung) und
  `scheduler_invalid` (Fehler, Exit `78`). Ältere Worker (bis
  0.12.0rc1) ignorieren die Variable
  ([Scheduler abschalten](../guides/scheduling-and-retries.md#scheduler-auf-einzelnen-workern-abschalten)).

- **`swarmq worker` und `swarmq inspect` finden Task-Module wieder im
  Arbeitsverzeichnis; `--app-dir DIR` nennt ein anderes Verzeichnis.**
  Das Konsolenskript suchte bisher nur im `bin`-Verzeichnis des
  Virtualenv, `--module myapp.tasks` brauchte also `PYTHONPATH`, außer
  beim Start mit `python -m swarmq.cli`. Das Arbeitsverzeichnis steht
  jetzt wie bei `python -m` an erster Stelle von `sys.path`, aber nicht
  unter `PYTHONSAFEPATH`/`-P`; ein ausdrückliches `--app-dir` gilt
  immer. Das gilt auch für `--configure`, `--check`, die Worker-Kinder
  des Supervisors und die Rechenkinder von `--pool cpu`. Bekannte
  Grenze: Eine Datei im App-Verzeichnis überdeckt ein installiertes
  Paket oder ein Modul der Standardbibliothek gleichen Namens
  (`valkey.py`, `queue.py`); `PYTHONSAFEPATH=1` schaltet den Standard
  ab. Ein nicht gefundenes Modul nennt jetzt `--app-dir` und das zuerst
  durchsuchte Verzeichnis
  ([Das Task-Modul finden](cli.md#das-task-modul-finden)).

### Geändert

- **Unter `from __future__ import annotations` lässt eine DI-Methode,
  die einen Alias von `FromDishka` oder `FromComponent` nutzt, aber bei
  der Klassendefinition nicht auflösbar ist, den Worker-Start jetzt mit
  `ConfigurationError` (Exit `78`) scheitern; bisher lief sie still
  ohne Injektion.** Das betrifft einen Vorwärtsverweis auf eine spätere
  Klasse und einen unauflösbaren Nicht-DI-Parameter daneben; die Meldung
  nennt Task, Parameter und den unauflösbaren Parameter.
  `Depends[X] | None` (Alias, optional) wird jetzt wie
  `FromDishka[X] | None` abgelehnt
  ([Dependency Injection](api.md#dependency-injection-dishka)).

- **`raise Retry(...)` wiederholt jetzt auch, wenn der Task
  `retry_exceptions` setzt, ohne `Retry` zu nennen; bisher ging die
  Nachricht direkt in die DLQ.** `Retry` zählt weiter gegen
  `max_retries`; bei `max_retries=0` endet er terminal
  ([`Retry`](errors.md#retry)).

- **Doku: Argument-Serialisierung, Argumentänderungen in Middleware,
  `ResultExpiredError` (nie geworfen) und der Hinweis zur DLQ-Frist
  beschreiben jetzt das tatsächliche Verhalten.** Keine Verhaltensänderung.
  Argumente reisen als JSON: `datetime`, `Decimal`, `UUID` und `bytes`
  kommen als `str` an, `tuple`/`set` als `list`; ein Objekt, das JSON
  nicht kodieren kann, wirft `ResultSerializationError` (mit
  `delay`/`eta` einen einfachen `TypeError`)
  ([Tasks definieren](../guides/defining-tasks.md#argumente-und-ruckgabewerte)).
  Eine Änderung am bestehenden `message["kwargs"]`-Dict oder an der
  `message["args"]`-Liste in `pre_execute` erreicht `run()`, auch im
  CPU-Modus; das ist jetzt dokumentierter Vertrag
  ([Middleware](../guides/middleware.md)). `ResultExpiredError` wird
  nie geworfen; ein abgelaufenes Ergebnis wirft `ResultNotReadyError`
  ([Fehler](errors.md#resultexpirederror)); der maschinenlesbare
  Fehlerkatalog nennt `not raised (reserved)`. Unbekannte Tasks
  erreichen die `.XQ` nicht mehr über den Dead-Letter-Exchange des
  Brokers ([Dead-Letter-Queues](../guides/dead-letter-queues.md)).

- **Nachrichten, die der Worker abweist (`undecodable`,
  `middleware_chain_factory`, `body_too_large`), gehen jetzt als Kopie
  mit der Dead-Letter-Frist und dem Diagnose-Header nach `{queue}.XQ`,
  wie unregistrierte Tasks; sie halten den Verfall am Kopf der `.XQ`
  nicht mehr auf.** Grund und Fehler reisen im bestehenden
  `x-swarmq-traceback` als `<reason>: <error>`, `swarmq dlq list` zeigt
  sie deshalb in `ERROR_PREVIEW` (z. B. `undecodable: …`).
  `TASK_REJECTED` und `swarmq_tasks_rejected_total` feuern einmal mit
  denselben `reason`-Werten wie bisher; es wird kein Fehlerergebnis
  gespeichert, und `TASK_DLQ` feuert nicht. Der Dead-Letter-Exchange des
  Brokers bleibt der Rückfall, wenn die Kopie scheitert. Worker bis
  0.12.0rc1 an derselben Queue routen diese Nachrichten weiter über den
  Dead-Letter-Exchange; beides landet in `.XQ`
  ([Dead-Letter-Queues](../guides/dead-letter-queues.md#drei-wege-in-die-xq)).

### Behoben

- **`Annotated[X, FromComponent(...)]` und Aliase von `FromDishka`
  werden unter `from __future__ import annotations` injiziert; bisher
  wurden sie still übergangen.** Die DI-Prüfung läuft auch im
  eingebetteten Worker (`SwarmQ.start_worker()`, vor jeder Verbindung)
  und bei `swarmq worker --check`, das dafür jetzt `--module` importiert
  und die Tasks prüft, die der Import registriert
  ([Dependency Injection](api.md#dependency-injection-dishka)).
- **Unter Python 3.14 scheitert ein Task ohne DI nicht mehr bei der
  Klassendefinition an einer unauflösbaren Annotation** (ohne
  `from __future__ import annotations`); ein Lock-Template mit
  Platzhaltern wird für einen solchen Task ebenfalls gerendert.
- **Der Bau der App scheitert nicht mehr mit `NameError` an einer
  unauflösbaren Annotation von `run()`** (etwa einem Typ, der nur unter
  `TYPE_CHECKING` importiert wird), mit und ohne
  `from __future__ import annotations`; `SwarmQ(...)` und der Start von
  `swarmq worker` lesen diese Annotationen tolerant.

- **`task_sent_to_dlq` erscheint nicht mehr, wenn der Dead-Letter-Publish
  scheiterte und die Nachricht neu zugestellt wurde; diesen Fall meldet
  `dlq_publish_failed`.** Das Event bleibt, wenn die Kopie in der
  Dead-Letter-Queue liegt (auch wenn danach nur das Ack scheiterte) und
  wenn ein Lock- oder Unknown-Task-Pfad die Nachricht über den Broker-DLX
  dorthin leitete ([Logging-Referenz](logging.md)).
- **B24: Ein Verbindungsabbruch beim Warten auf das Publisher-Confirm
  ist `PublishIndeterminate`, kein rohes `AMQPConnectionError` mehr.**
  Riss die Broker-Verbindung, nachdem eine Nachricht gesendet war, aber
  bevor der Broker sie bestätigte (Broker-Neustart, Netztrennung),
  endeten `schedule()` und die übrigen Publish-Wege mit
  `aiormq.exceptions.AMQPConnectionError`. Die Nachricht war meist
  angekommen und lief; ein Produzent, der wie für
  `BrokerConnectionError` dokumentiert wiederholte, führte den Task
  doppelt aus, und `schedule()` gab die `unique_for`-Sperre frei.
  swarmq unterscheidet jetzt die Phasen: Ist die Nachricht auf den
  Socket geschrieben, endet der Publish als `PublishIndeterminate`, und
  swarmq sendet sie nicht erneut (das gilt auch für einen geschlossenen
  Kanal und für aio-pikas interne Abbruch-Ausnahme, bei denen swarmq
  bisher erneut sendete); riss die Verbindung vorher, wiederholt swarmq
  wie bisher und meldet, wenn das nicht hilft, `BrokerConnectionError`.
  Retry- und Dead-Letter-Kopien des Workers, Workflow-Schritte,
  Scheduler-Termine und `swarmq dlq retry` unterscheiden ebenso. Ein
  Ack, das mit `AMQPConnectionError` scheitert, ist jetzt ein
  `BrokerConnectionError`
  ([`PublishIndeterminate`](errors.md#publishindeterminate)).
- **`schedule_many()` behält die `unique_for`-Sperre eines unbestimmten
  Elements**, wie `schedule()`; bisher gab es die Sperren aller
  gescheiterten Elemente frei, sodass ein Wiederholversuch einen Task
  ausführen konnte, dessen Nachricht angekommen war. Ist ein Element
  unbestimmt, wirft der Batch jetzt dessen `PublishIndeterminate` statt
  des ersten Fehlers nach Index
  ([`schedule_many()`](api.md#schedule_many)).
- **Ein kaputter Frame vom Broker schlägt nicht mehr als roher
  pamqp-Fehler durch.** Endet aiormqs Reader an einem unlesbaren Frame
  (`pamqp.exceptions.AMQPFrameError` und Verwandte), bevor die Nachricht
  geschrieben war, wiederholt swarmq auf einem neuen Kanal wie nach jedem
  anderen Verbindungsabbruch und meldet, wenn das nicht hilft,
  `BrokerConnectionError`; nach dem Schreiben ist es
  `PublishIndeterminate`. Ein Ack, das mit einem solchen Fehler
  scheitert, ist ebenfalls ein `BrokerConnectionError`.
- **Kein „Future exception was never retrieved“ mehr nach einem Publish,
  der vor dem Schreiben endete.** aiormq legt das Future für das
  Publisher-Confirm vor dem Schreiben an; scheiterte das Schreiben oder
  brach der Aufrufer den Publish ab (`asyncio.wait_for`), wartete niemand
  darauf, und asyncio meldete dessen Verbindungsfehler beim Einsammeln.
  swarmq holt jetzt die Ausnahme genau dieses Futures ab.

### Bekannte Grenzen

- **Eine abgewiesene Nachricht, deren Kopie scheitert, landet wie
  bisher ohne Frist in `.XQ` (Rückfall über den Dead-Letter-Exchange).**
  Endete die Kopie vor dem Rückfall mit unbestimmtem Publish-Ausgang,
  kann die Nachricht zweimal in `.XQ` liegen, einmal mit und einmal ohne
  Frist. Eine Broker-Policy auf `\.XQ$` bleibt die einzige vollständige
  Begrenzung.

- **`error_class` ist beratend.** Ergebnisse älterer Worker haben das
  Feld nicht, ebenso Pfade ohne Exception-Klasse (Ablauf durch
  `max_age`, `WorkerHungError`, Fehler im Lock-Template) und Fehler, die
  der Worker selbst erzeugt (harter Timeout, Drain, verlorenes
  Rechenkind, Kind ohne Fehlerdetail). Im CPU-Modus steht dort die
  Klasse, die das Kind meldet, auch wenn der Elternprozess sie nicht
  laden konnte.

- **DI-Annotationen, die weder bei der Klassendefinition noch beim
  Worker-Start auflösbar sind und keinen erkennbaren Marker zeigen**
  (etwa ein Typ, der nur unter `TYPE_CHECKING` importiert wird, oder
  unter PEP 649 `Depends[Later] | None`), werden nicht injiziert und
  nicht gemeldet; das ist der Preis dafür, dass die Klassendefinition
  eines Tasks ohne DI nie scheitert. `FromDishka` im Annotationstext
  wirft weiter `NameError` mit Hinweis.
- **PEP 563 und PEP 649 melden eine unauflösbare DI-Methode
  verschieden:** mit Future-Import `ConfigurationError` beim
  Worker-Start, ohne `NameError` bei der Klassendefinition. Beides
  erhält das Verhalten von 0.12.
- **`swarmq worker --check` prüft nur die Tasks, die der Import von
  `--module` registriert.**

## [0.12.0rc1] — 2026-10-11

Release-Candidate für 0.12.0 (Installation mit `pip install --pre`). Die Version behebt die Befunde der
Untersuchung „Betrieb über Zeit“ (BZ-01 bis BZ-29): verlorene und
doppelt ausgeführte Tasks, Wartezeiten ohne Grenze und Zustand, der
sich über Wochen Betrieb aufbaut. BZ-20 bleibt eine Grenze und wird
jetzt gemeldet. **Erst alle Apps als eigenes Deployment auf 0.11.1
ausrollen**, dann [Ausrollen und Upgraden](../guides/deployment.md)
folgen.

### Upgrade-Hinweise

#### Reihenfolge

1. **Zuerst `swarmq check`** mit 0.12 in einem eigenen virtualenv gegen
   Produktion ausführen (`pip install --pre 'swarmq==0.12.*'`; es liest nur:
   `CONFIG GET`, `INFO`, `SCAN`, passive Deklarationen). Unter
   Kubernetes geht das als Init-Container. Exit `78` heißt: Die neuen
   Worker würden den Start verweigern. Das nicht erst beim Ausrollen
   herausfinden: Ein Worker, der den Start verweigert, endet bei jedem
   Neustart wieder mit `78`, Kubernetes zeigt `CrashLoopBackOff`,
   Docker mit `restart:`-Policy läuft in eine Schleife.
   `maxmemory-policy noeviction` setzen oder bewusst
   `SWARMQ_VALKEY_EVICTION_CHECK=warn` (etwa bei einem Managed-Dienst
   mit fester Policy, mit bekanntem Risiko) ([CLI](cli.md#swarmq-check)).
2. **Zuerst alle Apps auf 0.11.1**, als eigenes Deployment. Ein Rollback
   per Kubernetes oder Docker geht auf das vorher deployte Image zurück;
   0.11.0 scheitert an Queues, die 0.12 angelegt hat, und ignoriert einen
   Namensraum still ([0.11.1](#0111-2026-10-10)).
3. **Worker vor Producern.** Für jede neue Queue gilt das jetzt
   dauerhaft: Ein Publish an eine Queue, die noch kein Worker deklariert
   hat, wirft `UnroutableMessage` (BZ-03). Beim Rollout von 0.11 gilt es
   auch für neue Task-Namen, weil nur 0.12-Worker einen unbekannten Task
   zurückstellen (BZ-06).
4. **Grace-Period:** Kubernetes `terminationGracePeriodSeconds` und
   Docker `stop_grace_period` mindestens `SWARMQ_SHUTDOWN_TIMEOUT + 15 s`
   (`swarmq check` zeigt `min_grace_period_s`). Mit dem Default 30 s
   drainiert ein eingebetteter Worker jetzt 25 s, ein Worker unter dem
   Supervisor 23 s (bisher 30 s); um langen Tasks den bisherigen Drain zu
   geben, `SWARMQ_SHUTDOWN_TIMEOUT` zusammen mit der Frist um 7 s
   anheben (BZ-18).
5. **Namensraum zuletzt.** Erst alle Komponenten (Worker, Producer, CLI,
   Kontrolldienst) auf 0.12, dann `SWARMQ_NAMESPACE` überall im selben
   Schritt setzen; gemischte Werte spalten die App. Nur für eine neue App
   oder nach dem Umzug der App: Der Namensraum migriert keine Daten unter
   `sq:` (Schedules, CLI-Crons, verzögerte Tasks, Ergebnisse, Locks).
   Umzug: die App vollständig stoppen (Kubernetes `strategy: Recreate`
   oder auf 0 skalieren, kein Rolling Update), ihre Crons und One-Shots
   ohne Namensraum entfernen (`swarmq schedule list`, `swarmq schedule
   remove --permanent`), ihre CLI-Crons und One-Shots mit Namensraum neu
   anlegen, Valkey-ACL-Muster von `~sq:*` auf `~<namensraum>:sq:*`
   umstellen (Kanäle `&sq:*` auf `&<namensraum>:sq:*`) und Werkzeuge, die
   die Keys direkt lesen (Dashboards wie swarmv), auf den Namensraum
   einstellen. Vorher alle Apps am Valkey auf 0.12 bringen: Ein Leader
   älter als 0.12 feuert die alten Definitionen bis zu 24 Stunden weiter.
   Getrennte RabbitMQ-vhosts an einem Valkey brauchen je App einen
   Namensraum. Solange Apps älter als 0.12 ohne Namensraum am Valkey
   laufen, `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0` setzen, sonst kann die
   Führung zu einer 0.11-App wandern (BZ-10;
   [Ablauf](configuration.md#eine-app-in-einen-namensraum-umziehen)).
6. **Verbindungen:** Worker öffnen zwei Verbindungen zu RabbitMQ
   (Producer eine). `connection_max`, `max-connections` je vhost,
   Benutzerlimits und Alarme auf die Verbindungszahl vor dem Rollout
   prüfen, oder `SWARMQ_BROKER_PUBLISH_CONNECTION=shared` setzen (BZ-16).
7. **Queues, Rechte, Policies:** Eine Queue-Familie hat jetzt bis zu
   sieben weitere Queues, die Delay-Stufen `{queue}.DQ.1s` bis
   `{queue}.DQ.1h` (21 mit `SWARMQ_PRIORITIES`); Rechte und Policies der
   Delay-Queues brauchen das Muster `\.DQ(\.\w+)?$` (BZ-01). Auf eine
   Queue wirkt nur eine Policy: jeden Schlüssel (at-least-once
   Dead-Lettering, `max-length`) in eine aufnehmen. Keine
   `delivery-limit`-Policy auf swarmq-Queues setzen (BZ-15, BZ-19).

#### Geändertes Verhalten

- **Valkey-Eviction (BZ-04):** Ein Worker verweigert den Start
  (`ConfigurationError`, Exit `78`), wenn Valkey swarmq-Keys verdrängen
  kann (jede Policy außer `noeviction` mit `maxmemory`-Grenze oder
  unbekannter Grenze). Eine nicht lesbare Policy blockiert nie.
- **Unroutbare Publishes (BZ-03):** `send()`/`schedule()` an eine Queue,
  die es nicht gibt, werfen sofort `UnroutableMessage`, statt Erfolg zu
  melden, während der Broker die Nachricht verwirft. Ein Task mit
  `TaskInfo(priority="high"|"low")`, dessen Worker ohne `--priorities`
  laufen, wirft sie jetzt ebenfalls.
- **Herunterfahren (BZ-18):** `SWARMQ_SHUTDOWN_TIMEOUT` ist das
  Gesamtbudget ab `SIGTERM`, nicht mehr der Drain allein;
  `ProcessManager(shutdown_grace_s=...)` bedeutet ebenfalls das
  Gesamtbudget.
- **Lock-Konflikte (BZ-05):** Eine Nachricht, deren Task-Lock belegt
  ist, wartet zeitbasiert statt 10 Versuche; `LockConflictLimitError`
  kommt nicht mehr vor (stattdessen `LockWaitExceeded`).
  `x-swarmq-lock-conflict-count` wird von 0.12 weder erhöht noch
  gelesen; das neue `x-swarmq-lock-conflicts` speist das Signal-kwarg
  `lock_conflict_count`, dessen Bedeutung gleich bleibt.
  `task_lock_conflict` wird beim ersten Konflikt und danach etwa einmal
  je Minute geloggt; `lock_conflict_dlq_failed` nennt das Feld
  `lock_conflicts` statt `lock_conflict_count`; die DLQ-Kopie trägt
  keinen Konfliktzähler-Header mehr (die Zahl steht im Text von
  `x-swarmq-traceback`, „(N conflicts)“).
- **Unbekannte Tasks (BZ-06):** Ein 0.12-Worker stellt einen Task, den
  er nicht registriert, 15 Minuten zurück, bevor er ihn in die DLQ
  schickt. `unregistered_task` folgt jetzt auf `TASK_DLQ`, die DLQ-Kopie
  trägt SwarmQ-Header, und `task_not_found` kommt erst nach der Frist.
- **Delivery-Limit (BZ-15):** Der Worker dead-lettert beim kleineren
  Wert aus `SWARMQ_DELIVERY_LIMIT` und dem `x-delivery-limit` der Queue:
  Eine Queue mit Limit 20 bei `SWARMQ_DELIVERY_LIMIT=50` dead-lettert der
  Worker bei 19 Zustellungen mit Fehlerergebnis, statt dass der Broker
  sie still verwirft. Ein geänderter Queue-Typ endet mit
  `QueueArgumentsMismatch`, Exit `78`, statt einer Neustartschleife.
  `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=at-least-once` zusammen mit
  `SWARMQ_QUEUE_TYPE=classic` ist ein `ConfigurationError`.
- **At-least-once-Dead-Lettering (BZ-19):** Auf neuen Quorum-Queues mit
  einer `max-length`-Policy (auch einer Operator-Policy) bekommt ein
  Publish an eine volle Queue ein Nack statt drop-head und scheitert
  nach fünf Versuchen mit `PublishRejected`. Duplikate aus
  at-least-once-Dead-Lettering laufen bei Tasks ohne Ausführungsschutz
  (`FIRE_AND_FORGET`, `execution_guard=False`) doppelt. Jeder
  Worker-Prozess loggt `queue_dead_letter_at_most_once` für
  Bestandsqueues, und jeder Start kostet je von 0.12 angelegter Queue
  bis zu zwei Channel-Fehler `PRECONDITION_FAILED` im Broker-Log.
- **Negative Publisher-Confirms** werfen `PublishRejected` (Unterklasse
  von `BrokerConnectionError`) statt eines schlichten
  `BrokerConnectionError`; ein Publish auf einer blockierten Verbindung
  wirft nach `SWARMQ_PUBLISH_TIMEOUT_S` `BrokerPublishBlocked` statt
  `PublishIndeterminate` (BZ-16). Bestehende
  `except BrokerConnectionError`-Handler fangen beide weiter.
- **Stornieren (BZ-23):** Nach der stornierten Zustellung wird das Flag
  auf höchstens 1 h verkürzt statt gelöscht: Dieselbe `task_id`, die
  innerhalb dieser Stunde neu eingereiht wird, wird storniert, und
  `cancel()` liefert `False`. `cancel()` liest außerdem den
  One-Shot-Eintrag der Task-ID und wirft `BackendConnectionError`, wenn
  dieses Lesen scheitert.
- **Crons mit `unique_for` (BZ-09)** bekommen einen neuen
  Fingerabdruck. Melden 0.11- und 0.12-Worker einer App einen solchen
  Cron länger als 10 Minuten gleichzeitig, loggt der Leader
  `scheduler_cron_conflict` (die Definition von 0.12 gewinnt); ein
  0.12-Leader schreibt die Definition einmal neu
  (`scheduler_static_cron_updated`).
- **Leader-Version (KTD11):** Worker warnen bei jedem Leader vor 0.12
  (bisher 0.10); während des Rollouts ist `scheduler_leader_outdated`
  erwartet, bis ein 0.12-Worker führt.
- **Autoscaling (BZ-01):** `swarmq worker --autoscale` zählt nur die
  Nachrichten, die in `{queue}` selbst bereitliegen (`ready_depth`); ein
  Rückstand zurückgestellter Nachrichten skaliert nicht mehr hoch.
- **`swarmq dlq retry` (BZ-08)** behält die `x-swarmq-replay-id`, die ein
  0.12-Worker gestempelt hat, und setzt `x-swarmq-enqueued-at` neu; ein
  zweiter Retry derselben `.XQ`-Nachricht wird als Duplikat übersprungen.
- **Queue-Deklarationen** laufen nach einer passiven Prüfung auf einem
  eigenen Channel; eine Queue ohne `x-delivery-limit` wird übernommen
  (WARNING; RabbitMQ nimmt 20). Eine Ablehnung, die swarmq nicht erkennt
  (Queue-Limit, Recht), meldet die Broker-Meldung und rät nicht mehr zum
  Löschen der Queue.
- **Retry-Backoff (BZ-26):** Oberhalb von `retry_max_delay` wartet ein
  `exponential`- oder `linear`-Retry 0,6 bis 0,8 × `retry_max_delay`
  statt genau den Cap.

#### Neue Defaults

| Einstellung | Default | Wirkung |
|---|---|---|
| `SWARMQ_VALKEY_EVICTION_CHECK` | `strict` | Start verweigern unter verdrängender Policy |
| `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY` | `auto` | neue Quorum-Queues at-least-once mit `reject-publish` |
| `SWARMQ_UNKNOWN_TASK_GRACE_S` | `900` | unbekannte Tasks 15 min zurückstellen |
| `SWARMQ_LOCK_WAIT_MAX_S` | `21600` | längste Wartezeit auf einen Task-Lock |
| `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` | `604800` | Ausführungszustand hinter einem Rückstand lebt 7 Tage |
| `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` | `86400` | verzögerten Task an fehlende Queue einen Tag erneut versuchen |
| `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` | `SWARMQ_SCHEDULER_LOCK_TTL` | Leader tritt nach wiederholten Publish-Fehlern zurück |
| `SWARMQ_BROKER_PUBLISH_CONNECTION` | `separate` | Worker nutzen zwei Broker-Verbindungen |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` (unverändert) | jetzt das Gesamtbudget des Herunterfahrens |
| Cancel-Flag | `max(24 h, result_ttl)` | bisher `result_ttl` |
| gesperrter Worker-Slot | 1 h Abkühlzeit | bisher gesperrt bis zum Neustart des Supervisors |

### Behoben

- **BZ-01: Ein kurzer Retry oder eine kurze Rückstellung wartet nicht
  mehr hinter einer langen.** Alle Wartezeiten einer Queue teilten sich
  `{queue}.DQ`, und RabbitMQ lässt nur die Nachricht am Kopf verfallen:
  Ein Retry, der eine Stunde wartete, hielt jede Lock-, Rate-Limit-,
  Backend- oder Guard-Rückstellung dahinter fest. Der Worker nutzt jetzt
  sieben Delay-Stufen `{queue}.DQ.1s` bis `{queue}.DQ.1h` und legt eine
  Wartezeit in die kleinste Stufe, die sie erreicht; Wartezeiten über
  einer Stunde bleiben in `{queue}.DQ`. Die Stufen entstehen beim Start
  des Workers mit den Argumenten von `{queue}.DQ`; eine Ablehnung ist
  nicht fatal (die Wartezeiten dieser Stufe nutzen `{queue}.DQ`,
  `delay_tier_unavailable`)
  ([Delay-Stufen](../explanation/delivery-semantics.md#delay-stufen)).
- **BZ-02: Ein verzögerter Task läuft unter der ID, die `schedule()`
  geliefert hat.** `schedule(delay=|eta=)` lieferte eine ID, und der
  Leader feuerte den Task unter einer neuen: `get_result()` lief in den
  Timeout, `cancel()` wirkte nicht, und das Dedup-Flag von `unique_for`
  wurde nie freigegeben. Der One-Shot-Eintrag speichert die ID jetzt im
  neuen Feld `ScheduleDefinition.task_id`; Einträge ohne das Feld feuern
  unter der ID aus ihrem Namen `oneshot:<task_id>`. Crons bekommen
  weiter je Fire eine neue ID
  ([verzögerte Ausführung](../guides/scheduling-and-retries.md#immediate-vs-deferred)).
- **BZ-03: Ein Publish an eine Queue, die es nicht gibt, verschwindet
  nicht mehr still.** Der Broker gab eine solche Nachricht zurück
  (`basic.return`), das Publisher-Confirm kam trotzdem, also meldeten
  `send()`/`schedule()` Erfolg, und die Nachricht war weg. Die
  Publish-Channels werten eine Rückgabe jetzt als Fehler: Der Publish
  wirft sofort `UnroutableMessage`, ohne Wiederholung.
  - Der Scheduler-Leader zählt einen Cron-Fire an eine fehlende Queue
    als verloren: Er schreibt den Fire fest, sendet
    `SCHEDULER_FIRE_UNROUTABLE` (`kind="cron_lost"`) und loggt
    `scheduler_fire_unroutable` höchstens einmal je Cron und 10 Minuten,
    dazu `scheduler_fire_unroutable_foreign`, wenn ein Worker einer
    anderen App den Cron meldet.
  - Ein verzögerter Task, dessen Queue fehlt, wird alle 60 s erneut
    versucht. Nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`, gezählt
    ab dem ersten unroutbaren Versuch
    (`ScheduleDefinition.unroutable_since`), wird er verworfen:
    `scheduler_oneshot_unroutable_dropped`, `kind="oneshot_dropped"`, ein
    Fehlerergebnis unter seiner Task-ID, und sein `unique_for`-Flag wird
    freigegeben.
  - Ein Worker, dessen eigene `{queue}.DQ`/`.XQ` (oder Delay-Stufe)
    während des Laufs gelöscht wurde, deklariert die Queue-Familie
    einmal neu, über dieselbe Kette wie beim Start, und veröffentlicht
    die Retry- bzw. DLQ-Kopie erneut (`own_queue_unroutable_redeclaring`).
  - Ein Workflow-Schritt, dessen nächster Schritt an eine fehlende Queue
    geht, markiert den Workflow sofort als `failed`
    (`workflow_advance_unroutable`), statt bis zum Zustelllimit neu
    zugestellt zu werden
  ([fehlende Queues](../guides/scheduling-and-retries.md#fehlende-queues)).
- **BZ-04: Verdrängte Locks und Marker sowie Datenverlust in Valkey
  bleiben nicht mehr unbemerkt.** Unter `volatile-*` oder `allkeys-*`
  verdrängte Valkey still Locks, den Leader-Lock, den Ausführungsschutz
  sowie Unique- und Fired-Marker, und Tasks liefen doppelt. Beim
  Verbinden liest das Backend jetzt `maxmemory-policy` und `maxmemory`
  (`CONFIG GET`, sonst `INFO memory`, höchstens 2 s, scheitert nie);
  unter einer verdrängenden Policy loggt jedes Verbinden
  `valkey_eviction_policy_unsafe`, ein Worker mit `strict` endet mit
  Exit `78` (Producer und CLI-Befehle warnen nur), und der
  Scheduler-Leader prüft höchstens einmal je Stunde erneut. Ein per
  `swarmq schedule remove` abgeschalteter Cron kam nach einem
  Datenverlust von Valkey aktiv zurück; jeder Worker merkt sich jetzt
  den Epoch-Key `[<namensraum>:]sq:{sched}:epoch` und loggt
  `valkey_data_loss_detected`, wenn er fehlt. Die interne Empfehlung
  `volatile-lru` ist zu `noeviction` plus AOF korrigiert
  ([Valkey-Anforderungen](configuration.md#valkey-anforderungen)). Zur
  Laufzeit wird ein Lock, der beim Verlängern oder Freigeben fehlt oder
  einem anderen Halter gehört, als `task_lock_lost` gemeldet (siehe
  BZ-25).
- **BZ-05: Ein Lock-Konflikt wartet zeitbasiert statt 10 Versuche.**
  Der 10. Konflikt (etwa 51 s) schickte die Nachricht ohne `TASK_DLQ`,
  Ergebnis und Workflow-Markierung in die DLQ. Eine Nachricht, deren
  Task-Lock belegt ist, wird jetzt zurückgestellt (100 ms, verdoppelt,
  höchstens 60 s), solange der Lock den Halter wechselt oder sein Halter
  lebt. In die DLQ geht sie als `LockWaitExceeded`, mit Fehlerergebnis,
  `TASK_DLQ` und fehlmarkiertem Workflow, erst wenn derselbe Halter den
  Lock länger hält als die TTL des Keys + 60 s oder seit dem ersten
  Konflikt `SWARMQ_LOCK_WAIT_MAX_S` vergangen ist; `max_age` bleibt die
  harte Grenze. Die Wartezeit nutzt die Wanduhren der Worker (ein
  Versatz bis etwa 60 s wird toleriert). Eine neue Wartezeit beginnt,
  wenn der Broker die Nachricht neu zustellt (ein Worker starb nach dem
  Acquire oder gab sie beim Herunterfahren zurück), und auf der Kopie
  einer späteren Ausführung (`x-swarmq-lock-wait-gen`: Retry-Zähler und
  Replay-ID); klassische Queues führen keinen Zustellzähler, dort setzt
  eine Neuzustellung die alte Wartezeit fort. Ein Lock-Store, dessen
  `observe` keine `LockObservation` liefert, gilt als nicht meldefähig.
  `swarmq dlq retry` entfernt auch den 0.11-Zähler
  `x-swarmq-lock-conflict-count`
  ([Lock-Acquisition](../guides/locking-and-dedup.md#lock-acquisition-verhalten)).
- **BZ-06: Ein unregistrierter Task landet beim ersten Kontakt nicht
  mehr in der `.XQ`.** Bei einem Rolling Deploy mit Producern vor den
  Workern wurde jeder neue Task-Name ohne Aufbewahrungsfrist und ohne
  Fehlerergebnis an den Broker-DLX abgewiesen, und jeder Workflow mit
  einem solchen Schritt schlug fehl. Der Worker stellt die Nachricht
  jetzt für `SWARMQ_UNKNOWN_TASK_GRACE_S` mit Backoff zurück (1 s, dann
  so lang wie bisher gewartet, höchstens 60 s je Runde), gemessen ab dem
  ersten Kontakt in `x-swarmq-unknown-since`; ein Scheduler-Fire behält
  dabei seinen In-Flight-Claim und sein Unique-Flag. Nach der Frist
  dead-lettert der Worker die Nachricht selbst: Fehlerergebnis,
  Workflow-Schritt als fehlgeschlagen markiert, Unique-Flag freigegeben,
  `TASK_DLQ`, Aufbewahrungsfrist und `x-swarmq-traceback` auf der
  DLQ-Kopie, danach `TASK_REJECTED` mit `unregistered_task` und
  `task_not_found` (ERROR)
  ([unregistrierte Tasks](../guides/dead-letter-queues.md#unregistrierte-tasks-und-rolling-deploys)).
- **BZ-07: Ein neuer Scheduler-Leader durchsucht nicht mehr die ganze
  Valkey-Datenbank.** Bei der Übernahme suchte er den Handover-Marker
  seines Vorgängers per `SCAN`, noch vor der ersten Lock-Verlängerung;
  bei Millionen Keys konnte das die Lock-TTL überdauern, und die
  Leader-Rolle flatterte. Der Marker hat jetzt einen festen Key,
  `[<namensraum>:]sq:{sched}:handover`, gelesen per `GETDEL`, und der
  Leader verlängert sein Lock direkt nach der Übernahme. Leader vor 0.12
  finden den Marker weiter, und ihr Marker wird weiter erkannt
  ([Leader-Übernahme](../guides/scheduling-and-retries.md#leader-ubernahme)).
- **BZ-08: Eine DLQ-Wiederholung ohne neue `x-swarmq-replay-id` wird
  nicht mehr übersprungen.** „Move messages“ in der RabbitMQ-UI, ein
  Shovel, swarmv und ein CLI vor 0.11 legen eine dead-gelettete
  Nachricht mit ihren Headern zurück; der Ausführungsschutz sah den
  Done-Marker des ersten Laufs und bestätigte die Wiederholung, ohne sie
  auszuführen. Der Worker stempelt jetzt die Replay-ID der nächsten
  Generation auf jede DLQ-Kopie, die er publiziert (`x-swarmq-dlq-gen`),
  und eine Nachricht, die der Broker selbst dead-gelettert hat
  (Delivery-Limit, `route_via_dlx`), bekommt ihre neue Identität aus
  `x-death`; jede Kopie, die SwarmQ publiziert, nimmt diesen Zähler als
  `x-swarmq-xdeath-base` mit, weil der Broker `x-death` beim nächsten
  Dead-Lettern neu aufbaut. Ein wiederholter Scheduler-Fire läuft
  ebenfalls. `swarmq dlq retry` leitet die Replay-ID einer Nachricht ohne
  passendes `x-swarmq-dlq-gen` (0.11-Worker, Broker-Dead-Lettering,
  Altbestand) aus ihrer Identität ab, ein wiederholter Retry läuft also
  einmal; nur eine Nachricht ohne Task-ID bekommt eine zufällige ID. Eine
  gestempelte, vom Broker erneut dead-gelettete Nachricht oder eine mit
  einer Replay-ID, die der Worker nicht liest, behandelt es als neue
  Generation
  ([Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz),
  [Nachrichten retryen](../guides/dead-letter-queues.md#nachrichten-retryen)).
- **BZ-09: `unique_for` eines Crons hängt nicht mehr davon ab, welcher
  Worker Leader ist.** Der Leader las `unique_for` aus seiner eigenen
  Task-Registry; ein Leader aus einer anderen App, mit anderem
  `--module` oder aus der anderen Hälfte getrennter CPU- und IO-Worker
  feuerte den Cron ohne Dedup, und lange Läufe überlappten. Worker melden
  `unique_for` und `unique_until` jetzt mit ihren Crons, und der Leader
  nimmt beide aus der Definition (neues Feld
  `ScheduleDefinition.unique_for`); eine Definition ohne sie (vor 0.12
  geschrieben oder ein CLI-Schedule) nutzt weiter die `TaskInfo` des
  Leaders. Der Cron-Fingerabdruck enthält beide Werte nur, wenn
  `unique_for` gesetzt ist. Scheitert der Publish eines Termins, nachdem
  der Leader das Flag genommen hat, gibt er es jetzt frei (nicht nach
  `PublishIndeterminate` und nicht, wenn `schedule(delay=|eta=)` es
  genommen hat); vorher zählte der nächste Takt den Termin als Duplikat,
  und er ging verloren
  ([Unique-Crons](../guides/locking-and-dedup.md#unique-crons)).
- **BZ-10: Mehrere Apps auf einem Valkey lassen sich trennen.** Ohne
  Namensraum teilten alle Apps Rate-Limit- und Lock-Keys, Unique-Hashes
  und einen Scheduler-Leader, und ein Leader mit defektem Broker hielt
  die Crons aller Apps an. `SWARMQ_NAMESPACE` verschiebt jeden
  Valkey-Key, jeden Pub/Sub-Kanal, den Scheduler-Leader und den
  Kontrollkanal von `sq:…` nach `<namensraum>:sq:…`; Queue-Namen bleiben
  ohne Präfix (ein RabbitMQ-vhost je App). Ohne Namensraum bleibt alles
  byte-identisch. Ein Leader, dessen Broker wiederholt scheitert, tritt
  jetzt zurück: Nach mindestens 2 gescheiterten Publishes über
  mindestens `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` ohne Erfolg dazwischen
  gibt er das Leader-Lock ohne Handover-Marker frei (der Nachfolger
  sendet `SCHEDULER_LEADER_FAILOVER`), sendet `SCHEDULER_STANDBY` und
  bewirbt sich 2 × Lock-TTL nicht erneut
  (`scheduler_publish_failures_stepdown`). Es zählen nur echte
  Verbindungsfehler: `UnroutableMessage`, `MessageHeadersTooLarge`,
  `BrokerPublishBlocked`, `PublishRejected` und `PublishIndeterminate`
  treffen jeden Kandidaten oder eine Queue und zählen nicht. Eine Lücke
  länger als die Frist zwischen zwei Fehlern und jede neue Amtszeit
  beginnen eine neue Serie. Backend und Konfiguration einigen sich auf
  einen Namensraum: Ein ausdrückliches `ValkeyBackend(namespace=…)`
  (oder `keys=`) gilt und füllt `app.config.namespace`; ohne es
  übernimmt das Backend `SwarmQConfig.namespace` (Code oder
  `SWARMQ_NAMESPACE`), sobald es an `SwarmQ(...)` geht. Beide gesetzt und
  verschieden (auch `namespace=""` neben gesetztem `SWARMQ_NAMESPACE`)
  ist ein `ConfigurationError` (`namespace mismatch`, Exit `78`). Der
  Namensraum ist nur kleingeschrieben zulässig
  (`[a-z0-9][a-z0-9_.-]{0,63}`, nicht `sq`); ein ungültiger Wert stoppt
  Worker, `SwarmQ(...)` und die CLI-Befehle vor jeder Verbindung (Exit
  `78`; `swarmq worker --check`: Exit 1).
  `scheduler_fire_unroutable_foreign` nennt im Hinweis
  `SWARMQ_NAMESPACE`, und die Lock-Hinweise (`valkey-cli DEL …`) nennen
  den Key mit dem Namensraum des Backends
  ([Namensraum](configuration.md#namensraum-mehrere-apps-auf-einem-valkey),
  [Herkunft des Namensraums](configuration.md#herkunft-des-namensraums)).
- **BZ-11: Ein Cron, dessen Task aus dem Code entfernt wurde, feuert
  nicht mehr bis zum Ablauf der Aufräumfrist.** Jeder Fire eines
  solchen Crons endete als unregistrierter Task in der `.XQ`, bis zu
  24 h lang. Der Scheduler-Leader hält jetzt einen statischen Cron
  zurück, den ein Worker ab 0.12 irgendwann gemeldet hat, den kein
  lebender Worker mehr meldet und den der Leader selbst nicht
  registriert: Der Termin wandert ohne Publish auf
  `jetzt + SWARMQ_SCHEDULER_SEEN_INTERVAL_S`, bis ein Worker ihn wieder
  meldet oder der Leader ihn aufräumt (`SCHEDULER_CRON_UNOWNED`,
  `scheduler_cron_held_unowned`). Ein CLI-Schedule gleichen Namens wird
  nie zurückgehalten. Direkt nach einer Übernahme wartet der Leader
  höchstens 5 s auf die Cron-Meldungen; ohne sie feuert er wie bisher
  (fail-open, `scheduler_unowned_check_skipped`)
  ([zurückgehaltene Crons](../guides/scheduling-and-retries.md#crons-die-niemand-meldet-werden-zuruckgehalten)).
- **BZ-12: Done- und Completed-Marker laufen hinter einem langen
  Rückstand nicht mehr ab.** Der Done-Marker des Ausführungsschutzes und
  der Completed-Marker eines Schedule-Fires lebten feste 3900 s,
  gemessen ab dem Lauf, nicht ab der Zeit, die eine zurückgestellte
  Kopie oder ein doppeltes Fire in der Queue wartet; nach mehr als
  65 Minuten Rückstand lief die Kopie ein zweites Mal. Der Claim, der
  eine Neuzustellung zurückstellt, verlängert den Zustand der Ausführung
  jetzt auf `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` oder `max_age + 300`,
  auch wenn die laufende Zustellung ihren Marker nur freigibt (etwa um
  auf einen Lock zu warten) oder länger als die Done-TTL läuft; die
  danach geschriebenen Marker verkürzen ihn nie, und ein gescheitertes
  Ack oder eine Kopie, deren Ack scheiterte, markiert die Ausführung für
  denselben Rückstand. Der Completed-Marker eines Schedule-Fires lebt
  `2 × Wartezeit + 300` Sekunden, wobei die Wartezeit die Zeit des Fires
  in der Queue ist (gedeckelt bei `max_age + 300` und der
  Rückstands-TTL, nie unter `SWARMQ_SCHEDULER_DEDUP_TTL`). Bei einem Task
  mit `max_age` leben Done- und Superseded-Marker und der
  Completed-Marker mindestens `max_age + 300` Sekunden, auch wenn der
  Lauf sofort begann (der Completed-Marker bleibt bei der Rückstands-TTL
  gedeckelt). **Kosten:** ein kleiner Hash je Ausführung für `max_age +
  300` Sekunden, etwa 100 MB bei einer Million Tasks am Tag mit
  `max_age` 24 h; Betreiber steuern das über `max_age`. `max_age` für
  Cron-Tasks und für Tasks setzen, die nicht doppelt laufen dürfen
  ([Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)).
- **BZ-13: Workflows hängen nicht mehr bis zu ihrer TTL, wenn ein
  Schritt außerhalb des Retry-Pfads endet.** Ein Schritt, der abläuft
  (`max_age`), am Lock-Template scheitert, seine Lock-Wartezeit beendet
  (`LockWaitExceeded`) oder im Worker nicht registriert ist, markiert
  seinen Workflow jetzt als `failed`; `get()` wirft `TaskError`, statt in
  den Timeout zu laufen. Lock-Template-Fehler und das Ende einer
  Lock-Wartezeit speichern außerdem ein Fehlerergebnis und feuern
  `TASK_DLQ`. Ein spätes Duplikat eines Schritts, der schon erfolgreich
  lief, markiert den Workflow nicht als gescheitert, und ein Workflow mit
  abgelaufenem Zustand loggt `workflow_state_missing`, statt still zu
  enden. Am Zustelllimit gibt eine Workflow-Markierung, die an einem
  nicht erreichbaren Valkey scheitert, die Zustellung nicht mehr zurück;
  sie geht trotzdem in die DLQ
  ([Fehlerfälle](../guides/workflows.md#fehlerfalle)).
- **BZ-14: Verwaiste Queues werden gemeldet.** Zog ein Task in eine
  andere Queue um oder änderte sich `--queues`, blieb der Rückstand der
  alten Queue ohne Consumer liegen, ohne Logzeile und ohne Metrik. Jetzt
  meldet jeder Worker die Queues, die er konsumiert, in jedem Meldetakt
  an Valkey (Hash `[<namensraum>:]sq:{sched}:report:queues`, Zeitstempel
  aus Valkeys `TIME`). In jedem Übernahme-Takt (nicht direkt nach einer
  Übernahme) prüft der Leader die Queues, die seit drei Takten kein
  lebender Worker gemeldet hat, per passivem Declare, ohne
  Management-API: Nachrichten und kein Consumer heißt verwaist. Er loggt
  `scheduler_queue_orphaned` (WARNING) und
  `scheduler_queue_orphan_resolved` (INFO) einmal je Übergang, sendet
  `SCHEDULER_QUEUE_ORPHANED` und setzt
  `swarmq_scheduler_queues_orphaned`. Einträge fehlender oder leerer
  Queues, die 7 Tage niemand gemeldet hat, werden entfernt; jede Prüfung
  hat ein Zeitlimit von 30 s (`scheduler_queue_check_failed`)
  ([Task in eine andere Queue verschieben](../guides/defining-tasks.md#task-in-eine-andere-queue-verschieben)).
- **BZ-15: Geänderte Queue-Argumente enden mit Exit 78 statt einer
  Neustartschleife.** Ein geändertes `SWARMQ_DELIVERY_LIMIT` lässt die
  bestehenden Queues deklarierbar (ihr Limit gilt, mit WARNING
  `queue_argument_adopted`; eine Queue ohne `x-delivery-limit` wird
  ebenfalls übernommen). Jeder andere erkannte Konflikt, etwa ein
  geänderter `SWARMQ_QUEUE_TYPE`, wirft `QueueArgumentsMismatch` mit
  deklariertem und vorhandenem Wert; bisher endete er als
  `BrokerConnectionError`, Exit 1 und endlose Neustarts. Deklarationen
  laufen nach einer passiven Prüfung auf einem eigenen kurzlebigen
  Channel (eine vorhandene Queue wird mit den Argumenten aus 0.11
  deklariert, von 0.11 angelegte Queues bekommen so nie eine Ablehnung),
  nie auf dem Publish-Channel, und das Ergebnis wird je Prozess gecacht
  (verworfen bei `UnroutableMessage` oder fehlender Queue). Nur die
  erste Übernahme von `x-dead-letter-strategy`/`x-overflow` je Prozess
  kommt als INFO, weitere als DEBUG; ein zuerst übernommenes
  `x-overflow` bleibt, wenn die Strategy folgt. Ein Verbindungsabriss
  während der Deklaration ist ein `BrokerConnectionError`. Der Worker
  nutzt das effektive Delivery-Limit (das kleinere aus
  `SWARMQ_DELIVERY_LIMIT` und dem `x-delivery-limit` der Queue; fehlend:
  20, `-1`: unbegrenzt, `0`: keine Wiederzustellung)
  ([vorhandene Queue-Argumente](../explanation/delivery-semantics.md#vorhandene-queue-argumente),
  [Queue-Argumente ändern](../explanation/delivery-semantics.md#queue-argumente-andern)).
- **BZ-16: Acks hängen nicht mehr hinter einem blockierten Publish.**
  Publish und Konsum teilten sich eine Verbindung, und bei einem
  Ressourcenalarm blockiert RabbitMQ publizierende Verbindungen. Ein
  Worker konsumiert jetzt über eine zweite Verbindung, die sein erster
  `consume()` öffnet: Consume-Kanal, Acks und Nacks wandern dorthin,
  Publishes bleiben auf der ersten Verbindung. Producer, die nie
  konsumieren, und Broker auf injizierter Verbindung oder Pool behalten
  eine Verbindung. Queue- und DLQ-Tiefen werden über die
  Consume-Verbindung gelesen, falls sie offen ist. Ein Publish auf einer
  blockierten Verbindung sendet nichts und wartet innerhalb von
  `SWARMQ_PUBLISH_TIMEOUT_S` auf die Freigabe; danach wirft er
  `BrokerPublishBlocked`, behält den Publish-Kanal und gibt eine
  `unique_for`-Sperre frei (`publish_connection_blocked`). Die Prüfung
  liest den Zustand synchron, ohne Roundtrip. Ein Worker-Start oder
  `connect()`, der während des Aufbaus einer Verbindung abgebrochen wird,
  schließt die halb offene Verbindung, statt aio-pika im Hintergrund
  alle 5 s neu verbinden zu lassen
  ([Fehlerkatalog](errors.md#brokerpublishblocked)).
- **BZ-17: Ein Task, der mit einem riesigen Fehlertext scheitert,
  schickt seine Nachbarn nicht mehr in die Queue zurück.**
  `x-swarmq-traceback` und `x-swarmq-lock-render-error` sind auf 8 KiB
  begrenzt (die ersten 6 KiB und die letzten 2 KiB); bisher schloss der
  Broker bei einem Fehlertext über `frame_max` (128 KiB) die
  Worker-Verbindung, und jede Nachricht, die der Worker hielt, wurde neu
  zugestellt. Jeder Publish prüft seine Header außerdem gegen die Hälfte
  von `frame_max` und wirft `MessageHeadersTooLarge`, ohne zu senden
  ([Eine Nachricht inspizieren](../guides/dead-letter-queues.md#eine-nachricht-inspizieren)).
- **BZ-18: Das Herunterfahren passt in die Grace-Period des
  Orchestrators.** Der Drain allein verbrauchte das ganze
  `SWARMQ_SHUTDOWN_TIMEOUT`, Lock-Freigabe, Leader-Übergabe, Marker des
  Ausführungsschutzes und Verbindungsschluss kamen danach; unter dem
  Supervisor kamen dessen eigenes `SIGTERM` (nach dem vollen Wert) und
  `SIGKILL` (5 s später) noch dahinter. Das Herunterfahren dauerte mit
  Defaults also 35 s und mehr, länger als der Kubernetes-Default von
  30 s bis `SIGKILL`. Jetzt endet der Drain 5 s vor dem Ende (höchstens
  die Hälfte des Budgets); der Supervisor schickt `SIGTERM` bei
  `T - 7 s`, `SIGKILL` bei `T - 2 s` und hat seine Worker bis `T`
  eingesammelt, sein Worker drainiert bis `T - 7 s`. Die Publish-Endfrist
  im Herunterfahren ist das Ende des Drains. Jeder Aufräumschritt hat ein
  Zeitlimit (der Rest des Budgets, mindestens 1 s): Rechen-Pool,
  abgetrennte Lock-Freigaben, Marker des Ausführungsschutzes, jede
  Lock-Freigabe, Scheduler-Stopp, DI-Container, Broker-Schluss (beide
  Verbindungen) und Backend-Schluss; ein Schritt darüber loggt
  `shutdown_step_timeout`, und das Herunterfahren geht weiter. Auf
  Tasks, die den Abbruch ignorieren, wird höchstens 1 s gewartet. Der
  Recycling-Drain bleibt unverändert (größter `hard_timeout`)
  ([Shutdown-Budget](../explanation/delivery-semantics.md#shutdown-budget)).
- **BZ-19: Neue Quorum-Queues dead-lettern at-least-once.**
  Rückstellungen und das Delivery-Limit bewegen Nachrichten per
  Dead-Lettering des Brokers, und das tut RabbitMQ bei Quorum-Queues
  standardmäßig at-most-once: Ein Leaderwechsel konnte sie verlieren,
  obwohl die Doku at-least-once versprach. Neue Queues bekommen jetzt
  `x-dead-letter-strategy=at-least-once` und `x-overflow=reject-publish`
  auf `{queue}`, `{queue}.DQ` und den Delay-Stufen (nicht
  `{queue}.XQ`). Bestandsqueues behalten ihre Argumente; eine WARNING
  `queue_dead_letter_at_most_once` je Prozess listet sie (auch solche mit
  `at-least-once`, aber einem anderen `x-overflow`). Eine Policy mit
  `dead-letter-strategy=at-least-once` und `overflow=reject-publish`
  behebt ihr Dead-Lettering, beendet die Warnung aber nicht, weil swarmq
  Policies nicht sieht; das tut nur
  `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=legacy`
  ([Garantie beim Dead-Lettering](../explanation/delivery-semantics.md#garantie-beim-dead-lettering)).
- **BZ-20 (Grenze, jetzt gemeldet): Prefetch je Consumer.**
  `--concurrency` ist der Prefetch jedes Consumers: Ein Worker, der K
  Queues konsumiert (jedes Prioritätsband zählt), hält bis zu K ×
  concurrency unbestätigte Nachrichten, und die letzte wartet bis zu K ×
  `hard_timeout` auf ihr Ack. Globales QoS geht mit Quorum-Queues nicht
  (RabbitMQ 4.2 schließt die Verbindung, 4.3 ignoriert es), es bleibt
  also eine Grenze. Neu: die Startwarnung
  `prefetch_exceeds_consumer_timeout` (einmal je Start, wenn K ≥ 2 und
  `hard_timeout` × K über `SWARMQ_CONSUMER_TIMEOUT_S` liegt) und die
  Gauge `swarmq_worker_prefetch_limit{worker_id}` (K × concurrency)
  ([Prefetch je Consumer](../explanation/delivery-semantics.md#prefetch-je-consumer)).
- **BZ-21: `dlq list`, `dlq inspect` und `dlq retry` verlieren keine
  Nachrichten mehr, wenn der Lesende stirbt.** Sie entnahmen jede
  Nachricht mit `no_ack=True` und legten sie erst am Ende zurück; ein
  Kill, OOM oder Netzverlust dazwischen kostete das bereits Gelesene.
  Jetzt lesen sie bestätigungspflichtig über einen eigenen Channel (auf
  der Consume-Verbindung, falls offen), kopieren jede Nachricht ans Ende
  von `{queue}.XQ` (mit ihrer Restfrist) bzw. bei `retry` nach `{queue}`
  und bestätigen das Original erst, wenn der Broker die Kopie bestätigt
  hat. Kopien tragen keine Zustellzähler des Brokers
  (`x-delivery-count`, `x-acquired-count`), wiederholtes Listen
  verbraucht also nicht das Delivery-Limit der XQ. Ein gescheitertes
  Bestätigen loggt `dlq_read_ack_failed` (WARNING) und lässt die
  Nachricht doppelt zurück
  ([DLQ-Inhalte auflisten](../guides/dead-letter-queues.md#dlq-inhalte-auflisten)).
- **BZ-22: Ein Unique-Task, der über `max_age` abläuft, gibt sein
  Dedup-Flag frei.** Bisher blieb das Flag bis zum Ende von `unique_for`
  stehen, und jedes neue `schedule()` mit denselben Argumenten lieferte
  still die ID des abgelaufenen Tasks. Das Flag wird freigegeben, sobald
  die abgelaufene Nachricht in der DLQ liegt, geprüft gegen ihre Task-ID
  ([Wann das Flag freigeben](../guides/locking-and-dedup.md#wann-das-flag-freigeben)).
- **BZ-23: Ein stornierter Task läuft nicht mehr, wenn er länger als
  `result_ttl` wartet.** `app.cancel()` gab dem Flag die TTL
  `result_ttl`, und der Worker löschte es nach der stornierten
  Zustellung; ein Task, der länger wartete (Rückstau, gestoppte Worker,
  langes `delay`), oder ein Duplikat einer stornierten Nachricht lief
  doch. Das Flag lebt jetzt `max(24 h, result_ttl)`, dazu die Wartezeit
  bis zum Fire eines verzögerten Tasks und dessen `max_age` + 300 s,
  wenn die Registry des Clients den Task kennt. Hat der Worker die
  stornierte Zustellung behandelt, auch wenn der Task während des Laufs
  storniert wurde, verkürzt er das Flag auf höchstens 1 h
  (`PEXPIRE … LT`), statt es zu löschen; ein kürzeres Flag wird nie
  verlängert ([`cancel()`](api.md#cancel)).
- **BZ-24: Ein Producer ohne Registry verliert Dedup und Priorität
  nicht mehr still.** Ein Client ohne Registry kann `unique_for`,
  `priority` und den Liefermodus aus der `TaskInfo` des Tasks nicht
  anwenden; er loggt jetzt einmal je Task-Name und Client-Instanz (für
  höchstens 1000 Namen) eine WARNING `client_task_defaults_unresolved`
  ([Client-Only-Modus](api.md#schedule)).
- **BZ-25: Task-Locks überleben einen Handler, der länger läuft als
  seine Lock-TTL.** Der Worker verlängert einen gehaltenen Mutex oder
  Semaphor-Slot jetzt, solange der Handler läuft, beginnend ein Fenster
  vor dem Ablauf (ein Fenster sind drei Takte von
  `min(max(TTL / 3, 50 ms), 20 s)`); bisher lief der Lock still ab, und
  ein zweiter Worker bekam ihn. Ab einem `hard_timeout` von 30 s kostet
  ein Task, der innerhalb von `hard_timeout - 30 s` fertig wird, keinen
  zusätzlichen Roundtrip; bei kürzerem `hard_timeout` deckt das Fenster
  die ganze TTL. Ein hart beendeter Halter hinterlässt den Lock weiter
  bis `hard_timeout + 30 s`, höchstens ein Fenster länger. Ein Lock, der
  beim Verlängern oder Freigeben fehlt oder einem anderen Halter gehört
  (verdrängt, Valkey-Datenverlust), wird als `task_lock_lost` gemeldet
  (WARNING, `stage` `running` oder `release`); gelang vor der Frist des
  Locks keine Verlängerung (Valkey weg), mit `stage="expired"`. Ein
  scheiternder Takt des Keepers loggt `lock_keeper_tick_failed`, der
  Keeper läuft weiter; höchstens 32 Verlängerungen laufen gleichzeitig.
  Ein Lock-Store, dessen `extend` weder `True` noch `False` liefert,
  gilt als ohne `extend` (`lock_extend_unsupported`). `lock_acquire.lua`
  verlängert die TTL des Keys nur noch und verkürzt sie nie; ein
  Semaphor-Erwerber mit kurzer TTL lässt den Key so nicht mehr unter
  einem Halter mit längerer TTL ablaufen
  ([Locks während der Ausführung](../guides/locking-and-dedup.md#locks-wahrend-der-ausfuhrung)).
- **BZ-26: Retries am Delay-Cap kommen nicht mehr alle gleichzeitig
  zurück.** Oberhalb von `retry_max_delay` wartet ein `exponential`- oder
  `linear`-Retry jetzt zwischen 0,6 und 0,8 × `retry_max_delay` statt
  genau den Cap, und eine sehr hohe Versuchszahl (ab etwa 1024) wirft
  kein `OverflowError` mehr. `fixed` bleibt ohne Jitter
  ([Backoff-Formeln](../guides/scheduling-and-retries.md#backoff-formeln)).
- **BZ-27: Ein gesperrter Worker-Slot läuft nach einer Abkühlzeit
  wieder an.** Der Supervisor sperrt einen Slot, dessen Worker binnen
  einer Stunde 5-mal abgestürzt ist (oder nach zu vielen hängenden
  Workern); die Sperre galt bis zum Neustart des Supervisors. Jetzt
  dauert sie 1 h (`ProcessManager.SLOT_COOLDOWN_S`); danach loggt der
  Supervisor `worker_slot_unlocked` und belegt den Slot wieder, mit
  leerer Neustart-Historie. `worker_restart_limit_exceeded` und
  `hang_replacement_budget_exhausted` tragen `cooldown_s`. Sind alle
  Slots gesperrt und lebt kein Worker mehr, endet der Supervisor weiter
  mit `1`, damit die Orchestrierung ihr eigenes Backoff anwendet.
  `SlotTable.lock(slot)` ohne `until` sperrt weiter dauerhaft.
- **BZ-28: Tote Einträge im Fire-Index halten nicht mehr alle Crons
  an.** Der Leader liest pro Takt höchstens 100 fällige Einträge.
  Index-Einträge ohne Definition und deaktivierte Definitionen, die noch
  einen Termin hatten (geschrieben vor 0.10), füllten dieses Limit,
  sodass 100 davon alle Crons blockierten. Der Poll nimmt sie jetzt aus
  dem Fire-Index, bevor das Limit greift, höchstens 1000 pro Takt; die
  Definitionen bleiben. Eine Definition, die der Leader nicht dekodieren
  kann, lässt auch nicht mehr den ganzen Takt scheitern: Sie wird als
  `schedule_def_malformed_parked` geloggt und aus dem Fire-Index
  genommen. `sched_poll.lua` behält Keys und Argumente, schreibt aber
  jetzt (`ZREM`)
  ([Fire-Index](../guides/scheduling-and-retries.md#fire-index)).
- **BZ-29: Eine Zustellung, die der Worker immer wieder zurückgibt,
  kreist auf RabbitMQ 4.3 nicht mehr endlos und verbraucht ihr
  Zustellbudget nicht mehr in einem Augenblick.** RabbitMQ 4.3 zählt
  `nack(requeue=True)` nicht in `x-delivery-count`, nur in
  `x-acquired-count`. Die Zustellzähler-Prüfung zählt jetzt beide und
  schickt eine solche Zustellung eine Runde vor `SWARMQ_DELIVERY_LIMIT`
  als `RedeliveryLimitError` in die DLQ; bisher kreiste eine Zustellung,
  die eine Prüfung jedes Mal zurückgab (Fehlerantwort von Valkey,
  gescheiterter Dead-Letter-Publish, kein Rechen-Slot), auf 4.3 ohne Ende
  und verbrauchte auf 4.2 ihre rund 20 Zustellungen in einem Augenblick.
  Vor jeder Rückgabe wartet der Worker jetzt `min(0,1 s × 2^k, 5 s)`
  (k = bisherige Zustellungen) und hält dabei den Slot; ein Shutdown
  beendet die Wartezeit
  ([Zustellsemantik](../explanation/delivery-semantics.md)).
- **KTD11: Die Leader-Versionsprüfung warnt bei jedem Leader vor 0.12**
  (Mindestversion für Leader, bisher 0.10), weil die Scheduler-Korrekturen
  oben nur wirken, solange der Leader sie kennt. Der `hint`, die
  Gauge-Beschreibung und die Signal-Dokumentation nennen keine Zeitzonen
  mehr ([Leader-Version](../guides/scheduling-and-retries.md#leader-version)).

### Hinzugefügt

#### Konfiguration

| Variable | Default | Zweck |
|---|---|---|
| `SWARMQ_VALKEY_EVICTION_CHECK` | `strict` | `strict`, `warn` oder `off`: was ein Worker unter einer verdrängenden Valkey-Policy tut (BZ-04) |
| `SWARMQ_NAMESPACE` | ungesetzt | App-Namensraum für jeden Valkey-Key und Kanal; `SwarmQConfig.namespace` (BZ-10) |
| `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` | `SWARMQ_SCHEDULER_LOCK_TTL` | Leader tritt nach gescheiterten Publishes über diese Frist zurück; `0` schaltet ab (BZ-10) |
| `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY` | `auto` | `auto`, `at-least-once` oder `legacy`: Dead-Lettering neuer Quorum-Queues (BZ-19) |
| `SWARMQ_UNKNOWN_TASK_GRACE_S` | `900` | wie lange ein Worker einen unbekannten Task zurückstellt; `0` dead-lettert beim ersten Kontakt (BZ-06) |
| `SWARMQ_LOCK_WAIT_MAX_S` | `21600` | Obergrenze für das Warten auf einen belegten Task-Lock, ab dem ersten Konflikt; `> 0` und endlich (BZ-05) |
| `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` | `604800` | wie lange der Ausführungszustand hinter einem Rückstand lebt (BZ-12) |
| `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` | `86400` | wie lange der Leader einen verzögerten Task erneut versucht, dessen Queue fehlt (BZ-03) |
| `SWARMQ_BROKER_PUBLISH_CONNECTION` | `separate` | `separate` oder `shared`: zweite Broker-Verbindung zum Konsumieren; `SwarmQConfig.broker_publish_connection` (BZ-16) |

Siehe [Konfiguration](configuration.md).

#### CLI

- **`swarmq check [--format (table|json)]`**: prüft vor einem Upgrade nur
  lesend die Umgebung. Abschnitt `valkey`: Eviction-Policy, `maxmemory`,
  Speicher, AOF, Version, Cluster-Modus, `namespace` und
  `unprefixed_keys` (Keys ohne Namensraum neben einem Namensraum, per
  `SCAN` höchstens 5 Sekunden lang gezählt; `unprefixed_keys_complete`
  unterscheidet eine vollständige Zählung von einer Untergrenze, im
  Cluster-Modus wird nur der verbundene Knoten gezählt). Abschnitt
  `shutdown`: `min_grace_period_s`. Abschnitt `queues` (mit
  `SWARMQ_BROKER_URL`): verwaiste Queues und gemeldete Queues ohne
  `.XQ`. Befunde `eviction_policy`, `eviction_policy_unknown`,
  `aof_disabled`, `cluster`, `version`, `unprefixed_keys`,
  `unprefixed_keys_unknown`, `orphaned_queue`,
  `dead_letter_queue_missing`, `queues_unchecked`. Exit `78`, wenn ein
  Worker den Start verweigern würde, sonst `0` (Warnungen erlaubt), `1`,
  wenn Valkey oder ein konfigurierter Broker nicht erreichbar ist
  ([CLI](cli.md#swarmq-check)).
- Hilfetexte (`--max-processes`, `schedule list`) schreiben Keys als
  `[<namensraum>:]sq:…`.

#### Ausnahmen

Alle neuen Broker-Ausnahmen sind Unterklassen von
`BrokerConnectionError`; bestehende Handler fangen sie weiter
([Fehlerkatalog](errors.md)):

- `swarmq.UnroutableMessage`: Der Broker hat einen Publish
  zurückgegeben, weil es keine Queue mit diesem Namen gibt (BZ-03).
- `swarmq.MessageHeadersTooLarge`: Die Header passen auch mit gekürzten
  Texten nicht in die Hälfte des ausgehandelten `frame_max`; nichts
  wurde gesendet (BZ-17).
- `swarmq.exceptions.BrokerPublishBlocked`: Die Verbindung blieb durch
  einen Ressourcenalarm bis zur Publish-Frist blockiert; nichts wurde
  gesendet (BZ-16).
- `swarmq.exceptions.PublishRejected`: Der Broker hat jeden Versuch mit
  einem negativen Confirm beantwortet (`reject-publish` an einer vollen
  Queue, Quorum-Queue ohne Quorum).
- `swarmq.QueueArgumentsMismatch` (Unterklasse von
  `ConfigurationError`): Eine vorhandene Queue hat ein Argument, das von
  der Konfiguration abweicht und das swarmq nicht übernimmt;
  `swarmq worker` endet mit `78` (BZ-15).
- `swarmq.RedeliveryLimitError` und `swarmq.LockWaitExceeded`: werden
  nie geworfen; der `error_type` einer DLQ-Kopie, deren Zustellungen bis
  zum Delivery-Limit zurückgegeben wurden (BZ-29) bzw. deren Wartezeit
  auf einen Task-Lock endete (BZ-05).

#### Signale

([Signale](../guides/signals.md))

- `TASK_UNKNOWN_DEFERRED` (`task_id`, `task_name`, `queue`, `delay`,
  `unknown_since`) (BZ-06).
- `SCHEDULER_FIRE_UNROUTABLE` (`worker_id`, `schedule_name`, `queue`,
  `kind`: `cron_lost` oder `oneshot_dropped`) (BZ-03).
- `SCHEDULER_CRON_UNOWNED` (`worker_id`, `schedule_name`, `task_name`,
  `held`, `held_count`) (BZ-11).
- `SCHEDULER_VALKEY_CHECKED` (BZ-04).
- `SCHEDULER_QUEUE_ORPHANED` (BZ-14).

#### Metriken

([Metrik-Katalog](configuration.md#metrik-katalog))

- `swarmq_valkey_memory_used_ratio{worker_id}` (BZ-04).
- `swarmq_lock_waits{worker_id}` und
  `swarmq_lock_wait_oldest_seconds{worker_id}` (BZ-05).
- `swarmq_tasks_unknown_deferred_total{queue}` (BZ-06).
- `swarmq_scheduler_fires_unroutable_total{queue, kind}` (BZ-03).
- `swarmq_scheduler_crons_held{worker_id}` (BZ-11).
- `swarmq_scheduler_queues_orphaned{worker_id}` (BZ-14).
- `swarmq_delay_tiers_unavailable{queue}` (BZ-01).
- `swarmq_supervisor_locked_slots` (BZ-27).
- `swarmq_worker_prefetch_limit{worker_id}` (BZ-20).
- `swarmq_queue_depth{kind="delayed"}`: nur die Nachrichten in den
  Delay-Queues; `kind="pending"` behält seine Bedeutung (`{queue}` plus
  alle Delay-Queues, jetzt mit den Stufen) (BZ-01).
- `swarmq_queue_depth` und `swarmq_dlq_messages` je konsumierter Queue:
  Mit `SWARMQ_PRIORITIES` tastet der Worker jetzt auch die Bänder
  `{queue}.high` und `{queue}.low` ab.

#### Log-Events

([Logging](logging.md))

- Valkey-Umgebung (BZ-04): `valkey_eviction_policy_unsafe`,
  `valkey_eviction_policy_unknown`, `valkey_data_loss_detected`.
- Locks (BZ-05, BZ-25): `lock_wait_exceeded`,
  `lock_observe_unsupported`, `lock_observe_failed`, `task_lock_lost`,
  `task_lock_held_past_hard_timeout`, `lock_extend_failed`,
  `lock_extend_unsupported`, `lock_keeper_tick_failed`.
- Unbekannte Tasks (BZ-06): `task_unknown_deferred`,
  `unknown_task_defer_failed`, `unknown_task_dlq_failed`.
- Fehlende Queues (BZ-03): `scheduler_fire_unroutable`,
  `scheduler_fire_unroutable_foreign`, `scheduler_oneshot_unroutable`,
  `scheduler_oneshot_unroutable_dropped`,
  `own_queue_unroutable_redeclaring`, `workflow_advance_unroutable`.
- Scheduler (BZ-09, BZ-10, BZ-11, BZ-14, BZ-28):
  `scheduler_unique_release_failed`,
  `scheduler_publish_failures_stepdown`, `scheduler_cron_held_unowned`,
  `scheduler_cron_unowned_released`, `scheduler_unowned_check_skipped`,
  `scheduler_queue_orphaned`, `scheduler_queue_orphan_resolved`,
  `scheduler_queue_check_failed`, `schedule_def_malformed_parked`.
- Queues (BZ-01, BZ-19): `delay_tier_unavailable`,
  `queue_dead_letter_at_most_once`.
- Broker-Verbindungen und DLQ (BZ-16, BZ-21):
  `publish_connection_blocked`, `publish_blocked_state_unreadable`,
  `dlq_read_ack_failed`.
- Weitere: `workflow_state_missing` (BZ-13),
  `client_task_defaults_unresolved` (BZ-24), `shutdown_step_timeout`
  (BZ-18), `worker_slot_unlocked` (BZ-27),
  `prefetch_exceeds_consumer_timeout` (BZ-20).

#### Header, Valkey-Keys und Golden-Dateien

Alles additiv; bestehende Golden-Einträge sind byte-identisch, wo nicht
anders genannt ([Header](agent/message-formats.md#amqp-header)).

- **Header:** `x-swarmq-unknown-since` (BZ-06); `x-swarmq-xdeath-base`,
  `x-swarmq-dlq-gen` (BZ-08); `x-swarmq-lock-conflicts`,
  `x-swarmq-lock-wait-since`, `x-swarmq-lock-holder`,
  `x-swarmq-lock-holder-since`, `x-swarmq-lock-holder-ttl-ms`,
  `x-swarmq-lock-wait-gen` (BZ-05).
- **Valkey-Keys** (jeweils unter `[<namensraum>:]`): `sq:{sched}:epoch`
  (ohne TTL, ein Key; BZ-04), `sq:{sched}:handover` (wenige Sekunden
  TTL; BZ-07), `sq:{sched}:report:known` (Namen, die ein 0.12-Worker
  gemeldet hat; wird mit der Definition entfernt; BZ-11),
  `sq:{sched}:report:queues` (verfällt 7 Tage nach der letzten Meldung;
  BZ-14).
- **Schedule-Definition:** neue Felder `task_id` (BZ-02),
  `unroutable_since` (BZ-03) und `unique_for` (BZ-09), jeweils per
  Default `null`.
- **Golden-Dateien:** `keys.json` (Epoch-, Handover- und Melde-Keys);
  `keys.namespace.json` und `channels.namespace.txt` (neu, Keys und
  Kanäle mit Namensraum; `channels.txt` unverändert); `headers.txt` (die
  Header oben); `lua_contract.txt` (neues `lock_extend`; `guard_claim`
  nimmt ein sechstes Argument; `lock_acquire` verlängert die TTL nur;
  `sched_poll.lua` schreibt jetzt); `queue_args.json` (neue Schlüssel
  `quorum_at_least_once` und `delay_tiers`, neue Beschreibung unter
  `delay`); `values/schedule_definition.json` (neue `null`-Felder).

#### API und Verträge

- `RabbitMQBroker.ready_depth(queue)` (Nachrichten, die in `{queue}`
  selbst bereitliegen), `RabbitMQBroker.publish_connection`, optionale
  Broker-Methode `queue_statuses(names)` mit
  `swarmq.contracts.broker.QueueStatus`.
- `ValkeyBackend(..., namespace=...)`, `ValkeyBackend.namespace`,
  `ValkeyBackend.reconcile_namespace`,
  `ValkeyBackend.eviction_check`/`recheck_eviction`,
  `swarmq.keyspace.validate_namespace`.
- `LockStore.observe` und `LockStore.extend`;
  `ScheduleStore.ensure_epoch`, `report_queues`, `queue_reports`,
  `forget_queue_reports` mit `swarmq.contracts.QueueReports`. Stores und
  Broker ohne die optionalen Methoden lassen die Funktion aus.
- `ProcessManager.SLOT_COOLDOWN_S`, `SlotTable.lock(slot, until=...)`.

### Wirkt erst, wenn …

- **… der Scheduler-Leader 0.12 fährt:** stabile One-Shot-IDs (BZ-02),
  Cron-Dedup aus der Definition (BZ-09), zurückgehaltene Crons (BZ-11),
  die Prüfung verwaister Queues (BZ-14) und der feste Handover-Key
  (BZ-07). Solange ein Leader vor 0.12 die Rolle hält, feuern verzögerte
  Tasks unter einer neuen ID, ein langer Cron-Lauf kann den nächsten
  Termin überlappen, und entfernte Crons feuern wieder in die `.XQ`. Die
  Gauge `swarmq_scheduler_leader_outdated` zeigt es.
- **… jeder Worker einer Queue 0.12 fährt:**
  - die Gnadenfrist für unbekannte Tasks (BZ-06): Ein 0.11-Worker weist
    einen unbekannten Task, auch eine Rückstellungs-Kopie, beim ersten
    Kontakt ab;
  - Wiederholungen nach einem Broker-Dead-Lettering (BZ-08): Eine
    Wiederholung, die einen 0.11-Worker erreicht, wird übersprungen, ein
    wiederholter Cron-Fire ebenso;
  - die Aufbewahrung der Marker hinter einem Rückstand (BZ-12): Ein
    0.11-Worker verkürzt den Zustand beim Schreiben seines Done-Markers
    ohne Bedingung;
  - das Warten auf einen Lock (BZ-05): Ein 0.11-Worker, der eine
    Nachricht selbst zurückstellt, erhöht weiter den alten Zähler und
    dead-lettert beim 10. Konflikt;
  - die Lock-Verlängerung (BZ-25): 0.11-Erwerber eines Semaphors
    verkürzen den Key weiter;
  - zwei Verbindungen (BZ-16): Im Alarm hängen die Acks von
    0.11-Workern wie bisher.
- **… der `swarmq dlq`-Befehl 0.12 ist** (BZ-21): Ein `dlq`-Befehl von
  0.11 liest weiter zerstörend.
- **… die Queue neu ist** (BZ-19): Nur neue Quorum-Queues bekommen
  at-least-once-Dead-Lettering; Bestandsqueues brauchen eine Policy oder
  einen Umzug. Die Delay-Stufen (BZ-01) legt der Worker an, der die
  Queue konsumiert; Producer und Scheduler publizieren nie an eine
  Delay-Queue, und ein Prozess, der die Familie nicht deklariert hat,
  nutzt `{queue}.DQ`.
- **… ein Worker ab 0.12 den Cron bzw. die Queue gemeldet hat:** Crons,
  die nur Worker vor 0.12 melden, werden nicht zurückgehalten (BZ-11),
  Queues, die nur sie konsumieren, nicht geprüft (BZ-14).
- **… die Registry des Clients den Task kennt:** nur dann deckt das
  Cancel-Flag die Wartezeit bis zum Fire eines verzögerten Tasks und
  dessen `max_age` ab (BZ-23).
- **… jede App am Valkey 0.12 fährt**, bevor ein Namensraum gesetzt
  wird (BZ-10).

### Bekannte Grenzen

- **BZ-01:** Über einer Stunde bleibt das Head-of-Line-Blocking von
  `{queue}.DQ`; innerhalb einer Stufe kann eine Wartezeit bis zum
  Vierfachen dauern, mindestens bis zu einer Sekunde.
- **BZ-04:** Ein Datenverlust bei gleichzeitigem Neustart aller Prozesse
  bleibt unerkannt, ebenso ein Verlust seit einem RDB-Snapshot, der den
  Epoch zurückbringt; die Policy wird nur stündlich geprüft.
- **BZ-05/BZ-25:** Gemischte `hard_timeout` unter einem Semaphor-Namen
  werden nicht unterstützt: Ein Erwerber räumt Slots ab, die älter als
  seine eigene TTL sind. Ein blockierter Event-Loop blockiert auch die
  Lock-Verlängerung; der Lock kann ablaufen und wird als
  `task_lock_lost` gemeldet.
- **BZ-08:**
  - Eine Broker-DLX-Wiederholung oder eine Cron-Wiederholung, die einen
    0.11-Worker erreicht, wird übersprungen (Verlust); eine Wiederholung
    in-flight beim Versionswechsel kann doppelt laufen.
  - Nach einem UI-Move zählt die erste Ausführung das rohe `x-death`;
    wiederholte UI-Moves nach erneutem Broker-Dead-Lettering werden
    übersprungen (`swarmq dlq retry` verwenden).
  - `route_via_dlx` nach einem UI-Move trägt den alten
    `x-swarmq-dead-lettered-at` weiter (ein `nack` kann keine Header
    setzen), `dlq list` kann eine solche Nachricht deshalb als
    abgelaufen verwerfen.
  - Eine UI-Wiederholung mit `max_age` kann sofort ablaufen.
  - Mit `dead_letter_ttl_s=None` sind Replay-Identitäten 7 Tage
    geschützt, nicht unbegrenzt.
- **BZ-10:**
  - Bei getrennten vhosts an einem Valkey ist der Namensraum Pflicht.
  - Nach dem Setzen des Namensraums kein Rollback unter 0.12.
  - Der Umzug einer bestehenden App in einen Namensraum braucht einen
    vollständigen Stopp; Ergebnisse, verzögerte Tasks und Locks unter
    `sq:` werden nicht übernommen.
  - Ein Leader älter als 0.12 am selben Valkey feuert die alten
    Definitionen bis zu 24 h weiter; Crons laufen doppelt.
  - Valkey-ACL-Muster und Werkzeuge, die die Keys direkt lesen
    (swarmv), müssen dem Namensraum folgen.
  - `swarmq check` zählt Keys ohne Präfix höchstens 5 s lang (danach
    „mindestens n“) und im Cluster-Modus nur auf dem verbundenen Knoten.
  - Rücktritt: `ACCESS_REFUSED` beim Publish (Schreibrecht des Benutzers
    auf `amq.default`) zählt weiter. Ein toter TCP ohne RST endet
    zunächst als `PublishIndeterminate` und zählt erst, sobald der
    Heartbeat den Verlust erkennt.
- **BZ-11:** Ein zurückgehaltener Cron holt verpasste Termine nicht
  nach.
- **BZ-12:** Ein Rückstand, der erst nach dem Lauf entsteht, ist bei
  Tasks ohne `max_age` nicht abgedeckt. Der Completed-Marker bleibt bei
  7 Tagen gedeckelt; Done- und Superseded-Marker haben keinen Deckel und
  leben bei einem `max_age` über 7 Tagen entsprechend länger.
- **BZ-14:** Eine Queue mit irgendeinem Consumer gilt nie als verwaist;
  eine Queue, deren Worker langsam neu startet, kann bis zum erneuten
  Konsumieren als verwaist gelten.
- **BZ-16:** RabbitMQ meldet die Blockade nur einer Verbindung, die
  während des Alarms publiziert; der erste Publish nach Beginn des
  Alarms endet deshalb weiter als `PublishIndeterminate`. Der
  Blocked-Zustand wird aus einem privaten aiormq-Attribut gelesen; fehlt
  es in einer späteren aiormq-Version, warnt swarmq einmal
  (`publish_blocked_state_unreadable`) und publiziert ungeprüft.
- **BZ-19:** Duplikate laufen bei Tasks ohne Ausführungsschutz doppelt;
  Rollback nur auf 0.11.1 oder neuer; `reject-publish` zusammen mit
  einer `max-length`-Policy lässt Publishes an eine volle Queue
  scheitern.
- **BZ-20:** Der Prefetch gilt je Consumer.
- **BZ-21:** Stirbt der Lesende zwischen Kopie und Bestätigung, liegt
  die Nachricht zweimal in der DLQ (ein wiederholtes `dlq retry` führt
  sie einmal aus); parallele `dlq list` sehen jeweils nur einen Teil der
  DLQ.
- **BZ-23:** Ein sofort publizierter Task ist 24 h stornierbar.
- **KTD11:** Die Gauge `swarmq_scheduler_leader_outdated` bleibt auf
  `1`, solange eine 0.11-App auf demselben Valkey die Führung hält.
- **Delivery-Limit per Policy:** Eine `delivery-limit`-Policy sieht
  swarmq nicht; auf swarmq-Queues wird sie nicht unterstützt.

## [0.11.1] — 2026-10-10

Rollback-Ziel vor 0.12. **Erst alle Apps als eigenes Deployment auf
0.11.1 ausrollen, dann auf 0.12.** Ein Rollback per Kubernetes oder
Docker landet auf dem vorher deployten Image; ist das 0.11.0, scheitert
es an Queues, die 0.12 angelegt hat, und übergeht einen Namensraum
still.

### Geändert

- **Vorhandene Queue-Argumente werden übernommen.** Existiert eine
  Queue schon mit einem anderen `x-dead-letter-strategy` oder
  `x-overflow` (wie 0.12 sie anlegt), deklariert der Worker sie mit den
  vorhandenen Werten, statt mit `PRECONDITION_FAILED` abzubrechen. Ein
  abweichendes `x-delivery-limit` wird ebenfalls übernommen und als
  WARNING `queue_argument_adopted` geloggt: Unter
  `SWARMQ_DELIVERY_LIMIT` dead-lettert der Broker selbst nach dem
  vorhandenen Limit, ohne Fehlerergebnis; darüber dead-lettert weiter
  der Worker bei `SWARMQ_DELIVERY_LIMIT - 1` Zustellungen. Jeder andere
  Argumentkonflikt scheitert wie in 0.11.0, auch eine Queue ohne
  `x-delivery-limit`. Die Übernahme von `x-overflow=reject-publish`
  heißt: Mit einer `max-length`-Policy bekommt ein Publish auf eine
  volle Queue ein Nack statt drop-head und scheitert nach den
  Wiederholversuchen. Jede
  Deklaration wird zuerst auf einem eigenen kurzlebigen Channel
  geprüft; ein Konflikt schließt so nie den Publish-Channel. Queues, die
  der Worker neu anlegt, bekommen genau die Argumente aus 0.11.0
  ([Zustellsemantik](../explanation/delivery-semantics.md#vorhandene-queue-argumente)).
- **`SWARMQ_NAMESPACE` wird abgelehnt.** Ist die Variable gesetzt und
  nicht leer, brechen Worker, Scheduler und `SwarmQ(...)` mit
  `ConfigurationError` ab (`swarmq worker`: Exit 78), ebenso
  `swarmq schedule`, `swarmq dlq` und `swarmq inspect` (Exit 78);
  `swarmq worker --check` meldet ihn als Fehler. Ein Namensraum
  erfordert swarmq >= 0.12; 0.11.x nutzte still die Keys ohne
  Namensraum ([Konfiguration](configuration.md#namensraum-erst-ab-012)).

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
