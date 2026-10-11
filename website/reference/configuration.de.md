# Konfiguration

SwarmQ hat zwei Konfigurationsoberflächen: **Environment-Variablen**,
die der Worker beim Start liest (und die auch die CLI ehrt), und
**`TaskInfo`-Felder**, die pro Task-Klasse deklariert werden.

Die Priorität hängt davon ab, wie SwarmQ läuft:

- **`swarmq worker`**: CLI-Flag, dann Env-Var, dann Default, für jede
  Worker-Option. Der Supervisor löst die Werte einmal auf und gibt sie
  an seine Worker-Subprozesse und Rechenkinder weiter, die das
  Environment nicht noch einmal lesen; `swarmq worker --check` prüft
  dieselben aufgelösten Werte.
- **Eingebettetes `SwarmQ(...)`**: zuerst die Keyword-Argumente von
  `SwarmQConfig`, dann überschreiben `SWARMQ_*`-Env-Vars sie; Defaults
  füllen den Rest.

## Environment-Variablen

### Pflicht

| Variable | Zweck |
|---|---|
| `SWARMQ_BROKER_URL` | AMQP-URL für RabbitMQ, z. B. `amqp://guest:guest@localhost:5672/`. |
| `SWARMQ_BACKEND_URL` | Valkey/Redis-URL für Ergebnisse und Shared State, z. B. `valkey://localhost:6379/0`. |

Die CLI gibt einen beschreibenden Fehler aus, wenn eine davon fehlt;
siehe [Fehlerkatalog](errors.md#configurationerror).

### Worker-Verhalten

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_QUEUES` | `default` | Komma-separierte Queue-Namen, aus denen der Worker konsumiert. |
| `SWARMQ_CONCURRENCY` | `10` (io) / zugeteilte CPUs (cpu) | Maximale Tasks in Flight pro Worker-Prozess. Mit `SWARMQ_POOL=cpu` die Zahl der Rechenslots; siehe [`--concurrency`](cli.md#optionen). |
| `SWARMQ_RESULT_TTL` | `86400` | Sekunden, die gespeicherte Ergebnisse in Valkey leben. |
| `SWARMQ_RETRY_MAX_DELAY` | `3600.0` | Obergrenze des Retry-Backoffs in Sekunden. |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` | Gesamtbudget des Herunterfahrens in Sekunden ab SIGTERM (seit 0.12; vorher nur der Drain): Drain der laufenden Tasks, Sperrenfreigabe, Leader-Übergabe, Verbindungsschluss und unter dem Supervisor dessen SIGTERM-/SIGKILL-Eskalation liegen darin. Der Drain endet 5 s (höchstens die Hälfte) vor dem Ende, unter dem Supervisor endet die Eskalation 2 s davor. Kubernetes `terminationGracePeriodSeconds` und Docker `stop_grace_period` auf mindestens diesen Wert plus 15 s setzen ([Shutdown-Budget](../explanation/delivery-semantics.de.md#shutdown-budget)). |
| `SWARMQ_PUBLISH_TIMEOUT_S` | `120.0` | Obergrenze für einen kompletten Publish-Vorgang inklusive interner Wiederholversuche. Wird sie überschritten, scheitert der Publish mit einem Broker-Verbindungsfehler, statt unbegrenzt auf eine Bestätigung zu warten, die ein still toter Transport nie liefert. Beim Herunterfahren gilt die kürzere aus diesem Wert und der verbleibenden Gnadenfrist. |
| `SWARMQ_DELIVERY_LIMIT` | `20` | Zustellbudget für Quorum-Queues (`x-delivery-limit`); bei klassischen Queues ohne Wirkung. Ist eine Nachricht so oft zugestellt worden, wirft der Broker sie selbst in die Dead-Letter-Queue — das Sicherheitsnetz gegen einen Task, der den Prozess zuverlässig abschießt (SIGSEGV/OOM) und deshalb ewig neu zugestellt würde, ohne je `max_retries` zu erreichen. Der Default deckt drei bis vier fachliche Wiederholungen je Zustellung plus Puffer für Broker-Schluckauf. Dieser Puffer ist seit dem Publish-Deckel schmaler: Bricht ein Publish im Worker an seiner Frist ab, wird die bearbeitete Nachricht neu zugestellt statt bestätigt, und jede Runde kostet eine Zustellung. Vor jeder solchen Rückgabe wartet der Worker bis zu 5 s (wachsend mit dem Zustellzähler), das Default-Budget reicht also mehr als eine Minute. Der Worker zählt auch `x-acquired-count` (RabbitMQ 4.3 zählt eine Rückgabe nicht in `x-delivery-count`). Höher setzen, wer längere Broker-Störungen aussitzen muss; niedriger, wer ein engeres Zustellfenster erzwingen will. Bestehende Queues behalten das Limit, mit dem sie angelegt wurden; der Worker dead-lettert dann beim kleineren der beiden Werte minus eins ([effektives Delivery-Limit](../explanation/delivery-semantics.md#effektives-delivery-limit)). |
| `SWARMQ_QUEUE_TYPE` | `quorum` | `quorum` (empfohlen) oder `classic` RabbitMQ-Queue-Typ. Nur Quorum-Queues tragen `x-delivery-limit`; ohne wird eine Nachricht, die Worker zum Hängen bringt, unbegrenzt erneut zugestellt (der Worker warnt beim Start). Eine Änderung für bestehende Queues scheitert mit `QueueArgumentsMismatch` (Exit 78); stattdessen die Queues [migrieren](../explanation/delivery-semantics.md#queue-argumente-andern). |
| `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY` | `auto` | Dead-Lettering der Quorum-Queues. `auto`: Neue Queues bekommen `x-dead-letter-strategy=at-least-once` und `x-overflow=reject-publish` (nicht `{queue}.XQ`), Bestandsqueues behalten ihre Argumente, und eine WARNING `queue_dead_letter_at_most_once` je Prozess listet die ohne at-least-once. `at-least-once`: wie `auto`, aber eine Bestandsqueue ohne at-least-once (Strategy und `x-overflow=reject-publish`) scheitert mit `QueueArgumentsMismatch` (Exit 78); mit `SWARMQ_QUEUE_TYPE=classic` nicht erlaubt (`ConfigurationError`). `legacy`: Argumente aus 0.11 (at-most-once), keine Warnung ([Garantie beim Dead-Lettering](../explanation/delivery-semantics.md#garantie-beim-dead-lettering)). |
| `SWARMQ_BROKER_PUBLISH_CONNECTION` | `separate` | `separate`: Ein Worker konsumiert und bestätigt über eine zweite RabbitMQ-Verbindung, die sein erster `consume()` öffnet; Publishes (Retry-, Dead-Letter- und Rückstellungskopien, Workflow-Schritte, Cron-Fires) bleiben auf der ersten. Bei einem Ressourcenalarm (Speicher, Platte) blockiert RabbitMQ nur Verbindungen, die publizieren, Acks laufen also weiter. Ein Producer, der nie konsumiert, behält eine Verbindung. `shared`: eine Verbindung wie vor 0.12. Mit injizierter `connection=`/`connection_pool=` gilt immer `shared` ([Ressourcenalarm des Brokers](../explanation/architecture.md#fehlerdomanen)). |
| `SWARMQ_POOL` | `io` | `io` oder `cpu` — siehe [CPU-Modus](../guides/cpu-mode.md). |
| `SWARMQ_RECYCLE_RSS_MB` | ungesetzt (`250` im CPU-Modus) | Prozess nach einem Task recyceln, sobald sein RSS diese MB überschreitet. IO-Worker: Opt-in-Selbst-Recycling; CPU-Modus: Schwelle für wiederverwendete DI-Kinder. |
| `SWARMQ_RECYCLE_MAX_TASKS` | ungesetzt (`1000` im CPU-Modus) | Prozess nach so vielen Tasks recyceln (zweite Linie hinter der RSS-Schwelle). |
| `SWARMQ_HEARTBEAT_INTERVAL_S` | `5` | Wie oft der Worker seinen Lebenszeichen-Zeitstempel für den Supervisor schreibt. |
| `SWARMQ_HEARTBEAT_TIMEOUT_S` | `60` | Nach so langer Zeit ohne Lebenszeichen ersetzt der Supervisor den Worker (SIGTERM, dann SIGKILL). Muss über dem Intervall und jeder legitimen Loop-Blockade liegen; unter RabbitMQs Heartbeat-Erkennung bleiben. |
| `SWARMQ_CONSUMER_TIMEOUT_S` | `1800` | `consumer_timeout` des Brokers; der Worker warnt, wenn ein `hard_timeout` darüber liegt. |
| `SWARMQ_CONSUMER_WATCH_INTERVAL_S` | `5.0` | Sekunden zwischen zwei Prüfungen des [Consumer-Wächters](../explanation/delivery-semantics.md#consumer-timeout): Einen vom Broker beendeten Consumer (`basic.cancel` ab RabbitMQ 4.3, Channel-Close bis 4.2) meldet er binnen zwei Takten ab und neu an. Muss `> 0` sein. |
| `SWARMQ_CONSUMER_READY_GRACE_S` | `30.0` | `/ready` meldet den Worker als nicht bereit, sobald eine konsumierte Queue länger als diese Frist keinen aktiven Consumer hatte. Muss `> 0` sein. `/ready` nie als Liveness-Probe nutzen. |
| `SWARMQ_EXECUTION_GUARD` | `true` | [Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz): Eine erneut zugestellte Nachricht läuft nicht, solange dieselbe Ausführung anderswo läuft, nachdem sie abgeschlossen ist oder nachdem ein Retry sie überholt hat. `false` schaltet ihn ab. |
| `SWARMQ_EXECUTION_GUARD_TTL_S` | `60.0` | TTL des Running-Markers; der Worker verlängert ihn in jedem Drittel der TTL, solange der Task läuft. Nach einem Absturz läuft die Neuzustellung, sobald der Marker abgelaufen ist. Muss `> 0` sein; anheben, wenn Task-Code den Event-Loop länger blockieren kann. |
| `SWARMQ_EXECUTION_GUARD_DONE_TTL_S` | `max(retry_max_delay, consumer_timeout_s) + 300` | Lebensdauer der Done- und Superseded-Marker. Muss mindestens `SWARMQ_EXECUTION_GUARD_TTL_S` betragen. |
| `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` | `3` | Ist dieselbe Ausführung so oft ohne Freigabe abgelaufen, geht die Zustellung als `WorkerHungError` nach `{queue}.XQ`. Muss `> 0` sein. |
| `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` | `604800` | Rückstand, den die Marker überdauern: Sobald eine Neuzustellung einer Ausführung zurückgestellt wurde (sie kommt hinter dem Rückstand der Queue zurück) oder ein Ack scheiterte, lebt der Zustand der Ausführung mindestens so lange (oder `max_age + 300`, falls länger); der Done-Marker einer Wiederholung lebt so lange, wenn `SWARMQ_DEAD_LETTER_TTL_S` `none` ist; die TTL des Completed-Markers eines Schedule-Fires ist hier gedeckelt. Muss `> 0` sein ([Aufbewahrung der Marker](../explanation/delivery-semantics.de.md#ausfuhrungsschutz)). |
| `SWARMQ_DEAD_LETTER_TTL_S` | `604800` | Sekunden, die eine vom Worker dead-gelettete Nachricht in `{queue}.XQ` bleibt ([Aufbewahrungsfrist](../guides/dead-letter-queues.md#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen)); Lesen verlängert sie nicht. `none` hält Nachrichten unbegrenzt. Muss `> 0` sein. Die `swarmq dlq`-CLI behält die eigene Frist jeder Nachricht (`x-swarmq-dead-letter-ttl-ms`) und nutzt diese Variable nur für Altbestand ohne sie. |
| `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES` | `true` | `false` speichert bei einem terminalen Task-Fehler (`NoRetry`, Exception außerhalb von `retry_exceptions`, Retries erschöpft) das Fehlerergebnis und bestätigt die Nachricht, statt sie zu dead-lettern ([Details](../guides/dead-letter-queues.md#dead-lettering-von-task-fehlern-abschalten)). `TaskInfo.dead_letter` gewinnt. |
| `SWARMQ_LOCK_WAIT_MAX_S` | `21600` | Obergrenze in Sekunden, wie lange eine Nachricht auf einen belegten Task-Lock wartet, gemessen ab ihrem ersten Konflikt (`x-swarmq-lock-wait-since`); danach geht sie mit `LockWaitExceeded` in die DLQ. Vorher beendet nur ein hängender Halter (derselbe Halter länger als seine TTL + 60 s) oder `max_age` die Wartezeit. Muss `> 0` und endlich sein ([Lock-Acquisition](../guides/locking-and-dedup.de.md#lock-acquisition-verhalten)). |
| `SWARMQ_VALKEY_EVICTION_CHECK` | `strict` | Was ein Worker tut, wenn Valkey swarmq-Keys verdrängen kann ([Valkey-Anforderungen](#valkey-anforderungen)): `strict` verweigert unter einer verdrängenden `maxmemory-policy` (alles außer `noeviction` mit `maxmemory`-Grenze oder unbekannter Grenze) den Start mit Exit `78`, `warn` loggt `valkey_eviction_policy_unsafe` und startet, `off` prüft gar nicht (keine Abfrage, keine Warnung). Eine nicht lesbare Policy (ACL verbietet `CONFIG` und `INFO`) blockiert nie, sie loggt nur `valkey_eviction_policy_unknown`. Groß-/Kleinschreibung egal. Auch `swarmq check` und jedes andere `ValkeyBackend` (CLI, Producer) lesen die Variable; dort ändert nur `off` etwas. |
| `SWARMQ_NAMESPACE` | *ungesetzt* | Namensraum dieser App auf einem gemeinsamen Valkey ([Namensraum](#namensraum-mehrere-apps-auf-einem-valkey)): Jeder Valkey-Key, jeder Pub/Sub-Kanal, der Scheduler-Leader und der Kontrollkanal wandern von `sq:…` nach `<namensraum>:sq:…`. 1–64 Zeichen `a-z 0-9 _ . -` (nur Kleinbuchstaben: Valkey-Keys unterscheiden Groß- und Kleinschreibung, `App1` und `app1` wären zwei Apps), beginnend mit Buchstabe oder Ziffer, nicht `sq`; alles andere ist ein Konfigurationsfehler (Exit `78`). Leer heißt ungesetzt. Queue-Namen bleiben unverändert. Jedes `ValkeyBackend` (Worker, Producer, CLI, Kontrollkanal) liest die Variable, außer es wird mit `namespace=` gebaut ([Herkunft des Namensraums](#herkunft-des-namensraums)). Nur für eine neue App oder nach dem [Umzug der App](#eine-app-in-einen-namensraum-umziehen) setzen, und erst, wenn alle Komponenten 0.12 fahren. |
| `SWARMQ_UNKNOWN_TASK_GRACE_S` | `900.0` | Gnadenfrist in Sekunden für einen Task-Namen, den dieser Worker nicht registriert (Rolling Deploy: Producer vor den Workern). Innerhalb der Frist stellt der Worker die Nachricht mit Backoff zurück (1 s, dann so lang wie bisher gewartet, höchstens 60 s je Runde) und hält den ersten Kontakt in `x-swarmq-unknown-since` fest; danach dead-lettert er sie mit Fehlerergebnis und `TASK_REJECTED` (`unregistered_task`). `0` schaltet die Frist ab (Dead-Letter beim ersten Kontakt). Muss `>= 0` sein ([unregistrierte Tasks](../guides/dead-letter-queues.de.md#unregistrierte-tasks-und-rolling-deploys)). |
| `SWARMQ_MALLOC_TRIM_THRESHOLD` | `131072` | `MALLOC_TRIM_THRESHOLD_` für Rechenkinder (glibc-Wachstum). |
| `SWARMQ_MALLOC_ARENA_MAX` | `2` | `MALLOC_ARENA_MAX` für Rechenkinder. |
| `SWARMQ_COMPUTE_THREAD_LIMIT` | `1` | Threads je Rechenkind (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`, `MAGICK_THREAD_LIMIT`). |
| `SWARMQ_CANCEL_POLL_INTERVAL` | `1.0` | Sekunden zwischen In-Task-Cancel-Flag-Checks. |
| `SWARMQ_BACKEND_OP_TIMEOUT` | `5.0` | Obergrenze in Sekunden für einzelne Valkey-Operationen in der Worker-Pipeline: Cancel-Check, Dedup-Prüfung und Claim eines Schedule-Fires, Rate-Limit-Prüfung, Lock-Acquire, Lesen des Ergebnisses eines abgebrochenen Workflow-Schritts, Ergebnis-Schreiben, Ergebnis-Benachrichtigung nach einem Abbruch vor dem Start sowie Abschluss oder Freigabe eines Schedule-Fire-Claims. Läuft eine Prüfung vor der Ausführung hinein, gilt das Backend als nicht erreichbar und die Nachricht wird zurückgestellt, statt auf das Socket-Timeout des Valkey-Clients zu warten. Muss `> 0` sein. |
| `SWARMQ_BACKEND_DEFER_BASE_DELAY` | `1.0` | Sekunden, die eine Nachricht nach ihrer ersten Zurückstellung wartet, wenn eine Prüfung vor der Ausführung (Schedule-Fire-Dedup, Cancel-Check, Rate-Limit, Lock) das Backend nicht erreicht. Jede weitere Zurückstellung derselben Nachricht verdoppelt die Wartezeit. Jede Wartezeit streut zufällig um ±20 %, damit gemeinsam zurückgestellte Nachrichten nicht gemeinsam wiederkommen. Die Nachricht geht als neue Nachricht über die Delay-Queue zurück, eine lange Backend-Störung verbraucht also weder `max_retries` noch das Zustelllimit. |
| `SWARMQ_BACKEND_DEFER_MAX_DELAY` | `60.0` | Obergrenze der verdoppelten Wartezeit in Sekunden, vor der Streuung um ±20 % (eine einzelne Wartezeit kann also bis zum 1,2-Fachen dieses Werts betragen); mindestens `SWARMQ_BACKEND_DEFER_BASE_DELAY`. Begrenzt, wie lange eine Nachricht nach der Rückkehr des Backends noch wartet. |
| `SWARMQ_BURST_MODE` | `false` | Bei `true` exited der Worker nach `SWARMQ_BURST_IDLE_TIMEOUT` Sekunden Queue-Stille. |
| `SWARMQ_BURST_IDLE_TIMEOUT` | `5.0` | Sekunden Idle, bevor Burst exitet. |
| `SWARMQ_PRIORITIES` | `false` | Bei `true` werden `.high`/`.normal`/`.low`-Triplets für jede Queue auto-deklariert und konsumiert. |
| `SWARMQ_MAX_MESSAGE_BODY_BYTES` | `16777216` | Obergrenze für den Body einer eingehenden Task-Nachricht (16 MiB). Muss `> 0` sein. Ein größerer Body wird nicht dekodiert: Der Worker loggt `message_body_too_large` und leitet die Nachricht in die DLQ (`TASK_REJECTED` mit Grund `body_too_large`). |

### Logging

Diese Variablen steuern das Logging, das `swarmq worker` in seinen
eigenen Prozessen einrichtet. Eine App, die `SwarmQ(...)` einbettet,
konfiguriert ihr Logging selbst; dort wirken die Variablen nur auf die
[Opt-in-Log-Queue](logging.md#opt-in-log-queue), das Lebenszyklus-Level
eines eingebetteten Workers und die Rechenkinder eines eingebetteten
CPU-Pools. Details, Rezepte und den Event-Katalog findest du in der
[Logging-Referenz](logging.md).

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_LOG_LEVEL` | `INFO` | Einer aus `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root-Level der SwarmQ-Prozesse; mit `SWARMQ_LOG_QUEUE` das Level des Loggers `swarmq`. |
| `SWARMQ_LOG_FORMAT` | `human` (CLI) | `human` oder `json`. Gewinnt gegen `SWARMQ_LOG_JSON`. |
| `SWARMQ_LOG_JSON` | `true` | Alias für das Format: `true`/`1`/`yes` heißt `json`, alles andere `human`. Die Vorgabe `true` ist die von `SwarmQConfig.log_json`; `swarmq worker` nimmt standardmäßig `human`. |
| `SWARMQ_LOG_CONFIG` | nicht gesetzt | Pfad einer `dictConfig`-Datei (JSON; YAML mit PyYAML) statt der eingebauten Einrichtung. Nicht kombinierbar mit einem ausdrücklichen Format oder `SWARMQ_CONFIGURE_LOGGING=false`. |
| `SWARMQ_CONFIGURE_LOGGING` | `true` | `false` überlässt das Logging in jedem SwarmQ-Prozess deiner App. |
| `SWARMQ_LOGGER_LEVELS` | nicht gesetzt | Komma-separierte `NAME=LEVEL`-Paare, z. B. `aio_pika=WARNING,valkey=ERROR`. |
| `SWARMQ_LOG_QUEUE_SIZE` | `10000` | Kapazität der prozessinternen Log-Queue. |
| `SWARMQ_LOG_LIFECYCLE_LEVEL` | `DEBUG` | Level von `task_received` und `task_started`. |
| `SWARMQ_LOG_DRAIN_TIMEOUT` | `5` | Obergrenze in Sekunden für jedes Leeren der Log-Queues (beim Prozessende, vor einem Reload, vor jedem Ergebnis eines Rechenkinds, in `SwarmQ.__aexit__`); nicht rechtzeitig geschriebene Einträge gelten als verworfen. Größer als 0 und höchstens `threading.TIMEOUT_MAX`. Siehe [begrenztes Leeren](logging.md#begrenztes-leeren). |
| `SWARMQ_LOG_QUEUE` | `false` | Eingebettete Apps: `swarmq.*`-Events über einen Hintergrund-Thread von SwarmQ an deine Root-Handler leiten ([Opt-in-Log-Queue](logging.md#opt-in-log-queue)). |

Der Supervisor von `swarmq worker` löst diese Werte einmal auf (CLI-Flag
vor Env-Var) und gibt das Ergebnis an seine Worker-Subprozesse und
Rechenkinder weiter, die das Environment nicht noch einmal lesen.
Dieselbe Reihenfolge CLI > Env > Default gilt für jede andere
Worker-Option mit Flag (siehe [CLI-Referenz](cli.md)). Ungültige Werte
beenden `swarmq worker` mit Exit `78`.

### Scheduler

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_SCHEDULER_POLL_INTERVAL` | `1.0` | Sekunden zwischen Scheduler-Tick-Polls. |
| `SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL` | `10.0` | Sekunden zwischen Leader-Heartbeat-Renews. |
| `SWARMQ_SCHEDULER_LOCK_TTL` | `30.0` | Leader-Lock-TTL; muss `≥ 2 × heartbeat_interval` sein. |
| `SWARMQ_SCHEDULER_DEDUP_TTL` | `max(60, retry_max_delay + 300)` | Dedup-TTL für scheduler-initiierte Dispatches; Untergrenze des Completed-Markers, dessen TTL der Wartezeit des Fires in der Queue folgt (`2 × Wartezeit + 300`, gedeckelt bei `max_age + 300` und `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S`). |
| `SWARMQ_SCHEDULER_INFLIGHT_TTL` | `300.0` | TTL des Schedule-Fire-Claims, den ein Worker hält, während ein geplanter Task läuft. Ein Claim, der nicht freigegeben werden konnte, blockiert eine erneute Zustellung desselben Fires höchstens so lange als Duplikat. |
| `SWARMQ_SCHEDULER_TIMEZONE` | *nicht gesetzt* (UTC) | IANA-Zone für Cron-Ausdrücke von Tasks ohne eigene `TaskInfo.timezone`, z. B. `Europe/Berlin`. `UTC` und `Etc/UTC` bedeuten UTC. Eine unbekannte Zone ist ein Konfigurationsfehler. Im selben Deploy umstellen wie die Ausdrücke, die sie betrifft; besser die Zone pro Task angeben ([Zeitzonen](../guides/scheduling-and-retries.md#zeitzonen)). |
| `SWARMQ_SCHEDULER_SEEN_INTERVAL_S` | `60.0` | Sekunden zwischen [Cron-Meldungen](../guides/scheduling-and-retries.md#cron-meldungen-und-leader): Jeder Worker meldet seine statischen Crons beim Start und danach in diesem Abstand; der Leader übernimmt gemeldete Crons im selben Takt. Eine Meldung lebt drei Takte. Muss `> 0` sein; alle Apps auf einem Valkey sollten denselben Wert nutzen. |
| `SWARMQ_SCHEDULER_PRUNE` | `on` | Was der Leader mit einem statischen Cron tut, der einmal gemeldet wurde und den seit `SWARMQ_SCHEDULER_PRUNE_AFTER_S` kein Worker mehr gemeldet hat: `on` entfernt ihn, `log` loggt nur `scheduler_cron_prune_skipped`, `off` tut nichts. Es gilt der Wert des Scheduler-Leaders, egal aus welcher App: in jeder App auf dem Valkey denselben Wert setzen. Setzt voraus, dass jede App auf dem Valkey mindestens 0.10.0 fährt ([Crons entfernen](../guides/scheduling-and-retries.md#crons-entfernen-die-keine-app-mehr-meldet)). |
| `SWARMQ_SCHEDULER_PRUNE_AFTER_S` | `86400.0` | Frist in Sekunden, nach der ein verstummter statischer Cron entfernt wird. Muss mindestens `3 × SWARMQ_SCHEDULER_SEEN_INTERVAL_S` betragen. Cron-Meldungen werden doppelt so lange aufbewahrt, mindestens 7 Tage. Es gilt der Wert des Scheduler-Leaders: in jeder App auf dem Valkey denselben Wert setzen. |
| `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` | `86400.0` | Wie lange in Sekunden der Leader einen verzögerten Task (`delay=`/`eta=`) erneut versucht, dessen Queue fehlt, gezählt ab dem ersten unroutbaren Versuch; er versucht es alle 60 s und verwirft den Task danach mit einem Fehlerergebnis. Muss > 0 sein. Es gilt der Wert des Scheduler-Leaders ([fehlende Queues](../guides/scheduling-and-retries.md#fehlende-queues)). |
| `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` | `SWARMQ_SCHEDULER_LOCK_TTL` | Der Scheduler-Leader gibt das Leader-Lock frei, wenn mindestens 2 Publishes scheitern (`BrokerConnectionError`, ohne `UnroutableMessage`, `MessageHeadersTooLarge`, `BrokerPublishBlocked`, `PublishRejected` und `PublishIndeterminate`), verteilt über mindestens so viele Sekunden, ohne erfolgreichen Publish dazwischen und ohne eine Lücke länger als diese Frist zwischen zwei Fehlern; eine neue Amtszeit als Leader beginnt eine neue Serie; danach bewirbt er sich `2 × SWARMQ_SCHEDULER_LOCK_TTL` nicht, und ein Worker mit funktionierendem Broker übernimmt (er sendet `SCHEDULER_LEADER_FAILOVER`). `0` schaltet den Rücktritt ab. Muss `≥ 0` sein. `0` setzen, solange Apps älter als 0.12 ohne Namensraum am selben Valkey laufen ([Namensraum](#namensraum-mehrere-apps-auf-einem-valkey)). |

### Observability

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_METRICS_PORT` | *ungesetzt* | **Basisport** für `/metrics`. Der Worker in Slot `s` serviert auf `SWARMQ_METRICS_PORT + s`; ein eingebetteter Worker ohne Slot serviert auf dem Basisport selbst. Braucht `pip install swarmq[metrics]`. Muss `1..65535` sein. |
| `SWARMQ_METRICS_HOST` | `127.0.0.1` | Bind-Adresse für den Metrics-Server. `0.0.0.0` setzen, wenn Prometheus von einem anderen Host oder Container scrapt. |
| `SWARMQ_HEALTH_PORT` | *ungesetzt* | **Basisport** für `/health` und `/ready`, derselbe Slot-Versatz. Muss `1..65535` sein. |
| `SWARMQ_HEALTH_HOST` | `127.0.0.1` | Bind-Adresse für den Health-Server. Für Kubernetes-`httpGet`-Probes `0.0.0.0` setzen: Das Kubelet prüft die Pod-IP, nicht Loopback. |
| `SWARMQ_METRICS_SAMPLE_INTERVAL` | `10.0` | Sekunden zwischen zwei Abtastungen der Gauges `swarmq_queue_depth`, `swarmq_dlq_messages` und `swarmq_worker_uptime_seconds`; derselbe Takt löst `WORKER_HEARTBEAT` aus. Muss `> 0` sein. Der Sampler läuft nur bei eingeschalteten Metriken. |
| `SWARMQ_WORKER_ID` | Hostname | Basis der Worker-Identität, die als `worker_id` in Signalen, Metrik-Labels und im Logkontext erscheint. Siehe [Worker-Identität](#worker-identitat). |

#### Metrik-Katalog

Jeder Worker mit eingeschalteten Metriken (`--metrics-port`/`SWARMQ_METRICS_PORT`,
`pip install 'swarmq[metrics]'`) stellt diese Reihen unter `/metrics` bereit:

| Metrik | Typ | Labels | Beschreibung |
|---|---|---|---|
| `swarmq_tasks_total` | Counter | `task_name`, `queue`, `outcome` | Tasks nach Ausgang: `completed`, `failed`, `cancelled`, `rate_limited`, `duplicate`, `expired`, `dlq`. `failed` zählt jeden gescheiterten Versuch, `dlq` die endgültige Übergabe an die DLQ. |
| `swarmq_task_duration_seconds` | Histogram | `task_name`, `queue` | Dauer von `run()` bei erfolgreichen Tasks. Buckets von 5 ms bis 300 s. |
| `swarmq_tasks_active` | Gauge | `queue` | Gerade laufende Tasks. |
| `swarmq_retries_total` | Counter | `task_name`, `queue` | Eingeplante Retries. |
| `swarmq_tasks_rejected_total` | Counter | `queue`, `reason` | Nachrichten, die ohne Diagnose-Header an den Broker-DLX gingen (`TASK_REJECTED`), z. B. `body_too_large`, `undecodable`; `unregistered_task` zählt einen nach `SWARMQ_UNKNOWN_TASK_GRACE_S` dead-geletterten Task. |
| `swarmq_tasks_unknown_deferred_total` | Counter | `queue` | Nachrichten, die der Worker zurückgestellt hat, weil er den Task nicht registriert, innerhalb von `SWARMQ_UNKNOWN_TASK_GRACE_S` (`TASK_UNKNOWN_DEFERRED`; [unregistrierte Tasks](../guides/dead-letter-queues.de.md#unregistrierte-tasks-und-rolling-deploys)). |
| `swarmq_recyclings_total` | Counter | `kind`, `reason` | Prozess-Recyclings; `kind` ist `worker` oder `compute_child`. |
| `swarmq_queue_depth` | Gauge | `queue`, `kind` | Vom Broker gemeldete Nachrichtenzahl, alle `SWARMQ_METRICS_SAMPLE_INTERVAL` abgetastet: `kind="pending"` ist `{queue}` plus alle Delay-Queues, `kind="delayed"` (seit 0.12) nur die Delay-Queues ([Delay-Stufen](../explanation/delivery-semantics.md#delay-stufen)). |
| `swarmq_dlq_messages` | Gauge | `queue` | Nachrichten in `{queue}.XQ`, abgetastet. |
| `swarmq_lock_waits` | Gauge | `worker_id` | Nachrichten, die dieser Worker wegen eines belegten Task-Locks zurückgestellt und in den letzten 3 Minuten wieder gesehen hat. Eine Nachricht, die länger in der Haupt-Queue wartet (ein Rückstand von mehr als etwa 2 Minuten zusätzlich zur Rückstellung von höchstens 60 s), fällt heraus; bei Rückstand zählt die Gauge also zu wenig. |
| `swarmq_lock_wait_oldest_seconds` | Gauge | `worker_id` | Sekunden, die die älteste dieser Nachrichten seit ihrem ersten Lock-Konflikt wartet. |
| `swarmq_supervisor_locked_slots` | Gauge | `worker_id` | Worker-Slots, die der Supervisor nach 5 Abstürzen oder zu vielen Hängern binnen einer Stunde gesperrt hat; jede Sperre dauert eine Abkühlzeit von 1 h (seit 0.12). Nur unter dem Supervisor (`swarmq worker`); alle Worker eines Supervisors melden denselben Wert. |
| `swarmq_worker_prefetch_limit` | Gauge | `worker_id` | Unbestätigte Nachrichten, die dieser Worker halten kann: konsumierte Queues (jedes Prioritätsband zählt) × `concurrency`, weil der Prefetch je Consumer gilt (seit 0.12, [Prefetch je Consumer](../explanation/delivery-semantics.de.md#prefetch-je-consumer)). |
| `swarmq_delay_tiers_unavailable` | Gauge | `queue` | Delay-Stufen der Queue, deren Deklaration der Broker abgelehnt hat; ihre Wartezeiten nutzen `{queue}.DQ` ([Delay-Stufen](../explanation/delivery-semantics.md#delay-stufen)). |
| `swarmq_worker_uptime_seconds` | Gauge | `worker_id` | Sekunden seit Worker-Start, abgetastet. |
| `swarmq_scheduler_leader` | Gauge | `worker_id` | `1`, solange dieser Worker den Scheduler-Leader-Lock hält, sonst `0`. |
| `swarmq_scheduler_leader_outdated` | Gauge | `worker_id` | `1`, solange dieser Worker einen Scheduler-Leader mit swarmq unter der Mindestversion für Leader (derzeit 0.12) sieht, sonst `0` ([Leader-Version](../guides/scheduling-and-retries.md#leader-version)). |
| `swarmq_scheduler_cron_conflicts_total` | Counter | `schedule_name` | Auswahltakte des Leaders, in denen mehrere Definitionen eines statischen Crons länger als 10 Minuten gleichzeitig lebten ([Konflikte](../guides/scheduling-and-retries.md#konflikte)). |
| `swarmq_scheduler_crons_held` | Gauge | `worker_id` | Statische Crons, die der Scheduler-Leader zurückhält, weil kein lebender Worker sie meldet und er sie selbst nicht registriert; `0`, wenn dieser Worker nicht Leader ist ([zurückgehaltene Crons](../guides/scheduling-and-retries.de.md#crons-die-niemand-meldet-werden-zuruckgehalten)). |
| `swarmq_scheduler_queues_orphaned` | Gauge | `worker_id` | Gemeldete Queues, die der Scheduler-Leader mit Nachrichten, ohne Consumer und ohne lebenden Worker, der sie meldet, findet (Signal `SCHEDULER_QUEUE_ORPHANED`); `0`, wenn dieser Worker nicht Leader ist. Bei `> 0` alarmieren ([Task in eine andere Queue verschieben](../guides/defining-tasks.de.md#task-in-eine-andere-queue-verschieben)). |
| `swarmq_valkey_memory_used_ratio` | Gauge | `worker_id` | Valkey `used_memory / maxmemory`, vom Scheduler-Leader höchstens einmal je Stunde gemessen (Signal `SCHEDULER_VALKEY_CHECKED`); nur mit `maxmemory`-Grenze. Unter `noeviction` verweigert ein volles Valkey Schreibzugriffe (OOM) für jede App darauf; deutlich vor `1` alarmieren ([Valkey-Anforderungen](#valkey-anforderungen)). |
| `swarmq_scheduler_fires_unroutable_total` | Counter | `queue`, `kind` | Scheduler-Fires, die der Broker zurückgab, weil die Queue fehlt: `cron_lost` (verlorener Cron-Fire) oder `oneshot_dropped` (verzögerter Task nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` verworfen) ([fehlende Queues](../guides/scheduling-and-retries.md#fehlende-queues)). |
| `swarmq_consumer_reregistrations_total` | Counter | `queue` | Consumer, die der [Consumer-Wächter](../explanation/delivery-semantics.md#consumer-timeout) neu angemeldet hat, nachdem der Broker sie beendet hatte; der Grund steht im Log-Event `consumer_reregistered`. |
| `swarmq_execution_guard_events_total` | Counter | `event` | Ereignisse des [Ausführungsschutzes](../explanation/delivery-semantics.md#ausfuhrungsschutz): `duplicate_skipped` (Neuzustellung bestätigt und übersprungen), `deferred` (dieselbe Ausführung läuft anderswo), `orphan_limit` (als `WorkerHungError` dead-gelettert), `lease_lost` (Running-Marker während der Ausführung abgelaufen oder übernommen), `defer_stalled` (die Zurückstellungen einer Neuzustellung überschreiten `SWARMQ_CONSUMER_TIMEOUT_S`; das Original läuft ungewöhnlich lange oder hängt). |
| `swarmq_log_records_dropped_total` | Counter | keine | Log-Einträge, die verworfen wurden, weil die Log-Queue voll war (siehe [Logging](logging.md#nicht-blockierende-ausgabe-und-verworfene-eintrage)). |

`task_name` und `queue` kommen unverändert aus der Nachricht; halte
Task-Namen begrenzt, sonst erzeugt jeder neue Wert eine eigene Reihe.

#### Basisports und Slots

Der Supervisor betreibt seine Worker in den Slots `0..N-1`, wobei `N`
`--processes` oder die Obergrenze von `--autoscale` ist. Jeder Slot
bindet eigene Beobachtungsports, abgeleitet vom Basisport:

| Slot | `/metrics` mit `SWARMQ_METRICS_PORT=9000` | `/health` mit `SWARMQ_HEALTH_PORT=9100` |
|---|---|---|
| `0` | `9000` | `9100` |
| `1` | `9001` | `9101` |
| `2` | `9002` | `9102` |

Ein Slot behält seine Ports für die Dauer des Laufs: Ein Worker, der
abstürzt, hängt oder sich recycelt, wird im selben Slot ersetzt, und
der Ersatz bindet denselben Port erneut (mit Wiederholung, während der
Vorgänger ihn freigibt). Scrape-Targets bleiben damit über Neustarts
hinweg stabil.

Zwei Konfigurationen werden beim Start mit `ConfigurationError`
(Exit `78`) abgelehnt, und `swarmq worker --check` meldet dasselbe:

- ein Bereich `[basis, basis + N)`, der über `65535` hinausreicht —
  Basisport senken oder weniger Prozesse fahren;
- Metrik- und Health-Bereich, die sich überschneiden — einen Basisport
  mindestens `N` Ports weit verschieben.

Das `--check`-Payload nennt die genauen Ports als `metrics_ports` und
`health_ports`, damit niemand sie selbst ausrechnen muss.

#### Prometheus-Beispiel

Ein Target je Slot; der Bind-Host muss von Prometheus aus erreichbar
sein (`SWARMQ_METRICS_HOST=0.0.0.0`, wenn es über das Netz scrapt):

```yaml
scrape_configs:
  - job_name: swarmq
    static_configs:
      - targets:
          - worker-host:9000   # Slot 0
          - worker-host:9001   # Slot 1
          - worker-host:9002   # Slot 2
```

Mit `--autoscale MIN,MAX` alle `MAX` Targets eintragen; nicht laufende
Slots erscheinen als `down`, was bei heruntergefahrenem Pool erwartet
ist.

### Worker-Identität

Jeder Worker trägt eine stabile Identität im Feld `worker_id` der
Signale `WORKER_*`, `SCHEDULER_*` und `WORKER_RECYCLING`, in den Labels
von `swarmq_worker_uptime_seconds` und `swarmq_scheduler_leader` und im
strukturierten Logkontext:

| Läuft als | `worker_id` | Beispiel |
|---|---|---|
| Unter dem Supervisor (`swarmq worker`) | `<basis>:<slot>` | `web-3:0`, `web-3:1` |
| Eingebettet (`await app.start_worker()`) | `<basis>:p<pid>` | `web-3:p41872` |

`<basis>` ist `SWARMQ_WORKER_ID` (oder das kwarg `worker_id=` von
`SwarmQConfig`); Standard ist der Hostname. Rechenkinder im CPU-Modus
melden die Identität ihres Loop-Prozesses, `WORKER_RECYCLING` mit
`kind="compute_child"` trägt also ebenfalls den Slot. In Dashboards ist
der Slot die stabile Dimension: Die Uptime eines Slots beginnt beim
Ersatz seines Workers von vorn, seine Identität bleibt.

Alle Ports werden zur Config-Zeit validiert: ein Bad Value raist
`ConfigurationError`, bevor der Worker versucht zu binden.

### CLI-Output

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_DEFAULT_FORMAT` | *TTY-Detect* | Überschreibt den Default für `--format` bei Output-orientierten CLI-Subcommands (`schedule list/add/remove/status`, `dlq list/inspect/retry/purge`, `inspect`). Akzeptiert `table` oder `json`; jeder andere Wert (Tippfehler, Leerstring) wird still ignoriert und der TTY-abhängige Default greift (`table` am Terminal, `json` in einer Pipe). Ein explizites `--format` auf der Kommandozeile gewinnt immer. |

### Valkey-Anforderungen

swarmq hält in Valkey Zustand, der nie von selbst verschwinden darf:
Task-Locks, den Leader-Lock des Schedulers, den Ausführungsschutz,
Unique- und Fired-Marker, Zeitpläne und verzögerte One-Shots. Valkey
(ab 8) deshalb so betreiben:

- **`maxmemory-policy noeviction`.** Jede andere Policy lässt Valkey
  unter Speicherdruck Keys verdrängen; `volatile-*` verdrängt genau die
  Keys mit TTL, also Locks und Marker. Folge sind doppelt laufende
  Tasks. Mit `noeviction` verweigert ein volles Valkey stattdessen
  Schreibzugriffe (OOM, für swarmq ein Ausfall);
  `swarmq_valkey_memory_used_ratio` beobachten. Ohne `maxmemory`-Grenze
  (`maxmemory 0`) verdrängt Valkey nie, gleich welche Policy.
- **AOF** (`appendonly yes`, `appendfsync everysec`). Ohne AOF verliert
  ein Neustart alles seit dem letzten RDB-Snapshot: verzögerte Tasks,
  CLI-Crons und Abschaltungen per `swarmq schedule remove` (die nächste
  Meldung bringt einen solchen Cron aktiv zurück).
- **Kein Cluster-Modus.** Ein einzelner Primary, bei Bedarf mit Replikas.
- **Managed-Dienste:** Manche legen die Policy fest (ElastiCache
  Serverless läuft mit `volatile-lru`); sie sind ungeeignet. Wo eine ACL
  `CONFIG` verbietet, liest swarmq `INFO memory`.

Ein Worker liest `maxmemory-policy` und `maxmemory` beim Verbinden
(höchstens 2 s, scheitert nie an der Prüfung) und verweigert mit
`SWARMQ_VALKEY_EVICTION_CHECK=strict` (Default) unter einer
verdrängenden Policy den Start mit Exit `78`. Der Scheduler-Leader prüft
höchstens einmal je Stunde erneut und loggt `valkey_eviction_policy_unsafe`,
wenn sich die Policy geändert hat. Jeder Worker merkt sich den
Epoch-Key `[<namensraum>:]sq:{sched}:epoch`; verschwindet er (Valkey hat seine Daten
verloren: Neustart ohne Persistenz, `FLUSHALL`, Failover mit verlorenen
Writes), loggt er `valkey_data_loss_detected`. Grenzen: Ein Datenverlust
bei gleichzeitigem Neustart aller Prozesse bleibt unerkannt, ebenso ein
Verlust seit einem RDB-Snapshot, der den Epoch mit zurückbringt, und die
Policy wird nur stündlich geprüft.

**Vor dem Upgrade auf 0.12** `swarmq check` in der neuen Version in einem
eigenen virtualenv gegen Produktion laufen lassen (nur lesend, siehe
[CLI](cli.de.md#swarmq-check)); unter Kubernetes optional als
Init-Container. Exit `78` heißt: Die neuen Worker würden den Start
verweigern. Das nicht erst beim Ausrollen herausfinden: Unter Kubernetes
führt Exit `78` zu `CrashLoopBackOff`, unter Docker mit
`restart:`-Policy zu einer Neustartschleife. Die Policy korrigieren oder
bewusst `SWARMQ_VALKEY_EVICTION_CHECK=warn` setzen.

### Namensraum (mehrere Apps auf einem Valkey)

Ohne Namensraum teilen sich alle Apps eines Valkey die Keys unter `sq:`:
Rate-Limits und Task-Locks gleichen Namens, Unique-Hashes und einen
Scheduler-Leader für alle Apps. Dieser Leader publiziert die Crons
jeder App über seinen eigenen Broker. `SWARMQ_NAMESPACE` je App trennt
sie: Jeder Key und Pub/Sub-Kanal, das Leader-Lock und der Kontrollkanal
liegen dann unter `<namensraum>:sq:…` (die Hash-Tags wie `{sched}`
bleiben gleich). Queue-Namen bekommen kein Präfix: Jede App braucht
ihren eigenen RabbitMQ-vhost.

- **Getrennte vhosts brauchen einen Namensraum.** Mit einem Leader für
  alle Apps geht ein Cron von App B über den Broker von App A; auf einem
  anderen vhost fehlt dort die Queue, und der Fire ist verloren
  (`scheduler_fire_unroutable_foreign`).
- **Ohne Namensraum** tritt der Leader nach wiederholten
  Publish-Fehlern zurück (`SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S`); ein
  Leader mit defektem Broker hält die Crons aller Apps nicht mehr an.
- **Der Namensraum migriert keine Daten.** Keys unter `sq:` (Schedules,
  CLI-Crons, verzögerte Tasks, Ergebnisse, Locks) sind unter dem neuen
  Namensraum nicht sichtbar. Für eine neue App setzen oder die App wie
  unter [Eine App in einen Namensraum umziehen](#eine-app-in-einen-namensraum-umziehen)
  umziehen. `swarmq check` zeigt den Namensraum und zählt Keys ohne ihn
  ([CLI](cli.md#swarmq-check)).
- **Rollout:** erst alle Komponenten (Worker, Producer, CLI,
  Steuerdienst) auf 0.12, dann den Namensraum überall im selben Schritt
  setzen. Gemischte Werte spalten die App: Ein Producer ohne Namensraum
  schreibt Ergebnisse und Termine dorthin, wo der Worker nicht sucht.
- **Rollback:** Nach dem Setzen des Namensraums nur noch auf 0.12 oder
  neuer. 0.11.1 verweigert den Start mit gesetztem `SWARMQ_NAMESPACE`
  (siehe unten); 0.11.0 ignorierte ihn still.
- **Solange Apps älter als 0.12 ohne Namensraum am Valkey laufen**,
  `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0` setzen. Sonst kann die Führung
  zu einer 0.11-App wandern, und die 0.12-Funktionen des Leaders (stabile
  Oneshot-IDs, Cron-Dedup, zurückgehaltene Crons) sind aus, solange sie
  führt.

#### Herkunft des Namensraums

Der Namensraum des Backends gilt überall: für jeden Key und Kanal und
für die Hinweise in Log-Ereignissen und in `swarmq check`.

1. **`ValkeyBackend(..., namespace="app1")`** (oder `keys=`) setzt ihn
   ausdrücklich. `namespace=""` heißt ausdrücklich *kein Namensraum*.
2. **Sonst gilt die Konfiguration:** `SWARMQ_NAMESPACE` oder
   `SwarmQ(broker, backend, namespace="app1")` im Code (die
   Umgebungsvariable schlägt den Code-Wert, wie bei jeder Einstellung).
   Ein Backend ohne `namespace=` übernimmt diesen Wert, sobald es an
   `SwarmQ(...)` geht; für sich allein (CLI, Kontrollkanal) liest es
   `SWARMQ_NAMESPACE`.
3. Mit ausdrücklichem Backend-Parameter und ohne konfigurierten
   Namensraum füllt `SwarmQ(...)` `app.config.namespace` aus dem Backend.

Sind Backend-Parameter und Konfiguration beide gesetzt und verschieden
-- auch `namespace=""` neben gesetztem `SWARMQ_NAMESPACE` --, wirft
`SwarmQ(...)` `ConfigurationError` (`namespace mismatch`; ein Worker
endet mit Exit `78`).

#### Eine App in einen Namensraum umziehen

Der Namensraum zieht keine Daten um, und alte und neue Prozesse einer
App dürfen nie nebeneinander laufen. Für eine bestehende App ohne
Namensraum:

1. **Die App vollständig stoppen**: alle Worker und Scheduler. Unter
   Kubernetes `strategy: Recreate` verwenden oder das Deployment auf 0
   skalieren, unter Docker alle ihre Container stoppen. Ein Rolling
   Update ließe Pods mit und ohne Namensraum gleichzeitig laufen: zwei
   Leader, doppelt gefeuerte Crons, Ergebnisse dort, wo die andere Hälfte
   nicht sucht.
2. **Ohne `SWARMQ_NAMESPACE`** die Termine der App mit `swarmq schedule
   list` auflisten und jeden Cron und Oneshot der App mit `swarmq
   schedule remove --permanent <name>` löschen. Bleiben sie unter `sq:`,
   feuert sie der Leader einer anderen App ohne Namensraum weiter.
3. **Mit `SWARMQ_NAMESPACE`** die CLI-Crons und -Oneshots der App neu
   anlegen (`swarmq schedule add …`). Im Code definierte Crons melden
   sich beim Start der Worker selbst an.
4. **Vorher alle Apps an diesem Valkey auf 0.12 bringen.** Ein Leader
   älter als 0.12 feuert die alten Definitionen, die er gesehen hat, bis
   zu 24 Stunden weiter (bis er sie aufräumt); Crons laufen dann doppelt.
5. **Valkey-ACLs:** Key-Muster `~sq:*` werden zu `~<namensraum>:sq:*`,
   Kanal-Muster `&sq:*` zu `&<namensraum>:sq:*`.
6. **Werkzeuge, die swarmq-Keys direkt lesen** (Dashboards wie swarmv,
   Skripte mit `valkey-cli`), müssen den Namensraum ebenfalls verwenden.

Danach die App mit Namensraum starten. `swarmq check` mit gesetztem
`SWARMQ_NAMESPACE` zählt die Keys, die noch ohne Namensraum übrig sind.

### Namensraum erst ab 0.12

`SWARMQ_NAMESPACE` gehört zu swarmq 0.12 und später. 0.11.1 lehnt die
Variable ab: Ist sie gesetzt und nicht leer, brechen Worker, Scheduler
und `SwarmQ(...)` mit `ConfigurationError` ab (`swarmq worker`:
Exit 78), ebenso die CLI-Befehle `swarmq schedule`, `swarmq dlq` und
`swarmq inspect` (Exit 78, vor jeder Verbindung). `swarmq worker
--check` meldet ihn als Fehler (Exit 1). Ohne diese Prüfung liefe ein
Rollback auf 0.11.x still ohne Namensraum, auf anderen Valkey-Keys. Ab
0.12 wird die Variable unterstützt; nur ein ungültiger Wert stoppt diese
Einstiege (Exit `78`; `swarmq worker --check`: Exit 1).

## `SwarmQConfig`-kwargs

Programmatisches Äquivalent der Env-Vars — übergib sie als
Keyword-Argumente an `SwarmQ(broker, backend, **kwargs)`:

```python
app = SwarmQ(
    broker, backend,
    queues=["emails", "reports"],
    concurrency=50,
    result_ttl=3600,
    shutdown_timeout=60,
    burst_mode=False,
    priorities=True,
)
```

Feldnamen entsprechen den Env-Vars ohne `SWARMQ_`-Präfix und in
Kleinbuchstaben. In einer eingebetteten App gewinnen Env-Vars immer,
wenn beides gesetzt ist (`swarmq worker` löst dagegen CLI-Flags vor
Env-Vars auf) — übergib kwargs nur, wenn du einen Nicht-Default brauchst, den du
nicht im Shell-Environment willst. Eine Ausnahme von der Namensregel:
`SWARMQ_LOG_FORMAT` setzt das Feld `log_json`. `worker_id=` setzt die Basis der
[Worker-Identität](#worker-identitat); ein eingebetteter Worker hat
keinen Slot, `metrics_port` und `health_port` werden also unverändert
gebunden.

`SwarmQ(...)` konfiguriert kein Logging. Die Logging-Felder
(`log_level`, `log_json`, `log_config`, `configure_logging`,
`logger_levels`, `log_queue_size`, `log_lifecycle_level`, `log_queue`,
`log_drain_timeout`) steuern nur die
[Opt-in-Log-Queue](logging.md#opt-in-log-queue), das Lebenszyklus-Level
eines eingebetteten Workers und die Rechenkinder eines eingebetteten
CPU-Pools. Mit `log_queue=True` begrenzt `log_drain_timeout` das Leeren
in `__aexit__` und beim Prozessende; ein ungültiger Wert wirft
`ConfigurationError`.

## `TaskInfo`-Felder

Auf jeder `Task`-Subklasse via `task_info = TaskInfo(...)`
deklariert. Die `TaskInfo`-Dataclass ist **frozen**: Felder werden
einmal zur Klassendefinition gesetzt und können danach nicht
mutiert werden.

### Identität und Routing

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `name` | `str` | *pflicht* | Kanonischer Task-Name, den der Client nutzt. Max 255 Zeichen. |
| `queues` | `list[str]` | *pflicht, ≥1* | Queue-Namen, auf die der Task publisht wird. Der Client nutzt `queues[0]`, außer ein expliziter `queue=` wird übergeben. |
| `priority` | `"high" \| "normal" \| "low"` | `"normal"` | Mit `--priorities` routet zu `{queue}.high`/`.low`. |
| `delivery` | `DeliveryMode` | `AT_LEAST_ONCE` | Alternative: `FIRE_AND_FORGET`. |

### Timeouts

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `soft_timeout` | `int \| None` | `None` | Sekunden, nach denen der Worker `SoftTimeoutError` im Task raist. |
| `hard_timeout` | `int \| None` | `None` | Sekunden, nach denen der Worker die Task-Coroutine abbricht. Muss `> soft_timeout` sein, wenn beides gesetzt. Pflicht, wenn `lock` gesetzt ist. |

### Retries

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `max_retries` | `int` | `0` | Anzahl Retry-Versuche vor DLQ. `0` heißt keine Retries. |
| `retry_backoff` | `str` | `"exponential"` | Einer aus `"exponential"`, `"linear"`, `"fixed"`. |
| `retry_base_delay` | `float` | `1.0` | Basis-Delay (Sekunden) für die Backoff-Formel. Muss `≥ 0` sein. |
| `retry_exceptions` | `tuple[type[BaseException], ...] \| None` | `None` | Whitelist — nur diese Exceptions triggern einen Retry. `None` retry't jede Exception. |
| `dead_letter` | `bool \| None` | `None` | Terminale Fehler dieses Tasks dead-lettern (`NoRetry`, Exception außerhalb von `retry_exceptions`, Retries erschöpft). `False` speichert stattdessen das Fehlerergebnis und bestätigt; `None` folgt `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES`. Siehe [Dead-Letter-Queues](../guides/dead-letter-queues.md#dead-lettering-von-task-fehlern-abschalten). |

### Rate-Limiting

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `rate_limit` | `str \| None` | `None` | Celery-Style-String wie `"100/m"`, `"10/s"`, `"1000/h"`. Validiert zur Klassendefinition. |

### Locking und Concurrency

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `lock` | `str \| None` | `None` | Lock-Name; kann ein Format-String wie `"user-{user_id}"` sein. Braucht `hard_timeout`. |
| `max_concurrency` | `int` | `1` | Mit `lock`: Max nebenläufige Ausführungen. `1` ist Mutex; `> 1` ist Semaphor. Braucht `lock`. |

### Scheduling (Cron)

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `cron` | `str \| None` | `None` | Standard-5-Feld-Cron-Expression. Extended Syntax (`L`, `W`, `#`) wird abgelehnt. |
| `catchup` | `bool` | `False` | Für ein künftiges Release reserviert. `True` raist `ValueError`. |
| `timezone` | `str \| None` | `None` | IANA-Zone, in der `cron` ausgewertet wird, z. B. `"Europe/Berlin"`. `None` fällt auf `SWARMQ_SCHEDULER_TIMEZONE` zurück, sonst UTC; ein ausdrückliches `"UTC"` schlägt diesen Standard. Braucht `cron`; eine unbekannte Zone wirft `ValueError`. Siehe [Zeitzonen](../guides/scheduling-and-retries.md#zeitzonen). |

### Deduplizierung

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `unique_for` | `int \| None` | `None` | Sekunden; aktiviert Dedup auf `(task_name, args, kwargs)` innerhalb der TTL. `0` abgelehnt. |
| `unique_until` | `"start" \| "completion"` | `"completion"` | Wann der Dedup-Flag freigegeben wird. `"start"` bei Worker-Pickup; `"completion"` bei Terminalzustand. |

### Expiry

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `max_age` | `int \| None` | `None` | Sekunden; Worker routet bei Pickup in die DLQ mit `TASK_EXPIRED`, wenn Alter diesen Wert überschreitet. Muss `> 0` sein, wenn gesetzt. |

### Result-Storage

| Feld | Typ | Default | Beschreibung |
|---|---|---|---|
| `result_ttl` | `int \| None` | `None` | Per-Task-Override von `SwarmQConfig.result_ttl`. |

## Validierungs-Verhalten

`TaskInfo.__post_init__` validiert jedes Feld zur Klassendefinitions-
zeit. Ein Bad Value raist `ValueError` beim Import deines Task-
Moduls, nicht beim ersten `schedule()`-Aufruf. Das ist Absicht:
Ein fehlkonfigurierter Task soll den Worker-Start scheitern lassen,
nicht den Request des Nutzers drei Stunden später.

Beispiele für Fehler, die beim Import auftreten:

- `queues` leer → `ValueError("queues must have at least 1 element")`.
- `rate_limit="100"` → `ValueError` von `parse_rate_limit` ("missing unit").
- `lock="user-{}"` mit `hard_timeout=None` → `ValueError` nennt den Lock-TTL-Vertrag.
- `catchup=True` → `ValueError("catchup=True is reserved for a future release")`.
- `max_concurrency=2` ohne `lock` → `ValueError("max_concurrency requires lock to be set")`.
- `unique_for=0` → `ValueError("unique_for=0 is invalid; use None to disable dedup")`.

Die betroffene `TaskInfo`-Zeile fixen und erneut laufen lassen —
kein Worker-Reset nötig, weil der Fehler verhindert, dass der
Worker überhaupt startet.

## Siehe auch

- [CLI-Referenz](cli.md) — die Flag-Äquivalente für die Worker-Env-Vars
- [Logging](logging.md) — Logger-Namen, Felder, Events und
  Integrationsrezepte
- [API-Referenz](api.md) — `SwarmQ`, `TaskInfo`, `Task`-Signaturen
- [Tasks definieren](../guides/defining-tasks.md) — Beispiele je
  `TaskInfo`-Feld
