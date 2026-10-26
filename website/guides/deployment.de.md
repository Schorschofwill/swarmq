# Ausrollen und Upgraden

Wie du swarmq in Produktion ausrollst und auf 0.14.4 upgradest, ohne
Tasks zu verlieren oder doppelt auszuführen. Die Schritte stehen in
ihrer Reihenfolge; jeder verlinkt die Referenzseite mit den
Einzelheiten.

## Voraussetzungen

- **Versionen:** Python 3.14, RabbitMQ 4.2 oder 4.3, Valkey 8. Ältere
  Broker und Valkey-Server sind nicht getestet.
- **Jede App, die den RabbitMQ-vhost oder die Valkey-Datenbank
  mitnutzt,** gehört zum Rollout. Die Schritte gelten für alle, nicht
  nur für die App, die du änderst.
- **Rollback-Plan:** Du weißt, auf welches Image Kubernetes oder Docker
  bei einem Rollback zurückgeht ([Schritt 6](#schritt-6-rollback-regeln)).

## Schritt 0: die neue Version vor dem Rollout prüfen

Installiere die neue Version in einem eigenen virtualenv und führe zwei
lesende Prüfungen gegen Produktion aus, bevor du etwas ausrollst:

```bash
python3.14 -m venv /tmp/swarmq-check
/tmp/swarmq-check/bin/pip install 'swarmq==0.14.*'

# 1. Umgebung: Valkey, Broker, SWARMQ_*-Konfiguration
SWARMQ_BACKEND_URL=valkey://valkey:6379/0 \
SWARMQ_BROKER_URL=amqp://user:pass@rabbitmq:5672/app \
SWARMQ_SHUTDOWN_TIMEOUT=30 \
  /tmp/swarmq-check/bin/swarmq check --format table

# 2. Task-Module: TaskInfo-Werte, Cron-Ausdrücke, Importe, DI
#    (das virtualenv braucht auch die Abhängigkeiten deiner App)
/tmp/swarmq-check/bin/swarmq worker --module myapp.tasks --app-dir /path/to/app --check
```

Setze dieselben `SWARMQ_*`-Variablen wie bei deinen Workern (vor allem
`SWARMQ_NAMESPACE` und `SWARMQ_VALKEY_EVICTION_CHECK`). `swarmq check`
liest nur: `CONFIG GET`, `INFO`, `SCAN` und Lesezugriffe auf Valkey,
passive Deklarationen auf RabbitMQ. In Kubernetes kann es auch als
Init-Container laufen.

| Exit von `swarmq check` | Bedeutung | Maßnahme |
|---|---|---|
| `0` | nichts blockiert; Warnungen möglich | Warnungen lesen (AOF aus, Valkey älter als 8, Cluster-Modus, Keys ohne Namensraum, verwaiste Queues, abgeschalteter Scheduler) |
| `78` | ein neuer Worker würde den Start verweigern: verdrängende `maxmemory-policy` unter `strict`, ungültiger `SWARMQ_NAMESPACE` oder ein anderer `SWARMQ_*`-Wert, den der Start ablehnt (etwa ein Wert über seiner Obergrenze) | vor dem Rollout beheben; siehe [Valkey-Anforderungen](../reference/configuration.md#valkey-anforderungen) und [Worker-Verhalten](../reference/configuration.md#worker-verhalten) |
| `1` | Valkey nicht erreichbar, `SWARMQ_BACKEND_URL` fehlt, oder `SWARMQ_BROKER_URL` gesetzt und der Broker nicht erreichbar | Zugang beheben und erneut prüfen |

`swarmq worker --check` importiert das Task-Modul wie der Worker-Start
und meldet `"ok": false` für alles, was der Start ablehnen würde. Seit
0.12.3 und 0.13 gehören dazu Werte, die früher starteten:
Cron-Ausdrücke außerhalb der crontab(5)-Syntax oder ohne nächsten
Termin, `soft_timeout`/`hard_timeout` ≤ 0 und `TaskInfo`-Fristen über
zehn Jahren ([Upgrade-Hinweise](#anderungen-die-ein-deploy-stoppen-konnen)).

Finde Exit `78` nicht erst beim Ausrollen heraus: Ein Worker, der den
Start verweigert, endet bei jedem Neustart wieder mit `78`; Kubernetes
zeigt `CrashLoopBackOff`, Docker mit `restart:`-Policy läuft in eine
Schleife. Einzelheiten:
[`swarmq check`](../reference/cli.md#swarmq-check) und
[Exit-Codes](../reference/cli.md#exit-codes).

## Schritt 1: den Upgrade-Pfad wählen

| Alle Apps am vhost/Valkey laufen auf | Upgrade-Pfad |
|---|---|
| 0.11.0 oder älter | Zuerst **0.11.1 auf jede App ausrollen, als eigenes Deployment**, und laufen lassen. Dann mit der nächsten Zeile weiter. |
| 0.11.1 | Direkt auf 0.14.4. Es gelten die Regeln für den Abschied von 0.11: Worker vor Producern ([Schritt 2](#schritt-2-worker-vor-producern)), Namensraum zuletzt ([Schritt 4](#schritt-4-vhost-und-namensraum)), Rollback nur bis 0.11.1 ([Schritt 6](#schritt-6-rollback-regeln)). |
| 0.12.x oder 0.13.x | Direkt auf 0.14.4. 0.12.0 bis 0.14.4 laufen nebeneinander auf denselben Queues und Keys. |
| 0.14.x | Patch-Upgrade ohne Formatänderungen. 0.14.4 behebt nur einen Shutdown-Fall im Retry- und Dead-Letter-Pfad. |

**Warum zuerst 0.11.1:** Kubernetes (`kubectl rollout undo`) und Docker
gehen beim Rollback auf das Image zurück, das vorher lief. Ist das
0.11.0, scheitert der Rollback an Queues, die 0.12 oder später angelegt
hat (`PRECONDITION_FAILED`; 0.11.0 kann At-least-once-Queues nicht
deklarieren), und 0.11.0 ignoriert einen `SWARMQ_NAMESPACE` still.
0.11.1 übernimmt die neuen Queue-Argumente und verweigert einen
Namensraum mit Exit `78`
([Changelog 0.11.1](../reference/changelog.md#0111-2026-10-10)).

Lies die Hinweise jeder Version, die du überspringst; der
[Changelog](../reference/changelog.md) enthält die vollständigen
Upgrade-Hinweise.

### Änderungen, die ein Deploy stoppen können

Diese machen Werte, die früher starteten, zu Fehlern. `swarmq check`
und `swarmq worker --check` (Schritt 0) finden sie alle.

| Seit | Änderung | Folge |
|---|---|---|
| 0.12.3 | Ein `TaskInfo`-Cron ohne nächsten Termin (`0 0 30 2 *`) wirft beim Import `ValueError`. | Exit `1`, Neustarts bis zum Neustartlimit |
| 0.12.3 | `inf`, `nan` und negative Werte in Frist-, TTL- und Intervall-Einstellungen sind Konfigurationsfehler; wo unbegrenzt unterstützt wird, `none` verwenden. | Exit `78` |
| 0.12.3 | Ein `ImportError` oder `SyntaxError` im Task-Modul beendet den Worker nach einem Versuch. | Exit `78` (vorher `1` mit Neustarts) |
| 0.13.0 | Cron-Ausdrücke folgen einer crontab(5)-Positivliste: genau 5 Felder; `L`, `W`, `#`, `R`, `H`, `?`, `@reboot` und Ausdrücke mit 6 oder 7 Feldern werden abgelehnt. | `TaskInfo`: Exit `1`; `swarmq schedule add --cron`: Exit `1` |
| 0.13.0 | Konfigurationswerte haben Obergrenzen: jede Frist und TTL höchstens `315360000` s (zehn Jahre), `concurrency` höchstens `65535`. | Exit `78` |
| 0.13.0 | `TaskInfo.result_ttl`, `max_age`, `unique_for` und `hard_timeout` höchstens `315360000` s; `nan` und unendliche Werte werden abgelehnt. | Exit `1` |
| 0.13.0 | Eine `SWARMQ_WORKER_ID`, die einen senkrechten Strich enthält, wird abgelehnt. | Exit `78` |
| 0.13.1 | `soft_timeout` und `hard_timeout` müssen größer als 0 sein (`False`, `0`, negative Werte, `nan` und Nicht-Zahlen werden abgelehnt; `None` heißt kein Timeout). | Exit `1` |

Änderungen zur Laufzeit, die eine Entscheidung oder einen angepassten
Alert brauchen:

| Seit | Änderung |
|---|---|
| 0.12.3 | Enqueue-Middleware (`pre_enqueue`, `post_enqueue`) und `TASK_ENQUEUED` laufen auch für `schedule(delay=…/eta=…)`; eine ablehnende Middleware blockiert jetzt auch verzögerte Aufrufe. `unique_for` von `schedule()` muss `> 0` und endlich sein (`None` schaltet die Deduplizierung ab). |
| 0.12.4 | Der Verbindungsaufbau zu Valkey hat eine Frist, `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (Default 30 s). Lock-Namen, die mit `{sched}` beginnen, sind reserviert; ein Template, das einen solchen Namen rendert, dead-lettert die Nachricht. |
| 0.13.0 | Log-Level: `shutdown_sigterm_escalation` ist INFO (stattdessen auf ERROR `shutdown_sigkill_escalation` alarmieren); eine bestätigte Retry-Kopie, deren Ack scheiterte, loggt WARNING `retry_ack_failed_after_publish`. Ein gescheitertes Speichern des Ergebnisses wird etwa 30 bis 40 s lang wiederholt, bevor die Nachricht neu zugestellt wird; der Slot bleibt so lange belegt. Ein fehlendes Glied-Ergebnis einer Group wird als `WorkflowResultExpired` gespeichert. `swarmq schedule status` hat ein Feld `process`; `schedule add --format json` meldet `already_exists` anders. |
| 0.13.1 | `delay`, `eta` und `swarmq schedule add --at` höchstens zehn Jahre voraus (`ValueError`, Exit `1`). Ein abgebrochenes `schedule()` behält seinen `unique_for`-Claim. Eine anhaltende Valkey-Fehlerantwort beim Voranbringen eines Workflows lässt den Workflow am Zustelllimit scheitern. |
| 0.14.0 | Gegen einen Host, der Pakete verwirft, wartet der Valkey-Verbindungsaufbau bis zu seiner Frist, und Befehle scheitern nach etwa 38 s; Fehlertexte haben sich geändert. Alarm-Logzeilen haben sich geändert. Die Channel-Zahl am Broker ist niedriger und bleibt über Reconnects flach. Eine konsumierte Queue, die bei laufendem Worker gelöscht wird, wird abgegrenzt: ERROR `consumer_queue_missing`, `/ready` rot, die übrigen Queues laufen weiter. |
| 0.14.1 | Ein eingebettetes `start_worker()` wirft bei einem Verbindungsverlust während des Abonnierens `BrokerConnectionError` (vorher ein rohes `OSError`/`AMQPConnectionError`/`RuntimeError`). |
| 0.14.2 | `--autoscale` zählt drainende Worker für `MIN`, `MAX` und die Hochskalierschwelle nicht mit; mit `--max-processes N` können bis zu `N − MAX` drainende Prozesse zusätzlich zu `MAX` aktiven laufen. |
| 0.14.3 | Eine Delay-Stufe, die sich nicht neu deklarieren lässt, loggt WARNING `delay_tier_redeclare_failed` und nutzt `{queue}.DQ`, bis sie wiederhergestellt ist. |

Nur Code, der Verträge von swarmq selbst implementiert, betreffen diese:
Eigene `ExecutionGuardStore`-Implementierungen brauchen `peek` (0.13.0),
eigene `ResultBackend`-Implementierungen müssen nebenläufige
`connect()`/`close()` erlauben (0.14.0), `LockStore.force_release`
meldet jetzt, was es entfernt hat (0.14.0). Siehe die
[API-Referenz](../reference/api.md).

## Schritt 2: Worker vor Producern

Starte oder rolle die **Worker vor den Producern** (Web-Apps, Skripte,
andere Dienste, die nur senden).

- **Für jede neue Queue, dauerhaft:** Queues deklarieren die Worker,
  die sie konsumieren. Ein Publish an eine Queue, die es nicht gibt,
  wirft `UnroutableMessage`, statt still verworfen zu werden. Rolle die
  Worker einer neuen Queue aus, bevor der erste Producer an sie sendet.
  Dasselbe gilt für `TaskInfo(priority=...)`: Die Worker dieser Queue
  brauchen `--priorities`
  ([fehlende Queues](scheduling-and-retries.md#fehlende-queues)).
- **Für neue Task-Namen:** Ein Worker stellt einen Task-Namen, den er
  noch nicht kennt, `SWARMQ_UNKNOWN_TASK_GRACE_S` lang zurück (Default
  15 Minuten); ein gewöhnlicher Rolling Deploy verliert also nichts.
  Ein Worker vor 0.12 schickt eine solche Nachricht beim ersten Kontakt
  in die `.XQ`; solange noch einer die Queue konsumiert, dürfen Producer
  mit neuen Task-Namen den Workern nicht vorauslaufen
  ([unregistrierte Tasks](dead-letter-queues.md#unregistrierte-tasks-und-rolling-deploys)).
- **Producer in anderen Diensten:** Producer-seitige Korrekturen wirken
  erst, wo der Producer die neue Version fährt, etwa die
  Enqueue-Middleware für `delay`/`eta` (0.12.3) oder `unique_for=None`
  mit `delay`/`eta` (Producer und Scheduler-Leader ab 0.12.2). Aktualisiere
  diese Dienste mit.
- **Task in eine andere Queue verschieben:** Lass einen Worker für die
  alte Queue laufen, bis sie leer ist; der Scheduler-Leader meldet
  verwaiste Queues
  ([Task verschieben](defining-tasks.md#task-in-eine-andere-queue-verschieben)).

Solange ein Worker älter als 0.12 die Scheduler-Führung hält, loggen
die neueren Worker `scheduler_leader_outdated`, bis die Führung wechselt
([Leader-Version](scheduling-and-retries.md#leader-version)). Das ist
während des Rollouts erwartet. Ein Leader ab 0.13 feuert Crons weiter,
die ein älterer Leader übernommen hat, übernimmt aber keinen neuen oder
geänderten Cron außerhalb der crontab(5)-Syntax und loggt ERROR
`scheduler_cron_report_invalid`; stelle einen solchen Cron auf 5 Felder
um und rolle die Worker aus.

## Schritt 3: Grace-Period des Orchestrators

`SWARMQ_SHUTDOWN_TIMEOUT` (Default 30 s) ist das **Gesamtbudget** ab
`SIGTERM`: Der Drain laufender Tasks endet 5 s vorher (unter dem
Supervisor von `swarmq worker` 7 s vorher), danach werden Locks
freigegeben, der Scheduler übergibt und die Verbindungen schließen. Gib
dem Orchestrator einen Puffer obendrauf:

| Orchestrator | Einstellung | Wert |
|---|---|---|
| Kubernetes | `terminationGracePeriodSeconds` (Default 30) | ≥ `SWARMQ_SHUTDOWN_TIMEOUT + 15` |
| Docker / Compose | `stop_grace_period` (Default 10 s) | ≥ `SWARMQ_SHUTDOWN_TIMEOUT + 15 s` |

```yaml
# Kubernetes
spec:
  terminationGracePeriodSeconds: 45   # SWARMQ_SHUTDOWN_TIMEOUT=30
---
# Docker Compose
services:
  worker:
    stop_grace_period: 45s            # SWARMQ_SHUTDOWN_TIMEOUT=30
```

`swarmq check` gibt den Wert als `min_grace_period_s` aus. Sollen lange
Tasks während eines Deploys fertig werden, erhöhe
`SWARMQ_SHUTDOWN_TIMEOUT` und die Grace-Period gemeinsam
([Shutdown-Budget](../explanation/delivery-semantics.md#shutdown-budget)).
Ein `SIGKILL` vor dem Ende des Budgets schneidet die Lock-Freigabe und
die letzten Publishes ab.

**Mit Surge ausrollen, nicht erst stoppen.** Starte die neuen Worker,
bevor die alten `SIGTERM` bekommen (Kubernetes `maxSurge` ≥ 1 mit
`maxUnavailable: 0`, Compose `update_config.order: start-first`). Ein
Worker, der abstürzt, während ein Schedule-Fire läuft, hinterlässt einen
Claim; die Neuzustellung wartet darauf und läuft nur auf Workern ab
0.12.1 ([Schedule-Fires nach einem Absturz](../explanation/delivery-semantics.md#was-den-ack-abbricht)).

Weitere Container-Hinweise:

- **Probes:** `/health` für Liveness und `/ready` für Readiness, nie
  `/ready` für Liveness. Der Health-Server bindet standardmäßig an
  `127.0.0.1`; für Kubernetes-Probes, die die Pod-IP erreichen, setze
  `SWARMQ_HEALTH_HOST=0.0.0.0`
  ([Observability](../reference/configuration.md#observability)).
- **Signale:** Starte den Worker in Exec-Form (`CMD ["swarmq",
  "worker", ...]`), damit er `SIGTERM` selbst bekommt; eine Shell als
  PID 1 leitet es nicht weiter. Ein Init-Prozess (`docker run --init`,
  tini) räumt verwaiste Enkelprozesse ab.
- **Arbeitsverzeichnis:** `swarmq worker` importiert `--module` aus dem
  Arbeitsverzeichnis; setze `WORKDIR` also auf das Verzeichnis, das dein
  Package enthält (`WORKDIR /app` für `/app/myapp/tasks.py`), oder gib
  `--app-dir /app` an
  ([das Task-Modul finden](../reference/cli.md#das-task-modul-finden)).
- **Uhren:** NTP auf jedem Host. `max_age` vergleicht die Uhr des
  Producers mit der des Workers, und die Lock-Wartezeit verträgt etwa
  60 s Abweichung.

## Schritt 4: vhost und Namensraum

- **Ein RabbitMQ-vhost pro App.** Queue-Namen haben kein Präfix; zwei
  Apps auf einem vhost teilen sich Queues gleichen Namens, auch
  `default`.
- **Ein Namensraum pro App auf einem geteilten Valkey.** Ohne
  `SWARMQ_NAMESPACE` teilen sich alle Apps auf einem Valkey die Keys
  unter `sq:`, die Task-Locks, die Rate-Limits und einen
  Scheduler-Leader für alle Crons. Mit getrennten vhosts auf einem
  Valkey ist der Namensraum Pflicht: Sonst geht ein Cron von App B über
  den Broker von App A, wo seine Queue nicht existiert, und das Fire ist
  verloren.
- **Nur Kleinbuchstaben:** `[a-z0-9][a-z0-9_.-]{0,63}`, nicht `sq`.
  Großbuchstaben sind ein `ConfigurationError` (Exit `78`), weil
  Valkey-Keys Groß- und Kleinschreibung unterscheiden und `App1` und
  `app1` zwei Apps wären.
- **Reihenfolge:** Zuerst jede Komponente der App (Worker, Producer,
  CLI, Kontrolldienst) auf 0.12 oder später bringen, dann
  `SWARMQ_NAMESPACE` überall im selben Schritt setzen. Gemischte Werte
  spalten die App.
- **Neue App oder Umzug:** Der Namensraum zieht keine Daten um. Bei
  einer neuen App einfach setzen. Eine bestehende App zieht mit einem
  vollständigen Stopp um (Kubernetes `strategy: Recreate` oder auf 0
  skalieren, kein Rolling Update), ihre CLI-Crons und Oneshots werden
  ohne Namensraum entfernt und mit ihm neu angelegt, und
  Valkey-ACL-Muster ändern sich von `~sq:*`/`&sq:*` zu
  `~<namespace>:sq:*`/`&<namespace>:sq:*`. Bringe vorher jede andere
  App auf dem Valkey auf 0.12 oder später: Ein Leader älter als 0.12
  feuert die alten Definitionen bis zu 24 h weiter
  ([eine App in einen Namensraum umziehen](../reference/configuration.md#eine-app-in-einen-namensraum-umziehen)).
- **Solange Apps älter als 0.12 das Valkey ohne Namensraum teilen,**
  setze `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0`, damit die Führung nicht
  zu einer solchen App wechselt
  ([Namensraum](../reference/configuration.md#namensraum-mehrere-apps-auf-einem-valkey)).
- **`SWARMQ_SCHEDULER=off`** hält einen Worker aus der Wahl des
  Scheduler-Leaders heraus, etwa einen Entwicklerrechner gegen eine
  Kopie der Live-Daten. Mindestens ein Worker je Valkey und Namensraum
  muss `on` behalten, sonst feuert kein Cron, kein `delay`/`eta`-Task
  und kein Oneshot
  ([Scheduler auf einzelnen Workern abschalten](scheduling-and-retries.md#scheduler-auf-einzelnen-workern-abschalten)).

## Schritt 5: Rechte, Policies, Mengen

### RabbitMQ-Rechte

Der swarmq-Benutzer braucht configure-, write- und read-Rechte auf jede
Queue einer Familie und write-Rechte auf den Default-Exchange
(`amq.default`). Eine Familie `{queue}` besteht aus:

| Queue | Zweck |
|---|---|
| `{queue}` | die Queue, die die Worker konsumieren |
| `{queue}.DQ` | Verzögerungen über einer Stunde |
| `{queue}.DQ.1s`, `.4s`, `.15s`, `.1m`, `.4m`, `.15m`, `.1h` | die sieben [Delay-Stufen](../explanation/delivery-semantics.md#delay-stufen) |
| `{queue}.XQ` | die Dead-Letter-Queue |

Das sind 10 Queues je Familie. Mit `SWARMQ_PRIORITIES` sind die Bänder
`{queue}.high` und `{queue}.low` eigene Familien: 30 Queues. Prüfe das
Queue-Limit des vhosts (`max-queues`) gegen die Zahl der Queues mal 10
(oder 30). Eine Stufe, die der Broker verweigert, ist nicht fatal: Der
Worker loggt `delay_tier_unavailable`, nutzt dafür `{queue}.DQ` und
setzt `swarmq_delay_tiers_unavailable`.

### RabbitMQ-Policies

- **Eine Policy pro Queue.** RabbitMQ wendet nur die Policy mit der
  höchsten Priorität an; eine neue Policy verdrängt eine bestehende.
  Lege jeden Schlüssel, den eine Queue braucht (etwa
  `dead-letter-strategy`, `overflow` und `max-length`), in eine Policy.
- **Muster für Delay-Queues:** `\.DQ(\.\w+)?$` statt `\.DQ$`, damit die
  Stufen erfasst sind.
- **At-least-once für bestehende Queues:** Neue Quorum-Queues bekommen
  `x-dead-letter-strategy=at-least-once` und
  `x-overflow=reject-publish`. Bestehende Queues behalten ihre
  Argumente; eine Policy mit `dead-letter-strategy=at-least-once` und
  `overflow=reject-publish` korrigiert ihr Dead-Lettering
  ([Garantie beim Dead-Lettering](../explanation/delivery-semantics.md#garantie-beim-dead-lettering)).
- **`max-length` mit `reject-publish`:** Ein Publish an eine volle
  Queue bekommt ein Nack, statt die älteste Nachricht zu verwerfen, und
  scheitert nach fünf Versuchen mit `PublishRejected` (einem
  `BrokerConnectionError`). Dasselbe gilt für eine **Operator-Policy**
  mit `max-length`. Bemiss `max-length` so, dass es im Normalbetrieb nie
  erreicht wird, oder setze es nicht auf `{queue}` und `{queue}.DQ*`. Zum
  Begrenzen der `.XQ` siehe
  [die `.XQ` begrenzen](dead-letter-queues.md#die-xq-per-broker-policy-begrenzen).
- **Keine `delivery-limit`-Policy** auf swarmq-Queues: swarmq sieht sie
  nicht ([effektives Delivery-Limit](../explanation/delivery-semantics.md#effektives-delivery-limit)).
- **`SWARMQ_QUEUE_TYPE` ändern** endet bei einer bestehenden Queue mit
  `QueueArgumentsMismatch` (Exit `78`); migriere die Queue
  ([Queue-Argumente ändern](../explanation/delivery-semantics.md#queue-argumente-andern)).
- **Lösche nie eine Queue, die ein laufender Worker konsumiert**, auch
  keine, die verwaist aussieht; stoppe zuerst ihre Worker. Der Worker
  hört auf, sie zu konsumieren, loggt den ERROR `consumer_queue_missing`
  und bleibt nicht bereit (`/ready` nennt die Queue), bis er neu startet
  oder die Queue wieder existiert; seine übrigen Queues laufen weiter.
  Eine gelöschte Delay-Stufe deklariert die nächste Rückstellung neu
  (WARNING `delay_tier_redeclare_failed`, wenn das scheitert)
  ([konsumierte Queue löschen](../explanation/delivery-semantics.md#konsumierte-queue-loschen)).

### Verbindungen und Prozesse

| Prozess | RabbitMQ-Verbindungen |
|---|---|
| Worker-Prozess (jedes Kind von `swarmq worker`) | 2: Publish und Consume (`SWARMQ_BROKER_PUBLISH_CONNECTION=separate`, Default) |
| Producer, CLI-Befehl | 1 |

Ein Host mit `--max-processes 8` öffnet also bis zu 16 Verbindungen.
Prüfe vor dem Rollout `connection_max`, `max-connections` des vhosts,
Benutzerlimits und Alerts auf die Verbindungszahl, oder behalte mit
`SWARMQ_BROKER_PUBLISH_CONNECTION=shared` eine Verbindung pro Worker
(dann können Acks während eines Ressourcen-Alarms des Brokers hinter
einem blockierten Publish hängen). Plane Speicher für den Spielraum ein:
Unter `--autoscale` mit `--max-processes N` können bis zu `N − MAX`
drainende Prozesse neben `MAX` aktiven laufen.

Der Valkey-Client-Pool hat kein Limit; jeder Prozess öffnet
Verbindungen, so wie seine Nebenläufigkeit sie braucht. Halte
`maxclients` von Valkey deutlich über der Zahl der Prozesse mal ihrer
Nebenläufigkeit.

### Valkey

- **`maxmemory-policy noeviction`**, mit einem `maxmemory`-Limit, das
  Luft lässt; beobachte `swarmq_valkey_memory_used_ratio`. Jede andere
  Policy kann Locks und Marker verdrängen, und Tasks laufen doppelt. Ein
  Worker verweigert unter einer verdrängenden Policy den Start, außer
  `SWARMQ_VALKEY_EVICTION_CHECK=warn` oder `off` ist bewusst gesetzt.
- **AOF** (`appendonly yes`, `appendfsync everysec`).
- **Kein Cluster-Modus**, kein Sentinel; ein einzelner Primary.
- **Speicher für `max_age`:** Für einen Task mit `max_age` leben die
  Ausführungsmarker mindestens `max_age + 300` s: etwa 100 MB je
  Million Tasks pro Tag bei `max_age` 24 h.
- **Verbindungsfrist:** Ein Worker, Producer oder CLI-Befehl gibt den
  Verbindungsaufbau nach `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (Default
  30 s) mit `BackendConnectionError` auf; ein Port, der Verbindungen
  ablehnt, scheitert nach etwa 3 s.

Einzelheiten: [Valkey-Anforderungen](../reference/configuration.md#valkey-anforderungen).

## Schritt 6: Rollback-Regeln

- **Zwischen 0.12.0 und 0.14.4 ist ein Rollback möglich**, in beide
  Richtungen auf denselben Queues und Keys; 0.14 wurde gegen 0.13.1,
  0.12.3, 0.12.0, 0.11.1 und 0.11.0 getestet. Jede Korrektur bleibt nur
  auf den Prozessen, die die neue Version behalten.
- **Nie unter 0.11.1.** 0.11.0 kann Queues, die 0.12 oder später
  angelegt hat, nicht deklarieren und scheitert mit
  `PRECONDITION_FAILED` (Schritt 1).
- **Mit Namensraum nur auf 0.12 oder später zurück.** 0.11.1 verweigert
  mit gesetztem `SWARMQ_NAMESPACE` den Start (Exit `78`); entferne den
  Namensraum nur mit einem vollständigen Rückumzug.
- **Code, der neuere API nutzt,** läuft auf der alten Version nicht,
  etwa `ValkeyBackend(connect_timeout_s=...)` (0.12.4) oder
  `from swarmq import BrokerPublishBlocked, PublishRejected` (0.12.3;
  importiere sie stattdessen aus `swarmq.exceptions`).
- **Delay-Stufen bleiben.** Nach einem Rollback kehren ihre Nachrichten
  bei Fälligkeit weiter nach `{queue}` zurück; lösche eine Stufe erst,
  wenn sie leer ist.
- **Was nach einem Rollback auf 0.11.1 oder im Mischbetrieb mit ihr
  aus ist:**
    - Stabile Oneshot-IDs, Cron-Dedup aus der Definition und
      zurückgehaltene Crons brauchen einen Leader ab 0.12; solange eine
      0.11-App führt, sind sie aus.
    - Die Gnadenfrist für unbekannte Tasks und Wiederholungen nach einem
      Dead-Lettering durch den Broker brauchen jeden Worker der Queue ab
      0.12: Eine Wiederholung, die einen 0.11-Worker erreicht, wird
      übersprungen, ein wiederholtes Cron-Fire ebenso.
    - Ein `swarmq dlq`-Befehl von 0.11 liest die DLQ weiter zerstörend.
    - Ein 0.11-Worker zählt Lock-Konflikte mit seinem alten Zähler und
      dead-lettert beim 10.; 0.11-Worker behalten eine Broker-Verbindung.
    - 0.11-Semaphor-Erwerber kürzen den Lock-Key weiter.

Die vollständige Liste für 0.12 steht im Changelog unter
[Bekannte Grenzen](../reference/changelog.md#bekannte-grenzen).

## Verifizieren

Nach dem Rollout:

- `swarmq check` endet mit Exit `0`.
- `swarmq schedule status` zeigt für den Leader und jeden Worker die
  neue Version, und `swarmq_scheduler_leader_outdated` ist `0`.
- Kein `queue_dead_letter_at_most_once` für Queues, die du at-least-once
  erwartest, kein `delay_tier_unavailable` und kein
  `delay_tier_redeclare_failed`.
- `swarmq_scheduler_queues_orphaned` ist `0`, und die Key-Zahl in
  Valkey pendelt sich binnen einer Woche ein (TTLs von 24 h und 7 Tagen).
- Es gibt Alerts auf `consumer_queue_missing` und
  `shutdown_sigkill_escalation` (beide ERROR).

## Siehe auch

- [Konfiguration](../reference/configuration.md): jede
  `SWARMQ_*`-Variable.
- [CLI](../reference/cli.md): `swarmq check`, `--check`, Exit-Codes.
- [Zustellungssemantik](../explanation/delivery-semantics.md):
  Delay-Stufen, Queue-Argumente, Shutdown-Budget.
- [Changelog](../reference/changelog.md): die Upgrade-Hinweise jedes
  Releases.
