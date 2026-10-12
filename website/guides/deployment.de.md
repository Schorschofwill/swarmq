# Ausrollen und Upgraden

Wie du swarmq in Produktion ausrollst und von 0.11 auf 0.12 upgradest,
ohne Tasks zu verlieren oder doppelt auszuführen. Die Schritte stehen
in ihrer Reihenfolge; jeder verlinkt die Referenzseite mit den
Einzelheiten.

## Voraussetzungen

- **Versionen:** Python 3.14, RabbitMQ 4.2 oder 4.3, Valkey 8. Ältere
  Broker und Valkey-Server sind nicht getestet.
- **Jede App, die den RabbitMQ-vhost oder die Valkey-Datenbank
  mitnutzt,** gehört zum Rollout. Die Schritte gelten für alle, nicht
  nur für die App, die du änderst.
- **Rollback-Plan:** Du weißt, auf welches Image Kubernetes oder Docker
  bei einem Rollback zurückgeht (Schritt 6).

## Schritt 0: `swarmq check` mit der neuen Version

Installiere die neue Version in einem eigenen virtualenv und führe die
lesende Prüfung gegen Produktion aus, bevor du etwas ausrollst:

```bash
python3.14 -m venv /tmp/swarmq-check
/tmp/swarmq-check/bin/pip install 'swarmq==0.12.*'
SWARMQ_BACKEND_URL=valkey://valkey:6379/0 \
SWARMQ_BROKER_URL=amqp://user:pass@rabbitmq:5672/app \
SWARMQ_SHUTDOWN_TIMEOUT=30 \
  /tmp/swarmq-check/bin/swarmq check --format table
```

Solange von 0.12 nur ein Release Candidate existiert, braucht `pip`
`--pre` (`pip install --pre 'swarmq==0.12.*'`).

Setze dieselben `SWARMQ_*`-Variablen wie bei deinen Workern (vor allem
`SWARMQ_NAMESPACE` und `SWARMQ_VALKEY_EVICTION_CHECK`). Die Prüfung
liest nur: `CONFIG GET`, `INFO`, `SCAN` und Lesezugriffe auf Valkey,
passive Deklarationen auf RabbitMQ. In Kubernetes kann sie auch als
Init-Container laufen.

| Exit | Bedeutung | Maßnahme |
|---|---|---|
| `0` | nichts blockiert; Warnungen möglich | Warnungen lesen (AOF aus, Valkey älter als 8, Cluster-Modus, Keys ohne Namensraum, verwaiste Queues) |
| `78` | ein 0.12-Worker würde den Start verweigern (verdrängende `maxmemory-policy` unter `strict`, ungültiger `SWARMQ_NAMESPACE`) | vor dem Rollout beheben; siehe [Valkey-Anforderungen](../reference/configuration.md#valkey-anforderungen) |
| `1` | Valkey nicht erreichbar, `SWARMQ_BACKEND_URL` fehlt, oder `SWARMQ_BROKER_URL` gesetzt und der Broker nicht erreichbar | Zugang beheben und erneut prüfen |

Finde Exit `78` nicht erst beim Ausrollen heraus: Ein Worker, der den
Start verweigert, endet bei jedem Neustart wieder mit `78`; Kubernetes
zeigt `CrashLoopBackOff`, Docker mit `restart:`-Policy läuft in eine
Schleife. Abschnitte und Befunde im Einzelnen:
[`swarmq check`](../reference/cli.md#swarmq-check).

## Schritt 1: zuerst alle Apps auf 0.11.1

Rolle **0.11.1 auf jede App** aus, die denselben RabbitMQ-vhost oder
Valkey nutzt, **als eigenes Deployment**, und lass es laufen, bevor du
mit 0.12 beginnst.

Der Grund ist der Rollback: Kubernetes (`kubectl rollout undo`) und
Docker gehen auf das Image zurück, das vorher lief. Ist das 0.11.0,
scheitert der Rollback an Queues, die 0.12 angelegt hat
(`PRECONDITION_FAILED`, 0.11.0 kann At-least-once-Queues nicht
deklarieren), und 0.11.0 ignoriert einen `SWARMQ_NAMESPACE` still.
0.11.1 übernimmt die Queue-Argumente von 0.12 und verweigert einen
Namensraum mit Exit `78`
([Changelog 0.11.1](../reference/changelog.md#0111-2026-10-10)).

## Schritt 2: Worker vor Producern

Starte oder rolle die **Worker vor den Producern** (Web-Apps, Skripte,
andere Dienste, die nur senden).

- **Während des Upgrades:** Nur 0.12-Worker stellen einen Task-Namen
  zurück, den sie noch nicht kennen (`SWARMQ_UNKNOWN_TASK_GRACE_S`,
  Default 15 Minuten). Ein 0.11-Worker an derselben Queue schickt eine
  solche Nachricht beim ersten Kontakt in die `.XQ`. Bis alle Worker
  einer Queue auf 0.12 laufen, darf ein Producer mit neuen Task-Namen
  den Workern nicht vorauslaufen
  ([unregistrierte Tasks](dead-letter-queues.md#unregistrierte-tasks-und-rolling-deploys)).
- **Für jede neue Queue, dauerhaft:** Queues deklarieren die Worker,
  die sie konsumieren. Seit 0.12 wirft ein Publish an eine Queue, die
  es nicht gibt, `UnroutableMessage`, statt still verworfen zu werden.
  Rolle die Worker einer neuen Queue aus, bevor der erste Producer an
  sie sendet. Dasselbe gilt für `TaskInfo(priority=...)`: Die Worker
  dieser Queue brauchen `--priorities`
  ([fehlende Queues](scheduling-and-retries.md#fehlende-queues)).
- **Task in eine andere Queue verschieben:** Lass einen Worker für die
  alte Queue laufen, bis sie leer ist; der Scheduler-Leader meldet
  verwaiste Queues
  ([Task verschieben](defining-tasks.md#task-in-eine-andere-queue-verschieben)).

Solange 0.11- und 0.12-Worker nebeneinander laufen, loggen die
0.12-Worker `scheduler_leader_outdated`, bis die Führung zu einem
0.12-Worker wechselt
([Leader-Version](scheduling-and-retries.md#leader-version)). Das ist
während des Rollouts erwartet.

## Schritt 3: Grace-Period des Orchestrators

`SWARMQ_SHUTDOWN_TIMEOUT` (Default 30 s) ist seit 0.12 das
**Gesamtbudget** ab `SIGTERM`: Der Drain laufender Tasks endet 5 s
vorher (unter dem Supervisor von `swarmq worker` 7 s vorher), danach
werden Locks freigegeben, der Scheduler übergibt und die Verbindungen
schließen. Gib dem Orchestrator einen Puffer obendrauf:

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

`swarmq check` gibt den Wert als `min_grace_period_s` aus. Mit dem
Default von 30 s ist der Drain kürzer als vor 0.12 (25 s eingebettet,
23 s unter dem Supervisor). Sollen lange Tasks während eines Deploys
fertig werden, erhöhe `SWARMQ_SHUTDOWN_TIMEOUT` und die Grace-Period
gemeinsam ([Shutdown-Budget](../explanation/delivery-semantics.md#shutdown-budget)).

Weitere Hinweise für Container:

- **Probes:** `/health` als Liveness, `/ready` als Readiness, nie
  `/ready` als Liveness. Der Health-Server bindet per Default an
  `127.0.0.1`; für Kubernetes-Probes, die die Pod-IP ansprechen, setze
  `SWARMQ_HEALTH_HOST=0.0.0.0`
  ([Observability](../reference/configuration.md#observability)).
- **Signale:** Starte den Worker in Exec-Form (`CMD ["swarmq",
  "worker", ...]`), damit er `SIGTERM` selbst erhält; eine Shell als
  PID 1 leitet es nicht weiter. Ein Init-Prozess (`docker run --init`,
  tini) räumt verwaiste Enkelprozesse ab.
- **Arbeitsverzeichnis:** `swarmq worker` importiert `--module` aus dem
  Arbeitsverzeichnis. Setze `WORKDIR` daher auf das Verzeichnis, das dein
  Paket enthält (`WORKDIR /app` für `/app/myapp/tasks.py`), oder übergib
  `--app-dir /app`. Vor diesem Release brauchte das Konsolenskript dafür
  `PYTHONPATH`
  ([Das Task-Modul finden](../reference/cli.md#das-task-modul-finden)).
- **Uhren:** NTP auf jedem Host. `max_age` vergleicht die Uhr des
  Producers mit der des Workers, und das Warten auf einen Lock toleriert
  einen Versatz von etwa 60 s.

## Schritt 4: vhost und Namensraum

- **Ein RabbitMQ-vhost je App.** Queue-Namen bekommen kein Präfix; zwei
  Apps auf einem vhost teilen gleichnamige Queues, auch `default`.
- **Ein Namensraum je App auf einem geteilten Valkey.** Ohne
  `SWARMQ_NAMESPACE` teilen alle Apps auf einem Valkey die Keys unter
  `sq:`, die Task-Locks, die Rate-Limits und einen Scheduler-Leader für
  alle Crons. Bei getrennten vhosts auf einem Valkey ist der Namensraum
  Pflicht: Sonst geht ein Cron von App B über den Broker von App A, wo
  seine Queue nicht existiert, und der Fire geht verloren.
- **Nur Kleinbuchstaben:** `[a-z0-9][a-z0-9_.-]{0,63}`, nicht `sq`.
  Großbuchstaben sind ein `ConfigurationError` (Exit `78`), weil
  Valkey-Keys groß-/kleinschreibungssensitiv sind und `App1` und `app1`
  zwei Apps wären.
- **Reihenfolge:** erst jede Komponente der App (Worker, Producer, CLI,
  Kontrolldienst) auf 0.12, dann `SWARMQ_NAMESPACE` überall im selben
  Schritt setzen. Gemischte Werte spalten die App.
- **Neue App oder Umzug:** Der Namensraum zieht keine Daten um. Für
  eine neue App setzt du ihn einfach. Eine bestehende App ziehst du mit
  vollständigem Stopp um (Kubernetes `strategy: Recreate` oder auf 0
  skalieren, kein Rolling Update); ihre CLI-Crons und One-Shots werden
  ohne Namensraum entfernt und mit Namensraum neu angelegt, und
  Valkey-ACL-Muster wechseln von `~sq:*`/`&sq:*` zu
  `~<namespace>:sq:*`/`&<namespace>:sq:*`. Bring vorher jede andere
  App auf dem Valkey auf 0.12: Ein Leader älter als 0.12 feuert die
  alten Definitionen bis zu 24 h weiter
  ([App in einen Namensraum umziehen](../reference/configuration.md#eine-app-in-einen-namensraum-umziehen)).
- **Solange Apps älter als 0.12 den Valkey ohne Namensraum teilen,**
  setze `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0`, damit die Führung nicht
  zu einer 0.11-App wechselt
  ([Namensraum](../reference/configuration.md#namensraum-mehrere-apps-auf-einem-valkey)).
- **`SWARMQ_SCHEDULER=off`** hält einen Worker aus der Wahl des
  Scheduler-Leaders heraus, etwa einen Entwicklerrechner gegen eine Kopie
  der Live-Daten. Mindestens ein Worker je Valkey und Namensraum muss
  `on` behalten, sonst feuert kein Cron, kein `delay`/`eta`-Task und kein
  Oneshot ([Scheduler abschalten](scheduling-and-retries.md#scheduler-auf-einzelnen-workern-abschalten)).

## Schritt 5: Rechte, Policies, Mengen

### RabbitMQ-Rechte

Der swarmq-Benutzer braucht configure-, write- und read-Recht auf jede
Queue einer Familie und write-Recht auf den Default-Exchange
(`amq.default`). Eine Familie `{queue}` besteht aus:

| Queue | Zweck |
|---|---|
| `{queue}` | die Queue, die Worker konsumieren |
| `{queue}.DQ` | Wartezeiten über eine Stunde |
| `{queue}.DQ.1s`, `.4s`, `.15s`, `.1m`, `.4m`, `.15m`, `.1h` | die sieben [Delay-Stufen](../explanation/delivery-semantics.md#delay-stufen) (neu in 0.12) |
| `{queue}.XQ` | die Dead-Letter-Queue |

Das sind 10 Queues je Familie. Mit `SWARMQ_PRIORITIES` sind die Bänder
`{queue}.high` und `{queue}.low` eigene Familien: 30 Queues. Prüfe das
Queue-Limit des vhosts (`max-queues`) gegen die Zahl der Queues mal 10
(bzw. 30). Eine Stufe, die der Broker ablehnt, ist nicht fatal; der
Worker loggt `delay_tier_unavailable`, nutzt dafür `{queue}.DQ` und
setzt `swarmq_delay_tiers_unavailable`.

### RabbitMQ-Policies

- **Eine Policy je Queue.** RabbitMQ wendet nur die Policy mit der
  höchsten Priorität an; eine neue Policy verdrängt eine bestehende.
  Nimm jeden Schlüssel, den eine Queue braucht (etwa
  `dead-letter-strategy`, `overflow` und `max-length`), in eine Policy
  auf.
- **Muster für Delay-Queues:** `\.DQ(\.\w+)?$` statt `\.DQ$`, damit
  die Stufen erfasst sind.
- **At-least-once für bestehende Queues:** Neue Quorum-Queues bekommen
  `x-dead-letter-strategy=at-least-once` und `x-overflow=reject-publish`.
  Bestehende Queues behalten ihre Argumente; eine Policy mit
  `dead-letter-strategy=at-least-once` und `overflow=reject-publish`
  behebt ihr Dead-Lettering
  ([Garantie beim Dead-Lettering](../explanation/delivery-semantics.md#garantie-beim-dead-lettering)).
- **`max-length` mit `reject-publish`:** Ein Publish an eine volle
  Queue bekommt ein Nack, statt die älteste Nachricht zu verdrängen, und
  scheitert nach fünf Versuchen mit `PublishRejected` (ein
  `BrokerConnectionError`). Dasselbe gilt für eine **Operator-Policy**
  mit `max-length`. Bemiss `max-length` so, dass es im Normalbetrieb nie
  erreicht wird, oder setze es nicht auf `{queue}` und `{queue}.DQ*`.
  Zum Begrenzen der `.XQ` siehe
  [die `.XQ` begrenzen](dead-letter-queues.md#die-xq-per-broker-policy-begrenzen).
- **Keine `delivery-limit`-Policy** auf swarmq-Queues: swarmq sieht sie
  nicht ([effektives Delivery-Limit](../explanation/delivery-semantics.md#effektives-delivery-limit)).
- **`SWARMQ_QUEUE_TYPE` ändern** endet bei einer bestehenden Queue mit
  `QueueArgumentsMismatch` (Exit `78`); zieh die Queue um
  ([Queue-Argumente ändern](../explanation/delivery-semantics.md#queue-argumente-andern)).

### Verbindungen

| Prozess | RabbitMQ-Verbindungen |
|---|---|
| Worker-Prozess (jedes Kind von `swarmq worker`) | 2: Publish und Consume (`SWARMQ_BROKER_PUBLISH_CONNECTION=separate`, Default) |
| Producer, CLI-Befehl | 1 |

Ein Host mit `--max-processes 8` öffnet also 16 Verbindungen. Prüfe
`connection_max`, das `max-connections` des vhosts, Benutzerlimits und
Alarme auf die Verbindungszahl vor dem Rollout, oder behalte mit
`SWARMQ_BROKER_PUBLISH_CONNECTION=shared` eine Verbindung je Worker
(dann können Acks während eines Broker-Ressourcenalarms hinter einem
blockierten Publish hängen).

Der Valkey-Client-Pool hat kein Limit; jeder Prozess öffnet so viele
Verbindungen, wie seine Concurrency braucht. Halte `maxclients` von
Valkey deutlich über der Zahl der Prozesse mal ihrer Concurrency.

### Valkey

- **`maxmemory-policy noeviction`**, mit einem `maxmemory`-Limit, das
  Luft lässt; beobachte `swarmq_valkey_memory_used_ratio`. Jede andere
  Policy kann Locks und Marker verdrängen, und Tasks laufen doppelt. Ein
  Worker verweigert unter einer verdrängenden Policy den Start, außer
  `SWARMQ_VALKEY_EVICTION_CHECK=warn` oder `off` ist bewusst gesetzt.
- **AOF** (`appendonly yes`, `appendfsync everysec`).
- **Kein Cluster-Modus**, kein Sentinel; ein einzelner Primary.
- **Speicher für `max_age`:** Bei einem Task mit `max_age` leben die
  Ausführungsmarker mindestens `max_age + 300` s: etwa 100 MB je
  Million Tasks am Tag bei `max_age` 24 h.

Einzelheiten: [Valkey-Anforderungen](../reference/configuration.md#valkey-anforderungen).

## Schritt 6: Rollback-Regeln

- **Rollback nur auf 0.11.1 oder neuer.** 0.11.0 kann Queues, die 0.12
  angelegt hat, nicht deklarieren und scheitert mit
  `PRECONDITION_FAILED` (Schritt 1).
- **Mit Namensraum Rollback nur auf 0.12 oder neuer.** 0.11.1
  verweigert mit gesetztem `SWARMQ_NAMESPACE` den Start (Exit `78`);
  den Namensraum entfernst du nur mit einem vollständigen Rückumzug.
- **Delay-Stufen bleiben.** Nach einem Rollback kehren ihre Nachrichten
  weiterhin bei Fälligkeit nach `{queue}` zurück; lösche eine Stufe nur,
  wenn sie leer ist.
- **Was nach einem Rollback oder im Mischbetrieb nicht wirkt:**
  - Stabile One-Shot-IDs, Cron-Dedup aus der Definition und
    zurückgehaltene Crons brauchen einen Leader auf 0.12; solange eine
    0.11-App führt, sind sie aus.
  - Die Gnadenfrist für unbekannte Tasks und Replays nach einem
    Broker-Dead-Lettering brauchen alle Worker auf 0.12: Ein Replay, das
    einen 0.11-Worker erreicht, wird übersprungen, ein wiederholter
    Cron-Fire ebenso.
  - Ein `swarmq dlq`-Befehl von 0.11 liest die DLQ weiter zerstörend.
  - Ein 0.11-Worker zählt Lock-Konflikte mit seinem alten Zähler und
    schickt beim 10. in die DLQ; 0.11-Worker behalten eine
    Broker-Verbindung.
  - 0.11-Erwerber eines Semaphors verkürzen den Lock-Key weiter.

Die vollständige Liste steht im Changelog unter
[Bekannte Grenzen](../reference/changelog.md#bekannte-grenzen).

## Verifizieren

Nach dem Rollout:

- `swarmq check` endet mit Exit `0`.
- `swarmq schedule status` zeigt einen Leader auf 0.12, und
  `swarmq_scheduler_leader_outdated` ist `0`.
- Kein `queue_dead_letter_at_most_once` für Queues, die du at-least-once
  erwartest, kein `delay_tier_unavailable`.
- `swarmq_scheduler_queues_orphaned` ist `0`, und die Keyzahl in Valkey
  pendelt sich innerhalb einer Woche ein (TTLs von 24 h und 7 Tagen).

## Siehe auch

- [Konfiguration](../reference/configuration.md): jede
  `SWARMQ_*`-Variable.
- [CLI](../reference/cli.md): `swarmq check`, Exit-Codes.
- [Zustellungssemantik](../explanation/delivery-semantics.md):
  Delay-Stufen, Queue-Argumente, Shutdown-Budget.
- [Changelog](../reference/changelog.md): Upgrade-Hinweise für 0.12.
