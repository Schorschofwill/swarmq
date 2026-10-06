# CLI-Referenz

Das `swarmq`-Binary wird als Console-Script installiert, wenn du
`pip install swarmq` aufrufst. Jedes Subcommand liest per Default
aus `SWARMQ_*`-Environment-Variablen; explizite Flags gewinnen.
`swarmq worker` löst seine Einstellungen einmal im Supervisor auf
(Flag, dann Env-Var, dann Default) und gibt die aufgelösten Werte an
seine Worker-Subprozesse und Rechenkinder weiter; die lesen das
Environment nicht noch einmal, ein Flag gewinnt also in jedem Prozess.

## Synopsis

```text
swarmq [--version]
swarmq worker   --module DOTTED.PATH [options]
swarmq schedule (list | add | remove | status) [options]
swarmq dlq      (list | inspect | retry | purge) [options]
swarmq inspect  --module DOTTED.PATH [--format FORMAT]
```

## Exit-Codes

| Code | Bedeutung |
|---|---|
| `0` | Erfolg oder Graceful Shutdown |
| `1` | Laufzeitfehler: Broker oder Backend unerreichbar, `swarmq dlq`/`schedule` zur Laufzeit gescheitert. Später erneut versuchen ist die richtige Antwort. |
| `2` | Ungültige Argumente (argparse-Usage-Fehler) |
| `3` | `swarmq dlq inspect` / `swarmq dlq retry <task_id>` fand keinen passenden DLQ-Eintrag |
| `78` | Konfigurationsfehler von `swarmq worker` (`EX_CONFIG`): `ConfigurationError`, Task-Modul nicht importierbar, `--configure`-Hook gescheitert, Portbereich ungültig, ungültige Logging-Einstellungen oder Logging-Konfigurationsdatei (`LoggingConfigError`). Kommt aus dem Elternprozess und aus dem Supervisor, wenn ein Worker-Kind mit `78` endet. Ein erneuter Versuch ohne Änderung ist sinnlos. |

`78` folgt `sysexits.h`. Jeder Konfigurationsfehler, ob er im
Elternprozess oder in einem Worker-Subprozess auftritt, beendet den
gesamten Lauf nach einem einzigen Versuch mit `78` und schreibt einen
strukturierten Logsatz `worker_config_error` im konfigurierten
`--log-format`. Verbindungsfehler bleiben bei `1`, weil dort „gleich
nochmal" richtig ist. Unter systemd `RestartPreventExitStatus=78`
setzen, damit ein kaputtes Deployment nicht in der Schleife neu
startet; in Kubernetes auf den Exit-Code statt auf Neustarts alarmieren.
Exit `75` (`EX_TEMPFAIL`) ist intern: Ein Worker-Kind, das Recycling
angefragt und keine Bestätigung bekommen hat, loggt
`worker_recycle_ack_timeout` und endet damit, der Supervisor loggt
`worker_recycle_noack` und startet es im Budget neu;
den Aufrufer erreicht dieser Code nie.

`--log-level DEBUG` fügt einen vollen Python-Traceback zu stderr
hinzu, wenn Exit-Code `1` oder `78` feuert.

## `swarmq worker`

Importiert ein Python-Modul, sodass seine `Task`-Subklassen
registriert werden, verbindet sich mit Broker + Backend und
konsumiert. `SWARMQ_BROKER_URL` und `SWARMQ_BACKEND_URL` müssen
gesetzt sein.

```text
swarmq worker --module DOTTED.PATH
              [--configure MODULE:CALLABLE]
              [--queues A,B,C]
              [--concurrency N] [--pool {io,cpu}]
              [--log-level LEVEL] [--log-format FORMAT]
              [--log-config PATH | --no-configure-logging]
              [--logger-level NAME=LEVEL]...
              [--log-queue-size N] [--log-lifecycle-level LEVEL]
              [--burst] [--burst-idle-timeout SECONDS]
              [--priorities]
              [--metrics-port PORT] [--health-port PORT]
              [--processes N | -p N] [--autoscale MIN,MAX]
              [--max-processes N] [--group LABEL]
              [--reload] [--watch-dir PATH]...
              [--check]
```

### Optionen

| Flag | Default | Beschreibung |
|---|---|---|
| `--module DOTTED.PATH` | *pflicht* | Python-Modul mit zu registrierenden `Task`-Subklassen (z. B. `myapp.tasks`). Muss vom aktuellen `PYTHONPATH` importierbar sein. |
| `--configure MODULE:CALLABLE` | ungesetzt | Callable, das mit der `SwarmQ`-App nach ihrer Konstruktion und vor dem Start des Workers aufgerufen wird, z. B. `myapp.hooks:setup`. Damit lassen sich Middleware und Signal-Handler registrieren, ohne einen eigenen Worker-Einstieg zu schreiben. Läuft genau einmal je Worker-Prozess, nie im Forkserver und nie in `schedule`, `dlq` oder `inspect`; ein Awaitable als Rückgabe wird abgewartet. Jede Ausnahme aus dem Hook ist ein Konfigurationsfehler (Exit `78`). Siehe [das Beispiel](#middleware-per-configure-registrieren). |
| `--queues A,B,C` | `default` | Komma-separierte Queue-Namen zum Konsumieren. Env-Var: `SWARMQ_QUEUES`. |
| `--concurrency N` | `10` (io) / zugeteilte CPUs (cpu) | Maximal parallel ausgeführte Tasks. Mit `--pool cpu` die Zahl der Rechenslots; Voreinstellung sind die dem Worker zugeteilten CPUs (Affinität und cgroup-Quote) geteilt durch `--processes`. Env-Var: `SWARMQ_CONCURRENCY`. |
| `--pool {io,cpu}` | `io` | Ausführungspool. `cpu` führt jeden Task-Körper in einem Rechenkind aus einem vorgewärmten Forkserver aus: der Loop-Prozess rechnet nie, `hard_timeout` und Cancel töten das Kind. Nur POSIX; nicht mit `--autoscale` kombinierbar. Siehe [CPU-Modus](../guides/cpu-mode.md). |
| `--log-level LEVEL` | `INFO` | Einer aus `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root-Level jedes SwarmQ-Prozesses. Env-Var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` oder `json` auf stderr. JSON passt zu Loki, Datadog, CloudWatch. Env-Var: `SWARMQ_LOG_FORMAT` (Alias `SWARMQ_LOG_JSON=true/false`; `SWARMQ_LOG_FORMAT` gewinnt). |
| `--log-config PATH` | nicht gesetzt | Logging-`dictConfig`-Datei (JSON; YAML mit installiertem PyYAML), die in jedem SwarmQ-Prozess statt der eingebauten Einrichtung gilt. Der Supervisor prüft sie einmal. Nicht kombinierbar mit einem ausdrücklichen Format oder `--no-configure-logging`. Env-Var: `SWARMQ_LOG_CONFIG`. Siehe [Logging](logging.md#eigene-dictconfig-datei-log-config). |
| `--no-configure-logging` | konfigurieren | In keinem SwarmQ-Prozess Logging einrichten; dein Task-Modul oder `--configure`-Hook ist zuständig. Env-Var: `SWARMQ_CONFIGURE_LOGGING=false`. |
| `--logger-level NAME=LEVEL` | keine | Level eines einzelnen Loggers, z. B. `aio_pika=WARNING` oder `swarmq.task=WARNING`. Wiederholbar; zuletzt angewandt. Env-Var: `SWARMQ_LOGGER_LEVELS` (komma-separiert). |
| `--log-queue-size N` | `10000` | Kapazität der prozessinternen Log-Queue; ist sie voll, werden Einträge verworfen statt zu blockieren. Env-Var: `SWARMQ_LOG_QUEUE_SIZE`. |
| `--log-lifecycle-level LEVEL` | `DEBUG` | Level der Events `task_received` und `task_started`. Env-Var: `SWARMQ_LOG_LIFECYCLE_LEVEL`. |
| `--burst` | aus | Exited sauber, sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren. Env-Var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Sekunden Queue-Stille, bevor `--burst` exited. Env-Var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--priorities` | aus | Auto-Deklaration und Konsum des `{queue}.high`/`.normal`/`.low`-Triplets für jede Queue. Env-Var: `SWARMQ_PRIORITIES`. |
| `--metrics-port PORT` | ungesetzt | **Basisport** für Prometheus `/metrics`: Der Worker in Slot `s` serviert auf `PORT + s`, `--processes 3 --metrics-port 9000` belegt also `9000-9002`. Braucht `pip install 'swarmq[metrics]'`. Bind-Host: `SWARMQ_METRICS_HOST` (Default `127.0.0.1`). Siehe [Basisports](configuration.de.md#observability). |
| `--health-port PORT` | ungesetzt | **Basisport** für `/health` + `/ready`, derselbe Slot-Versatz wie bei `--metrics-port`. Bind-Host: `SWARMQ_HEALTH_HOST` (Default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Lässt N Worker-Subprozesse in den Slots `0..N-1` laufen. Seit 0.5.0 läuft jeder `swarmq worker` unter dem Supervisor, auch mit `N = 1`: Er startet gecrashte Worker im selben Slot neu (max 5 pro Stunde), ersetzt Worker ohne Lebenszeichen, reloaded auf `SIGHUP` via `os.execvp()` und endet mit 0, sobald alle Worker sauber beendet sind (`--burst`). Ein Konfigurationsfehler in einem beliebigen Worker beendet den Lauf nach genau einem Versuch mit Exit `78`. |
| `--autoscale MIN,MAX` | ungesetzt | Aktiviert Queue-tiefen-getriebenes Autoscaling zwischen `MIN` und `MAX` Worker-Subprozessen. Impliziert `--processes`. |
| `--max-processes N` | ungesetzt | **Schaltet den Kontrollkanal ein** und erklärt den Spielraum: Ein im Backend hinterlegter Sollwert darf diesen Supervisor auf höchstens `N` Worker-Prozesse bringen. `N` muss mindestens `--processes` und die `--autoscale`-Obergrenze decken; mit `--pool cpu` darf es die zugeteilten CPUs nicht überbuchen (`N` ohne `--concurrency`, `N × --concurrency` mit). Ohne dieses Argument liest und schreibt der Supervisor keine Sollwerte und verhält sich exakt wie bisher. Siehe [Kontrollkanal](#kontrollkanal). |
| `--group LABEL` | sortierte Menge der `--queues` | Gruppenschlüssel des Kontrollkanals. Jeder Supervisor mit demselben Schlüssel (im selben Backend) folgt demselben Sollwert, jeder für sich. Deployments, die sich ein Backend teilen und dieselben Queues bedienen, brauchen verschiedene Labels, sonst steuern sie einander. |
| `--reload` | aus | Beobachtet Task-Module und triggert SIGHUP-Reload bei Änderung. Nur Developer. Braucht `pip install 'swarmq[reload]'`. |
| `--watch-dir PATH` | Elternverzeichnis der `--module`-Quelle | Zu überwachendes Verzeichnis (wiederholbar). Ohne `--reload` ignoriert. |
| `--check` | aus | Dry-Run-Validierung: prüft, ob `--module` importierbar ist, ob eine `--configure`-Angabe die Form `MODULE:CALLABLE` hat und ihr Modul auffindbar ist (ohne es zu importieren), mit `--reload` dass `watchfiles` installiert ist und Watch-Dirs auflösbar sind, und dass die Beobachtungs-Portbereiche unter 65535 bleiben und sich nicht überschneiden. Druckt eine JSON-Zusammenfassung inklusive `metrics_ports` und `health_ports` (die genauen Ports, die die Slots binden) und exited `0`/`1` ohne Supervisor-Start oder Broker/Backend-Kontakt. Payload: [Nachrichtenformate](agent/message-formats.de.md#payload-von-swarmq-worker-check). |

### Beispiele

Minimaler lokaler Worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"
swarmq worker --module myapp.tasks
```

Produktiv-Setup mit JSON-Logs, drei Worker-Subprozessen, Prometheus
und Health. Die drei Slots servieren Metriken auf `9100-9102` und
Health auf `9200-9202`:

```bash
swarmq worker \
  --module myapp.tasks \
  --queues priority,default \
  --concurrency 25 \
  --processes 3 \
  --log-format json \
  --metrics-port 9100 \
  --health-port 9200
```

### Middleware per `--configure` registrieren

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ

from myapp.middleware import TimingMiddleware


async def setup(app: SwarmQ) -> None:
    app.add_middleware(TimingMiddleware())

    @app.on_signal(Signal.TASK_REJECTED)
    async def on_rejected(queue, reason, **kwargs):
        log.warning("task_rejected", queue=queue, reason=reason)
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```

Der Hook kann eine gewöhnliche Funktion oder eine Coroutine-Funktion
sein. Er läuft in jedem Worker-Subprozess, nachdem die App existiert
und bevor der Worker sich verbindet; im Supervisor läuft er nicht.
Das Modul sollte deshalb beim Import keine Seiteneffekte haben.

Autoskaliertes Deployment:

```bash
swarmq worker --module myapp.tasks --autoscale 2,10
```

Burst-Mode für einen Cron-Style-Batch-Drain:

```bash
swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
```

### Kontrollkanal

Mit `--max-processes N` ändert ein Betreiber (oder ein steuernder Dienst
wie das swarmq-Dashboard) Prozesszahl und Autoscale-Grenzen einer
Worker-Gruppe im laufenden Betrieb, ohne Deploy und ohne Konsole:

```bash
swarmq worker --module myapp.tasks --autoscale 2,4 --max-processes 16 --group orders-eu
```

**Sollwert.** Der steuernde Dienst schreibt je Gruppe einen JSON-Wert nach
`sq:control:setpoint:<gruppe>`, etwa `{"min": 2, "max": 8}`. Eine feste
Prozesszahl `N` ist der Sollwert `{"min": N, "max": N}` und schaltet die
Autoskalierung für die Gruppe ab. Der Supervisor liest den Schlüssel
einmal, bevor er den ersten Worker startet, und danach alle 5 Sekunden.
Eine höhere Untergrenze startet die fehlenden Worker sofort, eine
niedrigere Obergrenze beendet die überzähligen Worker sanft (der laufende
Task wird fertig, dann endet der Prozess). Wird der Schlüssel gelöscht,
gelten wieder die Startargumente. Ein neuer Deploy fährt deshalb mit dem
zuletzt gesetzten Sollwert hoch, nicht mit der Deployment-Datei.

**Ablehnung.** Ein Sollwert wird abgelehnt -- und die bisherigen Grenzen
bleiben wirksam -- wenn seine Obergrenze über dem Spielraum liegt, seine
Untergrenze unter 1, seine Grenzen verdreht sind, der Wert nicht als
`{"min": N, "max": M}` lesbar ist oder ein Bereich für eine Gruppe ohne
Autoskalierung geschrieben wird (festes `--processes` oder `--pool cpu`
nehmen nur die feste Form `N..N`). Ein Sollwert lässt den Start des
Supervisors nie scheitern: Ist das Backend beim Start nicht erreichbar,
fährt die Gruppe mit ihren Startargumenten hoch und übernimmt den
Sollwert, sobald das Backend antwortet.

**Rückmeldung.** Jeder Supervisor meldet unter
`sq:control:status:<gruppe>:<supervisor>` (die Supervisor-Kennung ist
`SWARMQ_WORKER_ID` oder der Hostname, jeweils plus `:p<pid>`): den
Gruppenschlüssel, den zuletzt gelesenen Sollwert (oder `null`), ob der
Speicher `present`, `absent` oder `unread` (nicht erreichbar) war, die
wirksamen Grenzen samt `source` (`startup` oder `setpoint`), den
`rejection`-Grund, die Slot-Zahlen (`size`, `occupied`, `locked`) und
`reported_at`. Der Schlüssel verfällt nach Gnadenfrist des
Herunterfahrens plus Terminierungsfrist plus einem Abtastintervall; die
Meldung eines toten Supervisors verschwindet also von selbst. Ein Leser
unterscheidet damit „abgelehnt", „noch nicht angekommen" und „Backend
nicht erreichbar", ohne den Prozess anzusehen.

**Was der Spielraum dimensioniert.** Slot-Tabelle, Beobachtungs-Portbereiche
(`--metrics-port`/`--health-port` belegen `PORT..PORT+N-1`) und mit
`--pool cpu` die Voreinstellung der Rechenslots je Worker (zugeteilte
CPUs geteilt durch `N`) werden beim Start einmal aus `N` abgeleitet und
einmal geprüft. Nichts davon wächst zur Laufzeit: mehr Spielraum verlangt
einen Deploy. Das Einschalten auf einem bestehenden Deployment kann daher
eine Portänderung verlangen. Vom Neustart-Budget gesperrte Slots bleiben
gesperrt; die Rückmeldung nennt ihre Zahl.

**Sicherheit.** Wer auf das Backend schreiben darf, kann die Größe einer
Worker-Gruppe setzen. Sollwert- und Statusschlüssel liegen unter eigenen
Präfixen (`sq:control:setpoint:` und `sq:control:status:`), getrennt von
Task- und Ergebnisschlüsseln, sodass ein Betreiber Schreibrechte für
Sollwerte und für Rückmeldungen getrennt vergeben kann (etwa per
Valkey-ACL). Solange der steuernde Dienst keine eigenen Zugangsdaten mit
so eingeschränkten Schreibrechten hat, kann jeder Task-Erzeuger, der die
Backend-URL teilt, auch Sollwerte setzen.

## `swarmq schedule`

Verwaltet die Schedule-Definitionen des eingebetteten Schedulers in
Valkey. Braucht `SWARMQ_BACKEND_URL`; verbindet sich nicht mit dem
Broker.

### `swarmq schedule list`

```text
swarmq schedule list [--format (table|json)]
```

Listet jeden Schedule in `sq:sched:defs` mit Cron, Next-Fire-Zeit,
Queue und Dedup-Zustand auf.

### `swarmq schedule add`

```text
swarmq schedule add NAME
                    (--cron EXPR | --at ISO_DATETIME)
                    --task TASK_NAME
                    [--args JSON_ARRAY] [--kwargs JSON_OBJECT]
                    [--queue QUEUE]
                    [--unique-for SECONDS]
                    [--unique-until (start|completion)]
                    [--format (table|json)]
```

Legt einen neuen Schedule an (Insert-if-absent). Übergib genau
eines von `--cron` (5-Feld-Expression) oder `--at` (ISO-8601-
Einzelzeit). `--args` und `--kwargs` müssen JSON sein.

Beispiel:

```bash
swarmq schedule add nightly-billing \
  --cron '0 3 * * *' \
  --task run_billing \
  --queue priority \
  --kwargs '{"tenant": "acme"}'
```

### `swarmq schedule remove`

```text
swarmq schedule remove NAME [--permanent] [--format (table|json)]
```

Ohne `--permanent` wird der Schedule nur deaktiviert, bleibt aber
erhalten. Mit `--permanent` wird er hart gelöscht, damit
Insert-if-absent aus Code resync'en kann.

### `swarmq schedule status`

```text
swarmq schedule status [--limit N] [--format (table|json)]
```

Druckt den aktuellen Leader-Worker (falls vorhanden) und die
nächsten N Schedules, die innerhalb 24 Stunden anstehen.

## `swarmq dlq`

Operiert auf `{queue}.XQ`, der Broker-Level-Dead-Letter-Queue.
Braucht `SWARMQ_BROKER_URL`; verbindet sich nicht mit dem Backend.

### `swarmq dlq list`

```text
swarmq dlq list [--queue QUEUE] [--limit N] [--format (table|json)]
```

Drain-and-Republish-Scan — zeigt `task_id`, `task_name`, Retry-Count-
Header, Fehler-Preview, Body-Größe. Wiederholte Aufrufe sind safe:
Nachrichten bleiben in der DLQ. Die Tabellenausgabe zeigt
`(truncated)`, wenn `--limit` erreicht ist; `--format json`
enthält einen `truncated: bool`-Envelope.

### `swarmq dlq inspect`

```text
swarmq dlq inspect TASK_ID [--queue QUEUE]
                            [--scan-limit N]
                            [--format (table|json)]
```

Findet eine DLQ-Nachricht anhand `task_id` und druckt ihre vollen
Header und den dekodierten Body. Non-destruktiv. Exited `3`, wenn
die task_id nicht gefunden wird (nutze `--scan-limit`, um die
Suche zu verbreitern).

### `swarmq dlq retry`

```text
swarmq dlq retry (TASK_ID | --all)
                 [--queue QUEUE] [--limit N] [--scan-limit N]
                 [--format (table|json)]
```

Publisht zurück in die Haupt-Queue. `--all` bewegt jede Nachricht
(bis zu `--limit`, Default `10000`). Der
`x-swarmq-retry-count`-Header wird zurückgesetzt, damit der Task
sein volles Retry-Budget bekommt; alle anderen Header bleiben.
Exited `3`, wenn ein Einzel-Task-Retry nichts findet.

### `swarmq dlq purge`

```text
swarmq dlq purge [--queue QUEUE] [--format (table|json)]
```

Natives AMQP `queue.purge()` auf `{queue}.XQ`. Unumkehrbar — nutze
es, wenn der DLQ-Inhalt bekannt schlecht und nicht inspektionswürdig
ist.

## `swarmq inspect`

```text
swarmq inspect --module DOTTED.PATH [--format (table|json)]
```

Importiert ein Modul, registriert seine `Task`-Subklassen und
druckt die `TaskInfo`-Metadaten jedes Tasks — Queues, Retry-Policy,
Rate-Limit, Lock, Dedup-Settings, Cron. Read-only. Verbindet sich
nicht mit Broker oder Backend. Nützlich als Container-Healthcheck
oder in CI.

### Beispiel

```bash
$ swarmq inspect --module myapp.tasks
TASK           QUEUES        DELIVERY       RETRIES  RATE_LIMIT  LOCK               UNIQUE_FOR  UNIQUE_UNTIL  CRON
render_invoice pdf           at_least_once  5        -           -                  -           -             -
send_email     default,mail  at_least_once  3        100/m       -                  -           -             -
sync_user      users         at_least_once  2        -           user-{user_id}     60          completion    -
```

## Globale Flags

| Flag | Beschreibung |
|---|---|
| `--version` | Druckt `swarmq <version>` und exited `0`. |
| `-h`, `--help` | Druckt die Hilfe zum aktuellen Command/Subcommand und exited `0`. |

## Siehe auch

- [Konfiguration](configuration.md) — die Env-Vars, die diese Flags schatten
- [Logging](logging.md) — Ausgabeformat, Events und Integrationsrezepte
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) — Recovery-
  Workflow mit `swarmq dlq`
- [Hot Reload (nur Dev)](../guides/hot-reload.md) — wann `--reload`
  nutzen und wann nicht
