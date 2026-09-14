# CLI-Referenz

Das `swarmq`-Binary wird als Console-Script installiert, wenn du
`pip install swarmq` aufrufst. Jedes Subcommand liest per Default
aus `SWARMQ_*`-Environment-Variablen; explizite Flags gewinnen.

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
| `78` | Konfigurationsfehler von `swarmq worker` (`EX_CONFIG`): `ConfigurationError`, Task-Modul nicht importierbar, `--configure`-Hook gescheitert, Portbereich ungültig. Kommt aus dem Elternprozess und aus dem Supervisor, wenn ein Worker-Kind mit `78` endet. Ein erneuter Versuch ohne Änderung ist sinnlos. |

`78` folgt `sysexits.h`. Jeder Konfigurationsfehler, ob er im
Elternprozess oder in einem Worker-Subprozess auftritt, beendet den
gesamten Lauf nach einem einzigen Versuch mit `78` und schreibt einen
strukturierten Logsatz `worker_config_error` im konfigurierten
`--log-format`. Verbindungsfehler bleiben bei `1`, weil dort „gleich
nochmal" richtig ist. Unter systemd `RestartPreventExitStatus=78`
setzen, damit ein kaputtes Deployment nicht in der Schleife neu
startet; in Kubernetes auf den Exit-Code statt auf Neustarts alarmieren.
Exit `75` (`EX_TEMPFAIL`) ist intern: Ein Worker-Kind, das Recycling
angefragt und keine Bestätigung bekommen hat, endet damit, der
Supervisor loggt `worker_recycle_noack` und startet es im Budget neu;
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
              [--burst] [--burst-idle-timeout SECONDS]
              [--priorities]
              [--metrics-port PORT] [--health-port PORT]
              [--processes N | -p N] [--autoscale MIN,MAX]
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
| `--log-level LEVEL` | `INFO` | Einer aus `DEBUG`, `INFO`, `WARNING`, `ERROR`. Env-Var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` oder `json`. JSON passt zu Loki, Datadog, CloudWatch. Env-Var: `SWARMQ_LOG_JSON`. |
| `--burst` | aus | Exited sauber, sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren. Env-Var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Sekunden Queue-Stille, bevor `--burst` exited. Env-Var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--priorities` | aus | Auto-Deklaration und Konsum des `{queue}.high`/`.normal`/`.low`-Triplets für jede Queue. Env-Var: `SWARMQ_PRIORITIES`. |
| `--metrics-port PORT` | ungesetzt | **Basisport** für Prometheus `/metrics`: Der Worker in Slot `s` serviert auf `PORT + s`, `--processes 3 --metrics-port 9000` belegt also `9000-9002`. Braucht `pip install 'swarmq[metrics]'`. Bind-Host: `SWARMQ_METRICS_HOST` (Default `127.0.0.1`). Siehe [Basisports](configuration.de.md#observability). |
| `--health-port PORT` | ungesetzt | **Basisport** für `/health` + `/ready`, derselbe Slot-Versatz wie bei `--metrics-port`. Bind-Host: `SWARMQ_HEALTH_HOST` (Default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Lässt N Worker-Subprozesse in den Slots `0..N-1` laufen. Seit 0.5.0 läuft jeder `swarmq worker` unter dem Supervisor, auch mit `N = 1`: Er startet gecrashte Worker im selben Slot neu (max 5 pro Stunde), ersetzt Worker ohne Lebenszeichen, reloaded auf `SIGHUP` via `os.execvp()` und endet mit 0, sobald alle Worker sauber beendet sind (`--burst`). Ein Konfigurationsfehler in einem beliebigen Worker beendet den Lauf nach genau einem Versuch mit Exit `78`. |
| `--autoscale MIN,MAX` | ungesetzt | Aktiviert Queue-tiefen-getriebenes Autoscaling zwischen `MIN` und `MAX` Worker-Subprozessen. Impliziert `--processes`. |
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
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) — Recovery-
  Workflow mit `swarmq dlq`
- [Hot Reload (nur Dev)](../guides/hot-reload.md) — wann `--reload`
  nutzen und wann nicht
