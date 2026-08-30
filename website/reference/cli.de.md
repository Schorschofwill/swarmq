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
| `1` | Laufzeit- oder Config-Fehler (Broker/Backend unerreichbar, Modul-Import schlug fehl, `ConfigurationError`) |
| `2` | Ungültige Argumente (argparse-Usage-Fehler) |
| `3` | `swarmq dlq inspect` / `swarmq dlq retry <task_id>` fand keinen passenden DLQ-Eintrag |

`--log-level DEBUG` fügt einen vollen Python-Traceback zu stderr
hinzu, wenn Exit-Code `1` feuert.

## `swarmq worker`

Importiert ein Python-Modul, sodass seine `Task`-Subklassen
registriert werden, verbindet sich mit Broker + Backend und
konsumiert. `SWARMQ_BROKER_URL` und `SWARMQ_BACKEND_URL` müssen
gesetzt sein.

```text
swarmq worker --module DOTTED.PATH
              [--queues A,B,C]
              [--concurrency N]
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
| `--queues A,B,C` | `default` | Komma-separierte Queue-Namen zum Konsumieren. Env-Var: `SWARMQ_QUEUES`. |
| `--concurrency N` | `10` | Maximal parallel ausgeführte Tasks. Env-Var: `SWARMQ_CONCURRENCY`. |
| `--log-level LEVEL` | `INFO` | Einer aus `DEBUG`, `INFO`, `WARNING`, `ERROR`. Env-Var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` oder `json`. JSON passt zu Loki, Datadog, CloudWatch. Env-Var: `SWARMQ_LOG_JSON`. |
| `--burst` | aus | Exited sauber, sobald alle Queues `--burst-idle-timeout` Sekunden Idle waren. Env-Var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Sekunden Queue-Stille, bevor `--burst` exited. Env-Var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--priorities` | aus | Auto-Deklaration und Konsum des `{queue}.high`/`.normal`/`.low`-Triplets für jede Queue. Env-Var: `SWARMQ_PRIORITIES`. |
| `--metrics-port PORT` | ungesetzt | Serviert Prometheus `/metrics` auf diesem Port. Braucht `pip install 'swarmq[metrics]'`. Bind-Host: `SWARMQ_METRICS_HOST` (Default `127.0.0.1`). |
| `--health-port PORT` | ungesetzt | Serviert `/health` + `/ready` auf diesem Port. Bind-Host: `SWARMQ_HEALTH_HOST` (Default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Lässt N Subprozesse unter einem Supervisor laufen. Der Supervisor startet gecrashte Worker neu (max 5 pro Stunde) und reloaded auf `SIGHUP` via `os.execvp()`. |
| `--autoscale MIN,MAX` | ungesetzt | Aktiviert Queue-tiefen-getriebenes Autoscaling zwischen `MIN` und `MAX` Worker-Subprozessen. Impliziert `--processes`. |
| `--reload` | aus | Beobachtet Task-Module und triggert SIGHUP-Reload bei Änderung. Nur Developer. Braucht `pip install 'swarmq[reload]'`. |
| `--watch-dir PATH` | Elternverzeichnis der `--module`-Quelle | Zu überwachendes Verzeichnis (wiederholbar). Ohne `--reload` ignoriert. |
| `--check` | aus | Dry-Run-Validierung: prüft, ob `--module` importierbar ist, mit `--reload` dass `watchfiles` installiert ist und Watch-Dirs auflösbar sind, und dass Mehrprozess-Betrieb nicht mit einem festen `--metrics-port`/`--health-port` kombiniert wird. Druckt eine JSON-Zusammenfassung und exited `0`/`1` ohne Supervisor-Start oder Broker/Backend-Kontakt. |

### Beispiele

Minimaler lokaler Worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"
swarmq worker --module myapp.tasks
```

Produktiv-Setup mit JSON-Logs, drei Worker-Subprozessen, Prometheus
und Health:

```bash
swarmq worker \
  --module myapp.tasks \
  --queues priority,default \
  --concurrency 25 \
  --processes 3 \
  --log-format json \
  --metrics-port 9108 \
  --health-port 9109
```

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
