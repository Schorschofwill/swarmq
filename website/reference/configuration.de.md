# Konfiguration

SwarmQ hat zwei Konfigurationsoberflächen: **Environment-Variablen**,
die der Worker beim Start liest (und die auch die CLI ehrt), und
**`TaskInfo`-Felder**, die pro Task-Klasse deklariert werden.
CLI-Flags haben Vorrang vor Env-Vars; Env-Vars haben Vorrang vor
`SwarmQConfig`-Werten und -Defaults.

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
| `SWARMQ_CONCURRENCY` | `10` | Maximale Tasks in Flight pro Worker-Prozess. |
| `SWARMQ_RESULT_TTL` | `86400` | Sekunden, die gespeicherte Ergebnisse in Valkey leben. |
| `SWARMQ_RETRY_MAX_DELAY` | `3600.0` | Obergrenze des Retry-Backoffs in Sekunden. |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` | Sekunden, in denen In-Flight-Tasks bei SIGTERM beenden dürfen, bevor Hard-Kill. |
| `SWARMQ_PUBLISH_TIMEOUT_S` | `120.0` | Obergrenze für einen kompletten Publish-Vorgang inklusive interner Wiederholversuche. Wird sie überschritten, scheitert der Publish mit einem Broker-Verbindungsfehler, statt unbegrenzt auf eine Bestätigung zu warten, die ein still toter Transport nie liefert. Beim Herunterfahren gilt die kürzere aus diesem Wert und der verbleibenden Gnadenfrist. |
| `SWARMQ_DELIVERY_LIMIT` | `20` | Zustellbudget für Quorum-Queues (`x-delivery-limit`); bei klassischen Queues ohne Wirkung. Ist eine Nachricht so oft zugestellt worden, wirft der Broker sie selbst in die Dead-Letter-Queue — das Sicherheitsnetz gegen einen Task, der den Prozess zuverlässig abschießt (SIGSEGV/OOM) und deshalb ewig neu zugestellt würde, ohne je `max_retries` zu erreichen. Der Default deckt drei bis vier fachliche Wiederholungen je Zustellung plus Puffer für Broker-Schluckauf. Dieser Puffer ist seit dem Publish-Deckel schmaler: Bricht ein Publish im Worker an seiner Frist ab, wird die bearbeitete Nachricht neu zugestellt statt bestätigt, und jede Runde kostet eine Zustellung. Höher setzen, wer längere Broker-Störungen aussitzen muss; niedriger, wer ein engeres Zustellfenster erzwingen will. |
| `SWARMQ_QUEUE_TYPE` | `quorum` | `quorum` (empfohlen) oder `classic` RabbitMQ-Queue-Typ. Nur Quorum-Queues tragen `x-delivery-limit`; ohne wird eine Nachricht, die Worker zum Hängen bringt, unbegrenzt erneut zugestellt (der Worker warnt beim Start). |
| `SWARMQ_POOL` | `io` | `io` oder `cpu` — siehe [CPU-Modus](../guides/cpu-mode.md). |
| `SWARMQ_RECYCLE_RSS_MB` | ungesetzt (`250` im CPU-Modus) | Prozess nach einem Task recyceln, sobald sein RSS diese MB überschreitet. IO-Worker: Opt-in-Selbst-Recycling; CPU-Modus: Schwelle für wiederverwendete DI-Kinder. |
| `SWARMQ_RECYCLE_MAX_TASKS` | ungesetzt (`1000` im CPU-Modus) | Prozess nach so vielen Tasks recyceln (zweite Linie hinter der RSS-Schwelle). |
| `SWARMQ_HEARTBEAT_INTERVAL_S` | `5` | Wie oft der Worker seinen Lebenszeichen-Zeitstempel für den Supervisor schreibt. |
| `SWARMQ_HEARTBEAT_TIMEOUT_S` | `60` | Nach so langer Zeit ohne Lebenszeichen ersetzt der Supervisor den Worker (SIGTERM, dann SIGKILL). Muss über dem Intervall und jeder legitimen Loop-Blockade liegen; unter RabbitMQs Heartbeat-Erkennung bleiben. |
| `SWARMQ_CONSUMER_TIMEOUT_S` | `1800` | `consumer_timeout` des Brokers; der Worker warnt, wenn ein `hard_timeout` darüber liegt. |
| `SWARMQ_MALLOC_TRIM_THRESHOLD` | `131072` | `MALLOC_TRIM_THRESHOLD_` für Rechenkinder (glibc-Wachstum). |
| `SWARMQ_MALLOC_ARENA_MAX` | `2` | `MALLOC_ARENA_MAX` für Rechenkinder. |
| `SWARMQ_COMPUTE_THREAD_LIMIT` | `1` | Threads je Rechenkind (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`, `MAGICK_THREAD_LIMIT`). |
| `SWARMQ_CANCEL_POLL_INTERVAL` | `1.0` | Sekunden zwischen In-Task-Cancel-Flag-Checks. |
| `SWARMQ_BACKEND_OP_TIMEOUT` | `5.0` | Obergrenze einzelner Valkey-Operationen in der Cancel-/Dedup-Pipeline. |
| `SWARMQ_BURST_MODE` | `false` | Bei `true` exited der Worker nach `SWARMQ_BURST_IDLE_TIMEOUT` Sekunden Queue-Stille. |
| `SWARMQ_BURST_IDLE_TIMEOUT` | `5.0` | Sekunden Idle, bevor Burst exitet. |
| `SWARMQ_PRIORITIES` | `false` | Bei `true` werden `.high`/`.normal`/`.low`-Triplets für jede Queue auto-deklariert und konsumiert. |

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
| `SWARMQ_LOG_QUEUE` | `false` | Eingebettete Apps: `swarmq.*`-Events über einen Hintergrund-Thread von SwarmQ an deine Root-Handler leiten ([Opt-in-Log-Queue](logging.md#opt-in-log-queue)). |

Der Supervisor von `swarmq worker` löst diese Werte einmal auf (CLI-Flag
vor Env-Var) und gibt das Ergebnis an seine Worker-Subprozesse und
Rechenkinder weiter, die das Environment nicht noch einmal lesen.
Ungültige Werte beenden `swarmq worker` mit Exit `78`.

### Scheduler

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_SCHEDULER_POLL_INTERVAL` | `1.0` | Sekunden zwischen Scheduler-Tick-Polls. |
| `SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL` | `10.0` | Sekunden zwischen Leader-Heartbeat-Renews. |
| `SWARMQ_SCHEDULER_LOCK_TTL` | `30.0` | Leader-Lock-TTL; muss `≥ 2 × heartbeat_interval` sein. |
| `SWARMQ_SCHEDULER_DEDUP_TTL` | `max(60, retry_max_delay + 300)` | Dedup-TTL für scheduler-initiierte Dispatches. |
| `SWARMQ_SCHEDULER_INFLIGHT_TTL` | `300.0` | Pre-Run-Marker-TTL während ein geplanter Task läuft. |

### Observability

| Variable | Default | Beschreibung |
|---|---|---|
| `SWARMQ_METRICS_PORT` | *ungesetzt* | **Basisport** für `/metrics`. Der Worker in Slot `s` serviert auf `SWARMQ_METRICS_PORT + s`; ein eingebetteter Worker ohne Slot serviert auf dem Basisport selbst. Braucht `pip install swarmq[metrics]`. Muss `1..65535` sein. |
| `SWARMQ_METRICS_HOST` | `127.0.0.1` | Bind-Adresse für den Metrics-Server. `0.0.0.0` setzen, wenn Prometheus von einem anderen Host oder Container scrapt. |
| `SWARMQ_HEALTH_PORT` | *ungesetzt* | **Basisport** für `/health` und `/ready`, derselbe Slot-Versatz. Muss `1..65535` sein. |
| `SWARMQ_HEALTH_HOST` | `127.0.0.1` | Bind-Adresse für den Health-Server. |
| `SWARMQ_WORKER_ID` | Hostname | Basis der Worker-Identität, die als `worker_id` in Signalen, Metrik-Labels und im Logkontext erscheint. Siehe [Worker-Identität](#worker-identitat). |

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
Kleinbuchstaben. Env-Vars gewinnen immer, wenn beides gesetzt ist
— übergib kwargs nur, wenn du einen Nicht-Default brauchst, den du
nicht im Shell-Environment willst. Eine Ausnahme von der Namensregel:
`SWARMQ_LOG_FORMAT` setzt das Feld `log_json`. `worker_id=` setzt die Basis der
[Worker-Identität](#worker-identitat); ein eingebetteter Worker hat
keinen Slot, `metrics_port` und `health_port` werden also unverändert
gebunden.

`SwarmQ(...)` konfiguriert kein Logging. Die Logging-Felder
(`log_level`, `log_json`, `log_config`, `configure_logging`,
`logger_levels`, `log_queue_size`, `log_lifecycle_level`, `log_queue`)
steuern nur die [Opt-in-Log-Queue](logging.md#opt-in-log-queue), das
Lebenszyklus-Level eines eingebetteten Workers und die Rechenkinder
eines eingebetteten CPU-Pools.

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
