# Konfiguration

SwarmQ hat zwei Konfigurationsoberflächen: **Environment-Variablen**,
die der Worker beim Start liest (und die auch die CLI ehrt), und
**`TaskInfo`-Felder**, die pro Task-Klasse deklariert werden.
CLI-Flags haben Vorrang vor Env-Vars; Env-Vars haben Vorrang vor
`SwarmQConfig`-Defaults.

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
| `SWARMQ_LOG_LEVEL` | `INFO` | Einer aus `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `SWARMQ_LOG_JSON` | `true` | `true`/`1`/`yes` für strukturierte JSON-Logs, alles andere für human-formatierten Output. |
| `SWARMQ_RESULT_TTL` | `86400` | Sekunden, die gespeicherte Ergebnisse in Valkey leben. |
| `SWARMQ_RETRY_MAX_DELAY` | `3600.0` | Obergrenze des Retry-Backoffs in Sekunden. |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` | Sekunden, in denen In-Flight-Tasks bei SIGTERM beenden dürfen, bevor Hard-Kill. |
| `SWARMQ_QUEUE_TYPE` | `quorum` | `quorum` (empfohlen) oder `classic` RabbitMQ-Queue-Typ. |
| `SWARMQ_CANCEL_POLL_INTERVAL` | `1.0` | Sekunden zwischen In-Task-Cancel-Flag-Checks. |
| `SWARMQ_BACKEND_OP_TIMEOUT` | `5.0` | Obergrenze einzelner Valkey-Operationen in der Cancel-/Dedup-Pipeline. |
| `SWARMQ_BURST_MODE` | `false` | Bei `true` exited der Worker nach `SWARMQ_BURST_IDLE_TIMEOUT` Sekunden Queue-Stille. |
| `SWARMQ_BURST_IDLE_TIMEOUT` | `5.0` | Sekunden Idle, bevor Burst exitet. |
| `SWARMQ_PRIORITIES` | `false` | Bei `true` werden `.high`/`.normal`/`.low`-Triplets für jede Queue auto-deklariert und konsumiert. |

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
| `SWARMQ_METRICS_PORT` | *ungesetzt* | Wenn gesetzt, serve `/metrics` auf diesem Port. Braucht `pip install swarmq[metrics]`. Muss `1..65535` sein. |
| `SWARMQ_METRICS_HOST` | `127.0.0.1` | Bind-Adresse für den Metrics-Server. |
| `SWARMQ_HEALTH_PORT` | *ungesetzt* | Wenn gesetzt, serve `/health` und `/ready` auf diesem Port. Muss `1..65535` sein. |
| `SWARMQ_HEALTH_HOST` | `127.0.0.1` | Bind-Adresse für den Health-Server. |

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
    log_level="DEBUG",
    log_json=True,
    result_ttl=3600,
    shutdown_timeout=60,
    burst_mode=False,
    priorities=True,
)
```

Feldnamen entsprechen den Env-Vars ohne `SWARMQ_`-Präfix und in
Kleinbuchstaben. Env-Vars gewinnen immer, wenn beides gesetzt ist
— übergib kwargs nur, wenn du einen Nicht-Default brauchst, den du
nicht im Shell-Environment willst.

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
- [API-Referenz](api.md) — `SwarmQ`, `TaskInfo`, `Task`-Signaturen
- [Tasks definieren](../guides/defining-tasks.md) — Beispiele je
  `TaskInfo`-Feld
