# Nachrichtenformate

Kanonische Wire-Format-Referenz für jeden Payload, den SwarmQ an
den Broker legt, nach Valkey schreibt oder als Signal dispatcht.
Typen sind als JSON/msgspec-kompatible Formen ausgedrückt;
Python-spezifische Notation wird vermieden.

## Task-Nachricht (RabbitMQ)

Mit msgspec als JSON kodiert, auf den Routing-Key publisht, der
zur Ziel-Queue passt.

```json
{
  "task": {
    "id": "01HV3W3YGBN4ZCZ45F2D7R9PB8",
    "queue": "emails",
    "name": "send_email"
  },
  "args": ["first-positional", 42],
  "kwargs": {"to": "alice@example.com", "subject": "Hi"}
}
```

| Feld | Typ | Notizen |
|---|---|---|
| `task.id` | `string` | UUIDv7 per Default; Caller-gelieferte IDs akzeptiert `Client.schedule` auch. |
| `task.queue` | `string` | Voll aufgelöster Queue-Name inkl. Priority-Suffix (`.high`/`.low`), wenn angewendet. |
| `task.name` | `string` | Muss zu einer registrierten `TaskInfo.name` passen. |
| `args` | `array<any>` | An `Task.run()` weitergegebene Positional-Args. Nur JSON-Typen. |
| `kwargs` | `object<any>` | An `Task.run()` weitergegebene Keyword-Args. Nur JSON-Typen. |

### AMQP-Header

Die folgenden Header werden von SwarmQ beim Publish oder Consume
gestempelt. Alle sind aus Konsumentensicht optional — Fehlen
bedeutet, das Feature ist nicht aktiv.

| Header | Typ | Geschrieben von | Bedeutung |
|---|---|---|---|
| `x-swarmq-enqueued-at` | `int` (Unix-Sekunden) | Client / Scheduler | Genutzt vom `max_age`-Check des Workers. |
| `x-swarmq-retry-count` | `int` | Worker | Monotoner Counter; von `swarmq dlq retry` zurückgesetzt. |
| `x-swarmq-traceback` | `string` | Worker (bei Fehler) | Voller Python-Traceback des raisenden Versuchs. |
| `x-swarmq-unique-hash` | `string` (hex) | Client | SHA-256 der kanonischen `(name, args, kwargs)`. |
| `x-swarmq-unique-until` | `"start"` \| `"completion"` | Client | Wann der Worker den Dedup-Flag freigibt. |
| `x-swarmq-schedfire` | `int` (Unix-ms) | Scheduler | Vorhanden auf scheduler-publishten Nachrichten; Worker dedupt darauf. |
| `x-swarmq-expired` | `"1"` | Worker (bei Expiry) | Gesetzt, bevor die Nachricht DLQ-geroutet wird. |
| `x-swarmq-lock-error` | `"render_failed"` \| `"conflict_cap"` | Worker (bei Lock-Fehler) | Surfact Lock-System-Fehler auf DLQ'te Nachrichten. |
| `x-swarmq-error` | `"no_retry"` | Worker | Vorhanden, wenn `NoRetry` die Nachricht geroutet hat. |
| `x-death` | `array` | RabbitMQ | Broker-native DLX-Audit-Trail. |

## Task-Ergebnis (Valkey)

Gespeichert unter `sq:result:{task_id}`, mit msgpack kodiert, TTL
gleich dem aufgelösten `result_ttl`.

```jsonc
{
  "task": {"id": "01HV...", "name": "send_email", "queue": "emails"},
  "is_error": false,
  "is_cancelled": false,
  "result": "sent to alice@example.com",
  "error_message": null,
  "error_class": null
}
```

| Feld | Typ | Notizen |
|---|---|---|
| `task.id` | `string` | Echot die `task.id` der Task-Nachricht. |
| `task.name` | `string` | Echot die `task.name` der Task-Nachricht. |
| `task.queue` | `string` | Echot die `task.queue` der Task-Nachricht. |
| `is_error` | `bool` | `true`, wenn der Task beim letzten Versuch eine uncaught Exception raiste. |
| `is_cancelled` | `bool` | `true`, wenn der Task via `Client.cancel` abgebrochen wurde. |
| `result` | `any` | Vorhanden, wenn `is_error=false` und `is_cancelled=false`. msgpack-native Typen. |
| `error_message` | `string \| null` | `str(exc)` vom scheiternden Versuch; vorhanden, wenn `is_error=true`. |
| `error_class` | `string \| null` | Voll qualifizierter Klassenname der Exception. Beratend — nicht garantiert Wire-Encoding zukünftiger Versionen zu überleben. |

### Dekodierungs-Priorität

Beim Konsum eines Ergebnisses in dieser Reihenfolge auswerten:

1. `is_cancelled` → `TaskCancelledError` raisen.
2. `is_error` → `TaskError(error_message)` raisen.
3. Sonst → `result` zurückgeben.

## Progress-Update (Valkey)

Gespeichert unter `sq:progress:{task_id}` als Valkey-HASH. Auf dem
Pub/Sub-Channel `sq:progress:channel:{task_id}` als einzelnes
JSON-Dokument mit denselben Feldern published.

```jsonc
{
  "current": "42",
  "total": "100",
  "percent": "42.00",
  "message": "item 42 rendered",
  "updated_at": "2026-04-21T12:34:56.789012+00:00",
  "started_at": "2026-04-21T12:30:00.000000+00:00",
  "eta_seconds": "380"
}
```

Alle Werte sind auf der HASH-Ebene Strings. Clients dekodieren als:

| Feld | Ziel-Typ | Hinweis |
|---|---|---|
| `current` | `int` | |
| `total` | `int` | |
| `percent` | `float` | `0.0`–`100.0` |
| `message` | `string` | Kann leer sein. |
| `updated_at` | ISO-8601-Datetime, UTC | Immer vorhanden. |
| `started_at` | ISO-8601-Datetime, UTC \| leer | Leer bis zum ersten Update. |
| `eta_seconds` | `int` \| leer | Leer bei `current=0` und beim finalen Flush. |

Ein HASH, dem `current`, `total` oder `updated_at` fehlt, wird
als malformed behandelt und als `None` zurückgegeben.

## Scheduler-Eintrag

Gespeichert im Valkey-ZSET `sq:sched:fire` (Score = Fire-Timestamp
in ms) mit der Definition unter `sq:sched:defs:{name}`.

```jsonc
{
  "schedule_name": "nightly-billing",
  "task_name": "run_billing",
  "args": [],
  "kwargs": {"tenant": "acme"},
  "queue": "priority",
  "cron": "0 3 * * *",
  "fire_ts_ms": 1735272000000,
  "source": "cli",
  "dedup_hash": null,
  "unique_until": "completion",
  "disabled": false
}
```

| Feld | Typ | Notizen |
|---|---|---|
| `schedule_name` | `string` | Primärschlüssel. |
| `task_name` | `string` | Muss auf einen registrierten Task abbilden. |
| `args` | `array<any>` | Positional-Args zur Fire-Zeit. |
| `kwargs` | `object<any>` | Keyword-Args zur Fire-Zeit. |
| `queue` | `string` | Override für `TaskInfo.queues[0]`. |
| `cron` | `string \| null` | 5-Feld-Cron; `null` für One-Shot-Schedules. |
| `fire_ts_ms` | `int` | Nächste Fire-Zeit. |
| `source` | `string` | `"cli"`, `"cli_unique"`, `"python"` usw. |
| `dedup_hash` | `string \| null` | SHA-256 hex, wenn Dedup aktiv. |
| `unique_until` | `"start" \| "completion"` | Dedup-Freigabemodus. |
| `disabled` | `bool` | `true` nach `schedule remove` (ohne `--permanent`). |

## DLQ-Envelope

`swarmq dlq list --format json` und `swarmq dlq inspect --format
json` geben Envelopes dieser Form zurück:

### `dlq list`

```jsonc
{
  "entries": [
    {
      "task_id": "01HV...",
      "task_name": "send_email",
      "queue": "emails.XQ",
      "body_size": 312,
      "headers": {"x-swarmq-retry-count": 3, "x-swarmq-traceback": "..."},
      "body": {"task": {...}, "args": [], "kwargs": {...}}
    }
  ],
  "scanned": 1,
  "limit": 100,
  "truncated": false,
  "queue": "emails"
}
```

### `dlq inspect`

Gefunden:

```jsonc
{
  "found": true,
  "scanned": 17,
  "task_id": "01HV...",
  "task_name": "send_email",
  "queue": "emails.XQ",
  "body_size": 312,
  "headers": { ... },
  "body": { ... }
}
```

Nicht gefunden (Exit-Code `3`):

```jsonc
{
  "found": false,
  "task_id": "01HV...",
  "queue": "emails",
  "scanned": 1000,
  "scan_exhausted": true
}
```

### `dlq retry`

Einzelne task_id:

```jsonc
{
  "moved": 1,
  "queue": "emails",
  "task_id": "01HV...",
  "found": true,
  "scanned": 1,
  "scan_exhausted": false
}
```

`--all`:

```jsonc
{
  "moved": 42,
  "scanned": 45,
  "skipped_malformed": 3,
  "truncated": false,
  "queue": "emails",
  "all": true
}
```

### `dlq purge`

```jsonc
{"purged": 42, "queue": "emails"}
```

## Cancel-Flag

Gespeichert unter `sq:cancel:{task_id}` in Valkey mit TTL
`result_ttl × 1000` ms. Der Wert ist ein fester Marker-String —
Anwesenheit ist das Signal.

| Key-Form | Typ | Semantik |
|---|---|---|
| `sq:cancel:{task_id}` | `string` ("1") | Vorhanden → Cancel angefordert; abwesend → kein Cancel. |

Der Cancel-Monitor des Workers macht periodisch
`EXISTS`-Checks auf diesen Key (`cancel_poll_interval`, Default
1.0 s) und an jeder Valkey-Operationsgrenze. Wird gesetzt, während
der Task läuft, raist der Worker `TaskCancelledError` innerhalb
der Task-Coroutine.

## Dedup-Flag

Gespeichert unter `sq:unique:{sha256_hex}` in Valkey mit TTL
`unique_for × 1000` ms (plus beliebigem Fire-Offset für Deferred-
Pfade).

| Key-Form | Wert | Semantik |
|---|---|---|
| `sq:unique:{hash}` | `{task_id}` | Owner-task_id. Freigabe ist owner-checked via Lua. |

`compute_dedup_hash(task_name, args, kwargs)` produziert den Hash.
`args` ist ein Tupel, `kwargs` ist ein Dict, vor der Serialisierung
nach Key sortiert.

## Signal-Payloads

Signals feuern in-process innerhalb von SwarmQ-Workern und
-Clients. Handler bekommen folgende Kwargs pro Signal. Felder, die
als *(optional)* markiert sind, können in einigen Quellen fehlen
— nutze immer `.get()`-Style-Zugriff.

### `TASK_ENQUEUED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails"}
```

### `TASK_STARTED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email"}
```

### `TASK_COMPLETED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "duration_ms": 312}
```

### `TASK_FAILED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "exception": "<Exception>"}
```

### `TASK_RETRYING`

```jsonc
{"task_id": "01HV...", "retry_count": 1, "delay": 2.0}
```

### `TASK_CANCELLED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email"}
```

### `TASK_EXPIRED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "age_seconds": 120}
```

### `TASK_LOCKED`

```jsonc
{"task_id": "01HV...", "task_name": "sync_user", "lock_name": "user-42"}
```

### `TASK_RATE_LIMITED`

```jsonc
{
  "task_id": "01HV...",
  "task_name": "send_sms",
  "retry_after_ms": 1200,
  "remaining": 3,
  "rate_limit_count": 4
}
```

### `TASK_DUPLICATE_DETECTED`

```jsonc
{
  "existing_task_id": "01HV...",
  "task_name": "send_welcome",
  "dedup_hash": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3...",
  "source": "client.schedule",
  "batch_index": 2
}
```

- `source` ist einer von `"client.schedule"`,
  `"client.schedule_deferred"`, `"client.schedule_many"`,
  `"scheduler.fire"`.
- `batch_index` *(optional)* ist nur vorhanden, wenn
  `source == "client.schedule_many"`.

### `WORKER_*`

```jsonc
{"worker_id": "worker-01HV..."}
```

Emittiert für `WORKER_ONLINE`, `WORKER_SHUTDOWN`,
`WORKER_HEARTBEAT`.

### `SCHEDULER_*`

```jsonc
{"worker_id": "worker-01HV..."}
```

Emittiert für `SCHEDULER_LEADER`, `SCHEDULER_STANDBY`,
`SCHEDULER_HEARTBEAT`, `SCHEDULER_LEADER_HANDOVER`,
`SCHEDULER_LEADER_FAILOVER`.

## Siehe auch

- [Entscheidungsbäume](decision-trees.md) — wann welche Form
  emittieren
- [Fehlerkatalog (maschinenlesbar)](error-catalog-machine.md) —
  die Exceptions, die dein Konsument klassifizieren muss
- [API-Referenz](../api.md) — Python-Signaturen für die Typen
  hinter diesen Payloads
