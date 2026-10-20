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
| `x-swarmq-traceback` | `string` | Worker (bei Fehler) | Voller Python-Traceback des raisenden Versuchs; bei einer vom Worker abgewiesenen Nachricht (`TASK_REJECTED`) `<reason>: <error>`. |
| `x-swarmq-unique-hash` | `string` (hex) | Client | SHA-256 der kanonischen `(name, args, kwargs)`. |
| `x-swarmq-unique-until` | `"start"` \| `"completion"` | Client | Wann der Worker den Dedup-Flag freigibt. |
| `x-swarmq-schedfire` | `int` (Unix-ms) | Scheduler | Vorhanden auf scheduler-publishten Nachrichten; Worker dedupt darauf. |
| `x-swarmq-schedfire-claim` | `string` (kommagetrennte Tokens) | Worker (Kopie eines Schedule-Fires) | Owner-Tokens des In-Flight-Claims des Fires, neuestes zuerst, höchstens 16: das Token der Zustellung, aus der diese Kopie stammt, gefolgt von den Tokens, die dieser Zustellung angeboten wurden (ein Claim, dessen Antwort verloren ging, kann jedes davon tragen). Die Kopie übernimmt einen Claim mit einem dieser Tokens, statt ihn als Duplikat zu werten. Gesetzt bei Kopien für Zurückstellung, Retry, Rate-Limit und Lock sowie bei der Kopie, die ein Requeue ersetzt, wenn die Freigabe des Claims scheiterte; ein Requeue an Ort und Stelle (`nack`) trägt ihn nie. Der Worker liest höchstens 16 Tokens. |
| `x-swarmq-backend-defer-count` | `int` | Worker (Backend-Zurückstellung) | Wie oft eine Prüfung vor der Ausführung diese Nachricht zurückgestellt hat, weil das Backend nicht erreichbar war; bestimmt die nächste Wartezeit. Kein Retry-Zähler. Kopien, die entstehen, nachdem das Backend geantwortet hat (Retry, Rate-Limit-Treffer, Lock-Konflikt), tragen ihn nicht, und die Zählung beginnt neu, wenn die Zustellung einen Broker-Zähler trägt (`x-delivery-count`/`x-acquired-count` größer 0, der Broker hat die Nachricht also zurückgegeben). |
| `x-swarmq-guard-defer-count` | `int` | Worker (Ausführungsschutz) | Wie oft der [Ausführungsschutz](../../explanation/delivery-semantics.md#ausfuhrungsschutz) diese Nachricht zurückgestellt hat, weil dieselbe Ausführung anderswo lief; bestimmt die nächste Wartezeit. Kein Retry-Zähler. |
| `x-swarmq-unknown-since` | `int` (Unix-Millisekunden) | Worker (unregistrierter Task) | Erster Kontakt mit einem Task-Namen, den der Worker nicht registriert; ab ihm wird die Gnadenfrist `SWARMQ_UNKNOWN_TASK_GRACE_S` gemessen. Bleibt auf jeder Rückstellungs-Kopie, die der Worker vor der Ausführung anlegt; entfernt bei einer Retry-Kopie, beim Dead-Lettering und durch `dlq retry`. |
| `x-swarmq-replay-id` | `string` (hex, höchstens 64 Zeichen) | Worker (Dead-Lettering), `swarmq dlq retry` | Teil der [Ausführungs-Identität](../../explanation/delivery-semantics.de.md#ausfuhrungsschutz), sodass eine bewusste Wiederholung läuft, auch solange der Done-Marker des ersten Laufs steht. Der Worker stempelt die ID der nächsten Generation auf die DLQ-Kopie (`"g"` plus 39 Hex-Zeichen, aus der aktuellen Identität abgeleitet, gleich für zwei Kopien derselben Ausführung); `dlq retry` behält sie, wenn `x-swarmq-dlq-gen` gleich `x-swarmq-dead-lettered-at` ist, und setzt sonst eine neue `uuid4` (0.11-Worker, Altbestand). Ein längerer Wert oder kein String wird ignoriert. |
| `x-swarmq-xdeath-base` | `int` | Worker (jede Kopie), `swarmq dlq retry` | Zahl der Broker-Dead-Letterings außerhalb der Delay-Queues, die die Kopie geerbt hat: die `count`-Werte von `x-death`, deren Queue weder `{queue}.DQ` noch eine Delay-Stufe ist, plus die bisherige Base. Gesetzt, weil der Broker `x-death` beim nächsten Dead-Lettern neu aufbaut; fehlt der Header, gilt 0. Die Ausführungs-Identität ist `task_id|<Replay-ID>#<Base plus aktuelles x-death>`, ohne den Zusatz bei 0. |
| `x-swarmq-dlq-gen` | `int` (Unix-Millisekunden) | Worker (Dead-Lettering) | Der Wert von `x-swarmq-dead-lettered-at` des Stempels, der `x-swarmq-replay-id` gesetzt hat. `dlq retry` behält die Replay-ID nur, solange beide gleich sind, und entfernt diesen Header. |
| `x-swarmq-dead-lettered-at` | `int` (Unix-Millisekunden) | Worker (Dead-Lettering), `swarmq dlq` (Altbestand beim ersten Lesen) | Zeitpunkt des Dead-Letterings; daraus berechnen die `dlq`-Befehle die Restfrist. Millisekunden, weil AMQP-Tabellen Gleitkommazahlen nur einfach genau kodieren. `dlq retry` entfernt ihn. Vom Worker dead-gelettete Nachrichten tragen außerdem die Property `expiration` (die Frist, `SWARMQ_DEAD_LETTER_TTL_S`), außer die Frist ist `none`. |
| `x-swarmq-dead-letter-ttl-ms` | `int` (Millisekunden) oder `"none"` | Worker (Dead-Lettering), `swarmq dlq` (beim ersten Lesen, wenn er fehlt) | Die Frist, mit der die Nachricht dead-gelettert wurde; `"none"` heißt unbegrenzt. Die `dlq`-Befehle berechnen die Restfrist daraus, nicht aus ihrer eigenen `SWARMQ_DEAD_LETTER_TTL_S`. `dlq retry` entfernt ihn. |
| `x-swarmq-lock-render-error` | `string` | Worker (Lock-Template-Fehler) | Warum das Lock-Template nicht gerendert werden konnte; die Nachricht geht in die DLQ. |
| `x-swarmq-lock-conflict-count` | `int` | 0.11-Worker (Lock-Konflikt) | Legacy-Zähler: Ein 0.11-Worker erhöht ihn und dead-lettert beim 10. Konflikt. 0.12 erhöht und liest ihn nicht; Kopien tragen ihn unverändert weiter. |
| `x-swarmq-lock-conflicts` | `int` | Worker (Lock-Konflikt) | Bisherige Lock-Konflikte dieser Nachricht; der `lock_conflict_count` von `TASK_LOCKED`. |
| `x-swarmq-lock-wait-since` | `int` (Unix-Millisekunden) | Worker (Lock-Konflikt) | Erster Lock-Konflikt; ab ihm wird `SWARMQ_LOCK_WAIT_MAX_S` gemessen. |
| `x-swarmq-lock-holder` | `string` | Worker (Lock-Konflikt) | Fingerprint der Halter des Locks (Menge der Owner) beim letzten Halterwechsel. |
| `x-swarmq-lock-holder-since` | `int` (Unix-Millisekunden) | Worker (Lock-Konflikt) | Wann der Worker zuletzt einen Halterwechsel gesehen hat. |
| `x-swarmq-lock-holder-ttl-ms` | `int` (Millisekunden) | Worker (Lock-Konflikt) | Rest-TTL des Lock-Keys bei diesem Wechsel; ein Halter, der den Lock länger hält als diese plus 60 s, gilt als hängend. |
| `x-swarmq-lock-wait-gen` | `str` (`"<Retry-Zähler>:<Replay-ID>"`) | Worker (Lock-Konflikt) | Ausführung, zu der die Wartezeit gehört. Eine Nachricht mit anderem Retry-Zähler oder anderer Replay-ID (eine Kopie, die ein 0.11-Worker von einer späteren Ausführung angelegt hat) beginnt eine neue Wartezeit. Die sechs Lock-Header bleiben auf jeder Rückstellungs-Kopie, die ein Worker vor der Ausführung anlegt, und werden bei einer Retry-Kopie, beim Dead-Lettering und durch `dlq retry` entfernt; die Kopie einer vom Broker neu zugestellten Nachricht verliert sie ebenfalls. |
| `x-swarmq-lock-name` | `string` | Worker (Lock-Konflikt) | Gerenderter Lock-Name, auf den sich die Konflikte beziehen. |
| `x-swarmq-rate-limit-count` | `int` | Worker (Rate-Limit) | Wie oft die Nachricht am Rate-Limit abgeprallt ist. |
| `x-swarmq-workflow-id` | `string` | Client / Worker | Workflow, zu dem die Nachricht gehört (nur Workflow-Schritte). |
| `x-swarmq-workflow-step` | `string` | Client / Worker | Schrittpfad im Workflow (nur Workflow-Schritte). |
| `x-delivery-count`, `x-acquired-count` | `int` | RabbitMQ (Quorum-Queues) | Zustellzähler des Brokers; der Worker prüft beide gegen das [effektive Delivery-Limit](../../explanation/delivery-semantics.md#effektives-delivery-limit) (das kleinere von `SWARMQ_DELIVERY_LIMIT` und dem `x-delivery-limit` der Queue; RabbitMQ 4.3 zählt eine Rückgabe nur in `x-acquired-count`). Jede Kopie, die SwarmQ veröffentlicht (Retry, Zurückstellung, Rate-Limit, Lock, DLQ-Lesen durch `swarmq dlq`), lässt sie weg; die Kopie ist eine neue Nachricht mit eigenem Zustellbudget. |
| `x-death` | `array` | RabbitMQ | Broker-native DLX-Audit-Trail. Jede Kopie, die SwarmQ publiziert, entfernt ihn, nachdem sie seinen Zähler nach `x-swarmq-xdeath-base` übernommen hat. |

## Task-Ergebnis (Valkey)

Die Keys unten stehen ohne das optionale Namensraum-Präfix: Mit
gesetztem `SWARMQ_NAMESPACE` beginnen alle Keys und Kanäle mit
`<namensraum>:sq:` statt `sq:`.

Gespeichert unter `sq:result:{task_id}`, mit msgpack kodiert, TTL
gleich dem aufgelösten `result_ttl`.

```jsonc
{
  "task": {"id": "01HV...", "name": "send_email", "queue": "emails"},
  "is_error": false,
  "is_cancelled": false,
  "error_message": "",
  "result": "sent to alice@example.com"
}
```

Ein Fehlerergebnis trägt zusätzlich `error_class`:

```jsonc
{
  "task": {"id": "01HV...", "name": "send_email", "queue": "emails"},
  "is_error": true,
  "is_cancelled": false,
  "error_message": "",
  "result": null,
  "error_class": "builtins.ValueError"
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
| `error_message` | `string` | Immer vorhanden. Bei `is_error=true` `str(exc)` vom scheiternden Versuch, sonst `""`. Eine Exception ohne Text ergibt ebenfalls `""`; `error_class` nennt dann trotzdem die Klasse. |
| `error_class` | `string` (optional) | Nur auf Fehlerergebnissen und nur, wenn eine Klasse bekannt ist: die voll qualifizierte Klasse der Exception als `modul.qualname` (eingebaute als `builtins.ValueError`). Im CPU-Modus die Klasse, die das Rechenkind meldet, auch wenn der Elternprozess sie nicht laden konnte. Pfade, die im Worker ohne Task-Exception enden, nennen die öffentliche swarmq-Klasse, wo es eine gibt (`swarmq.exceptions.TaskNotFoundError`, `LockWaitExceeded`, `RedeliveryLimitError`, `UnroutableMessage`, `ResultSerializationError`). Sonst **fehlt** der Schlüssel (nie `null`): Ergebnisse älterer Worker, Ablauf durch `max_age`, `WorkerHungError` und Fehler, die der Worker selbst erzeugt (harter Timeout, Drain, verlorenes Rechenkind, Kind ohne Fehlerdetail). Beratend: zum Unterscheiden von Fehlern, nicht als stabile API. |

### Dekodierungs-Priorität

Beim Konsum eines Ergebnisses in dieser Reihenfolge auswerten:

1. `is_cancelled` → `TaskCancelledError` raisen.
2. `is_error` → `TaskError(error_message)` raisen; `TaskError.error_class`
   trägt `error_class` (`None`, wenn der Schlüssel fehlt).
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
| `started_at` | ISO-8601-Datetime, UTC \| leer | Wanduhr-Start des Laufs (vor 0.12.3 stammte er aus der monotonen Uhr und erschien als Datum von 1970). Leer nur, wenn der Worker keine Startzeit festgehalten hat. |
| `eta_seconds` | `int` \| leer | Leer bei `current=0` und beim finalen Flush. |

Ein HASH, dem `current`, `total` oder `updated_at` fehlt, wird
als malformed behandelt und als `None` zurückgegeben.

## Scheduler-Eintrag

Gespeichert im Valkey-ZSET `sq:{sched}:zset` (Member = Schedule-Name,
Score = Fire-Timestamp in ms) mit der Definition im HASH
`sq:{sched}:defs` (Feld = Schedule-Name).

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
| `fire_ts_ms` | `int \| null` | Nächste Fire-Zeit. |
| `source` | `string` | `"static"` (im Code definierter Cron), `"cli"` oder `"cli_unique"` (CLI-Schedules und One-Shots). |
| `disabled` | `bool` | `true` nach `schedule remove` (ohne `--permanent`). |
| `last_fire_at` | `int \| null` | Unix-ms des letzten Fires. |
| `dedup_hash` | `string \| null` | SHA-256 hex, wenn Dedup aktiv. |
| `unique_until` | `"start" \| "completion"` | Dedup-Freigabemodus. |
| `timezone` | `string \| null` | IANA-Zone von `cron`; `null` heißt UTC. |
| `task_id` | `string \| null` | Seit 0.12: die ID, die `schedule(delay=\|eta=)` geliefert hat; ein One-Shot feuert unter ihr. `null` bei Crons. |
| `unroutable_since` | `int \| null` | Seit 0.12: Unix-ms des ersten Fires eines One-Shots, dessen Queue nicht existierte. |
| `unique_for` | `int \| null` | Seit 0.12: `TaskInfo.unique_for` eines Crons, vom Worker gemeldet. |

Später hinzugekommene Felder stehen am Ende mit Default `null`, damit
Einträge älterer Versionen weiter dekodieren.

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
      "queue": "emails",
      "headers": {"x-swarmq-retry-count": 3, "x-swarmq-traceback": "..."},
      "body_size": 312
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
  "queue": "emails",
  "headers": { ... },
  "body": {"task": {...}, "args": [], "kwargs": {...}},
  "body_raw": "{\"task\": ...}",
  "body_size": 312
}
```

`queue` ist die Haupt-Queue; gelesen wird aus `{queue}.XQ`.
`dlq list` liefert keinen Body (dafür `dlq inspect`). `body` ist der
dekodierte Task-Body, `null`, wenn er kein JSON-Objekt ist;
`body_raw` ist der Body als UTF-8-Text (ungültige Bytes ersetzt).

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

Gespeichert unter `sq:cancel:{task_id}` in Valkey mit einer TTL von
`max(24 h, result_ttl)`, dazu die Wartezeit bis zum Feuern eines
verzögerten Tasks und dessen `max_age` + 300 s, wenn die Registry des
Clients den Task kennt. Hat der Worker die stornierte Zustellung
behandelt, verkürzt er die TTL auf höchstens 1 h (`PEXPIRE … LT`),
statt den Key zu löschen. Der Wert ist ein fester Marker-String —
Anwesenheit ist das Signal.

| Key-Form | Typ | Semantik |
|---|---|---|
| `sq:cancel:{task_id}` | `string` ("1") | Vorhanden → Cancel angefordert; abwesend → kein Cancel. |

Der Cancel-Monitor des Workers macht periodisch
`EXISTS`-Checks auf diesen Key (`cancel_poll_interval`, Default
1.0 s) und an jeder Valkey-Operationsgrenze. Wird es gesetzt, während
der Task läuft, bricht der Worker die laufende Coroutine ab; der Task
sieht `asyncio.CancelledError`, und das gespeicherte Ergebnis hat
`is_cancelled=true`, das `get_result()` als `TaskCancelledError` wirft.
`TaskCancelledError` kommt nie im Task an.

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
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails"}
```

### `TASK_COMPLETED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "duration_ms": 312}
```

### `TASK_FAILED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "exception": "smtp connection refused"}
```

`exception` ist `str(exc)`, nicht das Exception-Objekt. `TASK_FAILED`
feuert nach jedem gescheiterten Versuch, auch wenn ein Retry folgt;
`TASK_DLQ` markiert das endgültige Scheitern.

### `TASK_RETRYING`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "retry_count": 1, "delay": 2.0}
```

### `TASK_CANCELLED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails"}
```

### `TASK_EXPIRED`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "age_s": 120, "max_age_s": 60}
```

### `TASK_LOCKED`

```jsonc
{"task_id": "01HV...", "task_name": "sync_user", "lock_name": "user-42", "lock_conflict_count": 1}
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

### `TASK_DLQ`

```jsonc
{"task_id": "01HV...", "task_name": "send_email", "queue": "emails", "error": "smtp connection refused"}
```

Feuert einmal, wenn der Worker eine Nachricht terminal dead-lettert.
Für `task_id` existiert ein Fehlerergebnis.

### `TASK_REJECTED`

```jsonc
{
  "queue": "emails",
  "reason": "unregistered_task",
  "task_id": "01HV...",
  "task_name": "send_emial",
  "error": "task 'send_emial' is not registered"
}
```

- `reason` ist einer von `"body_too_large"`, `"undecodable"`,
  `"unregistered_task"`, `"middleware_chain_factory"`,
  `"dlq_publish_failed"`.
- `task_id` und `task_name` sind `null`, wenn der Body nie dekodiert
  wurde (`body_too_large`, `undecodable`).
- Es wird kein Fehlerergebnis gespeichert. Bei `body_too_large`,
  `undecodable` und `middleware_chain_factory` legt der Worker eine
  Kopie mit Frist in die `.XQ`, mit `x-swarmq-traceback` =
  `<reason>: <error>`; nur wenn diese Kopie scheitert, und bei
  `dlq_publish_failed`, trägt die Nachricht stattdessen nur den
  `x-death`-Header des Brokers.

### `WORKFLOW_STEP_ENQUEUED`

```jsonc
{
  "task_id": "01HV...",
  "task_name": "render_pdf",
  "queue": "pdf",
  "workflow_id": "01HW...",
  "step_path": "/1/g0"
}
```

Folgt dem `TASK_ENQUEUED` desselben Schritts. `step_path` ist die
Position des Schritts in der Workflow-Topologie: durch `/` getrennte
Segmente, wobei `N` der N-te Schritt einer Chain ist, `gN` das N-te
Mitglied einer Group und `h` der Header eines Chords (`""` für einen
Workflow aus einer einzelnen Signature, `/0` für den ersten
Chain-Schritt, `/1/g0` für das erste Mitglied einer Group als zweitem
Chain-Schritt).

### `WORKER_*`

```jsonc
{"worker_id": "web-3:0"}
```

Emittiert für `WORKER_ONLINE`, `WORKER_SHUTDOWN`,
`WORKER_HEARTBEAT` (letzteres zusätzlich mit `uptime_seconds`).
`worker_id` ist `<basis>:<slot>` unter dem Supervisor und
`<basis>:p<pid>` im eingebetteten Betrieb; `<basis>` ist
`SWARMQ_WORKER_ID` oder der Hostname. `WORKER_RECYCLING` ergänzt
`kind`, `slot`, `reason`, `value`.

### `SCHEDULER_*`

```jsonc
{"worker_id": "web-3:0"}
```

Emittiert für `SCHEDULER_LEADER`, `SCHEDULER_STANDBY`,
`SCHEDULER_HEARTBEAT`, `SCHEDULER_LEADER_HANDOVER`,
`SCHEDULER_LEADER_FAILOVER`.

## Payload von `swarmq worker --check`

`swarmq worker --check ...` druckt ein JSON-Objekt auf stdout und
endet mit `0` (`ok: true`) oder `1` (`ok: false`). Nichts wird
gestartet, keine Verbindung geöffnet. `ok: true` sagt den Start voraus:
Die Prüfung baut die Worker-Konfiguration aus den `SWARMQ_*`-Variablen
wie der Start, jeder Wert, den der Start mit Exit `78` ablehnt, ergibt
also `ok: false`. Fehlende `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL`
prüft sie nicht.

```jsonc
{
  "ok": true,
  "module": "myapp.tasks",
  "module_origin": "/srv/app/myapp/tasks.py",
  "reload": false,
  "watch_dirs": null,
  "warnings": [],
  "error": null,
  "error_type": null,
  "pool": "io",
  "slots": 10,
  "configure": "myapp.hooks:setup",
  "metrics_ports": [9000, 9002],
  "health_ports": [9100, 9102]
}
```

| Feld | Typ | Beschreibung |
|---|---|---|
| `ok` | `bool` | `true`, wenn jede Prüfung bestanden hat. |
| `module` | `str` | Das `--module`-Argument. |
| `module_origin` | `str \| null` | Aufgelöste Datei des Task-Moduls (`find_spec`). |
| `reload` | `bool` | Ob `--reload` angefordert wurde. |
| `watch_dirs` | `list[str] \| null` | Aufgelöste Watch-Verzeichnisse; `null` ohne `--reload`. |
| `warnings` | `list[str]` | Nicht-fatale Befunde, je ein String mit dem Code vorn, z. B. `"reload_burst_conflict: --reload and --burst are not designed to be combined; …"`, wenn `--reload` und `--burst` zusammen angegeben sind. |
| `error`, `error_type` | `str \| null` | Gesetzt, wenn `ok` `false` ist; `error_type` ist der Klassenname der Ausnahme (`ConfigurationError`, `ModuleNotFoundError`, `ArgparseError`, …). |
| `pool` | `"io" \| "cpu"` | Wirksamer Pool. |
| `slots` | `int` | Wirksame Concurrency je Prozess (Rechenslots im CPU-Modus). |
| `configure` | `str \| null` | Die `--configure`-Angabe nach Syntax- und `find_spec`-Prüfung; das Callable selbst wird erst im Worker aufgelöst. |
| `metrics_ports`, `health_ports` | `list[int] \| null` | Der Portbereich, den die Slots `0..N-1` binden, als `[erster, letzter]` (beide inklusive), nicht die Liste aller Ports: Mit Basisport `9000` und `N = 3` deckt `[9000, 9002]` die Ports `9000`, `9001` und `9002` ab. `N` ist das Größte aus `--processes`, der Obergrenze von `--autoscale` und `--max-processes`. Bei `N = 2` sieht der Bereich wie eine genaue Liste aus (`[9000, 9001]`), bei `N = 1` sind beide Einträge gleich (`[9000, 9000]`). `null`, wenn der Basisport ungesetzt ist. |

Jedes Payload trägt den vollen Feldsatz oben, auch bei einer
gescheiterten Prüfung; Felder, die an der Stelle noch unbekannt sind,
sind `null`. Bei einem argparse-Fehler (Exit `1`, nicht das bei
argparse übliche `2`) hat das Payload `ok: false`, `error_type:
"ArgparseError"`, die Meldung in `error`, `warnings: []`, und alle
übrigen Felder sind `null`.

## Siehe auch

- [Entscheidungsbäume](decision-trees.md) — wann welche Form
  emittieren
- [Fehlerkatalog (maschinenlesbar)](error-catalog-machine.md) —
  die Exceptions, die dein Konsument klassifizieren muss
- [API-Referenz](../api.md) — Python-Signaturen für die Typen
  hinter diesen Payloads
