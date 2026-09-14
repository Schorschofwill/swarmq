# Signals

Du willst einen Handler auf ein spezifisches Task-Lifecycle-Event
laufen lassen — eine Metrik bei Task-Fehler aufzeichnen, ein
Audit-Log bei Cancel emittieren, Slack bei Scheduler-Leader-Wechsel
benachrichtigen. Signals sind die In-Process-Pub/Sub-Schicht für
diese Events.

## Voraussetzungen

- Eine `SwarmQ`-Instanz.
- Eine async-Funktion, die du bei einem Event laufen lassen willst.

## Einen Handler registrieren

Nutze den `@app.on_signal(...)`-Decorator:

```python
from swarmq import SwarmQ, Signal

app = SwarmQ(broker, backend)


@app.on_signal(Signal.TASK_FAILED)
async def on_task_failed(task_id, task_name, exception):
    log.error(
        "task_failed",
        task_id=task_id, task_name=task_name,
        error=repr(exception),
    )
```

Der Handler muss async sein. Er läuft inline, nachdem das Event
dispatched wurde — keine separate Event-Loop, kein Worker-Thread.

## Verfügbare Signals

| Signal | Feuert | Kwargs |
|---|---|---|
| `TASK_ENQUEUED` | Nach erfolgreichem `schedule()` | `task_id`, `task_name`, `queue` |
| `TASK_STARTED` | Vor Aufruf von `run()` | `task_id`, `task_name` |
| `TASK_COMPLETED` | Nach Erfolg von `run()` | `task_id`, `task_name`, `duration_ms` |
| `TASK_FAILED` | Nach `run()`-Raise beim letzten Versuch | `task_id`, `task_name`, `exception` |
| `TASK_RETRYING` | Vor Publish eines Retry | `task_id`, `retry_count`, `delay` |
| `TASK_CANCELLED` | Wenn Cancel-Flag mitten im Run beobachtet | `task_id`, `task_name` |
| `TASK_EXPIRED` | Wenn `max_age` bei Pickup überschritten | `task_id`, `task_name`, `age_seconds` |
| `TASK_LOCKED` | Wenn Lock-Acquisition in Arbeit ist | `task_id`, `task_name`, `lock_name` |
| `TASK_RATE_LIMITED` | Wenn Rate-Limiter eine Nachricht aufschiebt | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` |
| `TASK_DUPLICATE_DETECTED` | Wenn Unique-Task-SETNX das Race verliert | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] |
| `TASK_DLQ` | Einmal, wenn der Worker eine Nachricht terminal dead-lettert (Retries erschöpft, `NoRetry`, Delivery-Limit erreicht) | `task_id`, `task_name`, `queue`, `error` |
| `TASK_REJECTED` | Wenn der Worker eine Nachricht ohne Diagnose-Header an den Dead-Letter-Exchange des Brokers gibt: zu großer Body, nicht dekodierbarer Body, unregistrierter Task, kaputte Middleware-Chain-Factory, gescheitertes Dead-Letter-Publish | `queue`, `reason`, `task_id`, `task_name`, `error` |
| `WORKFLOW_STEP_ENQUEUED` | Nach jedem publizierten Workflow-Schritt (durch `Client.apply` oder den Advancer im Worker), zusätzlich zu `TASK_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` / `WORKER_HEARTBEAT` | Worker-Lifecycle | `worker_id` |
| `WORKER_RECYCLING` | Ein Rechenkind (`kind="compute_child"`) oder der Worker selbst (`kind="worker"`) wird nach RSS oder Taskzahl recycelt | `worker_id`, `kind`, `slot`, `reason` (`rss`/`tasks`/`forced`), `value` |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | Scheduler-Leader-Übergänge | `worker_id` |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | Leader-Swap-Quellen | `worker_id` |
| `SCHEDULER_FIRE_SKIPPED` | Catch-up nach Downtime hat Cron-Termine übersprungen | `worker_id`, `schedule_name`, `skipped_count` |

Handler sollten `**kwargs` defensiv akzeptieren. Der
`batch_index`-Key im `TASK_DUPLICATE_DETECTED`-Signal ist nur
vorhanden, wenn die Quelle `client.schedule_many` ist — nutze
`kwargs.get("batch_index")`, um `KeyError` zu vermeiden.

`TASK_REJECTED` trägt `reason` aus einem festen Satz: `body_too_large`,
`undecodable`, `unregistered_task`, `middleware_chain_factory`,
`dlq_publish_failed`. `task_id` und `task_name` sind `None`, wenn der
Body nie dekodiert wurde. Es ist von `TASK_DLQ` zu unterscheiden, das
ein Fehlerergebnis und eine Task-Identität verspricht; der
[Dead-Letter-Guide](dead-letter-queues.de.md#drei-wege-in-die-xq)
erklärt, welchen Weg eine Nachricht genommen hat.

Workflow-Schritte feuern `TASK_ENQUEUED` mit dem echten Task-Namen des
Schritts (den Platzhalter `<workflow>` gibt es nicht mehr) und danach
`WORKFLOW_STEP_ENQUEUED` mit der Workflow-Identität. Handler, die nur
`TASK_ENQUEUED` kennen, funktionieren weiter; abonniere das zweite
Signal, wenn du `workflow_id` oder `step_path` brauchst.

### Das Feld `worker_id`

Jedes `WORKER_*`-, `SCHEDULER_*`- und `WORKER_RECYCLING`-Payload trägt
die stabile Identität des Workers: `<basis>:<slot>` unter dem
Supervisor (`web-3:0`, `web-3:1`, …) und `<basis>:p<pid>` im
eingebetteten Betrieb (`web-3:p41872`). `<basis>` ist
`SWARMQ_WORKER_ID` oder der Hostname. Der Slot überlebt Neustarts und
Recycling und ist damit der richtige Schlüssel für Dashboards je
Worker; siehe
[Worker-Identität](../reference/configuration.de.md#worker-identitat).

## Fehlersemantik

Handler-Exceptions werden **pro Handler gefangen** und auf WARNING
geloggt. Die Dispatch-Schleife läuft zum nächsten Handler weiter;
das Signal bricht den Task oder den Worker nicht ab.

```python
@app.on_signal(Signal.TASK_COMPLETED)
async def broken(task_id, task_name, duration_ms):
    raise RuntimeError("this is fine")
```

Die Library emittiert:

```text
signal_handler_error signal=task_completed handler=broken error=...
```

und macht weiter. Das ist Absicht — ein buggy Metrics-Handler darf
die Task-Ausführung nicht vergiften können.

## Mehrere Handler pro Signal

Registriere so viele wie du brauchst. Handler laufen in
Registrierungs-Reihenfolge:

```python
@app.on_signal(Signal.TASK_FAILED)
async def to_sentry(task_id, task_name, exception):
    await sentry.capture(exception, tags={"task": task_name})


@app.on_signal(Signal.TASK_FAILED)
async def to_pagerduty(task_id, task_name, exception):
    if isinstance(exception, DatabaseDown):
        await pagerduty.trigger(task_id)
```

## Middleware vs Signals — welches?

Die beiden Mechanismen überlappen sich. Nutze diese Heuristik:

- **Middleware**, wenn das Verhalten jeden Task wrappen muss
  (Timing, Tracing, Auth-Context). Sie hat Zugriff auf
  `pre_*`/`post_*`-Paare mit Shared State.
- **Signal-Handler**, wenn du dich für eine spezifische Event-
  Klasse interessierst und nicht jeden Task-Lifecycle wrappen
  musst. Handler sind einfacher zu durchdenken als Full-Middleware.

Im Zweifel: Schaut der Code nur auf `task_id` und `exception`, ist
es ein Signal-Handler. Braucht er `pre_execute` + `post_execute`-
Symmetrie, ist es Middleware.

## Typische Uses

### Sentry- / Error-Aggregator-Integration

```python
@app.on_signal(Signal.TASK_FAILED)
async def capture(task_id, task_name, exception):
    await sentry.capture(exception, tags={"task": task_name})
```

### Slack-Notification bei DLQ-Drops

```python
@app.on_signal(Signal.TASK_FAILED)
async def alert(task_id, task_name, exception):
    if task_name in ("charge_card", "send_invoice"):
        await slack.post(
            channel="#ops",
            text=f":fire: {task_name} failed: {task_id}",
        )
```

### Alert auf abgewiesene Nachrichten

`TASK_REJECTED` heißt: Der Worker konnte den Task nicht einmal starten.
Der Body war zu groß, nicht dekodierbar oder nannte einen Task, den
dieser Worker nicht kennt. Nichts davon wird wiederholt, deshalb
verdient es einen eigenen Alert getrennt vom gewöhnlichen DLQ-Verkehr:

```python
@app.on_signal(Signal.TASK_REJECTED)
async def on_rejected(queue, reason, task_name=None, **kwargs):
    if reason == "unregistered_task":
        await slack.post(
            channel="#ops",
            text=f":warning: {queue}: unbekannter Task {task_name!r} — Deploy-Versatz?",
        )
```

Mit `swarmq[metrics]` zählt `swarmq_tasks_rejected_total{queue,reason}`
dieselben Ereignisse.

### Audit-Log für Cancellations

```python
@app.on_signal(Signal.TASK_CANCELLED)
async def audit(task_id, task_name):
    await audit_log.insert(
        event="task_cancelled",
        task_id=task_id, task_name=task_name,
    )
```

### Prometheus-Counter ohne Exporter

Wenn du `swarmq[metrics]` nicht nutzt, kannst du Prometheus-Metriken
direkt aus Handlern emittieren:

```python
FAILURES = Counter("task_failures", "Task failures", ["task"])

@app.on_signal(Signal.TASK_FAILED)
async def count(task_id, task_name, exception):
    FAILURES.labels(task=task_name).inc()
```

## Was du in Handlern nicht tun sollst

- **Nicht `app.schedule()` für jedes Task-Event aufrufen.** Es
  funktioniert, aber es ist eine Queueing-Feedback-Loop. Handler
  → schedule → Worker läuft → Signal → Handler → ...
- **Nicht die Loop blockieren.** Handler laufen inline. Ein
  Handler, der synchrones I/O macht (`requests.post`,
  `time.sleep`), stallt die Event-Loop des Workers und staut
  jeden nebenläufigen Task zurück.
- **Nicht geteilten State ohne Lock mutieren.** Handler-Concurrency
  folgt Task-Concurrency — zwei Tasks, die im selben Moment
  fertig werden, feuern zwei `TASK_COMPLETED`-Handler.

## Verifizieren

Registriere einen Handler, plane einen Task, beobachte den
Worker-Log:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-format human
```

Die `log.info("...")`-Zeile deines Handlers erscheint sofort nach
`TASK_COMPLETED` im selben Log-Stream.

## Siehe auch

- [`Signal`-Enum](../reference/api.md#enum-signal) — volle Liste
  mit Payload-Schemata
- [Middleware](middleware.md) — wenn du Per-Task-Wrapper brauchst
- [Rate-Limiting](rate-limiting.md) — Beispiel-Konsument von
  `TASK_RATE_LIMITED`
