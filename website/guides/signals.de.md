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
from swarmq.logging import get_logger

app = SwarmQ(broker, backend)
log = get_logger("myapp.signals")


@app.on_signal(Signal.TASK_FAILED)
async def on_task_failed(task_id, task_name, exception, **kwargs):
    log.error(
        "myapp_task_attempt_failed",
        task_id=task_id, task_name=task_name,
        error=exception,
    )
```

Der Handler muss async sein. Er läuft inline, nachdem das Event
dispatched wurde — keine separate Event-Loop, kein Worker-Thread.

Schließe die Parameterliste immer mit `**kwargs` ab. Der Worker
übergibt jeden Schlüssel aus der Tabelle unten — `TASK_FAILED` trägt
auch `queue` —, und ein Handler mit fester Signatur scheitert bei jedem
Dispatch mit `TypeError` (gefangen und als `signal_handler_error`
geloggt; der Handler tut seine Arbeit also stillschweigend nie). Spätere
Releases können neue Schlüssel ergänzen.

`swarmq.logging.get_logger(name)` liefert einen Logger, der Events
unter `name` an das Standard-`logging` übergibt, mit den
Keyword-Argumenten als Record-Felder — derselbe Mechanismus, den SwarmQ
für seine eigenen Events nutzt (siehe
[Logging](../reference/logging.md)). Gib deinen eigenen Events Namen,
die nicht mit den eingebauten von SwarmQ kollidieren (`task_failed`,
`task_succeeded`, …), sonst kannst du sie im Log nicht auseinanderhalten.

!!! note "`exception` ist ein String"
    `TASK_FAILED` trägt `exception=str(exc)`, die Nachricht der
    Exception, nicht das Exception-Objekt. Du kannst es weder mit
    `isinstance()` prüfen noch als Exception an einen Error-Tracker
    geben. Brauchst du das Objekt (Klasse, Traceback), nutze den
    `on_error`-Hook einer [Middleware](middleware.md), der die echte
    Exception bekommt — siehe
    [Sentry- / Error-Aggregator-Integration](#sentry-error-aggregator-integration).

## Verfügbare Signals

| Signal | Feuert | Kwargs |
|---|---|---|
| `TASK_ENQUEUED` | Nach erfolgreichem `schedule()` | `task_id`, `task_name`, `queue` |
| `TASK_STARTED` | Vor Aufruf von `run()` | `task_id`, `task_name`, `queue` |
| `TASK_COMPLETED` | Nach Erfolg von `run()` | `task_id`, `task_name`, `queue`, `duration_ms` |
| `TASK_FAILED` | Nach **jedem** fehlgeschlagenen Versuch (`run()` hat geraist oder ist in einen Timeout gelaufen), vor der Entscheidung Retry oder DLQ | `task_id`, `task_name`, `queue`, `exception` (String) |
| `TASK_RETRYING` | Vor Publish eines Retry | `task_id`, `task_name`, `queue`, `retry_count` (Retry-Nummer des kommenden Versuchs), `delay` (Sekunden) |
| `TASK_CANCELLED` | Wenn Cancel-Flag mitten im Run beobachtet | `task_id`, `task_name`, `queue` |
| `TASK_EXPIRED` | Wenn `max_age` bei Pickup überschritten ist; die Nachricht geht in die DLQ | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` |
| `TASK_LOCKED` | Wenn der Lock des Tasks bei Pickup von einer anderen Ausführung gehalten wird (Lock-Konflikt); die Nachricht wird verzögert neu eingereiht | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` |
| `TASK_RATE_LIMITED` | Wenn Rate-Limiter eine Nachricht aufschiebt | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` |
| `TASK_DUPLICATE_DETECTED` | Wenn Unique-Task-SETNX das Race verliert | `existing_task_id`, `task_name`, `dedup_hash`, `source`[, `batch_index`] |
| `TASK_DLQ` | Einmal, wenn der Worker eine Nachricht terminal dead-lettert (Retries erschöpft, `NoRetry`, Delivery-Limit erreicht) | `task_id`, `task_name`, `queue`, `error` |
| `TASK_REJECTED` | Wenn der Worker eine Nachricht ohne Diagnose-Header an den Dead-Letter-Exchange des Brokers gibt: zu großer Body, nicht dekodierbarer Body, unregistrierter Task, kaputte Middleware-Chain-Factory, gescheitertes Dead-Letter-Publish | `queue`, `reason`, `task_id`, `task_name`, `error` |
| `WORKFLOW_STEP_ENQUEUED` | Nach jedem publizierten Workflow-Schritt (durch `app.apply()` oder den Advancer im Worker), zusätzlich zu `TASK_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | Worker-Lifecycle | `worker_id` |
| `WORKER_HEARTBEAT` | Periodischer Lebenszeichen-Tick des Metrik-Samplers im Worker | `worker_id`, `uptime_seconds` |
| `WORKER_RECYCLING` | Ein Rechenkind (`kind="compute_child"`) oder der Worker selbst (`kind="worker"`) wird nach RSS oder Taskzahl recycelt | `worker_id`, `kind`, `slot`, `reason` (`rss`/`tasks`/`forced`), `value` |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | Scheduler-Leader-Übergänge | `worker_id` |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | Leader-Swap-Quellen | `worker_id` |
| `SCHEDULER_FIRE_SKIPPED` | Catch-up nach Downtime hat Cron-Termine übersprungen | `worker_id`, `schedule_name`, `skipped_count` |

`TASK_FAILED` gilt pro Versuch: Ein Task mit `max_retries=3`, der immer
scheitert, feuert es viermal, im Wechsel mit drei `TASK_RETRYING`. Um
nur dann zu reagieren, wenn die Arbeit aufgegeben wird, abonniere
`TASK_DLQ`; es feuert einmal bei der terminalen Übergabe an die DLQ.

Der `batch_index`-Key im `TASK_DUPLICATE_DETECTED`-Signal ist nur
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
async def broken(task_id, task_name, duration_ms, **kwargs):
    raise RuntimeError("this is fine")
```

Die Library loggt ein WARNING auf dem Logger `swarmq.signals`, mit
angehängtem Traceback (`--log-format human`):

```text
2026-10-06T10:02:13.511204Z [warning  ] signal_handler_error           [swarmq.signals] error='this is fine' error_type=RuntimeError handler=broken signal=task_completed
```

und macht weiter. Das ist Absicht — ein buggy Metrics-Handler darf
die Task-Ausführung nicht vergiften können.

## Mehrere Handler pro Signal

Registriere so viele wie du brauchst. Handler laufen in
Registrierungs-Reihenfolge:

```python
@app.on_signal(Signal.TASK_DLQ)
async def to_slack(task_id, task_name, error, **kwargs):
    await slack.post(channel="#ops", text=f"{task_name} dead-lettered: {error}")


@app.on_signal(Signal.TASK_DLQ)
async def to_pagerduty(task_id, task_name, error, **kwargs):
    if task_name in ("charge_card", "send_invoice"):
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

Im Zweifel: Schaut der Code nur auf `task_id` und die
Fehlermeldung, ist es ein Signal-Handler. Braucht er das
Exception-Objekt oder `pre_execute` + `post_execute`-Symmetrie, ist es
Middleware.

## Typische Uses

### Sentry- / Error-Aggregator-Integration

Ein Error-Tracker will das Exception-Objekt mit Traceback. Das Signal
trägt nur die Nachricht, nutze deshalb den `on_error`-Hook einer
Middleware; er läuft bei jedem fehlgeschlagenen Versuch mit der echten
Exception:

```python
import sentry_sdk

from swarmq import Middleware, MessageContext


class SentryMiddleware(Middleware):
    async def on_error(
        self, message: MessageContext, exception: BaseException,
    ) -> None:
        with sentry_sdk.new_scope() as scope:
            scope.set_tag("task", message["task_name"])
            scope.set_tag("queue", message["queue"])
            sentry_sdk.capture_exception(exception)


app.add_middleware(SentryMiddleware())
```

### Slack-Notification bei DLQ-Drops

`TASK_DLQ` feuert einmal pro dead-letterter Nachricht, nachdem die
Retries aufgebraucht sind (oder bei `NoRetry`); `error` ist die
Fehlermeldung:

```python
@app.on_signal(Signal.TASK_DLQ)
async def alert(task_id, task_name, queue, error, **kwargs):
    if task_name in ("charge_card", "send_invoice"):
        await slack.post(
            channel="#ops",
            text=f":fire: {task_name} dead-lettered on {queue}: {task_id} ({error})",
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
async def audit(task_id, task_name, **kwargs):
    await audit_log.insert(
        event="task_cancelled",
        task_id=task_id, task_name=task_name,
    )
```

### Prometheus-Counter ohne Exporter

Wenn du `swarmq[metrics]` nicht nutzt, kannst du Prometheus-Metriken
direkt aus Handlern emittieren. Das zählt fehlgeschlagene Versuche,
nicht fehlgeschlagene Tasks:

```python
from prometheus_client import Counter

FAILED_ATTEMPTS = Counter("task_failed_attempts", "Failed task attempts", ["task"])

@app.on_signal(Signal.TASK_FAILED)
async def count(task_id, task_name, exception, **kwargs):
    FAILED_ATTEMPTS.labels(task=task_name).inc()
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

Registriere einen Handler für `TASK_COMPLETED`, der ein eigenes Event
loggt, plane einen Task, beobachte den Worker-Log:

```bash
swarmq worker --module myapp.tasks --log-level INFO --log-format human
```

Die Zeile deines Handlers erscheint direkt nach der
`task_succeeded`-Zeile des Workers für diesen Task, im selben
Log-Stream. Signals selbst sind keine Logzeilen: `TASK_COMPLETED` ist
der Hook, `task_succeeded` das Log-Event, das der Worker unmittelbar vor
dem Dispatch schreibt.

## Siehe auch

- [`Signal`-Enum](../reference/api.md#enum-signal) — volle Liste
  mit Payload-Schemata
- [Middleware](middleware.md) — wenn du Per-Task-Wrapper brauchst
- [Rate-Limiting](rate-limiting.md) — Beispiel-Konsument von
  `TASK_RATE_LIMITED`
