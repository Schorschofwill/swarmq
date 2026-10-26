# Signals

Du willst einen Handler bei einem bestimmten Ereignis im Lebenszyklus
laufen lassen: eine Metrik aufzeichnen, wenn ein Task scheitert, ein
Audit-Log schreiben, wenn ein Task abgebrochen wird, Slack
benachrichtigen, wenn der Scheduler-Leader wechselt. Signals sind die
In-Process-Pub/Sub-Schicht für diese Ereignisse.

## Voraussetzungen

- Die `SwarmQ`-Instanz, die das Signal auslöst; unter `swarmq worker`
  bekommt sie ein `--configure`-Hook.
- Eine async-Funktion, die bei einem Ereignis laufen soll.

## Einen Handler registrieren

Nutze den Decorator `@app.on_signal(...)`:

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

Der Handler muss async sein. Er läuft inline, wenn das Ereignis
ausgelöst wird: keine eigene Event-Loop, kein Worker-Thread.

Schließe die Parameterliste immer mit `**kwargs` ab. Der Worker
übergibt jeden Schlüssel aus der Tabelle unten (`TASK_FAILED` trägt
auch `queue`), und ein Handler mit fester Signatur scheitert bei jedem
Aufruf mit `TypeError` (gefangen und als `signal_handler_error`
geloggt; der Handler tut seine Arbeit also stillschweigend nie). Spätere
Releases können neue Schlüssel ergänzen.

!!! warning "An der App registrieren, die das Signal auslöst"
    Ein Handler läuft an der `SwarmQ`-Instanz, die das Signal auslöst.
    Worker-Signale (`TASK_STARTED`, `TASK_COMPLETED`, `TASK_FAILED`,
    `TASK_DLQ`, `TASK_RATE_LIMITED`, `WORKER_*`, `SCHEDULER_*` und die
    übrigen, die der Worker sendet) feuern an der App des Workers.
    `swarmq worker` baut diese App selbst: Eine modulweite `app` in deinem
    Task-Modul ist eine andere Instanz, und Handler darauf laufen nie,
    ohne Warnung. Registriere sie in einer Funktion und übergib sie mit
    `--configure`:

    ```python title="myapp/hooks.py"
    from swarmq import Signal, SwarmQ
    from swarmq.logging import get_logger

    log = get_logger("myapp.signals")


    def setup(app: SwarmQ) -> None:
        @app.on_signal(Signal.TASK_FAILED)
        async def on_task_failed(task_id, task_name, exception, **kwargs):
            log.error("myapp_task_attempt_failed", task_id=task_id,
                      task_name=task_name, error=exception)
    ```

    ```bash
    swarmq worker --module myapp.tasks --configure myapp.hooks:setup
    ```

    Ein eingebetteter Worker (`await app.start_worker()`) nutzt die App,
    die du gebaut hast. `TASK_ENQUEUED`, `WORKFLOW_STEP_ENQUEUED` und
    `TASK_DUPLICATE_DETECTED` mit einer `client.*`-Quelle feuern in dem
    Prozess, der publiziert; diese registrierst du an der App des
    Producers. Workflow-Folgeschritte stellt der Advancer im Worker
    ein; `TASK_ENQUEUED` und `WORKFLOW_STEP_ENQUEUED` für sie feuern
    deshalb an der App des Workers. Handler, die jeden Schritt sehen
    müssen, gehören an beide Apps, im Worker per `--configure`. Die
    Beispiele unten schreiben `app` für die jeweils passende App. Siehe
    [`--configure`](../reference/cli.md#middleware-per-configure-registrieren).

`swarmq.logging.get_logger(name)` liefert einen Logger, der Events
unter `name` an das Standard-`logging` übergibt, mit den
Keyword-Argumenten als Record-Felder; derselbe Mechanismus, den SwarmQ
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
    Exception bekommt; siehe
    [Sentry- / Error-Aggregator-Integration](#sentry-error-aggregator-integration).

## Verfügbare Signals

| Signal | Feuert | Kwargs |
|---|---|---|
| `TASK_ENQUEUED` | Nach erfolgreichem `schedule()`; bei `delay`/`eta`, sobald der Task gespeichert ist, nicht beim Fire (Cron-Fires lösen es nicht aus) | `task_id`, `task_name`, `queue` |
| `TASK_STARTED` | Nach `pre_execute`, vor `pre_execution()` und `run()` | `task_id`, `task_name`, `queue` |
| `TASK_COMPLETED` | Nachdem `run()` und `post_execution()` erfolgreich waren | `task_id`, `task_name`, `queue`, `duration_ms` |
| `TASK_FAILED` | Nach **jedem** gescheiterten Versuch (`run()`, `pre_execution()` oder `post_execution()` hat geworfen, oder Soft- bzw. Hard-Timeout), vor der Entscheidung Retry oder DLQ | `task_id`, `task_name`, `queue`, `exception` (String) |
| `TASK_RETRYING` | Bevor ein Retry publiziert wird (nicht, wenn eine Nachricht zurückgestellt wird, weil das Backend vor der Ausführung nicht erreichbar ist; diese Zurückstellung feuert kein Signal) | `task_id`, `task_name`, `queue`, `retry_count` (Retry-Nummer des kommenden Versuchs), `delay` (Sekunden) |
| `TASK_CANCELLED` | Wenn der Worker das Cancel-Flag sieht, während der Task läuft oder wenn er eine vor dem Start abgebrochene Nachricht abholt | `task_id`, `task_name`, `queue` |
| `TASK_EXPIRED` | Wenn `max_age` beim Abholen überschritten ist; die Nachricht geht in die DLQ (ohne `TASK_DLQ`) | `task_id`, `task_name`, `queue`, `age_s`, `max_age_s` |
| `TASK_LOCKED` | Wenn der Lock des Tasks beim Abholen von einer anderen Ausführung gehalten wird (Lock-Konflikt); die Nachricht wird über die Delay-Queue zurückgestellt | `task_id`, `task_name`, `lock_name`, `lock_conflict_count` |
| `TASK_RATE_LIMITED` | Wenn der Rate-Limiter eine Nachricht zurückstellt | `task_id`, `task_name`, `retry_after_ms`, `remaining`, `rate_limit_count` |
| `TASK_DUPLICATE_DETECTED` | Wenn ein `unique_for`-Claim das Flag eines anderen Tasks vorfindet | `existing_task_id`, `task_name`, `dedup_hash`, `source` (`client.schedule`, `client.schedule_deferred`, `client.schedule_many` oder `scheduler.fire`)[, `batch_index`] |
| `TASK_DLQ` | Einmal, wenn der Worker eine Nachricht mit Fehlerergebnis dead-lettert: Retries aufgebraucht, `NoRetry` oder eine Exception außerhalb von `retry_exceptions`, Delivery-Limit, Waisen-Grenze des Ausführungsschutzes, Ende einer Lock-Wartezeit (`LockWaitExceeded`), ein Lock-Template, das sich nicht rendern lässt, oder ein unregistrierter Task nach `SWARMQ_UNKNOWN_TASK_GRACE_S` | `task_id`, `task_name`, `queue`, `error` |
| `TASK_REJECTED` | Wenn der Worker eine Nachricht abweist, die er nicht ausführen kann (zu großer oder nicht dekodierbarer Body, kaputte Middleware-Chain-Factory; eine Kopie mit Frist geht in die `.XQ`, der Dead-Letter-Exchange des Brokers ist der Rückfall), wenn ein Dead-Letter-Publish scheiterte und die Nachricht ohne Diagnose-Header an den Dead-Letter-Exchange des Brokers geht, und wenn er einen unregistrierten Task nach `SWARMQ_UNKNOWN_TASK_GRACE_S` dead-lettert (nach `TASK_DLQ`) | `queue`, `reason`, `task_id`, `task_name`, `error` |
| `TASK_UNKNOWN_DEFERRED` | Einmal je Rückstellung einer Nachricht, deren Task dieser Worker nicht registriert, innerhalb von `SWARMQ_UNKNOWN_TASK_GRACE_S` (Rolling Deploy) | `task_id`, `task_name`, `queue`, `delay` (Sekunden), `unknown_since` (Unix-ms) |
| `WORKFLOW_STEP_ENQUEUED` | Nach jedem publizierten Workflow-Schritt (durch `app.apply()` oder den Advancer im Worker), zusätzlich zu `TASK_ENQUEUED` | `task_id`, `task_name`, `queue`, `workflow_id`, `step_path` |
| `WORKER_ONLINE` / `WORKER_SHUTDOWN` | Start und Shutdown des Workers | `worker_id` |
| `WORKER_HEARTBEAT` | Periodischer Lebenszeichen-Tick des Metrik-Samplers im Worker | `worker_id`, `uptime_seconds` |
| `WORKER_RECYCLING` | Ein Rechenkind (`kind="compute_child"`) oder der Worker selbst (`kind="worker"`) wird nach RSS oder Taskzahl recycelt | `worker_id`, `kind`, `slot`, `reason` (`rss`/`tasks`/`forced`), `value` |
| `SCHEDULER_LEADER` / `SCHEDULER_STANDBY` / `SCHEDULER_HEARTBEAT` | Dieser Worker wurde Leader, ist (wieder) im Standby oder hat seinen Leader-Lock erneuert | `worker_id` |
| `SCHEDULER_LEADER_HANDOVER` / `SCHEDULER_LEADER_FAILOVER` | Dieser Leader gibt seinen Lock beim Shutdown ab, oder dieser Worker hat übernommen, nachdem der Lock des vorigen Leaders abgelaufen war | `worker_id` |
| `SCHEDULER_FIRE_SKIPPED` | Catch-up nach Ausfallzeit hat Cron-Termine übersprungen | `worker_id`, `schedule_name`, `skipped_count` |
| `SCHEDULER_CRON_CONFLICT` | Mehrere Definitionen eines statischen Crons leben länger als 10 Minuten (Leader, in jedem Übernahme-Takt, solange er besteht) | `worker_id`, `schedule_name`, `fingerprints` |
| `SCHEDULER_LEADER_OUTDATED` | Der Scheduler-Leader führt ein swarmq unter der Mindestversion für Leader aus (derzeit 0.12; `outdated=True`, einmal pro Leader), oder ein aktueller Leader folgt (`outdated=False`); siehe [Leader-Version](scheduling-and-retries.md#leader-version) | `worker_id`, `leader`, `leader_version` (`None` = vor 0.10.0), `outdated` |
| `SCHEDULER_CRON_UNOWNED` | Der Leader beginnt, einen statischen Cron zurückzuhalten, den kein lebender Worker meldet und den er selbst nicht registriert (`held=True`), oder gibt ihn wieder frei (`held=False`); einmal je Übergang | `worker_id`, `schedule_name`, `task_name`, `held`, `held_count` |
| `SCHEDULER_VALKEY_CHECKED` | Der Leader hat Eviction-Policy und Speicher von Valkey erneut geprüft, höchstens einmal je Stunde (seit 0.12) | `worker_id`, `status` (`safe`, `unsafe` oder `unknown`), `policy`, `maxmemory` (`0` = keine Grenze, `None` = unbekannt), `used_memory` |
| `SCHEDULER_QUEUE_ORPHANED` | Der Leader hat eine gemeldete Queue mit Nachrichten, ohne Consumer und ohne lebenden Worker, der sie meldet, gefunden (`orphaned=True`), oder sie ist nicht mehr verwaist (`orphaned=False`); einmal je Übergang (seit 0.12) | `worker_id`, `queue`, `orphaned`, `message_count`, `orphaned_count` |
| `SCHEDULER_FIRE_UNROUTABLE` | Der Broker hat einen Publish des Leaders zurückgegeben, weil die Queue fehlt: ein verlorener Cron-Fire (`kind="cron_lost"`) oder ein nach `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` verworfener verzögerter Task (`kind="oneshot_dropped"`) | `worker_id`, `schedule_name`, `queue`, `kind` |

`TASK_FAILED` gilt pro Versuch: Ein Task mit `max_retries=3`, der immer
scheitert, feuert es viermal, im Wechsel mit drei `TASK_RETRYING`. Um
nur dann zu reagieren, wenn die Arbeit aufgegeben wird, abonniere
`TASK_DLQ`; es feuert einmal bei der terminalen Übergabe an die DLQ.
Mit abgeschaltetem Dead-Lettering von Task-Fehlern
(`TaskInfo(dead_letter=False)` oder
`SWARMQ_DEAD_LETTER_TERMINAL_FAILURES=false`) gibt es für diese Tasks
keine Übergabe und kein `TASK_DLQ`; das letzte `TASK_FAILED` ist dann
das terminale.

Der Schlüssel `batch_index` von `TASK_DUPLICATE_DETECTED` ist nur
vorhanden, wenn die Quelle `client.schedule_many` ist; nutze
`kwargs.get("batch_index")`, um `KeyError` zu vermeiden.

`TASK_DLQ` feuert nicht bei einem Ablauf über `max_age` (das ist
`TASK_EXPIRED`) und nicht für Nachrichten, die der Worker abweist
(`TASK_REJECTED`).

`TASK_REJECTED` trägt `reason` aus einem festen Satz: `body_too_large`,
`undecodable`, `unregistered_task`, `middleware_chain_factory`,
`dlq_publish_failed`. `task_id` und `task_name` sind `None`, wenn der
Body nie dekodiert wurde. Es ist von `TASK_DLQ` zu unterscheiden, das
ein Fehlerergebnis und eine Task-Identität verspricht; der
[Dead-Letter-Guide](dead-letter-queues.md#drei-wege-in-die-xq)
erklärt, welchen Weg eine Nachricht genommen hat. `unregistered_task`
ist der eine Grund, der auf `TASK_DLQ` folgt: Seit 0.12 stellt der
Worker einen unbekannten Task für `SWARMQ_UNKNOWN_TASK_GRACE_S` zurück
(`TASK_UNKNOWN_DEFERRED` bei jeder Rückstellung) und dead-lettert ihn
danach selbst, mit Fehlerergebnis
([unregistrierte Tasks](dead-letter-queues.md#unregistrierte-tasks-und-rolling-deploys)).

Workflow-Schritte feuern `TASK_ENQUEUED` mit dem echten Task-Namen des
Schritts (den Platzhalter `<workflow>` gibt es nicht mehr) und danach
`WORKFLOW_STEP_ENQUEUED` mit der Workflow-Identität. Handler, die nur
`TASK_ENQUEUED` kennen, funktionieren weiter; abonniere das zweite
Signal, wenn du `workflow_id` oder `step_path` brauchst.

### Das Feld `worker_id`

Jedes `WORKER_*`- und `SCHEDULER_*`-Payload trägt die stabile Identität
des Workers: `<basis>:<slot>` unter dem Supervisor (`web-3:0`,
`web-3:1`, …) und `<basis>:p<pid>` im eingebetteten Betrieb
(`web-3:p41872`). `<basis>` ist `SWARMQ_WORKER_ID` oder der Hostname.
Der Slot überlebt Neustarts und Recycling und ist damit der richtige
Schlüssel für Dashboards je Worker; siehe
[Worker-Identität](../reference/configuration.md#worker-identitat).

## Fehlersemantik

Exceptions eines Handlers werden **je Handler gefangen** und auf
WARNING geloggt. Die Schleife läuft zum nächsten Handler weiter; ein
scheiternder Handler bricht weder den Task noch den Worker ab.

```python
@app.on_signal(Signal.TASK_COMPLETED)
async def broken(task_id, task_name, duration_ms, **kwargs):
    raise RuntimeError("this is fine")
```

Die Bibliothek loggt ein WARNING auf dem Logger `swarmq.signals`, mit
angehängtem Traceback (`--log-format human`):

```text
2026-10-06T10:02:13.511204Z [warning  ] signal_handler_error           [swarmq.signals] error='this is fine' error_type=RuntimeError handler=broken signal=task_completed
```

und macht weiter. Das ist Absicht: Ein fehlerhafter Metrik-Handler darf
die Task-Ausführung nicht stören.

Gefangen wird nur `Exception`; ein `asyncio.CancelledError` (Shutdown)
geht durch. Handler für `WORKER_ONLINE`, `WORKER_SHUTDOWN` und
`WORKER_HEARTBEAT` werden nach 10 s abgebrochen, solche für
`WORKER_RECYCLING` nach 5 s, jeweils mit dem WARNING
`signal_handler_timeout`; ein hängender Handler kann Start oder
Shutdown so nicht blockieren. Alle anderen Signals warten ohne Grenze
auf ihre Handler.

## Mehrere Handler pro Signal

Registriere so viele, wie du brauchst. Handler laufen in der
Reihenfolge ihrer Registrierung:

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

Die beiden Mechanismen überlappen sich. Eine Faustregel:

- **Middleware**, wenn das Verhalten jeden Task umschließen muss
  (Timing, Tracing, Auth-Kontext). Sie hat `pre_*`/`post_*`-Paare mit
  gemeinsamem Zustand.
- **Signal-Handler**, wenn dich ein bestimmtes Ereignis interessiert
  und du nicht jeden Task umschließen musst. Handler sind einfacher zu
  durchschauen als eine vollständige Middleware.

Im Zweifel: Schaut der Code nur auf `task_id` und die Fehlermeldung,
ist es ein Signal-Handler. Braucht er das Exception-Objekt oder die
Symmetrie von `pre_execute` und `post_execute`, ist es Middleware.

## Typische Einsätze

### Sentry- / Error-Aggregator-Integration

Ein Error-Tracker will das Exception-Objekt mit Traceback. Das Signal
trägt nur die Nachricht, nutze deshalb den `on_error`-Hook einer
Middleware; er läuft bei jedem gescheiterten Versuch mit der echten
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

### Slack-Benachrichtigung bei DLQ-Einträgen

`TASK_DLQ` feuert einmal pro dead-letterter Nachricht, etwa nachdem die
Retries aufgebraucht sind oder bei `NoRetry`; `error` ist die
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
Der Body war zu groß oder nicht dekodierbar, oder er nannte einen Task,
den dieser Worker auch nach der Karenzzeit nicht kannte. Nichts davon
wird wiederholt; deshalb verdient es einen eigenen Alert, getrennt vom
gewöhnlichen DLQ-Verkehr:

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

### Audit-Log für Abbrüche

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
direkt aus Handlern schreiben. Das zählt gescheiterte Versuche, nicht
gescheiterte Tasks:

```python
from prometheus_client import Counter

FAILED_ATTEMPTS = Counter("task_failed_attempts", "Failed task attempts", ["task"])

@app.on_signal(Signal.TASK_FAILED)
async def count(task_id, task_name, exception, **kwargs):
    FAILED_ATTEMPTS.labels(task=task_name).inc()
```

## Was du in Handlern nicht tun sollst

- **Nicht für jedes Task-Ereignis `app.schedule()` aufrufen.** Es
  funktioniert, ergibt aber eine Rückkopplungsschleife: Handler →
  schedule → Worker läuft → Signal → Handler → …
- **Nicht die Event-Loop blockieren.** Handler laufen inline. Ein
  Handler mit synchronem I/O (`requests.post`, `time.sleep`) hält die
  Event-Loop des Workers an und staut jeden nebenläufigen Task.
- **`SCHEDULER_HEARTBEAT`-Handler kurz halten.** Sie laufen ohne
  Zeitgrenze im Wahl-Takt des Leaders; ein Handler, der länger als
  etwa 10 bis 15 s braucht (Standardwerte), lässt einen gesunden Leader
  zurücktreten (`SCHEDULER_STANDBY`, danach
  `SCHEDULER_LEADER_FAILOVER`).
- **Geteilten Zustand nicht ohne Lock ändern.** Die Nebenläufigkeit
  der Handler folgt der der Tasks: Zwei Tasks, die im selben Moment
  fertig werden, lösen zwei `TASK_COMPLETED`-Handler aus.

## Verifizieren

Registriere in einem `--configure`-Hook einen Handler für
`TASK_COMPLETED`, der ein eigenes Event loggt, plane einen Task und
beobachte das Worker-Log:

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup \
  --log-level INFO --log-format human
```

Die Zeile deines Handlers erscheint direkt nach der
`task_succeeded`-Zeile des Workers für diesen Task, im selben
Log-Stream. Signals selbst sind keine Logzeilen: `TASK_COMPLETED` ist
der Hook, `task_succeeded` das Log-Event, das der Worker unmittelbar vor
dem Auslösen schreibt. Erscheint die Zeile nicht, prüfe, ob der
Handler im Hook registriert ist und nicht an einer modulweiten `app` in
`myapp/tasks.py`.

## Siehe auch

- [`Signal`-Enum](../reference/api.md#enum-signal): vollständige Liste
  mit Payload-Schemata
- [Middleware](middleware.md): wenn du Wrapper je Task brauchst
- [Rate-Limiting](rate-limiting.md): Beispiel-Konsument von
  `TASK_RATE_LIMITED`
