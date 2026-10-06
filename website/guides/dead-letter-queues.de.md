# Dead-Letter-Queues

Ein Task hat seine Retries erschöpft, `NoRetry` geraist oder ist
vor Pickup abgelaufen. Die Nachricht liegt nun in der Per-Queue-
Dead-Letter-Queue — `{queue}.XQ` — wo sie bleibt, bis ein Operator
eingreift. Dieser Guide behandelt, wie du DLQ-Nachrichten
inspizierst, retry'st und purgest.

## Voraussetzungen

- `SWARMQ_BROKER_URL` auf dasselbe RabbitMQ gerichtet, aus dem du
  konsumierst.
- `swarmq`-CLI installiert (`pip install swarmq`).

## Wann Nachrichten in der DLQ landen

| Trigger | Header auf der DLQ-Nachricht |
|---|---|
| Retry-Erschöpfung (`max_retries` erreicht — inkl. `max_retries=0`, wo bereits der ERSTE Fehlschlag terminal ist) | `x-swarmq-retry-count` am Limit, `x-swarmq-traceback` mit der Fehlermeldung |
| `raise NoRetry("reason")` aus Task-Body | `x-swarmq-traceback` mit der Fehlermeldung |
| `x-delivery-limit=20` von RabbitMQ ausgelöst | `x-death` vom Broker |
| `TaskInfo.max_age` bei Pickup überschritten | kein eigener — die Nachricht behält ihre ursprünglichen Header; das gespeicherte Fehlerergebnis lautet `task expired: age …s exceeds max_age …s` |
| Lock-Render-Fehler (Template-Kwarg fehlt, leerer Render) | `x-swarmq-lock-render-error` mit der Fehlermeldung |
| Lock-Konflikt-Budget erschöpft (10. Konflikt) | `x-swarmq-lock-conflict-count`, `x-swarmq-lock-name` |
| Body über der Größengrenze, nicht dekodierbarer Body, unregistrierter Task-Name, kaputte Middleware-Chain-Factory, gescheitertes Dead-Letter-Publish | keiner von SwarmQ — nur das `x-death` des Brokers |

Die DLQ ist eine reguläre RabbitMQ-Queue — du kannst sie mit
`rabbitmqadmin` oder der Management-UI inspizieren, aber die
`swarmq dlq`-CLI rendert sie SwarmQ-aware.

## Drei Wege in die `.XQ`

Nachrichten erreichen `{queue}.XQ` auf drei verschiedenen Wegen, und
die sind nicht gleich gut sichtbar. Ein Alert auf „Queue-Tiefe > 0"
fängt alle drei; zu wissen, welcher Weg gefeuert hat, sagt dir, was zu
reparieren ist.

| Weg | Wer bewegt die Nachricht | Signal | Metrik | Header |
|---|---|---|---|---|
| **Dead-Lettering durch den Worker** — Retries erschöpft, `NoRetry`, `max_age`, Lock-Fehler, Delivery-Limit kurz vor dem Auslösen | Worker publiziert nach `.XQ` und bestätigt das Original | `TASK_DLQ` (`task_id`, `task_name`, `queue`, `error`) bei erschöpften Retries, `NoRetry` und Delivery-Limit; `TASK_EXPIRED` bei `max_age`; keines bei Lock-Fehlern | `swarmq_tasks_total{outcome="dlq"}` | SwarmQ-Diagnose (`x-swarmq-retry-count`, `x-swarmq-traceback`, `x-swarmq-lock-*`, siehe Tabelle oben); ein Fehlerergebnis wird gespeichert, außer bei Lock-Fehlern |
| **Abweisung durch den Worker** — die Nachricht ließ sich nicht in einen Task verwandeln | Worker weist ohne Requeue ab; der Dead-Letter-Exchange des Brokers routet sie | `TASK_REJECTED` (`queue`, `reason`, `task_id`, `task_name`, `error`) | `swarmq_tasks_rejected_total{queue,reason}` | nur das `x-death` des Brokers; `task_id`/`task_name` sind `None`, wenn der Body nie dekodiert wurde |
| **Dead-Lettering durch den Broker** — `x-delivery-limit` nach wiederholten Worker-Abstürzen, Nachrichten-TTL, Längenlimit der Queue | RabbitMQ selbst | keines — kein Worker hat die letzte Zustellung gesehen | `swarmq_dlq_messages{queue}` (Tiefen-Gauge, vom Worker abgetastet) | nur `x-death` |

`reason` auf dem Abweisungsweg ist einer von `body_too_large`,
`undecodable`, `unregistered_task`, `middleware_chain_factory`,
`dlq_publish_failed`. `unregistered_task` nach einem Deploy heißt
meist, dass Worker und Produzent verschiedene Versionen des Task-Moduls
fahren; `body_too_large` und `undecodable` zeigen auf den Produzenten.

Der dritte Weg ist der, den man beobachten muss, wenn die anderen beiden
still sind: Ein Task, der den Prozess zuverlässig tötet (SIGSEGV, OOM),
erreicht nie `TASK_FAILED`. Seit 0.5.0 dead-lettert der Worker eine
solche Nachricht ab der vorletzten Zustellung selbst, sie landet also
auf dem ersten Weg mit `TASK_DLQ`; der Broker-Weg bleibt für TTL und
Längenlimits und für Abstürze, die schneller sind als die eigene
Prüfung des Workers.

## DLQ-Inhalte auflisten

```bash
swarmq dlq list --queue default
```

```text
TASK_ID                               TASK_NAME    RETRIES  ERROR_PREVIEW                  BODY_BYTES
01HV3W3YGBN4ZCZ45F2D7R9PB8            send_email   3        smtp connection refused         312
01HV3W4P5N6D2B5J0YRD9X7V6F            charge       -        stripe.CardDeclinedError        184
```

Nützliche Flags:

- `--limit N` — maximale Anzahl aufzulistender Nachrichten
  (Default 100). Die Ausgabe zeigt `(truncated)` oder JSON
  `truncated: true`, wenn getroffen.
- `--format json` — maschinenlesbare Ausgabe mit Headern, Body
  und einem `truncated`-Envelope für Agent-Skripte.
- `--queue QUEUE` — der Haupt-Queue-Name; die DLQ ist `{queue}.XQ`.

Die Operation ist non-destruktiv: Nachrichten bleiben auch nach
dem Auflisten in der DLQ.

## Eine Nachricht inspizieren

```bash
swarmq dlq inspect 01HV3W3YGBN4ZCZ45F2D7R9PB8 --queue default
```

Druckt die vollen Header und den dekodierten Body einer Nachricht:

```text
TASK_ID:   01HV3W3YGBN4ZCZ45F2D7R9PB8
QUEUE:     default.XQ
BODY_SIZE: 312 bytes
HEADERS:
  x-swarmq-retry-count: 3
  x-swarmq-traceback: |
    Traceback (most recent call last):
      File ".../smtp.py", line 42, in send
        ...
  x-swarmq-enqueued-at: 1729502821
BODY:
{
  "task": {"id": "01HV3W3YGBN4ZCZ45F2D7R9PB8", ...},
  "args": [],
  "kwargs": {"to": "bad@example.com", "subject": "Welcome"}
}
```

Nutze `--scan-limit N`, um die Suche zu verbreitern, wenn die
task_id tief in einer großen DLQ liegt. Exit-Code `3` bedeutet
task_id nicht gefunden; `0` bedeutet gefunden.

## Nachrichten retryen

### Eine Nachricht

```bash
swarmq dlq retry 01HV3W3YGBN4ZCZ45F2D7R9PB8 --queue default
```

Publisht die Nachricht zurück in die Haupt-Queue. Der
`x-swarmq-retry-count`-Header wird **zurückgesetzt** — der Task
bekommt sein volles Retry-Budget bei Redelivery. Andere Header
(Traceback, Dedup-Hash, Enqueued-at) bleiben erhalten, damit der
Operator-Kontext bleibt.

### Alle Nachrichten

```bash
swarmq dlq retry --all --queue default
```

Bewegt jede Nachricht. Respektiert `--limit N` (Default 10000) als
Sicherheitscap. Malformed Messages (nicht dekodierbarer Body,
fehlende Header) bleiben in der DLQ — sie werden separat gemeldet,
damit du entscheiden kannst, ob du sie purgen willst.

## Purgen

```bash
swarmq dlq purge --queue default
```

Natives AMQP `queue.purge()` auf `{queue}.XQ`. **Unumkehrbar.**
Nachrichten sind verloren. Nutze, wenn:

- Ein Code-Bug die DLQ mit Nachrichten geflutet hat, die du nicht
  retry'n willst.
- Ein Purge einem `dlq retry --all` folgt, das den erholbaren
  Anteil geleert und nur malformed Einträge hinterlassen hat.

`--format json` gibt `{"purged": N, "queue": "..."}` zurück, damit
Skripte Zählungen aufzeichnen können.

## Exit-Codes fürs Scripting

Die `swarmq dlq`-Subcommands teilen den CLI-Exit-Code-Vertrag:

| Code | Bedeutung |
|---|---|
| `0` | Erfolg |
| `1` | Laufzeitfehler (Broker unerreichbar, Bad Format usw.) |
| `2` | Schlechte Argumente |
| `3` | `dlq inspect` / `dlq retry <task_id>` fand keinen Match |

In einem Automation-Skript:

```bash
if ! swarmq dlq retry "$TASK_ID" --queue default; then
    case $? in
        3) echo "task nicht in DLQ" ;;
        1) echo "Laufzeitfehler — Broker prüfen" ;;
        *) echo "anderer Fehler" ;;
    esac
fi
```

## Operator-Workflow

Ein typischer Recovery-Durchgang sieht so aus:

1. **Triage.** `swarmq dlq list --queue default --limit 200
   --format json`, durch `jq` geleitet, um nach Error-Preview zu
   gruppieren — sieh, was dominiert.
2. **Root-Cause fixen.** Deploy, oder manuelle Korrektur der
   Inputs, falls der Bug in den Args lag.
3. **Massen-Retry.** `swarmq dlq retry --all --queue default`.
4. **Nachzügler behandeln.** Inspect jede Task, die beim
   Republish scheiterte (`--format json` zeigt
   `skipped_malformed`).
5. **Bei Bedarf purgen.** `swarmq dlq purge` für malformed oder
   wirklich unwiederherstellbare Nachrichten.

## Alerting

Richte einen Alert auf DLQ-Queue-Tiefe in deinem Metrics-Stack
ein. `swarmq[metrics]` tastet `swarmq_dlq_messages{queue}` für die
`.XQ` jeder konsumierten Queue ab — alarmiere bei `> 0 for 10m`.
Dieses Gauge fängt alle [drei Wege](#drei-wege-in-die-xq), auch das
broker-seitige Dead-Lettering, das kein Signal meldet.

Alarmiere getrennt auf die beiden Zähler, weil sie Verschiedenes
bedeuten:

- `increase(swarmq_tasks_total{outcome="dlq"}[10m]) > 0` — Tasks sind
  gelaufen und terminal gescheitert; Fehlerergebnis und
  Traceback-Header ansehen.
- `increase(swarmq_tasks_rejected_total[10m]) > 0` — Nachrichten sind
  nie zu Tasks geworden; nach `reason` gruppieren und Produzent oder
  Deployment prüfen.

Ohne Prometheus exposet das RabbitMQ-Management-Plugin Queue-Tiefe
unter `/api/queues/%2F/{queue}.XQ`.

## Verifizieren

Aus einem Task, der `NoRetry` raist:

```python
from swarmq import Task, TaskInfo, NoRetry

class AlwaysBad(Task):
    task_info = TaskInfo(name="always_bad", queues=["default"])
    async def run(self) -> None:
        raise NoRetry("absichtliche DLQ")
```

Plane ihn, dann:

```bash
swarmq dlq list --queue default
# → eine Nachricht mit ERROR_PREVIEW "absichtliche DLQ"

swarmq dlq retry --all --queue default
# → 1 Nachricht republished

swarmq dlq purge --queue default
# → 0 Nachrichten gepurged
```

## Siehe auch

- [Scheduling und Retries](scheduling-and-retries.md) — Tuning von
  `max_retries` und `retry_exceptions`, um unnötige DLQ-Hits zu
  vermeiden
- [Fehlerkatalog](../reference/errors.md) — `NoRetry` und die
  anderen DLQ-auslösenden Exceptions
- [`swarmq dlq`-CLI-Referenz](../reference/cli.md#swarmq-dlq) —
  Flag- und Options-Grammatik
