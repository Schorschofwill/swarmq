# Dead-Letter-Queues

Ein Task hat seine Retries erschöpft, `NoRetry` geraist oder ist
vor Pickup abgelaufen. Die Nachricht liegt nun in der Per-Queue-
Dead-Letter-Queue — `{queue}.XQ` — wo sie bleibt, bis ein Operator
eingreift oder ihre Aufbewahrungsfrist abläuft (standardmäßig 7 Tage,
siehe [Aufbewahrungsfrist](#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen)).
Dieser Guide behandelt, wie du DLQ-Nachrichten inspizierst, retry'st
und purgest, wie lange sie bleiben und wie du das Dead-Lettering von
Task-Fehlern abschaltest.

## Voraussetzungen

- `SWARMQ_BROKER_URL` auf dasselbe RabbitMQ gerichtet, aus dem du
  konsumierst.
- `swarmq`-CLI installiert (`pip install swarmq`).

## Wann Nachrichten in der DLQ landen

| Trigger | Header auf der DLQ-Nachricht | Mit abgeschaltetem Dead-Lettering von Task-Fehlern |
|---|---|---|
| Retry-Erschöpfung (`max_retries` erreicht — inkl. `max_retries=0`, wo bereits der ERSTE Fehlschlag terminal ist) | `x-swarmq-retry-count` am Limit, `x-swarmq-traceback` mit der Fehlermeldung | nicht dead-gelettert: Fehlerergebnis gespeichert, bestätigt |
| `raise NoRetry("reason")` aus Task-Body | `x-swarmq-traceback` mit der Fehlermeldung | nicht dead-gelettert: Fehlerergebnis gespeichert, bestätigt |
| Exception außerhalb von `retry_exceptions` | `x-swarmq-traceback` mit der Fehlermeldung | nicht dead-gelettert: Fehlerergebnis gespeichert, bestätigt |
| `x-delivery-limit=20` von RabbitMQ ausgelöst, oder die eigene Zustellzähler-Prüfung des Workers eine Zustellung vorher | `x-death` vom Broker; vom Worker `x-swarmq-traceback` mit `WorkerHungError` | weiter dead-gelettert |
| Der Worker hat die Zustellung zurückgegeben (`nack(requeue=True)`), bis `x-acquired-count` das Zustelllimit erreichte (RabbitMQ 4.3) | `x-swarmq-traceback` mit `RedeliveryLimitError` | weiter dead-gelettert |
| Dieselbe Ausführung ist `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS`-mal ohne Freigabe abgelaufen ([Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)) | `x-swarmq-traceback` mit `WorkerHungError` | weiter dead-gelettert |
| `TaskInfo.max_age` bei Pickup überschritten | kein eigener — die Nachricht behält ihre ursprünglichen Header; das gespeicherte Fehlerergebnis lautet `task expired: age …s exceeds max_age …s` | weiter dead-gelettert |
| Lock-Render-Fehler (Template-Kwarg fehlt, leerer Render) | `x-swarmq-lock-render-error` mit der Fehlermeldung, `x-swarmq-traceback` | weiter dead-gelettert |
| Lock-Konflikt-Budget erschöpft (10. Konflikt) | `x-swarmq-lock-conflict-count`, `x-swarmq-lock-name`, `x-swarmq-traceback` mit `LockConflictLimitError` | weiter dead-gelettert |
| Task-Name im Worker nicht registriert, nach der Gnadenfrist `SWARMQ_UNKNOWN_TASK_GRACE_S` (Standard 15 min; `0` = beim ersten Kontakt), siehe [unregistrierte Tasks](#unregistrierte-tasks-und-rolling-deploys) | `x-swarmq-traceback` mit `task '…' is not registered`; das gespeicherte Fehlerergebnis sagt dasselbe | weiter dead-gelettert |
| Body über der Größengrenze, nicht dekodierbarer Body, kaputte Middleware-Chain-Factory, gescheitertes Dead-Letter-Publish | keiner von SwarmQ — nur das `x-death` des Brokers | weiter dead-gelettert |

Jede Nachricht, die der Worker selbst dead-lettert, trägt außerdem
`x-swarmq-dead-lettered-at` (Unix-Millisekunden) und eine
`expiration` pro Nachricht, siehe
[Aufbewahrungsfrist](#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen).
Die dritte Spalte erklärt
[Dead-Lettering abschalten](#dead-lettering-von-task-fehlern-abschalten).

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
| **Dead-Lettering durch den Worker** — Retries erschöpft, `NoRetry`, `max_age`, Lock-Fehler, Delivery-Limit kurz vor dem Auslösen, Waisen-Grenze des Ausführungsschutzes, unregistrierter Task nach seiner Gnadenfrist | Worker publiziert nach `.XQ` und bestätigt das Original | `TASK_DLQ` (`task_id`, `task_name`, `queue`, `error`) bei erschöpften Retries, `NoRetry`, Delivery-Limit, Waisen-Grenze und unregistriertem Task (dort gefolgt von `TASK_REJECTED` mit `unregistered_task`); `TASK_EXPIRED` bei `max_age`; keines bei Lock-Fehlern | `swarmq_tasks_total{outcome="dlq"}` | SwarmQ-Diagnose (`x-swarmq-retry-count`, `x-swarmq-traceback`, `x-swarmq-lock-*`, siehe Tabelle oben); ein Fehlerergebnis wird gespeichert, außer bei Lock-Fehlern |
| **Abweisung durch den Worker** — die Nachricht ließ sich nicht in einen Task verwandeln | Worker weist ohne Requeue ab; der Dead-Letter-Exchange des Brokers routet sie | `TASK_REJECTED` (`queue`, `reason`, `task_id`, `task_name`, `error`) | `swarmq_tasks_rejected_total{queue,reason}` | nur das `x-death` des Brokers; `task_id`/`task_name` sind `None`, wenn der Body nie dekodiert wurde |
| **Dead-Lettering durch den Broker** — `x-delivery-limit` nach wiederholten Worker-Abstürzen, Nachrichten-TTL, Längenlimit der Queue | RabbitMQ selbst | keines — kein Worker hat die letzte Zustellung gesehen | `swarmq_dlq_messages{queue}` (Tiefen-Gauge, vom Worker abgetastet) | nur `x-death` |

`reason` auf dem Abweisungsweg ist einer von `body_too_large`,
`undecodable`, `middleware_chain_factory`, `dlq_publish_failed`;
`unregistered_task` ist die Ausnahme, die vom ersten Weg kommt (seit
0.12, siehe unten). `unregistered_task` nach einem Deploy heißt
meist, dass Worker und Produzent verschiedene Versionen des Task-Moduls
fahren; `body_too_large` und `undecodable` zeigen auf den Produzenten.

Der dritte Weg ist der, den man beobachten muss, wenn die anderen beiden
still sind: Ein Task, der den Prozess zuverlässig tötet (SIGSEGV, OOM),
erreicht nie `TASK_FAILED`. Seit 0.5.0 dead-lettert der Worker eine
solche Nachricht ab der vorletzten Zustellung selbst, sie landet also
auf dem ersten Weg mit `TASK_DLQ`; der Broker-Weg bleibt für TTL und
Längenlimits und für Abstürze, die schneller sind als die eigene
Prüfung des Workers.

## Unregistrierte Tasks und Rolling Deploys

Eine Nachricht, deren Task-Namen der Worker nicht registriert, ist beim
ersten Kontakt nicht verloren. Bei einem Rolling Deploy laufen die
Producer meist mit dem neuen Code, bevor der letzte alte Worker weg
ist; jeder neue Task-Name landete sonst in der `.XQ`, und jeder
Workflow mit einem solchen Schritt schlüge fehl. Seit 0.12 stellt der
Worker die Nachricht deshalb innerhalb einer Gnadenfrist zurück:

- **`SWARMQ_UNKNOWN_TASK_GRACE_S`** (Standard `900`, 15 Minuten) wird
  ab dem ersten Kontakt gemessen, den der Worker in den Header
  `x-swarmq-unknown-since` stempelt. Die Nachricht geht über die
  Delay-Queue zurück: 1 s beim ersten Mal, dann so lang, wie sie bisher
  gewartet hat (also etwa verdoppelt), höchstens 60 s je Runde und nie
  über das Ende der Frist hinaus. Jede Runde loggt
  `task_unknown_deferred` (WARNING), feuert `TASK_UNKNOWN_DEFERRED` und
  zählt `swarmq_tasks_unknown_deferred_total{queue}`. Ein Worker, der
  den Task kennt, nimmt die Kopie, sobald er die Queue konsumiert.
- **Nach der Frist** dead-lettert der Worker die Nachricht selbst:
  Fehlerergebnis `task '…' is not registered`, Workflow-Schritt als
  fehlgeschlagen markiert, Unique-Flag freigegeben, `TASK_DLQ`, danach
  `TASK_REJECTED` mit `reason="unregistered_task"`, Log
  `task_not_found` (ERROR). Die DLQ-Kopie trägt die Aufbewahrungsfrist
  und `x-swarmq-traceback` wie jede andere dead-geletterte Nachricht;
  `dlq retry` beginnt eine frische Frist.
- **`0` schaltet die Frist ab**: Dead-Letter beim ersten Kontakt. Für
  Worker, die fremde Nachrichten nie halten sollen, etwa eine Flotte,
  die eine geteilte Queue mit einem bewusst kleineren Task-Modul
  konsumiert.
- Ein Cron-Fire eines entfernten Tasks nimmt denselben Weg: Der
  Scheduler-Leader hält einen Cron zurück, den kein lebender Worker
  meldet (siehe
  [Scheduling](scheduling-and-retries.de.md#crons-die-niemand-meldet-werden-zuruckgehalten)),
  und ein Fire, das trotzdem durchkommt, endet mit Aufbewahrungsfrist in
  der `.XQ`, statt dort für immer zu liegen.

**Deploy-Reihenfolge.** Worker vor den Producern starten bzw. zuerst
ausrollen. Die Gnadenfrist deckt die Lücke eines gewöhnlichen Rolling
Deploys; ein Producer, der länger als `SWARMQ_UNKNOWN_TASK_GRACE_S`
voraus ist, füllt die `.XQ` wie vor 0.12.

**Mischbetrieb.** Nur Worker ab 0.12 stellen zurück. Ein Worker auf
0.11, der dieselbe Queue konsumiert, weist einen unbekannten Task beim
ersten Kontakt ohne Aufbewahrungsfrist ab (`TASK_REJECTED`, nur das
`x-death` des Brokers), auch wenn er eine Rückstellungs-Kopie eines
0.12-Workers bekommt. Bis jeder Worker einer Queue 0.12 fährt, gilt die
alte Regel: Worker vor Producern.

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

`x-swarmq-traceback` und `x-swarmq-lock-render-error` fassen höchstens
8 KiB: Ein längerer Text behält seine ersten 6 KiB und seine letzten
2 KiB, dazwischen steht ein Vermerk `[... N bytes truncated ...]`. Die
volle Fehlermeldung steht weiter im gespeicherten Fehlerergebnis. Ein
Nachrichten-Header muss in einen AMQP-Frame passen; ein zu großer Header
schloss die ganze Worker-Verbindung, und jede Nachricht, die der Worker
hielt, kam in die Queue zurück. Vor jedem Publish prüft SwarmQ zusätzlich
alle Header gegen die Hälfte des ausgehandelten `frame_max` (bei RabbitMQ
standardmäßig 128 KiB) und kürzt lange Texte bei Bedarf weiter; passen
sie dann immer noch nicht, wirft der Publish
[`MessageHeadersTooLarge`](../reference/errors.md#messageheaderstoolarge),
ohne etwas zu senden.

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
Operator-Kontext bleibt. Die zurückgelegte Nachricht trägt weder
`expiration` noch `x-swarmq-dead-lettered-at`; scheitert sie erneut,
wird sie mit frischer Frist dead-gelettert; `x-swarmq-enqueued-at`
beginnt neu, damit ein Task mit `max_age` nicht sofort abläuft. Die
Wiederholung behält die `x-swarmq-replay-id`, die der Worker beim
Dead-Lettern gestempelt hat (seit 0.12): Der
[Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
behandelt sie als neue Ausführung und lässt sie laufen; ein zweiter
`dlq retry` derselben Nachricht -- oder ihr at-least-once-Duplikat --
trägt dieselbe Identität und wird übersprungen. Eine Nachricht, die ein
0.11-Worker dead-gelettert hat, oder Altbestand ohne diesen Stempel
bekommt stattdessen bei jedem Retry eine neue ID. "Move messages" in
der RabbitMQ-UI oder ein Shovel legt die Nachricht mit denselben Headern
zurück und lässt sie deshalb ebenfalls laufen, setzt aber
`x-swarmq-enqueued-at` nicht neu.

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

## Aufbewahrungsfrist: dead-gelettete Nachrichten verfallen

Ab 0.11.0 verfällt jede Nachricht, die der Worker selbst dead-lettert,
`SWARMQ_DEAD_LETTER_TTL_S` Sekunden nach dem Dead-Lettering (Standard
`604800`, 7 Tage). Der Worker setzt die Nachrichten-Property
`expiration` und die Header `x-swarmq-dead-lettered-at`
(Unix-Millisekunden) und `x-swarmq-dead-letter-ttl-ms` (die Frist in
Millisekunden oder `"none"`). Die Queue-Argumente von `{queue}.XQ` bleiben
unverändert, bestehende Deployments starten also ohne
`PRECONDITION_FAILED`.

- **Lesen verlängert die Frist nicht.** `dlq list` und `dlq inspect`
  nehmen Nachrichten heraus und legen sie zurück, jede mit ihrer
  Restzeit, berechnet aus `x-swarmq-dead-lettered-at` und der eigenen
  Frist der Nachricht in `x-swarmq-dead-letter-ttl-ms`. Eine Nachricht,
  die nach der Hälfte ihrer Frist gelesen wird, verfällt trotzdem zum
  ursprünglichen Zeitpunkt. Eine CLI mit kürzerer oder
  Standard-`SWARMQ_DEAD_LETTER_TTL_S` kürzt die Frist des Workers nicht,
  und eine mit `none` dead-gelettete Nachricht fällt beim Lesen nie weg.
- **Abgelaufene Nachrichten fallen beim Lesen weg.** Eine Nachricht,
  deren Zeit abgelaufen ist, während ein Lesevorgang sie hielt, wird
  nicht zurückgelegt, auch bei `dlq retry` und `dlq retry --all`. Kein
  Log-Event meldet das, und der Zähler `scanned` von `--format json`
  enthält sie nicht.
- **Altbestand bekommt beim ersten Lesen die volle Frist.** Nachrichten
  ohne `x-swarmq-dead-lettered-at` oder ohne
  `x-swarmq-dead-letter-ttl-ms` (vor 0.11.0 dead-gelettert oder vom
  Dead-Letter-Exchange des Brokers geroutet) bekommen ab diesem
  Zeitpunkt beide Header und die volle `SWARMQ_DEAD_LETTER_TTL_S` der
  lesenden CLI.
- **`SWARMQ_DEAD_LETTER_TTL_S=none`** hält dead-gelettete Nachrichten
  unbegrenzt, wie vor 0.11.0. Die `swarmq dlq`-CLI liest die Variable
  nur für Altbestand ohne Frist-Header.

RabbitMQ verwirft abgelaufene Nachrichten nur am Kopf einer Queue. Eine
Nachricht ohne `expiration` davor — Altbestand oder eine Nachricht, die
der Dead-Letter-Exchange des Brokers geroutet hat (`x-delivery-limit`,
nicht dekodierbare oder unregistrierte Nachrichten) — hält den Verfall
aller dahinter auf, bis ein `dlq list` ihr eine Frist gibt. Vollständig
begrenzt diese Wege nur eine Broker-Policy.

## Die `.XQ` per Broker-Policy begrenzen

Die Frist pro Nachricht deckt Nachrichten nicht ab, die der Broker
selbst nach `{queue}.XQ` routet, und sie setzt keine Längengrenze.
SwarmQ ändert die Argumente bestehender Queues nicht. Eine
RabbitMQ-Policy begrenzt die Dead-Letter-Queues vollständig und sofort;
vor 0.11.0 ist sie die einzige Grenze:

```bash
rabbitmqctl set_policy swarmq-xq '\.XQ$' \
  '{"message-ttl": 604800000, "max-length": 100000}' \
  --apply-to queues
```

Damit bleiben dead-geletterte Nachrichten höchstens 7 Tage
(`message-ttl` in Millisekunden) und höchstens 100 000 pro Queue; die
Werte an das eigene Volumen anpassen. Für einen anderen vhost als `/`
`-p VHOST` ergänzen.

!!! warning "Bevor du die Policy setzt"
    - **Pro Queue gilt nur eine Policy**: die passende mit der höchsten
      Priorität. Passt schon eine andere Policy auf deine `.XQ`-Queues
      (etwa ein allgemeines `.*`), werden die beiden nicht
      zusammengeführt: Entweder ersetzt deine deren Einstellungen für
      diese Queues, oder sie wirkt gar nicht. Alle Schlüssel in eine
      Policy mit höherer `--priority` legen.
    - **Überlauf wird verworfen.** Nachrichten über `max-length` (per
      Default die ältesten) und abgelaufene Nachrichten werden
      gelöscht, nicht weiter dead-gelettert, und kein SwarmQ-Signal
      meldet das. Was du vielleicht noch retryen wolltest, ist weg.

## Dead-Lettering von Task-Fehlern abschalten

Jeder terminale Task-Fehler landet in `{queue}.XQ`, auch mit
`max_retries=0`. Vor 0.1.2 hat ein Task mit `max_retries=0`
stattdessen das Fehlerergebnis gespeichert und die Nachricht bestätigt.
So bekommst du dieses Verhalten für jeden terminalen Task-Fehler:

```python
class SyncInventory(Task):
    task_info = TaskInfo(
        name="sync_inventory", queues=["default"], dead_letter=False,
    )
```

oder global mit `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES=false`
(`SwarmQConfig(dead_letter_terminal_failures=False)`). Ein am Task
gesetzter Wert gewinnt über den globalen, in beide Richtungen.

Abgeschaltet speichern die drei terminalen Zweige — `NoRetry`, eine
Exception außerhalb von `retry_exceptions`, erschöpfte Retries — das
Fehlerergebnis mit dem `result_ttl` des Tasks, markieren einen
Workflow als fehlgeschlagen, geben das Unique-Flag frei und bestätigen
die Nachricht. `get_result()` wirft wie gewohnt `TaskError`.

Was sich fürs Monitoring ändert:

- `TASK_DLQ` und `swarmq_tasks_total{outcome="dlq"}` feuern für diese
  Tasks nicht mehr; ein Alarm auf `outcome="dlq"` deckt sie also nicht
  mehr ab.
- `TASK_FAILED` und `swarmq_tasks_total{outcome="failed"}` feuern
  weiter bei jedem gescheiterten Versuch; alarmiere stattdessen darauf.
- Die Zustellzähler-Prüfung (`WorkerHungError`), die Waisen-Grenze des
  Ausführungsschutzes, nicht dekodierbare Nachrichten, `max_age` und
  Lock-Fehler gehen weiter in die `.XQ` (dritte Spalte der
  [Tabelle oben](#wann-nachrichten-in-der-dlq-landen)).

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
bedeuten (mit abgeschaltetem Dead-Lettering von Task-Fehlern deckt der
erste diese Tasks nicht mehr ab; dann `outcome="failed"` nutzen):

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
