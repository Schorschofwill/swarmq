# Dead-Letter-Queues

Ein Task hat seine Retries erschöpft, `NoRetry` geworfen oder ist vor
dem Abholen abgelaufen. Seine Nachricht liegt jetzt in der
Dead-Letter-Queue der Queue, `{queue}.XQ`, bis ein Operator eingreift
oder ihre Aufbewahrungsfrist abläuft (standardmäßig 7 Tage, siehe
[Aufbewahrungsfrist](#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen)).
Dieser Guide zeigt, wie du herausfindest, warum eine Nachricht dort
gelandet ist, wie du sie mit `swarmq dlq` auflistest, inspizierst,
retryst und purgst, wie du die DLQ begrenzt und wie du das
Dead-Lettering von Task-Fehlern abschaltest.

## Voraussetzungen

- Die `swarmq`-CLI (Teil des Pakets `swarmq`).
- `SWARMQ_BROKER_URL` zeigt auf den RabbitMQ-vhost, aus dem deine Worker
  konsumieren. Setze `SWARMQ_NAMESPACE` wie bei den Workern; ein
  ungültiger Namensraum beendet jeden `swarmq dlq`-Befehl mit Exit `78`.

## Wann Nachrichten in der DLQ landen

`max_retries` ist standardmäßig `0`: Ohne eigene Retry-Policy ist schon
der **erste** Fehlschlag eines Tasks terminal.

| Auslöser | Header auf der DLQ-Nachricht | Mit abgeschaltetem Dead-Lettering von Task-Fehlern |
|---|---|---|
| Retries erschöpft (`max_retries` erreicht, auch `max_retries=0`) | `x-swarmq-retry-count` am Limit, `x-swarmq-traceback` mit der Fehlermeldung | nicht dead-gelettert: Fehlerergebnis gespeichert, bestätigt |
| `raise NoRetry("reason")` im Task | `x-swarmq-traceback` mit der Fehlermeldung | nicht dead-gelettert: Fehlerergebnis gespeichert, bestätigt |
| Exception außerhalb von `retry_exceptions` | `x-swarmq-traceback` mit der Fehlermeldung | nicht dead-gelettert: Fehlerergebnis gespeichert, bestätigt |
| `x-delivery-limit` (Default 20) von RabbitMQ ausgelöst, oder die eigene Zustellzähler-Prüfung des Workers eine Zustellung vorher | `x-death` vom Broker; vom Worker `x-swarmq-traceback` mit `WorkerHungError` | weiter dead-gelettert |
| Der Worker hat die Zustellung zurückgegeben (`nack(requeue=True)`), bis `x-acquired-count` das Zustelllimit erreichte (RabbitMQ 4.3) | `x-swarmq-traceback` mit `RedeliveryLimitError` | weiter dead-gelettert |
| Dieselbe Ausführung ist `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS`-mal (Default 3) ohne Freigabe abgelaufen ([Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)) | `x-swarmq-traceback` mit `WorkerHungError` | weiter dead-gelettert |
| `TaskInfo.max_age` beim Abholen überschritten | kein eigener: Die Nachricht behält ihre ursprünglichen Header; das gespeicherte Fehlerergebnis lautet `task expired: age …s exceeds max_age …s` | weiter dead-gelettert |
| Lock-Template ließ sich nicht rendern (fehlendes Kwarg, leerer oder reservierter Name) | `x-swarmq-lock-render-error` mit der Fehlermeldung, `x-swarmq-traceback` | weiter dead-gelettert |
| Wartezeit auf einen Task-Lock zu Ende (hängender Halter oder `SWARMQ_LOCK_WAIT_MAX_S`, Default 6 h) | `x-swarmq-lock-name`, `x-swarmq-traceback` mit `LockWaitExceeded` | weiter dead-gelettert |
| Task-Name im Worker nach der Gnadenfrist `SWARMQ_UNKNOWN_TASK_GRACE_S` (Default 15 min) nicht registriert, siehe [unregistrierte Tasks](#unregistrierte-tasks-und-rolling-deploys) | `x-swarmq-traceback` mit `task '…' is not registered`; das gespeicherte Fehlerergebnis sagt dasselbe | weiter dead-gelettert |
| Body über `SWARMQ_MAX_MESSAGE_BODY_BYTES`, nicht dekodierbarer Body, kaputte Middleware-Chain-Factory | `x-swarmq-traceback` mit `<reason>: <error>` (z. B. `undecodable: Expecting value …`) | weiter dead-gelettert |
| Die eigene Dead-Letter-Kopie des Workers ließ sich nicht publizieren (Rückfall) | keiner von SwarmQ, nur das `x-death` des Brokers | weiter dead-gelettert |

`x-swarmq-traceback` enthält die Fehlermeldung (`str(exc)`), keinen
Python-Traceback. Jede Nachricht, die der Worker selbst dead-lettert,
trägt außerdem `x-swarmq-dead-lettered-at` (Unix-Millisekunden) und eine
`expiration` pro Nachricht, siehe
[Aufbewahrungsfrist](#aufbewahrungsfrist-dead-gelettete-nachrichten-verfallen).
Die dritte Spalte erklärt
[Dead-Lettering abschalten](#dead-lettering-von-task-fehlern-abschalten).

Die DLQ ist eine gewöhnliche RabbitMQ-Queue. `rabbitmqadmin` und die
Management-UI können sie lesen, aber `swarmq dlq` dekodiert sie
SwarmQ-gerecht und lässt die Aufbewahrungsfrist unangetastet.

## Drei Wege in die `.XQ`

Nachrichten erreichen `{queue}.XQ` auf drei Wegen, und die sind nicht
gleich gut sichtbar. Ein Alert auf die Queue-Tiefe fängt alle drei; zu
wissen, welcher Weg gefeuert hat, sagt dir, was zu reparieren ist.

| Weg | Wer bewegt die Nachricht | Signal | Metrik | Header |
|---|---|---|---|---|
| **Dead-Lettering durch den Worker**: Retries erschöpft, `NoRetry`, `max_age`, Lock-Fehler, Delivery-Limit kurz vor dem Auslösen, Waisen-Grenze des Ausführungsschutzes, unregistrierter Task nach seiner Gnadenfrist | Worker publiziert nach `.XQ` und bestätigt das Original | `TASK_DLQ` (`task_id`, `task_name`, `queue`, `error`); beim unregistrierten Task gefolgt von `TASK_REJECTED` mit `unregistered_task`; bei `max_age` stattdessen `TASK_EXPIRED` | `swarmq_tasks_total{outcome="dlq"}` | SwarmQ-Diagnose (`x-swarmq-retry-count`, `x-swarmq-traceback`, `x-swarmq-lock-*`); ein Fehlerergebnis wird gespeichert |
| **Abweisung durch den Worker**: Die Nachricht ließ sich nicht in einen Task verwandeln | Worker publiziert eine Kopie mit Frist nach `.XQ` und bestätigt das Original; scheitert die Kopie, weist er ohne Requeue ab, und der Dead-Letter-Exchange des Brokers routet sie | `TASK_REJECTED` (`queue`, `reason`, `task_id`, `task_name`, `error`) | `swarmq_tasks_rejected_total{queue,reason}` | `x-swarmq-traceback` mit `<reason>: <error>` plus die Frist-Stempel; kein Fehlerergebnis. Nur der Rückfall trägt stattdessen das `x-death` des Brokers. `task_id`/`task_name` sind `None`, wenn der Body nie dekodiert wurde |
| **Dead-Lettering durch den Broker**: `x-delivery-limit` nach wiederholten Worker-Abstürzen, Nachrichten-TTL, Längenlimit der Queue | RabbitMQ selbst | keines, kein Worker hat die letzte Zustellung gesehen | `swarmq_dlq_messages{queue}` (Tiefen-Gauge, vom Worker abgetastet) | nur `x-death` |

`reason` auf dem Abweisungsweg ist einer von `body_too_large`,
`undecodable`, `middleware_chain_factory` oder `dlq_publish_failed`;
`swarmq dlq list` zeigt die ersten drei in `ERROR_PREVIEW`.
`unregistered_task` kommt vom ersten Weg. Nach einem Deploy heißt es
meist, dass Worker und Producer verschiedene Versionen des Task-Moduls
fahren; `body_too_large` und `undecodable` zeigen auf den Producer.

Beobachte den dritten Weg, wenn die anderen beiden still sind: Ein
Task, der den Prozess zuverlässig tötet (SIGSEGV, OOM), erreicht nie
`TASK_FAILED`. Der Worker dead-lettert eine solche Nachricht ab der
vorletzten Zustellung selbst, sie kommt also meist auf dem ersten Weg
mit `TASK_DLQ` an; der Broker-Weg bleibt für TTL und Längenlimits und
für Abstürze, die schneller sind als die eigene Prüfung des Workers.

## Unregistrierte Tasks und Rolling Deploys

Eine Nachricht, deren Task-Namen der Worker nicht kennt, wird beim
ersten Kontakt nicht dead-gelettert. Bei einem Rolling Deploy laufen die
Producer oft schon mit dem neuen Code, bevor der letzte alte Worker weg
ist; jeder neue Task-Name, und jeder Workflow mit einem solchen Schritt,
landete sonst in der `.XQ`. Der Worker stellt die Nachricht deshalb
während einer Gnadenfrist zurück:

- **`SWARMQ_UNKNOWN_TASK_GRACE_S`** (Default `900`, 15 Minuten) zählt
  ab dem ersten Kontakt, den der Worker in den Header
  `x-swarmq-unknown-since` stempelt. Die Nachricht geht über die
  Delay-Queue zurück: 1 s beim ersten Mal, dann so lang, wie sie bisher
  gewartet hat (etwa verdoppelt), höchstens 60 s je Runde und nie über
  das Ende der Gnadenfrist hinaus. Jede Runde loggt
  `task_unknown_deferred` (WARNING), feuert `TASK_UNKNOWN_DEFERRED` und
  zählt `swarmq_tasks_unknown_deferred_total{queue}`. Ein Worker, der
  den Task kennt, nimmt die Kopie, sobald er die Queue konsumiert.
- **Nach der Gnadenfrist** dead-lettert der Worker die Nachricht:
  Fehlerergebnis `task '…' is not registered`, Workflow-Schritt als
  fehlgeschlagen markiert, Unique-Flag freigegeben, `TASK_DLQ`, danach
  `TASK_REJECTED` mit `reason="unregistered_task"`, Log
  `task_not_found` (ERROR). `dlq retry` beginnt eine frische
  Gnadenfrist.
- **`0` schaltet die Gnadenfrist ab**: Dead-Letter beim ersten Kontakt.
  Für Worker, die nie fremde Nachrichten halten sollen, etwa eine
  Flotte, die eine geteilte Queue mit einem bewusst kleineren
  Task-Modul konsumiert.
- Ein Cron-Fire eines entfernten Tasks nimmt denselben Weg: Der
  Scheduler-Leader hält einen Cron zurück, den kein lebender Worker
  meldet (siehe
  [Scheduling](scheduling-and-retries.md#crons-die-niemand-meldet-werden-zuruckgehalten)),
  und ein Fire, das trotzdem durchkommt, endet mit Aufbewahrungsfrist in
  der `.XQ`.

**Deploy-Reihenfolge.** Starte oder rolle die Worker vor den Producern.
Die Gnadenfrist deckt die Lücke eines gewöhnlichen Rolling Deploys; ein
Producer, der länger als `SWARMQ_UNKNOWN_TASK_GRACE_S` vorausläuft,
füllt die `.XQ` wieder. Nur Worker ab 0.12 stellen zurück: Ein
0.11-Worker an derselben Queue weist einen unbekannten Task beim ersten
Kontakt ab, auch eine Rückstellungs-Kopie eines neueren Workers (siehe
[Ausrollen und Upgraden](deployment.md)).

## DLQ-Inhalte auflisten

```bash
swarmq dlq list --queue default
```

```text
TASK_ID                               TASK_NAME   RETRIES  ERROR_PREVIEW            BODY_BYTES
0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71  send_email  3        smtp connection refused  312
0199f3a4-1d02-7e55-8c3b-61a9d0f7e2c8  charge      -        card declined            184
```

`RETRIES` ist `-`, wenn die Nachricht nie wiederholt wurde (etwa nach
`NoRetry`); `ERROR_PREVIEW` ist die erste Zeile von
`x-swarmq-traceback`, nach 60 Zeichen abgeschnitten.

| Flag | Default | Bedeutung |
|---|---|---|
| `--queue QUEUE` | `default` | die Haupt-Queue; die DLQ ist `{queue}.XQ` |
| `--limit N` | `100` | höchstens N Nachrichten lesen. Endete das Lesen am Limit, schließt die Tabelle mit `(truncated at --limit N; …)` und JSON hat `truncated: true`, auch wenn abgelaufene Nachrichten die Lesungen verbraucht haben (seit 0.13.0) |
| `--format json` | Tabelle | Umschlag `{"entries", "scanned", "limit", "truncated", "queue"}`; jeder Eintrag hat `task_id`, `task_name`, `queue`, `headers` und `body_size`. Den Body zeigt `dlq inspect` |

Auflisten zerstört nichts. `dlq list`, `dlq inspect` und `dlq retry`
lesen mit Bestätigung über einen eigenen Channel: Jede gelesene
Nachricht wird ans Ende der DLQ kopiert (bei `retry` in die Haupt-Queue)
und erst bestätigt, wenn der Broker die Kopie bestätigt hat. Stirbt der
Lesende mittendrin, geht nichts verloren. Drei Folgen:

- Stirbt er zwischen Kopie und Bestätigung, liegt die Nachricht zweimal
  in der DLQ; ein Retry beider führt den Task einmal aus (dieselbe
  Ausführungsidentität).
- Lesen ordnet die DLQ um: Gelesene Nachrichten wandern ans Ende.
- Zwei gleichzeitige Listings sehen jeweils nur einen Teil der DLQ.

## Eine Nachricht inspizieren

```bash
swarmq dlq inspect 0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71 --queue default
```

Gibt die Header und den dekodierten Body einer Nachricht aus:

```text
TASK_ID:   0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71
QUEUE:     default.XQ
BODY_SIZE: 312 bytes
HEADERS:
  x-swarmq-enqueued-at: 1792836421
  x-swarmq-retry-count: 3
  x-swarmq-traceback: smtp connection refused
  x-swarmq-dead-lettered-at: 1792836512345
  x-swarmq-dead-letter-ttl-ms: 604800000
  ...
BODY:
{
  "task": {"id": "0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71", "queue": "default", "name": "send_email"},
  "args": [],
  "kwargs": {"to": "bad@example.com", "subject": "Welcome"}
}
```

`--scan-limit N` (Default 1000) verbreitert die Suche, wenn die Task-ID
tief in einer großen DLQ liegt. Exit-Code `3` heißt nicht gefunden; die
Meldung sagt dann, ob die Suche an `--scan-limit` endete.

`x-swarmq-traceback` und `x-swarmq-lock-render-error` fassen höchstens
8 KiB: Ein längerer Text behält seine ersten 6 KiB und seine letzten
2 KiB, dazwischen steht ein Vermerk `[... N bytes truncated ...]`. Die
volle Fehlermeldung steht weiter im gespeicherten Fehlerergebnis. Vor
jedem Publish prüft SwarmQ außerdem alle Header gegen die Hälfte des
ausgehandelten `frame_max` (bei RabbitMQ standardmäßig 128 KiB) und
kürzt lange Texte weiter; passen sie dann immer noch nicht, wirft der
Publish [`MessageHeadersTooLarge`](../reference/errors.md#messageheaderstoolarge),
ohne etwas zu senden. Ein zu großer Header schlösse sonst die ganze
Worker-Verbindung.

## Nachrichten retryen

### Eine Nachricht

```bash
swarmq dlq retry 0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71 --queue default
```

Publiziert die Nachricht zurück in die Haupt-Queue und entfernt sie aus
der DLQ. Die Wiederholung beginnt frisch:

- **Retry-Budget:** `x-swarmq-retry-count` wird entfernt, der Task
  bekommt also wieder sein volles `max_retries`.
- **Alter:** `x-swarmq-enqueued-at` beginnt neu, damit ein Task mit
  `max_age` nicht sofort abläuft.
- **Aufbewahrungsfrist:** `expiration`, `x-swarmq-dead-lettered-at` und
  `x-swarmq-dead-letter-ttl-ms` werden entfernt; scheitert die
  Wiederholung erneut, wird sie mit frischer Frist dead-gelettert.
- **Schleifenzustand:** Die Gnadenfrist für unbekannte Tasks, die
  Lock-Wartezeit und der Rückstellungszähler eines Cron- oder
  `delay`/`eta`-Fires beginnen neu.
- **Diagnose:** Die übrigen Header, etwa `x-swarmq-traceback` und der
  Dedup-Hash, bleiben erhalten.

Die Wiederholung läuft einmal, auch wenn du den Befehl wiederholst. Sie
behält die `x-swarmq-replay-id`, die der Worker beim Dead-Lettern
gestempelt hat: Der
[Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
behandelt die Wiederholung als neue Ausführung, während ein zweiter
`dlq retry` derselben Nachricht oder ihr At-least-once-Duplikat
dieselbe Identität trägt und übersprungen wird. Eine Nachricht ohne
diesen Stempel (vom Broker selbst, von einem Worker vor 0.12
dead-gelettert, oder Altbestand) bekommt eine aus ihrer Identität
abgeleitete ID mit derselben Wirkung; nur eine Nachricht ohne Task-ID
bekommt eine zufällige.

!!! note "Wiederholen über die RabbitMQ-UI oder einen Shovel"
    „Move messages“ in der Management-UI oder ein Shovel legt die
    Nachricht mit unveränderten Headern zurück. Das führt den Task
    ebenfalls aus, behält aber `x-swarmq-enqueued-at` und den
    Retry-Zähler. Kopien, die Worker ab 0.12.2 dead-gelettert haben,
    tragen keine Zustellzähler des Brokers und lassen sich so bewegen.
    Einträge älterer Worker und Nachrichten, die der Broker selbst
    dead-gelettert hat (Delivery-Limit, `route_via_dlx`), können sie
    noch tragen und gehen sofort zurück in die DLQ; dead-lettert der
    Broker eine bewegte Nachricht erneut, wird ein zweiter Move als
    Duplikat übersprungen. Nimm dafür `swarmq dlq retry`.

### Alle Nachrichten

```bash
swarmq dlq retry --all --queue default
```

Bewegt jede Nachricht, höchstens `--limit N` (Default 10000).
Nachrichten ohne dekodierbare Task-ID bleiben in der DLQ und werden
getrennt gemeldet (`skipped_malformed` bei `--format json`), damit du
entscheiden kannst, ob du sie purgst.

## Purgen

```bash
swarmq dlq purge --queue default
```

Natives AMQP-`queue.purge()` auf `{queue}.XQ`, ohne Rückfrage.
**Unumkehrbar**: Die Nachrichten sind weg. Nutze es, wenn ein Bug die
DLQ mit Nachrichten geflutet hat, die du nicht retryen wirst, oder
nachdem `dlq retry --all` nur noch kaputte Einträge zurückgelassen hat.
`--format json` gibt `{"purged": N, "queue": "..."}` zurück.

## Aufbewahrungsfrist: dead-gelettete Nachrichten verfallen

Jede Nachricht, die der Worker selbst dead-lettert, verfällt
`SWARMQ_DEAD_LETTER_TTL_S` Sekunden nach dem Dead-Lettering (Default
`604800`, 7 Tage; seit 0.11.0). Der Worker setzt die
Nachrichten-Property `expiration` und die Header
`x-swarmq-dead-lettered-at` (Unix-Millisekunden) und
`x-swarmq-dead-letter-ttl-ms` (die Frist in Millisekunden oder
`"none"`). Die Queue-Argumente von `{queue}.XQ` bleiben unverändert,
bestehende Deployments starten also ohne `PRECONDITION_FAILED`.

- **Lesen verlängert die Frist nicht.** `dlq list` und `dlq inspect`
  legen jede Nachricht mit ihrer Restzeit zurück, berechnet aus ihren
  eigenen Headern. Eine CLI mit kürzerer oder Standard-
  `SWARMQ_DEAD_LETTER_TTL_S` kürzt die Frist des Workers nicht, und
  eine mit `none` dead-gelettete Nachricht fällt beim Lesen nie weg.
- **Abgelaufene Nachrichten fallen beim Lesen weg.** Eine Nachricht,
  deren Zeit ablief, während ein Lesevorgang sie hielt, wird nicht
  zurückgelegt, auch bei `dlq retry` und `dlq retry --all`. Kein
  Log-Event meldet das, und `scanned` zählt sie nicht. Bei `--limit` und
  `--scan-limit` zählt sie trotzdem mit, `truncated` und
  `scan_exhausted` sind also wahr, sobald das Lesen am Limit endete
  (seit 0.13.0).
- **Altbestand bekommt beim ersten Lesen die volle Frist.** Nachrichten
  ohne die beiden Header (vor 0.11.0 dead-gelettert oder vom
  Dead-Letter-Exchange des Brokers geroutet) bekommen ab diesem
  Zeitpunkt beide Header und die volle `SWARMQ_DEAD_LETTER_TTL_S` der
  lesenden CLI.
- **`SWARMQ_DEAD_LETTER_TTL_S=none`** hält dead-gelettete Nachrichten
  unbegrenzt. Die `swarmq dlq`-CLI liest die Variable nur für
  Altbestand ohne Frist-Header.

RabbitMQ verwirft abgelaufene Nachrichten nur am Kopf einer Queue. Eine
Nachricht ohne `expiration` davor hält den Verfall aller dahinter auf,
bis ein `dlq list` ihr eine Frist gibt. Solche Nachrichten sind
Altbestand und Nachrichten, die der Dead-Letter-Exchange des Brokers
geroutet hat:

- Nachrichten über dem `x-delivery-limit` der Queue,
- der Rückfall, wenn die eigene Dead-Letter-Kopie des Workers nicht
  publiziert werden konnte (`dlq_publish_failed`): auf den Lock- und
  Unknown-Task-Pfaden und bei einer abgewiesenen Nachricht
  (`undecodable`, `body_too_large`, `middleware_chain_factory`). Endete
  eine solche Kopie mit unbestimmtem Publish-Ausgang, kann die
  Nachricht zweimal in der `.XQ` liegen, einmal mit und einmal ohne
  Frist.

Unbekannte Tasks (seit 0.12) und abgewiesene Nachrichten (seit 0.12.0)
bekommen normalerweise eine Frist; Worker bis 0.12.0rc1 an derselben
Queue routen abgewiesene Nachrichten weiter über den
Dead-Letter-Exchange. Jeden Weg begrenzt nur eine Broker-Policy.

## Die `.XQ` per Broker-Policy begrenzen

Die Frist pro Nachricht deckt Nachrichten nicht ab, die der Broker
selbst nach `{queue}.XQ` routet, und sie setzt keine Längengrenze. Eine
RabbitMQ-Policy begrenzt die Dead-Letter-Queues vollständig und sofort:

```bash
rabbitmqctl set_policy swarmq-xq '\.XQ$' \
  '{"message-ttl": 604800000, "max-length": 100000}' \
  --apply-to queues
```

Damit bleiben dead-gelettete Nachrichten höchstens 7 Tage
(`message-ttl` in Millisekunden) und höchstens 100 000 pro Queue; passe
die Werte an dein Volumen an. Für einen anderen vhost als `/`
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
`max_retries=0`. Genügt dir für einen Task ein gespeichertes
Fehlerergebnis, schalte das Dead-Lettering seiner Fehler ab:

```python
class SyncInventory(Task):
    task_info = TaskInfo(
        name="sync_inventory", queues=["default"], dead_letter=False,
    )
```

oder global mit `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES=false`
(`SwarmQConfig(dead_letter_terminal_failures=False)`). Ein am Task
gesetzter Wert gewinnt über den globalen, in beide Richtungen.

Abgeschaltet speichern die drei terminalen Zweige (`NoRetry`, eine
Exception außerhalb von `retry_exceptions`, erschöpfte Retries) das
Fehlerergebnis mit dem `result_ttl` des Tasks, markieren einen Workflow
als fehlgeschlagen, geben das Unique-Flag frei und bestätigen die
Nachricht. `get_result()` wirft wie gewohnt `TaskError`.

Was sich fürs Monitoring ändert:

- `TASK_DLQ` und `swarmq_tasks_total{outcome="dlq"}` feuern für diese
  Tasks nicht mehr; ein Alert auf `outcome="dlq"` deckt sie also nicht
  mehr ab.
- `TASK_FAILED` und `swarmq_tasks_total{outcome="failed"}` feuern
  weiter bei jedem gescheiterten Versuch; alarmiere stattdessen darauf.
- Die Zustellzähler-Prüfung (`WorkerHungError`), die Waisen-Grenze des
  Ausführungsschutzes, abgewiesene Nachrichten, `max_age` und
  Lock-Fehler gehen weiter in die `.XQ` (dritte Spalte der
  [Tabelle oben](#wann-nachrichten-in-der-dlq-landen)).

## Exit-Codes fürs Scripting

| Code | Bedeutung |
|---|---|
| `0` | Erfolg |
| `1` | Laufzeitfehler (Broker nicht erreichbar, …) |
| `2` | falsche Argumente, etwa `dlq retry` mit Task-ID und `--all` zugleich |
| `3` | `dlq inspect` / `dlq retry <task_id>` hat nichts gefunden |
| `78` | ungültige Konfiguration, etwa `SWARMQ_NAMESPACE` |

```bash
swarmq dlq retry "$TASK_ID" --queue default
case $? in
    0) echo "republiziert" ;;
    3) echo "Task nicht in der DLQ" ;;
    1) echo "Laufzeitfehler, Broker prüfen" ;;
    *) echo "anderer Fehler" ;;
esac
```

## Operator-Workflow

1. **Sichten.** `swarmq dlq list --queue default --limit 200 --format json`
   und die Einträge nach `headers["x-swarmq-traceback"]` gruppieren,
   etwa mit `jq`, um zu sehen, was dominiert.
2. **Ursache beheben.** Den Fix ausrollen oder die Eingaben reparieren,
   wenn der Fehler in den Argumenten lag.
3. **Gesammelt retryen.** `swarmq dlq retry --all --queue default`.
4. **Nachzügler behandeln.** `--format json` meldet
   `skipped_malformed`; diese Nachrichten inspizieren.
5. **Bei Bedarf purgen.** `swarmq dlq purge` für kaputte oder nicht
   wiederherstellbare Nachrichten.

## Alerting

Alarmiere auf die DLQ-Tiefe: Mit `swarmq[metrics]` tastet der Worker
`swarmq_dlq_messages{queue}` für die `.XQ` jeder konsumierten Queue ab;
alarmiere bei `> 0 for 10m`. Dieses Gauge fängt alle
[drei Wege](#drei-wege-in-die-xq), auch das broker-seitige
Dead-Lettering, das kein Signal meldet.

Alarmiere getrennt auf die beiden Zähler, weil sie Verschiedenes
bedeuten:

- `increase(swarmq_tasks_total{outcome="dlq"}[10m]) > 0`: Tasks sind
  gelaufen und terminal gescheitert; Fehlerergebnis und Traceback-Header
  ansehen. Mit abgeschaltetem Dead-Lettering von Task-Fehlern für diese
  Tasks `outcome="failed"` nutzen.
- `increase(swarmq_tasks_rejected_total[10m]) > 0`: Nachrichten sind nie
  zu Tasks geworden; nach `reason` gruppieren und Producer oder
  Deployment prüfen.

Ohne Prometheus meldet die Management-API von RabbitMQ die Tiefe unter
`/api/queues/%2F/{queue}.XQ`.

## Verifizieren

Mit einem laufenden Worker für die Queue `default` einen Task einplanen,
der immer `NoRetry` wirft:

```python
from swarmq import NoRetry, Task, TaskInfo


class AlwaysBad(Task):
    task_info = TaskInfo(name="always_bad", queues=["default"])

    async def run(self) -> None:
        raise NoRetry("absichtliche DLQ")
```

```bash
swarmq dlq list --queue default
# → eine Nachricht mit ERROR_PREVIEW "absichtliche DLQ"

swarmq dlq retry --all --queue default
# → republished 1 message(s) from default.XQ to default
#   der Task scheitert erneut und liegt wieder in der DLQ

swarmq dlq purge --queue default
# → purged 1 message(s) from default.XQ
```

## Siehe auch

- [Scheduling und Retries](scheduling-and-retries.md): `max_retries`
  und `retry_exceptions` so einstellen, dass unnötige DLQ-Treffer
  ausbleiben
- [Fehlerkatalog](../reference/errors.md): `NoRetry` und die anderen
  Exceptions, die in die DLQ führen
- [`swarmq dlq`-CLI-Referenz](../reference/cli.md#swarmq-dlq): jedes
  Flag und die JSON-Ausgabe
