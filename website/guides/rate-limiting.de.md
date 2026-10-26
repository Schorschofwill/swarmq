# Rate-Limiting

Du hast einen Task, der nicht öfter als N-mal pro Zeiteinheit laufen
darf: ein Webhook-Kontingent, ein API-Tarif eines Drittanbieters, eine
Höflichkeitsregel für einen Crawler. Das Rate-Limit von SwarmQ ist
deklarativ. Du setzt einen String in `TaskInfo`, und alle Worker, die
sich ein Valkey teilen, setzen ihn gemeinsam durch.

## Voraussetzungen

- Ein Valkey-Backend, das alle Worker teilen.
- Ein Task, den du begrenzen willst.

## Das Limit deklarieren

```python
from swarmq import Task, TaskInfo


class SendSMS(Task):
    task_info = TaskInfo(
        name="send_sms",
        queues=["sms"],
        rate_limit="100/m",
    )

    async def run(self, to: str, body: str) -> None:
        await sms_provider.send(to, body)
```

Das Format ist `"N/unit"`: eine ganze Zahl von mindestens 1, ein
Schrägstrich und eine Zeiteinheit. Leerzeichen um den Schrägstrich sind
erlaubt, Groß- und Kleinschreibung der Einheit spielt keine Rolle.

| Einheit | Schreibweisen |
|---|---|
| Pro Sekunde | `s` |
| Pro Minute | `m`, `min` |
| Pro Stunde | `h`, `hour` |

Beispiele: `"5/s"`, `"100/m"`, `"1000/h"`. Ein String, der sich nicht
lesen lässt (unbekannte Einheit wie `"100/day"`, fehlende Zahl, Anzahl
unter 1), wirft beim Import des Task-Moduls `ValueError`.

## Wie das Limit durchgesetzt wird

Gezählt wird je Task-Name in Valkey, mit einem atomaren Lua-Skript. Es
nähert ein gleitendes Fenster aus zwei festen Fenstern an: die Zahl des
aktuellen Fensters plus die des vorigen, gewichtet mit dem Anteil, der
noch überlappt. Die Zeit kommt von der Valkey-Uhr (`TIME`), die Uhren
der Worker spielen keine Rolle.

Der Worker prüft das Limit bei jeder Zustellung, nach dem
Ausführungsschutz und der `max_age`-Prüfung und vor dem Task-Lock:

- **Unter dem Limit:** Der Zähler steigt, und die Nachricht geht weiter
  zum Lock (falls der Task einen hat) und zu `run()`. Der Slot ist ab
  diesem Moment verbraucht, auch wenn `run()` nicht folgt (siehe
  [mit einem Lock](#kombination-mit-retries-und-locks)).
- **Über dem Limit:** Der Worker veröffentlicht eine Kopie der Nachricht
  in die Delay-Queue, bestätigt das Original und feuert
  `TASK_RATE_LIMITED`. Die Kopie wartet `retry_after_ms`, bis die
  gewichtete Zählung wieder unter dem Limit liegt, plus einen zufälligen
  Jitter, und durchläuft dann alle Prüfungen erneut.
  In einer [Delay-Stufe](../explanation/delivery-semantics.md#delay-stufen)
  kann die Wartezeit etwas länger ausfallen. Lässt sich die Kopie nicht
  veröffentlichen, loggt der Worker `rate_limit_requeue_failed` (ERROR)
  und gibt das Original an den Broker zurück.

Der Jitter verteilt Kopien, die im selben Moment abgewiesen wurden, damit
sie nicht alle für denselben freien Platz zurückkommen. Er ist
gleichverteilt bis zu zwei Platzabständen (Fenster geteilt durch das
Limit, bei `100/m` also 1,2 s) und verdoppelt sich mit jeder weiteren
Abweisung derselben Nachricht, höchstens bis zu einem Fenster. Bei
anhaltendem Rückstand bleibt der Durchsatz nahe am Limit; es gibt keine
harte Kante bei der Minutenmarke. Vor 0.15 wartete eine Kopie bis zum
Ende des festen Fensters, und ein Rückstand lief mit etwa der Hälfte des
Limits.

Der Zähler hängt am Task-Namen. Zwei Apps, die denselben Task-Namen auf
einem Valkey registrieren, teilen sich das Limit, solange nicht jede
ihren eigenen
[`SWARMQ_NAMESPACE`](../reference/configuration.md#namensraum-mehrere-apps-auf-einem-valkey)
setzt.

## Was du als Konsument siehst

Eine begrenzte Nachricht scheitert nicht. Sie wartet. Die Kopien sind
neue Nachrichten ohne Zustellzähler des Brokers, das Delivery-Limit
beendet das Warten also nie. Nur `TaskInfo.max_age` macht aus langem
Warten einen DLQ-Eintrag; es zählt ab der ursprünglichen Einreihung:

```python
task_info = TaskInfo(
    name="send_sms",
    queues=["sms"],
    rate_limit="100/m",
    max_age=300,   # nach 5 min aufgeben
)
```

Ist Valkey bei der Prüfung nicht erreichbar (oder antwortet nicht
innerhalb von `SWARMQ_BACKEND_OP_TIMEOUT`), wird die Nachricht nicht
begrenzt, sondern mit eigenem Backoff über die Delay-Queue
zurückgestellt: kein `TASK_RATE_LIMITED`, `rate_limit_count` bleibt
unverändert, und der Worker loggt `task_deferred_backend_unavailable`
(`gate="rate_limit"`). Jeder andere Fehler der Prüfung, auch eine
Fehlerantwort von Valkey, loggt `rate_limit_check_failed` (ERROR) und
reiht die Nachricht an derselben Stelle neu ein; ein bleibender Fehler
endet so über das Delivery-Limit in der DLQ. Siehe
[Zustellsemantik](../explanation/delivery-semantics.md#was-den-ack-abbricht).

## Global, nicht je Worker

Jeder Worker-Prozess nutzt denselben Valkey-Zähler, das Limit gilt also
**global**, unabhängig von `--processes`, `--concurrency` oder der Zahl
der Worker-Pods. Einen Modus je Worker gibt es nicht. Um einen einzelnen
Worker-Prozess zu begrenzen, nutze `--concurrency`.

## Beobachtbarkeit

Der Worker loggt jede Zurückstellung selbst als `task_rate_limited`
(INFO, Logger `swarmq.worker.gates.rate_limit`) mit `retry_after_ms`,
`remaining` und `rate_limit_count`. Mit installiertem `swarmq[metrics]`
und gesetztem `--metrics-port` zählt jede Zurückstellung außerdem in
`swarmq_tasks_total{outcome="rate_limited"}`.

Um im Code zu reagieren, behandle `Signal.TASK_RATE_LIMITED`. Der Worker
löst es aus; unter `swarmq worker` registrierst du den Handler deshalb
in einem `--configure`-Hook (siehe
[Signals](signals.md#einen-handler-registrieren)):

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ
from swarmq.logging import get_logger

log = get_logger("myapp.rate_limit")


def setup(app: SwarmQ) -> None:
    @app.on_signal(Signal.TASK_RATE_LIMITED)
    async def on_rate_limit(
        task_id, task_name, retry_after_ms, remaining, rate_limit_count,
        **kwargs,
    ):
        # Eine Nachricht, die immer wieder abprallt: Das Limit liegt weit unter dem Bedarf.
        if rate_limit_count >= 10:
            log.warning(
                "myapp_rate_limit_bouncing",
                task_id=task_id, task_name=task_name,
                retry_after_ms=retry_after_ms,
                bounce_count=rate_limit_count,
            )
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```

Felder der Nutzlast:

- `retry_after_ms`: Zeit, bis das Limit wieder Platz hat; die Kopie
  wartet so lange plus den Jitter.
- `remaining`: geschätztes Restbudget im aktuellen Fenster.
- `rate_limit_count`: wie oft das Rate-Limit diese Nachricht schon
  zurückgestellt hat (Header `x-swarmq-rate-limit-count`), ein
  Abprall-Zähler für Alarme.

Schließe die Parameterliste des Handlers mit `**kwargs` ab, damit er
weiterläuft, wenn ein Release ein Feld ergänzt.

## Das Limit wählen

Fang etwas unter dem Kontingent des Anbieters an, nicht genau darauf.
Erlaubt der Anbieter 100 Anfragen pro Sekunde, setze
`rate_limit="90/s"`. Der Puffer fängt die Näherung des Fensters ab und
Aufrufe, die dein Code außerhalb von SwarmQ macht.

Das Limit steht fest, sobald das Task-Modul importiert ist. Hängt es von
deinem Tarif ab, lies es beim Bau der `TaskInfo` aus deiner
Konfiguration; ein Neustart übernimmt einen neuen Wert.

## Kombination mit Retries und Locks

- **Mit Retries.** Jeder Retry ist eine neue Zustellung und passiert das
  Rate-Limit erneut, Retries zählen also gegen das Limit.
- **Mit einem Lock.** Zuerst läuft die Rate-Limit-Prüfung; den Lock
  (`lock`, `max_concurrency`) versucht der Worker erst danach. Eine
  begrenzte Nachricht versucht den Lock nie. Umgekehrt hat eine
  Nachricht, die das Rate-Limit passiert und dann den Lock besetzt
  findet, einen Slot verbraucht, obwohl `run()` nicht lief: Sie geht mit
  dem Lock-Backoff zurück (100 ms, verdoppelt bis 60 s) und passiert das
  Rate-Limit bei jeder Rückkehr erneut. Warten viele Kopien auf einen
  Lock, geht ein großer Teil des Budgets an diese Versuche, und es laufen
  weniger Tasks, als das Limit erlaubt. Lass Luft im `rate_limit` oder
  halte die Zahl der Kopien klein, die um einen Lock konkurrieren (etwa
  mit `unique_for`).

## Verifizieren

```bash
swarmq inspect --module myapp.tasks
```

Die Spalte `RATE_LIMIT` zeigt den deklarierten String jedes Tasks. Plane
dann einen Burst, der größer ist als das Limit, und beobachte die
Worker-Logs: Einige Nachrichten sind sofort fertig (`task_succeeded`),
andere loggen `task_rate_limited` mit `retry_after_ms` und laufen nach
der Wartezeit.

## Siehe auch

- [Signals](signals.md): den `TASK_RATE_LIMITED`-Handler registrieren
- [`TaskInfo.rate_limit`-Referenz](../reference/configuration.md#rate-limiting)
- [Locking und Deduplizierung](locking-and-dedup.md): Ausführung
  serialisieren
