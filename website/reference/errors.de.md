# Fehlerkatalog

Jede Exception, die SwarmQ raist, erbt von `SwarmQError`. Die
Tabelle unten ist der Sammelfang; die Abschnitte danach sind die
Per-Klasse-Einträge mit **Wann geraist**, **Ursache** und
**Recovery**.

```python
from swarmq import SwarmQError

try:
    result = await app.run("send_email", to="a@example.com")
except SwarmQError as exc:
    # Jeder von SwarmQ ausgehende Fehler
    ...
```

## Hierarchie im Überblick

| Exception | Parent | Wann du sie siehst |
|---|---|---|
| `SwarmQError` | `Exception` | Wurzel der Hierarchie |
| `ConfigurationError` | `SwarmQError` | Fehlkonfiguration beim Worker-Start, CLI-Args, fehlende Env-Vars |
| `BrokerConnectionError` | `SwarmQError` | RabbitMQ nicht erreichbar |
| `BackendConnectionError` | `SwarmQError` | Valkey nicht erreichbar |
| `TaskNotFoundError` | `SwarmQError` | `schedule()` mit unbekanntem Task-Namen |
| `TaskError` | `SwarmQError` | Der Task-Body raiste; bis `get_result()` propagiert |
| `TaskCancelledError` | `TaskError` | `app.cancel(task_id)` griff während der Ausführung |
| `ResultNotReadyError` | `SwarmQError` | `get_result()` lief in Timeout, bevor ein Ergebnis ankam |
| `ResultExpiredError` | `SwarmQError` | Ergebnis existierte, Valkey-TTL hat es verfallen lassen |
| `ResultSerializationError` | `SwarmQError` | Ergebnis ist nicht msgpack-serialisierbar |
| `Retry` | `SwarmQError` | Control Flow — aus Task-Code geraist, um Retry anzufordern |
| `NoRetry` | `SwarmQError` | Control Flow — aus Task-Code geraist, um Retries zu umgehen |
| `SoftTimeoutError` | `SwarmQError` | Task überschritt `soft_timeout`, sollte aufräumen |

## `SwarmQError`

**Wann geraist.** Nie direkt. Die Basisklasse für jede Exception,
die die Library wirft. Sie abzufangen ist der sichere Weg, jedes
SwarmQ-Versagen zu schlucken, ohne auch unbeteiligte Exceptions
einzufangen.

**Recovery.** Per Subklasse entscheiden; siehe unten.

## `ConfigurationError`

**Wann geraist.** Beim Worker-Start oder bei einem CLI-Aufruf, wenn
die Library oder der CLI-Parser einen fehlerhaften Input erkennt.

**Ursache.** Typische Quellen:

- Fehlende Env-Var (`SWARMQ_BROKER_URL`, `SWARMQ_BACKEND_URL`).
- Ungültiger Port (`SWARMQ_METRICS_PORT=not-a-number`).
- `log_level` außerhalb `{DEBUG, INFO, WARNING, ERROR, CRITICAL}`.
- `scheduler_lock_ttl < 2 × scheduler_heartbeat_interval`.
- `--args` / `--kwargs` für `swarmq schedule add` kein gültiges JSON.
- `swarmq dlq retry` mit `task_id` und `--all` zugleich.

**Recovery.** Config fixen und neustarten. Die Fehlermeldung nennt
das betroffene Feld.

```text
swarmq worker: SWARMQ_BROKER_URL is not set; export it or pass it
via env, e.g. SWARMQ_BROKER_URL=amqp://guest:guest@localhost:5672/
```

## `BrokerConnectionError`

**Wann geraist.** Beim Worker-Start, wenn RabbitMQ innerhalb des
Connect-Timeouts nicht erreichbar ist, oder bei einem Reconnect-
Versuch, der das Retry-Budget erschöpft. Außerdem, wenn ein Publish
`SWARMQ_PUBLISH_TIMEOUT_S` überschreitet (Default 120 s) — siehe den
Zustellvorbehalt unten.

!!! warning "Ein Deadline-Fehler heißt nicht, dass nichts gesendet wurde"

    Lautet die Meldung `exceeded the publish deadline`, ist die Zustellung
    **unbestimmt**: Die Frist reißt beim Warten auf die Bestätigung des
    Brokers, er kann die Nachricht also bereits haben. Ein Wiederholversuch
    kann den Task ein zweites Mal ausführen. Das unterscheidet diesen Fall
    von allen anderen hier genannten Ursachen, bei denen nichts gesendet
    wurde.

    Auch ein mit `unique_for` eingereihter Task ist nicht geschützt: swarmq
    gibt die Dedup-Sperre frei, wenn der Publish wirft, der Wiederholversuch
    läuft also nicht dagegen. Wenn dir Höchstens-einmal wichtiger ist als
    der Wiederholversuch, behandle diesen Fall gesondert und gleiche ab,
    statt blind erneut einzureihen.

**Ursache.** Netzwerk-Partition, falsche URL, RabbitMQ läuft nicht,
TLS-Mismatch, VHost/User nicht autorisiert.

**Recovery.** Verifiziere die URL mit `rabbitmqctl status` auf dem
Broker-Host oder `nc -vz <host> 5672`. Im Code propagiert die
Exception aus `async with SwarmQ(...)` — fang sie an deiner
Startup-Grenze ab.

```python
from swarmq.exceptions import BrokerConnectionError

try:
    async with SwarmQ(broker, backend) as app:
        await app.start_worker()
except BrokerConnectionError as exc:
    # Loggen, alarmieren, mit Nicht-Null exiten.
    ...
```

## `BackendConnectionError`

**Wann geraist.** Beim Worker-Start, wenn Valkey nicht erreichbar
ist, oder zur Laufzeit, wenn eine Backend-Operation
`backend_op_timeout` überschreitet.

**Ursache.** Valkey down, URL falsch, `AUTH`-Passwort-Mismatch,
TLS-Mismatch.

**Recovery.** `valkey-cli -u $SWARMQ_BACKEND_URL ping` sollte
`PONG` drucken. Laufzeit-Connection-Losses werden vom valkey-py-
Reconnect-Loop automatisch erholt — diese Exception feuert nur,
wenn das Retry-Fenster erschöpft ist.

## `TaskNotFoundError`

**Wann geraist.** `app.schedule("name", ...)` mit einem Namen, den
keine registrierte `Task`-Subklasse in ihrem `TaskInfo` deklariert
hat.

**Ursache.** Typo, Task-Modul nicht importiert, Task nicht von
`swarmq.Task` abgeleitet, oder Modul importiert, aber die Klasse
darin nicht evaluiert (z. B. hinter einer Bedingung).

**Recovery.**

```bash
swarmq inspect --module myapp.tasks
```

listet jeden registrierten Task. Fehlt dein Task, wird sein Modul
beim Worker-Load nicht importiert, oder die Klasse ist durch einen
`if TYPE_CHECKING:`-Block abgeschirmt.

## `TaskError`

**Wann geraist.** `await app.get_result(task_id)`, wenn der
Task-Body eine uncaught Exception raiste und sein Retry-Budget
erschöpft (oder nie genutzt) hat.

**Ursache.** Jede Exception, die deine `run()`-Methode hat
entweichen lassen. Die Original-Exception-Klasse geht über den
Draht verloren; die Fehlermeldung trägt den String, den der
Worker gespeichert hat.

**Recovery.** Lies die Worker-Logs — sie tragen den vollständigen
Traceback. Dann fixe den Task-Body oder erweitere deine
`retry_exceptions`, damit die transiente Klasse retry't statt
DLQ't wird.

## `TaskCancelledError`

**Wann geraist.** `get_result()` beobachtet ein Cancel-Flag im
gespeicherten Ergebnis. Außerdem im Task-Body, wenn der Cancel-
Monitor des Workers zuschlägt.

**Ursache.** Jemand — der Client, ein Operator, ein anderer
Service — rief `app.cancel(task_id)` auf, während der Task
geplant oder laufend war.

**Recovery.** War der Cancel gewollt, fang ihn ab und mach weiter.
War er versehentlich, prüfe die TTL deines Cancel-Flags — siehe
[`app.cancel()`](api.md#cancel) für den Stale-Flag-Caveat.

```python
from swarmq.exceptions import TaskCancelledError

try:
    result = await app.get_result(task_id, timeout=60)
except TaskCancelledError:
    # Task wurde abgebrochen, bevor er ein Ergebnis produzierte.
    ...
```

## `ResultNotReadyError`

**Wann geraist.** `get_result(task_id, timeout=T)` nach T Sekunden
ohne Ergebnis in Valkey.

**Ursache.** Der Task läuft noch, der Worker starb, der Task landete
in der DLQ ohne Ergebnis zu speichern, oder die `task_id` ist falsch.

**Recovery.** Timeout erhöhen, Worker-Logs prüfen oder — falls der
Task in Retry-Loops hängt — die DLQ inspizieren.

```python
from swarmq.exceptions import ResultNotReadyError

try:
    result = await app.get_result(task_id, timeout=5)
except ResultNotReadyError:
    # Noch pending; später erneut prüfen.
    ...
```

## `ResultExpiredError`

**Wann geraist.** `get_result()` findet den Result-Key weg, obwohl
ein Metadaten-Marker noch anzeigt, dass der Task beendet war. In
v0.x ist das selten — das Backend raist den „weg"-Fall meist als
`ResultNotReadyError` — und die Klasse bleibt als stabiler Haken
für ein künftiges Release, das beides trennt.

**Recovery.** `result_ttl` (`SWARMQ_RESULT_TTL`) erhöhen oder das
Ergebnis früher abholen.

## `ResultSerializationError`

**Wann geraist.** In der Store-Result-Phase des Workers — der
Rückgabewert von `run()` ist nicht msgpack-kompatibel.

**Ursache.** Du gibst eine Klasse zurück, die msgpack nicht zu
kodieren weiß. Häufige Kandidaten: offene File-Handles, SQLAlchemy-
ORM-Objekte, `asyncio.Task`-Instanzen, Custom Classes ohne
Konvertierungs-Hook.

**Recovery.** Vor dem Return in `dict`, `list`, `str`, `int`,
`float`, `bool`, `bytes` oder Kombinationen davon umwandeln.

```python
class FetchUser(Task):
    task_info = TaskInfo(name="fetch_user", queues=["default"])

    async def run(self, user_id: int) -> dict:
        user = await db.fetch_one(user_id)
        # dict zurückgeben, kein ORM-Row-Objekt.
        return {"id": user.id, "email": user.email}
```

## `Retry`

**Wann geraist.** Aus deinem eigenen Task-Code, um einen Retry mit
spezifischer Verzögerung zu erzwingen.

**Ursache.** Kein Fehler — es ist ein Control-Flow-Signal. Das
Raisen sagt dem Worker „dieser Versuch scheiterte, retry in `delay`
Sekunden".

**Recovery.** Keine Recovery nötig; so nutzt du ihn:

```python
from swarmq import Retry

class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email", queues=["default"], max_retries=5,
    )

    async def run(self, to: str, subject: str) -> None:
        response = await smtp.send(to, subject)
        if response.status == 429:
            raise Retry(delay=60.0)  # In einer Minute retry
```

Ist `delay` `None`, wendet der Worker die konfigurierte Backoff-
Strategie an (`retry_backoff`, `retry_base_delay`).

## `NoRetry`

**Wann geraist.** Aus deinem eigenen Task-Code, um das Retry-Budget
zu überspringen und direkt in die DLQ zu gehen.

**Ursache.** Kein Fehler. Raise sie, wenn das Scheitern permanent
ist und Retries Budget verschwenden würden.

**Recovery.** Wie bei `Retry` ist das die beabsichtigte Nutzung:

```python
from swarmq import NoRetry

class ChargeCard(Task):
    task_info = TaskInfo(name="charge", queues=["payments"])

    async def run(self, card_id: str, amount: int) -> dict:
        try:
            return await stripe.charge(card_id, amount)
        except stripe.CardDeclinedError as exc:
            # Kein Retry wird eine abgelehnte Karte rehabilitieren.
            raise NoRetry(str(exc))
```

Der Task landet sofort in `{queue}.XQ`. Erhole ihn mit
`swarmq dlq retry <task_id>`, nachdem du den Input korrigiert
hast, oder `swarmq dlq purge --queue payments`, um den Backlog zu
leeren.

## `SoftTimeoutError`

**Wann geraist.** Der Task-Body läuft noch, wenn `soft_timeout`
abläuft. Der Worker bricht den Task mit dieser Klasse als Ursache
ab, damit der Task vor `hard_timeout` noch graceful aufräumen kann.

**Ursache.** Der Task braucht länger als `TaskInfo(soft_timeout=N)`
erlaubte.

**Recovery.** Den Task schneller machen, `soft_timeout` erhöhen
oder abfangen und checkpointen:

```python
from swarmq.exceptions import SoftTimeoutError

class LongJob(Task):
    task_info = TaskInfo(
        name="long_job", queues=["default"],
        soft_timeout=60, hard_timeout=120,
    )

    async def run(self) -> None:
        try:
            await process_items()
        except SoftTimeoutError:
            # Was fertig ist, committen, dann den Worker finalisieren lassen.
            await checkpoint()
            raise
```

## Siehe auch

- [API-Referenz](api.md) — die Methoden, die diese Exceptions raisen
- [Scheduling und Retries](../guides/scheduling-and-retries.md) —
  wie Retry-Policy mit `Retry` und `NoRetry` interagiert
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) —
  Operator-Werkzeuge zur Wiederherstellung DLQ'ter Tasks
