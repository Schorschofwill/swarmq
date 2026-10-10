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
| `LoggingConfigError` | `ConfigurationError` | Logging-Konfigurationsdatei (`--log-config`) nicht ladbar oder ungültig |
| `BrokerConnectionError` | `SwarmQError` | RabbitMQ nicht erreichbar |
| `PublishIndeterminate` | `BrokerConnectionError` | Ein Publish hat seine Frist gerissen; ob er ankam, ist unbekannt |
| `SettleFailedAfterPublish` | `BrokerConnectionError` | Worker-intern: Eine Retry- oder Dead-Letter-Kopie ist veröffentlicht, nur das Ack des Originals scheiterte |
| `MessageHeadersTooLarge` | `BrokerConnectionError` | Die Nachrichten-Header passen auch mit gekürzten Texten nicht in die Hälfte von `frame_max`; nichts wurde gesendet |
| `UnroutableMessage` | `BrokerConnectionError` | Der Broker hat einen Publish zurückgegeben: Es gibt keine Queue mit diesem Namen; nichts wurde zugestellt |
| `BackendConnectionError` | `SwarmQError` | Valkey nicht erreichbar |
| `BackendResponseError` | `BackendConnectionError` | Valkey hat eine Operation mit einem Fehler beantwortet (`WRONGTYPE`, Skriptfehler) |
| `TaskNotFoundError` | `SwarmQError` | `schedule()` mit unbekanntem Task-Namen |
| `RedeliveryLimitError` | `SwarmQError` | Wird nie geworfen: Fehlertyp einer DLQ-Kopie, deren Zustellungen bis zum Zustelllimit zurückgegeben wurden |
| `TaskError` | `SwarmQError` | Der Task-Body raiste; bis `get_result()` propagiert |
| `TaskCancelledError` | `TaskError` | `app.cancel(task_id)` griff während der Ausführung |
| `ResultNotReadyError` | `SwarmQError` | `get_result()` lief in Timeout, bevor ein Ergebnis ankam |
| `ResultExpiredError` | `SwarmQError` | Ergebnis existierte, Valkey-TTL hat es verfallen lassen |
| `ResultSerializationError` | `SwarmQError` | Ergebnis ist nicht msgpack-serialisierbar |
| `Retry` | `SwarmQError` | Control Flow — aus Task-Code geraist, um Retry anzufordern |
| `NoRetry` | `SwarmQError` | Control Flow — aus Task-Code geraist, um Retries zu umgehen |
| `SoftTimeoutError` | `SwarmQError` | Task überschritt `soft_timeout`, sollte aufräumen |
| `WorkflowValidationError` | `SwarmQError` | Workflow-Topologie ungültig oder Fan-in-Ergebnisse unvollständig |
| `WorkflowNotFound` | `SwarmQError` | `app.workflow(id)` oder ein Handle-Aufruf fand keinen Workflow-Zustand |
| `WorkflowResultExpired` | `SwarmQError` | Ein Schritt- oder End-Ergebnis des Workflows lief ab, bevor es gebraucht wurde |
| `WorkflowStateConflict` | `SwarmQError` | Worker-intern: Aktualisierung des Workflow-Zustands verlor jede Compare-and-Set-Runde |

Jede Klasse außer `WorkflowStateConflict` und `SettleFailedAfterPublish` lässt sich direkt aus
`swarmq` importieren (`from swarmq import PublishIndeterminate`); alle
liegen außerdem in `swarmq.exceptions`.

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
- Widersprüchliche Logging-Einstellungen, etwa `--log-config` zusammen
  mit `--log-format` oder `--no-configure-logging` (siehe
  [Logging](logging.md#konfigurationsfehler)).
- `scheduler_lock_ttl < 2 × scheduler_heartbeat_interval`.
- `--args` / `--kwargs` für `swarmq schedule add` kein gültiges JSON.

**Recovery.** Config fixen und neustarten. Die Fehlermeldung nennt
das betroffene Feld. `swarmq worker` loggt eine strukturierte Zeile
`worker_config_error` und endet mit Exit `78`:

```text
[error    ] worker_config_error            [swarmq.cli] error='ConfigurationError: SWARMQ_BROKER_URL and SWARMQ_BACKEND_URL must both be set' error_type=ConfigurationError hint='fix the configuration and start again; no restart attempted' source=supervisor
```

`swarmq schedule`, `swarmq dlq` und `swarmq inspect` drucken eine Zeile
auf stderr und enden mit Exit `1`:

```text
swarmq schedule list: SWARMQ_BACKEND_URL is not set; export it or pass it via env, e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0
```

## `LoggingConfigError`

**Wann geraist.** Beim Start von `swarmq worker` (und bei der Prüfung
von `--log-config` vor einem Reload), von
`swarmq.logging.load_log_config` und `configure_logging`, wenn sich die
Logging-Konfigurationsdatei nicht verwenden lässt. Subklasse von
`ConfigurationError`, importierbar aus `swarmq`, `swarmq.exceptions`
und `swarmq.logging`.

**Ursache.** Die Datei aus `--log-config` / `SWARMQ_LOG_CONFIG`:

- lässt sich nicht lesen (fehlt, keine Rechte, kein UTF-8);
- ist kein gültiges JSON, oder ist YAML, ohne dass PyYAML installiert
  ist;
- ist kein Mapping;
- wird von `logging.config.dictConfig` abgelehnt (Tippfehler in einem
  Handler-Schlüssel, unbekannte Klasse, ungültiges Level).

Die Exception trägt die Datei in `path`; der ursprüngliche Fehler steht
in `__cause__` und in der Meldung.

**Recovery.** Datei korrigieren und neu starten. `swarmq worker` endet
mit `78` und loggt genau eine Zeile `worker_config_error` mit `path`,
`error_type` und `error`. Bei einem Reload loggt der laufende Supervisor
stattdessen `log_config_reload_failed` und lässt seine Worker
weiterlaufen.

## `BrokerConnectionError`

**Wann geraist.** Beim Worker-Start, wenn RabbitMQ innerhalb des
Connect-Timeouts nicht erreichbar ist, oder bei einem Reconnect-
Versuch, der das Retry-Budget erschöpft. Außerdem, wenn ein Publish
`SWARMQ_PUBLISH_TIMEOUT_S` überschreitet (Default 120 s) — siehe den
Zustellvorbehalt unten.
Seit 0.10.0 auch, wenn der Worker eine Zustellung bestätigt, neu
einreiht oder an den DLX gibt, nachdem Kanal oder Verbindung verloren
gingen; der Worker loggt `delivery_settle_failed`, und RabbitMQ stellt
die unbestätigte Nachricht neu zu.

!!! warning "Ein Deadline-Fehler heißt nicht, dass nichts gesendet wurde"

    Lautet die Meldung `exceeded the publish deadline`, ist die Zustellung
    **unbestimmt**: Die Frist reißt beim Warten auf die Bestätigung des
    Brokers, er kann die Nachricht also bereits haben. Ein Wiederholversuch
    kann den Task ein zweites Mal ausführen. Das unterscheidet diesen Fall
    von allen anderen hier genannten Ursachen, bei denen nichts gesendet
    wurde.

    Ein mit `unique_for` eingereihter Task **ist** geschützt: swarmq lässt
    die Dedup-Sperre in genau diesem Fall stehen, statt sie freizugeben. Ein
    Wiederholversuch wird also abgewiesen, bis die Sperre abläuft, statt den
    Task ein zweites Mal auszuführen. Jeder andere Publish-Fehler gibt sie
    weiterhin frei, weil dort nichts gesendet wurde.

    Wer die beiden Fälle unterscheiden muss, fängt `PublishIndeterminate` —
    eine Unterklasse, bestehende `except BrokerConnectionError`-Stellen
    funktionieren unverändert.

**Ursache.** Netzwerk-Partition, falsche URL, RabbitMQ läuft nicht,
TLS-Mismatch, VHost/User nicht autorisiert.

**Recovery.** Verifiziere die URL mit `rabbitmqctl status` auf dem
Broker-Host oder `nc -vz <host> 5672`. Im Code propagiert die
Exception aus `async with SwarmQ(...)` — fang sie an deiner
Startup-Grenze ab.

```python
from swarmq import BrokerConnectionError

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

## `BackendResponseError`

**Wann geraist.** Eine Backend-Operation hat Valkey erreicht, Valkey
hat aber mit einem Fehler geantwortet: ein Key vom falschen Typ
(`WRONGTYPE`), ein Fehler im Lua-Skript. Eine Unterklasse von
`BackendConnectionError`, jedes `except BackendConnectionError` fängt
sie also weiter.

**Ursache.** Meist ein Key, den etwas anderes als SwarmQ unter einem
SwarmQ-Namen geschrieben hat (etwa ein Lock-Key `sq:lock:...` als
Hash), oder ein inkompatibler Server.

**Recovery.** Warten hilft nicht, deshalb stellt der Worker eine
Nachricht dafür vor der Ausführung nicht zurück (anders als bei einem
nicht erreichbaren Backend): Sie geht an derselben Stelle in die Queue
zurück und landet, wenn der Fehler bleibt, über das Zustelllimit in
`{queue}.XQ`. Den im Fehler genannten Key prüfen und löschen oder
korrigieren.

Fehlerantworten, die durch Warten heilen, sind kein
`BackendResponseError`, sondern ein einfacher `BackendConnectionError`
und werden wie ein nicht erreichbares Backend behandelt: `OOM`
(maxmemory erreicht), `READONLY` (der Client spricht nach einem
Failover noch mit dem herabgestuften Primary), `LOADING`, `TRYAGAIN`,
`CLUSTERDOWN`, `MASTERDOWN` und `MISCONF` (Persistenz scheitert, etwa
bei voller Platte).

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
from swarmq import TaskCancelledError

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
from swarmq import ResultNotReadyError

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
from swarmq import SoftTimeoutError

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

## `PublishIndeterminate`

**Wann geraist.** Von `schedule()`, `schedule_many()`, `run()` und
`apply()`, wenn ein Publish beim Warten auf das Publisher-Confirm des
Brokers `SWARMQ_PUBLISH_TIMEOUT_S` überschreitet. Subklasse von
`BrokerConnectionError`.

**Ursache.** Broker oder Netz sind nach dem Senden der Nachricht
hängen geblieben; der Broker hat sie vielleicht, vielleicht nicht.

**Recovery.** Die Zustellung als unbekannt behandeln. Nur wiederholen,
wenn der Task idempotent ist oder mit `unique_for` eingereiht wurde
(SwarmQ behält in diesem Fall den Dedup-Claim, eine Wiederholung im
Fenster wird also abgewiesen statt doppelt zu laufen). Siehe die Warnung
unter [`BrokerConnectionError`](#brokerconnectionerror).

```python
from swarmq import BrokerConnectionError, PublishIndeterminate

try:
    await app.schedule("charge", order_id=42)
except PublishIndeterminate:
    # Vielleicht eingereiht -- vor erneutem Senden prüfen.
    ...
except BrokerConnectionError:
    # Nichts gesendet; Wiederholen ist sicher.
    ...
```

## `SettleFailedAfterPublish`

**Wann geworfen.** Nur im Worker, von `retry_later` und `dead_letter`:
Der Broker hat die Retry- oder Dead-Letter-Kopie bestätigt, aber das
Bestätigen des Originals scheiterte (meist, weil der Channel weg ist).
Unterklasse von `BrokerConnectionError`, bestehende Handler fangen sie
also weiter. Sie erreicht nie `schedule()` oder `get_result()`.

**Ursache.** Channel oder Verbindung gingen zwischen dem Publish der
Kopie und dem Ack verloren, etwa nach einem Consumer-Timeout auf
RabbitMQ bis 4.2.

**Recovery.** Im Anwendungscode nichts nötig. Der Broker stellt das
Original erneut zu, während die Kopie schon unterwegs ist; der
[Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
setzt seinen Superseded- bzw. Done-Marker wie nach Erfolg, sodass das
neu zugestellte Original übersprungen wird. Für Tests aus
`swarmq.exceptions` importieren.

## `MessageHeadersTooLarge`

**Wann geworfen.** Von einem Publish (`schedule()`, im Worker auch von
Retry- und Dead-Letter-Kopien), wenn die kodierten Nachrichten-Header
mehr als die Hälfte des ausgehandelten `frame_max` brauchen, auch
nachdem lange Texte auf 8 KiB gekürzt wurden. Unterklasse von
`BrokerConnectionError`. Es wurde nichts gesendet.

**Ursache.** Viele oder große Header mit Nicht-Text-Werten, etwa eine
Middleware, die Hunderte Header ergänzt. Die eigenen Diagnose-Header von
SwarmQ sind begrenzt und lösen den Fehler nicht aus.

**Recovery.** Nicht mit denselben Headern wiederholen; sie scheitern
wieder. Die Header verkleinern. Anders als bei `PublishIndeterminate`
wird eine `unique_for`-Sperre freigegeben, weil nichts beim Broker
ankam.

## `UnroutableMessage`

**Wann geworfen.** Von einem Publish (`send()`, `schedule()`,
Workflow-Schritte, im Worker auch Retry- und Dead-Letter-Kopien), wenn
der Broker die Nachricht zurückgibt, weil es keine Queue mit diesem
Namen gibt. Unterklasse von `BrokerConnectionError`. Sofort geworfen,
ohne Wiederholung; nichts wurde zugestellt. Vor 0.12 bestätigte der
Broker einen solchen Publish und verwarf die Nachricht still.

**Ursache.** Die Queue wurde nie deklariert oder wurde gelöscht. Worker
deklarieren die Queues, die sie konsumieren; meist startet also ein
Producer vor dem ersten Worker einer neuen Queue, `queue=` ist
vertippt, oder ein Task mit `TaskInfo(priority="high"|"low")` hat
Worker ohne `--priorities` (kein `{queue}.high`/`.low`).

**Recovery.** Zuerst den Worker der Queue starten, dann erneut
veröffentlichen. Eine `unique_for`-Sperre wird freigegeben, weil nichts
beim Broker ankam. Der Worker deklariert seine eigene
`{queue}.DQ`/`.XQ` einmal selbst neu; der Scheduler-Leader schreibt
einen verlorenen Cron-Fire fest und versucht einen verzögerten Task
alle 60 s erneut, bis `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`
([fehlende Queues](../guides/scheduling-and-retries.md#fehlende-queues)).

## `RedeliveryLimitError`

**Wann geworfen.** Nie; der Name steht nur als `error_type` in
`task_sent_to_dlq` und am Anfang von `x-swarmq-traceback` der
DLQ-Nachricht. Der Worker gibt eine Zustellung an die Queue zurück
(`nack(requeue=True)`), wenn er gerade nicht über sie entscheiden kann,
etwa weil Valkey mit einem Fehler antwortet oder ein Dead-Letter-Publish
scheitert. Auf RabbitMQ 4.3 erhöht eine solche Rückgabe nur
`x-acquired-count`, nicht `x-delivery-count`. Erreicht
`x-acquired-count` den Wert `SWARMQ_DELIVERY_LIMIT - 1`, schickt der
Worker die Zustellung mit diesem Fehlertyp in die DLQ. Hat zuerst
`x-delivery-count` die Grenze erreicht (hängender Worker, geschlossener
Channel), bleibt der Typ `WorkerHungError`.

**Ursache.** Ein Fehler, der über alle Zustellungen anhält: Valkey
antwortet weiter mit einem Fehler, die Dead-Letter-Queue nimmt keinen
Publish an, oder es wird kein Rechen-Slot frei.

**Recovery.** Die Ursache beheben, die das Worker-Log vor dem
Dead-Letter-Eintrag nennt, dann die Nachricht mit `swarmq dlq retry`
erneut einreihen.

## `WorkflowValidationError`

**Wann geraist.** Beim Bauen eines Workflows mit `chain`, `group` oder
`chord`, bevor irgendetwas eingereiht wird (ein per Name mit `sig()`
gebauter Schritt wird erst zur Laufzeit auf `FIRE_AND_FORGET` geprüft).
Außerdem zur Laufzeit im Worker, wenn eine Group oder ein Chord ihre
Ergebnisliste nicht vollständig zusammenbekommt; der Workflow wird dann
als gescheitert markiert und `handle.get()` wirft `TaskError`.

**Ursache.** Verschachtelung tiefer als `max_workflow_depth` (10), eine
Group mit mehr als `max_group_size` (1000) Mitgliedern, eine leere
Chain oder Group, ein Chord-Header, der kein `group(...)` ist, oder ein
`FIRE_AND_FORGET`-Task als Schritt.

**Recovery.** Topologie abflachen, große Groups aufteilen oder den Task
auf `DeliveryMode.AT_LEAST_ONCE` umstellen. Siehe
[Workflow-Primitive](api.md#workflow-primitive).

## `WorkflowNotFound`

**Wann geraist.** `app.workflow(workflow_id)` und die
`WorkflowHandle`-Methoden `status()`, `children()` und `get()`, wenn es
zur ID keinen Workflow-Zustand gibt.

**Ursache.** Die ID ist falsch, der Workflow wurde nie gestartet, oder
sein Zustand ist abgelaufen (er lebt `result_ttl` Sekunden).

**Recovery.** ID prüfen. Für lange Workflows `result_ttl`
(`SWARMQ_RESULT_TTL`) erhöhen, damit der Zustand den Lauf und die Zeit
bis zum Abholen des Ergebnisses überlebt.

## `WorkflowResultExpired`

**Wann geraist.** Von `handle.get()`, wenn der Workflow fertig ist,
sein Endergebnis aber schon abgelaufen ist; im Worker, wenn ein
Schritt-Ergebnis, das der nächste Schritt oder das Fan-in braucht,
abgelaufen ist — der Workflow scheitert dann.

**Ursache.** Eine Ergebnis-TTL, die kürzer ist als die Laufzeit des
Workflows plus die Zeit bis zum Abholen.

**Recovery.** `result_ttl` erhöhen (global oder je Task über
`TaskInfo.result_ttl`) und Workflow-Ergebnisse zeitnah abholen.

## `WorkflowStateConflict`

**Wann geraist.** Nur im Worker, beim Voranbringen eines Workflows: Das
Compare-and-Set auf den Workflow-Zustand hat alle 25 Runden gegen
gleichzeitige Schreiber verloren. Erreicht nie `schedule()` oder
`handle.get()`.

**Ursache.** Mehr Zweige eines Workflows, die im selben Moment fertig
werden, als die Konfliktgrenze erlaubt.

**Recovery.** Im Anwendungscode keine nötig. Der Worker loggt
`workflow_advance_backend_error` und stellt die Schritt-Nachricht neu zu
(`at_least_once`); die Neuzustellung bringt den Workflow voran,
deterministische Schritt-IDs halten das idempotent. Für Tests aus
`swarmq.exceptions` importieren.

## Außerhalb der Hierarchie: `ForkSourceRejected`

`swarmq.supervision.ForkSourceRejected` ist ein `RuntimeError`, kein
`SwarmQError`; `except SwarmQError` fängt ihn also nicht.

**Wann geraist.** Von `Worker.start()` (und damit `app.start_worker()`)
eines eingebetteten CPU-Pools, nachdem der Worker geordnet
heruntergefahren ist, weil sein Pool den Forkserver verworfen hat: Der
Forkserver wurde still neu gestartet und lief danach mit mehr als einem
Thread, ein Fork aus ihm ist also nicht mehr sicher.

**Ursache.** Ein Thread, der beim Import im Forkserver gestartet wurde,
meist eine BLAS/OpenMP-Berechnung, ein logging-`QueueListener` deiner
App oder die Opt-in-Log-Queue. Der Worker loggt
`forkserver_restarted_multithreaded` mit den Thread-Namen und einem
Hinweis, danach `compute_fork_source_rejected`.

**Recovery.** Den Worker-Prozess neu starten; ein neuer Prozess bringt
einen frischen Forkserver. Unter `swarmq worker` geschieht das von
selbst: Der Worker-Subprozess endet mit Code `71`, und der Supervisor
startet ihn im Restart-Budget neu (`worker_fork_source_rejected`).
Siehe [CPU-Modus](../guides/cpu-mode.md).

## Siehe auch

- [API-Referenz](api.md) — die Methoden, die diese Exceptions raisen
- [Logging](logging.md) — Logging-Einstellungen und `--log-config`
- [Scheduling und Retries](../guides/scheduling-and-retries.md) —
  wie Retry-Policy mit `Retry` und `NoRetry` interagiert
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) —
  Operator-Werkzeuge zur Wiederherstellung DLQ'ter Tasks
