# Fehlerkatalog

Jede Exception, die SwarmQ auslöst, erbt von `SwarmQError`, mit zwei
dokumentierten Ausnahmen [außerhalb der Hierarchie](#auerhalb-der-hierarchie-forksourcerejected).
Die Tabelle unten gibt den Überblick; die Abschnitte danach beschreiben
jede Klasse mit **Wann ausgelöst**, **Ursache** und **Behebung**.

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
| `QueueArgumentsMismatch` | `ConfigurationError` | Eine vorhandene Queue hat andere Argumente als die Konfiguration, die swarmq nicht übernehmen kann (etwa ein geänderter `SWARMQ_QUEUE_TYPE`) |
| `BrokerConnectionError` | `SwarmQError` | RabbitMQ nicht erreichbar |
| `PublishIndeterminate` | `BrokerConnectionError` | Ein Publish ist gesendet, hat aber vor dem Confirm seine Frist gerissen oder die Verbindung verloren; ob er ankam, ist unbekannt |
| `SettleFailedAfterPublish` | `BrokerConnectionError` | Worker-intern: Eine Retry-, Dead-Letter- oder Rückstellungs-Kopie ist veröffentlicht, nur das Ack des Originals scheiterte |
| `MessageHeadersTooLarge` | `BrokerConnectionError` | Die Nachrichten-Header passen auch mit gekürzten Texten nicht in die Hälfte von `frame_max`; nichts wurde gesendet |
| `UnroutableMessage` | `BrokerConnectionError` | Der Broker hat einen Publish zurückgegeben: Es gibt keine Queue mit diesem Namen; nichts wurde zugestellt |
| `BrokerPublishBlocked` | `BrokerConnectionError` | Der Broker hat die Publish-Verbindung über die ganze Publish-Frist blockiert (Ressourcenalarm); nichts wurde gesendet |
| `PublishRejected` | `BrokerConnectionError` | Der Broker hat den Publish an diese Queue abgelehnt (negatives Publisher-Confirm im letzten von 5 Versuchen); die Verbindung ist gesund |
| `BackendConnectionError` | `SwarmQError` | Valkey nicht erreichbar |
| `BackendResponseError` | `BackendConnectionError` | Valkey hat eine Operation mit einem Fehler beantwortet (`WRONGTYPE`, Skriptfehler) |
| `TaskNotFoundError` | `SwarmQError` | `schedule()`, `schedule_many()` oder `run()` mit einem Task-Namen, den die App nicht registriert; außerdem Fehlertyp einer DLQ-Kopie für einen Task, den kein Worker kennt |
| `LockWaitExceeded` | `SwarmQError` | Wird nie geworfen: Fehlertyp einer DLQ-Kopie, deren Wartezeit auf einen Task-Lock endete (hängender Halter oder `SWARMQ_LOCK_WAIT_MAX_S`) |
| `RedeliveryLimitError` | `SwarmQError` | Wird nie geworfen: Fehlertyp einer DLQ-Kopie, deren Zustellungen bis zum Zustelllimit zurückgegeben wurden |
| `TaskError` | `SwarmQError` | Der Task endete mit einem Fehlerergebnis; ausgelöst von `get_result()`, `run()` und `handle.get()` eines gescheiterten Workflows |
| `TaskCancelledError` | `TaskError` | `get_result()` oder `app.run()` für einen per `app.cancel(task_id)` abgebrochenen Task; der Task selbst sieht `asyncio.CancelledError` |
| `ResultNotReadyError` | `SwarmQError` | `get_result()`, `get_results()`, `run()` oder `handle.get()` lief in den Timeout, bevor ein Ergebnis ankam |
| `ResultExpiredError` | `SwarmQError` | Derzeit nie geworfen (reserviert); ein abgelaufenes Ergebnis wirft `ResultNotReadyError` |
| `ResultSerializationError` | `SwarmQError` | Argumente nicht als JSON kodierbar, oder ein gespeichertes Ergebnis nicht dekodierbar |
| `Retry` | `SwarmQError` | Kontrollfluss — vom Task-Code ausgelöst, um einen Retry anzufordern |
| `NoRetry` | `SwarmQError` | Kontrollfluss — vom Task-Code ausgelöst, um Retries zu umgehen |
| `SoftTimeoutError` | `SwarmQError` | Task überschritt `soft_timeout`; `run()` wurde abgebrochen (im Task kommt `CancelledError` an) |
| `WorkflowValidationError` | `SwarmQError` | Workflow-Topologie ungültig oder Fan-in-Ergebnisse unvollständig |
| `WorkflowNotFound` | `SwarmQError` | `app.workflow(id)` oder ein Handle-Aufruf fand keinen Workflow-Zustand |
| `WorkflowResultExpired` | `SwarmQError` | Ein Schritt- oder End-Ergebnis des Workflows lief ab, bevor es gebraucht wurde |
| `WorkflowStateConflict` | `SwarmQError` | Worker-intern: Aktualisierung des Workflow-Zustands verlor jede Compare-and-Set-Runde |

Jede Klasse außer `WorkflowStateConflict` und `SettleFailedAfterPublish` lässt sich direkt aus
`swarmq` importieren (`from swarmq import PublishIndeterminate`); alle
liegen außerdem in `swarmq.exceptions`.

## `SwarmQError`

**Wann ausgelöst.** Nie direkt. Die Basisklasse jeder Exception, die die
Bibliothek wirft. Wer sie fängt, fängt jeden Fehler aus SwarmQ, ohne
unbeteiligte Exceptions mitzufangen.

**Behebung.** Je Unterklasse entscheiden; siehe unten.

## `ConfigurationError`

**Wann ausgelöst.** Beim Worker-Start oder bei einem CLI-Aufruf, wenn
die Bibliothek oder der CLI-Parser eine fehlerhafte Eingabe erkennt.

**Ursache.** Typische Quellen:

- Fehlende Env-Var (`SWARMQ_BROKER_URL`, `SWARMQ_BACKEND_URL`).
- Ungültiger Port (`SWARMQ_METRICS_PORT=not-a-number`).
- `log_level` außerhalb `{DEBUG, INFO, WARNING, ERROR, CRITICAL}`.
- Widersprüchliche Logging-Einstellungen, etwa `--log-config` zusammen
  mit `--log-format` oder `--no-configure-logging` (siehe
  [Logging](logging.md#konfigurationsfehler)).
- `scheduler_lock_ttl < 2 × scheduler_heartbeat_interval`.
- `--args` / `--kwargs` für `swarmq schedule add` kein gültiges JSON.

**Behebung.** Konfiguration korrigieren und neu starten. Die
Fehlermeldung nennt das betroffene Feld. `swarmq worker` unternimmt
genau einen Versuch, loggt eine strukturierte Zeile
`worker_config_error` und endet mit Exit `78` (`EX_CONFIG`); der
Supervisor startet ihn nicht neu. Ein `ImportError` oder `SyntaxError`
beim Import des Task-Moduls endet genauso; andere Importfehler und
Verbindungsfehler zu Broker oder Backend enden mit Exit `1`
(E-057):

```text
[error    ] worker_config_error            [swarmq.cli] error='ConfigurationError: SWARMQ_BROKER_URL and SWARMQ_BACKEND_URL must both be set' error_type=ConfigurationError hint='fix the configuration and start again; no restart attempted' source=supervisor
```

`swarmq schedule`, `swarmq dlq` und `swarmq inspect` drucken eine Zeile
auf stderr und enden mit Exit `1`:

```text
swarmq schedule list: SWARMQ_BACKEND_URL is not set; export it or pass it via env, e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0
```

## `LoggingConfigError`

**Wann ausgelöst.** Beim Start von `swarmq worker` (und bei der Prüfung
von `--log-config` vor einem Reload), von
`swarmq.logging.load_log_config` und `configure_logging`, wenn sich die
Logging-Konfigurationsdatei nicht verwenden lässt. Unterklasse von
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

**Behebung.** Datei korrigieren und neu starten. `swarmq worker` endet
mit `78` und loggt genau eine Zeile `worker_config_error` mit `path`,
`error_type` und `error`. Bei einem Reload loggt der laufende Supervisor
stattdessen `log_config_reload_failed` und lässt seine Worker
weiterlaufen.

## `QueueArgumentsMismatch`

**Wann ausgelöst.** Wenn der Worker beim Start seine Queues deklariert
und eine vorhandene Queue ein Argument hat, das von der Konfiguration
abweicht und das swarmq nicht übernimmt. Unterklasse von
`ConfigurationError`, importierbar aus `swarmq` und `swarmq.exceptions`.
Die Meldung und die Attribute `queue`, `argument`, `declared` und
`existing` nennen den Konflikt.

**Ursache.** RabbitMQ ändert die Argumente einer bestehenden Queue nie.
Typisch: `SWARMQ_QUEUE_TYPE` wurde für schon vorhandene Queues geändert,
oder `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=at-least-once` trifft auf eine
Queue ohne at-least-once-Dead-Lettering
(`x-dead-letter-strategy=at-least-once` zusammen mit
`x-overflow=reject-publish`). Ein abweichendes
`x-delivery-limit`, `x-dead-letter-strategy` oder `x-overflow` wird
übernommen und löst sie nicht aus
([vorhandene Queue-Argumente](../explanation/delivery-semantics.md#vorhandene-queue-argumente)).

**Behebung.** Ein Neustart hilft nicht; `swarmq worker` endet deshalb
mit `78`, und der Supervisor startet ihn nicht neu. Die vorherige
Einstellung wiederherstellen oder die
[Queue migrieren](../explanation/delivery-semantics.md#queue-argumente-andern).

## `BrokerConnectionError`

**Wann ausgelöst.** Beim Worker-Start, wenn RabbitMQ innerhalb des
Connect-Timeouts nicht erreichbar ist, oder bei einem
Reconnect-Versuch, der das Retry-Budget erschöpft. Außerdem, wenn ein Publish
`SWARMQ_PUBLISH_TIMEOUT_S` überschreitet (Default 120 s) — siehe den
Zustellvorbehalt unten.
Seit 0.10.0 auch, wenn der Worker eine Zustellung bestätigt, neu
einreiht oder an den DLX gibt, nachdem Kanal oder Verbindung verloren
gingen; der Worker loggt `delivery_settle_failed`, und RabbitMQ stellt
die unbestätigte Nachricht neu zu.
Seit 0.14.1 auch, wenn die Verbindung abreißt, während der Worker seine
Queues anmeldet; der ursprüngliche Fehler steht in `__cause__`.

!!! warning "Ein Deadline-Fehler heißt nicht, dass nichts gesendet wurde"

    Lautet die Meldung `exceeded the publish deadline` oder `lost the
    connection while waiting for the broker's confirm`, ist die Zustellung
    **unbestimmt**: Die Nachricht ist gesendet, und beim Warten auf die
    Bestätigung des Brokers ist die Frist gerissen oder die Verbindung
    abgebrochen; er kann die Nachricht also bereits haben. Ein
    Wiederholversuch kann den Task ein zweites Mal ausführen. Das
    unterscheidet diesen Fall von allen anderen hier genannten Ursachen,
    bei denen nichts gesendet wurde. swarmq sendet eine solche Nachricht nie
    selbst erneut; reißt die Verbindung, bevor die Nachricht auf den Socket
    geschrieben ist, wiederholt swarmq intern und meldet, wenn das nicht
    hilft, ein einfaches `BrokerConnectionError`.

    Ein mit `unique_for` eingereihter Task **ist** geschützt: swarmq lässt
    die Dedup-Sperre in genau diesem Fall stehen, statt sie freizugeben. Ein
    Wiederholversuch wird also abgewiesen, bis die Sperre abläuft, statt den
    Task ein zweites Mal auszuführen. Jeder andere Publish-Fehler gibt sie
    weiterhin frei, weil dort nichts gesendet wurde.

    Wer die beiden Fälle unterscheiden muss, fängt `PublishIndeterminate` —
    eine Unterklasse, bestehende `except BrokerConnectionError`-Stellen
    funktionieren unverändert.

**Ursache.** Netzpartition, falsche URL, RabbitMQ läuft nicht,
TLS passt nicht, VHost oder User nicht berechtigt.

**Behebung.** Die URL mit `rabbitmqctl status` auf dem Broker-Host
oder `nc -vz <host> 5672` prüfen. Im Code kommt die Exception aus
`async with SwarmQ(...)` heraus — an der Startgrenze der Anwendung
abfangen.

```python
from swarmq import BrokerConnectionError

try:
    async with SwarmQ(broker, backend) as app:
        await app.start_worker()
except BrokerConnectionError as exc:
    # Loggen, alarmieren, mit Exit-Code ungleich 0 beenden.
    ...
```

## `BackendConnectionError`

**Wann ausgelöst.** Beim Worker-Start, wenn Valkey nicht erreichbar
ist, oder zur Laufzeit, wenn eine Backend-Operation
`backend_op_timeout` überschreitet. `ValkeyBackend.connect()`
(Worker-Start, CLI-Befehle, Producer) wirft sie für jeden
Treiberfehler, auch für einen Zeitablauf und eine Fehlerantwort auf
seinen `PING` (ACL `NOPERM` als `BackendResponseError`), mit der
Meldung `Valkey connect failed: …`. Der `PING` läuft unter einer
Gesamtfrist, [`SWARMQ_BACKEND_CONNECT_TIMEOUT_S`](configuration.md#worker-verhalten)
(Vorgabe 30 s); läuft sie ab, lautet die Meldung
`Valkey connect timed out after …s`. Gegen einen Host, der Pakete
verwirft, wiederholt `connect()` den `PING` bis zu dieser Frist (seit
0.14; vorher endete es nach rund 33 s mit `Valkey connect failed:
Timeout connecting to server`); ein Port, der Verbindungen abweist,
endet nach rund 3,1 s mit `Valkey connect failed: …`. Ein gescheiterter `connect()`
schließt den Client, den er gebaut hat, und hinterlässt kein halb
verbundenes Backend; ein späterer `connect()` erreicht Valkey erneut
und wiederholt die Eviction-Prüfung. Wird die Verbindung angenommen,
aber im Handshake getrennt (Proxy oder Sidecar vor einem neu
startenden Valkey), baut der Client sie im selben Fenster wie eine
Befehlswiederholung neu auf, rund 3,1 s ab dem ersten Fehlschlag, bevor
der Befehl gesendet wird. Ein hängender Handshake endet nach dem
Socket-Timeout (10 s) ohne Wiederholung.

**Ursache.** Valkey läuft nicht, URL falsch, `AUTH`-Passwort falsch,
TLS passt nicht.

**Behebung.** `valkey-cli -u $SWARMQ_BACKEND_URL ping` sollte `PONG`
ausgeben. Verbindungsverluste zur Laufzeit fängt die
Reconnect-Schleife von valkey-py automatisch ab — diese Exception
kommt erst, wenn das Wiederholungsfenster erschöpft ist.

## `BackendResponseError`

**Wann ausgelöst.** Eine Backend-Operation hat Valkey erreicht, Valkey
hat aber mit einem Fehler geantwortet: ein Key vom falschen Typ
(`WRONGTYPE`), ein Fehler im Lua-Skript. Eine Unterklasse von
`BackendConnectionError`, jedes `except BackendConnectionError` fängt
sie also weiter.

**Ursache.** Meist ein Key, den etwas anderes als SwarmQ unter einem
SwarmQ-Namen geschrieben hat (etwa ein Lock-Key `sq:lock:...` als
Hash), oder ein inkompatibler Server.

**Behebung.** Warten hilft nicht, deshalb stellt der Worker eine
Nachricht dafür vor der Ausführung nicht zurück (anders als bei einem
nicht erreichbaren Backend): Sie geht an derselben Stelle in die Queue
zurück und landet, wenn der Fehler bleibt, über das Zustelllimit in
`{queue}.XQ`. Den im Fehler genannten Key prüfen und löschen oder
korrigieren. Trifft der Fehler das Voranbringen eines schon erfolgreichen
Workflow-Schritts, wird der Schritt ebenso bis zum Zustelllimit neu
zugestellt; seit 0.13.1 markiert der Worker den Workflow danach mit dem
Fehlertext als `failed`, statt die Nachricht in die DLQ zu legen
(`FIRE_AND_FORGET`-Schritte: sofort). Siehe
[Fehlerfälle bei Workflows](../guides/workflows.md#fehlerfalle).

Fehlerantworten, die durch Warten heilen, sind kein
`BackendResponseError`, sondern ein einfacher `BackendConnectionError`
und werden wie ein nicht erreichbares Backend behandelt: `OOM`
(maxmemory erreicht), `READONLY` (der Client spricht nach einem
Failover noch mit dem herabgestuften Primary), `LOADING`, `TRYAGAIN`,
`CLUSTERDOWN`, `MASTERDOWN`, `MISCONF` (Persistenz scheitert, etwa
bei voller Platte), `BUSY` (ein lang laufendes Skript) und `NOREPLICAS`
(zu wenige Replikate für `min-replicas-to-write`; die beiden letzten
seit 0.13.1).

## `TaskNotFoundError`

**Wann ausgelöst.** Von `app.schedule()`, `app.schedule_many()` und
`app.run()`, wenn keine in den Producer-Prozess importierte
`Task`-Unterklasse diesen Namen in ihrem `TaskInfo` deklariert. Es wird
nichts gesendet. Ein bloßer `Client` ohne Registry wirft nicht; er
sendet die Nachricht und loggt einmal je Task-Name die WARNING
`client_task_defaults_unresolved`, weil er die Vorgaben des Tasks
(Dedup, Priorität) nicht anwenden kann. Auch `swarmq schedule add`
prüft den Namen nicht.

Im Worker erscheint der Name als Fehlertyp einer DLQ-Kopie: Ein Worker,
der den Task nicht registriert, stellt die Nachricht bis zu
`SWARMQ_UNKNOWN_TASK_GRACE_S` (Vorgabe 900 s, `0` schaltet die
Gnadenfrist ab) mit Backoff zurück, damit ein Rolling Deploy keine neuen
Tasks verliert. Erst danach schickt er sie mit
`error_type=TaskNotFoundError` in die DLQ und loggt `task_not_found`.

**Ursache.** Tippfehler, Task-Modul nicht importiert, Task nicht von
`swarmq.Task` abgeleitet, oder Modul importiert, aber die Klasse darin
nicht ausgewertet (z. B. hinter einer Bedingung). Im Worker außerdem
ein Producer, der vor den Workern ausgerollt wurde, die den Task
kennen.

**Behebung.**

```bash
swarmq inspect --module myapp.tasks
```

listet jeden registrierten Task. Fehlt dein Task, wird sein Modul
nicht importiert, oder die Klasse steht in einem
`if TYPE_CHECKING:`-Block. Worker vor Producern ausrollen; eine so in
die DLQ gelangte Nachricht lässt sich mit `swarmq dlq retry` erneut
einreihen, sobald ein Worker den Task kennt.

## `TaskError`

**Wann ausgelöst.** Von `get_result()`, `get_results()` und
`app.run()`, wenn das gespeicherte Ergebnis ein Fehlerergebnis ist:
Der Task-Körper hat eine Exception geworfen und sein Retry-Budget ist
verbraucht (oder er wurde nie wiederholt), oder der Worker hat den
Task selbst beendet (harter Timeout, Ablauf, verlorenes Rechenkind, ein
Ergebnis, das er nicht serialisieren konnte). `get()` eines
Workflow-Handles wirft sie, wenn der Workflow gescheitert ist.

**Ursache.** Jede Exception, die deine `run()`-Methode entweichen
ließ. Das ursprüngliche Exception-Objekt überquert die Leitung nicht;
die Fehlermeldung enthält den Text, den der Worker gespeichert hat
(`""` bei einer Exception ohne Text).
`error_class` nennt die Klasse (beratend): `modul.qualname`, z. B.
`builtins.ValueError`; im CPU-Modus die Klasse, die das Rechenkind
meldet. Bei Ergebnissen älterer Worker und bei Fehlern, die der
Worker selbst erzeugt (harter Timeout, Drain, verlorenes
Rechenkind, Ablauf), ist es `None`.

```python
from swarmq import TaskError

try:
    result = await app.get_result(task_id, timeout=60)
except TaskError as exc:
    if exc.error_class == "builtins.ValueError":
        ...  # ungültige Eingabe, nicht unverändert erneut einreihen
```

**Behebung.** Die Worker-Logs lesen — sie enthalten den vollständigen
Traceback. Dann den Task-Körper korrigieren oder `retry_exceptions`
erweitern, damit die vorübergehende Fehlerklasse wiederholt wird,
statt in der DLQ zu landen.

## `TaskCancelledError`

**Wann ausgelöst.** Beim Client: `get_result()` und `app.run()` werfen
sie, wenn das gespeicherte Ergebnis das Cancel-Flag trägt, ein
Workflow-Handle bei einem abgebrochenen Workflow. Unterklasse von
`TaskError`; das Attribut `task_id` nennt den Task (bei einem Workflow:
die Workflow-ID). Nie im Task-Körper:
Der Worker bricht die laufende `run()`-Coroutine ab, der Task sieht
`asyncio.CancelledError`. Räume in `finally` auf, oder fange
`asyncio.CancelledError`, räume auf und wirf sie erneut. Im CPU-Modus
wird stattdessen das Rechenkind beendet
([Verhaltensänderungen](../guides/cpu-mode.md#verhaltensanderungen-fur-task-autoren)).

**Ursache.** Jemand — der Client, ein Betreiber, ein anderer
Dienst — rief `app.cancel(task_id)` auf, während der Task wartete oder
lief.

**Behebung.** War der Cancel gewollt, abfangen und weitermachen. War
er unbeabsichtigt, die Lebensdauer des Cancel-Flags prüfen — siehe
[`app.cancel()`](api.md#cancel) zum Vorbehalt veralteter Flags.

```python
from swarmq import TaskCancelledError

try:
    result = await app.get_result(task_id, timeout=60)
except TaskCancelledError:
    # Task wurde abgebrochen, bevor er ein Ergebnis produzierte.
    ...
```

## `ResultNotReadyError`

**Wann ausgelöst.** `get_result(task_id, timeout=T)` und
`app.run(..., timeout=T)` nach T Sekunden ohne Ergebnis in Valkey
(`timeout=0` liest genau einmal). `get_results()` wirft sie mit den
noch fehlenden IDs, `get(timeout=T)` eines Workflow-Handles, wenn der
Workflow nicht rechtzeitig fertig ist.

**Ursache.** Der Task läuft noch, der Worker starb, der Task landete
in der DLQ, ohne ein Ergebnis zu speichern, das Ergebnis ist nach
seiner `result_ttl` abgelaufen, oder die `task_id` ist falsch.

**Behebung.** Timeout erhöhen, Worker-Logs prüfen oder — falls der
Task in Retry-Schleifen hängt — die DLQ ansehen.

```python
from swarmq import ResultNotReadyError

try:
    result = await app.get_result(task_id, timeout=5)
except ResultNotReadyError:
    # Noch offen; später erneut prüfen.
    ...
```

## `ResultExpiredError`

**Wann ausgelöst.** Derzeit nie. Nach Ablauf der Ergebnis-TTL wirft
`get_result()` `ResultNotReadyError`, genau wie bei einem Ergebnis, das
noch nicht da ist. Die Klasse bleibt als stabiler Haken für ein
späteres Release, das beide Fälle trennt; sie heute zu fangen bewirkt
nichts.

**Behebung.** Damit Ergebnisse länger lesbar bleiben, `result_ttl`
(`SWARMQ_RESULT_TTL`) erhöhen oder das Ergebnis früher abholen.

## `ResultSerializationError`

**Wann ausgelöst.** Beim Producer von `schedule()`, wenn sich die
Argumente nicht als JSON kodieren lassen; es wird nichts gesendet. Beim
Client von `get_result()`, wenn sich ein gespeichertes Ergebnis nicht
dekodieren lässt, etwa ein Ergebnis einer neueren SwarmQ-Version mit
einem Typ, den diese Version nicht kennt.

Ein Rückgabewert von `run()`, den msgpack nicht kodieren kann, löst
sie beim Aufrufer **nicht** aus: Der Worker loggt
`result_serialization_failed`, speichert stattdessen ein
Fehlerergebnis und bestätigt die Nachricht (kein Retry).
`get_result()` wirft dann `TaskError` mit
`error_class == "swarmq.exceptions.ResultSerializationError"`.

**Ursache.** Ein Argument oder Rückgabewert eines Typs, den die
Kodierer nicht kennen. Häufige Kandidaten: offene File-Handles,
SQLAlchemy-ORM-Objekte, `asyncio.Task`-Instanzen, eigene Klassen ohne
Konvertierung.

**Behebung.** In einfache `dict`, `list`, `str`, `int`, `float`,
`bool`, `None` oder Kombinationen davon umwandeln. Ergebnisse
unterstützen zusätzlich `bytes`, `datetime`, `date`, `time`, `Decimal`,
`UUID`, `set` und `frozenset`.

```python
class FetchUser(Task):
    task_info = TaskInfo(name="fetch_user", queues=["default"])

    async def run(self, user_id: int) -> dict:
        user = await db.fetch_one(user_id)
        # dict zurückgeben, kein ORM-Row-Objekt.
        return {"id": user.id, "email": user.email}
```

## `Retry`

**Wann ausgelöst.** Von deinem eigenen Task-Code, um einen Retry mit
einer bestimmten Wartezeit zu erzwingen.

**Ursache.** Kein Fehler, sondern ein Kontrollfluss-Signal. Es sagt dem
Worker: „Dieser Versuch ist gescheitert, wiederhole in `delay`
Sekunden.“

**Behebung.** Nichts zu beheben; so wird er benutzt:

```python
from swarmq import Retry

class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email", queues=["default"], max_retries=5,
    )

    async def run(self, to: str, subject: str) -> None:
        response = await smtp.send(to, subject)
        if response.status == 429:
            raise Retry(delay=60.0)  # in einer Minute erneut versuchen
```

Ist `delay` `None`, wendet der Worker die konfigurierte
Backoff-Strategie an (`retry_backoff`, `retry_base_delay`). Ein
negatives `delay` gilt als 0 (sofortiger Retry) und zählt trotzdem
gegen `max_retries`. `inf`, `nan` und Werte über 315 360 000 s (zehn
Jahre) werfen beim Erzeugen von `Retry` einen `ValueError`; aus einem
Task geworfen, ist dieser `ValueError` ein gewöhnlicher Task-Fehler und
folgt `retry_exceptions` und dem Backoff
(E-060).

`Retry` wiederholt auch, wenn der Task `retry_exceptions` setzt, ohne
ihn zu nennen. Er zählt gegen `max_retries`: Ist das Budget verbraucht,
und immer bei `max_retries=0`, endet der Task terminal (DLQ bzw.
Fehlerergebnis, wenn das Dead-Lettering abgeschaltet ist).

## `NoRetry`

**Wann ausgelöst.** Von deinem eigenen Task-Code, um das Retry-Budget
zu überspringen und direkt in die DLQ zu gehen.

**Ursache.** Kein Fehler. Auslösen, wenn das Scheitern dauerhaft ist
und Retries nur Budget verschwenden würden.

**Behebung.** Wie bei `Retry` ist das die beabsichtigte Nutzung. Der
an `NoRetry` übergebene Text steht im Attribut `reason` und wird zur
Fehlermeldung:

```python
from swarmq import NoRetry

class ChargeCard(Task):
    task_info = TaskInfo(name="charge", queues=["payments"])

    async def run(self, card_id: str, amount: int) -> dict:
        try:
            return await stripe.charge(card_id, amount)
        except stripe.CardDeclinedError as exc:
            # Kein Retry macht eine abgelehnte Karte wieder gültig.
            raise NoRetry(str(exc))
```

Der Task landet sofort in `{queue}.XQ`. Nach dem Korrigieren der
Eingabe holt ihn `swarmq dlq retry --queue payments <task_id>` zurück;
`swarmq dlq purge --queue payments` leert den Rückstand.

## `SoftTimeoutError`

**Wann ausgelöst.** Der Task-Körper läuft noch, wenn `soft_timeout`
abläuft. Der Worker führt `run()` unter `asyncio.wait_for` aus: Ist
die Zeit um, bricht er `run()` ab. Im Task kommt deshalb ein
`asyncio.CancelledError` an, kein `SoftTimeoutError`. Der Worker
speichert danach `SoftTimeoutError` als Fehlerergebnis. Es ist wie
jeder andere Task-Fehler retry-fähig, und `post_execution` entfällt.
`SoftTimeoutError` ist kein `TimeoutError`;
`retry_exceptions=[TimeoutError]` erfasst ihn also nicht.

Nur der Ablauf von `soft_timeout` selbst wird zu `SoftTimeoutError`.
Ein `TimeoutError`, den der Task selbst wirft (`asyncio.timeout`,
`socket.timeout`, `aiohttp.ServerTimeoutError`), bleibt, was er ist:
Klasse und Meldung bleiben erhalten, und
`retry_exceptions=[TimeoutError]` greift.

**Ursache.** Der Task braucht länger als `TaskInfo(soft_timeout=N)`
erlaubte.

**Behebung.** Den Task schneller machen oder `soft_timeout` erhöhen.
Um Teilergebnisse zu sichern, in `finally` aufräumen oder den Abbruch
abfangen und erneut werfen:

```python
import asyncio

class LongJob(Task):
    task_info = TaskInfo(
        name="long_job", queues=["default"],
        soft_timeout=60, hard_timeout=120,
    )

    async def run(self) -> None:
        try:
            await process_items()
        except asyncio.CancelledError:
            # Was fertig ist, committen, dann den Worker finalisieren lassen.
            await checkpoint()
            raise
```

Das `raise` ist nötig. Ein Task, der die `CancelledError` verschluckt
und normal zurückkehrt, liefert ein **Erfolgsergebnis** mit seinem
Rückgabewert; der Worker verbucht dann keinen `SoftTimeoutError`.
`hard_timeout` greift nur, wenn der Task den Abbruch ignoriert oder
hinauszögert.

## `PublishIndeterminate`

**Wann ausgelöst.** Von `schedule()`, `schedule_many()`, `run()` und
`apply()`, wenn ein Publish beim Warten auf das Publisher-Confirm des
Brokers `SWARMQ_PUBLISH_TIMEOUT_S` überschreitet oder Verbindung oder
Kanal in dieser Phase abbrechen (etwa ein Broker-Neustart oder eine
Netztrennung direkt nach dem Senden). Ebenso, wenn ein anderer Publish
auf demselben Kanal nach dem Senden seine Frist überschreitet: SwarmQ
verwirft dann den Kanal, und jeder Publish, der dort noch auf sein
Confirm wartet, endet beim Schließen des Kanals als
`PublishIndeterminate`, auch wenn seine eigene Frist noch läuft; der
nächste Publish öffnet einen neuen Kanal. Im Ressourcenalarm des
Brokers wartet dieses Schließen auf die Freigabe (siehe
[`BrokerPublishBlocked`](#brokerpublishblocked)). Unterklasse von
`BrokerConnectionError`.

**Ursache.** Broker oder Netz sind nach dem Senden der Nachricht
hängen geblieben oder abgebrochen; der Broker hat sie vielleicht,
vielleicht nicht.

**Behebung.** Die Zustellung als unbekannt behandeln. Nur wiederholen,
wenn der Task idempotent ist oder mit `unique_for` eingereiht wurde
(SwarmQ behält in diesem Fall den Dedup-Claim, eine Wiederholung im
Fenster wird also abgewiesen statt doppelt zu laufen). Siehe die Warnung
unter [`BrokerConnectionError`](#brokerconnectionerror). Ein
Scheduler-Leader zählt den Fehler nicht für den Rücktritt: Ausbleibende
Confirms hängen meist an einer Queue (etwa einer Quorum-Queue ohne
Quorum), nicht an der Verbindung des Leaders. Nach einem
Verbindungsabbruch scheitern die folgenden Publishes, die nichts senden,
als `BrokerConnectionError` und zählen.

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

**Wann ausgelöst.** Nur im Worker, von `retry_later` und `dead_letter`:
Der Broker hat die Retry-, Dead-Letter- oder Rückstellungs-Kopie
bestätigt, aber das Bestätigen des Originals scheiterte (meist, weil
der Channel weg ist).
Unterklasse von `BrokerConnectionError`, bestehende Handler fangen sie
also weiter. Sie erreicht nie `schedule()` oder `get_result()`.

**Ursache.** Channel oder Verbindung gingen zwischen dem Publish der
Kopie und dem Ack verloren, etwa nach einem Consumer-Timeout auf
RabbitMQ bis 4.2.

**Behebung.** Im Anwendungscode nichts nötig. Der Broker stellt das
Original erneut zu, während die Kopie schon unterwegs ist. Bei einer
Retry- oder Dead-Letter-Kopie setzt der
[Ausführungsschutz](../explanation/delivery-semantics.md#ausfuhrungsschutz)
seinen Superseded- bzw. Done-Marker wie nach Erfolg, sodass das neu
zugestellte Original übersprungen wird. Eine Rückstellungs-Kopie
(Rate-Limit, Lock, Ausführungsschutz, Backend-Ausfall, unbekannter
Task, Schedfire; gleicher Retry-Zähler) bekommt keinen Marker: Kopie
und Original laufen beide erneut durch die Gates, und der
Ausführungsschutz serialisiert sie (seit 0.13.0rc2, E-043). Das gilt
nicht für `FIRE_AND_FORGET`-Tasks, ohne `ExecutionGuardStore` und mit
`SWARMQ_EXECUTION_GUARD=false`; dann können Kopie und Original beide
laufen. Für Tests aus `swarmq.exceptions` importieren.

## `MessageHeadersTooLarge`

**Wann ausgelöst.** Von einem Publish (`schedule()`, im Worker auch von
Retry- und Dead-Letter-Kopien), wenn die kodierten Nachrichten-Header
mehr als die Hälfte des ausgehandelten `frame_max` brauchen, auch
nachdem lange Texte auf 8 KiB gekürzt wurden. Unterklasse von
`BrokerConnectionError`. Es wurde nichts gesendet.

**Ursache.** Viele oder große Header mit Nicht-Text-Werten, etwa eine
Middleware, die Hunderte Header ergänzt. Die eigenen Diagnose-Header von
SwarmQ sind begrenzt und lösen den Fehler nicht aus.

**Behebung.** Nicht mit denselben Headern wiederholen; sie scheitern
wieder. Die Header verkleinern. Anders als bei `PublishIndeterminate`
wird eine `unique_for`-Sperre freigegeben, weil nichts beim Broker
ankam.

## `UnroutableMessage`

**Wann ausgelöst.** Von einem Publish (`send()`, `schedule()`,
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

**Behebung.** Zuerst den Worker der Queue starten, dann erneut
veröffentlichen. Eine `unique_for`-Sperre wird freigegeben, weil nichts
beim Broker ankam. Der Worker deklariert seine eigene
`{queue}.DQ`/`.XQ` einmal selbst neu; der Scheduler-Leader schreibt
einen verlorenen Cron-Fire fest und versucht einen verzögerten Task
alle 60 s erneut, bis `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`
([fehlende Queues](../guides/scheduling-and-retries.md#fehlende-queues)).

## `BrokerPublishBlocked`

**Wann ausgelöst.** Von einem Publish (`send()`, `schedule()`,
Workflow-Schritte, Cron-Fires, im Worker auch Retry-, Rückstellungs- und
Dead-Letter-Kopien), wenn RabbitMQ die Publish-Verbindung blockiert hat
(`connection.blocked`, Speicher- oder Plattenalarm) und sie nicht
innerhalb von `SWARMQ_PUBLISH_TIMEOUT_S` freigibt (beim Herunterfahren:
die restliche Gnadenfrist). Unterklasse von `BrokerConnectionError`.
Der Publish wartet zuerst auf die Freigabe und sendet nichts, solange
blockiert ist; die WARNING `publish_connection_blocked` markiert das
Warten. Eine nicht blockierte Verbindung kostet keinen zusätzlichen
Roundtrip.

**Verbindungsabriss beim Warten.** Seit 0.14 endet das Warten auch,
wenn die blockierte Verbindung abreißt (aiormq meldet dann keine
Freigabe). Gesendet war nichts, der Publish wartet darum nicht mehr bis
zur Publish-Frist und wirft diesen Fehler: Er wartet wie jeder Publish
ohne Verbindung auf den Reconnect und wird dann auf der neuen
Verbindung gesendet (E-042). Ist auch die neue Verbindung blockiert,
wartet er weiter. Dauert der Alarm an, hat RabbitMQ die neue Verbindung
aber noch nicht blockiert (die Blockade meldet es nur einer Verbindung,
die im Alarm publiziert), wird der Publish gesendet und fällt unter
dieselbe Grenze wie jeder erste Publish im Alarm (D-1, siehe
**Ursache**): Er kann als `PublishIndeterminate` enden, oder bestätigt,
wenn der Alarm vor seiner Frist endet. Die Publish-Frist bleibt die
Obergrenze für das ganze Warten; ein Abbruch darin wirft weiter
`PublishCancelledUnsent`. Vor 0.14 wartete ein solcher Publish auf der
toten Verbindung bis zu seiner Frist (F4-21, E-025).

**Ursache.** Ein Ressourcenalarm des Brokers. RabbitMQ meldet die
Blockade (`connection.blocked`) nur einer Verbindung, die während des
Alarms publiziert. Der Publish, der die Meldung auslöst, kann trotzdem
bestätigt werden; Publishes, die vor dem Eintreffen von
`connection.blocked` schon gesendet waren, hängen und enden als
`PublishIndeterminate` -- je Verbindung keiner, einer oder mehrere.
Spätere warten und werfen diesen Fehler.

**Im Worker.** Eine Retry-, Rückstellungs- oder Dead-Letter-Kopie, die
auf die Freigabe wartet, belegt ihren Task-Slot bis zur Publish-Frist;
danach geht das Original zurück und läuft erneut (ein Task, der bei
jedem Lauf scheitert, landet nach rund 19 Runden als
`RedeliveryLimitError` in der DLQ). Bei jeder Fehlerrate über null sinkt
der Konsum eines Workers im Alarm auf etwa Slots / (Taskdauer +
Fehlerrate × Publish-Frist): gemessen mit 20 s Frist auf ein Viertel,
mit der Vorgabe 120 s rechnerisch rund 3 %. Acks laufen über die getrennte
Consume-Verbindung weiter. Das ist gewollt, kein Fehler. Bei häufigen
Alarmen kommt eine kürzere `SWARMQ_PUBLISH_TIMEOUT_S` für Worker in
Frage; der Preis sind mehr Zusatzläufe. Schneidet die Frist einen
Publish ab, der gesendet sein kann, verwirft der Worker seinen
Publish-Kanal. Seit 0.14 schickt er das Schließen dieses Kanals erst,
wenn RabbitMQ die Verbindung freigibt oder die Verbindung endet,
höchstens nach der Publish-Frist, und schließt dann wie bisher mit 5 s
Frist. Bei einem Alarm, der kürzer ist als die Publish-Frist, erscheint
keine Fehlerzeile; aiormq kann die WARNING
`Delivery tag … confirmed … was ignored` loggen, wenn der Broker den
auslösenden Publish nach der Freigabe bestätigt (er endet trotzdem als
`PublishIndeterminate`). Dauert der Alarm länger als die Publish-Frist,
läuft das Schließen in seine 5-s-Frist (`publish_channel_close_incomplete`
ohne Traceback, `blocked=True`), und aiormq loggt bei der Freigabe
`Got frame for closed channel` ([Event-Katalog](logging.md#event-katalog)).
Alles das ist harmlos; kein Kanal bleibt zurück. Die übrigen noch
offenen Confirms auf dem verworfenen Kanal bleiben bis zum Schließen
offen: Sie enden bestätigt, wenn die Freigabe vor ihrer eigenen Frist
kommt, sonst mit ihrer eigenen Frist als `PublishIndeterminate`, und
belegen ihren Slot höchstens bis dahin (E-078). Vor 0.14 endeten sie
sofort als `PublishIndeterminate`; bei der Freigabe konnte das einen
ganzen Schub wartender Kopien treffen, wenn eine von ihnen ihre Frist
im Alarm fast aufgebraucht hatte (gemessen mit 0.13: 12 weitere Kopien
bei 20 s Frist). Ihre Originale werden wie bei jeder unbestimmten Kopie neu
zugestellt; die Kopien sind beim Broker und laufen, und der
Ausführungsschutz lässt kein Original neben oder nach seiner Kopie
laufen (im gemessenen Lauf wurden alle Originale als `done`
übersprungen, kein Task lief doppelt).

**Behebung.** Ein späterer Versuch ist sicher, nichts kam beim Broker
an; eine `unique_for`-Sperre wird freigegeben. Speicher oder Platte am
Broker freimachen. Ein Scheduler-Leader zählt den Fehler nicht für den
Rücktritt: Ein Alarm blockiert jeden Kandidaten. Legt aiormq den
Blocked-Zustand nicht mehr offen, loggt swarmq einmal
`publish_blocked_state_unreadable` und publiziert ungeprüft wie vor 0.12.

## `PublishRejected`

**Wann ausgelöst.** Von einem Publish (`send()`, `schedule()`,
Workflow-Schritte, Cron-Fires, Kopien im Worker), wenn alle 5 Versuche
gescheitert sind und RabbitMQ den letzten mit einem negativen
Publisher-Confirm (`basic.nack`) beantwortet hat. Unterklasse von `BrokerConnectionError`; der Text ist
derselbe wie vor 0.12 (`publish to '<queue>' failed after 5 attempts:
…`).

**Ursache.** Die Queue lehnt die Nachricht ab, nicht die Verbindung: eine
volle Queue mit `x-overflow=reject-publish` (seit 0.12 Default neuer
Quorum-Queues, zusammen mit einer `max-length`-Policy) oder eine
Quorum-Queue ohne Quorum ihrer Mitglieder.

**Behebung.** Ein späterer Versuch ist sicher, der Broker hat die
Nachricht nicht gespeichert; ein `unique_for`-Claim wird freigegeben. Die
Queue reparieren (Längenlimit anheben oder entfernen, Quorum
wiederherstellen). Ein Scheduler-Leader zählt den Fehler nicht für den
Rücktritt: Eine kaputte Cron-Queue ließe die Führung sonst nur wandern,
ohne zu helfen.

## `LockWaitExceeded`

**Wann ausgelöst.** Nie; der Name steht nur als `error_type` in
`task_sent_to_dlq` und am Anfang von `x-swarmq-traceback` der
DLQ-Nachricht. Eine Nachricht, deren Task-Lock belegt ist, wartet über
die Delay-Queue. Der Worker schickt sie mit diesem Fehlertyp in die
DLQ, wenn derselbe Halter den Lock länger hält als die TTL des Keys
plus 60 s (der Text sagt `lock holder stalled`) oder wenn seit dem
ersten Konflikt `SWARMQ_LOCK_WAIT_MAX_S` vergangen ist (`lock wait
limit reached`). Das Fehlerergebnis wird gespeichert, `TASK_DLQ`
feuert, und ein Workflow-Schritt markiert seinen Workflow als
gescheitert. Bis 0.11 ging die Nachricht beim 10. Konflikt als
`LockConflictLimitError` in die DLQ.

**Ursache.** Ein Halter, der nicht fertig wird (hängender Task, dessen
Lock der Worker am Leben hält, zu langes `hard_timeout`), oder ein
Lock, der länger belegt ist, als die Grenze erlaubt.

**Behebung.** Den Halter ansehen: Die Logzeile `lock_wait_exceeded`
nennt den Lock (`lock_name`), ebenso `x-swarmq-lock-name` an der
DLQ-Nachricht. Einen festgefahrenen Lock entfernt
`valkey-cli DEL [<namensraum>:]sq:lock:<name>`. Danach die Nachricht mit
`swarmq dlq retry` wiederholen. `SWARMQ_LOCK_WAIT_MAX_S` anheben, wenn
lange Wartezeiten erwartet sind.

## `RedeliveryLimitError`

**Wann ausgelöst.** Nie; der Name steht nur als `error_type` in
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

**Behebung.** Die Ursache beheben, die das Worker-Log vor dem
Dead-Letter-Eintrag nennt, dann die Nachricht mit `swarmq dlq retry`
erneut einreihen.

## `WorkflowValidationError`

**Wann ausgelöst.** Beim Bauen eines Workflows mit `chain`, `group` oder
`chord`, bevor irgendetwas eingereiht wird (ein per Name mit `sig()`
gebauter Schritt wird erst zur Laufzeit auf `FIRE_AND_FORGET` geprüft).
Außerdem zur Laufzeit im Worker, wenn eine Group oder ein Chord ihre
Ergebnisliste nicht vollständig zusammenbekommt; der Workflow wird dann
als gescheitert markiert und `handle.get()` wirft `TaskError`.

**Ursache.** Verschachtelung tiefer als `max_workflow_depth` (10), eine
Group mit mehr als `max_group_size` (1000) Mitgliedern, eine leere
Chain oder Group, ein Chord-Header, der kein `group(...)` ist, oder ein
`FIRE_AND_FORGET`-Task als Schritt.

**Behebung.** Topologie abflachen, große Groups aufteilen oder den Task
auf `DeliveryMode.AT_LEAST_ONCE` umstellen. Siehe
[Workflow-Primitive](api.md#workflow-primitive).

## `WorkflowNotFound`

**Wann ausgelöst.** `app.workflow(workflow_id)` und die
`WorkflowHandle`-Methoden `status()`, `children()` und `get()`, wenn es
zur ID keinen Workflow-Zustand gibt.

**Ursache.** Die ID ist falsch, der Workflow wurde nie gestartet, oder
sein Zustand ist abgelaufen (er lebt `result_ttl` Sekunden).

**Behebung.** ID prüfen. Für lange Workflows `result_ttl`
(`SWARMQ_RESULT_TTL`) erhöhen, damit der Zustand den Lauf und die Zeit
bis zum Abholen des Ergebnisses überlebt.

## `WorkflowResultExpired`

**Wann ausgelöst.** Von `handle.get()`, wenn der Workflow fertig ist,
sein Endergebnis aber schon abgelaufen ist; im Worker, wenn ein
Schritt-Ergebnis, das der nächste Schritt oder das Fan-in braucht,
abgelaufen ist — der Workflow scheitert dann.

**Ursache.** Eine Ergebnis-TTL, die kürzer ist als die Laufzeit des
Workflows plus die Zeit bis zum Abholen.

**Behebung.** `result_ttl` erhöhen (global oder je Task über
`TaskInfo.result_ttl`) und Workflow-Ergebnisse zeitnah abholen.

## `WorkflowStateConflict`

**Wann ausgelöst.** Nur im Worker, beim Voranbringen eines Workflows: Das
Compare-and-Set auf den Workflow-Zustand hat alle 25 Runden gegen
gleichzeitige Schreiber verloren. Erreicht nie `schedule()` oder
`handle.get()`.

**Ursache.** Mehr Zweige eines Workflows, die im selben Moment fertig
werden, als die Konfliktgrenze erlaubt.

**Behebung.** Im Anwendungscode keine nötig. Der Worker loggt
`workflow_advance_backend_error` und stellt die Schritt-Nachricht neu zu
(`at_least_once`); die Neuzustellung bringt den Workflow voran,
deterministische Schritt-IDs halten das idempotent. Für Tests aus
`swarmq.exceptions` importieren.

## Außerhalb der Hierarchie: `ForkSourceRejected`

`swarmq.supervision.ForkSourceRejected` ist ein `RuntimeError`, kein
`SwarmQError`; `except SwarmQError` fängt ihn also nicht.

**Wann ausgelöst.** Von `Worker.start()` (und damit `app.start_worker()`)
eines eingebetteten CPU-Pools, nachdem der Worker geordnet
heruntergefahren ist, weil sein Pool den Forkserver verworfen hat: Der
Forkserver wurde still neu gestartet und lief danach mit mehr als einem
Thread, ein Fork aus ihm ist also nicht mehr sicher.

**Ursache.** Ein Thread, der beim Import im Forkserver gestartet wurde,
meist eine BLAS/OpenMP-Berechnung, ein logging-`QueueListener` deiner
App oder die Opt-in-Log-Queue. Der Worker loggt
`forkserver_restarted_multithreaded` mit den Thread-Namen und einem
Hinweis, danach `compute_fork_source_rejected`.

**Behebung.** Den Worker-Prozess neu starten; ein neuer Prozess bringt
einen frischen Forkserver. Unter `swarmq worker` geschieht das von
selbst: Der Worker-Subprozess endet mit Code `71`, und der Supervisor
startet ihn im Restart-Budget neu (`worker_fork_source_rejected`).
Siehe [CPU-Modus](../guides/cpu-mode.md).

## Außerhalb der Hierarchie: `PublishCancelledUnsent`

`swarmq.exceptions.PublishCancelledUnsent` (seit 0.14) ist eine
Unterklasse von `asyncio.CancelledError`, kein `SwarmQError`, und wird
nicht aus `swarmq` exportiert. Er ist ein Abbruch wie jeder andere:
`except asyncio.CancelledError` fängt ihn, `task.cancelled()` ist
`True`, und `asyncio.timeout` macht `TimeoutError` daraus.

**Wann ausgelöst.** Vom Publish des RabbitMQ-Brokers, wenn die aufrufende
Task abgebrochen wird, bevor ein Versuch die Nachricht gesendet haben
kann: beim Warten auf eine blockierte Publish-Verbindung
(Ressourcenalarm), beim Öffnen des Publish-Kanals, beim Warten auf die
Kanal-Sperre oder im Backoff nach einem Versuch, der nichts gesendet
hat. Ein Abbruch nach dem Senden bleibt ein einfacher `CancelledError`.

**Ursache.** Ein Timeout um `schedule()` oder `schedule_many()` oder
ein `task.cancel()` der aufrufenden Task.

**Behebung.** Nichts zu tun. Der Client gibt den `unique_for`-Claim
eines solchen Aufrufs frei, ein Wiederholversuch wird also nicht als
Duplikat abgewiesen; nach einem einfachen `CancelledError` im Publish
bleibt der Claim bis zu seinem Ablauf
([Deduplizierung](../guides/locking-and-dedup.md), E-041).

## Siehe auch

- [API-Referenz](api.md) — die Methoden, die diese Exceptions auslösen
- [Logging](logging.md) — Logging-Einstellungen und `--log-config`
- [Scheduling und Retries](../guides/scheduling-and-retries.md) —
  wie Retry-Policy mit `Retry` und `NoRetry` interagiert
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) —
  Betreiberwerkzeuge, um Tasks aus der DLQ zurückzuholen
