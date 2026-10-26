# Ersten E-Mail-Worker bauen

In diesem Tutorial baust du einen Worker, der (zum Schein) E-Mails
verschickt. Schritt für Schritt lernt er, vorübergehende Fehler zu
wiederholen, ein Rate-Limit einzuhalten, Lifecycle-Events über
Signal-Handler zu melden und dauerhafte Fehler in eine Dead-Letter-Queue
zu verschieben — aus der du sie danach von Hand erneut einspielst. Plane
etwa 45 Minuten ein. Jedes Kapitel endet mit dem vollständigen Stand
deiner Dateien, sodass du an jeder Kapitelgrenze wieder einsteigen
kannst.

## Was du baust

```mermaid
sequenceDiagram
    participant Script as enqueue.py
    participant App as SwarmQ-App
    participant MQ as RabbitMQ
    participant W as Worker
    participant V as Valkey

    Script->>App: await app.run("send_email", ...)
    App->>MQ: Nachricht veröffentlichen
    MQ->>W: zustellen
    W->>V: Rate-Limit prüfen
    W->>W: SendEmail.run(...)
    W->>V: Ergebnis speichern
    W->>MQ: ack
    V-->>App: Ergebnis
    App-->>Script: "sent to <addr>: ..."
```

Du arbeitest in einem Verzeichnis mit zwei Terminals: Terminal 1 führt
den Worker aus, Terminal 2 die Client-Skripte.

## Voraussetzungen

- Python 3.14 oder neuer
- Docker mit Compose v2
- [`uv`](https://docs.astral.sh/uv/) (oder `python3.14 -m venv` und
  `pip`)

## Kapitel 1 — Infrastruktur und ein erster Task

Lege ein Projektverzeichnis und ein Virtualenv an:

```bash
mkdir swarmq-tutorial && cd swarmq-tutorial
uv venv --python 3.14
source .venv/bin/activate
uv pip install swarmq
```

Lege eine `docker-compose.yml` für RabbitMQ und Valkey an:

```yaml title="docker-compose.yml"
services:
  rabbitmq:
    image: rabbitmq:4.3-management
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  valkey:
    image: valkey/valkey:8
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10
```

```bash
docker compose up -d
docker compose ps
```

Nach 10–15 Sekunden melden beide Dienste `healthy`. Die
RabbitMQ-Management-Oberfläche unter <http://localhost:15672>
(guest/guest) ist optional, aber praktisch, um später in die Queues zu
schauen.

Jetzt der Task:

```python title="tasks.py"
from swarmq import Task, TaskInfo


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["emails"],
    )

    async def run(self, to: str, subject: str) -> str:
        # Stell dir vor, hier spricht der Task mit einem echten SMTP-Anbieter.
        return f"sent to {to}: {subject!r}"
```

Prüfe, ob SwarmQ ihn findet. `swarmq inspect` importiert nur das Modul
und verbindet sich mit nichts:

```bash
swarmq inspect --module tasks
```

```text
TASK        QUEUES  DELIVERY       RETRIES  RATE_LIMIT  LOCK  UNIQUE_FOR  UNIQUE_UNTIL  CRON
send_email  emails  at_least_once  0        -           -     -           -             -
```

Im Terminal bekommst du diese Tabelle; in ein anderes Programm geleitet,
gibt derselbe Befehl JSON aus (`--format` wählt). Wird das Modul nicht
gefunden, starte den Befehl in dem Verzeichnis, das `tasks.py` enthält.

### Stand am Ende von Kapitel 1

```text
swarmq-tutorial/
├── docker-compose.yml
└── tasks.py
```

## Kapitel 2 — Worker starten, aus einem Client enqueuen

Setze in **Terminal 1** (Virtualenv aktiv) die Verbindungs-URLs und
starte den Worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module tasks --queues emails
```

Der Worker loggt `worker_started`, deklariert die Queue `emails` und
wartet. Starte den Worker vor dem ersten Client: Ein Publish an eine
Queue, die es noch nicht gibt, scheitert mit `UnroutableMessage`.

Lege in **Terminal 2** (`source .venv/bin/activate`) den Client an:

```python title="enqueue.py"
import asyncio

from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

import tasks  # noqa: F401  (registriert SendEmail in diesem Prozess)


async def main() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend) as app:
        result = await app.run(
            "send_email",
            to="alice@example.com",
            subject="Welcome",
            timeout=10,
        )
        print("client received:", result)


asyncio.run(main())
```

```bash
python enqueue.py
```

```text
client received: sent to alice@example.com: 'Welcome'
```

In Terminal 1 loggt der Worker `task_succeeded` mit `task_id` und
`duration_ms`. (`task_received` und `task_started` stehen auf DEBUG;
starte den Worker mit `--log-lifecycle-level INFO`, um sie auch zu
sehen.)

### Was passiert ist

1. `enqueue.py` hat sich beim Betreten von `async with` mit RabbitMQ und
   Valkey verbunden.
2. `app.run("send_email", ...)` hat den Task in seiner Registry
   nachgeschlagen, die Nachricht in die Queue `emails` veröffentlicht
   (`schedule()`) und dann bis zu 10 Sekunden auf das Ergebnis gewartet
   (`get_result()`).
3. Der Worker hat die Nachricht empfangen, `SendEmail().run(...)`
   aufgerufen, den Rückgabewert in Valkey gespeichert und die Nachricht
   bestätigt.
4. `get_result()` wurde über Valkey-Pub/Sub benachrichtigt und hat den
   Wert zurückgegeben.

### Stand am Ende von Kapitel 2

```text
swarmq-tutorial/
├── docker-compose.yml
├── enqueue.py
└── tasks.py
```

## Kapitel 3 — Retries und ein Rate-Limit

Der E-Mail-Anbieter ist unzuverlässig und hat eine Quote. Gib dem Task
ein Retry-Budget und ein Rate-Limit, und simuliere einen Anbieter, der
in 30 % der Fälle scheitert:

```python title="tasks.py" hl_lines="1 10-13 17-18"
import random

from swarmq import Task, TaskInfo


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["emails"],
        max_retries=3,
        retry_backoff="exponential",
        retry_base_delay=1.0,
        rate_limit="5/s",
    )

    async def run(self, to: str, subject: str) -> str:
        if random.random() < 0.3:
            raise RuntimeError("smtp temporarily unavailable")
        return f"sent to {to}: {subject!r}"
```

- `max_retries=3` — bis zu drei weitere Versuche nach dem ersten. Jede
  Exception gilt als wiederholbar, solange du sie nicht mit
  `retry_exceptions` einschränkst.
- `retry_backoff="exponential"`, `retry_base_delay=1.0` — etwa 1, 2,
  dann 4 Sekunden Pause zwischen den Versuchen (jeweils mit bis zu
  doppeltem Zufallsaufschlag).
- `rate_limit="5/s"` — höchstens fünf Starts pro Sekunde. Das Limit
  liegt in Valkey und gilt für alle Worker gemeinsam.

Der Worker lädt den Task-Code einmal beim Start. Stoppe ihn mit
++ctrl+c++ und starte ihn neu:

```bash
swarmq worker --module tasks --queues emails
```

Schicke in Terminal 2 zwölf E-Mails auf einmal:

```python title="burst.py"
import asyncio

from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

import tasks  # noqa: F401


async def main() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend) as app:
        calls = [
            app.run(
                "send_email",
                to=f"user-{i}@example.com",
                subject=f"Burst message {i}",
                timeout=60,
            )
            for i in range(12)
        ]
        results = await asyncio.gather(*calls, return_exceptions=True)
        for i, result in enumerate(results):
            print(i, result)


asyncio.run(main())
```

```bash
python burst.py
```

Beobachte Terminal 1:

- Die meisten Tasks gelingen beim ersten Versuch (`task_succeeded`).
- Ein gescheiterter Versuch loggt `task_failed` (mit `error_type` und
  `error`), danach `task_retrying` mit `retry_count` und `delay_ms`; ein
  späterer Versuch gelingt meist.
- Nachrichten über dem Limit von fünf pro Sekunde loggen
  `task_rate_limited` mit `retry_after_ms` und laufen einen Moment
  später.

In Terminal 2 lauten meist alle zwölf Zeilen
`sent to user-N@example.com: 'Burst message N'`: Retries und
Rate-Limit-Wartezeiten verlängern nur den Roundtrip. Ab und zu scheitert
ein Task bei allen vier Versuchen (etwa 1 von 120). Seine Zeile zeigt
dann einen `TaskError`, und der Worker verschiebt die Nachricht in die
Dead-Letter-Queue — darum geht es in Kapitel 5.

### Stand am Ende von Kapitel 3

```text
swarmq-tutorial/
├── burst.py
├── docker-compose.yml
├── enqueue.py
└── tasks.py        # mit max_retries, Backoff und rate_limit
```

## Kapitel 4 — Signal-Handler

Mit Signal-Handlern reagierst du auf Lifecycle-Events, etwa um Metriken
zu zählen oder zu alarmieren. Ergänze zwei am Ende von `tasks.py`:

- `Signal.TASK_DLQ` feuert einmal, wenn ein Task aufgegeben und in die
  Dead-Letter-Queue gelegt wird. (`TASK_FAILED` feuert nach jedem
  gescheiterten Versuch, Retries eingeschlossen, und ist deshalb der
  falsche Hook für „diese E-Mail wurde nicht verschickt“.)
- `Signal.TASK_RATE_LIMITED` feuert jedes Mal, wenn das Rate-Limit eine
  Nachricht zurückhält.

```python title="tasks.py" hl_lines="1 4 6 25-38"
import logging
import random

from swarmq import Signal, SwarmQ, Task, TaskInfo

log = logging.getLogger("tutorial")


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["emails"],
        max_retries=3,
        retry_backoff="exponential",
        retry_base_delay=1.0,
        rate_limit="5/s",
    )

    async def run(self, to: str, subject: str) -> str:
        if random.random() < 0.3:
            raise RuntimeError("smtp temporarily unavailable")
        return f"sent to {to}: {subject!r}"


def install_handlers(app: SwarmQ) -> None:
    @app.on_signal(Signal.TASK_DLQ)
    async def on_dead_lettered(task_id, task_name, error, **kwargs):
        log.warning("DEAD_LETTERED %s %s: %s", task_name, task_id, error)

    @app.on_signal(Signal.TASK_RATE_LIMITED)
    async def on_limited(
        task_id, task_name, retry_after_ms, remaining, rate_limit_count,
        **kwargs,
    ):
        log.info(
            "RATE_LIMIT task=%s retry_after_ms=%d remaining=%d bounces=%d",
            task_name, retry_after_ms, remaining, rate_limit_count,
        )
```

Handler sind `async`-Funktionen und enden immer mit `**kwargs`: Der
Worker übergibt mehr Keys, als ein Handler benennt (zum Beispiel
`queue`), und spätere Releases können weitere ergänzen.

Ein Handler gehört zu einer `SwarmQ`-App, und diese Signals feuern im
Worker. `swarmq worker` baut seine eigene App; Handler auf einer App,
die du in `tasks.py` anlegst, liefen deshalb nie. Stattdessen ruft der
Worker vor dem Start eine Funktion von dir mit seiner App auf: Übergib
sie als `--configure MODULE:CALLABLE`. Starte den Worker in Terminal 1
neu:

```bash
swarmq worker --module tasks --queues emails --configure tasks:install_handlers
```

Führe in Terminal 2 noch einmal `python burst.py` aus. Das Worker-Log
enthält jetzt `RATE_LIMIT`-Zeilen deines Handlers neben den Events von
SwarmQ und — selten, wenn ein Task alle Versuche verbraucht — eine
Zeile `DEAD_LETTERED`. Das Logging-Setup des Workers erfasst auch deine
eigenen Logger; `tutorial` braucht keine Konfiguration.

!!! tip "Den Worker im eigenen Programm starten"
    Statt über die CLI kannst du den Worker mit
    `await app.start_worker()` auf einer selbst gebauten App starten und
    die Handler direkt darauf registrieren. Dann gehört das Logging
    deinem Programm: SwarmQ richtet keines ein (siehe
    [Logging-Referenz](../reference/logging.md)). Für einen ersten
    Worker ist die CLI einfacher.

### Stand am Ende von Kapitel 4

```text
swarmq-tutorial/
├── burst.py
├── docker-compose.yml
├── enqueue.py
└── tasks.py        # plus install_handlers()
```

## Kapitel 5 — Eine Nachricht in die Dead-Letter-Queue schicken und erneut einspielen

Manche Fehler sind dauerhaft: Eine abgelehnte Adresse erneut zu
versuchen, kostet nur Zeit. Wirf `NoRetry`, um die Nachricht sofort in
die Dead-Letter-Queue zu schicken. Ändere den Import und den Anfang von
`run()`:

```python title="tasks.py (Auszug)"
from swarmq import NoRetry, Signal, SwarmQ, Task, TaskInfo

# ... SendEmail.task_info unverändert ...

    async def run(self, to: str, subject: str) -> str:
        if to.endswith("@nope.invalid"):
            raise NoRetry(f"rejected recipient: {to}")
        if random.random() < 0.3:
            raise RuntimeError("smtp temporarily unavailable")
        return f"sent to {to}: {subject!r}"
```

Starte den Worker neu (derselbe Befehl wie in Kapitel 4). Schicke dann
eine Nachricht, die abgelehnt wird. Dieses Skript nutzt `schedule()`,
das die Task-ID zurückgibt, ohne zu warten:

```python title="reject.py"
import asyncio

from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

import tasks  # noqa: F401


async def main() -> None:
    async with SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    ) as app:
        task_id = await app.schedule(
            "send_email", to="spammy@nope.invalid", subject="nope"
        )
        print("scheduled", task_id)


asyncio.run(main())
```

```bash
python reject.py
```

Der Worker loggt `task_failed`, dann `task_sent_to_dlq`: Er hat ein
Fehlerergebnis gespeichert, die Nachricht in die Dead-Letter-Queue
`emails.XQ` kopiert und das Original bestätigt. Dein `TASK_DLQ`-Handler
ergänzt seine Zeile `DEAD_LETTERED`.

Schau in die Dead-Letter-Queue. Die Befehle `swarmq dlq` brauchen
`SWARMQ_BROKER_URL`, setze die Variable also auch in Terminal 2:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
swarmq dlq list --queue emails
```

```text
TASK_ID                               TASK_NAME   RETRIES  ERROR_PREVIEW                            BODY_BYTES
019a1c2e-5f3b-7d40-8a6e-2b9c4d1e0f7a  send_email  -        rejected recipient: spammy@nope.invalid  98
```

`RETRIES` ist `-`, weil die Nachricht nie einen Retry durchlaufen hat.
Deine Task-ID und Body-Größe weichen ab. `swarmq dlq inspect <task_id>
--queue emails` zeigt alle Header und die dekodierten Argumente. Das
Auflisten ist gefahrlos: Die Nachrichten bleiben in der Queue.

Behebe jetzt die Ursache. Angenommen, das Team entscheidet, ungültige
Adressen zu überspringen statt abzulehnen. Ändere die Prüfung in
`run()`:

```python title="tasks.py (Auszug)"
        if to.endswith("@nope.invalid"):
            log.warning("skipping invalid recipient %s", to)
            return f"skipped {to}"
```

Starte den Worker neu, damit er den neuen Code ausführt, und spiele die
Nachricht mit der Task-ID aus `dlq list` erneut ein:

```bash
swarmq dlq retry 019a1c2e-5f3b-7d40-8a6e-2b9c4d1e0f7a --queue emails
```

Der Worker nimmt die Nachricht wieder auf und loggt deine Zeile
`skipping invalid recipient` und `task_succeeded`. Ein erneutes
Einspielen beginnt mit vollem Retry-Budget.

Nachrichten, die du nicht behalten willst, löschst du in einem Rutsch:

```bash
swarmq dlq purge --queue emails
```

Das lässt sich nicht rückgängig machen. Unbeachtet laufen Nachrichten in
der Dead-Letter-Queue nach 7 Tagen ab (`SWARMQ_DEAD_LETTER_TTL_S`).

### Stand am Ende von Kapitel 5

```text
swarmq-tutorial/
├── burst.py
├── docker-compose.yml
├── enqueue.py
├── reject.py
└── tasks.py        # überspringt ungültige Empfänger, wiederholt den Rest
```

## Zusammenfassung

Dein Worker kann jetzt:

- **Vorübergehende Fehler wiederholen**, mit exponentiellem Backoff
  (`max_retries`, `retry_backoff`), und mit `NoRetry` früh aufhören, wenn
  ein Fehler dauerhaft ist.
- **Eine Quote des Anbieters einhalten.** `rate_limit="5/s"` gilt über
  alle Worker, die sich das Valkey teilen.
- **Nachrichten isolieren, die er nicht verarbeiten kann**, in
  `emails.XQ`, wo du sie mit `swarmq dlq` ansiehst, erneut einspielst
  oder löschst.
- **Melden, was passiert**, über Signal-Handler, registriert mit
  `--configure`.

Eine Option zum Ausprobieren: `unique_for=60` in `TaskInfo`
dedupliziert Enqueues. Solange ein Task mit denselben Argumenten noch
wartet oder läuft (höchstens 60 Sekunden), gibt ein zweites
`schedule()` die ID des ersten Tasks zurück, statt eine weitere E-Mail
zu verschicken.

Dasselbe Setup wächst von einem Worker auf deinem Laptop zu vielen
Prozessen — siehe `--processes` und `--autoscale` unter
[`swarmq worker`](../reference/cli.md#swarmq-worker).

## Nächste Schritte

- [Signals](../guides/signals.md) — jedes Signal und seine Argumente.
- [Middleware](../guides/middleware.md) — wenn du jeden Task umhüllen
  statt nur beobachten willst.
- [Scheduling und Retries](../guides/scheduling-and-retries.md) —
  `delay`/`eta`, Crons und Details zum Backoff.
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) — der komplette
  Ablauf für Operatoren, Aufbewahrung und Alerting.
- [Locking und Deduplizierung](../guides/locking-and-dedup.md) —
  `unique_for` und Locks über Worker hinweg.
- [Architektur](../explanation/architecture.md) — was hinter
  `schedule()` und `run()` läuft.
