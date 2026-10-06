# Ersten E-Mail-Worker bauen

Am Ende dieses Tutorials hast du einen laufenden SwarmQ-Worker, der
E-Mail-Send-Requests annimmt, ein Rate-Limit durchsetzt, bei
transienten Fehlern retry't, permanente Fehler in eine
Dead-Letter-Queue routet und eine Metrik bei jedem Lifecycle-Event
emittiert. Du hast auch eine fehlgeschlagene Nachricht von Hand
wiederhergestellt. Hands-on-Zeit: 45–60 Minuten. Nichts
Nachfolgendes hängt von späteren Abschnitten ab — falls du hängen
bleibst, starte im vorigen Kapitel mit dem dort gezeigten
kumulativen Code neu.

## Was du bauen wirst

```mermaid
sequenceDiagram
    participant Script as enqueue.py
    participant App as SwarmQ-App
    participant MQ as RabbitMQ
    participant W as Worker
    participant V as Valkey

    Script->>App: await app.run("send_email", ...)
    App->>MQ: publish message
    MQ->>W: deliver
    W->>V: Rate-Limit-Check
    W->>W: Task.run(...) → "sent"
    W->>V: Ergebnis speichern
    W->>MQ: ack
    App->>V: Ergebnis lesen
    App-->>Script: "sent to <addr>"
```

Drei Dateien auf deiner Platte, ein Shell pro Prozess:

- `tasks.py` — der `SendEmail`-Task.
- `enqueue.py` — ein Einmal-Client.
- `docker-compose.yml` — RabbitMQ und Valkey (aus dem Repo kopieren).

## Voraussetzungen

- Python 3.11 oder neuer (3.14 in CI getestet).
- Docker mit Compose v2.
- Ein Package-Manager. Befehle unten nutzen
  [`uv`](https://docs.astral.sh/uv/); wechsle zu `pip`/`poetry`,
  falls bevorzugt.
- Zehn Minuten. Nimm sie dir gleich — das Tutorial bricht an
  Kapitelgrenzen, aber ein einzelnes Kapitel läuft beim ersten
  Durchgang ~5 Minuten und später weniger.

## Kapitel 1 — Infrastruktur und der Minimum-Task

Lege ein frisches Verzeichnis und ein Virtualenv an:

```bash
mkdir swarmq-tutorial && cd swarmq-tutorial
uv venv
source .venv/bin/activate
uv pip install swarmq
```

Lege eine `docker-compose.yml` an:

```yaml title="docker-compose.yml"
services:
  rabbitmq:
    image: rabbitmq:3.13-management
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

Starte es und verifiziere:

```bash
docker compose up -d
docker compose ps
```

Beide Services sollten innerhalb 10–15 Sekunden `healthy` melden.
Die RabbitMQ-Management-UI unter <http://localhost:15672>
(guest/guest) ist optional, aber nützlich, um später in Queues zu
schauen.

Definiere den Minimum-Task:

```python title="tasks.py"
from swarmq import Task, TaskInfo


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["emails"],
    )

    async def run(self, to: str, subject: str) -> str:
        # So tun, als würden wir mit einem echten SMTP-Provider sprechen.
        return f"sent to {to}: {subject!r}"
```

Sanity-Check aus der CLI:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq inspect --module tasks
```

Du solltest eine Zeile sehen:

```text
TASK        QUEUES  DELIVERY       RETRIES  RATE_LIMIT  LOCK  UNIQUE_FOR  UNIQUE_UNTIL  CRON
send_email  emails  at_least_once  0        -           -     -           -             -
```

Findet `swarmq inspect` dein Modul nicht, stelle sicher, dass das
Shell im selben Verzeichnis wie `tasks.py` steht und das
Virtualenv aktiv ist.

### Kumulativer Stand — Ende Kapitel 1

```text
swarmq-tutorial/
├── docker-compose.yml
└── tasks.py
```

## Kapitel 2 — Worker starten, aus Client enqueuen

In **Terminal 1**, mit den Env-Vars noch gesetzt:

```bash
swarmq worker --module tasks --queues emails --log-format human
```

Der Worker loggt `worker_started` und wartet. Schließe dieses
Terminal nicht.

In **Terminal 2** erzeuge das Client-Skript:

```python title="enqueue.py"
import asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend

import tasks  # noqa: F401 — registriert SendEmail


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

Laufen lassen:

```bash
python enqueue.py
```

Du solltest sehen:

```text
client received: sent to alice@example.com: 'Welcome'
```

In Terminal 1 zeigt der Worker-Log `task_succeeded` mit derselben
`task_id` und ihrer `duration_ms`. Der Roundtrip funktioniert.
(`task_received` und `task_started` werden auf DEBUG geloggt; mit
`--log-lifecycle-level INFO` siehst du sie auch.)

### Was passiert ist

1. `enqueue.py` öffnete eine Verbindung zu RabbitMQ und Valkey.
2. `app.run("send_email", ...)` rief `schedule()` auf (publishte
   in die `emails`-Queue) und wartete dann mit einem 10-Sekunden-
   Budget auf `get_result()`.
3. Der Worker konsumierte die Nachricht, rief `SendEmail().run()`
   auf, speicherte das Ergebnis in Valkey und ack'te.
4. `get_result()` wachte auf dem Pub/Sub-Notify auf und gab das
   Ergebnis zurück.

### Kumulativer Stand — Ende Kapitel 2

```text
swarmq-tutorial/
├── docker-compose.yml
├── enqueue.py
└── tasks.py
```

## Kapitel 3 — Retries und ein Rate-Limit hinzufügen

E-Mails gehen an einen unzuverlässigen Provider. Gib dem Task ein
Retry-Budget und ein Rate-Limit, damit ein einzelner Durchrufer
nicht das Kontingent des Dritt-Services sprengt:

```python title="tasks.py"
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

Stoppe den Worker in Terminal 1 (Ctrl+C) und starte ihn neu —
Worker-Prozesse lesen die Task-Definitionen beim Start neu ein:

```bash
swarmq worker --module tasks --queues emails --log-format human
```

Plane einen Burst aus Terminal 2, um beide Features zu trainieren:

```python title="burst.py"
import asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend

import tasks  # noqa: F401


async def main() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend) as app:
        coros = [
            app.run("send_email",
                    to=f"user-{i}@example.com",
                    subject=f"Burst message {i}",
                    timeout=60)
            for i in range(12)
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)
        for i, result in enumerate(results):
            print(i, result)


asyncio.run(main())
```

`python burst.py` laufen lassen. Beobachte Terminal 1. Du solltest
sehen:

- Einige Tasks beenden beim ersten Versuch.
- Einige raisen: eine `task_failed`-Zeile (mit `error_type` und
  `error`), dann `task_retrying` mit wachsendem `delay_ms`-Feld, dann
  schließlich `task_succeeded` beim Retry.
- Landen mehr als 5 Tasks in einer Sekunde, erscheinen
  `task_rate_limited`-Zeilen mit `retry_after_ms`, bevor irgendwann
  `task_succeeded` folgt.

Alle 12 Aufrufe in Terminal 2 drucken irgendwann
`sent to user-N@example.com: 'Burst message N'` — die Retries und
Rate-Limit-Wartezeiten sind für den Aufrufer unsichtbar, nur
längere Roundtrips.

### Kumulativer Stand — Ende Kapitel 3

```python title="tasks.py"
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

## Kapitel 4 — Einen Signal-Handler für Metriken hinzufügen

Ein Signal-Handler ist der leichtgewichtige Weg, jedes Task-
Lifecycle-Event zu beobachten. Füge einen für `TASK_DLQ` hinzu (der
Task wird nach seinen Retries aufgegeben und dead-lettered) und einen
für `TASK_RATE_LIMITED`. `TASK_FAILED` feuerte nach jedem
fehlgeschlagenen Versuch, Retries eingeschlossen, und ist deshalb der
falsche Hook für „diese E-Mail wurde nicht verschickt". Schließe die
Parameterliste jedes Handlers mit `**kwargs` ab: Der Worker übergibt
mehr Schlüssel, als der Handler nennt (zum Beispiel `queue`).

```python title="tasks.py"
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

Handler brauchen eine `SwarmQ`-Instanz zum Registrieren, also
können sie nicht zum Modul-Import-Zeitpunkt leben. Die Worker-CLI
baut intern eine eigene `SwarmQ`, was bedeutet, dass aus
`tasks.py` registrierte Signal-Handler einen Einhänge-Punkt
brauchen.

Der einfachste Einhänge-Punkt: rufe `install_handlers(app)` aus
einem Modul-Attribut, das der Worker evaluiert. Leg es ans Ende
von `tasks.py` und nutze es auch im Client:

```python title="tasks.py (append)"
# Der Worker importiert dieses Modul. Wir haben hier noch keine
# App-Instanz — also exponiere install_handlers() für worker-
# und client-seitigen Code zur Nutzung.
```

Und rufe es vom Client aus:

```python title="enqueue.py"
import asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend

import tasks


async def main() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend) as app:
        tasks.install_handlers(app)
        result = await app.run(
            "send_email",
            to="alice@example.com",
            subject="Welcome",
            timeout=30,
        )
        print("client received:", result)


asyncio.run(main())
```

Wechsle für die Worker-Seite zum programmatischen Entrypoint
statt der CLI, damit du das `SwarmQ`-Objekt besitzt:

```python title="worker.py"
import asyncio
import logging

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
from swarmq.logging import build_formatter

import tasks


async def main() -> None:
    # SwarmQ() konfiguriert kein Logging — das macht dein Programm.
    handler = logging.StreamHandler()
    handler.setFormatter(build_formatter(json=False, colors=False))
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend, queues=["emails"]) as app:
        tasks.install_handlers(app)
        await app.start_worker()


asyncio.run(main())
```

Anders als bei `swarmq worker` gehört das Logging einer App, die
`SwarmQ(...)` einbettet, der App selbst: SwarmQ übergibt seine Events
nur an deine Handler. Ein nacktes `logging.basicConfig(level=logging.INFO)`
zeigt nur die Event-Namen; `build_formatter` aus `swarmq.logging` rendert
auch die Felder (`task_id`, `duration_ms`, …) und deine eigenen
`tutorial`-Zeilen. Die [Logging-Referenz](../reference/logging.md) hat
Rezepte für JSON- und structlog-Setups.

Stoppe den CLI-Worker in Terminal 1 und starte stattdessen
`python worker.py`. Laufe `python burst.py` in Terminal 2 erneut.
Der Worker-Log enthält jetzt die `RATE_LIMIT`-Zeile und — für den
kleinen Anteil Tasks, der alle 3 Retries erschöpft — eine
`DEAD_LETTERED`-Zeile.

### Kumulativer Stand — Ende Kapitel 4

```text
swarmq-tutorial/
├── docker-compose.yml
├── enqueue.py
├── burst.py
├── tasks.py
└── worker.py
```

## Kapitel 5 — Eine DLQ-Route triggern und wiederherstellen

`RuntimeError` retry't bis zur Budget-Erschöpfung. Um beim ersten
Versuch eine DLQ-Route zu triggern, nutze `NoRetry`:

```python title="tasks.py"
from swarmq import NoRetry, Signal, SwarmQ, Task, TaskInfo
# ... (Rest unverändert)

    async def run(self, to: str, subject: str) -> str:
        if to.endswith("@nope.invalid"):
            raise NoRetry(f"rejected recipient: {to}")
        if random.random() < 0.3:
            raise RuntimeError("smtp temporarily unavailable")
        return f"sent to {to}: {subject!r}"
```

Starte den Worker neu, um den neuen Task-Body aufzunehmen.

Sende eine Nachricht, die abgelehnt wird:

```bash
python -c "
import asyncio
from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
import tasks

async def main():
    async with SwarmQ(
        RabbitMQBroker('amqp://guest:guest@localhost:5672/'),
        ValkeyBackend('valkey://localhost:6379/0'),
    ) as app:
        tid = await app.schedule('send_email',
                                  to='spammy@nope.invalid',
                                  subject='nope')
        print('scheduled', tid)

asyncio.run(main())
"
```

Der Worker loggt `task_failed` und `task_sent_to_dlq`, publiziert die
Nachricht nach `emails.XQ` und bestätigt das Original; dein
`TASK_DLQ`-Handler ergänzt seine `DEAD_LETTERED`-Zeile. Inspiziere:

```bash
swarmq dlq list --queue emails
```

```text
TASK_ID                               TASK_NAME    RETRIES  ERROR_PREVIEW                 BODY_BYTES
01HV....                              send_email   -        rejected recipient: ...       104
```

`swarmq dlq inspect` zeigt die vollen Header (inklusive
`x-swarmq-traceback`) und den dekodierten Body. Angenommen du
entscheidest, zu retry'n — nach einem Deploy, der die Invalid-
Domain-Behandlung fixt:

```bash
swarmq dlq retry 01HV....... --queue emails
```

Der Worker nimmt sie sofort auf. Dieses Mal fängt der Fix das
`.invalid`-Suffix ab und der Task beendet erfolgreich.

Alternativ, für Fälle in denen du gar nicht retry'n willst:

```bash
swarmq dlq purge --queue emails
```

löscht jede DLQ-Nachricht in einem Aufruf. Unumkehrbar — nutze es,
wenn du weißt, dass die Nachrichten nicht behaltenswert sind.

### Kumulativer Stand — Ende Kapitel 5

Dein `tasks.py` deckt nun `NoRetry` für harte Ablehnungen ab,
Retries für transiente Fehler, ein Rate-Limit für Throughput-
Disziplin und zwei Signal-Handler für Observability. `worker.py`
lässt den Worker In-Process laufen; `enqueue.py` und `burst.py`
trainieren den Scheduler.

## Zusammenfassung

Du hast eine Task-Queue gebaut, die:

- **Einfache Fehler dedupliziert.** Füge `unique_for=60` zu
  `TaskInfo` hinzu, wenn du willst, dass derselbe
  `(to, subject)`-Aufruf innerhalb 60 s zu einer Nachricht
  kollabiert.
- **Upstream-Contention beschränkt.** `rate_limit="5/s"` wird
  über jeden Worker in der Flotte durchgesetzt.
- **Sich von transienten Fehlern erholt.** `max_retries=3` mit
  exponentiellem Backoff, mit Option zum Short-Circuit via
  `NoRetry`, wenn das Scheitern permanent ist.
- **Poison-Messages isoliert.** DLQ-Routing via `NoRetry` oder
  Erschöpfung; erholbar mit `swarmq dlq retry`.
- **Sichtbar macht, was passiert.** Signal-Handler verwandeln
  Task-Events in Log-Zeilen, Metriken oder Slack-Benachrichtigungen.

Dasselbe Muster skaliert vom Single-Worker auf deinem Laptop zu
einem Multi-Process-Deployment hinter einem Autoscaler — siehe
[`swarmq worker --autoscale`](../reference/cli.md#swarmq-worker).

## Nächste Schritte

- [Middleware](../guides/middleware.md) — wenn ein Signal-Handler
  nicht reicht und du Per-Task-Wrapper brauchst.
- [Locking und Deduplizierung](../guides/locking-and-dedup.md) —
  Ausführungen desselben Keys über die Flotte serialisieren.
- [Dead-Letter-Queues](../guides/dead-letter-queues.md) — der
  volle Operator-Workflow für DLQ-Recovery.
- [Architektur](../explanation/architecture.md) — was tatsächlich
  hinter `schedule()` und `run()` läuft.
