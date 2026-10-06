# Schnellstart

Von einem frischen Virtualenv zu einem laufenden Worker, der Tasks
verarbeitet — in unter fünf Minuten. Kein Entrypoint-Boilerplate,
keine Config-Dateien nötig.

## Voraussetzungen

SwarmQ braucht einen RabbitMQ-Broker und ein Valkey- (oder Redis-)
Backend. Das Repository liefert eine `docker-compose.yml`, die beides
lokal hochfährt.

- Python **3.11+** (3.14 in CI getestet)
- Docker, oder ein laufendes RabbitMQ + Valkey-Paar, das du
  kontrollierst
- Ein Package-Manager — die Beispiele nutzen
  [`uv`](https://docs.astral.sh/uv/)

## Installation

=== "uv"

    ```bash
    uv add swarmq
    uv run swarmq --version
    ```

=== "pip"

    ```bash
    pip install swarmq
    swarmq --version
    ```

=== "poetry"

    ```bash
    poetry add swarmq
    poetry run swarmq --version
    ```

!!! note
    SwarmQ zieht `aio-pika`, `valkey` und `msgpack` als direkte
    Abhängigkeiten. Die Library ist reines Python — keine
    C-Extensions zu bauen.

## Broker starten

Bring RabbitMQ und Valkey hoch. Die Compose-Datei im Repo enthält
Healthchecks, sodass Worker warten, bis beide Services bereit sind.

```bash
docker compose up -d
docker compose ps
```

Beide Container sollten `healthy` melden. Wenn du RabbitMQ und
Valkey woanders laufen lässt, überspringe diesen Schritt und passe
die URLs in den nächsten Abschnitten an.

## Ersten Task definieren

Ein Task ist eine Python-Klasse, die von `Task` erbt und ihre
Identität über einen `TaskInfo`-Descriptor deklariert. Der Body
lebt in einer `async def run(...)`-Methode. Die Registrierung
läuft automatisch über das Subclassing — kein Decorator-Aufruf
nötig.

```python title="myapp/tasks.py"
from swarmq.task import Task, TaskInfo


class Greet(Task):
    # Identität, Queue-Routing und Limits leben hier.
    task_info = TaskInfo(
        name="greet",
        queues=["default"],
        max_retries=2,
    )

    async def run(self, who: str) -> str:
        return f"hello, {who}"
```

!!! note
    Der `name` in `TaskInfo` ist das, worüber Client und Scheduler
    den Task adressieren — nicht der Klassenname. Wähle einen
    stabilen, kollisionsfreien String. Ihn umzubenennen ist ein
    Breaking Change für alle bereits in der Queue liegenden
    Payloads.

## Worker starten

Kein Entrypoint-File, kein `celery -A app worker`-Wrapper. Zeige
der CLI auf dein Task-Modul und fang an zu konsumieren.

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module myapp.tasks --queues default --concurrency 10
```

Der Worker importiert `myapp.tasks`, registriert jede gefundene
`Task`-Subklasse, verbindet sich mit Broker und Backend und beginnt
zu konsumieren.

!!! tip
    Für JSON-Logs, die zu Loki, Datadog oder CloudWatch passen,
    ergänze `--log-format json`. Der Prozess behandelt ++ctrl+c++
    und `SIGTERM` mit einem Graceful-Drain — laufende Tasks werden
    abgeschlossen, bevor der Prozess beendet wird.

!!! note
    `swarmq worker` richtet das Logging für seine eigenen Prozesse ein.
    Eine App, die `SwarmQ(...)` einbettet, bekommt von SwarmQ keine
    Logging-Konfiguration — richte sie selbst ein, z. B. mit
    `logging.basicConfig(level=logging.INFO)`. Siehe
    [Logging-Referenz](reference/logging.md).

## Aus deiner App enqueuen

Auf der Client-Seite baust du eine `SwarmQ`-App aus Broker- und
Backend-Instanzen, betrittst den Async-Context und rufst `run()`
auf — das enqueued den Task per Name und wartet in einem Schritt
auf das Ergebnis.

```python title="enqueue.py"
import asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend

# Das Importieren des Task-Moduls triggert die Klassenregistrierung
# auch auf der Client-Seite, damit die Registry den Task-Namen kennt.
import myapp.tasks  # noqa: F401


async def main() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend) as app:
        result = await app.run("greet", who="world", timeout=10)
        print(result)  # "hello, world"


asyncio.run(main())
```

!!! warning
    Der `async with`-Block verwaltet sowohl die Broker-Verbindung
    als auch den Valkey-Pool. Wer ihn vergisst, leakt AMQP-Channels
    und hält Valkey-Verbindungen bis zum Garbage Collection offen.

### Fire-and-return-later

Wenn du nicht warten willst, teile `run` in `schedule` +
`get_result` auf. `schedule` gibt eine `task_id` zurück, die du
persistieren und später abholen kannst.

```python
async with SwarmQ(broker, backend) as app:
    task_id = await app.schedule("greet", who="world")
    # ... andere Arbeit erledigen oder task_id an anderen Prozess weitergeben ...
    result = await app.get_result(task_id, timeout=30)
```

## Roundtrip verifizieren

Mit dem Worker in einem Terminal und dem Enqueue-Skript in einem
anderen solltest du sehen:

- Der Client gibt `hello, world` aus.
- Der Worker-Log zeigt eine Zeile `task_succeeded` für den
  `greet`-Task, mit ihrer `task_id` und `duration_ms`.

Wenn nichts passiert, prüfe, ob die `--queues`-Liste des Workers
`default` enthält und beide Prozesse auf dieselben Broker- und
Backend-URLs zeigen.

## Nächste Schritte

Du hast jetzt einen Worker, der Tasks Ende-zu-Ende verarbeitet. Von
hier aus decken die Guides die Konzepte ab, die du in jedem
nicht-trivialen Projekt brauchst.

| Thema | Wann du es brauchst | Link |
|---|---|---|
| **Tasks definieren** | Queue-Routing, Retries, Timeouts, Rate-Limits | [Guide](guides/defining-tasks.md) |
| **Middleware** | Querschnittsthemen: Logging, Tracing, Auth | [Guide](guides/middleware.md) |
| **Rate-Limiting** | Third-Party-API-Quoten, Selbstschutz | [Guide](guides/rate-limiting.md) |
| **Retries & DLQ** | Transiente Fehler, Poison-Message-Isolation | [Guide](guides/dead-letter-queues.md) |
| **Signals** | Lifecycle-Hooks, Metriken, Audit-Trails | [Guide](guides/signals.md) |
| **Tasks testen** | Unit-Tests ohne Broker | [Guide](guides/testing-tasks.md) |

Suchst du einen längeren, geführten Walkthrough? Das
[First-Email-Worker-Tutorial](tutorials/first-email-worker.md) baut
denselben Roundtrip auf und erweitert ihn dann um Retries, ein
Middleware und einen Signal-Handler.
