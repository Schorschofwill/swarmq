# Schnellstart

Von einem leeren Verzeichnis zu einem Worker, der deinen ersten Task
verarbeitet, in etwa fünf Minuten. Kein Entrypoint-Boilerplate, keine
Config-Dateien.

## Voraussetzungen

- Python **3.14** oder neuer
- RabbitMQ **4.2 oder 4.3** und Valkey **8** oder neuer — der nächste
  Abschnitt startet beide mit Docker
- Ein Package-Manager; die Beispiele nutzen
  [`uv`](https://docs.astral.sh/uv/)

## Installation

=== "uv"

    ```bash
    uv init hello-swarmq --python 3.14 && cd hello-swarmq
    uv add swarmq
    uv run swarmq --version
    ```

=== "pip"

    ```bash
    python3.14 -m venv .venv && source .venv/bin/activate
    pip install swarmq
    swarmq --version
    ```

=== "poetry"

    ```bash
    poetry add swarmq
    poetry run swarmq --version
    ```

Die folgenden Befehle setzen ein aktives Virtualenv voraus. Mit uv
stellst du ihnen `uv run` voran (`uv run swarmq worker ...`,
`uv run python enqueue.py`).

!!! note "Abhängigkeiten und Extras"
    SwarmQ ist reines Python. Es zieht `aio-pika`, `valkey`, `msgspec`,
    `msgpack`, `structlog`, `croniter` und `tzdata`; alle liefern Wheels
    für Python 3.14, es wird nichts kompiliert. Optionale Extras:
    `swarmq[metrics]` (Prometheus-`/metrics`), `swarmq[reload]`
    (`swarmq worker --reload`), `swarmq[dishka]` (Dependency Injection)
    und `swarmq[fast]` (`orjson` und `libvalkey`; `libvalkey` braucht
    eine C-Toolchain).

## RabbitMQ und Valkey starten { #broker-starten }

Speichere das als `docker-compose.yml`:

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

Warte, bis beide Container `healthy` melden.

!!! tip "Eigenes RabbitMQ und Valkey"
    Überspringe diesen Schritt und passe unten die URLs an. Valkey darf
    keine Keys verdrängen: Betreibe es mit `maxmemory-policy noeviction`
    oder ohne `maxmemory`-Limit, sonst verweigert der Worker den Start
    (Exit `78`). Siehe
    [Valkey-Anforderungen](reference/configuration.md#valkey-anforderungen).

## Ersten Task definieren

Ein Task ist eine Klasse, die von `Task` erbt, sich im Klassenattribut
`task_info = TaskInfo(...)` beschreibt und ihre Arbeit in
`async def run(...)` erledigt. Das Erben registriert den Task; einen
Decorator gibt es nicht.

```python title="myapp/tasks.py"
from swarmq import Task, TaskInfo


class Greet(Task):
    task_info = TaskInfo(
        name="greet",         # so rufen Clients ihn auf; nicht der Klassenname
        queues=["default"],   # in die erste Queue wird veröffentlicht
        max_retries=2,
    )

    async def run(self, who: str) -> str:
        return f"hello, {who}"
```

!!! note
    Clients und Scheduler sprechen den Task über `name` an. Wähle einen
    stabilen String: Eine Umbenennung trifft Nachrichten, die schon unter
    dem alten Namen in der Queue liegen.

## Worker starten

Zeige der CLI dein Task-Modul. Starte sie aus dem Projektverzeichnis:
Der Worker stellt das Arbeitsverzeichnis an den Anfang des Importpfads
(`--app-dir` nennt ein anderes).

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module myapp.tasks --queues default
```

Der Worker importiert `myapp.tasks`, registriert jede `Task`-Subklasse
darin, deklariert die Queue `default` und konsumiert mit bis zu 10
parallelen Tasks (`--concurrency`). Lass ihn laufen.

!!! tip
    `--log-format json` gibt ein JSON-Objekt pro Zeile aus, passend für
    Loki, Datadog oder CloudWatch. ++ctrl+c++ und `SIGTERM` fahren den
    Worker geordnet herunter: Er nimmt keine neuen Nachrichten mehr an
    und lässt laufende Tasks innerhalb von `SWARMQ_SHUTDOWN_TIMEOUT`
    (Standard 30 s, das Gesamtbudget) zu Ende laufen.
    `swarmq worker --check` prüft Modul und Einstellungen, ohne sich zu
    verbinden.

## Aus deiner App enqueuen

Baue in einem zweiten Terminal eine `SwarmQ`-App aus Broker und
Backend, betritt sie mit `async with` und rufe `run()` auf. Das
enqueued den Task per Name und wartet auf das Ergebnis.

```python title="enqueue.py"
import asyncio

from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

import myapp.tasks  # noqa: F401  (registriert "greet" auch in diesem Prozess)


async def main() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/")
    backend = ValkeyBackend("valkey://localhost:6379/0")

    async with SwarmQ(broker, backend) as app:
        result = await app.run("greet", who="world", timeout=10)
        print(result)


asyncio.run(main())
```

```bash
python enqueue.py
```

Der Client sucht den Task in seiner eigenen Registry, um die Queue zu
finden, und importiert deshalb ebenfalls das Task-Modul; ohne den
Import wirft `run()` `TaskNotFoundError`. Der `async with`-Block öffnet
und schließt die Broker-Verbindungen und den Valkey-Pool — ohne ihn ist
nichts verbunden.

### Jetzt enqueuen, später abholen

Willst du nicht warten, teile `run()` in `schedule()` und
`get_result()` auf. `schedule()` gibt die Task-ID zurück, die du
speichern und später aus jedem Prozess auflösen kannst:

```python
async with SwarmQ(broker, backend) as app:
    task_id = await app.schedule("greet", who="world")
    # ... andere Arbeit, oder task_id an einen anderen Prozess geben ...
    result = await app.get_result(task_id, timeout=30)
```

`get_result()` wirft `ResultNotReadyError`, wenn der Timeout zuerst
abläuft, und `TaskError`, wenn der Task fehlgeschlagen ist.

## Roundtrip prüfen

- Der Client gibt `hello, world` aus.
- Der Worker loggt für `greet` eine Zeile `task_succeeded` mit
  `task_id` und `duration_ms`.

Wenn es nicht klappt:

- `UnroutableMessage` im Client: Die Queue gibt es noch nicht. Worker
  deklarieren ihre Queues, starte also zuerst den Worker.
- Der Client wartet bis zu seinem Timeout: Der Worker konsumiert
  `default` nicht, oder die beiden Prozesse nutzen verschiedene Broker-
  oder Backend-URLs.

!!! note "Logging im eigenen Prozess"
    `swarmq worker` richtet das Logging für seine Prozesse ein. Ein
    Prozess, der `SwarmQ(...)` einbettet, wie `enqueue.py`, bekommt von
    SwarmQ keine Logging-Konfiguration; richte sie selbst ein. Um dort
    die Event-Felder von SwarmQ (`task_id`, `duration_ms`, …) zu sehen,
    nutze seinen Formatter:

    ```python
    import logging

    from swarmq.logging import build_formatter

    handler = logging.StreamHandler()
    handler.setFormatter(build_formatter(json=False, colors=False))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    ```

    Die [Logging-Referenz](reference/logging.md) behandelt JSON,
    structlog und die
    [Opt-in-Log-Queue](reference/logging.md#opt-in-log-queue).

## Nächste Schritte

| Thema | Wann du es brauchst |
|---|---|
| [Tasks definieren](guides/defining-tasks.md) | Queue-Routing, Argumente, Timeouts, reservierte Parameternamen |
| [Scheduling und Retries](guides/scheduling-and-retries.md) | `delay`/`eta`, Crons, Retry-Backoff |
| [Dead-Letter-Queues](guides/dead-letter-queues.md) | Fehlgeschlagene Nachrichten: ansehen, wiederholen, löschen |
| [Rate-Limiting](guides/rate-limiting.md) | Quoten von Drittanbietern, Selbstschutz |
| [Locking und Deduplizierung](guides/locking-and-dedup.md) | Immer nur ein Lauf, keine doppelten Enqueues |
| [Signals](guides/signals.md) und [Middleware](guides/middleware.md) | Metriken, Tracing, Audit-Trails |
| [Workflows](guides/workflows.md) | Chains, Groups und Chords aus Tasks |
| [Tasks testen](guides/testing-tasks.md) | Unit-Tests ohne Broker |
| [Deployment](guides/deployment.md) | Rollout, Shutdown, Produktionseinstellungen |

Für einen geführten Durchgang baut das
[Tutorial zum ersten E-Mail-Worker](tutorials/first-email-worker.md)
denselben Roundtrip auf und ergänzt Retries, ein Rate-Limit,
Signal-Handler und die Wiederherstellung aus der Dead-Letter-Queue.
