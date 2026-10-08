---
title: SwarmQ
hide:
  - navigation
  - toc
---

# Eine Task-Queue, die dir nicht im Weg steht.

Async-first Python-Tasks auf RabbitMQ und Valkey. Ein Befehl startet
einen Worker, Middleware und Signals, die sich komponieren lassen,
Rate-Limits und Retries, die nicht überraschen.

[Schnellstart :material-arrow-right:](getting-started.md){ .md-button .md-button--primary }
[API-Referenz](reference/api.md){ .md-button }

---

## In fünf Minuten einsatzbereit

Task definieren, Worker starten, aus deiner App enqueuen. Kein
Entrypoint-Boilerplate, keine Config-Dateien.

```python title="tasks.py"
from swarmq.task import Task, TaskInfo


class Greet(Task):
    task_info = TaskInfo(name="greet", queues=["default"])

    async def run(self, who: str) -> str:
        return f"hello, {who}"
```

```bash title="shell"
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module tasks
```

Der [Schnellstart](getting-started.md) führt durch den vollständigen
Roundtrip — Installation, Broker, Worker, Client, Ergebnis.

---

## Alles, was ein echter Worker braucht

<div class="grid cards" markdown>

- :material-check-decagram:{ .lg .middle } **At-least-once-Zustellung**

    ---

    RabbitMQ-gestützte Acks, Retries mit exponentiellem, linearem
    oder festem Backoff, DLQ-Routing bei Budget-Erschöpfung.

- :material-lightning-bolt:{ .lg .middle } **Async von Grund auf**

    ---

    Gebaut auf `asyncio` und `aio-pika`. Semaphor-begrenzte
    Concurrency pro Worker, keine Thread-Hacks.

- :material-layers-triple:{ .lg .middle } **Middleware & Signals**

    ---

    Sechs Middleware-Hooks mit Stack-Semantik, 23 Lifecycle-Signals
    und ein `@on_signal`-Decorator.

- :material-speedometer:{ .lg .middle } **Rate-Limiting**

    ---

    Sliding-Window-Counter via Lua auf Valkey. Deklariere `"100/m"`
    auf dem Task — die Prüfung passiert vor der Ausführung.

- :material-lock:{ .lg .middle } **Locks & Dedup**

    ---

    Task-Level-Mutex und -Semaphor, plus Unique-Task-Keys für
    idempotentes Scheduling.

- :material-console:{ .lg .middle } **Ein-Befehl-Worker**

    ---

    `swarmq worker --module myapp.tasks`. Graceful `SIGTERM`-Drain,
    ein Supervisor, der abgestürzte Worker neu startet, mehrere
    Prozesse, Autoscaling und ein CPU-Pool für rechenlastige Tasks.

- :material-sitemap:{ .lg .middle } **Workflows**

    ---

    Tasks mit `chain`, `group` und `chord` komponieren. Die Worker
    schalten den Workflow weiter; `app.apply(...)` liefert ein Handle,
    auf das du warten, das du abbrechen oder per `workflow_id` wieder
    aufnehmen kannst. Siehe den
    [Workflow-Guide](guides/workflows.md).

- :material-text-box-search:{ .lg .middle } **Strukturiertes Logging**

    ---

    Human- oder JSON-Ausgabe über eine nicht blockierende Queue,
    Task-Kontext an jedem Event, auf Wunsch deine eigene `dictConfig`.
    Siehe die [Logging-Referenz](reference/logging.md).

</div>

---

## Wohin als Nächstes

<div class="grid cards" markdown>

- :material-school:{ .lg .middle } **SwarmQ lernen**

    ---

    Folge dem [First-Email-Worker-Tutorial](tutorials/first-email-worker.md)
    und baue einen kompletten Worker von Grund auf.

- :material-book-open-variant:{ .lg .middle } **Ein Problem lösen**

    ---

    Spring direkt in einen [How-to-Guide](guides/index.md) — Tasks
    definieren, Retries, Rate-Limiting, Locking, Middleware.

- :material-file-tree:{ .lg .middle } **Etwas nachschlagen**

    ---

    Die [API-Referenz](reference/api.md) und die
    [CLI-Referenz](reference/cli.md) dokumentieren jede öffentliche
    Oberfläche.

- :material-head-cog:{ .lg .middle } **Das Design verstehen**

    ---

    [Architektur](explanation/architecture.md) und
    [Zustellungssemantik](explanation/delivery-semantics.md) erklären
    die Trade-offs hinter der Library.

</div>

---

!!! info "Pre-Release"
    SwarmQ ist aktuell in v0.10 und in aktiver Entwicklung. Der Core-
    Roundtrip, Retries, DLQ, Middleware, Signals, Rate-Limiting,
    Locking, Deduplizierung, der Scheduler, Workflows, der
    CPU-Worker-Modus, strukturiertes Logging, Metriken und
    Health-Endpunkte sind drin. APIs können sich vor v1.0 noch
    verschieben — siehe den [Changelog](reference/changelog.md) für
    Breaking Changes.
