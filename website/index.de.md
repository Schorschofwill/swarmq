---
title: SwarmQ
hide:
  - navigation
  - toc
---

<div class="swarmq-hero" markdown>

![SwarmQ](assets/brand/swarmq-stacked-light.svg#only-light)
![SwarmQ](assets/brand/swarmq-stacked-dark.svg#only-dark)

# Die async-first Task-Queue, die <span class="swarmq-gold">keine Arbeit verliert.</span>

Python-Tasks auf RabbitMQ-Quorum-Queues und Valkey. Ein Befehl startet einen
überwachten, selbst skalierenden Worker; Retries, Rate-Limits, Locks, Workflows und Cron
sind im Kern enthalten.

[Schnellstart :material-arrow-right:](getting-started.md){ .md-button .md-button--primary }
[Warum SwarmQ](#warum-swarmq){ .md-button }

<div class="swarmq-badges">
<span>durchgehend async def</span><span>standardmäßig at-least-once</span><span>Python 3.14+</span><span>MIT</span>
</div>

<p class="swarmq-shields">
  <a href="https://pypi.org/project/swarmq/"><img alt="PyPI" src="https://img.shields.io/pypi/v/swarmq?color=1F2B47&label=PyPI"></a>
  <img alt="Tests" src="https://img.shields.io/badge/tests-7%20000%2B-1F2B47">
  <img alt="Coverage ≥97 %" src="https://img.shields.io/badge/coverage-%E2%89%A597%25-1F2B47">
  <a href="https://github.com/Schorschofwill/swarmq/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/Schorschofwill/swarmq/ci.yml?branch=main&label=CI"></a>
  <a href="https://github.com/Schorschofwill/swarmq"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-Schorschofwill%2Fswarmq-1F2B47?logo=github"></a>
</p>

</div>

---

## In fünf Minuten einsatzbereit

Task definieren, Worker starten, aus der App einplanen. Kein Entrypoint-Boilerplate,
keine Konfigurationsdateien.

=== "tasks.py"

    ```python
    from swarmq import Task, TaskInfo


    class Greet(Task):
        task_info = TaskInfo(name="greet", queues=["default"], rate_limit="100/m")

        async def run(self, who: str) -> str:
            return f"hello, {who}"
    ```

=== "Shell"

    ```bash
    export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
    export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

    swarmq worker --module tasks --autoscale 1,4
    ```

=== "app.py"

    ```python
    import asyncio

    from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

    import tasks  # registriert Greet


    async def main() -> None:
        app = SwarmQ(
            RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
            ValkeyBackend("valkey://localhost:6379/0"),
        )
        async with app:
            print(await app.run("greet", who="world"))  # hello, world


    asyncio.run(main())
    ```

Der [Schnellstart](getting-started.md) führt durch den ganzen Weg – Installation,
Broker, Worker, Client, Ergebnis.

---

## Warum SwarmQ

<div class="grid cards" markdown>

- :material-lightning-bolt:{ .lg .middle } **Async bis in den Kern**

    ---

    Worker sind durchgehend `asyncio`. Tausende I/O-lastige Tasks teilen sich einen
    Event-Loop mit begrenzter Parallelität – ohne Thread-Pools, ohne neuen Loop je Job.

- :material-shield-check:{ .lg .middle } **Standardmäßig at-least-once**

    ---

    Quorum-Queues, Ack nach der Ausführung, ein Zustelllimit und eine
    [Dead-Letter-Queue](guides/dead-letter-queues.md) mit Recovery-CLI. Ausfälle
    verzögern Arbeit, sie verwerfen sie nicht. Siehe
    [Zustellungssemantik](explanation/delivery-semantics.md).

- :material-cpu-64-bit:{ .lg .middle } **CPU-Last neben dem Loop**

    ---

    `--pool cpu` führt Task-Rümpfe in Rechenkindern aus einem vorgewärmten Forkserver
    aus. Hard-Timeout und Abbruch beenden das Kind, nie den Worker. Siehe
    [CPU-Modus](guides/cpu-mode.md).

- :material-tune-variant:{ .lg .middle } **Steuerung im Kern**

    ---

    Verteilte [Rate-Limits](guides/rate-limiting.md),
    [Locks und eindeutige Tasks](guides/locking-and-dedup.md) je Schlüssel, Prioritäten,
    Ablauf, Abbruch und Fortschritt – atomar in Valkey per Lua.

- :material-sitemap:{ .lg .middle } **Workflows, die enden**

    ---

    `chain`, `group` und `chord` mit `|`-Syntax. Ein fehlgeschlagener Schritt weckt jeden
    Wartenden, statt zu hängen. Siehe [Workflows](guides/workflows.md).

- :material-clock-outline:{ .lg .middle } **Verlässlicher Cron**

    ---

    IANA-Zeitzone je Task, feste Regel für Zeitumstellungen, Leader-Wahl mit Failover –
    keine doppelten Läufe über mehrere Instanzen. Siehe
    [Scheduling und Retries](guides/scheduling-and-retries.md).

- :material-server-network:{ .lg .middle } **Vom ersten Tag an betreibbar**

    ---

    Supervisor als Standard, Autoscaling, ein Kontrollkanal zur Laufzeit,
    Prometheus-Metriken, `/health` und `/ready`. Siehe
    [Ausrollen und Upgraden](guides/deployment.md).

- :material-text-box-search:{ .lg .middle } **Lesbare Logs**

    ---

    Menschlich lesbar oder JSON über eine nicht blockierende Queue, Task-Kontext an jedem
    Ereignis, eigene `dictConfig` oder structlog. Siehe
    [Logging-Referenz](reference/logging.md).

- :material-puzzle:{ .lg .middle } **Erweiterbar**

    ---

    Sechs [Middleware](guides/middleware.md)-Hooks, 30 Lifecycle-[Signals](guides/signals.md)
    und optionale Dependency Injection mit Dishka.

</div>

---

## Wie es weitergeht

<div class="grid cards" markdown>

- :material-school:{ .lg .middle } **SwarmQ lernen**

    ---

    Das [Tutorial zum ersten E-Mail-Worker](tutorials/first-email-worker.md) baut einen
    vollständigen Worker von Grund auf.

- :material-book-open-variant:{ .lg .middle } **Ein Problem lösen**

    ---

    Direkt zu einem [How-to-Guide](guides/index.md) – Retries, Rate-Limiting, Locking,
    Workflows, CPU-Modus, Deployment, Tests.

- :material-file-tree:{ .lg .middle } **Etwas nachschlagen**

    ---

    Die Referenzen zu [API](reference/api.md), [CLI](reference/cli.md) und
    [Konfiguration](reference/configuration.md) beschreiben jede öffentliche Schnittstelle.

- :material-head-cog:{ .lg .middle } **Das Design verstehen**

    ---

    [Architektur](explanation/architecture.md) und
    [Designentscheidungen](explanation/design-decisions.md) erklären die Abwägungen
    hinter der Bibliothek.

</div>

---

!!! info "Stand: 0.14"
    SwarmQ ist vor 1.0 und in aktiver Entwicklung. Jedes Release besteht mehr als 7 000
    Tests (Unit, Integration, e2e, Chaos, Property, Fuzz) und einen Abnahmelauf einer
    eigenen Lasttest-Suite. Produktiv setzt es bisher nur der Autor ein, und APIs können
    sich vor 1.0 noch ändern – Breaking Changes sind im [Changelog](reference/changelog.md)
    markiert.
