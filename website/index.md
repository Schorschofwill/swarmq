---
title: SwarmQ
hide:
  - navigation
  - toc
---

<div class="swarmq-hero" markdown>

![SwarmQ](assets/brand/swarmq-stacked-light.svg#only-light)
![SwarmQ](assets/brand/swarmq-stacked-dark.svg#only-dark)

# The async-first task queue that <span class="swarmq-gold">does not lose your work.</span>

Python tasks on RabbitMQ quorum queues and Valkey. One command runs a supervised,
autoscaling worker; retries, rate limits, locks, workflows and cron are in the core.

[Get started :material-arrow-right:](getting-started.md){ .md-button .md-button--primary }
[Why SwarmQ](#why-swarmq){ .md-button }

<div class="swarmq-badges">
<span>async def all the way</span><span>at-least-once by default</span><span>Python 3.14+</span><span>MIT</span>
</div>

</div>

---

## Ready in five minutes

Define a task, start a worker, schedule from your app. No entry-point boilerplate, no
config files.

=== "tasks.py"

    ```python
    from swarmq import Task, TaskInfo


    class Greet(Task):
        task_info = TaskInfo(name="greet", queues=["default"], rate_limit="100/m")

        async def run(self, who: str) -> str:
            return f"hello, {who}"
    ```

=== "shell"

    ```bash
    export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
    export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

    swarmq worker --module tasks --autoscale 1,4
    ```

=== "app.py"

    ```python
    import asyncio

    from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

    import tasks  # registers Greet


    async def main() -> None:
        app = SwarmQ(
            RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
            ValkeyBackend("valkey://localhost:6379/0"),
        )
        async with app:
            print(await app.run("greet", who="world"))  # hello, world


    asyncio.run(main())
    ```

The [Getting Started](getting-started.md) guide walks through the full round trip –
install, broker, worker, client, result.

---

## Why SwarmQ

<div class="grid cards" markdown>

- :material-lightning-bolt:{ .lg .middle } **Async to the core**

    ---

    Workers are `asyncio` all the way down. Thousands of I/O-bound tasks share one
    event loop with bounded concurrency – no thread pools, no new loop per job.

- :material-shield-check:{ .lg .middle } **At-least-once by default**

    ---

    Quorum queues, ack after execution, a delivery limit, and a
    [dead-letter queue](guides/dead-letter-queues.md) with a recovery CLI. Outages delay
    work; they do not drop it. See [delivery semantics](explanation/delivery-semantics.md).

- :material-cpu-64-bit:{ .lg .middle } **CPU work off the loop**

    ---

    `--pool cpu` runs task bodies in compute children from a pre-warmed forkserver. Hard
    timeouts and cancel kill the child, never the worker. See [CPU mode](guides/cpu-mode.md).

- :material-tune-variant:{ .lg .middle } **Control in the core**

    ---

    Distributed [rate limits](guides/rate-limiting.md), per-key
    [locks and unique tasks](guides/locking-and-dedup.md), priorities, expiry,
    cancellation and progress – atomic in Valkey via Lua.

- :material-sitemap:{ .lg .middle } **Workflows that finish**

    ---

    `chain`, `group` and `chord` with `|` syntax. A failed step wakes every waiter
    instead of hanging. See the [workflows guide](guides/workflows.md).

- :material-clock-outline:{ .lg .middle } **Cron you can trust**

    ---

    IANA time zone per task, a fixed rule for clock changes, leader election with
    failover – no duplicate runs across instances. See
    [scheduling and retries](guides/scheduling-and-retries.md).

- :material-server-network:{ .lg .middle } **Operable from day one**

    ---

    Supervisor by default, autoscaling, a runtime control channel, Prometheus metrics,
    `/health` and `/ready`. See [deploying and upgrading](guides/deployment.md).

- :material-text-box-search:{ .lg .middle } **Logs you can read**

    ---

    Human or JSON through a non-blocking queue, task context on every event, your own
    `dictConfig` or structlog. See the [logging reference](reference/logging.md).

- :material-puzzle:{ .lg .middle } **Extensible**

    ---

    Six [middleware](guides/middleware.md) hooks, 30 lifecycle [signals](guides/signals.md)
    and optional dependency injection with Dishka.

</div>

---

## Where to go next

<div class="grid cards" markdown>

- :material-school:{ .lg .middle } **Learn SwarmQ**

    ---

    Follow the [first-email-worker tutorial](tutorials/first-email-worker.md) to build a
    complete worker from scratch.

- :material-book-open-variant:{ .lg .middle } **Solve a problem**

    ---

    Jump straight to a [how-to guide](guides/index.md) – retries, rate limiting, locking,
    workflows, CPU mode, deployment, testing.

- :material-file-tree:{ .lg .middle } **Look something up**

    ---

    The [API](reference/api.md), [CLI](reference/cli.md) and
    [configuration](reference/configuration.md) references document every public surface.

- :material-head-cog:{ .lg .middle } **Understand the design**

    ---

    [Architecture](explanation/architecture.md) and
    [design decisions](explanation/design-decisions.md) explain the trade-offs behind the
    library.

</div>

---

!!! info "Status: 0.14"
    SwarmQ is pre-1.0 and under active development. Every release passes more than
    7 000 tests (unit, integration, e2e, chaos, property, fuzz) and an acceptance run of a
    separate load-test suite. It is not yet in production use by anyone but the author,
    and APIs may still change before 1.0 – breaking changes are marked in the
    [changelog](reference/changelog.md).
