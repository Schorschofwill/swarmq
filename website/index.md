---
title: SwarmQ
hide:
  - navigation
  - toc
---

# A task queue that gets out of your way.

Async-first Python tasks on RabbitMQ and Valkey. One command to run a
worker, middleware and signals that compose, rate limits and retries
that don't surprise you.

[Get started :material-arrow-right:](getting-started.md){ .md-button .md-button--primary }
[API reference](reference/api.md){ .md-button }

---

## Ready in five minutes

Define a task, start a worker, enqueue from your app. No entrypoint
boilerplate, no config files.

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

The [Getting Started](getting-started.md) guide walks through the full
round-trip — install, broker, worker, client, result.

---

## Everything a real worker needs

<div class="grid cards" markdown>

- :material-check-decagram:{ .lg .middle } **At-least-once delivery**

    ---

    RabbitMQ-backed acks, retries with exponential, linear, or fixed
    backoff, DLQ routing on exhaustion.

- :material-lightning-bolt:{ .lg .middle } **Async from the core**

    ---

    Built on `asyncio` and `aio-pika`. Semaphore-bounded concurrency
    per worker, no thread hacks.

- :material-layers-triple:{ .lg .middle } **Middleware & signals**

    ---

    Six middleware hooks with stack semantics, 23 lifecycle signals,
    and an `@on_signal` decorator.

- :material-speedometer:{ .lg .middle } **Rate limiting**

    ---

    Sliding-window counters via Lua on Valkey. Declare `"100/m"` on the
    task — enforcement happens before execution.

- :material-lock:{ .lg .middle } **Locks & dedup**

    ---

    Task-level mutex and semaphore, plus unique-task keys for idempotent
    scheduling.

- :material-console:{ .lg .middle } **One-command worker**

    ---

    `swarmq worker --module myapp.tasks`. Graceful `SIGTERM` drain,
    a supervisor that restarts crashed workers, multi-process,
    autoscaling and a CPU pool for compute-bound tasks.

- :material-sitemap:{ .lg .middle } **Workflows**

    ---

    Compose tasks with `chain`, `group` and `chord`. Workers advance the
    workflow; `app.apply(...)` returns a handle you can wait on, cancel
    or reattach by `workflow_id`. See the
    [workflows guide](guides/workflows.md).

- :material-text-box-search:{ .lg .middle } **Structured logging**

    ---

    Human or JSON output through a non-blocking queue, task context on
    every event, your own `dictConfig` if you want it. See the
    [logging reference](reference/logging.md).

</div>

---

## Where to go next

<div class="grid cards" markdown>

- :material-school:{ .lg .middle } **Learn SwarmQ**

    ---

    Follow the [first-email-worker tutorial](tutorials/first-email-worker.md)
    to build a complete worker from scratch.

- :material-book-open-variant:{ .lg .middle } **Solve a problem**

    ---

    Jump straight to a [how-to guide](guides/index.md) — defining tasks,
    retries, rate limiting, locking, middleware.

- :material-file-tree:{ .lg .middle } **Look something up**

    ---

    The [API reference](reference/api.md) and
    [CLI reference](reference/cli.md) document every public surface.

- :material-head-cog:{ .lg .middle } **Understand the design**

    ---

    [Architecture](explanation/architecture.md) and
    [delivery semantics](explanation/delivery-semantics.md) explain the
    trade-offs behind the library.

</div>

---

!!! info "Pre-release"
    SwarmQ is currently v0.9 and under active development. The core
    round-trip, retries, DLQ, middleware, signals, rate limiting,
    locking, deduplication, the scheduler, workflows, the CPU worker
    mode, structured logging, metrics and health endpoints are in. APIs
    may still shift before v1.0 — see the
    [changelog](reference/changelog.md) for breaking changes.
