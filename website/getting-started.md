# Getting Started

From a fresh virtualenv to a running worker processing tasks in under
five minutes. No entrypoint boilerplate, no config files required.

## Prerequisites

SwarmQ needs a RabbitMQ broker and a Valkey (or Redis) backend. The
repository ships a `docker-compose.yml` that brings both up locally.

- Python **3.11+** (3.14 tested on CI)
- Docker, or a running RabbitMQ + Valkey pair you control
- A package manager — examples use [`uv`](https://docs.astral.sh/uv/)

## Install

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
    SwarmQ pulls `aio-pika`, `valkey`, and `msgpack` as direct
    dependencies. The library is pure Python — no C extensions to
    build.

## Start the broker

Bring up RabbitMQ and Valkey. The compose file in the repo includes
healthchecks, so workers wait for both services to be ready.

```bash
docker compose up -d
docker compose ps
```

You should see both containers reporting `healthy`. If you already run
RabbitMQ and Valkey elsewhere, skip this step and adjust the URLs in
the next sections.

## Define your first task

A task is a Python class that inherits from `Task` and declares its
identity through a `TaskInfo` descriptor. The body lives in an
`async def run(...)` method. Subclassing `Task` registers the class
automatically — no decorator call needed.

```python title="myapp/tasks.py"
from swarmq.task import Task, TaskInfo


class Greet(Task):
    # Identity, queue routing, and limits live here.
    task_info = TaskInfo(
        name="greet",
        queues=["default"],
        max_retries=2,
    )

    async def run(self, who: str) -> str:
        return f"hello, {who}"
```

!!! note
    The `name` in `TaskInfo` is what the client and scheduler refer to
    the task by — not the class name. Choose a stable, collision-free
    string. Renaming it is a breaking change for any scheduled payloads
    already in the queue.

## Run a worker

No entrypoint file, no `celery -A app worker`-style wrapper. Point the
CLI at your task module and start consuming.

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module myapp.tasks --queues default --concurrency 10
```

The worker imports `myapp.tasks`, registers every `Task` subclass it
finds, connects to the broker and backend, and starts consuming.

!!! tip
    For JSON logs fit for Loki, Datadog, or CloudWatch, add
    `--log-format json`. The process handles ++ctrl+c++ and `SIGTERM`
    with a graceful drain — in-flight tasks finish before the process
    exits.

## Enqueue from your app

On the client side, build a `SwarmQ` app from broker and backend
instances, enter its async context, and call `run()` — which enqueues
the task by name and waits for the result in one step.

```python title="enqueue.py"
import asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend

# Importing the task module triggers class registration on the client
# side as well, so the registry knows the task name.
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
    The `async with` block manages both the broker connection and the
    Valkey pool. Forgetting it leaks AMQP channels and keeps Valkey
    connections open until garbage collection.

### Fire-and-return-later

When you don't want to wait, split `run` into `schedule` + `get_result`.
`schedule` returns a `task_id` you can persist and pick up later.

```python
async with SwarmQ(broker, backend) as app:
    task_id = await app.schedule("greet", who="world")
    # ... do other work, or hand task_id to another process ...
    result = await app.get_result(task_id, timeout=30)
```

## Verify the round-trip

With the worker running in one terminal and the enqueue script in
another, you should see:

- The client prints `hello, world`.
- The worker log shows a `TASK_STARTED` and `TASK_COMPLETED` pair for
  the `greet` task.

If nothing happens, check that the worker's `--queues` list contains
`default` and that both processes point at the same broker and backend
URLs.

## Next steps

You now have a worker consuming tasks end-to-end. From here, the guides
cover the concepts you'll reach for in any non-trivial project.

| Topic | When you need it | Link |
|---|---|---|
| **Defining tasks** | Queue routing, retries, timeouts, rate limits | [Guide](guides/defining-tasks.md) |
| **Middleware** | Cross-cutting concerns: logging, tracing, auth | [Guide](guides/middleware.md) |
| **Rate limiting** | Third-party API quotas, self-protection | [Guide](guides/rate-limiting.md) |
| **Retries & DLQ** | Transient failures, poison-message isolation | [Guide](guides/dead-letter-queues.md) |
| **Signals** | Lifecycle hooks, metrics, audit trails | [Guide](guides/signals.md) |
| **Testing tasks** | Unit tests without a broker | [Guide](guides/testing-tasks.md) |

Looking for a longer, guided walkthrough? The
[first-email-worker tutorial](tutorials/first-email-worker.md) builds
the same round-trip and then extends it with retries, a middleware,
and a signal handler.
