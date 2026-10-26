# Getting Started

From an empty directory to a worker that processes your first task in
about five minutes. No entrypoint boilerplate, no config files.

## Prerequisites

- Python **3.14** or newer
- RabbitMQ **4.2 or 4.3** and Valkey **8** or newer — the next section
  starts both with Docker
- A package manager; the examples use [`uv`](https://docs.astral.sh/uv/)

## Install

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

The commands below assume an active virtualenv. With uv, prefix them
with `uv run` (`uv run swarmq worker ...`, `uv run python enqueue.py`).

!!! note "Dependencies and extras"
    SwarmQ is pure Python. It pulls `aio-pika`, `valkey`, `msgspec`,
    `msgpack`, `structlog`, `croniter` and `tzdata`; all ship wheels for
    Python 3.14, so nothing is compiled. Optional extras:
    `swarmq[metrics]` (Prometheus `/metrics`), `swarmq[reload]`
    (`swarmq worker --reload`), `swarmq[dishka]` (dependency injection)
    and `swarmq[fast]` (`orjson` and `libvalkey`; `libvalkey` needs a
    C toolchain).

## Start RabbitMQ and Valkey { #start-the-broker }

Save this as `docker-compose.yml`:

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

Wait until both containers report `healthy`.

!!! tip "Using your own RabbitMQ and Valkey"
    Skip this step and adjust the URLs below. Valkey must not evict
    keys: run it with `maxmemory-policy noeviction` or without a
    `maxmemory` limit, otherwise the worker refuses to start (exit
    `78`). See [Valkey requirements](reference/configuration.md#valkey-requirements).

## Define your first task

A task is a class that inherits from `Task`, describes itself in a
`task_info = TaskInfo(...)` class attribute and does its work in
`async def run(...)`. Subclassing registers the task; there is no
decorator.

```python title="myapp/tasks.py"
from swarmq import Task, TaskInfo


class Greet(Task):
    task_info = TaskInfo(
        name="greet",         # what clients call; not the class name
        queues=["default"],   # the first queue is where it is published
        max_retries=2,
    )

    async def run(self, who: str) -> str:
        return f"hello, {who}"
```

!!! note
    Clients and the scheduler address the task by `name`. Pick a stable
    string: renaming it breaks messages that are already queued under the
    old name.

## Run a worker

Point the CLI at your task module. Run it from the project directory:
the worker puts the working directory first on the import path
(`--app-dir` names another one).

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module myapp.tasks --queues default
```

The worker imports `myapp.tasks`, registers every `Task` subclass in
it, declares the queue `default`, and starts consuming with up to 10
tasks in parallel (`--concurrency`). Leave it running.

!!! tip
    `--log-format json` gives one JSON object per line for Loki, Datadog
    or CloudWatch. ++ctrl+c++ and `SIGTERM` shut the worker down
    gracefully: it stops taking new messages and lets running tasks
    finish within `SWARMQ_SHUTDOWN_TIMEOUT` (default 30 s, the total
    budget). `swarmq worker --check` validates module and settings
    without connecting.

## Enqueue from your app

In a second terminal, build a `SwarmQ` app from a broker and a backend,
enter it with `async with`, and call `run()`. It enqueues the task by
name and waits for the result.

```python title="enqueue.py"
import asyncio

from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

import myapp.tasks  # noqa: F401  (registers "greet" in this process too)


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

The client looks the task up in its own registry to find the queue, so
it imports the task module as well; without it `run()` raises
`TaskNotFoundError`. The `async with` block opens and closes the broker
connections and the Valkey pool — without it nothing is connected.

### Enqueue now, collect later

To not wait, split `run()` into `schedule()` and `get_result()`.
`schedule()` returns the task ID, which you can store and resolve later,
from any process:

```python
async with SwarmQ(broker, backend) as app:
    task_id = await app.schedule("greet", who="world")
    # ... other work, or hand task_id to another process ...
    result = await app.get_result(task_id, timeout=30)
```

`get_result()` raises `ResultNotReadyError` when the timeout passes
first, and `TaskError` when the task failed.

## Check the round trip

- The client prints `hello, world`.
- The worker logs a `task_succeeded` line for `greet` with its `task_id`
  and `duration_ms`.

If it does not work:

- `UnroutableMessage` in the client: the queue does not exist yet.
  Workers declare their queues, so start the worker first.
- The client waits until its timeout: the worker does not consume
  `default`, or the two processes use different broker or backend URLs.

!!! note "Logging in your own process"
    `swarmq worker` configures logging for its processes. A process that
    embeds `SwarmQ(...)`, like `enqueue.py`, gets no logging setup from
    SwarmQ; configure it yourself. To see SwarmQ's event fields
    (`task_id`, `duration_ms`, …) there, use its formatter:

    ```python
    import logging

    from swarmq.logging import build_formatter

    handler = logging.StreamHandler()
    handler.setFormatter(build_formatter(json=False, colors=False))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    ```

    The [Logging reference](reference/logging.md) covers JSON, structlog
    and the [opt-in log queue](reference/logging.md#opt-in-log-queue).

## Next steps

| Topic | When you need it |
|---|---|
| [Defining tasks](guides/defining-tasks.md) | Queue routing, arguments, timeouts, reserved parameter names |
| [Scheduling and retries](guides/scheduling-and-retries.md) | `delay`/`eta`, crons, retry backoff |
| [Dead letter queues](guides/dead-letter-queues.md) | Failed messages: inspect, retry, purge |
| [Rate limiting](guides/rate-limiting.md) | Third-party quotas, self-protection |
| [Locking and deduplication](guides/locking-and-dedup.md) | One run at a time, no duplicate enqueues |
| [Signals](guides/signals.md) and [Middleware](guides/middleware.md) | Metrics, tracing, audit trails |
| [Workflows](guides/workflows.md) | Chains, groups and chords of tasks |
| [Testing tasks](guides/testing-tasks.md) | Unit tests without a broker |
| [Deployment](guides/deployment.md) | Rollout, shutdown, production settings |

For a guided walkthrough, the
[first email worker tutorial](tutorials/first-email-worker.md) builds
the same round trip and adds retries, a rate limit, signal handlers and
dead-letter recovery.
