# Build your first email worker

In this tutorial you build a worker that sends (pretend) emails. Step by
step it learns to retry transient failures, respect a rate limit, report
lifecycle events through signal handlers, and move permanent failures to
a dead-letter queue — which you then replay by hand. Plan about 45
minutes. Each chapter ends with the complete state of your files, so you
can pick up again at any chapter boundary.

## What you will build

```mermaid
sequenceDiagram
    participant Script as enqueue.py
    participant App as SwarmQ app
    participant MQ as RabbitMQ
    participant W as Worker
    participant V as Valkey

    Script->>App: await app.run("send_email", ...)
    App->>MQ: publish message
    MQ->>W: deliver
    W->>V: rate-limit check
    W->>W: SendEmail.run(...)
    W->>V: store result
    W->>MQ: ack
    V-->>App: result
    App-->>Script: "sent to <addr>: ..."
```

You work in one directory and two terminals: terminal 1 runs the
worker, terminal 2 runs the client scripts.

## Prerequisites

- Python 3.14 or newer
- Docker with Compose v2
- [`uv`](https://docs.astral.sh/uv/) (or `python3.14 -m venv` and `pip`)

## Chapter 1 — Infrastructure and a first task

Create a project directory and a virtualenv:

```bash
mkdir swarmq-tutorial && cd swarmq-tutorial
uv venv --python 3.14
source .venv/bin/activate
uv pip install swarmq
```

Add a `docker-compose.yml` for RabbitMQ and Valkey:

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

Both services report `healthy` after 10–15 seconds. The RabbitMQ
management UI at <http://localhost:15672> (guest/guest) is optional but
handy for looking at queues later.

Now the task:

```python title="tasks.py"
from swarmq import Task, TaskInfo


class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email",
        queues=["emails"],
    )

    async def run(self, to: str, subject: str) -> str:
        # Pretend this talks to a real SMTP provider.
        return f"sent to {to}: {subject!r}"
```

Check that SwarmQ finds it. `swarmq inspect` only imports the module and
connects to nothing:

```bash
swarmq inspect --module tasks
```

```text
TASK        QUEUES  DELIVERY       RETRIES  RATE_LIMIT  LOCK  UNIQUE_FOR  UNIQUE_UNTIL  CRON
send_email  emails  at_least_once  0        -           -     -           -             -
```

In a terminal you get this table; piped into another program, the same
command prints JSON (`--format` chooses). If the module is not found, run
the command from the directory that contains `tasks.py`.

### State at the end of chapter 1

```text
swarmq-tutorial/
├── docker-compose.yml
└── tasks.py
```

## Chapter 2 — Start a worker, enqueue from a client

In **terminal 1** (virtualenv active), set the connection URLs and start
the worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq worker --module tasks --queues emails
```

The worker logs `worker_started`, declares the queue `emails` and waits.
Start the worker before the first client: a publish to a queue that does
not exist yet fails with `UnroutableMessage`.

In **terminal 2** (`source .venv/bin/activate`), create the client:

```python title="enqueue.py"
import asyncio

from swarmq import RabbitMQBroker, SwarmQ, ValkeyBackend

import tasks  # noqa: F401  (registers SendEmail in this process)


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

In terminal 1, the worker logs `task_succeeded` with the `task_id` and
`duration_ms`. (`task_received` and `task_started` are logged at DEBUG;
start the worker with `--log-lifecycle-level INFO` to see them too.)

### What happened

1. `enqueue.py` connected to RabbitMQ and Valkey when it entered
   `async with`.
2. `app.run("send_email", ...)` looked up the task in its registry,
   published the message to the queue `emails` (`schedule()`), and then
   waited up to 10 seconds for the result (`get_result()`).
3. The worker received the message, called `SendEmail().run(...)`,
   stored the return value in Valkey, and acknowledged the message.
4. `get_result()` was notified through Valkey Pub/Sub and returned the
   value.

### State at the end of chapter 2

```text
swarmq-tutorial/
├── docker-compose.yml
├── enqueue.py
└── tasks.py
```

## Chapter 3 — Retries and a rate limit

The email provider is unreliable and has a quota. Give the task a retry
budget and a rate limit, and simulate a provider that fails 30 % of the
time:

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

- `max_retries=3` — up to three more attempts after the first. Any
  exception counts as retryable unless you restrict it with
  `retry_exceptions`.
- `retry_backoff="exponential"`, `retry_base_delay=1.0` — wait about 1,
  2, then 4 seconds between attempts (each with up to 2× random jitter).
- `rate_limit="5/s"` — at most five starts per second. The limit lives
  in Valkey and applies to all workers together.

The worker loads task code once at startup. Stop it with ++ctrl+c++ and
start it again:

```bash
swarmq worker --module tasks --queues emails
```

In terminal 2, send a burst of twelve emails at once:

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

Watch terminal 1:

- Most tasks succeed on the first attempt (`task_succeeded`).
- A failed attempt logs `task_failed` (with `error_type` and `error`),
  followed by `task_retrying` with `retry_count` and `delay_ms`; a later
  attempt usually succeeds.
- Messages over the limit of five per second log `task_rate_limited`
  with `retry_after_ms` and run a moment later.

In terminal 2, all twelve lines usually read
`sent to user-N@example.com: 'Burst message N'`: retries and rate-limit
waits only make the round trip longer. Now and then a task fails all four
attempts (about 1 in 120). Its line then shows a `TaskError`, and the
worker moves the message to the dead-letter queue — chapter 5 covers
that.

### State at the end of chapter 3

```text
swarmq-tutorial/
├── burst.py
├── docker-compose.yml
├── enqueue.py
└── tasks.py        # with max_retries, backoff and rate_limit
```

## Chapter 4 — Signal handlers

Signal handlers let you react to lifecycle events, for example to count
metrics or alert. Add two at the end of `tasks.py`:

- `Signal.TASK_DLQ` fires once when a task is given up and dead-lettered.
  (`TASK_FAILED` fires after every failed attempt, retries included, so
  it is the wrong hook for "this email was not sent".)
- `Signal.TASK_RATE_LIMITED` fires whenever the rate limit holds a
  message back.

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

Handlers are `async` functions and always end with `**kwargs`: the worker
passes more keys than a handler names (for example `queue`), and later
releases may add more.

A handler belongs to a `SwarmQ` app, and these signals fire in the
worker. `swarmq worker` builds its own app, so handlers on an app you
create in `tasks.py` would never run. Instead, the worker calls a
function of yours with its app before it starts: pass it as
`--configure MODULE:CALLABLE`. Restart the worker in terminal 1:

```bash
swarmq worker --module tasks --queues emails --configure tasks:install_handlers
```

Run `python burst.py` again in terminal 2. The worker log now contains
`RATE_LIMIT` lines from your handler next to SwarmQ's own events, and —
rarely, when a task fails all attempts — a `DEAD_LETTERED` line. The
worker's logging setup covers your own loggers too, so `tutorial` needs
no configuration.

!!! tip "Running the worker inside your own program"
    Instead of the CLI you can start the worker with
    `await app.start_worker()` on an app you build yourself and register
    the handlers on it directly. Your program then owns logging: SwarmQ
    configures none (see the
    [Logging reference](../reference/logging.md)). For a first worker the
    CLI is simpler.

### State at the end of chapter 4

```text
swarmq-tutorial/
├── burst.py
├── docker-compose.yml
├── enqueue.py
└── tasks.py        # plus install_handlers()
```

## Chapter 5 — Dead-letter a message and replay it

Some failures are permanent: retrying a rejected address only wastes
time. Raise `NoRetry` to send the message to the dead-letter queue right
away. Change the import and the start of `run()`:

```python title="tasks.py (excerpt)"
from swarmq import NoRetry, Signal, SwarmQ, Task, TaskInfo

# ... SendEmail.task_info unchanged ...

    async def run(self, to: str, subject: str) -> str:
        if to.endswith("@nope.invalid"):
            raise NoRetry(f"rejected recipient: {to}")
        if random.random() < 0.3:
            raise RuntimeError("smtp temporarily unavailable")
        return f"sent to {to}: {subject!r}"
```

Restart the worker (same command as in chapter 4). Then send a message
that will be rejected. This script uses `schedule()`, which returns the
task ID without waiting:

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

The worker logs `task_failed`, then `task_sent_to_dlq`: it stored an
error result, copied the message to the dead-letter queue `emails.XQ`
and acknowledged the original. Your `TASK_DLQ` handler adds its
`DEAD_LETTERED` line.

Look at the dead-letter queue. The `swarmq dlq` commands need
`SWARMQ_BROKER_URL`, so set it in terminal 2 as well:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
swarmq dlq list --queue emails
```

```text
TASK_ID                               TASK_NAME   RETRIES  ERROR_PREVIEW                            BODY_BYTES
019a1c2e-5f3b-7d40-8a6e-2b9c4d1e0f7a  send_email  -        rejected recipient: spammy@nope.invalid  98
```

`RETRIES` is `-` because the message never went through a retry. Your
task ID and body size will differ. `swarmq dlq inspect <task_id> --queue
emails` shows all headers and the decoded arguments. Listing is safe:
the messages stay in the queue.

Now fix the cause. Say the team decides that invalid addresses should be
skipped, not rejected. Change the check in `run()`:

```python title="tasks.py (excerpt)"
        if to.endswith("@nope.invalid"):
            log.warning("skipping invalid recipient %s", to)
            return f"skipped {to}"
```

Restart the worker so it runs the new code, then replay the message
with the task ID from `dlq list`:

```bash
swarmq dlq retry 019a1c2e-5f3b-7d40-8a6e-2b9c4d1e0f7a --queue emails
```

The worker picks the message up again, logs your `skipping invalid
recipient` line and `task_succeeded`. A replay starts with a fresh retry
budget.

Messages you do not want to keep can be deleted in one go:

```bash
swarmq dlq purge --queue emails
```

This cannot be undone. Unattended, dead-lettered messages expire after
7 days (`SWARMQ_DEAD_LETTER_TTL_S`).

### State at the end of chapter 5

```text
swarmq-tutorial/
├── burst.py
├── docker-compose.yml
├── enqueue.py
├── reject.py
└── tasks.py        # skips invalid recipients, retries the rest
```

## Wrap-up

Your worker now:

- **Retries transient failures** with exponential backoff
  (`max_retries`, `retry_backoff`) and stops early with `NoRetry` when a
  failure is permanent.
- **Respects an upstream quota.** `rate_limit="5/s"` holds across every
  worker that shares the Valkey.
- **Isolates messages it cannot process** in `emails.XQ`, where you
  inspect, replay or purge them with `swarmq dlq`.
- **Reports what happens** through signal handlers registered with
  `--configure`.

One more option to try: `unique_for=60` in `TaskInfo` deduplicates
enqueues. While a task with the same arguments is still waiting or
running (at most 60 seconds), a second `schedule()` returns the first
task's ID instead of sending another email.

The same setup grows from one worker on your laptop to many processes
— see `--processes` and `--autoscale` in
[`swarmq worker`](../reference/cli.md#swarmq-worker).

## Next steps

- [Signals](../guides/signals.md) — every signal and its arguments.
- [Middleware](../guides/middleware.md) — when you need to wrap each
  task, not just observe it.
- [Scheduling and retries](../guides/scheduling-and-retries.md) —
  `delay`/`eta`, crons and backoff details.
- [Dead letter queues](../guides/dead-letter-queues.md) — the full
  operator workflow, retention and alerting.
- [Locking and deduplication](../guides/locking-and-dedup.md) —
  `unique_for` and locks across workers.
- [Architecture](../explanation/architecture.md) — what runs behind
  `schedule()` and `run()`.
