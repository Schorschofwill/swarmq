# Build your first email worker

By the end of this tutorial you will have a running SwarmQ worker
that accepts email-send requests, enforces a rate limit, retries on
transient failures, routes permanent failures to a dead-letter
queue, and emits a metric on every lifecycle event. You will also
have recovered a failed message by hand. Hands-on time: 45–60
minutes. Nothing below depends on later sections — if you get stuck,
restart at the previous chapter with the cumulative code shown at
its end.

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
    W->>W: Task.run(...) → "sent"
    W->>V: store result
    W->>MQ: ack
    App->>V: read result
    App-->>Script: "sent to <addr>"
```

Three files on your disk, one shell per process:

- `tasks.py` — the `SendEmail` task.
- `enqueue.py` — a one-shot client.
- `docker-compose.yml` — RabbitMQ and Valkey (copy from the repo).

## Prerequisites

- Python 3.11 or newer (3.14 is tested on CI).
- Docker with Compose v2.
- A package manager. Commands below use [`uv`](https://docs.astral.sh/uv/);
  swap to `pip`/`poetry` if you prefer.
- Ten minutes. Grab them up front — the tutorial breaks at chapter
  boundaries, but a single chapter runs ~5 minutes on the first
  pass and less on repeats.

## Chapter 1 — Infrastructure and the minimum task

Create a fresh directory and a virtualenv:

```bash
mkdir swarmq-tutorial && cd swarmq-tutorial
uv venv
source .venv/bin/activate
uv pip install swarmq
```

Drop in a `docker-compose.yml`:

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

Start it and verify:

```bash
docker compose up -d
docker compose ps
```

Both services should report `healthy` within 10–15 seconds. The
RabbitMQ management UI at <http://localhost:15672> (guest/guest)
is optional but useful for peeking at queues later.

Define the minimum task:

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

Sanity check from the CLI:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

swarmq inspect --module tasks
```

You should see one row:

```text
TASK        QUEUES  DELIVERY       RETRIES  RATE_LIMIT  LOCK  UNIQUE_FOR  UNIQUE_UNTIL  CRON
send_email  emails  at_least_once  0        -           -     -           -             -
```

If `swarmq inspect` cannot find your module, make sure the shell is
in the same directory as `tasks.py` and the virtualenv is active.

### Cumulative state — end of Chapter 1

```text
swarmq-tutorial/
├── docker-compose.yml
└── tasks.py
```

## Chapter 2 — Start a worker, enqueue from a client

In **terminal 1**, with the env vars still set:

```bash
swarmq worker --module tasks --queues emails --log-format human
```

The worker logs `worker_started` and waits. Do not close this
terminal.

In **terminal 2**, create the client script:

```python title="enqueue.py"
import asyncio

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend

import tasks  # noqa: F401 — registers SendEmail


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

Run it:

```bash
python enqueue.py
```

You should see:

```text
client received: sent to alice@example.com: 'Welcome'
```

In terminal 1, the worker log shows `task_succeeded` with the same
`task_id` and its `duration_ms`. The round-trip works. (`task_received`
and `task_started` are logged at DEBUG; add
`--log-lifecycle-level INFO` to see them too.)

### What happened

1. `enqueue.py` opened a connection to RabbitMQ and Valkey.
2. `app.run("send_email", ...)` called `schedule()` (publishing to
   the `emails` queue) and then waited on `get_result()` with a
   10-second budget.
3. The worker consumed the message, called `SendEmail().run()`,
   stored the result in Valkey, and ack'd.
4. `get_result()` woke up on the Pub/Sub notify and returned the
   result.

### Cumulative state — end of Chapter 2

```text
swarmq-tutorial/
├── docker-compose.yml
├── enqueue.py
└── tasks.py
```

## Chapter 3 — Add retries and a rate limit

Emails go to an unreliable provider. Give the task a retry budget
and a rate limit so one runaway call does not blow the 3rd-party
quota:

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

Stop the worker in terminal 1 (Ctrl+C), then restart it — worker
processes re-read the task definitions on startup:

```bash
swarmq worker --module tasks --queues emails --log-format human
```

Schedule a burst from terminal 2 to exercise both features:

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

Run `python burst.py`. Watch terminal 1. You should see:

- Some tasks complete on the first attempt.
- Some raise, then `TASK_RETRYING` with a growing `delay` field,
  then eventually `TASK_COMPLETED` on retry.
- When more than 5 tasks land in a single second,
  `TASK_RATE_LIMITED` entries appear with `retry_after_ms` before
  the eventual `TASK_COMPLETED`.

All 12 calls in terminal 2 eventually print
`sent to user-N@example.com: 'Burst message N'` — the retries and
rate-limit waits are invisible to the caller, just longer round
trips.

### Cumulative state — end of Chapter 3

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

## Chapter 4 — Add a signal handler for metrics

A signal handler is the lightweight way to observe every task
lifecycle event. Add one for `TASK_FAILED` (the final-attempt
failure, after retries) and one for `TASK_RATE_LIMITED`:

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
    @app.on_signal(Signal.TASK_FAILED)
    async def on_failed(task_id, task_name, exception):
        log.warning("FAILED %s %s: %r", task_name, task_id, exception)

    @app.on_signal(Signal.TASK_RATE_LIMITED)
    async def on_limited(
        task_id, task_name, retry_after_ms, remaining, rate_limit_count,
    ):
        log.info(
            "RATE_LIMIT task=%s retry_after_ms=%d remaining=%d bounces=%d",
            task_name, retry_after_ms, remaining, rate_limit_count,
        )
```

Handlers need a `SwarmQ` instance to register on, so they cannot
live at module import time. The worker CLI builds its own `SwarmQ`
internally, which means signal handlers registered from `tasks.py`
need a hook.

The simplest hook: call `install_handlers(app)` from a module
attribute the worker evaluates. Put it at the bottom of `tasks.py`
and re-use it from the client:

```python title="tasks.py (append)"
# The worker imports this module. We do not have the app instance
# here yet — so expose install_handlers() for both worker-side and
# client-side code to call.
```

And call it from the client:

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

For the worker side, switch to the programmatic entry instead of
the CLI so you own the `SwarmQ` object:

```python title="worker.py"
import asyncio
import logging

from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
from swarmq.logging import build_formatter

import tasks


async def main() -> None:
    # SwarmQ() does not configure logging — your program does.
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

Unlike `swarmq worker`, an app that embeds `SwarmQ(...)` owns its
logging: SwarmQ only hands its events to your handlers. A bare
`logging.basicConfig(level=logging.INFO)` would show just the event
names; `build_formatter` from `swarmq.logging` also renders the fields
(`task_id`, `duration_ms`, …) and your own `tutorial` lines. The
[Logging reference](../reference/logging.md) has recipes for JSON and
structlog setups.

Stop the CLI worker in terminal 1 and run `python worker.py`
instead. Re-run `python burst.py` in terminal 2. The worker log
now includes the `RATE_LIMIT` line and — for the small fraction of
tasks that exhaust all 3 retries — a `FAILED` line.

### Cumulative state — end of Chapter 4

```text
swarmq-tutorial/
├── docker-compose.yml
├── enqueue.py
├── burst.py
├── tasks.py
└── worker.py
```

## Chapter 5 — Trigger a DLQ route and recover

`RuntimeError` retries until the budget is gone. To trigger a DLQ
route on the first try, use `NoRetry`:

```python title="tasks.py"
from swarmq import NoRetry, Signal, SwarmQ, Task, TaskInfo
# ... (rest unchanged)

    async def run(self, to: str, subject: str) -> str:
        if to.endswith("@nope.invalid"):
            raise NoRetry(f"rejected recipient: {to}")
        if random.random() < 0.3:
            raise RuntimeError("smtp temporarily unavailable")
        return f"sent to {to}: {subject!r}"
```

Restart the worker to pick up the new task body.

Send a message that will be rejected:

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

The worker logs `task_failed` and `task_sent_to_dlq` and nacks the
message into
`emails.XQ`. Inspect:

```bash
swarmq dlq list --queue emails
```

```text
TASK_ID                               TASK_NAME    RETRIES  ERROR_PREVIEW                 BODY_BYTES
01HV....                              send_email   -        rejected recipient: ...       104
```

`swarmq dlq inspect` shows the full headers (including
`x-swarmq-traceback`) and the decoded body. Suppose you decide to
retry — after deploying a fix that handles the invalid domain:

```bash
swarmq dlq retry 01HV....... --queue emails
```

The worker picks it up immediately. This time the fix catches the
`.invalid` suffix and the task completes.

Alternatively, for the cases where you do not want to retry at
all:

```bash
swarmq dlq purge --queue emails
```

deletes every DLQ message in one call. Irreversible — use when you
know the messages are not worth keeping.

### Cumulative state — end of Chapter 5

Your `tasks.py` now covers `NoRetry` for hard rejects, retries for
transient failures, a rate limit for throughput discipline, and
two signal handlers for observability. `worker.py` runs the worker
in-process; `enqueue.py` and `burst.py` exercise the scheduler.

## Wrap-up

You built a task queue that:

- **Deduplicates the easy mistakes.** Add `unique_for=60` to
  `TaskInfo` if you want the same `(to, subject)` call within 60 s
  to collapse to one message.
- **Bounds upstream contention.** `rate_limit="5/s"` is enforced
  across every worker in the fleet.
- **Recovers from transient failures.** `max_retries=3` with
  exponential backoff, with the option to short-circuit via
  `NoRetry` when the failure is permanent.
- **Isolates poison messages.** DLQ routing via `NoRetry` or
  exhaustion; recoverable with `swarmq dlq retry`.
- **Surfaces what is happening.** Signal handlers turn task events
  into log lines, metrics, or Slack notifications.

The same pattern scales from a single worker on your laptop to a
multi-process deployment behind an autoscaler — see
[`swarmq worker --autoscale`](../reference/cli.md#swarmq-worker).

## Next steps

- [Middleware](../guides/middleware.md) — when a signal handler is
  not enough and you need per-task wrappers.
- [Locking and deduplication](../guides/locking-and-dedup.md) —
  serialise executions of the same key across the fleet.
- [Dead letter queues](../guides/dead-letter-queues.md) — the full
  operator workflow for DLQ recovery.
- [Architecture](../explanation/architecture.md) — what actually
  runs behind `schedule()` and `run()`.
