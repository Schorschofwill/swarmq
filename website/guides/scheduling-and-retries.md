# Scheduling and retries

You want to control **when** a task runs and **how many times** it is
retried on failure. Both live on `TaskInfo` (defaults for every
execution) and on `Client.schedule()` (per-call overrides).

## Prerequisites

- A task class with `task_info = TaskInfo(name=..., queues=...)`.
- A running broker and backend — see [Getting Started](../getting-started.md).

## Immediate vs deferred execution

Default `schedule()` is immediate — the message lands in the queue
and the first ready worker picks it up.

```python
task_id = await app.schedule("send_email", to="a@example.com")
```

Two kwargs defer the publish: `delay=` and `eta=`.

```python
# Run in 30 seconds.
await app.schedule("send_email", to="a@example.com", delay=30)

# Run at a specific moment (must be in the future; 1s clock skew ok).
from datetime import datetime, timedelta, UTC
eta = datetime.now(UTC) + timedelta(minutes=15)
await app.schedule("send_email", to="a@example.com", eta=eta)
```

Deferred tasks do not sit in the RabbitMQ queue — they sit in the
Valkey scheduler store. One worker (the elected leader) polls the
store and publishes to RabbitMQ at fire time. See
[Architecture → Message lifecycle](../explanation/architecture.md#message-lifecycle).

`delay=0` is a plain immediate publish; only a positive `delay` or
any `eta` takes the scheduler path.

## Cron scheduling

For recurring work, set `cron` on `TaskInfo`:

```python
class DailyReport(Task):
    task_info = TaskInfo(
        name="daily_report",
        queues=["reports"],
        cron="0 3 * * *",  # every day at 03:00 UTC
    )

    async def run(self) -> None:
        ...
```

Standard 5-field cron only; extended syntax (`L`, `W`, `#`) is
rejected at class definition. The worker's embedded scheduler picks
up the declaration automatically — no separate cron daemon.

### CLI-defined schedules

Add schedules without redeploying code:

```bash
swarmq schedule add weekly-billing \
  --cron '0 4 * * 1' \
  --task run_billing \
  --kwargs '{"tenant": "acme"}'

swarmq schedule list
swarmq schedule status --limit 5
```

See [CLI → `swarmq schedule`](../reference/cli.md#swarmq-schedule)
for the full grammar.

## Retry policy

Retries are off by default. Enable them on `TaskInfo`:

```python
class ChargeCard(Task):
    task_info = TaskInfo(
        name="charge",
        queues=["payments"],
        max_retries=5,
        retry_backoff="exponential",  # "linear", "fixed" also available
        retry_base_delay=1.0,
    )

    async def run(self, card_id: str, amount: int) -> dict:
        return await stripe.charge(card_id, amount)
```

### Backoff formulas

| Strategy | Delay at attempt *n* (starting from `n=1`) |
|---|---|
| `exponential` (default) | `retry_base_delay × 2^(n-1)` |
| `linear` | `retry_base_delay × n` |
| `fixed` | `retry_base_delay` |

Global cap: `retry_max_delay` (default 3600 s) truncates any
computed delay. Set `SWARMQ_RETRY_MAX_DELAY=...` or pass
`retry_max_delay=...` as a `SwarmQ` kwarg.

### Selective retries

By default, any exception from `run()` burns a retry attempt. To
retry only specific classes, pass `retry_exceptions`:

```python
task_info = TaskInfo(
    name="charge",
    queues=["payments"],
    max_retries=5,
    retry_exceptions=[stripe.RateLimitError, stripe.APIConnectionError],
)
```

Any other exception — `ValueError`, `KeyError`, the card-declined
error — bypasses the retry budget and goes straight to the DLQ.

### Task-body control with `Retry` and `NoRetry`

Inside `run()`, you can override the policy:

```python
from swarmq import Retry, NoRetry

class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email", queues=["default"], max_retries=3,
    )

    async def run(self, to: str) -> None:
        response = await smtp.send(to)
        if response.status == 429:
            raise Retry(delay=60.0)  # Retry in 60 s
        if response.status in (400, 403):
            raise NoRetry(f"unrecoverable: {response.status}")
```

- `Retry(delay=...)` — retry with a specific delay, ignoring the
  configured backoff. `Retry()` (no delay) uses the backoff formula.
- `NoRetry(reason)` — bypass the retry budget, route to DLQ
  immediately.

## Per-call overrides

Everything on `TaskInfo` is a default. `schedule()` takes overrides
for the scheduling-related kwargs:

```python
# Override the queue at schedule time.
await app.schedule("send_email", to="...", queue="priority")

# Override the dedup settings.
await app.schedule("send_email", to="...", unique_for=300)
```

Retry policy cannot be overridden per-call — it is a property of the
task, not the invocation.

## Timeouts

`TaskInfo.soft_timeout` and `TaskInfo.hard_timeout` (both in seconds)
control how long the worker lets `run()` run.

```python
task_info = TaskInfo(
    name="render_report",
    queues=["reports"],
    soft_timeout=240,
    hard_timeout=300,
)
```

- **Soft timeout.** The worker raises `SoftTimeoutError` inside
  `run()` so your task can checkpoint and wrap up.
- **Hard timeout.** The worker cancels the coroutine outright.

`hard_timeout` must be strictly greater than `soft_timeout` when
both are set. `hard_timeout` is also required when a task declares
`lock=...`.

## Task expiry

For time-sensitive work, declare `max_age` so stale tasks never run:

```python
task_info = TaskInfo(
    name="send_otp",
    queues=["auth"],
    max_age=60,  # Abandon if older than 60 s at pickup
)
```

On pickup, if `now - enqueued_at > max_age`, the worker routes the
message to the DLQ with a `TASK_EXPIRED` signal and does not call
`run()`.

## Cancellation

To cancel a task already in-flight (or still in the queue):

```python
cancelled = await app.cancel(task_id)
```

The call is fire-and-forget — it sets a flag in Valkey and returns.
The worker's cancel monitor picks up the flag and raises
`TaskCancelledError` inside the task coroutine. See
[`app.cancel()`](../reference/api.md#cancel) for the stale-flag
caveat.

## Verify

Schedule something with retries and watch the logs:

```python
import asyncio
from swarmq import SwarmQ
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.backend.valkey import ValkeyBackend
import myapp.tasks  # noqa — registers the task

async def main() -> None:
    async with SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    ) as app:
        tid = await app.schedule("flaky", delay=2.0)
        print("scheduled", tid)

asyncio.run(main())
```

The worker log shows a `TASK_ENQUEUED` immediately and a
`TASK_STARTED` / `TASK_RETRYING` / `TASK_COMPLETED` sequence with
the configured delays between attempts.

## See also

- [Delivery semantics](../explanation/delivery-semantics.md) — what
  at-least-once means when retries fire
- [Dead letter queues](dead-letter-queues.md) — what happens after
  `max_retries` is exhausted
- [`TaskInfo.retry_*` reference](../reference/configuration.md#retries)
- [`Retry` / `NoRetry` API](../reference/api.md#exceptions)
