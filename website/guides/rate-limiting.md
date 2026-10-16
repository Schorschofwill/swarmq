# Rate limiting

You have a task that must not run more than N times per unit of time —
a webhook quota, a third-party API plan, a crawler politeness rule.
SwarmQ's rate limiter is declarative: put a string on `TaskInfo`, the
worker enforces it globally across every worker sharing the same
Valkey backend.

## Prerequisites

- A running Valkey backend (shared by every worker).
- A task you want to limit.

## Declare the limit

```python
from swarmq import Task, TaskInfo


class SendSMS(Task):
    task_info = TaskInfo(
        name="send_sms",
        queues=["sms"],
        rate_limit="100/m",
    )

    async def run(self, to: str, body: str) -> None:
        await sms_provider.send(to, body)
```

The format is `"N/unit"` — an integer count followed by a slash and
a time unit:

| Unit | Aliases |
|---|---|
| Per second | `s` |
| Per minute | `m`, `min` |
| Per hour | `h`, `hour` |

Examples: `"5/s"`, `"100/m"`, `"1000/h"`. A bad string (wrong unit,
missing number, negative count) raises `ValueError` at class
definition.

## How the limit is enforced

The limit is a **2-period sliding window** implemented as a single
Lua script on Valkey. Every worker that picks up a message checks
the counter atomically:

- **Under the limit** — the counter is incremented and the message
  moves on to the lock check (if the task has one) and `run()`. The
  slot counts from this moment, also when `run()` does not follow (see
  [with a lock](#combining-with-retries-and-locks)).
- **Over the limit** — the worker nacks the message into a delay
  queue and fires `TASK_RATE_LIMITED`. After the computed wait, the
  broker redelivers and the check runs again.

The sliding window means a burst of 100 at `T` plus another 50 at
`T + 30s` under a `100/m` limit will let the first 100 through, queue
the remaining 50 for a partial-window wait, and release them when
enough headroom reappears. There is no hard edge at the minute mark.

## What you see as a consumer

A rate-limited message does not fail. It waits. The `TaskInfo.max_age`
guard is the only thing that can convert a long wait into a DLQ
route — without it, rate-limited messages retry indefinitely.

```python
task_info = TaskInfo(
    name="send_sms",
    queues=["sms"],
    rate_limit="100/m",
    max_age=300,   # Give up after 5 min of waiting
)
```

If Valkey is unreachable for the check (or does not answer within
`SWARMQ_BACKEND_OP_TIMEOUT`), the message is not rate-limited but
deferred through the delay queue with its own backoff: no
`TASK_RATE_LIMITED`, `rate_limit_count` stays as it was, and the
worker logs `task_deferred_backend_unavailable` (`gate="rate_limit"`).
Any other error of the check logs `rate_limit_check_failed` (ERROR)
and requeues the message in place. See
[Delivery semantics](../explanation/delivery-semantics.md#what-aborts-the-ack).

## Global vs per-worker

Every worker process hits the same Valkey counter, so the limit is
**global** regardless of `--processes`, `--concurrency`, or the
number of deployed worker pods. There is no per-worker mode.

If you need a per-worker cap, use `--concurrency` — a single worker
process does not exceed its configured concurrency for any single
task.

## Observability

Hook `Signal.TASK_RATE_LIMITED` to record when limits are active. The
worker dispatches it, so under `swarmq worker` register the handler in
a `--configure` hook (see [Signals](signals.md#registering-a-handler)):

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ
from swarmq.logging import get_logger

log = get_logger("myapp.rate_limit")


def setup(app: SwarmQ) -> None:
    @app.on_signal(Signal.TASK_RATE_LIMITED)
    async def on_rate_limit(
        task_id, task_name, retry_after_ms, remaining, rate_limit_count,
        **kwargs,
    ):
        # A message that keeps bouncing means the limit is far below demand.
        if rate_limit_count >= 10:
            log.warning(
                "myapp_rate_limit_bouncing",
                task_id=task_id, task_name=task_name,
                retry_after_ms=retry_after_ms,
                bounce_count=rate_limit_count,
            )
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```

Payload fields:

- `retry_after_ms` — how long the message will wait in the delay queue.
- `remaining` — estimated remaining budget in the current window.
- `rate_limit_count` — how many times this exact message has been
  requeued by the rate limiter (a bounce counter for operator alerts).

You do not need a handler just to see deferrals: the worker logs every
one itself as a `task_rate_limited` line (INFO, logger
`swarmq.worker.gates.rate_limit`) with `retry_after_ms`, `remaining`
and `rate_limit_count`. End the handler's parameter list with
`**kwargs` so it keeps working when a release adds a key.

When `swarmq[metrics]` is installed and `--metrics-port` is set,
rate-limit events are exported as Prometheus counters as well.

## Choosing the limit

Start slightly below the upstream quota, not at it. Example: if
SendGrid allows 100 req/s, set `rate_limit="90/s"`. Leaving 10% of
headroom tolerates clock drift and burst smoothing.

For limits that change with your plan tier, read the limit from your
config and set it at `TaskInfo` construction — there is no runtime
mutation, but a restart picks up the new value.

## Combining with retries and locks

Rate limiting composes naturally with retries and locks:

- **With retries.** Retry attempts also count against the limit — a
  retry is a second `run()` of the same work.
- **With a lock.** The rate-limit check runs first; the lock (`lock`,
  `max_concurrency`) is tried only after it passes. A rate-limited
  message never attempts the lock. The other way round, a message that
  passes the rate limit and then finds the lock taken has used a slot,
  although `run()` did not run: it goes back with the lock backoff
  (100 ms, doubling up to 60 s) and passes the rate limit again on every
  redelivery. With many copies waiting for one lock, a large part of
  the budget goes to these attempts, and fewer tasks run than the limit
  allows. Leave headroom in `rate_limit`, or keep the number of copies
  that compete for one lock small (for example with `unique_for`).

## Verify

```bash
swarmq inspect --module myapp.tasks
```

The `RATE_LIMIT` column lists the declared string for every task.
Then schedule a burst larger than the limit and watch the worker
logs: some messages complete immediately (`task_succeeded`), others
show `task_rate_limited` lines with `retry_after_ms` and reappear
after the delay.

## See also

- [Signals](signals.md) — registering the `TASK_RATE_LIMITED` handler
- [`TaskInfo.rate_limit` reference](../reference/configuration.md#rate-limiting)
- [Locking and deduplication](locking-and-dedup.md) — the companion
  primitive for serialising execution
