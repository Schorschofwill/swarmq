# Rate limiting

You have a task that must not run more than N times per unit of time:
a webhook quota, a third-party API plan, a crawler politeness rule.
SwarmQ's rate limit is declarative. Put a string on `TaskInfo`, and
every worker that shares the same Valkey enforces it together.

## Prerequisites

- A Valkey backend shared by every worker.
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

The format is `"N/unit"`: a whole number of at least 1, a slash and a
time unit. Spaces around the slash are allowed, the unit is
case-insensitive.

| Unit | Spellings |
|---|---|
| Per second | `s` |
| Per minute | `m`, `min` |
| Per hour | `h`, `hour` |

Examples: `"5/s"`, `"100/m"`, `"1000/h"`. A string that does not parse
(unknown unit such as `"100/day"`, missing number, count below 1)
raises `ValueError` when the task module is imported.

## How the limit is enforced

The limit is counted per task name in Valkey, by one atomic Lua
script. It approximates a sliding window from two fixed windows: the
count of the current window plus the count of the previous one,
weighted by how much of it still overlaps. The time comes from
Valkey's clock (`TIME`), so the workers' clocks do not matter.

The worker checks the limit for every delivery, after the execution
guard and the `max_age` check and before the task lock:

- **Under the limit:** the counter goes up and the message moves on to
  the lock (if the task has one) and to `run()`. The slot is used from
  this moment, also when `run()` does not follow (see
  [with a lock](#combining-with-retries-and-locks)).
- **Over the limit:** the worker publishes a copy of the message to the
  delay queue, acknowledges the original and fires `TASK_RATE_LIMITED`.
  The copy waits `retry_after_ms`, the rest of the current fixed
  window, then passes all checks again. In a
  [delay tier](../explanation/delivery-semantics.md#delay-tiers) the
  wait can be somewhat longer. If the copy cannot be published, the
  worker logs `rate_limit_requeue_failed` (ERROR) and hands the
  original back to the broker.

Because a waiting message comes back at the end of a window, while the
previous window still counts almost fully, a large backlog bounces
several times. Under a sustained backlog the throughput can stay
noticeably below the limit. The limit is an upper bound, not a
schedule.

The counter is keyed by the task name. Two apps that register the same
task name on one Valkey share the limit unless each sets its own
[`SWARMQ_NAMESPACE`](../reference/configuration.md#namespace-several-apps-on-one-valkey).

## What you see as a consumer

A rate-limited message does not fail. It waits. The copies are new
messages without broker delivery counters, so the delivery limit never
ends the wait. Only `TaskInfo.max_age` turns a long wait into a DLQ
entry; it counts from the original enqueue time:

```python
task_info = TaskInfo(
    name="send_sms",
    queues=["sms"],
    rate_limit="100/m",
    max_age=300,   # give up after 5 min
)
```

If Valkey is unreachable for the check (or does not answer within
`SWARMQ_BACKEND_OP_TIMEOUT`), the message is not rate-limited. It is
deferred through the delay queue with its own backoff: no
`TASK_RATE_LIMITED`, `rate_limit_count` stays as it was, and the
worker logs `task_deferred_backend_unavailable` (`gate="rate_limit"`).
Any other error of the check, including an error reply from Valkey,
logs `rate_limit_check_failed` (ERROR) and requeues the message in
place, so a lasting error ends in the DLQ through the delivery limit.
See [delivery semantics](../explanation/delivery-semantics.md#what-aborts-the-ack).

## Global, not per worker

Every worker process uses the same Valkey counter, so the limit is
**global**, independent of `--processes`, `--concurrency` or the
number of worker pods. There is no per-worker mode. To cap one worker
process, use `--concurrency`.

## Observability

The worker logs every deferral itself as `task_rate_limited` (INFO,
logger `swarmq.worker.gates.rate_limit`) with `retry_after_ms`,
`remaining` and `rate_limit_count`. With `swarmq[metrics]` installed
and `--metrics-port` set, each deferral also counts in
`swarmq_tasks_total{outcome="rate_limited"}`.

To react in code, handle `Signal.TASK_RATE_LIMITED`. The worker
dispatches it, so under `swarmq worker` register the handler in a
`--configure` hook (see [Signals](signals.md#registering-a-handler)):

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

- `retry_after_ms`: how long the copy waits in the delay queue.
- `remaining`: estimated remaining budget in the current window.
- `rate_limit_count`: how often the rate limit has deferred this
  message (header `x-swarmq-rate-limit-count`), a bounce counter for
  alerts.

End the handler's parameter list with `**kwargs`, so it keeps working
when a release adds a field.

## Choosing the limit

Start slightly below the upstream quota, not at it. If the provider
allows 100 requests per second, set `rate_limit="90/s"`. The headroom
absorbs the approximation of the window and calls your code makes
outside SwarmQ.

The limit is fixed when the task module is imported. For a limit that
depends on your plan tier, read it from your configuration when you
build `TaskInfo`; a restart picks up a new value.

## Combining with retries and locks

- **With retries.** Every retry is a new delivery and passes the rate
  limit again, so retries count against the limit.
- **With a lock.** The rate limit is checked first; the lock (`lock`,
  `max_concurrency`) is tried only after it passes. A rate-limited
  message never tries the lock. The other way round, a message that
  passes the rate limit and then finds the lock taken has used a slot
  although `run()` did not run: it goes back with the lock backoff
  (100 ms, doubling up to 60 s) and passes the rate limit again on
  every return. With many copies waiting for one lock, a large part of
  the budget goes to these attempts, and fewer tasks run than the limit
  allows. Leave headroom in `rate_limit`, or keep the number of copies
  that compete for one lock small (for example with `unique_for`).

## Verify

```bash
swarmq inspect --module myapp.tasks
```

The `RATE_LIMIT` column shows the declared string of every task. Then
schedule a burst larger than the limit and watch the worker logs: some
messages finish at once (`task_succeeded`), others log
`task_rate_limited` with `retry_after_ms` and run after the wait.

## See also

- [Signals](signals.md): registering the `TASK_RATE_LIMITED` handler
- [`TaskInfo.rate_limit` reference](../reference/configuration.md#rate-limiting)
- [Locking and deduplication](locking-and-dedup.md): serialising
  execution
