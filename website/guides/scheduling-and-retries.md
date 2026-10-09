# Scheduling and retries

You want to control **when** a task runs and **how many times** it is
retried on failure. Both live on `TaskInfo` (defaults for every
execution) and on `app.schedule()` (per-call overrides).

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
        cron="0 3 * * *",  # every day at 03:00 UTC (no time zone set)
    )

    async def run(self) -> None:
        ...
```

Standard 5-field cron only; extended syntax (`L`, `W`, `#`) is
rejected at class definition. The worker's embedded scheduler picks
up the declaration automatically — no separate cron daemon.

!!! info "Missed fires are skipped, not replayed"
    After scheduler downtime each cron fires exactly **once** as
    catch-up; occurrences missed during the outage never run. The
    scheduler emits a `SCHEDULER_FIRE_SKIPPED` signal (with
    `schedule_name` and `skipped_count`) so important crons can be
    alerted on.

### Time zones

Without a time zone a cron expression is evaluated in UTC: `0 3 * * *`
fires at 03:00 UTC, year-round. Set `timezone` to an IANA zone to have
it fire at the local wall-clock time instead:

```python
class DailyReport(Task):
    task_info = TaskInfo(
        name="daily_report",
        queues=["reports"],
        cron="0 3 * * *",
        timezone="Europe/Berlin",  # 03:00 in Berlin, summer and winter
    )
```

- The zone is checked when the class is defined; an unknown name
  raises `ValueError`. `timezone` without `cron` is rejected as well.
- `SWARMQ_SCHEDULER_TIMEZONE` (`SwarmQConfig(scheduler_timezone=...)`)
  sets a default for every cron task without its own `timezone`. An
  unknown zone there is a `ConfigurationError`.
- A `timezone` on the task always wins over the default, including an
  explicit `timezone="UTC"`. Use that for crons that must stay in UTC
  when the default is set.
- `"UTC"`, `"Etc/UTC"` and no zone at all are the same: the stored
  definition carries no zone, so existing schedules keep their
  fingerprint when you upgrade.
- swarmq depends on `tzdata`, so zones resolve in slim images without
  `/usr/share/zoneinfo`.

Prefer the zone per task. A global default that changes in a different
deploy than the expressions it affects shifts every cron of that app by
the UTC offset.

Crons created with `swarmq schedule add --cron` are always UTC; the CLI
has no zone option yet.

### Clock changes

In a zone with daylight saving time, one night a year has an hour
twice (autumn) and one night has an hour missing (spring). The rule
depends on whether the hour field of the expression contains a `*`
(also `*/n`; `@hourly` and the other `@` aliases count in their
five-field form, so `@hourly` is `0 * * * *` and `@daily` is
`0 0 * * *`):

| Where the fire falls | Hour field without `*` (e.g. `30 2 * * *`) | Hour field with `*` (e.g. `15 * * * *`, `*/30 * * * *`) |
|---|---|---|
| normal time | fires | fires |
| doubled hour, first pass | fires | fires |
| doubled hour, second pass | does not fire; the next fire is searched from the end of the doubled hour | fires (follows real time) |
| spring gap | fires once at the next existing time | croniter's behaviour, see below |

Examples for `Europe/Berlin` in 2026:

- `30 2 * * *` fires on 25 October at 02:30 CEST only, not again at
  02:30 CET; on 29 March, when 02:30 does not exist, it fires at 03:00
  CEST.
- `15 * * * *` fires on 25 October at 02:15 CEST and again at 02:15
  CET (one hour of real time apart). On 29 March 02:15 is skipped: the
  next fire after 01:15 CET is 03:15 CEST.
- `45 */2 * * *` fires on 29 March at 00:45, then at 03:00 instead of
  the missing 02:45, then at 04:45.

With a `*` in the hour field the spring gap follows croniter and can
depend on the point the calculation starts from. In
`Australia/Lord_Howe`, where the clock moves by half an hour,
`15 * * * *` fires on 4 October 2026 at 01:15 (+10:30) and then at
02:30 (+11:00). If the calculation starts during the second pass of a
doubled hour, a cron without `*` in its hour field fires next on the
following day: `30 2 * * *` started at 02:10 CET on 25 October fires
next on 26 October at 02:30.

A cron whose next fire cannot be calculated (for example a zone the
leader's `tzdata` does not know) blocks no other cron: the leader logs
`scheduler_next_fire_failed`, takes the entry out of the fire index
and keeps the definition. As long as a live worker reports the cron,
the next adoption tick plans it again once the fire can be calculated.

`swarmq.scheduler.next_fire_times` calculates fires with the same rule
the scheduler uses:

```python
from datetime import UTC, datetime
from swarmq.scheduler import next_fire_times

next_fire_times("0 3 * * *", datetime.now(UTC), timezone="Europe/Berlin", count=3)
# [datetime(…, 3, 0, tzinfo=ZoneInfo('Europe/Berlin')), …]
```

`start` must be timezone-aware (a naive `start` raises `ValueError`);
the fires come back aware in the given zone (UTC without `timezone`). An invalid expression raises croniter's
`CroniterBadCronError`.

### Cron reports and the leader

Every worker reports the static crons of its own task registry to
Valkey: at start and then every `SWARMQ_SCHEDULER_SEEN_INTERVAL_S`
seconds (default `60`). A report holds the name, the full definition
and a fingerprint over the fields that come from code: cron
expression, time zone (already resolved from the task or the worker's
default), queue and task name. Reports carry the Valkey server time,
so clock skew between workers does not matter.

The scheduler leader adopts reported crons on the same interval, right
after it takes over and then every interval. For each name it picks,
among the live reports, the definition that appeared most recently. A
report is live while it was refreshed within the last three intervals.
The leader checks the definition (zone, expression) before it writes
it; an invalid report leaves the stored definition untouched and logs
`scheduler_cron_report_invalid` on every tick.

What this means in practice:

- A cron changed in the code of any app, also one whose workers never
  become leader, takes effect within two intervals after the first
  worker with the new code starts.
- A fire that is already due when a definition changes is not lost:
  it fires once with the new content. A fire that is not due yet is
  recalculated.
- During a rolling deploy the newer code state wins. An old worker
  that restarts later does not reset it, because its definition is
  not new. A definition counts as new again once no worker has
  reported it for three intervals, so a revert from B back to A wins
  as well.
- A schedule disabled with `swarmq schedule remove NAME` stays
  disabled when its code changes.
- A static cron that a live worker reports comes back within one
  interval after `swarmq schedule remove --permanent`. It only stays
  gone once no code reports it anymore.
- A cron removed from code stops firing once no worker has reported
  it for `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (default 24 h; from 0.11.0,
  see [removing crons](#removing-crons-no-app-reports-anymore)). To get
  rid of it at once, use `swarmq schedule remove --permanent NAME` once
  no worker reports it anymore.
- A CLI schedule of the same name still beats the static cron, with
  the warning `scheduler_static_cron_shadowed_by_cli`.
- Crons of other apps on the same Valkey fire without the warning
  `scheduler_task_not_in_local_registry`, as long as a live worker
  reports them. The leader still does not know their `TaskInfo`, so
  `unique_for` of such a task has no effect on its cron fires.

The leader deletes reports that were not refreshed for 7 days, or for
twice `SWARMQ_SCHEDULER_PRUNE_AFTER_S` if that is longer.

Use the same `SWARMQ_SCHEDULER_SEEN_INTERVAL_S` in every app on one
Valkey: the leader judges liveness with its own interval.

### Removing crons no app reports anymore

From 0.11.0 the leader removes a static cron that was reported at
least once and that no worker has reported for
`SWARMQ_SCHEDULER_PRUNE_AFTER_S` seconds (default `86400`, 24 h). It
checks on every adoption tick and logs `scheduler_cron_pruned`. The
definition and its fire are removed in one atomic step, and only while
the definition is unchanged and every report of the name is still too
old.

- **Crons that were never reported stay.** Old entries from before
  0.10.0 and crons of apps that still run a swarmq before 0.10.0 have
  no report. The leader never removes them on its own; it logs
  `scheduler_cron_prune_candidate` once per name and leader run.
  `swarmq schedule prune --orphans` removes them on purpose (see
  [CLI reference](../reference/cli.md#swarmq-schedule-prune)).
  `swarmq schedule list` shows the newest report per cron in the
  column `LAST_SEEN`: a date, `never` for a static cron without any
  report, `-` for CLI schedules.
- **Apps that pause stay safe within the period.** A cron comes back
  with its next report; App B's crons stay as long as one of B's
  workers reports within `SWARMQ_SCHEDULER_PRUNE_AFTER_S`. Raise the
  period if an app may be down longer.
- **The leader's settings apply.** `SWARMQ_SCHEDULER_PRUNE` and
  `SWARMQ_SCHEDULER_PRUNE_AFTER_S` are read by whichever worker is
  scheduler leader, from any app on the Valkey. Set the same values in
  every app; raising the period in one app alone has no effect while
  another app leads.
- **Disabled crons come back disabled.** The leader remembers a cron
  that was disabled when it was removed (`sq:{sched}:disabled`); when a
  worker reports it again, it is created disabled and the leader logs
  `scheduler_cron_revived_disabled` (WARNING). This also applies after
  `schedule prune --orphans`. If the name now belongs to a different
  cron, `swarmq schedule remove --permanent <name>` clears the mark;
  the next report creates it enabled.
- **CLI schedules are never touched.**
- To get rid of a cron at once, `swarmq schedule remove --permanent`
  still works.

`SWARMQ_SCHEDULER_PRUNE` switches it: `on` (default) removes, `log`
only logs `scheduler_cron_prune_skipped` once per name, `off` does
nothing. The period must be at least three times
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`.

!!! warning "Every app on the Valkey must run 0.10.0 or later"
    Only workers from 0.10.0 on report their crons. A cron that was
    reported once and is now served only by a worker before 0.10.0 —
    for example after an app rolled back — counts as silent and is
    removed after the period, although the app still runs. Upgrade
    every app on the same Valkey to at least 0.10.0 before workers on
    0.11.0 become leader, or set `SWARMQ_SCHEDULER_PRUNE=log` in the
    0.11.0 apps until the last app is upgraded; then watch
    `scheduler_cron_prune_skipped` for crons that would have been
    removed.

### Conflicts

When several definitions of one cron name stay live side by side for
more than 10 minutes, the leader logs `scheduler_cron_conflict` (with
all live fingerprints and the chosen one) on every adoption tick,
sends the `SCHEDULER_CRON_CONFLICT` signal and counts
`swarmq_scheduler_cron_conflicts_total{schedule_name}`. The definition
that appeared most recently keeps firing. A short overlap during a
rolling deploy is not a conflict. The typical cause is tasks with the
same name in two apps on one Valkey; give them different names.

### Leader version

The leader writes its swarmq version next to the leader lock, and
every worker reports its version with its cron reports. A swarmq
before 0.10.0 writes no version and does not know the `timezone`
field: as leader it drops the zone of every cron it fires and fires
it in UTC again.

A worker that sees such a leader (no version, or one before 0.10.0) on two
election ticks in a row (`SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL`) logs `scheduler_leader_outdated` once per
leader, sends the `SCHEDULER_LEADER_OUTDATED` signal and sets the
gauge `swarmq_scheduler_leader_outdated{worker_id}` to `1`. When a
current leader follows, the signal comes again with `outdated=False`
and the gauge drops to `0`. A leader on 0.10.0 or later that is older
than the worker (a rolling upgrade) knows time zones and triggers no
warning.

`swarmq schedule status` shows `LEADER_VERSION` and the reporting
workers with their versions (see
[CLI reference](../reference/cli.md#swarmq-schedule-status)).

### Migrating hand-converted UTC crons

Before 0.10.0, crons that should fire at a local time had to be
converted to UTC by hand. In the EU such an expression is right for
summer time only: after the clocks go back on 25 October 2026 at
01:00 UTC it fires an hour late. To move such crons to a zone:

1. **Upgrade every app on the same Valkey to 0.10.0 first, without
   changing any cron.** A leader before 0.10.0 drops `timezone` on
   every fire. Check with `swarmq schedule status` that
   `LEADER_VERSION` and every entry under `WORKERS` show 0.10.0 or
   later before you go on.
2. **Per app, set the zone on each task and change the expression
   back to local time in the same commit.** A zone without the
   converted expression, or the other way round, shifts the cron by
   the UTC offset. Prefer `timezone=` per task over
   `SWARMQ_SCHEDULER_TIMEZONE`; if you use the variable, change it in
   the same deploy as the expressions. Watch for these pitfalls:
    - **Weekday and day of month.** A conversion that crossed midnight
      also shifted the day: `30 22 * * 0` (Sunday 22:30 UTC) is
      `30 0 * * 1` (Monday 00:30) in `Europe/Berlin`. The same applies
      to the day of month and to month boundaries.
    - **`*/n` and ranges in the hour field.** `0 */2 * * *` happens to
      fire at the same instants in UTC and in Berlin, `0 */5 * * *`
      does not: the hand-converted hours were 0, 5, 10, 15, 20 UTC,
      which is `0 2,7,12,17,22 * * *` in Berlin summer time. Write such
      hours out explicitly and check them with the recipe below.
    - **`*` in the hour field.** An expression like `*/15 * * * *`
      without day restrictions fires at the same instants in UTC and
      in a zone whose offset is whole hours; it needs no zone. Note
      the different rule for the doubled hour in the
      [table above](#clock-changes).
3. **Deploy before 25 October 2026, 01:00 UTC**, and check the
   equivalence before you deploy: until the switch, the new
   expression with zone must produce the same fires as the old UTC
   expression (recipe below).
4. **Do not deploy between 24 October 22:00 and 25 October 04:00
   UTC**, so no rolling deploy of crons overlaps the switch.
5. **Rollback:** roll back the expressions together with the zone, in
   one commit. swarmq before 0.10.0 does not accept `timezone=`, so
   rolling back swarmq itself also means going back to the
   hand-converted UTC expressions.

Recipe for the equivalence check, for example as a test in your
project:

```python
from datetime import UTC, datetime

from swarmq.scheduler import next_fire_times

# End of summer time in the EU: 2026-10-25 01:00 UTC.
SWITCH = datetime(2026, 10, 25, 1, 0, tzinfo=UTC)


def fires_until(cron: str, start: datetime, end: datetime, timezone: str | None = None) -> list[datetime]:
    """Every fire of *cron* after *start* and before *end*, in UTC."""
    fires = []
    while True:
        (fire,) = next_fire_times(cron, start, timezone=timezone)
        fire = fire.astimezone(UTC)  # compare instants, not wall clocks
        if fire >= end:
            return fires
        fires.append(fire)
        start = fire


def test_daily_report_keeps_its_fires() -> None:
    start = datetime.now(UTC)
    old = fires_until("0 1 * * *", start, SWITCH)  # hand-converted from 03:00 CEST
    new = fires_until("0 3 * * *", start, SWITCH, timezone="Europe/Berlin")
    assert new == old
```

Convert to UTC before comparing: Python never treats two datetimes in
different zones as equal when one of them lies in the doubled hour,
even if they denote the same instant. After the switch the two
expressions differ by design.

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

On pickup, if `now - enqueued_at >= max_age`, the worker logs
`task_expired` (WARNING, with `age_s` and `max_age_s`), fires the
`TASK_EXPIRED` signal, routes the message to the DLQ and does not call
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

The client logs nothing for the enqueue; `scheduled <task_id>` is
your own `print`. After the two-second delay the worker log shows,
for every failed attempt, a `task_failed` line (with `error_type` and
`error`) followed by `task_retrying` (with `retry_count`,
`max_retries` and `delay_ms`), with the configured delays between
the attempts, and `task_succeeded` once an attempt succeeds. Add
`--log-lifecycle-level INFO` to the worker to also see `task_received`
and `task_started` for every attempt.

## See also

- [Delivery semantics](../explanation/delivery-semantics.md) — what
  at-least-once means when retries fire
- [Dead letter queues](dead-letter-queues.md) — what happens after
  `max_retries` is exhausted
- [`TaskInfo.retry_*` reference](../reference/configuration.md#retries)
- [`Retry` / `NoRetry` API](../reference/api.md#exceptions)
