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

A deferred task runs under the ID that `schedule()` returned, so
`get_result()`, `cancel()` and the dedup flag of `unique_for` work as
for an immediate task. The ID is stored in the field `task_id` of the
one-shot entry (`oneshot:<task_id>`). A leader before 0.12 ignores the
field and fires under a new ID: while such a leader holds the role,
`get_result()` on the returned ID times out and `cancel()` has no
effect. Each cron fire gets a new ID.

Dedup of a deferred task is settled when you call `schedule()`. With
`unique_for=` the producer claims the flag right away. With
`unique_for=None` dedup is off for this call: the one-shot entry
records the opt-out, and the leader fires without a dedup claim. If you
leave out `unique_for`, a producer whose registry knows the task
applies `TaskInfo.unique_for` right away: it claims the flag when you
call `schedule()`, for `unique_for` plus the delay, and a call with the
same arguments in that time is a duplicate. Only a producer without a
registry (or one that does not know the task) leaves it to the leader,
which resolves `unique_for` from its own `TaskInfo` at fire time and
skips the task if another run with the same arguments holds the flag.
The producer computes its dedup hash from the arguments before the
[enqueue middleware](middleware.md#deferred-tasks-delayeta) runs; the
leader hashes the stored arguments, so in the producer-without-registry
case `kwargs` that a middleware adds on every call (a `correlation_id`,
say) end up in the hash and such deferred calls are no longer
deduplicated. The opt-out needs a producer and a leader after 0.12.1.
A producer up to 0.12.1 does not record it, and a leader up to 0.12.1
falls back to its own `TaskInfo`; producers often live in other
services, so update them too.

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
default), queue and task name, plus `unique_for` and `unique_until`
when the task sets `unique_for` (from 0.12). Reports carry the Valkey server time,
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
- A cron removed from code stops firing as soon as no live worker
  reports it any more: from 0.12 the leader holds it (see
  [held crons](#crons-nobody-reports-are-held)) and removes it once no
  worker has reported it for `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (default
  24 h; from 0.11.0, see
  [removing crons](#removing-crons-no-app-reports-anymore)). To get
  rid of it at once, use `swarmq schedule remove --permanent NAME` once
  no worker reports it anymore.
- A CLI schedule of the same name still beats the static cron, with
  the warning `scheduler_static_cron_shadowed_by_cli`.
- Crons of other apps on the same Valkey fire without the warning
  `scheduler_task_not_in_local_registry`, as long as a live worker
  reports them.
- `unique_for` of a cron task applies on every leader, also one whose
  own registry does not know the task (another app, another
  `--module`, separate CPU and IO workers). The report carries
  `unique_for` and `unique_until`, and the leader takes both from the
  definition; a definition without them (written before 0.12, or a CLI
  schedule) uses the leader's own `TaskInfo`. This needs a leader on
  0.12 or later; while a leader before 0.12 holds the role, a long cron
  run can overlap the next fire again
  ([unique crons](locking-and-dedup.md#unique-crons)).

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
  that was disabled when it was removed (`[<namespace>:]sq:{sched}:disabled`); when a
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

### Crons nobody reports are held

From 0.12 the leader does not fire a static cron that no live worker
reports any more and that it does not register itself. Before, a cron
whose task was removed from the code kept firing until the prune
period (24 h) ran out, and every fire ended as an unregistered task in
the `.XQ`. Now the leader holds such a cron: it moves the fire to
`now + SWARMQ_SCHEDULER_SEEN_INTERVAL_S` without publishing, on every
poll, until a worker reports the cron again or the leader prunes it.

A cron counts as unowned when all of the following hold:

- the leader has loaded the cron reports in its current term (right
  after it took the lock it waits up to 5 s for that; without reports,
  for example because Valkey was unreachable, it fires as before —
  fail-open);
- a worker from 0.12 has reported the name at some point
  (`[<namespace>:]sq:{sched}:report:known`, a set of names that is only removed
  together with the definition, by the prune or by `swarmq schedule
  remove --permanent`);
- no live report of the name exists;
- the cron is not one of the leader's own static crons;
- the stored definition has `source="static"` (a CLI schedule of the
  same name is never held).

The transition is logged once per cron: `scheduler_cron_held_unowned`
(WARNING) when the hold starts, `scheduler_cron_unowned_released`
(INFO) when the cron fires again or is removed. The signal
`SCHEDULER_CRON_UNOWNED` (`schedule_name`, `task_name`, `held`,
`held_count`) fires on both transitions, and the gauge
`swarmq_scheduler_crons_held{worker_id}` shows how many crons the
leader holds. A held cron does not fire its missed occurrences later:
the next fire is the next occurrence after a worker reports it.

**Mixed versions.** Only a leader from 0.12 holds crons; while a 0.11
app holds the leadership, removed crons fire as before (their fires
end in the `.XQ` with a retention once a 0.12 worker gives up on them,
see [unregistered tasks](dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)).
Crons reported only by workers before 0.12 are never in
`[<namespace>:]sq:{sched}:report:known` and are not held.

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
every worker reports its version with its cron reports. Scheduler
fixes only take effect while the leader has them: a swarmq before
0.10.0 writes no version and does not know the `timezone` field (as
leader it drops the zone of every cron it fires), and a leader before
0.12 lacks, among others, the stable task ID of deferred tasks.

A worker that sees a leader below the **minimum leader version**
(currently 0.12; no version counts as below) on two election ticks in
a row (`SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL`) logs
`scheduler_leader_outdated` once per leader, sends the
`SCHEDULER_LEADER_OUTDATED` signal and sets the gauge
`swarmq_scheduler_leader_outdated{worker_id}` to `1`. When a current
leader follows, the signal comes again with `outdated=False` and the
gauge drops to `0`. During a rolling upgrade from 0.11 the warning is
expected until the leadership moves to a 0.12 worker; the gauge stays
at `1` as long as a 0.11 app on the same Valkey holds the leadership.

`swarmq schedule status` shows `LEADER_VERSION` and the reporting
workers with their versions (see
[CLI reference](../reference/cli.md#swarmq-schedule-status)).

### Fire index

The leader reads at most 100 due entries per poll tick. Entries that
can never fire are taken out of the fire index before that limit
applies, so they cannot hold back due crons behind them:

- an index entry without a definition (for example after a race
  between `put` and `swarmq schedule remove --permanent`);
- a disabled definition that still has a fire time (written by a
  swarmq before 0.10, which planned disabled crons too).

One poll removes at most 1000 such entries; a larger backlog clears
over the next ticks. The definitions themselves stay.

A definition the leader cannot decode (broken JSON, wrong field
types) does not stop the poll either. The leader logs
`schedule_def_malformed_parked` and takes the entry out of the fire
index; the definition stays for inspection. Fix it with
`swarmq schedule add` or clear it with
`swarmq schedule remove --permanent <name>`. A static cron comes back
with the next report of a worker that defines it.

### Leader takeover

A leader that stops cleanly leaves a short-lived handover marker, so
its successor sends `SCHEDULER_LEADER` instead of
`SCHEDULER_LEADER_FAILOVER`. From 0.12 the marker sits under one fixed
key, `[<namespace>:]sq:{sched}:handover`, and the successor reads it with a single
`GETDEL`. Before, it scanned the whole Valkey database, which could
take longer than the lock TTL on a large database and cost the new
leader its lock again. Right after the takeover the leader also renews
its lock once, before the first regular renewal.

Mixed versions keep working: a 0.12 leader also writes the marker
under the old per-owner key that successors before 0.12 scan for, and
a 0.12 successor recognises the marker of a leader before 0.12 through
the leader it saw last. A missed marker only changes the signal.

### Turning the scheduler off on single workers

Every worker runs for scheduler leader by default. `SWARMQ_SCHEDULER=off`
(or `swarmq worker --no-scheduler`) keeps a worker out of the election.
The typical case is local development against a copy of live data: the
developer's worker consumes its queues but must not fire the crons of
production. Embedded workers take the same switch:
`SwarmQ(broker, backend, scheduler="off")`.

```bash
SWARMQ_SCHEDULER=off swarmq worker --module myapp.tasks
# or
swarmq worker --module myapp.tasks --no-scheduler
```

What it changes:

- The leader fires crons, oneshots **and** `delay`/`eta` tasks, because
  `schedule(..., delay=...)` stores them in the scheduler store. If every
  worker on the same Valkey and namespace runs with `off`, all of them
  wait until a worker with `on` starts.
- Pruning and adoption of crons, the check for orphaned queues and the
  hourly Valkey check also run only on the leader.
- The worker keeps reporting its static crons, its version and its
  queues. The leader therefore does not prune the crons of this worker
  after `SWARMQ_SCHEDULER_PRUNE_AFTER_S`, and `swarmq check` does not
  report its queues as orphaned.
- Each worker process logs the WARNING `scheduler_disabled` at start:
  N times with `--processes N`, and again after every recycle.
  `swarmq check` reports `scheduler_disabled` as a warning; any value
  other than `on` or `off` is an error there and at worker start
  (exit `78`).

Workers before this release ignore `SWARMQ_SCHEDULER` and keep running
for leader. `swarmq check` does not check whether any leader is alive.

### Missing queues

Queues are declared by the workers that consume them. From 0.12 a
publish to a queue that does not exist raises `UnroutableMessage`
instead of being confirmed and dropped by the broker. Start the
workers of a **new queue before its producers** — this holds for every
new queue, not only during an upgrade. A producer gets the exception
right away; it is not retried.

The scheduler leader handles a missing queue like this:

| Fire | What happens |
|---|---|
| Cron | The fire is lost and committed, so the cron moves to its next occurrence. `SCHEDULER_FIRE_UNROUTABLE` with `kind="cron_lost"` counts it (`swarmq_scheduler_fires_unroutable_total`). `scheduler_fire_unroutable` is logged at most once per cron and 10 minutes; `scheduler_fire_unroutable_foreign` is added when the cron is reported by a worker of another app (check that all apps sharing the Valkey use the same RabbitMQ vhost). |
| Deferred task (`delay=`/`eta=`) | Retried every 60 s (`scheduler_oneshot_unroutable`, throttled the same way). The definition records the first unroutable attempt in `unroutable_since`. After `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` (default one day) from that attempt the task is dropped: `scheduler_oneshot_unroutable_dropped`, `kind="oneshot_dropped"`, an error result under its task ID (so `get_result()` raises `TaskError`), and its `unique_for` flag is released. |

A worker whose own `{queue}.DQ` or `{queue}.XQ` was deleted while it
ran declares the queue family once on a separate channel and publishes
the retry or DLQ copy again; a deleted
[delay tier](../explanation/delivery-semantics.md#delay-tiers)
`{queue}.DQ.<tier>` is declared again the same way. A workflow step whose next step goes to a
missing queue fails the workflow at once.

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
| `fixed` | `retry_base_delay`, at most `retry_max_delay` |

`exponential` and `linear` multiply the delay by a random factor
between 1 and 2 (jitter), so retries of many messages that failed
together spread out. `fixed` has no jitter.

Global cap: `retry_max_delay` (default 3600 s) bounds any computed
delay. With `exponential` and `linear` a delay above the cap becomes
0.6 to 0.8 × `retry_max_delay`, drawn from the same jitter, so retries
at the cap stay spread out as well. `fixed` waits `retry_base_delay`,
but never longer than `retry_max_delay` (exactly `retry_max_delay` when
the base delay is larger; since 0.12.3, before it waited the full base
delay). Set `SWARMQ_RETRY_MAX_DELAY=...` or pass
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
`Retry` (below) is not filtered: it retries whether or not
`retry_exceptions` lists it.

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
  `Retry` applies independently of `retry_exceptions` but counts
  against `max_retries`: once the budget is used up, and always with
  `max_retries=0`, it ends terminally (DLQ, or the error result when
  dead-lettering is disabled). A negative delay, as a computed
  `reset_at - time.time()` after a 429 easily becomes, is treated as
  `0` (retry immediately) and counts like any other retry. `inf` and
  `nan` raise `ValueError` when `Retry` is created; that `ValueError`
  is an ordinary task error, subject to `retry_exceptions` and the
  backoff.
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

- **Soft timeout.** The worker cancels `run()` (it runs under
  `asyncio.wait_for`). Inside the task this arrives as
  `asyncio.CancelledError`, not as `SoftTimeoutError`. To checkpoint,
  clean up in `finally` or in `except asyncio.CancelledError:` and
  re-raise. The worker then stores `SoftTimeoutError` as a retriable
  error result and skips `post_execution`. A task that swallows the
  `CancelledError` and returns normally produces a success result.
  Only the expiry of `soft_timeout` becomes `SoftTimeoutError`; a
  `TimeoutError` raised by the task itself keeps its class and is
  matched by `retry_exceptions=[TimeoutError]`. See
  [`SoftTimeoutError`](../reference/errors.md#softtimeouterror).
- **Hard timeout.** The worker cancels the coroutine outright. It
  only matters when the task ignores or delays the soft-timeout
  cancellation.

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

`max_age` also bounds how long a duplicate can turn up, so it sets how
long the duplicate protection holds: the done marker of the
[execution guard](../explanation/delivery-semantics.md#execution-guard)
and the completed marker of a cron fire live at least `max_age + 300`
seconds. Set `max_age` on cron tasks and on tasks that must not run
twice; without it, a duplicate that waits behind a backlog longer than
the marker retention (about 65 minutes by default) runs again. The
cost is one small hash per execution in Valkey for `max_age + 300`
seconds (about 100 MB at one million tasks a day with `max_age` of
24 h).

## Cancellation

To cancel a task already in-flight (or still in the queue):

```python
cancelled = await app.cancel(task_id)
```

The call is fire-and-forget — it sets a flag in Valkey and returns.
The worker's cancel monitor checks the flag every
`SWARMQ_CANCEL_POLL_INTERVAL` seconds (default 1) and cancels the
running `run()` coroutine: inside the task the cancel arrives as
`asyncio.CancelledError`, not as `TaskCancelledError`. `finally`
blocks run; if you catch `CancelledError` to clean up, re-raise it.
The task ends as cancelled: no retry, no DLQ, no `post_execution`, no
`on_error` or `post_execute` of the middleware, only the
`TASK_CANCELLED` signal. In CPU mode the worker stops the compute
child (SIGTERM, SIGKILL after one second;
[behaviour changes](cpu-mode.md#behaviour-changes-for-task-authors)).
The client sees `TaskCancelledError` from `get_result()` or
`app.run()`. See [`app.cancel()`](../reference/api.md#cancel) for the
stale-flag caveat.

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
