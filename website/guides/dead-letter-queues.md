# Dead letter queues

A task exhausted its retries, raised `NoRetry`, or expired before
pickup. The message is now in the per-queue dead-letter queue —
`{queue}.XQ` — where it stays until an operator intervenes or its
retention runs out (7 days by default, see
[retention](#retention-dead-lettered-messages-expire)). This guide
covers how to inspect, retry, and purge DLQ messages, how long they
stay, and how to switch dead-lettering of task failures off.

## Prerequisites

- `SWARMQ_BROKER_URL` pointed at the same RabbitMQ you consume from.
- `swarmq` CLI installed (`pip install swarmq`).

## When messages land in the DLQ

| Trigger | Header on the DLQ message | With dead-lettering of task failures off |
|---|---|---|
| Retry exhaustion (`max_retries` reached — incl. `max_retries=0`, where the FIRST failure is already terminal) | `x-swarmq-retry-count` at the limit, `x-swarmq-traceback` with the error message | not dead-lettered: error result stored, acked |
| `raise NoRetry("reason")` from task body | `x-swarmq-traceback` with the error message | not dead-lettered: error result stored, acked |
| Exception not in `retry_exceptions` | `x-swarmq-traceback` with the error message | not dead-lettered: error result stored, acked |
| `x-delivery-limit=20` tripped by RabbitMQ, or the worker's own delivery-count check one delivery earlier | `x-death` from broker; from the worker `x-swarmq-traceback` with `WorkerHungError` | still dead-lettered |
| The worker handed the delivery back (`nack(requeue=True)`) until `x-acquired-count` reached the delivery limit (RabbitMQ 4.3) | `x-swarmq-traceback` with `RedeliveryLimitError` | still dead-lettered |
| The same execution expired `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` times without release ([execution guard](../explanation/delivery-semantics.md#execution-guard)) | `x-swarmq-traceback` with `WorkerHungError` | still dead-lettered |
| `TaskInfo.max_age` exceeded at pickup | none of its own — the message keeps its original headers; the stored error result says `task expired: age …s exceeds max_age …s` | still dead-lettered |
| Lock-render failure (template kwarg missing, empty render) | `x-swarmq-lock-render-error` with the error message, `x-swarmq-traceback` | still dead-lettered |
| Wait for a task lock ended (stalled holder or `SWARMQ_LOCK_WAIT_MAX_S`) | `x-swarmq-lock-name`, `x-swarmq-traceback` with `LockWaitExceeded` | still dead-lettered |
| Task name not registered in the worker, after the grace period `SWARMQ_UNKNOWN_TASK_GRACE_S` (default 15 min; `0` = at first contact), see [unregistered tasks](#unregistered-tasks-and-rolling-deploys) | `x-swarmq-traceback` with `task '…' is not registered`; the stored error result says the same | still dead-lettered |
| Body over the size cap, undecodable body, broken middleware-chain factory | `x-swarmq-traceback` with `<reason>: <error>` (e.g. `undecodable: Expecting value …`) | still dead-lettered |
| Failed dead-letter publish (fallback of the worker's own copy) | none from SwarmQ — only the broker's `x-death` | still dead-lettered |

Every message the worker dead-letters itself also carries
`x-swarmq-dead-lettered-at` (Unix milliseconds) and a per-message
`expiration`, see [retention](#retention-dead-lettered-messages-expire).
The third column is explained under
[switching dead-lettering off](#switching-off-dead-lettering-of-task-failures).

The DLQ is a regular RabbitMQ queue — you can inspect it with
`rabbitmqadmin` or the management UI, but the `swarmq dlq` CLI
renders it in SwarmQ-aware shape.

## Three ways into the `.XQ`

Messages reach `{queue}.XQ` on three different paths, and they are
not equally visible. Alerting on "queue depth > 0" catches all three;
knowing which path fired tells you what to fix.

| Path | Who moves the message | Signal | Metric | Headers |
|---|---|---|---|---|
| **Dead-lettering by the worker** — retries exhausted, `NoRetry`, `max_age`, lock errors, delivery limit about to trip, orphan limit of the execution guard, unregistered task after its grace period | Worker publishes to `.XQ` and acks the original | `TASK_DLQ` (`task_id`, `task_name`, `queue`, `error`) for retries exhausted, `NoRetry`, the delivery limit, the orphan limit, lock errors (render failure, end of a lock wait) and the unregistered task (there followed by `TASK_REJECTED` with `unregistered_task`); `TASK_EXPIRED` for `max_age` | `swarmq_tasks_total{outcome="dlq"}` | SwarmQ diagnostics (`x-swarmq-retry-count`, `x-swarmq-traceback`, `x-swarmq-lock-*`, see the table above); an error result is stored |
| **Rejection by the worker** — the message could not be turned into a task | Worker publishes a copy with retention to `.XQ` and acks the original; if the copy fails, it rejects without requeue and the broker's dead-letter exchange routes it | `TASK_REJECTED` (`queue`, `reason`, `task_id`, `task_name`, `error`) | `swarmq_tasks_rejected_total{queue,reason}` | `x-swarmq-traceback` with `<reason>: <error>` and the retention stamps (`x-swarmq-dead-lettered-at`, `x-swarmq-dead-letter-ttl-ms`); no error result. Only the fallback carries the broker's `x-death` instead. `task_id`/`task_name` are `None` when the body never decoded |
| **Dead-lettering by the broker** — `x-delivery-limit` after repeated worker crashes, message TTL, queue length limit | RabbitMQ itself | none — no worker saw the terminal delivery | `swarmq_dlq_messages{queue}` (depth gauge sampled by the worker) | only `x-death` |

`reason` on the rejection path is one of `body_too_large`,
`undecodable`, `middleware_chain_factory`, `dlq_publish_failed`
(`swarmq dlq list` shows the first three in `ERROR_PREVIEW`, e.g.
`undecodable: …`);
`unregistered_task` is the exception that comes from the first path
(from 0.12, see below). `unregistered_task` after a deploy usually means
the worker and the producer run different versions of the task module;
`body_too_large` and `undecodable` point at the producer.

The third path is the one to watch when the other two are quiet: a
task that reliably kills the process (SIGSEGV, OOM) never reaches
`TASK_FAILED`. From 0.5.0 the worker dead-letters such a message itself
from the penultimate delivery on, so it lands on the first path with
`TASK_DLQ`; the broker path remains for TTL and length limits, and for
crashes faster than the worker's own check.

## Unregistered tasks and rolling deploys

A message whose task name the worker does not register is not lost at
first contact. During a rolling deploy the producers usually run the
new code before the last old worker is gone; every new task name would
otherwise end in the `.XQ` and every workflow with such a step would
fail. From 0.12 the worker therefore defers the message within a
grace period:

- **`SWARMQ_UNKNOWN_TASK_GRACE_S`** (default `900`, 15 minutes) is
  measured from the first contact, which the worker stamps into the
  header `x-swarmq-unknown-since`. The message goes back through the
  delay queue: 1 s the first time, then as long as it has waited so
  far (so roughly doubling), at most 60 s per round and never past the
  end of the grace period. Each round logs `task_unknown_deferred`
  (WARNING), fires `TASK_UNKNOWN_DEFERRED` and counts
  `swarmq_tasks_unknown_deferred_total{queue}`. A worker that knows
  the task picks the copy up as soon as it consumes the queue.
- **After the grace period** the worker dead-letters the message
  itself: error result `task '…' is not registered`, workflow step
  marked failed, unique flag released, `TASK_DLQ`, then
  `TASK_REJECTED` with `reason="unregistered_task"`, log
  `task_not_found` (ERROR). The DLQ copy carries the retention and
  `x-swarmq-traceback` like every other dead-lettered message; `dlq
  retry` starts a fresh grace period.
- **`0` switches the grace period off**: dead-letter at first contact.
  Use it for workers that must never hold foreign messages, for
  example a fleet that consumes a shared queue with a deliberately
  smaller task module.
- A cron fire of a removed task takes the same path: the scheduler
  leader holds a cron no live worker reports (see
  [scheduling](scheduling-and-retries.md#crons-nobody-reports-are-held)),
  and a fire that still gets through ends in the `.XQ` with a
  retention instead of staying there forever.

**Deploy order.** Start the workers before the producers, or roll
them first. The grace period covers the gap of an ordinary rolling
deploy; a producer that is ahead for longer than
`SWARMQ_UNKNOWN_TASK_GRACE_S` fills the `.XQ` as before 0.12.

**Mixed versions.** Only workers from 0.12 defer. A worker on 0.11
that consumes the same queue rejects an unknown task at first contact
without retention (`TASK_REJECTED`, broker `x-death` only), also when
it receives a deferral copy made by a 0.12 worker. Until every worker
of a queue runs 0.12, the old rule applies: workers before producers.

## Listing DLQ contents

```bash
swarmq dlq list --queue default
```

```text
TASK_ID                               TASK_NAME    RETRIES  ERROR_PREVIEW                  BODY_BYTES
01HV3W3YGBN4ZCZ45F2D7R9PB8            send_email   3        smtp connection refused         312
01HV3W4P5N6D2B5J0YRD9X7V6F            charge       -        stripe.CardDeclinedError        184
```

Useful flags:

- `--limit N` — maximum number of messages to list (default 100).
  The output shows `(truncated)` or JSON `truncated: true` when hit,
  also when expired messages used up the limit (since 0.13.0; they
  count towards `--limit` but not towards `scanned`).
- `--format json` — machine-readable output with headers, body, and
  a `truncated` envelope for agent scripts.
- `--queue QUEUE` — the main queue name; the DLQ is `{queue}.XQ`.

The operation is non-destructive: messages stay in the DLQ even
after listing. `dlq list`, `dlq inspect` and `dlq retry` read with
acknowledgement on their own channel: each read message is copied to
the end of the DLQ (or, for `retry`, to the main queue) and acknowledged
only after the broker confirmed the copy. A reader that dies or loses
its connection mid-way loses nothing; the broker returns what it had not
acknowledged yet. Dying between a copy and its acknowledgement leaves
the message twice in the DLQ; retrying both runs the task once (the
second has the same execution identity). Listing reorders the DLQ: read
messages move to its end. Two listings at the same time each see only
part of the DLQ.

## Inspecting one message

```bash
swarmq dlq inspect 01HV3W3YGBN4ZCZ45F2D7R9PB8 --queue default
```

Prints the full headers and decoded body of one message:

```text
TASK_ID:   01HV3W3YGBN4ZCZ45F2D7R9PB8
QUEUE:     default.XQ
BODY_SIZE: 312 bytes
HEADERS:
  x-swarmq-retry-count: 3
  x-swarmq-traceback: |
    Traceback (most recent call last):
      File ".../smtp.py", line 42, in send
        ...
  x-swarmq-enqueued-at: 1729502821
BODY:
{
  "task": {"id": "01HV3W3YGBN4ZCZ45F2D7R9PB8", ...},
  "args": [],
  "kwargs": {"to": "bad@example.com", "subject": "Welcome"}
}
```

`x-swarmq-traceback` and `x-swarmq-lock-render-error` hold at most
8 KiB: a longer text keeps its first 6 KiB and its last 2 KiB with a
`[... N bytes truncated ...]` note in between. The full error message
stays in the stored error result. A message header must fit into one
AMQP frame; an oversized header closed the whole worker connection, and
every message the worker held came back to the queue. Before each
publish SwarmQ also checks all headers against half of the negotiated
`frame_max` (128 KiB on RabbitMQ by default) and truncates long texts
further if needed; if they still do not fit, the publish raises
[`MessageHeadersTooLarge`](../reference/errors.md#messageheaderstoolarge)
without sending anything.

Use `--scan-limit N` to widen the search when the task_id is deep
in a large DLQ. Exit code `3` means the task_id was not found; `0`
means found.

## Retrying messages

### One message

```bash
swarmq dlq retry 01HV3W3YGBN4ZCZ45F2D7R9PB8 --queue default
```

Republishes the message back to the main queue. The
`x-swarmq-retry-count` header is **reset** — the task gets its
full retry budget on redelivery. Other headers (traceback, dedup
hash, enqueued-at) survive so the operator context is preserved.
The republished message carries no `expiration` and no
`x-swarmq-dead-lettered-at`; if it fails again, it is dead-lettered
with a fresh retention, and `x-swarmq-enqueued-at` restarts, so a task
with `max_age` does not expire at once. The replay keeps the
`x-swarmq-replay-id` the worker stamped when it dead-lettered the
message (from 0.12): the
[execution guard](../explanation/delivery-semantics.md#execution-guard)
treats the replay as a new execution and runs it, while a second
`dlq retry` of the same message -- or its at-least-once duplicate --
carries the same identity and is skipped. A message without that
stamp -- dead-lettered by a 0.11 worker or by the broker itself
(delivery limit, `route_via_dlx`), or old stock -- gets an id derived
from its identity instead, so repeating the retry runs it once as
well; only a message without a task id gets a random id. "Move messages" in the RabbitMQ UI or a shovel
replays the message with the same headers and therefore runs it too,
but does not restart `x-swarmq-enqueued-at`. If the broker
dead-letters such a replay again (`route_via_dlx`, delivery limit), a
second UI move of it is skipped as a duplicate of the first; use
`swarmq dlq retry` for it, which gives it a new id.

### All messages

```bash
swarmq dlq retry --all --queue default
```

Moves every message. Respects `--limit N` (default 10000) as a
safety cap. Malformed messages (undecodable body, missing headers)
stay in the DLQ — they are reported separately so you can decide
whether to purge them.

## Purging

```bash
swarmq dlq purge --queue default
```

Native AMQP `queue.purge()` on `{queue}.XQ`. **Irreversible.**
Messages are lost. Use when:

- A code bug flooded the DLQ with messages you have no intent of
  retrying.
- A purge follows a `dlq retry --all` that cleared the recoverable
  fraction and left only malformed entries.

`--format json` returns `{"purged": N, "queue": "..."}` so scripts
can record counts.

## Retention: dead-lettered messages expire

From 0.11.0 every message the worker dead-letters itself expires
`SWARMQ_DEAD_LETTER_TTL_S` seconds after dead-lettering (default
`604800`, 7 days). The worker sets the message property `expiration`
and the headers `x-swarmq-dead-lettered-at` (Unix milliseconds) and
`x-swarmq-dead-letter-ttl-ms` (the retention in milliseconds, or
`"none"`). The queue arguments of `{queue}.XQ` stay unchanged, so existing deployments
start without `PRECONDITION_FAILED`.

- **Reading does not extend the retention.** `dlq list` and
  `dlq inspect` take messages out and put them back; they put each one
  back with its remaining time, computed from
  `x-swarmq-dead-lettered-at` and the message's own
  `x-swarmq-dead-letter-ttl-ms`. A message read halfway through its
  retention still expires at the original moment. A CLI with a shorter
  or default `SWARMQ_DEAD_LETTER_TTL_S` does not shorten the retention
  the worker chose, and a message dead-lettered with `none` is never
  dropped on read.
- **Expired messages are dropped on read.** A message whose time ran
  out while a read held it is not put back, also with `dlq retry` and
  `dlq retry --all`. No log event reports it, and the `scanned` count of
  `--format json` does not include it. It still counts towards `--limit`
  and `--scan-limit`: `truncated` and `scan_exhausted` are true whenever
  reading stopped at the limit, including reads spent on expired entries
  (since 0.13.0).
- **Old stock gets the full retention on its first read.** Messages
  without `x-swarmq-dead-lettered-at` or without
  `x-swarmq-dead-letter-ttl-ms` (dead-lettered before 0.11.0, or routed
  by the broker's dead-letter exchange) get both headers and the full
  `SWARMQ_DEAD_LETTER_TTL_S` of the reading CLI from that moment.
- **`SWARMQ_DEAD_LETTER_TTL_S=none`** keeps dead-lettered messages
  without limit, as before 0.11.0. The `swarmq dlq` CLI reads the
  variable only for old stock without the retention header.

RabbitMQ only drops expired messages at the head of a queue. A message
without `expiration` in front holds up the expiry of everything behind
it until a `dlq list` gives it a retention. Such messages are old stock
and messages the broker's dead-letter exchange routed:

- messages over the queue's `x-delivery-limit`,
- the fallback when the worker's own dead-letter copy could not be
  published (`dlq_publish_failed` on the lock and unknown-task paths,
  and a failed copy of a rejected message: `undecodable`,
  `body_too_large`, `middleware_chain_factory`). If such a copy ended
  with an indeterminate publish, the message can be in `.XQ` twice,
  once with and once without retention.

Unknown tasks are no longer among them: since 0.12 the worker puts
them into `.XQ` itself, with a retention, once
`SWARMQ_UNKNOWN_TASK_GRACE_S` has passed. From this release the same
holds for the messages the worker rejects (undecodable bodies, bodies
over `SWARMQ_MAX_MESSAGE_BODY_BYTES`, a failing middleware factory);
workers before this release (up to 0.12.0rc1) on the same queue still
route them via the dead-letter exchange. Only a broker policy bounds the paths above
completely.

## Bounding the `.XQ` with a broker policy

SwarmQ's per-message retention does not cover messages the broker
routes into `{queue}.XQ` itself, and it sets no length limit. SwarmQ
does not change the arguments of existing queues. A RabbitMQ policy
bounds the dead-letter queues completely and right away; before 0.11.0
it is the only bound:

```bash
rabbitmqctl set_policy swarmq-xq '\.XQ$' \
  '{"message-ttl": 604800000, "max-length": 100000}' \
  --apply-to queues
```

This keeps dead-lettered messages for at most 7 days (`message-ttl` in
milliseconds) and at most 100 000 per queue; pick values that fit your
volume. Add `-p VHOST` for a vhost other than `/`.

!!! warning "Before you set the policy"
    - **Only one policy applies per queue**: the matching one with the
      highest priority. If another policy already matches your `.XQ`
      queues (for example a catch-all `.*`), the two do not merge:
      either yours replaces the other's settings for these queues, or
      it has no effect. Put all keys into one policy with a higher
      `--priority`.
    - **Overflow is dropped.** Messages beyond `max-length` (by
      default the oldest ones) and expired messages are deleted, not
      dead-lettered further, and no SwarmQ signal reports it. Whatever
      you might still have wanted to retry is gone.

## Switching off dead-lettering of task failures

Every terminal task failure goes into `{queue}.XQ`, also with
`max_retries=0`. Before 0.1.2, a task with `max_retries=0` stored the
error result and acknowledged the message instead. To get that
behaviour for every terminal task failure:

```python
class SyncInventory(Task):
    task_info = TaskInfo(
        name="sync_inventory", queues=["default"], dead_letter=False,
    )
```

or globally with `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES=false`
(`SwarmQConfig(dead_letter_terminal_failures=False)`). A value set on
the task wins over the global one, in both directions.

When switched off, the three terminal branches — `NoRetry`, an
exception outside `retry_exceptions`, retries exhausted — store the
error result with the task's `result_ttl`, mark a workflow as failed,
release the unique flag and acknowledge the message. `get_result()`
raises `TaskError` as usual.

What changes for monitoring:

- `TASK_DLQ` and `swarmq_tasks_total{outcome="dlq"}` no longer fire for
  these tasks, so an alert on `outcome="dlq"` stops covering them.
- `TASK_FAILED` and `swarmq_tasks_total{outcome="failed"}` keep firing
  on every failed attempt; alert on those instead.
- The delivery-count check (`WorkerHungError`), the execution guard's
  orphan limit, undecodable messages, `max_age` and lock errors still
  go into the `.XQ` (third column of the
  [table above](#when-messages-land-in-the-dlq)).

## Exit codes for scripting

`swarmq dlq` subcommands share the CLI exit-code contract:

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Runtime error (broker unreachable, bad format, etc.) |
| `2` | Bad arguments |
| `3` | `dlq inspect` / `dlq retry <task_id>` found no match |

In an automation script:

```bash
if ! swarmq dlq retry "$TASK_ID" --queue default; then
    case $? in
        3) echo "task not in DLQ" ;;
        1) echo "runtime error — check broker" ;;
        *) echo "other failure" ;;
    esac
fi
```

## Operator workflow

A typical recovery pass looks like this:

1. **Triage.** `swarmq dlq list --queue default --limit 200 --format json`
   piped through `jq` to group by error preview — see what is
   dominant.
2. **Fix the root cause.** Deploy, or manually repair the inputs if
   the bug was in the args.
3. **Retry in bulk.** `swarmq dlq retry --all --queue default`.
4. **Handle stragglers.** Inspect any task that failed to republish
   (`--format json` shows `skipped_malformed`).
5. **Purge if necessary.** `swarmq dlq purge` for malformed or
   genuinely unrecoverable messages.

## Alerting

Set up an alert on DLQ queue depth in your metrics stack.
`swarmq[metrics]` samples `swarmq_dlq_messages{queue}` for every
consumed queue's `.XQ` — alert on `> 0 for 10m`. That gauge catches
all [three paths](#three-ways-into-the-xq), including broker-side
dead-lettering that no signal reports.

Alert separately on the two counters, because they mean different
things (with dead-lettering of task failures switched off, the first
one no longer covers those tasks; use `outcome="failed"`):

- `increase(swarmq_tasks_total{outcome="dlq"}[10m]) > 0` — tasks ran
  and failed terminally; look at the error result and the traceback
  header.
- `increase(swarmq_tasks_rejected_total[10m]) > 0` — messages never
  became tasks; group by `reason` and check the producer or the
  deployment.

Without Prometheus, the RabbitMQ management plugin exposes queue
depth at `/api/queues/%2F/{queue}.XQ`.

## Verify

From a task that raises `NoRetry`:

```python
from swarmq import Task, TaskInfo, NoRetry

class AlwaysBad(Task):
    task_info = TaskInfo(name="always_bad", queues=["default"])
    async def run(self) -> None:
        raise NoRetry("intentional DLQ")
```

Schedule it, then:

```bash
swarmq dlq list --queue default
# → one message with ERROR_PREVIEW "intentional DLQ"

swarmq dlq retry --all --queue default
# → republished 1 message

swarmq dlq purge --queue default
# → purged 0 messages
```

## See also

- [Scheduling and retries](scheduling-and-retries.md) — tuning
  `max_retries` and `retry_exceptions` to avoid unnecessary DLQ hits
- [Error catalog](../reference/errors.md) — `NoRetry` and the other
  DLQ-bound exceptions
- [`swarmq dlq` CLI reference](../reference/cli.md#swarmq-dlq) — flag
  and option grammar
