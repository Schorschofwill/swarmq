# Dead letter queues

A task exhausted its retries, raised `NoRetry`, or expired before
pickup. Its message now sits in the per-queue dead letter queue
`{queue}.XQ` until an operator acts or its retention runs out (7 days
by default, see [retention](#retention-dead-lettered-messages-expire)).
This guide shows how to find out why a message landed there, how to
list, inspect, retry and purge it with `swarmq dlq`, how to bound the
DLQ, and how to switch dead-lettering of task failures off.

## Prerequisites

- The `swarmq` CLI (part of the `swarmq` package).
- `SWARMQ_BROKER_URL` pointing at the RabbitMQ vhost your workers
  consume from. Set `SWARMQ_NAMESPACE` as for the workers; an invalid
  namespace ends every `swarmq dlq` command with exit `78`.

## When messages land in the DLQ

`max_retries` defaults to `0`: without your own retry policy the
**first** failure of a task is already terminal.

| Trigger | Header on the DLQ message | With dead-lettering of task failures off |
|---|---|---|
| Retries exhausted (`max_retries` reached, including `max_retries=0`) | `x-swarmq-retry-count` at the limit, `x-swarmq-traceback` with the error message | not dead-lettered: error result stored, acked |
| `raise NoRetry("reason")` in the task | `x-swarmq-traceback` with the error message | not dead-lettered: error result stored, acked |
| Exception not in `retry_exceptions` | `x-swarmq-traceback` with the error message | not dead-lettered: error result stored, acked |
| `x-delivery-limit` (default 20) tripped by RabbitMQ, or the worker's own delivery-count check one delivery earlier | `x-death` from the broker; from the worker `x-swarmq-traceback` with `WorkerHungError` | still dead-lettered |
| The worker handed the delivery back (`nack(requeue=True)`) until `x-acquired-count` reached the delivery limit (RabbitMQ 4.3) | `x-swarmq-traceback` with `RedeliveryLimitError` | still dead-lettered |
| The same execution expired `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` times (default 3) without release ([execution guard](../explanation/delivery-semantics.md#execution-guard)) | `x-swarmq-traceback` with `WorkerHungError` | still dead-lettered |
| `TaskInfo.max_age` exceeded at pickup | none of its own: the message keeps its original headers; the stored error result says `task expired: age …s exceeds max_age …s` | still dead-lettered |
| Lock template could not be rendered (missing kwarg, empty or reserved name) | `x-swarmq-lock-render-error` with the error message, `x-swarmq-traceback` | still dead-lettered |
| Wait for a task lock ended (stalled holder or `SWARMQ_LOCK_WAIT_MAX_S`, default 6 h) | `x-swarmq-lock-name`, `x-swarmq-traceback` with `LockWaitExceeded` | still dead-lettered |
| Task name not registered in the worker after the grace period `SWARMQ_UNKNOWN_TASK_GRACE_S` (default 15 min), see [unregistered tasks](#unregistered-tasks-and-rolling-deploys) | `x-swarmq-traceback` with `task '…' is not registered`; the stored error result says the same | still dead-lettered |
| Body over `SWARMQ_MAX_MESSAGE_BODY_BYTES`, undecodable body, broken middleware-chain factory | `x-swarmq-traceback` with `<reason>: <error>` (e.g. `undecodable: Expecting value …`) | still dead-lettered |
| The worker's own dead-letter copy could not be published (fallback) | none from SwarmQ, only the broker's `x-death` | still dead-lettered |

`x-swarmq-traceback` holds the error message (`str(exc)`), not a Python
traceback. Every message the worker dead-letters itself also carries
`x-swarmq-dead-lettered-at` (Unix milliseconds) and a per-message
`expiration`, see [retention](#retention-dead-lettered-messages-expire).
The third column is explained under
[switching dead-lettering off](#switching-off-dead-lettering-of-task-failures).

The DLQ is a regular RabbitMQ queue. `rabbitmqadmin` and the management
UI can read it, but `swarmq dlq` decodes it SwarmQ-aware and keeps the
retention intact.

## Three ways into the `.XQ`

Messages reach `{queue}.XQ` on three paths, and they are not equally
visible. An alert on queue depth catches all three; knowing which path
fired tells you what to fix.

| Path | Who moves the message | Signal | Metric | Headers |
|---|---|---|---|---|
| **Dead-lettering by the worker**: retries exhausted, `NoRetry`, `max_age`, lock errors, delivery limit about to trip, orphan limit of the execution guard, unregistered task after its grace period | Worker publishes to `.XQ` and acks the original | `TASK_DLQ` (`task_id`, `task_name`, `queue`, `error`); for an unregistered task followed by `TASK_REJECTED` with `unregistered_task`; `TASK_EXPIRED` instead for `max_age` | `swarmq_tasks_total{outcome="dlq"}` | SwarmQ diagnostics (`x-swarmq-retry-count`, `x-swarmq-traceback`, `x-swarmq-lock-*`); an error result is stored |
| **Rejection by the worker**: the message could not be turned into a task | Worker publishes a copy with retention to `.XQ` and acks the original; if the copy fails, it rejects without requeue and the broker's dead-letter exchange routes it | `TASK_REJECTED` (`queue`, `reason`, `task_id`, `task_name`, `error`) | `swarmq_tasks_rejected_total{queue,reason}` | `x-swarmq-traceback` with `<reason>: <error>` plus the retention stamps; no error result. Only the fallback carries the broker's `x-death` instead. `task_id`/`task_name` are `None` when the body never decoded |
| **Dead-lettering by the broker**: `x-delivery-limit` after repeated worker crashes, message TTL, queue length limit | RabbitMQ itself | none, no worker saw the terminal delivery | `swarmq_dlq_messages{queue}` (depth gauge sampled by the worker) | only `x-death` |

`reason` on the rejection path is one of `body_too_large`,
`undecodable`, `middleware_chain_factory` or `dlq_publish_failed`;
`swarmq dlq list` shows the first three in `ERROR_PREVIEW`.
`unregistered_task` comes from the first path. After a deploy it usually
means that worker and producer run different versions of the task
module; `body_too_large` and `undecodable` point at the producer.

Watch the third path when the other two are quiet: a task that reliably
kills the process (SIGSEGV, OOM) never reaches `TASK_FAILED`. The worker
dead-letters such a message itself from the penultimate delivery on, so
it usually arrives on the first path with `TASK_DLQ`; the broker path
remains for TTL and length limits and for crashes faster than the
worker's own check.

## Unregistered tasks and rolling deploys

A message whose task name the worker does not know is not dead-lettered
at first contact. During a rolling deploy the producers often run the
new code before the last old worker is gone; every new task name, and
every workflow with such a step, would otherwise end in the `.XQ`. The
worker therefore defers the message for a grace period:

- **`SWARMQ_UNKNOWN_TASK_GRACE_S`** (default `900`, 15 minutes) counts
  from the first contact, which the worker stamps into the header
  `x-swarmq-unknown-since`. The message goes back through the delay
  queue: 1 s the first time, then as long as it has waited so far
  (roughly doubling), at most 60 s per round and never past the end of
  the grace period. Each round logs `task_unknown_deferred` (WARNING),
  fires `TASK_UNKNOWN_DEFERRED` and counts
  `swarmq_tasks_unknown_deferred_total{queue}`. A worker that knows the
  task picks the copy up as soon as it consumes the queue.
- **After the grace period** the worker dead-letters the message:
  error result `task '…' is not registered`, workflow step marked
  failed, unique flag released, `TASK_DLQ`, then `TASK_REJECTED` with
  `reason="unregistered_task"`, log `task_not_found` (ERROR). `dlq
  retry` starts a fresh grace period.
- **`0` switches the grace period off**: dead-letter at first contact.
  Use it for workers that must never hold foreign messages, for
  example a fleet that consumes a shared queue with a deliberately
  smaller task module.
- A cron fire of a removed task takes the same path: the scheduler
  leader holds a cron no live worker reports (see
  [scheduling](scheduling-and-retries.md#crons-nobody-reports-are-held)),
  and a fire that still gets through ends in the `.XQ` with a
  retention.

**Deploy order.** Start or roll the workers before the producers. The
grace period covers the gap of an ordinary rolling deploy; a producer
that is ahead for longer than `SWARMQ_UNKNOWN_TASK_GRACE_S` fills the
`.XQ` again. Only workers from 0.12 on defer: a 0.11 worker on the same
queue rejects an unknown task at first contact, also a deferral copy
of a newer worker (see [deploying and upgrading](deployment.md)).

## Listing DLQ contents

```bash
swarmq dlq list --queue default
```

```text
TASK_ID                               TASK_NAME   RETRIES  ERROR_PREVIEW            BODY_BYTES
0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71  send_email  3        smtp connection refused  312
0199f3a4-1d02-7e55-8c3b-61a9d0f7e2c8  charge      -        card declined            184
```

`RETRIES` is `-` when the message never retried (for example after
`NoRetry`); `ERROR_PREVIEW` is the first line of `x-swarmq-traceback`,
cut at 60 characters.

| Flag | Default | Meaning |
|---|---|---|
| `--queue QUEUE` | `default` | the main queue; the DLQ is `{queue}.XQ` |
| `--limit N` | `100` | read at most N messages. When reading stopped at the limit, the table ends with `(truncated at --limit N; …)` and JSON has `truncated: true`, also when expired messages used up the reads (since 0.13.0) |
| `--format json` | table | envelope `{"entries", "scanned", "limit", "truncated", "queue"}`; each entry has `task_id`, `task_name`, `queue`, `headers` and `body_size`. Use `dlq inspect` for the body |

Listing is non-destructive. `dlq list`, `dlq inspect` and `dlq retry`
read with acknowledgement on their own channel: each read message is
copied to the end of the DLQ (for `retry`, to the main queue) and
acknowledged only after the broker confirmed the copy. A reader that
dies mid-way loses nothing. Three consequences:

- Dying between copy and acknowledgement leaves the message twice in
  the DLQ; retrying both runs the task once (same execution identity).
- Reading reorders the DLQ: read messages move to its end.
- Two listings at the same time each see only part of the DLQ.

## Inspecting one message

```bash
swarmq dlq inspect 0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71 --queue default
```

Prints the headers and the decoded body of one message:

```text
TASK_ID:   0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71
QUEUE:     default.XQ
BODY_SIZE: 312 bytes
HEADERS:
  x-swarmq-enqueued-at: 1792836421
  x-swarmq-retry-count: 3
  x-swarmq-traceback: smtp connection refused
  x-swarmq-dead-lettered-at: 1792836512345
  x-swarmq-dead-letter-ttl-ms: 604800000
  ...
BODY:
{
  "task": {"id": "0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71", "queue": "default", "name": "send_email"},
  "args": [],
  "kwargs": {"to": "bad@example.com", "subject": "Welcome"}
}
```

`--scan-limit N` (default 1000) widens the search when the task id is
deep in a large DLQ. Exit code `3` means not found; the message then
says whether the scan hit `--scan-limit`.

`x-swarmq-traceback` and `x-swarmq-lock-render-error` hold at most
8 KiB: a longer text keeps its first 6 KiB and its last 2 KiB with a
`[... N bytes truncated ...]` note in between. The full error message
stays in the stored error result. Before each publish SwarmQ also
checks all headers against half of the negotiated `frame_max` (128 KiB
on RabbitMQ by default) and truncates long texts further; if they
still do not fit, the publish raises
[`MessageHeadersTooLarge`](../reference/errors.md#messageheaderstoolarge)
without sending anything. An oversized header would otherwise close the
whole worker connection.

## Retrying messages

### One message

```bash
swarmq dlq retry 0199f3a2-7c41-7b3e-9a5d-2f8c1e0b4d71 --queue default
```

Republishes the message to the main queue and removes it from the DLQ.
The replay starts fresh:

- **Retry budget:** `x-swarmq-retry-count` is removed, so the task gets
  its full `max_retries` again.
- **Age:** `x-swarmq-enqueued-at` restarts, so a task with `max_age`
  does not expire at once.
- **Retention:** `expiration`, `x-swarmq-dead-lettered-at` and
  `x-swarmq-dead-letter-ttl-ms` are removed; a replay that fails again
  is dead-lettered with a fresh retention.
- **Loop state:** the grace period for unknown tasks, the lock wait and
  the defer counter of a cron or `delay`/`eta` fire start again.
- **Diagnostics:** the remaining headers, such as `x-swarmq-traceback`
  and the dedup hash, are kept.

The replay runs once, even if you repeat the command. It keeps the
`x-swarmq-replay-id` the worker stamped when it dead-lettered the
message: the
[execution guard](../explanation/delivery-semantics.md#execution-guard)
treats the replay as a new execution, while a second `dlq retry` of
the same message, or its at-least-once duplicate, carries the same
identity and is skipped. A message without that stamp (dead-lettered
by the broker itself, by a worker before 0.12, or old stock) gets an id
derived from its identity, with the same effect; only a message without
a task id gets a random one.

!!! note "Replaying from the RabbitMQ UI or a shovel"
    "Move messages" in the management UI or a shovel replays the message
    with its headers unchanged. That runs the task too, but keeps
    `x-swarmq-enqueued-at` and the retry count. Copies dead-lettered by
    workers from 0.12.2 on carry no broker delivery counters and can be
    moved this way. Entries from older workers and messages the broker
    dead-lettered itself (delivery limit, `route_via_dlx`) may still
    carry them and go straight back to the DLQ; if the broker
    dead-letters a moved message again, a second move is skipped as a
    duplicate. Use `swarmq dlq retry` for these.

### All messages

```bash
swarmq dlq retry --all --queue default
```

Moves every message, at most `--limit N` (default 10000). Messages
without a decodable task id stay in the DLQ and are reported
separately (`skipped_malformed` in `--format json`), so you can decide
whether to purge them.

## Purging

```bash
swarmq dlq purge --queue default
```

Native AMQP `queue.purge()` on `{queue}.XQ`, without a confirmation
prompt. **Irreversible**: the messages are gone. Use it when a bug
flooded the DLQ with messages you will not retry, or after a
`dlq retry --all` left only malformed entries. `--format json` returns
`{"purged": N, "queue": "..."}`.

## Retention: dead-lettered messages expire

Every message the worker dead-letters itself expires
`SWARMQ_DEAD_LETTER_TTL_S` seconds after dead-lettering (default
`604800`, 7 days; since 0.11.0). The worker sets the message property
`expiration` and the headers `x-swarmq-dead-lettered-at` (Unix
milliseconds) and `x-swarmq-dead-letter-ttl-ms` (the retention in
milliseconds, or `"none"`). The queue arguments of `{queue}.XQ` stay
unchanged, so existing deployments start without `PRECONDITION_FAILED`.

- **Reading does not extend the retention.** `dlq list` and
  `dlq inspect` put each message back with its remaining time, computed
  from the message's own headers. A CLI with a shorter or default
  `SWARMQ_DEAD_LETTER_TTL_S` does not shorten the retention the worker
  chose, and a message dead-lettered with `none` is never dropped on
  read.
- **Expired messages are dropped on read.** A message whose time ran
  out while a read held it is not put back, also with `dlq retry` and
  `dlq retry --all`. No log event reports it, and `scanned` does not
  count it. It still counts towards `--limit` and `--scan-limit`, so
  `truncated` and `scan_exhausted` are true whenever reading stopped at
  the limit (since 0.13.0).
- **Old stock gets the full retention on its first read.** Messages
  without the two headers (dead-lettered before 0.11.0, or routed by the
  broker's dead-letter exchange) get both headers and the full
  `SWARMQ_DEAD_LETTER_TTL_S` of the reading CLI from that moment.
- **`SWARMQ_DEAD_LETTER_TTL_S=none`** keeps dead-lettered messages
  without limit. The `swarmq dlq` CLI reads the variable only for old
  stock without the retention header.

RabbitMQ only drops expired messages at the head of a queue. A message
without `expiration` in front holds up the expiry of everything behind
it until a `dlq list` gives it a retention. Such messages are old stock
and messages the broker's dead-letter exchange routed:

- messages over the queue's `x-delivery-limit`,
- the fallback when the worker's own dead-letter copy could not be
  published (`dlq_publish_failed`): on the lock and unknown-task paths,
  and for a rejected message (`undecodable`, `body_too_large`,
  `middleware_chain_factory`). If such a copy ended with an
  indeterminate publish, the message can be in `.XQ` twice, once with
  and once without retention.

Unknown tasks (since 0.12) and rejected messages (since 0.12.0) normally
get a retention; workers up to 0.12.0rc1 on the same queue still route
rejected messages via the dead-letter exchange. Only a broker policy
bounds every path.

## Bounding the `.XQ` with a broker policy

The per-message retention does not cover messages the broker routes
into `{queue}.XQ` itself, and it sets no length limit. A RabbitMQ
policy bounds the dead letter queues completely and right away:

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
`max_retries=0`. If a stored error result is enough for a task, switch
dead-lettering of its failures off:

```python
class SyncInventory(Task):
    task_info = TaskInfo(
        name="sync_inventory", queues=["default"], dead_letter=False,
    )
```

or globally with `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES=false`
(`SwarmQConfig(dead_letter_terminal_failures=False)`). A value set on
the task wins over the global one, in both directions.

When switched off, the three terminal branches (`NoRetry`, an exception
outside `retry_exceptions`, retries exhausted) store the error result
with the task's `result_ttl`, mark a workflow as failed, release the
unique flag and acknowledge the message. `get_result()` raises
`TaskError` as usual.

What changes for monitoring:

- `TASK_DLQ` and `swarmq_tasks_total{outcome="dlq"}` no longer fire for
  these tasks, so an alert on `outcome="dlq"` stops covering them.
- `TASK_FAILED` and `swarmq_tasks_total{outcome="failed"}` keep firing
  on every failed attempt; alert on those instead.
- The delivery-count check (`WorkerHungError`), the execution guard's
  orphan limit, rejected messages, `max_age` and lock errors still go
  into the `.XQ` (third column of the
  [table above](#when-messages-land-in-the-dlq)).

## Exit codes for scripting

| Code | Meaning |
|---|---|
| `0` | success |
| `1` | runtime error (broker unreachable, …) |
| `2` | bad arguments, for example `dlq retry` with both a task id and `--all` |
| `3` | `dlq inspect` / `dlq retry <task_id>` found no match |
| `78` | invalid configuration, for example `SWARMQ_NAMESPACE` |

```bash
swarmq dlq retry "$TASK_ID" --queue default
case $? in
    0) echo "republished" ;;
    3) echo "task not in DLQ" ;;
    1) echo "runtime error, check the broker" ;;
    *) echo "other failure" ;;
esac
```

## Operator workflow

1. **Triage.** `swarmq dlq list --queue default --limit 200 --format json`
   and group the entries by `headers["x-swarmq-traceback"]`, for example
   with `jq`, to see what dominates.
2. **Fix the root cause.** Deploy the fix, or repair the inputs if the
   bug was in the arguments.
3. **Retry in bulk.** `swarmq dlq retry --all --queue default`.
4. **Handle stragglers.** `--format json` reports `skipped_malformed`;
   inspect those messages.
5. **Purge if necessary.** `swarmq dlq purge` for malformed or
   unrecoverable messages.

## Alerting

Alert on DLQ depth: with `swarmq[metrics]` the worker samples
`swarmq_dlq_messages{queue}` for the `.XQ` of every consumed queue;
alert on `> 0 for 10m`. That gauge catches all
[three paths](#three-ways-into-the-xq), including broker-side
dead-lettering that no signal reports.

Alert separately on the two counters, because they mean different
things:

- `increase(swarmq_tasks_total{outcome="dlq"}[10m]) > 0`: tasks ran and
  failed terminally; look at the error result and the traceback header.
  With dead-lettering of task failures switched off, use
  `outcome="failed"` for those tasks.
- `increase(swarmq_tasks_rejected_total[10m]) > 0`: messages never
  became tasks; group by `reason` and check the producer or the
  deployment.

Without Prometheus, the RabbitMQ management API reports the depth at
`/api/queues/%2F/{queue}.XQ`.

## Verify

With a worker running for the `default` queue, schedule a task that
always raises `NoRetry`:

```python
from swarmq import NoRetry, Task, TaskInfo


class AlwaysBad(Task):
    task_info = TaskInfo(name="always_bad", queues=["default"])

    async def run(self) -> None:
        raise NoRetry("intentional DLQ")
```

```bash
swarmq dlq list --queue default
# → one message with ERROR_PREVIEW "intentional DLQ"

swarmq dlq retry --all --queue default
# → republished 1 message(s) from default.XQ to default
#   the task fails again and is back in the DLQ

swarmq dlq purge --queue default
# → purged 1 message(s) from default.XQ
```

## See also

- [Scheduling and retries](scheduling-and-retries.md): tune
  `max_retries` and `retry_exceptions` to avoid unnecessary DLQ hits
- [Error catalog](../reference/errors.md): `NoRetry` and the other
  DLQ-bound exceptions
- [`swarmq dlq` CLI reference](../reference/cli.md#swarmq-dlq): every
  flag and the JSON output
