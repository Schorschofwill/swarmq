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
| The same execution expired `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` times without release ([execution guard](../explanation/delivery-semantics.md#execution-guard)) | `x-swarmq-traceback` with `WorkerHungError` | still dead-lettered |
| `TaskInfo.max_age` exceeded at pickup | none of its own — the message keeps its original headers; the stored error result says `task expired: age …s exceeds max_age …s` | still dead-lettered |
| Lock-render failure (template kwarg missing, empty render) | `x-swarmq-lock-render-error` with the error message | still dead-lettered |
| Lock conflict budget exhausted (10th conflict) | `x-swarmq-lock-conflict-count`, `x-swarmq-lock-name` | still dead-lettered |
| Body over the size cap, undecodable body, unregistered task name, broken middleware-chain factory, failed dead-letter publish | none from SwarmQ — only the broker's `x-death` | still dead-lettered |

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
| **Dead-lettering by the worker** — retries exhausted, `NoRetry`, `max_age`, lock errors, delivery limit about to trip, orphan limit of the execution guard | Worker publishes to `.XQ` and acks the original | `TASK_DLQ` (`task_id`, `task_name`, `queue`, `error`) for retries exhausted, `NoRetry`, the delivery limit and the orphan limit; `TASK_EXPIRED` for `max_age`; none for lock errors | `swarmq_tasks_total{outcome="dlq"}` | SwarmQ diagnostics (`x-swarmq-retry-count`, `x-swarmq-traceback`, `x-swarmq-lock-*`, see the table above); an error result is stored, except for lock errors |
| **Rejection by the worker** — the message could not be turned into a task | Worker rejects without requeue; the broker's dead-letter exchange routes it | `TASK_REJECTED` (`queue`, `reason`, `task_id`, `task_name`, `error`) | `swarmq_tasks_rejected_total{queue,reason}` | only the broker's `x-death`; `task_id`/`task_name` are `None` when the body never decoded |
| **Dead-lettering by the broker** — `x-delivery-limit` after repeated worker crashes, message TTL, queue length limit | RabbitMQ itself | none — no worker saw the terminal delivery | `swarmq_dlq_messages{queue}` (depth gauge sampled by the worker) | only `x-death` |

`reason` on the rejection path is one of `body_too_large`,
`undecodable`, `unregistered_task`, `middleware_chain_factory`,
`dlq_publish_failed`. `unregistered_task` after a deploy usually means
the worker and the producer run different versions of the task module;
`body_too_large` and `undecodable` point at the producer.

The third path is the one to watch when the other two are quiet: a
task that reliably kills the process (SIGSEGV, OOM) never reaches
`TASK_FAILED`. From 0.5.0 the worker dead-letters such a message itself
from the penultimate delivery on, so it lands on the first path with
`TASK_DLQ`; the broker path remains for TTL and length limits, and for
crashes faster than the worker's own check.

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
  The output shows `(truncated)` or JSON `truncated: true` when hit.
- `--format json` — machine-readable output with headers, body, and
  a `truncated` envelope for agent scripts.
- `--queue QUEUE` — the main queue name; the DLQ is `{queue}.XQ`.

The operation is non-destructive: messages stay in the DLQ even
after listing.

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
with a fresh retention. It also gets a new `x-swarmq-replay-id`: the
[execution guard](../explanation/delivery-semantics.md#execution-guard)
would otherwise skip it as already finished while the marker of the
first run still exists.

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
  `--format json` does not include it.
- **Old stock gets the full retention on its first read.** Messages
  without `x-swarmq-dead-lettered-at` or without
  `x-swarmq-dead-letter-ttl-ms` (dead-lettered before 0.11.0, or routed
  by the broker's dead-letter exchange) get both headers and the full
  `SWARMQ_DEAD_LETTER_TTL_S` of the reading CLI from that moment.
- **`SWARMQ_DEAD_LETTER_TTL_S=none`** keeps dead-lettered messages
  without limit, as before 0.11.0. The `swarmq dlq` CLI reads the
  variable only for old stock without the retention header.

RabbitMQ only drops expired messages at the head of a queue. A message
without `expiration` in front — old stock, or a message the broker's
dead-letter exchange routed (`x-delivery-limit`, undecodable or
unregistered messages) — holds up the expiry of everything behind it
until a `dlq list` gives it a retention. Only a broker policy bounds
those paths completely.

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
