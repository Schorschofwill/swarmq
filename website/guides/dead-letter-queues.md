# Dead letter queues

A task exhausted its retries, raised `NoRetry`, or expired before
pickup. The message is now in the per-queue dead-letter queue —
`{queue}.XQ` — where it stays until an operator intervenes. This
guide covers how to inspect, retry, and purge DLQ messages.

## Prerequisites

- `SWARMQ_BROKER_URL` pointed at the same RabbitMQ you consume from.
- `swarmq` CLI installed (`pip install swarmq`).

## When messages land in the DLQ

| Trigger | Header on the DLQ message |
|---|---|
| Retry exhaustion (`max_retries` reached — incl. `max_retries=0`, where the FIRST failure is already terminal) | `x-swarmq-retry-count` at the limit |
| `raise NoRetry("reason")` from task body | `x-swarmq-error=no_retry` |
| `x-delivery-limit=20` tripped by RabbitMQ | `x-death` from broker |
| `TaskInfo.max_age` exceeded at pickup | `x-swarmq-expired=1` |
| Lock-render failure (template kwarg missing) | `x-swarmq-lock-error=render_failed` |
| Lock conflict budget exhausted | `x-swarmq-lock-error=conflict_cap` |

The DLQ is a regular RabbitMQ queue — you can inspect it with
`rabbitmqadmin` or the management UI, but the `swarmq dlq` CLI
renders it in SwarmQ-aware shape.

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
`swarmq[metrics]` emits `rabbitmq_queue_messages` for every queue
including `*.XQ` — filter to `queue=~".+\\.XQ"` and alert on
`> 0 for 10m`.

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
