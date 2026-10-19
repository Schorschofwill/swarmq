# Error catalog

Every exception SwarmQ raises descends from `SwarmQError`. The table
below gives the catch-all; the sections after it are the per-class
entries with **When raised**, **Cause**, and **Recovery**.

```python
from swarmq import SwarmQError

try:
    result = await app.run("send_email", to="a@example.com")
except SwarmQError as exc:
    # Any SwarmQ-originated failure
    ...
```

## Hierarchy at a glance

| Exception | Parent | When you see it |
|---|---|---|
| `SwarmQError` | `Exception` | Root of the hierarchy |
| `ConfigurationError` | `SwarmQError` | Bad config at worker start, bad CLI args, missing env vars |
| `LoggingConfigError` | `ConfigurationError` | Logging config file (`--log-config`) not loadable or invalid |
| `QueueArgumentsMismatch` | `ConfigurationError` | An existing queue has other arguments than the configuration, which swarmq cannot adopt (for example a changed `SWARMQ_QUEUE_TYPE`) |
| `BrokerConnectionError` | `SwarmQError` | Cannot reach RabbitMQ |
| `PublishIndeterminate` | `BrokerConnectionError` | A publish was sent but broke its deadline or lost its connection before the confirm; whether it arrived is unknown |
| `SettleFailedAfterPublish` | `BrokerConnectionError` | Worker-internal: a retry or dead-letter copy was published, only the ack of the original failed |
| `MessageHeadersTooLarge` | `BrokerConnectionError` | Message headers do not fit into half of `frame_max`, even with long texts truncated; nothing was sent |
| `UnroutableMessage` | `BrokerConnectionError` | The broker returned a publish: no queue with this name exists; nothing was delivered |
| `BrokerPublishBlocked` | `BrokerConnectionError` | The broker blocked the publish connection (resource alarm) for the whole publish deadline; nothing was sent |
| `PublishRejected` | `BrokerConnectionError` | The broker rejected the publish to this queue in every attempt (negative publisher confirm); the connection is healthy |
| `BackendConnectionError` | `SwarmQError` | Cannot reach Valkey |
| `BackendResponseError` | `BackendConnectionError` | Valkey answered an operation with an error (`WRONGTYPE`, script error) |
| `TaskNotFoundError` | `SwarmQError` | `schedule()` called with an unknown task name |
| `LockWaitExceeded` | `SwarmQError` | Never raised: error type of a DLQ copy whose wait for a task lock ended (stalled holder or `SWARMQ_LOCK_WAIT_MAX_S`) |
| `RedeliveryLimitError` | `SwarmQError` | Never raised: error type of a DLQ copy whose deliveries were handed back until the delivery limit |
| `TaskError` | `SwarmQError` | The task body raised; propagated to `get_result()` |
| `TaskCancelledError` | `TaskError` | `get_result()` or `app.run()` on a task cancelled with `app.cancel(task_id)`; the task itself sees `asyncio.CancelledError` |
| `ResultNotReadyError` | `SwarmQError` | `get_result()` timed out before a result landed |
| `ResultExpiredError` | `SwarmQError` | Currently never raised (reserved); an expired result raises `ResultNotReadyError` |
| `ResultSerializationError` | `SwarmQError` | Result cannot be msgpack-serialised |
| `Retry` | `SwarmQError` | Control flow — raised by task code to request a retry |
| `NoRetry` | `SwarmQError` | Control flow — raised by task code to bypass retries |
| `SoftTimeoutError` | `SwarmQError` | Task exceeded `soft_timeout`; `run()` was cancelled (the task sees `CancelledError`) |
| `WorkflowValidationError` | `SwarmQError` | Workflow topology invalid, or fan-in results incomplete |
| `WorkflowNotFound` | `SwarmQError` | `app.workflow(id)` or a handle call found no workflow state |
| `WorkflowResultExpired` | `SwarmQError` | A step or final workflow result expired before it was used |
| `WorkflowStateConflict` | `SwarmQError` | Worker-internal: workflow state update lost every compare-and-set round |

Every class except `WorkflowStateConflict` and `SettleFailedAfterPublish` can be imported from
`swarmq` directly (`from swarmq import PublishIndeterminate`); all of
them also live in `swarmq.exceptions`.

## `SwarmQError`

**When raised.** Never directly. The base class for every exception
the library throws. Catching it is the safe way to swallow any
SwarmQ-originated failure without also catching unrelated exceptions.

**Recovery.** Decide per-exception; see the specific subclasses
below.

## `ConfigurationError`

**When raised.** During worker startup or at a CLI invocation, when
the library or the CLI parser detects a misconfigured input.

**Cause.** Typical sources:

- Missing env var (`SWARMQ_BROKER_URL`, `SWARMQ_BACKEND_URL`).
- Invalid port (`SWARMQ_METRICS_PORT=not-a-number`).
- `log_level` outside `{DEBUG, INFO, WARNING, ERROR, CRITICAL}`.
- Contradicting logging settings, e.g. `--log-config` together with
  `--log-format` or `--no-configure-logging` (see
  [Logging](logging.md#configuration-errors)).
- `scheduler_lock_ttl < 2 × scheduler_heartbeat_interval`.
- `--args` / `--kwargs` for `swarmq schedule add` not valid JSON.

**Recovery.** Fix the config and restart. The error message names
the offending field. `swarmq worker` logs one structured
`worker_config_error` line and exits `78`:

```text
[error    ] worker_config_error            [swarmq.cli] error='ConfigurationError: SWARMQ_BROKER_URL and SWARMQ_BACKEND_URL must both be set' error_type=ConfigurationError hint='fix the configuration and start again; no restart attempted' source=supervisor
```

`swarmq schedule`, `swarmq dlq` and `swarmq inspect` print one line to
stderr and exit `1`:

```text
swarmq schedule list: SWARMQ_BACKEND_URL is not set; export it or pass it via env, e.g. SWARMQ_BACKEND_URL=valkey://localhost:6379/0
```

## `LoggingConfigError`

**When raised.** At the start of `swarmq worker` (and on the
`--log-config` check before a reload), by `swarmq.logging.load_log_config`
and `configure_logging`, when the logging config file cannot be used.
Subclass of `ConfigurationError`, importable from `swarmq`,
`swarmq.exceptions` and `swarmq.logging`.

**Cause.** The file named by `--log-config` / `SWARMQ_LOG_CONFIG`:

- cannot be read (missing file, permissions, not UTF-8);
- is not valid JSON, or is YAML while PyYAML is not installed;
- is not a mapping;
- is rejected by `logging.config.dictConfig` (typo in a handler key,
  unknown class, bad level).

The exception carries the file in `path`; the original error is in
`__cause__` and in the message.

**Recovery.** Fix the file and start again. `swarmq worker` exits with
`78` and logs one `worker_config_error` line with `path`, `error_type`
and `error`. During a reload the running supervisor logs
`log_config_reload_failed` instead and keeps its workers running.

## `QueueArgumentsMismatch`

**When raised.** When the worker declares its queues at startup and an
existing queue has an argument that differs from the configuration and
that swarmq does not adopt. Subclass of `ConfigurationError`,
importable from `swarmq` and `swarmq.exceptions`. The message and the
attributes `queue`, `argument`, `declared` and `existing` name the
conflict.

**Cause.** RabbitMQ never changes the arguments of an existing queue.
Typical: `SWARMQ_QUEUE_TYPE` was changed for queues that already exist,
or `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=at-least-once` meets a queue
without at-least-once dead-lettering (`x-dead-letter-strategy=at-least-once`
together with `x-overflow=reject-publish`). A different `x-delivery-limit`,
`x-dead-letter-strategy` or `x-overflow` is adopted and does not raise
it ([existing queue arguments](../explanation/delivery-semantics.md#existing-queue-arguments)).

**Recovery.** A restart does not help, so `swarmq worker` exits with
`78` and the supervisor does not restart it. Restore the previous
setting, or [migrate the queue](../explanation/delivery-semantics.md#changing-queue-arguments).

## `BrokerConnectionError`

**When raised.** Worker startup when RabbitMQ is not reachable within
the connect timeout, or during a reconnect attempt that exceeds the
retry budget. Also when a publish exceeds `SWARMQ_PUBLISH_TIMEOUT_S`
(default 120s) — see the delivery caveat below.
Since 0.10.0 also when the worker acknowledges, requeues or
dead-letters a delivery after the channel or connection was lost; the
worker logs `delivery_settle_failed`, and RabbitMQ redelivers the
unacknowledged message.

!!! warning "A publish-deadline failure does not mean the message was not sent"

    When the message reads `exceeded the publish deadline` or `lost the
    connection while waiting for the broker's confirm`, delivery is
    **indeterminate**: the message was sent and the deadline ran out or
    the connection broke while waiting for the broker's publisher confirm,
    so the broker may already hold the message. Retrying such a call can
    execute the task a second time. This is different from every other
    cause listed here, where nothing was sent. swarmq itself never resends
    such a message; a connection loss before the message was written to
    the socket is retried internally and, if it persists, ends as a plain
    `BrokerConnectionError`.

    A task submitted with `unique_for` **is** protected: swarmq keeps the
    dedup claim in this one case instead of releasing it, so a retry is
    refused until the claim expires rather than running the task twice.
    Every other publish failure still releases it, because nothing was sent.

    Catch `PublishIndeterminate` (a subclass, so existing
    `except BrokerConnectionError` handlers keep working) when you need to
    tell the two apart.

**Cause.** Network partition, wrong URL, RabbitMQ not running, TLS
mismatch, vhost or user not authorized.

**Recovery.** Verify the URL with `rabbitmqctl status` on the broker
host or `nc -vz <host> 5672`. In code, the exception propagates out
of `async with SwarmQ(...)` — catch it at your startup boundary.

```python
from swarmq import BrokerConnectionError

try:
    async with SwarmQ(broker, backend) as app:
        await app.start_worker()
except BrokerConnectionError as exc:
    # Log, alert, exit non-zero.
    ...
```

## `BackendConnectionError`

**When raised.** Worker startup when Valkey is not reachable, or at
runtime when a backend operation exceeds `backend_op_timeout`.
`ValkeyBackend.connect()` (worker start, CLI commands, producers)
raises it for every driver error, also for a timeout and for an error
reply to its `PING` (ACL `NOPERM` as `BackendResponseError`), with the
message `Valkey connect failed: …`. The `PING` runs under an overall
deadline, [`SWARMQ_BACKEND_CONNECT_TIMEOUT_S`](configuration.md#worker-behaviour)
(default 30 s); when it expires, the message is
`Valkey connect timed out after …s`. A failed `connect()` closes the
client it built and leaves no half-connected backend behind; a later
`connect()` reaches Valkey again and repeats the eviction check. When
the connection is accepted but cut during the handshake (a proxy or
sidecar in front of a restarting Valkey), the client rebuilds it
within the same window as a command retry, about 3.1 s from the first
failure, before the command is sent. A handshake that hangs ends after
the socket timeout (10 s) without a retry.

**Cause.** Valkey down, URL wrong, `AUTH` password mismatch, TLS
mismatch.

**Recovery.** `valkey-cli -u $SWARMQ_BACKEND_URL ping` should print
`PONG`. Runtime connection losses are recovered automatically by the
valkey-py reconnect loop — this exception fires only when the retry
window is exhausted.

## `BackendResponseError`

**When raised.** A backend operation reached Valkey, but Valkey
answered with an error: a key of the wrong type (`WRONGTYPE`), a Lua
script error. A subclass of `BackendConnectionError`, so every
`except BackendConnectionError` still catches it.

**Cause.** Usually a key that something other than SwarmQ wrote under a
SwarmQ name (for example a lock key `sq:lock:...` set to a hash), or an
incompatible server.

**Recovery.** Waiting does not help, so the worker does not defer a
message for it before execution (it does for an unreachable backend):
the message goes back to the queue in place and, if the error
persists, reaches `{queue}.XQ` through the delivery limit. Inspect the
key named in the error and delete or fix it.

Error replies that heal by waiting are not a `BackendResponseError`
but a plain `BackendConnectionError`, handled like an unreachable
backend: `OOM` (maxmemory reached), `READONLY` (the client still talks
to a demoted primary after a failover), `LOADING`, `TRYAGAIN`,
`CLUSTERDOWN`, `MASTERDOWN` and `MISCONF` (persistence failing, for
example a full disk).

## `TaskNotFoundError`

**When raised.** `app.schedule("name", ...)` with a name no
registered `Task` subclass declared in its `TaskInfo`.

**Cause.** Typo, task module not imported, task not subclassed from
`swarmq.Task`, or the module imported but the class inside it not
evaluated (e.g. behind a conditional).

**Recovery.**

```bash
swarmq inspect --module myapp.tasks
```

lists every registered task. If your task is missing, its module is
not being imported when the worker loads, or the class is guarded by
a `if TYPE_CHECKING:` block.

## `TaskError`

**When raised.** `await app.get_result(task_id)` when the task body
raised an uncaught exception and exhausted (or was never going to
use) its retry budget.

**Cause.** Any exception your `run()` method let escape. The original
exception class is lost across the wire; the error message contains
the string the worker stored (`""` for an exception without text).
`error_class` names the class (advisory): `module.qualname`, e.g.
`builtins.ValueError`; in CPU mode the class reported by the compute
child. It is `None` for results of older workers and for failures the
worker generates itself (hard timeout, drain, lost compute child,
expiry).

```python
from swarmq import TaskError

try:
    result = await app.get_result(task_id, timeout=60)
except TaskError as exc:
    if exc.error_class == "builtins.ValueError":
        ...  # bad input, do not resubmit unchanged
```

**Recovery.** Read the worker logs — they carry the full traceback.
Then fix your task body, or widen your `retry_exceptions` so the
transient class is retried instead of DLQ'd.

## `TaskCancelledError`

**When raised.** On the client: `get_result()` and `app.run()` raise
it when the stored result carries the cancel flag, and a workflow
handle raises it for a cancelled workflow. Never inside the task body:
the worker cancels the running `run()` coroutine, so the task sees
`asyncio.CancelledError`. Clean up in `finally`, or catch
`asyncio.CancelledError`, clean up and re-raise it. In CPU mode the
compute child is killed instead
([behaviour changes](../guides/cpu-mode.md#behaviour-changes-for-task-authors)).

**Cause.** Somebody — the client, an operator, another service —
called `app.cancel(task_id)` while the task was queued or running.

**Recovery.** If the cancel was deliberate, catch it and move on. If
it was spurious, check your cancel flag TTL — see
[`app.cancel()`](api.md#cancel) for the stale-flag caveat.

```python
from swarmq import TaskCancelledError

try:
    result = await app.get_result(task_id, timeout=60)
except TaskCancelledError:
    # Task was cancelled before it produced a result.
    ...
```

## `ResultNotReadyError`

**When raised.** `get_result(task_id, timeout=T)` after T seconds
with no result in Valkey.

**Cause.** The task is still running, the worker died, the task was
DLQ'd without storing a result, the result expired after its
`result_ttl`, or the task_id is wrong.

**Recovery.** Raise the timeout, check the worker logs, or inspect
the DLQ if the task is stuck in retry loops.

```python
from swarmq import ResultNotReadyError

try:
    result = await app.get_result(task_id, timeout=5)
except ResultNotReadyError:
    # Still pending; check later.
    ...
```

## `ResultExpiredError`

**When raised.** Currently never. After the result TTL has expired,
`get_result()` raises `ResultNotReadyError`, the same as for a result
that is not there yet. The class is kept as a stable hook for a later
release that tells the two cases apart; catching it today has no
effect.

**Recovery.** To keep results readable longer, raise `result_ttl`
(`SWARMQ_RESULT_TTL`) or fetch the result sooner.

## `ResultSerializationError`

**When raised.** Worker stored-result phase — the return value of
`run()` is not msgpack-compatible.

**Cause.** You returned a class that msgpack does not know how to
encode. Common offenders: live file handles, SQLAlchemy ORM objects,
`asyncio.Task` instances, custom classes without a conversion hook.

**Recovery.** Convert to a plain `dict`, `list`, `str`, `int`,
`float`, `bool`, `bytes`, or combinations thereof before returning.

```python
class FetchUser(Task):
    task_info = TaskInfo(name="fetch_user", queues=["default"])

    async def run(self, user_id: int) -> dict:
        user = await db.fetch_one(user_id)
        # Return a dict, not an ORM row object.
        return {"id": user.id, "email": user.email}
```

## `Retry`

**When raised.** By your own task code, to force a retry with a
specific delay.

**Cause.** Not an error — it is a control-flow signal. Raising it
tells the worker "this attempt failed, retry in `delay` seconds".

**Recovery.** No recovery needed; this is how you use it:

```python
from swarmq import Retry

class SendEmail(Task):
    task_info = TaskInfo(
        name="send_email", queues=["default"], max_retries=5,
    )

    async def run(self, to: str, subject: str) -> None:
        response = await smtp.send(to, subject)
        if response.status == 429:
            raise Retry(delay=60.0)  # retry in a minute
```

If `delay` is `None`, the worker applies the configured backoff
strategy (`retry_backoff`, `retry_base_delay`). A negative `delay` is
treated as 0 (retry immediately) and still counts against
`max_retries`; `inf` and `nan` raise `ValueError` when `Retry` is
created.

`Retry` retries even when the task sets `retry_exceptions` without
listing it. It counts against `max_retries`: when the budget is used
up, and always with `max_retries=0`, the task ends terminally (DLQ, or
the error result when dead-lettering is disabled).

## `NoRetry`

**When raised.** By your own task code, to skip the retry budget and
go straight to the DLQ.

**Cause.** Not an error. Raise it when the failure is permanent and
retrying would waste the budget.

**Recovery.** As with `Retry`, this is the intended usage:

```python
from swarmq import NoRetry

class ChargeCard(Task):
    task_info = TaskInfo(name="charge", queues=["payments"])

    async def run(self, card_id: str, amount: int) -> dict:
        try:
            return await stripe.charge(card_id, amount)
        except stripe.CardDeclinedError as exc:
            # No amount of retrying will make a declined card work.
            raise NoRetry(str(exc))
```

The task lands in `{queue}.XQ` immediately. Recover it with
`swarmq dlq retry <task_id>` after you fix the input, or
`swarmq dlq purge --queue payments` to clear the backlog.

## `SoftTimeoutError`

**When raised.** The task body is still running when `soft_timeout`
elapses. The worker runs `run()` under `asyncio.wait_for`: when the
time is up, it cancels `run()`, so the task sees an
`asyncio.CancelledError`, not a `SoftTimeoutError`. The worker then
stores `SoftTimeoutError` as the error result. It is retriable like
any other task error and skips `post_execution`. `SoftTimeoutError`
is not a `TimeoutError`, so `retry_exceptions=[TimeoutError]` does
not cover it.

Only the expiry of `soft_timeout` itself becomes `SoftTimeoutError`.
A `TimeoutError` that the task raises on its own (`asyncio.timeout`,
`socket.timeout`, `aiohttp.ServerTimeoutError`) stays what it is: it
keeps its class and message and is matched by
`retry_exceptions=[TimeoutError]`.

**Cause.** The task is taking longer than `TaskInfo(soft_timeout=N)`
allowed.

**Recovery.** Make the task faster or raise `soft_timeout`. To save
partial work, clean up in `finally` or catch the cancellation and
re-raise it:

```python
import asyncio

class LongJob(Task):
    task_info = TaskInfo(
        name="long_job", queues=["default"],
        soft_timeout=60, hard_timeout=120,
    )

    async def run(self) -> None:
        try:
            await process_items()
        except asyncio.CancelledError:
            # Commit whatever is done, then let the worker finalise.
            await checkpoint()
            raise
```

The `raise` is required. A task that swallows the `CancelledError`
and returns normally produces a **success** result with whatever it
returned; the worker records no `SoftTimeoutError`. `hard_timeout`
only comes into play when the task ignores or delays the
cancellation.

## `PublishIndeterminate`

**When raised.** By `schedule()`, `schedule_many()`, `run()` and
`apply()` when a publish exceeds `SWARMQ_PUBLISH_TIMEOUT_S` while
waiting for the broker's publisher confirm, or when the connection or
channel breaks in that phase (for example a broker restart or a network
cut right after the message was sent). Also when another publish on
the same channel exceeds its deadline after sending: SwarmQ then
discards the channel, and every publish still waiting for its confirm
there ends as `PublishIndeterminate`, even if its own deadline has not
run out; the next publish opens a new channel. Subclass of
`BrokerConnectionError`.

**Cause.** The broker or the network stalled or broke after the
message was sent; the broker may or may not have it.

**Recovery.** Treat delivery as unknown. Retry only when the task is
idempotent or was submitted with `unique_for` (SwarmQ keeps the dedup
claim in this case, so a retry within the window is refused instead of
running twice). See the warning under
[`BrokerConnectionError`](#brokerconnectionerror). A scheduler leader
does not count it towards stepping down: missing confirms usually hang
on one queue (for example a quorum queue without quorum), not on the
leader's connection. After a connection loss the following publishes,
which send nothing, fail as `BrokerConnectionError` and do count.

```python
from swarmq import BrokerConnectionError, PublishIndeterminate

try:
    await app.schedule("charge", order_id=42)
except PublishIndeterminate:
    # May have been enqueued -- check before sending again.
    ...
except BrokerConnectionError:
    # Nothing was sent; safe to retry.
    ...
```

## `SettleFailedAfterPublish`

**When raised.** Only inside the worker, by `retry_later` and
`dead_letter`: the broker confirmed the retry or dead-letter copy, but
acknowledging the original failed (usually because the channel is
gone). Subclass of `BrokerConnectionError`, so existing handlers keep
catching it. It never reaches `schedule()` or `get_result()`.

**Cause.** The channel or connection was lost between publishing the
copy and the ack, for example after a consumer timeout on RabbitMQ up
to 4.2.

**Recovery.** None needed in application code. The broker redelivers
the original while the copy is already on its way; the
[execution guard](../explanation/delivery-semantics.md#execution-guard)
sets its superseded or done marker as after success, so the
redelivered original is skipped. Import it from `swarmq.exceptions` if
you need it in tests.

## `MessageHeadersTooLarge`

**When raised.** By a publish (`schedule()`, and inside the worker by
retry and dead-letter copies) when the encoded message headers need
more than half of the negotiated `frame_max`, even after long texts
were truncated to 8 KiB. Subclass of `BrokerConnectionError`. Nothing
was sent.

**Cause.** Many or large headers of non-text values, for example a
middleware that adds hundreds of headers. SwarmQ's own diagnostic
headers are bounded and do not cause it.

**Recovery.** Do not retry with the same headers; they fail again.
Reduce the headers. Unlike `PublishIndeterminate`, a `unique_for` claim
is released, because nothing reached the broker.

## `UnroutableMessage`

**When raised.** By a publish (`send()`, `schedule()`, workflow steps,
and inside the worker by retry and dead-letter copies) when the broker
returns the message because no queue with that name exists. Subclass
of `BrokerConnectionError`. Raised at once, without retries; nothing
was delivered. Before 0.12 the broker confirmed such a publish and
dropped the message silently.

**Cause.** The queue was never declared or was deleted. Workers declare
the queues they consume, so the usual cause is a producer that starts
before the first worker of a new queue, a typo in `queue=`, or a task
with `TaskInfo(priority="high"|"low")` whose workers run without
`--priorities` (no `{queue}.high`/`.low`).

**Recovery.** Start the worker of the queue first, then publish again.
A `unique_for` claim is released, because nothing reached the broker.
The worker redeclares its own `{queue}.DQ`/`.XQ` once by itself; the
scheduler leader commits a lost cron fire and retries a deferred task
every 60 s up to `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`
([missing queues](../guides/scheduling-and-retries.md#missing-queues)).

## `BrokerPublishBlocked`

**When raised.** By a publish (`send()`, `schedule()`, workflow steps,
cron fires, and inside the worker by retry, deferral and dead-letter
copies) when RabbitMQ has blocked the publish connection
(`connection.blocked`, a memory or disk alarm) and does not unblock it
within `SWARMQ_PUBLISH_TIMEOUT_S` (during shutdown: the remaining grace
period). Subclass of `BrokerConnectionError`. The publish waits for the
unblock first and sends nothing while blocked; the WARNING
`publish_connection_blocked` marks the wait. A connection that is not
blocked costs no extra round trip.

**Cause.** A broker resource alarm. RabbitMQ announces the block
(`connection.blocked`) only to a connection that publishes during the
alarm. The publish that triggers the announcement can still be
confirmed; publishes that were already on the wire before
`connection.blocked` arrived hang and end as `PublishIndeterminate` --
per connection none, one or several. Later ones wait and raise this
error.

**In the worker.** A retry, deferral or dead-letter copy that waits for
the unblock keeps its task slot until the publish deadline; then the
original is handed back and runs again (a task that fails on every run
reaches the DLQ as `RedeliveryLimitError` after about 19 rounds). With
any failure rate above zero, a worker's consumption during the alarm
drops to roughly slots / (task duration + failure rate × publish
deadline): measured with a 20 s deadline to a quarter, with the default
120 s about 3 % by calculation. Acks keep flowing over the separate consume connection.
This is deliberate, not a fault. If alarms are frequent, consider a
shorter `SWARMQ_PUBLISH_TIMEOUT_S` for workers; the trade-off is more
extra runs. When the deadline cuts off a publish that may have been
sent, the worker discards its publish channel and closes it in the
background; during the alarm that close runs into its 5 s limit
(`publish_channel_close_incomplete` without traceback, `blocked=True`)
and aiormq logs `Got frame for closed channel` once RabbitMQ unblocks
([event catalog](logging.md#event-catalog)). Both are harmless.
Discarding the channel also ends every other confirm still open on it
as `PublishIndeterminate`. At the unblock this can hit a whole batch
of waiting copies when one of them had used up nearly all of its
deadline during the alarm (measured: 12 more copies with a 20 s
deadline). Their originals are redelivered as for any indeterminate
copy; the copies already reached the broker and run, and the execution
guard keeps each original from running next to or after its copy (in
the measured run all originals were skipped as `done`, no task ran
twice).

**Recovery.** Retrying later is safe, nothing reached the broker; a
`unique_for` claim is released. Free memory or disk on the broker. A
scheduler leader does not count it towards stepping down: an alarm
blocks every candidate. If aiormq no longer exposes the blocked state,
swarmq logs `publish_blocked_state_unreadable` once and publishes
unchecked, as before 0.12.

## `PublishRejected`

**When raised.** By a publish (`send()`, `schedule()`, workflow steps,
cron fires, worker copies) when RabbitMQ answers every one of the 5
attempts with a negative publisher confirm (`basic.nack`). Subclass of
`BrokerConnectionError`; the message text is the same as before 0.12
(`publish to '<queue>' failed after 5 attempts: …`).

**Cause.** The queue refuses the message, not the connection: a full
queue with `x-overflow=reject-publish` (the default for new quorum
queues since 0.12, together with a `max-length` policy), or a quorum
queue without a quorum of its members.

**Recovery.** Retrying later is safe, the broker did not store the
message; a `unique_for` claim is released. Fix the queue (raise or
remove the length limit, restore the quorum). A scheduler leader does
not count it towards stepping down: one broken cron queue would
otherwise move the leadership around without helping.

## `LockWaitExceeded`

**When raised.** Never; the name only appears as `error_type` in
`task_sent_to_dlq` and at the start of `x-swarmq-traceback` on the DLQ
message. A message whose task lock is held waits through the delay
queue. The worker dead-letters it with this error type when the same
holder has held the lock longer than the key's TTL plus 60 s (the text
says `lock holder stalled`) or when `SWARMQ_LOCK_WAIT_MAX_S` has passed
since the first conflict (`lock wait limit reached`). The error result
is stored, `TASK_DLQ` fires, and a workflow step marks its workflow
failed. Until 0.11 the worker dead-lettered at the 10th conflict as
`LockConflictLimitError`.

**Cause.** A holder that does not finish (hung task whose lock the
worker keeps alive, `hard_timeout` too long), or a lock that is busy
for longer than the limit allows.

**Recovery.** Look at the holder: the log line `lock_wait_exceeded`
names the lock (`lock_name`), `x-swarmq-lock-name` on the DLQ message
as well. A wedged lock is removed with `valkey-cli DEL [<namespace>:]sq:lock:<name>`.
Then replay the message with `swarmq dlq retry`. Raise
`SWARMQ_LOCK_WAIT_MAX_S` if long waits are expected.

## `RedeliveryLimitError`

**When raised.** Never; the name only appears as `error_type` in
`task_sent_to_dlq` and at the start of `x-swarmq-traceback` on the DLQ
message. The worker hands a delivery back to the queue
(`nack(requeue=True)`) when it cannot decide about it right now, for
example when Valkey answers with an error or a dead-letter publish
fails. On RabbitMQ 4.3 such a hand-back only raises
`x-acquired-count`, not `x-delivery-count`. Once `x-acquired-count`
reaches `SWARMQ_DELIVERY_LIMIT - 1`, the worker dead-letters the
delivery with this error type. If `x-delivery-count` reached the limit
first (hung worker, closed channel), the type stays `WorkerHungError`.

**Cause.** A failure that persists over all deliveries: Valkey keeps
answering with an error, the dead-letter queue cannot be published to,
or no compute slot becomes available.

**Recovery.** Fix the cause named in the worker log before the
dead-letter entry, then replay the message with `swarmq dlq retry`.

## `WorkflowValidationError`

**When raised.** When building a workflow with `chain`, `group` or
`chord`, before anything is enqueued (a step built with `sig()` by name
is checked for `FIRE_AND_FORGET` only at run time). Also at run
time inside the worker when a group or chord cannot assemble its
complete result list; the workflow is then marked failed and
`handle.get()` raises `TaskError`.

**Cause.** Nesting deeper than `max_workflow_depth` (10), a group with
more than `max_group_size` (1000) members, an empty chain or group, a
chord header that is not a `group(...)`, or a `FIRE_AND_FORGET` task as
a step.

**Recovery.** Flatten the topology, split large groups, or switch the
task to `DeliveryMode.AT_LEAST_ONCE`. See
[workflow primitives](api.md#workflow-primitives).

## `WorkflowNotFound`

**When raised.** `app.workflow(workflow_id)` and the `WorkflowHandle`
methods `status()`, `children()` and `get()` when no workflow state
exists for the id.

**Cause.** The id is wrong, the workflow was never started, or its
state expired (it lives for `result_ttl` seconds).

**Recovery.** Check the id. For long-running workflows raise
`result_ttl` (`SWARMQ_RESULT_TTL`) so the state outlives the run and
the time you need to read the result.

## `WorkflowResultExpired`

**When raised.** By `handle.get()` when the workflow completed but its
final result has already expired; inside the worker when a step result
needed for the next step or for fan-in expired, which fails the
workflow.

**Cause.** A result TTL shorter than the workflow's run time plus the
time until the result is read.

**Recovery.** Raise `result_ttl` (globally or per task via
`TaskInfo.result_ttl`) and read workflow results promptly.

## `WorkflowStateConflict`

**When raised.** Only inside the worker, while advancing a workflow:
the compare-and-set on the workflow state lost all 25 rounds against
concurrent writers. It never reaches `schedule()` or `handle.get()`.

**Cause.** More branches of one workflow finishing at the same moment
than the conflict limit allows.

**Recovery.** None needed in application code. The worker logs
`workflow_advance_backend_error` and redelivers the step message
(`at_least_once`); the redelivery advances the workflow, and
deterministic step ids keep it idempotent. Import it from
`swarmq.exceptions` if you need it in tests.

## Outside the hierarchy: `ForkSourceRejected`

`swarmq.supervision.ForkSourceRejected` is a `RuntimeError`, not a
`SwarmQError`, so `except SwarmQError` does not catch it.

**When raised.** By `Worker.start()` (and so `app.start_worker()`) of an
embedded CPU pool, after the worker shut down gracefully because its
pool rejected the forkserver: the forkserver restarted silently and
then ran more than one thread, so forking from it is no longer safe.

**Cause.** A thread started at import time in the forkserver, usually
a BLAS/OpenMP computation, a logging `QueueListener` of your app or
the opt-in log queue. The worker logs
`forkserver_restarted_multithreaded` with the thread names and a hint,
then `compute_fork_source_rejected`.

**Recovery.** Restart the worker process; a new process brings a fresh
forkserver. Under `swarmq worker` this happens on its own: the worker
subprocess exits with code `71` and the supervisor restarts it within
the restart budget (`worker_fork_source_rejected`). See the
[CPU mode guide](../guides/cpu-mode.md).

## See also

- [API reference](api.md) — the methods that raise these exceptions
- [Logging](logging.md) — logging settings and `--log-config`
- [Scheduling and retries](../guides/scheduling-and-retries.md) — how
  retry policy interacts with `Retry` and `NoRetry`
- [Dead letter queues](../guides/dead-letter-queues.md) — operator
  tools for recovering DLQ'd tasks
