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
| `BrokerConnectionError` | `SwarmQError` | Cannot reach RabbitMQ |
| `PublishIndeterminate` | `BrokerConnectionError` | A publish broke its deadline; whether it arrived is unknown |
| `BackendConnectionError` | `SwarmQError` | Cannot reach Valkey |
| `BackendResponseError` | `BackendConnectionError` | Valkey answered an operation with an error (`WRONGTYPE`, script error) |
| `TaskNotFoundError` | `SwarmQError` | `schedule()` called with an unknown task name |
| `TaskError` | `SwarmQError` | The task body raised; propagated to `get_result()` |
| `TaskCancelledError` | `TaskError` | `app.cancel(task_id)` tripped during execution |
| `ResultNotReadyError` | `SwarmQError` | `get_result()` timed out before a result landed |
| `ResultExpiredError` | `SwarmQError` | Result existed but Valkey TTL expired it |
| `ResultSerializationError` | `SwarmQError` | Result cannot be msgpack-serialised |
| `Retry` | `SwarmQError` | Control flow — raised by task code to request a retry |
| `NoRetry` | `SwarmQError` | Control flow — raised by task code to bypass retries |
| `SoftTimeoutError` | `SwarmQError` | Task exceeded `soft_timeout`, asked to wrap up |
| `WorkflowValidationError` | `SwarmQError` | Workflow topology invalid, or fan-in results incomplete |
| `WorkflowNotFound` | `SwarmQError` | `app.workflow(id)` or a handle call found no workflow state |
| `WorkflowResultExpired` | `SwarmQError` | A step or final workflow result expired before it was used |
| `WorkflowStateConflict` | `SwarmQError` | Worker-internal: workflow state update lost every compare-and-set round |

Every class except `WorkflowStateConflict` can be imported from
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

## `BrokerConnectionError`

**When raised.** Worker startup when RabbitMQ is not reachable within
the connect timeout, or during a reconnect attempt that exceeds the
retry budget. Also when a publish exceeds `SWARMQ_PUBLISH_TIMEOUT_S`
(default 120s) — see the delivery caveat below.

!!! warning "A publish-deadline failure does not mean the message was not sent"

    When the message reads `exceeded the publish deadline`, delivery is
    **indeterminate**: the deadline runs out while waiting for the broker's
    publisher confirm, so the broker may already hold the message. Retrying
    such a call can execute the task a second time. This is different from
    every other cause listed here, where nothing was sent.

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
the string the worker stored.

**Recovery.** Read the worker logs — they carry the full traceback.
Then fix your task body, or widen your `retry_exceptions` so the
transient class is retried instead of DLQ'd.

## `TaskCancelledError`

**When raised.** `get_result()` observes a cancel flag in the stored
result. Also raised inside a task body when the worker's cancel
monitor trips.

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
DLQ'd without storing a result, or the task_id is wrong.

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

**When raised.** `get_result()` finds the result key gone while a
metadata marker still indicates the task completed. In v0.x this is
rare — the backend usually raises `ResultNotReadyError` for the
"gone" case — and the class is kept as a stable hook for a future
release that splits the two.

**Recovery.** Raise `result_ttl` (`SWARMQ_RESULT_TTL`) or fetch the
result sooner.

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
strategy (`retry_backoff`, `retry_base_delay`).

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
elapses. The worker cancels the task with this class as the cause so
the task can wrap up gracefully before `hard_timeout` kills it.

**Cause.** The task is taking longer than `TaskInfo(soft_timeout=N)`
allowed.

**Recovery.** Make the task faster, raise `soft_timeout`, or catch
and checkpoint:

```python
from swarmq import SoftTimeoutError

class LongJob(Task):
    task_info = TaskInfo(
        name="long_job", queues=["default"],
        soft_timeout=60, hard_timeout=120,
    )

    async def run(self) -> None:
        try:
            await process_items()
        except SoftTimeoutError:
            # Commit whatever is done, then let the worker finalise.
            await checkpoint()
            raise
```

## `PublishIndeterminate`

**When raised.** By `schedule()`, `schedule_many()`, `run()` and
`apply()` when a publish exceeds `SWARMQ_PUBLISH_TIMEOUT_S` while
waiting for the broker's publisher confirm. Subclass of
`BrokerConnectionError`.

**Cause.** The broker or the network stalled after the message was
sent; the broker may or may not have it.

**Recovery.** Treat delivery as unknown. Retry only when the task is
idempotent or was submitted with `unique_for` (SwarmQ keeps the dedup
claim in this case, so a retry within the window is refused instead of
running twice). See the warning under
[`BrokerConnectionError`](#brokerconnectionerror).

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

## See also

- [API reference](api.md) — the methods that raise these exceptions
- [Logging](logging.md) — logging settings and `--log-config`
- [Scheduling and retries](../guides/scheduling-and-retries.md) — how
  retry policy interacts with `Retry` and `NoRetry`
- [Dead letter queues](../guides/dead-letter-queues.md) — operator
  tools for recovering DLQ'd tasks
