# Workflows

You want several tasks to run as one unit of work: fetch, then
transform, then store; or download ten files in parallel and merge
them once all ten are done. Workflows compose tasks into a chain, a
group or a chord, start them with one call and give you a handle to
wait for, inspect or cancel the whole thing.

## Prerequisites

- A `SwarmQ` instance and a running worker that consumes the queues of
  every task in the workflow.
- The task classes imported **before** `SwarmQ(...)` is constructed,
  in the client and in every worker. The process that enqueues a step
  resolves its queue from the task registry: the client for the first
  steps, the worker that finished the predecessor for every later
  step.

## The three primitives

| Primitive | Runs | Result |
|---|---|---|
| `chain(a, b, c)` | one after the other; each step's result is prepended to the next step's arguments | the last step's result |
| `group(a, b, c)` | all members in parallel | list of member results, in **definition order**, not completion order |
| `chord(group(a, b, c), body)` | the header group in parallel, then `body` exactly once with the list of header results | the body's result |

All three are importable from `swarmq`:

```python
from swarmq import chain, chord, group, sig
```

## Signatures: `Task.s()`, `Task.si()`, `sig()`

A workflow step is a `Signature`: a task name plus bound arguments.
Build one from a task class:

```python title="myapp/tasks.py"
from swarmq import Task, TaskInfo


class Fetch(Task):
    task_info = TaskInfo(name="fetch", queues=["default"])

    async def run(self, url: str) -> str:
        ...


class Parse(Task):
    task_info = TaskInfo(name="parse", queues=["default"])

    async def run(self, html: str, strict: bool = False) -> dict:
        ...


class Store(Task):
    task_info = TaskInfo(name="store", queues=["default"])

    async def run(self, doc: dict) -> str:
        ...


class Notify(Task):
    task_info = TaskInfo(name="notify", queues=["default"])

    async def run(self, message: str) -> None:
        ...


class Merge(Task):
    task_info = TaskInfo(name="merge", queues=["default"])

    async def run(self, pages: list[str]) -> str:
        ...
```

- `Fetch.s("https://example.com")` — a **mutable** signature. Inside a
  chain, the predecessor's result is inserted as the first positional
  argument: `Parse.s(strict=True)` after `Fetch` runs as
  `Parse.run(<html>, strict=True)`.
- `Notify.si("done")` — an **immutable** signature. The predecessor's
  result is not injected; the step runs with exactly the bound
  arguments.
- `sig("parse", strict=True)` — a signature by task name, without a
  reference to the class. Pass `immutable=True` for the `si` behaviour.
  The task must still be registered in the enqueuing process (see
  [prerequisites](#prerequisites)).

Arguments must be JSON-serialisable, like those of `schedule()`.

## Composing

```python
from swarmq import chain, chord, group

from myapp.tasks import Fetch, Merge, Notify, Parse, Store

urls = ["https://example.com/a", "https://example.com/b"]

# Sequential: fetch → parse → store, then a notification that ignores
# the stored id.
pipeline = chain(
    Fetch.s("https://example.com"),
    Parse.s(strict=True),
    Store.s(),
    Notify.si("page stored"),
)

# Parallel: one fetch per URL; the result is a list of strings in URL order.
fan_out = group(Fetch.s(u) for u in urls)

# Fan-out + fan-in: the body receives the list of header results as
# its first argument.
merged = chord(group(Fetch.s(u) for u in urls), Merge.s())
```

The `|` operator is shorthand:

```python
Fetch.s(u) | Parse.s()    # same as chain(Fetch.s(u), Parse.s())
chain(...) | Store.s()    # same as chain(..., Store.s())
group(...) | Merge.s()    # same as chord(group(...), Merge.s())
```

`group()` accepts members as arguments or as one iterable (a list or a
generator expression).

### Nesting

A chain step, a group member and a chord body may themselves be a
chain, group or chord:

```python
# The group's members each receive Fetch's result; the chain's result
# is the group's list.
chain(Fetch.s(urls[0]), group(Parse.s(), Parse.s(strict=True)))

# A composite group member contributes its own result to the list:
# [<html of a>, <parsed b>]
group(Fetch.s(urls[0]), chain(Fetch.s(urls[1]), Parse.s()))
```

A step that follows a group or chord in a chain receives the whole
list as its first argument.

## Starting a workflow: `app.apply()`

```python
async with SwarmQ(broker, backend) as app:
    handle = await app.apply(
        chord(group(Fetch.s(u) for u in urls), Merge.s())
    )
    print(handle.workflow_id)
    result = await handle.get(timeout=60)
```

`apply()` persists the topology in Valkey, publishes the first steps
(the first chain step, all group members, or the chord header) and
returns a `WorkflowHandle` immediately. The workers drive the rest:
when a step succeeds, the worker that ran it enqueues the next steps,
and the last member of a group or chord header to finish triggers the
fan-in exactly once. The client process can exit; the workflow keeps
running.

Every step is an ordinary task execution: retries, timeouts, rate
limits and locks of its `TaskInfo` apply. Step messages are always
published as `AT_LEAST_ONCE`.

The workflow state lives for `result_ttl` of the `SwarmQ` instance
that called `apply()` (default 86400 s). Set `result_ttl` high enough
to cover the whole workflow including retries.

## The `WorkflowHandle`

| Member | Returns | Notes |
|---|---|---|
| `workflow_id` | `str` | Stable id; store it to reattach later. |
| `await status()` | `"running"`, `"completed"`, `"failed"` or `"cancelled"` | |
| `await children()` | `dict[str, dict]` | One entry per enqueued step, keyed by its topology path (see below): `{"task_id", "status", "result"?}`. `status` is `"running"` (no result yet), `"completed"` (with `result`) or `"failed"`. Empty before any step is enqueued. |
| `await cancel()` | `bool` | Sets the workflow cancel flag; `True` if it was newly set. The flag lives `max(24 h, result_ttl)`, also on a reattached handle (since 0.13.0). |
| `await get(timeout=30.0)` | the final result | Waits until the workflow is terminal; see [failure modes](#failure-modes). |

Reattach from any process with the id:

```python
handle = await app.workflow(workflow_id)  # WorkflowNotFound if absent
print(await handle.status())
```

### Step paths

`children()` keys steps by their position in the topology: chain step
`i` is `/i`, group member `i` is `/gi`, the chord header is `/h` (its
members `/h/g0`, `/h/g1`, …) and the chord body `/b`. Nested paths
concatenate: in `chain(Fetch.s(u), group(Parse.s(), Parse.s()))` the
group members are `/1/g0` and `/1/g1`. The same path is the `step_path`
of middleware contexts and of the `WORKFLOW_STEP_ENQUEUED` signal.

## Cancelling

`await handle.cancel()` is cooperative:

- `get()` raises `TaskCancelledError` promptly.
- Steps not yet picked up by a worker — including group and chord
  members that are already enqueued — are stopped at pickup and finish
  as cancelled.
- A step that is already running runs to completion; the worker then
  enqueues no further steps.

## Limits

Validated when you build the workflow, before anything is enqueued;
a violation raises `WorkflowValidationError`:

| Limit | Value |
|---|---|
| Nesting depth (`max_workflow_depth`) | 10. A signature counts 1, each enclosing chain, group or chord adds 1. |
| Group size (`max_group_size`) | 1000 members per group. |
| Empty compositions | `chain()` and `group()` need at least one element. |
| Chord header | must be a `group(...)`. |
| Delivery mode | A `Task.s()`/`Task.si()` of a `FIRE_AND_FORGET` task is rejected: it acks before running and could never advance the workflow. |

Both limits are fixed constants, not configuration options. Flatten
deeper topologies or split very large fan-outs into several
workflows.

## Failure modes

| What happens | Result |
|---|---|
| A step fails and has retries left | It is retried like any task; the workflow keeps running. |
| A step fails terminally (retries exhausted, `NoRetry`, delivery limit, `max_age` exceeded, lock template error, end of a lock wait, task not registered in the worker) | The worker marks the workflow `failed` when it dead-letters the step; `get()` raises `TaskError` with the step's error message. `TaskError.error_class` names the class of the step's exception (e.g. `builtins.ValueError`), or the swarmq class of the terminal path (`swarmq.exceptions.LockWaitExceeded`, `TaskNotFoundError`, `RedeliveryLimitError`); it is `None` after `max_age` expiry, a lock template error or a hung worker, and for workflows failed by an older worker. No further steps run, and a chord body never runs if a header member failed. A late duplicate of a step that already succeeded does not mark the workflow failed (`workflow_fail_skipped_step_done`). |
| A step whose success is already stored is dead-lettered late (for example at the delivery limit, after its advance failed) | Since 0.13.0 the worker completes the workflow advance with the stored result instead of marking the workflow failed (`workflow_advance_caught_up`); the message is acked without a DLQ copy and without `TASK_DLQ`. If that last advance fails with an error reply from Valkey (`BackendResponseError`, for example `WRONGTYPE` on the chord or result key), the workflow is `failed` since 0.13.1 and `get()` raises `TaskError` with the error text; the message is acked without a DLQ copy and the stored success stays. If it fails with an outage (connection, timeout) or the failure mark itself fails, the message goes to the DLQ without an error result and the workflow stays `running` until its TTL. |
| The advance of a `FIRE_AND_FORGET` step (built with `sig()` and an explicit `result_ttl`) gets an error reply from Valkey | Since 0.13.1 the workflow is `failed` at once, because the step is never redelivered; `get()` raises `TaskError` with the error text. After an outage the workflow stays `running` until its TTL. |
| The workflow state expired while a step was still running | The step's result cannot advance the workflow; the worker logs `workflow_state_missing` (WARNING, field `workflow_id`). Raise `result_ttl` for long workflows. |
| A middleware `pre_enqueue` raises for one of the first steps (in the client) | `apply()` raises that exception; nothing is persisted or published. |
| A middleware `pre_enqueue` raises for a later step (in the worker) | No step of that front is published; the workflow is `failed` and `get()` raises `TaskError` with the exception's message. See [middleware for workflow steps](middleware.md#workflow-steps). |
| A step's task is not registered | In the client, `apply()` raises `TaskNotFoundError`. In the worker that enqueues the step, the workflow is `failed` and `get()` raises `TaskError`. |
| The queue of the next step does not exist (no worker has declared it yet) | The worker marks the workflow `failed` at once (recorded error type `UnroutableMessage`, log `workflow_advance_unroutable`) instead of redelivering the step up to the delivery limit; `get()` raises `TaskError`. Start workers before producers ([missing queues](scheduling-and-retries.md#missing-queues)). |
| A step built with `sig()` turns out to be `FIRE_AND_FORGET` (or stores no result, `result_ttl=0`) | Caught at run time: the workflow is `failed` (recorded error type `WorkflowValidationError`); `get()` raises `TaskError`. |
| The workflow is cancelled | `get()` raises `TaskCancelledError`. |
| A single step is cancelled with `cancel(task_id)`, before or while it runs | The workflow is `failed`; `get()` raises `TaskError` with the message `task was cancelled`. No further steps run. A step that already succeeded does not mark the workflow failed. If both the step and its workflow are cancelled, the step's cancel wins (`failed`). |
| `get()` times out while the workflow is still running | `ResultNotReadyError`; the workflow keeps running, call `get()` again. |
| The workflow completed but its final result expired | `WorkflowResultExpired`; raise `result_ttl`. |
| A member result needed for fan-in expired, or a member result is missing (lost) | The workflow is `failed` (recorded error type `WorkflowResultExpired`; until 0.12, `WorkflowValidationError` "resolved N of M" for a missing member) instead of passing an incomplete list on; `get()` raises `TaskError`. A member whose step marker is persisted late no longer fails the workflow, because fan-in derives member result ids from the workflow id and step path (since 0.13.0). |
| The workflow state no longer exists (never started, or expired) | `app.workflow(id)`, and `status()`/`children()`/`get()` on an existing handle, raise `WorkflowNotFound`. |

`WorkflowValidationError`, `WorkflowNotFound`, `WorkflowResultExpired`,
`TaskError`, `TaskCancelledError` and `ResultNotReadyError` are
importable from `swarmq`. `WorkflowStateConflict`
(`swarmq.exceptions`) is internal to the worker: it means concurrent
branches kept overwriting each other's state update, and the step
message is redelivered — you do not handle it in client code.

## Observing workflows

- **Logs:** every event logged while a step runs carries the
  `workflow_id` [task context field](../reference/logging.md#task-context-fields).
- **Signals:** each published step fires `TASK_ENQUEUED` with its real
  task name and then `WORKFLOW_STEP_ENQUEUED` with `workflow_id` and
  `step_path` (see [signals](signals.md#available-signals)).
- **Middleware:** `pre_enqueue`/`post_enqueue` run for every step, with
  `workflow_id` and `step_path` in the message context.

## Verify

With a worker running for the `default` queue:

```python
import asyncio

from swarmq import SwarmQ, chain, group
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker

from myapp.tasks import Fetch, Parse  # import before SwarmQ(...)


async def main() -> None:
    async with SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    ) as app:
        handle = await app.apply(
            chain(Fetch.s("https://example.com"), group(Parse.s(), Parse.s(strict=True)))
        )
        print(await handle.get(timeout=60))  # list of two parse results
        print(await handle.status())          # completed
        print(sorted(await handle.children()))  # ['/0', '/1/g0', '/1/g1']


asyncio.run(main())
```

## See also

- [Middleware](middleware.md#workflow-steps) — how `pre_enqueue` sees
  workflow steps
- [Signals](signals.md) — `WORKFLOW_STEP_ENQUEUED`
- [Error catalog](../reference/errors.md) — the exception classes
- [Delivery semantics](../explanation/delivery-semantics.md) — why
  steps must be idempotent under at-least-once delivery
