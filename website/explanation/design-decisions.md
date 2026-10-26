# Design decisions

SwarmQ's public surface is small on purpose. Each decision on this page
trades flexibility for a library that a team can still reason about
after a year in production. Every section follows the same shape: the
**decision**, the alternatives, the reason, and what it costs you.

Behaviour that was decided deliberately is recorded in a decision
register in the repository, one rule per decision with an ID such as
`E-072`. This page cites those IDs where a section rests on one. If
something on this list surprises you in production, it is a known
trade-off, not a bug.

## At-least-once, never exactly-once

**Decision.** Every task runs at least once. SwarmQ narrows duplicates
where it can, but it promises no exactly-once execution.

**Alternatives considered.** Exactly-once through distributed
transactions or a fencing service; at-most-once (ack before running)
as the default.

**Why.** No queue-plus-worker system can guarantee exactly-once
without help from the application: a worker can always die after the
side effect and before the ack. Losing work silently is worse than
running it twice, so SwarmQ chooses at-least-once and then makes
duplicates rare. The [execution guard](delivery-semantics.md#execution-guard)
skips a redelivery of an execution that already finished, a replay
from the DLQ is a new execution (E-009), and a scheduled fire whose
worker crashed runs again rather than being dropped (E-046).
`FIRE_AND_FORGET` tasks opt into at-most-once explicitly.

**What this costs you.** Task bodies must be idempotent; see
[idempotency expectations](delivery-semantics.md#idempotency-expectations).

## Ack only after the result is stored

**Decision.** A worker acknowledges an `AT_LEAST_ONCE` message only
after its terminal state is durable: the result stored in Valkey, or
the retry or dead-letter copy confirmed by the broker. If storing the
result fails, the worker retries for about half a minute before it
hands the message back (E-072). A dead-letter copy that is safe in
`{queue}.XQ` counts as terminal even if the ack of the original fails
afterwards (E-043).

**Alternatives considered.** Ack on receipt (fast, loses work on every
crash); ack after `run()` but before storing (loses results on a
backend outage).

**Why.** The ack is the only point where the broker forgets a message.
Moving it behind the durable store means a crash at any point leaves
either the message or the result, never neither.

**What this costs you.** A Valkey outage longer than the store retry
makes finished tasks run again, and the task slot stays busy while the
worker retries.

## Retries go through delay queues, never `requeue=True`

**Decision.** A retry publishes a fresh copy of the message to a delay
queue with its own expiration and acks the original once the broker
confirmed the copy. RabbitMQ returns the copy to `{queue}` when it
expires. Seven delay tiers from 1 s to 1 h keep short waits from
queuing behind long ones (E-026). Deferrals that are not retries (rate
limit, lock conflict, backend outage, execution guard, unknown task)
use the same mechanism.

**Alternatives considered.** `basic.nack(requeue=True)` with a sleep in
the worker; retrying inside `run()`; the RabbitMQ delayed-message
plugin.

**Why.** `requeue=True` hands the message to the next free consumer at
once, which hot-loops the broker and burns the retry budget in
milliseconds. A delay queue gives exponential, linear or fixed backoff
for free (`fixed` waits exactly `base_delay`, without jitter, E-023),
keeps the worker's slots free for other work and needs no broker
plugin. A backend outage defers through the same queues instead of
requeuing in place, which would drain the delivery limit after about
20 rounds.

**What this costs you.** One extra publish per retry, seven delay tiers
per queue (21 with priorities), and a wait inside one tier of up to
four times the requested delay. Waits over one hour share
`{queue}.DQ` and can block each other.

## Async-first API, no sync adapter

**Decision.** Every public coroutine is `async def`. There is no
synchronous wrapper that runs an event loop under the hood.

**Alternatives considered.** A sync shim so tasks can be scheduled
from any Python code; two parallel APIs, one sync and one async.

**Why async-only.** asyncio on Python 3.14 is fast enough for the
I/O-bound work a task queue does. A sync shim forces an
`asyncio.run()` per call, which leaks event loops in long-lived
processes and breaks when called from inside a running loop. Two APIs
double the test matrix and invite users to pick the less safe one.
CPU-bound task bodies get [CPU mode](../guides/cpu-mode.md) instead of
threads.

**What this costs you.** If your app is not async yet, you need an
entry point: `asyncio.run(main())` for scripts, your framework's async
lifespan for web apps (FastAPI, Starlette, Quart, Django async views).

## Class-based `Task` with declarative `TaskInfo`

**Decision.** Tasks subclass `swarmq.Task` and declare their identity
in a class-level `task_info = TaskInfo(...)`. A class registers itself
when it is defined, and `TaskInfo` validates every field when the task
module is imported (E-065).

**Alternatives considered.** Decorated async functions (`@app.task`),
the shape Celery and arq use.

**Why classes.** Task metadata (queue, retry policy, rate limit, lock,
timeouts, cron, dedup, expiry) is a lot of configuration to thread
through a decorator. On a dataclass it stays inspectable:
`SendEmail.task_info.max_retries` is a plain attribute read, and
`swarmq inspect` just walks the registered classes. Validating at
import turns a typo in a cron expression into a start error instead of
a silent cron that never fires. `run()` must not take the parameter
names that `schedule()` itself uses (`delay`, `eta`, `unique_for`,
`unique_until`); the class fails to build otherwise (E-064).

**What this costs you.** A few more lines per task than a decorator.
In exchange each task is a stable type that your IDE, type checker and
tests can reason about.

## JSON on the wire, msgpack at rest

**Decision.** Task messages are JSON, encoded with `msgspec`. Results
are msgpack. There is no `pickle`, protobuf or Avro.

**Alternatives considered.** `pickle` (Celery's former default,
rejected outright: deserialising it can execute arbitrary code).
Protobuf (a schema registry for a single-language library). JSON for
both (results often carry datetimes, Decimals or bytes, which msgpack
keeps natively).

**Why this split.** A JSON message is readable without tooling:
`swarmq dlq inspect` or the RabbitMQ UI show you the payload. msgpack
keeps the Python types your task returned without hand-written
serialisation. Both are fast, safe and deterministic, which also makes
the dedup hash of `unique_for` stable.

**What this costs you.** Task arguments must be JSON-compatible.
Convert NumPy arrays or custom classes before you schedule. That keeps
your task contract language-neutral.

## Results in Valkey, not in the broker

**Decision.** Results, progress and workflow state live in Valkey. The
broker only carries task messages.

**Alternatives considered.** RPC reply queues; a results queue with
correlation-id matching.

**Why.** RabbitMQ queues are poor key-value stores: reading a result
consumes it, there is no per-key TTL, and "what was the result of task
X" needs a search. Valkey answers `GET sq:result:{task_id}` in O(1),
with a TTL set on write, and it already holds the rate limiter, locks
and scheduler: one connection pool, not two.

**What this costs you.** A second service to run, with rules: no
evicting `maxmemory-policy`, AOF persistence, no cluster mode
([Valkey requirements](../reference/configuration.md#valkey-requirements)).

## Fail at start, not at 3 a.m.

**Decision.** A worker refuses to start with exit code 78 when its
configuration cannot work: an invalid value, an import or syntax error
in the task module (E-057), queue arguments that conflict with the
existing queue (E-016), a Valkey that may evict keys (E-005), an
infinite deadline (E-063). The supervisor does not restart such a
worker. `swarmq check` and `swarmq worker --check` predict the same
refusal without starting (E-006, E-062).

**Alternatives considered.** Start anyway and log warnings; crash with
exit 1 and let the orchestrator retry.

**Why.** Each of these mistakes otherwise shows up hours later as
duplicated or lost work. Exit 78 (`EX_CONFIG`) tells the orchestrator
and the operator that a restart will not help.

**What this costs you.** A misconfigured rollout stops visibly: under
Kubernetes the pods go into `CrashLoopBackOff`. Run `swarmq check`
before you deploy ([deployment](../guides/deployment.md)).

## Quorum queues, with at-least-once dead-lettering

**Decision.** Queues are
[quorum queues](https://www.rabbitmq.com/docs/quorum-queues) unless
`SWARMQ_QUEUE_TYPE=classic` is set. New queues get
`x-dead-letter-strategy=at-least-once` with `x-overflow=reject-publish`;
existing queues keep their arguments, and the worker warns (E-017).

**Alternatives considered.** Classic mirrored queues (removed upstream),
streams (append-log semantics, wrong shape for tasks), changing
existing queues automatically (only possible by deleting them).

**Why quorum.** Raft replication with predictable failover, a native
delivery limit and native dead-lettering, without mirror-policy tuning.
RabbitMQ itself recommends them for durable work. Their default
dead-lettering is at-most-once, which can lose a retry on a leader
change, so new queues opt into at-least-once.

**What this costs you.** Slightly more overhead per message than a
classic queue on one node. Migrating an existing queue is an explicit
operator step ([changing queue arguments](delivery-semantics.md#changing-queue-arguments)).

## Workflows without an orchestrator

**Decision.** `chain`, `group` and `chord` are built on ordinary task
messages. There is no orchestrator process: the worker that finishes a
step advances the workflow in Valkey and publishes the next steps.
Steps must store a result, so `FIRE_AND_FORGET` steps are rejected
(E-039). A step that fails terminally fails the whole workflow at once
(E-040).

**Alternatives considered.** A central orchestrator service; no
workflow primitives at all, leaving composition to `await app.run(...)`
inside a parent task.

**Why.** A parent task that awaits its children holds a slot the whole
time and loses its place on every crash. Advancing
in the worker needs no extra process and inherits the delivery
guarantees of the steps. Failing fast means `get()` never waits for a
timeout on a workflow that is already lost.

**What this costs you.** Fixed limits (depth 10, 1000 group members),
workflow state that lives only as long as `result_ttl`, and the API
shape (`Task.s()`, `Task.si()`) borrowed from Celery's canvas without
being compatible with it. See the [workflows guide](../guides/workflows.md).

## An embedded scheduler with a leader

**Decision.** Every worker embeds the scheduler; one of them holds a
leader lock in Valkey and publishes due crons and delayed tasks. Static
crons come from `TaskInfo`; workers report them, and the leader holds
or removes crons that no worker reports any more (E-014, E-049). A
short double leader is accepted; the fire claim in the worker drops
its duplicate publishes (E-047).

**Alternatives considered.** A separate scheduler process (Celery
beat), crons in the orchestrator (Kubernetes `CronJob`), a consensus
service for fencing.

**Why.** A separate process is one more thing to deploy, monitor and
keep singular. Any worker can take over within a lock TTL. True fencing
would require a consensus service SwarmQ does not assume, and the fire
claim makes a double publish harmless.

**What this costs you.** A fire can run twice after a crash (at least
once, E-046), and crons pause for up to a lock TTL during a failover.
Set `SWARMQ_SCHEDULER=off` on workers that should never lead.

## Signals as in-process pub/sub, not a broker

**Decision.** Signal handlers are awaited in the process that emitted
the event (worker or client).

**Alternatives considered.** Publishing lifecycle events to a broker
topic so other services can subscribe.

**Why in-process.** Signals cover operational concerns: metrics,
logging, tracing, error reporting. Those consumers live inside the
worker anyway, so a network hop is pure cost. A cross-service event
bus is a separate concern you can build on your own topic.

**What this costs you.** Nothing at the signal boundary. For
cross-process events, `schedule()` a follow-up task from your handler.

## A cancel is not an error

**Decision.** Cancelling a task ends it as cancelled: the task sees
`asyncio.CancelledError`, the caller of `get_result()` gets
`TaskCancelledError`, middleware sees neither `on_error` nor
`post_execute`, only the `TASK_CANCELLED` signal fires, and no retry
follows (E-068).

**Alternatives considered.** Treating a cancel as a failure that runs
through the retry and error hooks.

**Why.** A cancel is a decision, not a fault. Counting it as an error
would trigger retries, alerts and error tracking for something the
operator asked for.

**What this costs you.** Clean-up that must also run on a cancel
belongs in a `finally` block or the middleware's finalizer, not in
`on_error`.

## `--module` is mandatory, hooks go through `--configure`

**Decision.** Every `swarmq worker` needs `--module package.name`.
There is no auto-discovery of tasks. The worker builds its own `SwarmQ`
app; middleware and signal handlers are registered by a callable passed
with `--configure MODULE:CALLABLE` (E-069).

**Alternatives considered.** Scanning `sys.path`, reading a config
file, Celery's `-A app` discovery, picking up a module-level `app`
from the task module.

**Why explicit.** The module import is where the task registry is
built. Making it explicit means container images declare it once, a
typo fails at start with exit 78 instead of leaving an empty registry,
and `swarmq inspect` reuses the same argument. A module-level `app` in
the task module does not exist yet when the CLI builds its own, so
handlers registered on it would silently do nothing.

**What this costs you.** One flag, and one small hook function if you
use middleware or signals.

## The namespace separates Valkey, the vhost separates RabbitMQ

**Decision.** `SWARMQ_NAMESPACE` prefixes every Valkey key, channel,
the scheduler leader and the control channel. Queue names never get a
prefix; apps that share a RabbitMQ use separate vhosts (E-012).

**Alternatives considered.** Prefixing queue names too.

**Why.** Several apps on one Valkey shared locks, rate limits, dedup
claims and the leader. RabbitMQ already has vhosts for isolation, and
prefixed queue names would have broken every existing topology.

**What this costs you.** Two apps in the same vhost share their queues
even with different namespaces. Moving an app into a namespace needs a
planned migration ([namespace](../reference/configuration.md#namespace-several-apps-on-one-valkey)).

## No Celery compatibility layer

**Decision.** SwarmQ has no migration layer for Celery apps; tasks are
not plug-compatible with `@celery.task`.

**Alternatives considered.** A `swarmq.compat.celery` module that maps
Celery's API onto SwarmQ.

**Why.** Celery's surface is enormous: request context, eager mode,
routers, canvas, custom serialisers and result backends, signal names,
exception classes. Any meaningful subset leaves users guessing what is
supported, and the shim would outgrow SwarmQ itself. A clean port is
simpler and ages better.

**What this costs you.** A real port: rewrite each `@app.task` as a
`Task` subclass and adapt its signature.

## Deliberately left out

- **A synchronous client API.** See [async-first](#async-first-api-no-sync-adapter).
- **Multi-broker routing.** One RabbitMQ per app; for isolation run two
  apps.
- **Database as broker or backend.** Postgres queueing suits small
  workloads; supporting it next to RabbitMQ and Valkey would double the
  maintenance surface.
- **A built-in web dashboard.** SwarmQ exposes Prometheus metrics
  ([`--metrics-port`](../reference/cli.md#swarmq-worker)), health
  endpoints and the [control channel](../reference/cli.md#control-channel);
  dashboards and control services build on those, outside the library.
- **Valkey cluster mode.** Keys carry hash tags, but SwarmQ is run and
  tested against a single primary.

If one of these becomes a real blocker, the decision can be revisited
with a new argument. Until then, the carrying cost of "probably useful
someday" is higher than the cost of saying no.

## See also

- [Architecture](architecture.md): how the parts implement these
  decisions
- [Delivery semantics](delivery-semantics.md): the at-least-once
  contract in detail
