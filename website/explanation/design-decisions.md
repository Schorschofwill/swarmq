# Design decisions

SwarmQ's public surface is small on purpose. Every decision on this
page was an explicit trade-off between flexibility and a library that
stays understandable after a year on a real team. The format is:
**decision**, what was considered, why SwarmQ picked this one.

## Async-first API, no sync adapter

**Decision.** Every public coroutine is `async def`. There is no
synchronous wrapper that runs an event loop under the hood.

**Alternatives considered.** A sync shim (the Celery-compatibility
shape) so tasks can be scheduled from any Python code. Two APIs,
one sync and one async, side-by-side.

**Why async-only.** Python 3.11+ asyncio is competitive with
`anyio`, uvloop, and threaded executors for the I/O-bound work a
task queue actually does. A sync shim would force a
`asyncio.run()`-per-call pattern that leaks event loops in
long-lived processes and silently breaks when called from inside an
existing loop. Offering both shapes doubles the test matrix and
encourages users to pick the less safe one.

**What this costs you.** If your app is not already async, you need
an entry point. For one-shot schedulers, `asyncio.run(main())`
inside `main()`. For web apps, your framework's async lifespan
(FastAPI, Starlette, Quart, Django async views).

## Class-based `Task` with declarative `TaskInfo`

**Decision.** Tasks subclass `swarmq.Task` and declare their identity
through a class-level `task_info = TaskInfo(...)` field. Registration
is automatic on class definition.

**Alternatives considered.** Decorated async functions (`@app.task`),
the shape Celery and arq use.

**Why classes.** Task metadata — queue routing, retry policy, rate
limit, lock key, timeouts, cron expression, dedup TTL — is a lot of
declarative config to thread through a decorator. Putting it on a
dataclass keeps tasks inspectable: `SendEmail.task_info.max_retries`
is a plain attribute read. It also makes `swarmq inspect --module
myapp.tasks` trivial: walk the registered classes, print their
`task_info`.

**What this costs you.** Slightly more ceremony per task (three lines
of class definition instead of one decorator). In exchange, every
task is a stable type that your IDE, type checker, and tests can
reason about.

## JSON on the wire, msgpack at rest

**Decision.** Task messages are `msgspec`-encoded JSON. Task results
are `msgpack`-encoded. No support for `pickle`, protobuf, or Avro.

**Alternatives considered.** `pickle` (Celery's default, rejected
outright — it deserializes arbitrary classes, every deserialization
is a potential RCE). Protobuf (schema registry overhead for a
single-language library). JSON for both (results can include
datetimes, Decimals, bytes — msgpack handles those naturally).

**Why this split.** JSON-on-the-wire means a message is inspectable
without tooling — `rabbitmqadmin get` or `swarmq dlq inspect` shows
you a readable payload. msgpack-at-rest keeps the native Python
types your task returned intact without you serializing by hand.
Both formats are fast, safe, and deterministic.

**What this costs you.** Task arguments must be JSON-compatible.
Anything exotic (NumPy arrays, custom classes) has to be converted
before you schedule. This is a feature, not a bug: it forces your
task contract to be language-neutral.

## Separate backend for results

**Decision.** Results live in Valkey, not in RabbitMQ. The broker's
only job is carrying task messages.

**Alternatives considered.** Publishing results to an RPC reply
queue (Celery's default mode with `CELERY_TASK_IGNORE_RESULT=False`).
A results-only queue with correlation-id matching.

**Why a separate backend.** RabbitMQ queues are poor key-value
stores: consuming a result mutates state, results have no TTL, and
asking "what was the result of task X" requires searching. Valkey
gives you `GET sq:result:{task_id}` with O(1) lookup and a TTL
applied on write. It is also where the rate limiter, lock manager,
and scheduler already live — one connection pool, not two.

**What this costs you.** One more service to run. SwarmQ ships a
`docker-compose.yml` that co-locates both for development.

## No Celery compatibility shims

**Decision.** SwarmQ does not expose a migration layer for Celery
apps. Your tasks are not plug-compatible with `@celery.task`.

**Alternatives considered.** A `swarmq.compat.celery` module that
maps Celery's API onto SwarmQ's internals so a migrating codebase
can keep its existing `@app.task(bind=True)` decorators.

**Why no shim.** Celery's public surface is enormous — request
context, eager mode, routers, the Canvas DSL, custom serializers,
custom result backends, signal names, exception classes. Covering
any meaningful subset leaves users surprised by what is and is not
supported, and the shim code is larger than SwarmQ itself. A clean
port that writes idiomatic SwarmQ is both simpler and produces code
that ages better.

**What this costs you.** The initial port is real work: rewrite
`@app.task` as `class Task(swarmq.Task)` and adapt the task
signature. What you get back is a smaller, more predictable library.

## One queue type by default: RabbitMQ quorum queues

**Decision.** Queues are declared as
[quorum queues](https://www.rabbitmq.com/quorum-queues.html) unless
`queue_type="classic"` is set.

**Alternatives considered.** Classic mirrored queues (deprecated
upstream). Streams (append-log semantics, wrong shape for tasks).

**Why quorum.** Raft-based replication with deterministic failover,
native `x-delivery-limit` support, native DLX handling. No mirrored-
queue quirks, no mirror-policy tuning, no split-brain corner cases.
For a workload the size of SwarmQ's target deployments, quorum
queues are the default RabbitMQ itself recommends.

**What this costs you.** Very slightly higher per-message overhead
than a classic queue on a single node. In exchange, survival of a
node loss is built-in.

## Signals as in-process pub/sub, not a broker

**Decision.** SwarmQ's `Signal` system is in-process: registered
handlers are awaited in the worker (or client) that emitted the
event.

**Alternatives considered.** Publishing lifecycle events to a
broker topic so multiple services could subscribe.

**Why in-process.** Signals in this library cover operational
concerns — metrics, logging, tracing, Sentry integration. Those
consumers live inside the worker anyway, so a network hop is pure
cost. A broker-published event system is a separate concern that
application code can build on top using its own topic.

**What this costs you.** Nothing at the signal boundary. If you want
cross-process events, you `schedule()` a follow-up task from your
handler — which is the shape you wanted anyway.

## Retries go through a delay queue, never `requeue=True`

**Decision.** Every retry publishes a fresh message to a delay queue
and lets RabbitMQ wait before redelivering. SwarmQ never calls
`basic.nack(requeue=True)` on the primary retry path.

**Alternatives considered.** In-place requeue with client-side
sleep before ack. Retrying inside the `run()` call.

**Why the delay queue.** `requeue=True` delivers immediately to any
available consumer, which hot-loops the broker and burns the retry
budget in milliseconds. A delay queue gives you exponential /
linear / fixed backoff for free and keeps the worker's prefetch
slots open for other work.

**What this costs you.** Slightly more AMQP traffic per retry (one
extra publish + one DLX route). In exchange, retry storms stop
existing.

## `--module` is mandatory

**Decision.** Every `swarmq worker` invocation requires
`--module package.name`. There is no auto-discovery of tasks in the
working directory.

**Alternatives considered.** Scanning `sys.path`, reading a config
file, or adopting `celery -A` auto-magic.

**Why explicit.** The module import is where your task registry is
built. Making it explicit means:

- CI and container images declare it once, in one place.
- A typo surfaces as an `ImportError` at process start, not as an
  empty registry three hours in.
- `swarmq inspect --module ...` re-uses the same argument.

**What this costs you.** One flag on the command line. Worth it.

## YAGNI on things that look obvious

A few features that sound reasonable are deliberately not in:

- **Workflow primitives** (`chain`, `group`, `chord`). Celery's
  Canvas DSL is powerful but rarely used well; application-level
  orchestration with `await app.run(...)` inside a parent task is
  more debuggable.
- **Multi-broker routing.** One RabbitMQ per SwarmQ app. If you need
  isolation, run two apps.
- **Database-as-broker / DB-as-backend.** Postgres queueing is a
  fine tool for small workloads; at SwarmQ's target scale, the
  RabbitMQ + Valkey pair is a better fit, and supporting both would
  double the maintenance surface.
- **Built-in web dashboard.** Prometheus + your existing dashboards
  (Grafana, Datadog, whatever is already running) covers this
  properly. See [`swarmq[metrics]`](../reference/cli.md#swarmq-worker)
  for the Prometheus exporter.

If one of these becomes a real blocker for real users, the decision
can be revisited. Until then, the carrying cost of a feature that is
"probably useful someday" is higher than the cost of saying no.

## See also

- [Architecture](architecture.md) — how the three-role topology
  implements these decisions
- [Delivery semantics](delivery-semantics.md) — the at-least-once
  contract the design rests on
