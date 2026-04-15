# Dramatiq Issue Intelligence: Lessons for SwarmQ

**Sources:** Bogdanp/dramatiq (38 open, ~100 closed), Bogdanp/django_dramatiq (16 open, ~50 closed)
**Analysis date:** 2026-04-15
**Focus:** Reliability patterns, broker integration pain, worker management, threading model consequences

---

## Executive Summary

Dramatiq is a mature, reliability-first task queue. Its issue tracker reveals recurring pain in
specific areas that SwarmQ should learn from. The most significant lessons cluster around:
threading model consequences (deadlocks, GIL contention, interrupt delivery), Redis broker
visibility timeout causing phantom re-enqueues, RabbitMQ connection/channel management overhead,
and the difficulty of composing workflows (pipelines/groups). The async story is notably weak --
a clear gap SwarmQ can fill by being async-native from day one.

---

## Theme 1: Threading Model and Interrupt Delivery Hazards

**Severity: Critical | Issues: 6+ open/closed | Recurrence: High**

Dramatiq uses threads for worker concurrency. This design choice has cascading consequences
that show up repeatedly in the issue tracker.

### Key Issues

- **#845 (open)** -- `PyThreadState_SetAsyncExc` causes deadlocks in downstream libraries.
  Dramatiq uses this CPython-internal API to inject `TimeLimitExceeded` exceptions into worker
  threads. This can corrupt lock state in libraries like loguru, causing sporadic deadlocks.

- **#445 (open, bug)** -- Uncaught `TimeLimitExceeded` exception leads to worker thread death.
  When a thread is in a syscall and `TimeLimitExceeded` is raised asynchronously, the exception
  can be swallowed or mishandled, leaving the thread permanently stuck. Over time this silently
  reduces worker capacity until the process is functionally dead.

- **#791 (closed, bug)** -- Raising `TimeoutError` in an async actor causes infinite loop and
  OOM. The event loop polling code cannot distinguish between `concurrent.futures.TimeoutError`
  (still running) and `TimeoutError` (task failed). This turns into a spin loop with no sleep,
  hitting 100% CPU and OOM within a minute.

- **#578 (open)** -- Workers hang with no logs. Likely related to thread death from unhandled
  exceptions in the interrupt delivery mechanism.

- **#788 (closed)** -- Child worker processes hang when main process exits abruptly. Signal
  handling in the process/thread hierarchy is fragile.

### Lesson for SwarmQ

**Do not use `PyThreadState_SetAsyncExc` for task cancellation.** This API is inherently unsafe:
- It cannot interrupt syscalls or C extensions
- It can corrupt lock state in any library that holds Python-level locks
- It creates a class of bugs that are sporadic and nearly impossible to reproduce

**SwarmQ approach:** Use `asyncio.Task.cancel()` which delivers `CancelledError` at the next
`await` point. This is cooperative, predictable, and cannot corrupt lock state. For CPU-bound
tasks running in process pools, use process termination (SIGTERM/SIGKILL) rather than thread
interruption.

**Also:** The async `TimeoutError` confusion (#791) is a direct consequence of bridging sync
threads with async code. SwarmQ being async-native avoids this entire class of bugs.

---

## Theme 2: Redis Broker -- Visibility Timeout and Re-enqueue Problems

**Severity: High | Issues: 5+ open/closed | Recurrence: High**

The Redis broker uses a visibility timeout pattern: messages become invisible when claimed,
and re-appear if not acknowledged within the timeout. This is the #1 source of "phantom
duplicate execution" reports.

### Key Issues

- **#692 (open, redis)** -- Long-running CPU-intensive task re-enqueued without any worker
  failure or error. The gurobi optimization job takes longer than the visibility timeout,
  so Redis re-enqueues it while it is still running. User sees duplicate execution with no
  error logs explaining why.

- **#691 (open, redis)** -- Heartbeat polling causes hundreds of Redis operations per second.
  On hosted Redis (e.g., AWS ElastiCache), the heartbeat mechanism burns through operation
  quotas. User consumed 500K operations in 2 days from heartbeats alone.

- **#431 (open)** -- Delayed messages duplicated on the work queue, leading to one message
  being processed by many worker threads. Race condition in the delay queue mechanism.

- **#682 (open, redis)** -- Lua scripts use `KEYS` command, which Redis documentation
  explicitly warns against in production. Should use `SCAN` instead.

- **#754 (open, wontdo)** -- Redis broker incompatible with AWS ElastiCache due to cluster
  mode limitations. The broker uses commands that are not available in Redis Cluster mode.

- **#628 (closed)** -- `ReadOnlyError` when Redis fails over to a read-only replica.
  No automatic reconnection to the new primary.

### Lesson for SwarmQ

**Visibility timeout must be dynamically extended, not static.** The pattern should be:
1. Claim message with initial visibility timeout (e.g., 30s)
2. Worker sends periodic heartbeat to extend the timeout while processing
3. If heartbeat stops, message becomes visible again after timeout

This is what SQS calls "visibility timeout extension" and what Dramatiq's heartbeat system
tries to do -- but the implementation has too many edge cases.

**SwarmQ approach:**
- Use Valkey streams with consumer groups (XREADGROUP/XACK). Messages stay in a "pending"
  state until explicitly ACKed. No visibility timeout timer to race against.
- For the heartbeat cost problem (#691), use a single heartbeat per worker process, not per
  message. Batch heartbeat updates.
- For ElastiCache compatibility (#754), avoid `KEYS` commands and ensure all Lua scripts
  use hash tags to guarantee key locality in cluster mode.

---

## Theme 3: RabbitMQ Connection and Channel Management

**Severity: High | Issues: 5+ open/closed | Recurrence: Medium**

RabbitMQ integration has persistent issues around connection lifecycle, heartbeat management,
and connection count scaling.

### Key Issues

- **#649 (open, rabbitmq)** -- Dramatiq uses far more RabbitMQ connections than necessary.
  Creates one connection per worker process per queue. With many queues, this rapidly exhausts
  the connection limit (500 in their case). Best practice is to use one connection with
  multiple channels.

- **#827 (open, enhancement, rabbitmq)** -- RabbitMQ `consumer_timeout` (default 30 min)
  closes connections for unacknowledged messages. Long-running tasks trigger this, causing
  reprocessing loops. The `TimeLimit` middleware default is higher than the RabbitMQ timeout,
  creating a guaranteed failure mode.

- **#832 (closed, bug, rabbitmq)** -- Missing queue re-declaration does not work. When
  RabbitMQ deletes a queue (e.g., after connection drop), the consumer fails to re-declare
  it, leading to permanent `NOT_FOUND` errors.

- **#794 (open, bug)** -- Race condition creating queues vs consuming from them. On first
  startup, consumers try to consume before queues are declared, causing error spam.

- **#558 (open, rabbitmq)** -- `OSError: Address already in use` when calling send function.
  Connection pooling or reuse issue.

- **#145 (closed, django_dramatiq, duplicate)** -- Heartbeat missed errors with RabbitMQ.
  The worker does not send AMQP heartbeats during long-running tasks, causing RabbitMQ to
  close the connection.

### Lesson for SwarmQ

**If SwarmQ ever supports RabbitMQ (or any AMQP broker):**
- Use one connection per process with multiplexed channels, not one connection per queue
- Separate the AMQP heartbeat thread from task processing -- heartbeats must continue
  even during long-running tasks
- Queue declaration must be idempotent and retried on consumer restart
- Align task time limits with broker-side consumer timeouts -- document the interaction

**SwarmQ approach (Valkey-first):** By using Valkey streams, SwarmQ avoids AMQP connection
management entirely. But the lesson about separating heartbeat from task execution applies:
the worker health check mechanism must not be blocked by task execution.

---

## Theme 4: Async Compatibility Gap

**Severity: High | Issues: 4+ open/closed | Recurrence: Growing**

Dramatiq's thread-based model creates friction for users running async code. The issues show
a growing demand for native async support.

### Key Issues

- **#791 (closed, bug)** -- `TimeoutError` in async task causes infinite loop and process
  death. The sync-to-async bridge has a fundamental flaw in error handling (see Theme 1).

- **#586 (closed)** -- `CurrentMessage` does not work with `AsyncIO` middleware. The
  thread-local storage mechanism for tracking the current message is incompatible with
  async context.

- **#643 (closed)** -- Error when running async actor with `AsyncIO` middleware. General
  fragility of the async bridge.

- **#595 (closed)** -- Documentation for asyncio support is sparse. Community demand for
  better async story.

- **#671 (open, documentation)** -- Users coming from Celery want async task polling
  patterns. Shows migration demand.

- **django_dramatiq #190 (open)** -- `AdminMiddleware` fails when sending tasks from an
  async context. The middleware assumes synchronous execution.

### Lesson for SwarmQ

**This is SwarmQ's biggest competitive advantage.** Dramatiq bolted async support onto a
thread-based architecture, and it shows. Every async-related issue stems from the impedance
mismatch between threads and coroutines.

**SwarmQ approach:**
- Native async from day one -- no sync-to-async bridge needed
- Use `contextvars` (not thread-locals) for message context propagation
- Task cancellation via `asyncio.Task.cancel()` is clean and predictable
- Support sync tasks by running them in a thread pool executor, not the other way around

---

## Theme 5: Prometheus / Observability Middleware Fragility

**Severity: Medium | Issues: 5 open | Recurrence: High**

The Prometheus middleware is one of the most bug-reported components, suggesting that
observability as an afterthought (via middleware) is fragile.

### Key Issues

- **#703 (open, prometheus)** -- Prometheus middleware fails with `AttributeError` because
  `after_process_boot` is never called. The hook name was wrong -- it should be
  `after_worker_boot`. Key metrics are never initialized.

- **#701 (open, prometheus)** -- Cannot start Prometheus exposition server in production
  containers. Works locally but not in prod -- likely a multiprocess/fork issue.

- **#672 (open, prometheus)** -- `delayed_messages_inprogress` gauge never decreases.
  The middleware listens to the wrong lifecycle events, so completed delayed messages
  are never decremented.

- **#474 (open, prometheus)** -- Prometheus endpoint stops responding after random time.
  Workers continue processing but metrics are dead. No recovery mechanism.

- **#529 (open, prometheus)** -- StubBroker includes Prometheus middleware in defaults,
  breaking unit tests that do not have prometheus-client installed.

- **#576 (closed)** -- Inconsistent `/tmp` path for Prometheus multiprocess directory.

### Lesson for SwarmQ

**Observability should be a first-class concern, not a middleware.** Dramatiq's approach of
using a middleware that hooks into lifecycle events creates multiple failure modes:
- Wrong hook names go undetected until runtime
- Multiprocess metric aggregation is complex and fragile
- The exposition server can die independently of the worker

**SwarmQ approach:**
- Emit metrics from the core worker loop, not from middleware hooks
- Use OpenTelemetry as the primary observability API (vendor-neutral)
- Expose metrics via a pull endpoint in the main process, not a forked subprocess
- Test metrics in CI -- assert that metric values change after processing messages

---

## Theme 6: Pipeline and Group Composition Limitations

**Severity: Medium | Issues: 5+ open | Recurrence: Stable (long-standing requests)**

Workflow composition is a persistent pain point. Users want more flexible DAG-like
composition but Dramatiq's pipeline/group model is limited.

### Key Issues

- **#103 (open, enhancement, since 2018!)** -- Cannot create a pipeline of groups. Groups
  can contain pipelines, but pipelines cannot contain groups. This prevents fan-out-then-join
  patterns.

- **#625 (open, enhancement)** -- Pipeline with dynamic children. Users migrating from
  Celery want to build task lists dynamically (map/reduce pattern). No equivalent in Dramatiq.

- **#278 (open, enhancement)** -- `as_completed` iteration over group results. Users want
  to process results as they arrive, not in submission order.

- **#335 (open, enhancement)** -- Pass arguments automatically and individually in pipelines.
  Current pipeline result passing is via tuple indexing, which is fragile.

- **#708 (closed)** -- Unexpected multiple executions in pipelines. The composition system
  has subtle bugs that lead to duplicate execution.

- **#564 (closed)** -- Result retrieval from groups/pipelines only works for the default
  broker. Multi-broker setups break workflow composition.

### Lesson for SwarmQ

**Design workflow composition as a DAG from the start, not as nested linear constructs.**

**SwarmQ approach:**
- Model workflows as directed acyclic graphs with explicit edges
- Support fan-out (one task triggers N tasks) and fan-in (wait for N tasks, then trigger one)
- Dynamic child task creation (map/reduce) should be a core primitive
- Result passing should use explicit parameter names, not positional tuple indexing
- `as_completed` iteration is a natural fit for async iterators

---

## Theme 7: Retry and Backoff Configuration Rigidity

**Severity: Medium | Issues: 3+ open/closed | Recurrence: Medium**

The retry system works well for simple cases but lacks flexibility for real-world retry
policies.

### Key Issues

- **#580 (open, enhancement)** -- Cannot provide a custom function to compute backoff time
  per actor. Users want different retry schedules for different failure modes (e.g., webhook
  retries at 1min, 5min, 15min -- not exponential).

- **#651 (closed, bug)** -- `compute_backoff` always returns a value below `min_backoff`
  for the first retry. The backoff calculation has an off-by-one that makes `min_backoff`
  ineffective on the first retry.

- **#605 (closed)** -- Dramatiq sends more retries than the configured max of 3. The retry
  counting mechanism has edge cases.

- **#469 (open, question)** -- No way to suppress error logging for exceptions that will
  be retried. Every retryable exception triggers Sentry alerts, creating noise.

- **#618 (closed)** -- Retries should ignore `Shutdown` exception from
  `ShutdownNotifications`. When a worker shuts down, in-progress messages get retried
  unnecessarily because the shutdown exception is treated as a task failure.

- **django_dramatiq #819 (closed)** -- Users want separate configuration for exception
  logging vs retry behavior. Currently they are coupled.

### Lesson for SwarmQ

**Retry policy should be fully customizable per actor, not just `max_retries` and
`max_backoff`.**

**SwarmQ approach:**
- Accept a callable `backoff_strategy` parameter on task registration
- Provide built-in strategies: exponential, linear, fixed-schedule, custom
- Separate retry decisions from error logging -- a retried error should log at WARNING,
  not ERROR
- Never retry on worker shutdown (graceful or forced) -- re-enqueue the message instead
- Distinguish between "task failed, retry" and "worker died, re-deliver"

---

## Theme 8: Broker Initialization and Configuration Footguns

**Severity: Medium | Issues: 5+ across both repos | Recurrence: Medium**

Setting up Dramatiq correctly -- especially with Django -- is surprisingly error-prone.
The broker-actor coupling creates initialization ordering issues.

### Key Issues

- **#324 (open, enhancement)** -- Feature request to decouple task registry from broker.
  Currently actors cannot be declared without a broker instance, creating import-order
  dependencies.

- **django_dramatiq #133 (closed)** -- `DjangoDramatiqConfig.ready()` called multiple times,
  overwriting the broker and losing registered actors.

- **django_dramatiq #100 (closed)** -- Subclassing `DjangoDramatiqConfig` fails because
  initialization happens at module import time, before the subclass is loaded.

- **django_dramatiq #157 (closed)** -- Cannot pass parameters to middleware via Django
  settings. The configuration format does not support constructor arguments.

- **django_dramatiq #164 (closed)** -- Dramatiq fails to boot when Django `LOGGING` is
  configured. Configuration interaction between Django logging and Dramatiq startup.

- **django_dramatiq #86 (open)** -- Database connection leaks. Middleware ordering
  (DbConnectionsMiddleware must be above AdminMiddleware) is not documented clearly.

### Lesson for SwarmQ

**Broker and task registry must be decoupled.** The Dramatiq pattern of `@dramatiq.actor`
requiring a global broker at import time is a well-known pain point.

**SwarmQ approach:**
- Tasks are registered in a global registry without requiring a broker
- Broker binding happens at startup time, not import time
- Configuration is validated at startup with clear error messages
- Middleware ordering constraints should be enforced programmatically, not by documentation
- Django integration (if any) should use Django's `AppConfig.ready()` correctly with
  idempotent initialization

---

## Theme 9: Security -- Pickle and Message Injection

**Severity: Critical (if applicable) | Issues: 1 open | Recurrence: N/A (new)**

### Key Issue

- **#848 (open, 2026-04-11)** -- `PickleEncoder` uses unrestricted `pickle.loads` on broker
  messages. Any entity with write access to the message broker can achieve arbitrary code
  execution on worker hosts by injecting crafted pickle payloads.

### Lesson for SwarmQ

**Never support pickle serialization.** Use JSON as the default and only encoder.
If users need to pass complex objects, they should serialize them explicitly or use
a reference pattern (pass an ID, load from database in the task).

**SwarmQ approach:**
- JSON-only message encoding, no pickle option
- Validate message schema before execution
- Document that the broker is a trusted transport -- but minimize the blast radius anyway

---

## Theme 10: Worker Process Management and Graceful Shutdown

**Severity: Medium | Issues: 4+ open/closed | Recurrence: Medium**

### Key Issues

- **#788 (closed)** -- Child worker processes hang when main process exits abruptly. Orphaned
  workers continue running but cannot receive new work or be managed.

- **#693 (closed)** -- Master process fails to exit after worker process termination. The
  shutdown coordination between master and workers has edge cases.

- **#601 (open, bug)** -- Leaked semaphore warnings in `--watch` mode. Resource cleanup
  during worker restart is incomplete, suggesting multiprocessing resource management issues.

- **#666 (open)** -- `Bad file descriptor` errors with playwright. Dramatiq closes
  stdin/stdout/stderr file descriptors in worker processes, breaking libraries that need them.

- **#686 (open, enhancement)** -- Request to not close stdin/stdout/stderr, specifically
  for debugging with PDB/IPython.

### Lesson for SwarmQ

**Worker lifecycle management must be bulletproof.**

**SwarmQ approach:**
- Use structured shutdown: SIGTERM -> grace period -> SIGKILL, with explicit state tracking
- Never close standard file descriptors -- redirect them instead if needed
- Worker processes should have a "watchdog" that detects orphaned state and self-terminates
- Resource cleanup (semaphores, shared memory) must be explicit, not left to Python's
  resource tracker

---

## Theme 11: Django Integration Pain Points (from django_dramatiq)

**Severity: Medium | Issues: 16 open in django_dramatiq | Recurrence: Stable**

### Key Issues

- **#154 (open, django_dramatiq)** -- Race condition in `AdminMiddleware`. Task status
  updates happen out of order: `after_enqueue` (sets ENQUEUED) runs after `after_process`
  (sets DONE), leaving tasks permanently in ENQUEUED status.

- **#106 (open, django_dramatiq)** -- `InterfaceError: connection already closed`. Database
  connections become stale during long-running tasks, and the middleware does not handle
  reconnection.

- **#105 (open, django_dramatiq)** -- Duplicate key violation in task table. Multiple workers
  processing the same message (due to re-delivery) cause primary key conflicts.

- **#86 (open, django_dramatiq)** -- Database connection leak. Each periodic task opens a new
  connection that is never closed, eventually exhausting the connection pool.

- **#75 (open, django_dramatiq)** -- Running two queues with different process/thread
  configurations causes task ID collisions at the database level.

### Lesson for SwarmQ

**If SwarmQ provides a Django integration:**
- Task status tracking must handle out-of-order updates (use state machines with valid
  transitions, not blind overwrites)
- Database connection lifecycle must be managed per-task, not per-thread
- Task IDs must be globally unique regardless of queue or worker configuration
- Consider whether admin/tracking middleware belongs in the task queue at all, or whether
  it should be an external observer

---

## Cross-Cutting Lessons Summary

| Area | Dramatiq Pain | SwarmQ Design Response |
|------|---------------|----------------------|
| Concurrency model | Threads + `PyThreadState_SetAsyncExc` | Native async + `Task.cancel()` |
| Message visibility | Static timeout -> phantom duplicates | Stream consumer groups + explicit ACK |
| Heartbeat cost | Per-message heartbeat burns Redis ops | Per-worker heartbeat, batched |
| Connection management | 1 conn per queue per process | Valkey connection pool, no AMQP |
| Async support | Bolted-on bridge, buggy | Native from day one |
| Observability | Middleware hooks, fragile | Core instrumentation, OpenTelemetry |
| Workflow composition | Pipeline + Group, no DAG | DAG-first with fan-out/fan-in |
| Retry policy | Rigid exponential-only config | Pluggable strategy per task |
| Serialization | Pickle option = RCE risk | JSON-only, no pickle |
| Broker coupling | Actors require broker at import | Decoupled registry + late binding |
| Shutdown | Fragile process/thread coordination | Structured shutdown with watchdog |
| Error logging | Retried errors logged as ERROR | Retried = WARNING, final failure = ERROR |

---

## Notable Closed Issues with Recurrence Signal

These issues were fixed but the underlying patterns keep resurfacing:

1. **Queue re-declaration after connection drop** (#832 closed, #794 open) -- RabbitMQ
   queue lifecycle management is fragile. Same class of bug fixed and reopened.

2. **Backoff calculation bugs** (#651 closed, #580 open requesting redesign) -- The retry
   system's edge cases keep surfacing.

3. **Worker hanging/thread death** (#445 open, #788 closed, #578 open) -- The threading
   model's fundamental fragility keeps manifesting in different ways.

4. **Database connection management in Django** (#86 open, #76 closed, #106 open) --
   Connection lifecycle in a threaded worker is inherently tricky.

---

## Quantitative Summary

- **Total issues analyzed:** 38 open (dramatiq) + 16 open (django_dramatiq) + ~150 closed
- **Oldest open issue:** #103 (2018-07-22) -- pipeline of groups, 8 years unresolved
- **Most impactful themes by issue count:**
  1. Threading/interrupt hazards: ~6 issues, critical severity
  2. Redis visibility/heartbeat: ~5 issues, high severity
  3. RabbitMQ connections: ~5 issues, high severity
  4. Prometheus/observability: 5 open issues, medium severity
  5. Pipeline/group composition: 5+ issues, medium severity (longest-standing)
- **Enhancement vs bug ratio (open):** ~15 enhancements, ~8 bugs, ~15 questions/other
- **Active maintenance:** Yes -- recent commits, v2.0 released, issues being triaged
