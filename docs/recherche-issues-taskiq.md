# TaskIQ Issue Intelligence: Lessons for SwarmQ

**Date:** 2026-04-15
**Sources:** taskiq-python/taskiq (65 open, 100 closed), taskiq-python/taskiq-redis (20 open, 35 closed), taskiq-python/taskiq-aio-pika (4 open, 12 closed), taskiq-python/taskiq-dependencies (2 open, 2 closed)
**Method:** Full GitHub issue analysis across the TaskIQ ecosystem

---

## Executive Summary

TaskIQ's issue tracker reveals a mature async-first task queue with **systemic pain in 10 recurring areas**. The most critical lesson for SwarmQ: TaskIQ's biggest architectural gap -- the lack of message rejection/nack -- has cascaded into dozens of downstream issues (poison messages, missing DLQ support, no error-driven requeue). This single omission is the root cause of at least 15-20 issues across repos.

---

## Theme 1: Missing Nack/Reject -- The Architectural Gap

**Impact: CRITICAL | Issues: ~15 across repos | Recurring since 2024**

### The Problem

TaskIQ's receiver always ACKs messages, even on task failure. There is no `message.reject()` or `message.nack()` in the core abstraction. This is documented explicitly in:

- **taskiq#322** -- "Add messages rejection": User rewrote the Receiver to add `reject()`, notes that `taskiq-aio-pika` creates a DLQ but it is "essentially always empty" because reject is never called.
- **taskiq#578** -- "How to properly implement dead_letter logic": User tried `ack_on_error=False`, custom middleware, manual nack -- nothing works. The receiver always ACKs.
- **taskiq#569** -- "Receiver does not ack/nack Tasks that can't be found": Unknown tasks get ACKed, leaving them permanently pending.
- **taskiq-aio-pika#35** -- "Crash Loop Due To Poison Message Without Quorum Queues": OOM-killed consumer can't ACK, message gets redelivered forever. Without nack+DLQ, there's no way to break the cycle.
- **taskiq-aio-pika#46** -- "Prefetched messages don't fail gracefully on RabbitMQ Connection errors": Network disruption causes `ChannelInvalidStateError` on prefetched messages that can't be properly handled.

### Downstream Effects

Without nack, users cannot implement:
- Dead letter queues (messages that fail N times go to a DLQ)
- Poison message detection (bad messages get retried forever)
- Selective requeue with backoff
- Error-driven routing (failed messages to different queue)

### Lesson for SwarmQ

**SwarmQ MUST have nack/reject as a first-class primitive in the broker abstraction.** The `AckableMessage` pattern should be `AckableRejectableMessage` from day one. The broker interface needs:
```python
class BrokerMessage:
    async def ack(self) -> None: ...
    async def nack(self, requeue: bool = False) -> None: ...
    async def reject(self) -> None: ...  # send to DLQ
```

The receiver must NOT auto-ack on error. Default behavior should be configurable: `ack_on_error=False` should actually work.

---

## Theme 2: Worker Lifecycle and Signal Handling

**Impact: HIGH | Issues: ~12 | Recurring, with fixes that don't stick**

### The Problem

Worker shutdown, signal handling, and process management are a persistent source of bugs:

- **taskiq#593** -- "Worker hangs when using mise": Non-deterministic hangs due to signal handling issues. Author documented multiple problems with how signals are handled in the worker implementation.
- **taskiq#63** -- "Worker hangs sometimes" (open since 2023!): Worker doesn't react to SIGTERM. This is the oldest open bug.
- **taskiq#372** -- "Unhandled KeyboardInterruptError from interrupt_handler": SIGINT/SIGTERM in subprocess workers causes unhandled exceptions.
- **taskiq#380** -- "Graceful Shutdown Issue with Taskiq in K8s": `shutdown-timeout` and `wait-tasks-timeout` don't work as expected because the receiver continues accepting new tasks after SIGTERM.
- **taskiq#325** -- "Graceful shutdown and reload": KeyboardInterrupt cancels unfinished tasks instead of waiting.
- **taskiq#461** -- "Scheduler SIGTERM Handler": Scheduler has no SIGTERM handler at all, breaking Kubernetes deployments where scheduler runs as PID 1.
- **taskiq#470** -- "Bug: worker thrashing on task startup": Logic in `process_manager.py` appears backwards -- waiting when task is alive instead of when dead.
- **taskiq#424** -- "Shutdown middleware not called in some cases".
- **taskiq#422** -- Windows `PermissionError` on worker stop.

### Closed but recurring:
- **taskiq#591** (closed 2026-03) -- FileWatcher .gitignore in wrong directory, fixed.
- **taskiq#565** (open) -- Endless reload loop from `closed_no_write` events.
- **taskiq#574** (open) -- Stuck in reload loop with `--reload-dir`.

### Lesson for SwarmQ

**Worker lifecycle needs to be designed as a state machine with explicit transitions:**
1. STARTING -> READY -> DRAINING -> SHUTTING_DOWN -> STOPPED
2. Signal handlers must be registered for SIGTERM, SIGINT, and SIGHUP on all platforms.
3. On SIGTERM: stop accepting new messages, wait for in-flight tasks (with configurable timeout), then exit.
4. The scheduler must handle SIGTERM independently (Kubernetes sends SIGTERM to PID 1).
5. File watcher reload needs debouncing and proper event filtering (not just "opened"/"closed").
6. Windows compatibility: don't use Unix-only signals (SIGHUP) without platform guards.

---

## Theme 3: Serialization and Type Handling

**Impact: HIGH | Issues: ~12 | The #1 user confusion area**

### The Problem

TaskIQ uses Pydantic for message serialization, which creates a cascade of issues when types don't round-trip cleanly:

- **taskiq#457** -- "Pydantic model with bytes cannot be sent to worker": `model_dump(mode='json')` fails on binary data with UnicodeDecodeError.
- **taskiq#441** -- "No way of sending a task with arbitrary types": PickleSerializer doesn't work because the formatter still validates through Pydantic first. Custom PickleFormatter also fails because the receiver hardcodes JSON deserialization.
- **taskiq#587** -- "Unable to serialize unknown type AsyncTaskiqTask": Result backend fails to serialize task results that contain internal types.
- **taskiq#456** -- "Exception Serialization": Exceptions initialized with kwargs lose their kwargs during serialization, breaking `exception_to_python()`.
- **taskiq#605** -- CronSpec with `datetime.timezone` offset serializes to invalid string "PT2H".
- **taskiq#484** (closed) -- Pydantic models with aliased fields don't deserialize correctly.
- **taskiq#558** (closed) -- Result backend returns dict instead of Pydantic model.
- **taskiq#419** (closed) -- Taskiq fails with Pydantic v1.
- **taskiq-redis#75** -- "Incorrect return_value": After v1.0 upgrade, Pydantic models returned as dict.
- **taskiq-redis#56** (closed) -- Custom serializers needed for result backend.

### The Pattern

The serialization pipeline is too tightly coupled to Pydantic:
1. Formatter uses `model_dump(mode='json')` -- fails on bytes, custom types
2. Receiver hardcodes JSON deserialization -- can't use alternative serializers end-to-end
3. Result backend has its own serialization -- inconsistent with message serialization
4. Exception serialization is ad-hoc and lossy

### Lesson for SwarmQ

**Serialization must be a pluggable, end-to-end pipeline:**
1. Default to msgpack or similar binary format (not JSON) to handle bytes natively.
2. The serializer interface must cover the full path: task args -> broker message -> result backend -> caller.
3. Never hardcode Pydantic anywhere in the pipeline. Support it as an optional layer.
4. Exception serialization needs special handling: preserve class, args, AND kwargs.
5. Provide a `PickleSerializer` that actually works end-to-end (for trusted environments).
6. Feature request #603 asks for first-class msgspec support -- this is a real demand signal.

---

## Theme 4: Redis Broker Reliability

**Impact: HIGH | Issues: ~14 (across taskiq-redis) | Production-critical**

### The Problem

Redis Stream and List broker implementations have several reliability issues:

- **taskiq-redis#123** -- "Race condition in RedisStreamBroker.listen": The autoclaim lock check is not atomic (`if await lock.locked()` then `async with lock`), allowing duplicate task execution.
- **taskiq-redis#84** (closed) -- "All items in a queue assigned to one worker": Task distribution breaks under load, one worker gets all tasks.
- **taskiq-redis#91** (closed) -- "Unfair Allocation of Tasks With RedisStreamBroker": Related to #84, workers don't get balanced work.
- **taskiq-redis#104** -- "Occasional endless blocking": Worker shows no activity despite tasks being posted. Requires manual restart. Azure Redis makes it worse.
- **taskiq-redis#101** -- "Disable automatic redelivery for long-running tasks": Tasks running >10min get autoclaimed and re-executed. No way to extend the timeout from within the task.
- **taskiq-redis#87** -- "Allow setting of autoclaim timeout": Worker shutdown leaves lock stuck for entire queue.
- **taskiq-redis#86** -- "Ability to get stream id of a task": Need stream ID to reclaim messages during long tasks.
- **taskiq-redis#65** (closed) -- "Worker Crashes on Connection Disruption": Redis restart kills all in-flight tasks with no recovery.
- **taskiq-redis#70** -- "Connection error when kiq new task from within a task": Sending a new task from inside a running task causes connection errors.
- **taskiq#488** -- "ConnectionError: Connection closed by server": Redis Sentinel connections drop 5-6 times per day, causing worker restarts.
- **taskiq#480** -- "High CPU When Message Queue Unavailable": No backoff on broker connection failure, CPU goes to 100%.

### The Pattern

Redis Streams bring their own complexity: consumer groups, pending entries, autoclaim. TaskIQ's abstractions don't fully handle the edge cases:
1. Autoclaim is both essential (for failed workers) and dangerous (for long tasks)
2. Connection pooling issues cause cascading failures
3. No reconnection backoff -- connection failure burns CPU
4. Lock-based approaches to prevent duplicate delivery are racy

### Lesson for SwarmQ

**Redis broker design must handle these edge cases from the start:**
1. Use Lua scripts for atomic operations (no Python-level lock+check patterns).
2. Implement exponential backoff on connection failure (taskiq#480 is trivial to prevent).
3. Long-running tasks need a heartbeat mechanism to extend their visibility timeout (like SQS).
4. Stream trimming must be built-in (taskiq-redis#114 -- OOM from untrimmed streams).
5. Connection disruption recovery: reconnect and resume, don't crash.
6. Task distribution fairness: test under load with multiple workers.
7. Provide `socket_timeout` guidance -- many issues stem from timeout mismatch (taskiq-redis#110).

---

## Theme 5: Scheduler Bugs and Limitations

**Impact: MEDIUM-HIGH | Issues: ~10 | Recurring**

### The Problem

The scheduler has fundamental timing and reliability issues:

- **taskiq#296** -- "Scheduler can send task twice": If `datetime.now()` falls on a minute boundary, delay calculation goes negative, causing duplicate execution.
- **taskiq#244** -- "Possible scheduled tasks duplication": Restart during a scheduled minute sends the task twice.
- **taskiq#527** -- "Slow get_schedules() leads to missed jobs": `get_schedules()` takes >2 minutes, causing the "every 1 minute" loop to skip intervals.
- **taskiq-redis#78** -- "Getting schedules is very slow": Full SCAN of 600K Redis keys takes 2-3 minutes.
- **taskiq-redis#122** -- "ListRedisScheduleSource runs full SCAN every update_interval": Same SCAN problem, causes CPU load on Redis.
- **taskiq-redis#89** -- "`self._is_first_run` doesn't work" in ListRedisScheduleSource.
- **taskiq-redis#115** (closed) -- "ListRedisScheduleSource doesn't support schedule_by_interval": Interval schedules stored but never executed.
- **taskiq-redis#105** (closed) -- "Scheduled cron tasks don't execute with ListRedisScheduleSource".
- **taskiq#438** -- "Worker embedded scheduler": Users want celery-like `--beat` flag instead of separate scheduler process.
- **taskiq#328** -- "Run missed task from persistent storage": No mechanism to detect and fire missed schedules after restart.

### Lesson for SwarmQ

**The embedded scheduler approach (requested in #438) is the right default:**
1. Avoid Redis SCAN for schedule discovery -- use sorted sets with score=next_run_time.
2. Scheduler must be leader-elected to prevent duplicate execution.
3. Build in missed-schedule detection: on startup, check if any schedules should have fired during downtime.
4. Sub-minute scheduling should work from day one (taskiq#348, #321).
5. `schedule_by_interval` must be a first-class citizen, not an afterthought.

---

## Theme 6: Middleware and Hook Ordering

**Impact: MEDIUM | Issues: ~8**

### The Problem

Middleware semantics are confusing and sometimes broken:

- **taskiq#505** -- "Middleware order is reversed for exit hooks": `post_execute` and `shutdown` run in the same order as `pre_execute` and `startup`, instead of LIFO. This breaks the common pattern where middleware A sets up context and middleware B uses it.
- **taskiq#586** -- "post_send fires after task execution with InMemoryBroker": `await_inplace=True` breaks the semantic meaning of "post send" -- it fires after task completes, not after message is sent.
- **taskiq#530** -- "Context.requeue doesn't trigger middlewares": Requeue bypasses pre_send/post_send, breaking middleware that tracks all message sends.
- **taskiq#327** -- "How to cancel sending a task using middleware": No documented way to abort a task send from `pre_send`.
- **taskiq#133** -- "Task cancellation from middlewares or dependencies" (open since 2023).
- **taskiq#397** -- "PrometheusMiddleware crashes when tasks split into modules": Import ordering issues.
- **taskiq#504** -- "Prometheus Middleware doesn't work with scheduler".

### Lesson for SwarmQ

**Middleware must have clear, documented execution order:**
1. Pre-hooks: FIFO (A, B, C). Post-hooks: LIFO (C, B, A). This is the standard onion model.
2. InMemoryBroker must have identical semantics to real brokers -- otherwise testing is unreliable.
3. Middleware should be able to abort/cancel a task send by raising a specific exception.
4. Requeue/retry must go through the full middleware pipeline.
5. Document the hook execution order with a diagram.

---

## Theme 7: Multi-Queue and Task Routing

**Impact: MEDIUM | Issues: ~8**

### The Problem

Users struggle with multi-queue setups and routing tasks to specific workers:

- **taskiq-aio-pika#51** -- "Cannot enqueue to another queue from within a worker": A worker listening on queue A can't send a task to queue B.
- **taskiq-aio-pika#49** -- "2 brokers with different queue names receive all tasks": Tasks meant for broker1 also arrive at broker2.
- **taskiq-aio-pika#43** (closed) -- "Tasks not delivered with DIRECT exchange": Routing key mismatch when using ExchangeType.DIRECT.
- **taskiq#181** -- "Task routing to specific worker": No built-in way to send a task to a named worker/queue.
- **taskiq#455** / **taskiq#526** / **taskiq#454** -- "Broker mismatch" log spam when using `AsyncSharedBroker` with schedulers. Three separate issues about the same confusing warning.
- **taskiq#469** -- "Need advice on dynamically switching brokers": No clean API for runtime broker selection.

### Lesson for SwarmQ

**Multi-queue and routing need a clear, first-class API:**
1. A task should be declaratively routable: `@broker.task(queue="heavy")`.
2. Workers should be filterable: `swarmq worker --queues heavy,default`.
3. Cross-queue publishing from within a task must work.
4. Avoid the "shared broker" pattern -- it creates confusion. Use explicit broker references.

---

## Theme 8: Result Backend Usability

**Impact: MEDIUM | Issues: ~8**

### The Problem

- **taskiq#426** (closed) -- "wait_result() doesn't block": No result backend configured, but `wait_result()` silently returns empty result instead of raising.
- **taskiq#427** (closed) -- Enhancement to warn when result backend is unset.
- **taskiq#464** -- "How to use result_backend.set_progress": No documented way to report task progress.
- **taskiq#479** -- "Noisy 'Cannot parse return type' log warning": On task error, result parsing logs unhelpful warnings.
- **taskiq-redis#27** (closed) -- "Change expiration policy": Implicit TTL even when not specified.
- **taskiq-redis#14** (closed) -- "Remove result from Redis after reading": No auto-cleanup.
- **taskiq#282** -- "Future attached to a different loop": Event loop mismatch when polling for results.
- **taskiq#501** -- "Get last executed tasks IDs" for scheduled tasks: No way to correlate scheduled task executions with their results.

### Lesson for SwarmQ

1. If no result backend is configured, `wait_result()` must raise, not silently return None.
2. Progress reporting should be a built-in feature with a clean API.
3. Result TTL should default to "no expiry" with opt-in expiration.
4. Auto-cleanup after read should be an option.
5. Scheduled task results need correlation IDs.

---

## Theme 9: Testing with InMemoryBroker

**Impact: MEDIUM | Issues: ~5**

### The Problem

- **taskiq#586** -- `post_send` fires at wrong time with `await_inplace=True`.
- **taskiq#291** -- InMemoryBroker doesn't call `startup()` on result backend.
- **taskiq#377** -- "Dependency Context Not Propagating to Broker in Task Execution": InMemoryBroker + InmemoryResultBackend don't propagate DI context correctly.
- **taskiq#491** -- "Task cannot generate pydantic-core schema for methods with TaskiqDepends": Using `Context` as dependency in task signatures causes PydanticSchemaGenerationError.
- **taskiq#345** (closed) -- "Calling requeue from task with async generator dependency treats as dependency error".

### Lesson for SwarmQ

**The test broker must be a faithful simulation of the real broker:**
1. InMemoryBroker must call startup/shutdown on all components (result backend, middlewares).
2. Hook ordering must match production behavior.
3. DI context must propagate identically.
4. Consider providing a `TestBroker` that wraps the real broker with an in-memory transport, rather than a separate implementation.

---

## Theme 10: Type Safety and Developer Experience

**Impact: MEDIUM | Issues: ~6**

### The Problem

- **taskiq#577** -- Type checker error: `No overload of bound method 'kiq' matches arguments`.
- **taskiq#400** (closed) -- mypy error: `TaskiqResult has no attribute 'raise_for_result'`.
- **taskiq#278** -- "If label is not str worker fails without traceback": Non-string labels cause silent failures.
- **taskiq#442** -- "Broker kick exception is not specific": `SendTaskError` with no details.
- **taskiq#439** -- "Sending a task by name" (like Celery's `send_task`): No way to decouple producer from consumer code.
- **taskiq#279** -- Documentation shows `with_labels(delay=1)` but this isn't valid.

### Lesson for SwarmQ

1. Type stubs must work with mypy, pyright, AND ty (the new type checker).
2. Error messages must include root cause -- never raise generic `SendTaskError`.
3. Labels/metadata should accept `Any` or be typed as `dict[str, str]` with clear documentation.
4. Support `send_task_by_name()` for decoupled architectures.
5. Documentation examples must be tested -- invalid API calls in docs cause confusion.

---

## Theme 11: Connection Resilience and Performance

**Impact: MEDIUM | Issues: ~6**

### The Problem

- **taskiq#480** -- CPU spins to 100% when broker is unreachable (no backoff).
- **taskiq#488** -- Redis Sentinel connections drop multiple times daily.
- **taskiq-redis#53** (closed) -- `max_connection_pool_size` exceeded kills scheduler with uncaught ConnectionError.
- **taskiq-redis#104** -- Workers go silent on Azure Redis, require manual restart.
- **taskiq#368** -- "DB QueuePool limit exceeded": TaskIQ's concurrency model exhausts database connection pools.
- **taskiq#468** -- Potential conflict between `max-tasks-per-child` and concurrent task processing.

### Lesson for SwarmQ

1. **Mandatory exponential backoff** on all connection retries (broker, result backend).
2. Connection pool exhaustion must be handled gracefully -- not with uncaught exceptions.
3. Worker concurrency controls (`max-async-tasks`, `max-tasks-per-child`) must interact correctly.
4. Health monitoring: workers should report their state (idle, busy, draining).

---

## Theme 12: Documentation and Onboarding Gaps

**Impact: MEDIUM | Issues: ~8**

### Key Confusion Points (derived from issues)

1. **No clear architecture overview**: Users don't understand broker vs worker vs scheduler separation (taskiq#438, #462).
2. **Result backend not explained**: Multiple users don't configure one and then wonder why `wait_result()` doesn't work (taskiq#426).
3. **Shared broker confusion**: 3 separate issues about "broker mismatch" warnings (#455, #526, #454).
4. **Delay/scheduling confusion**: Users expect Celery-like `delay=N` but it doesn't work that way (#279, #347).
5. **Multi-queue setup**: No clear guide for routing tasks to different queues (#181, taskiq-aio-pika#49).
6. **Docker Compose example**: Requested in #420 -- no example for the common Redis+Worker+Scheduler setup.
7. **Deprecated API in docs**: taskiq-redis#109 -- docs show deprecated `RedisScheduleSource`.
8. **Progress tracking**: #464 -- no docs on how to report task progress.

### Lesson for SwarmQ

1. Provide a complete Docker Compose example in the README.
2. Architecture overview diagram showing message flow.
3. "Common patterns" guide: delay, retry, DLQ, progress, multi-queue.
4. Error messages should link to relevant documentation.
5. All code examples in docs must be tested in CI.

---

## Cross-Cutting Observations

### 1. The Ack-Only Design is the Root Cause

At least 15-20 issues across repos trace back to the receiver always ACKing. This single architectural decision created: no DLQ support, no poison message handling, no error-driven routing, and confused users who expect AMQP-standard behavior.

**SwarmQ lesson:** Build nack/reject into the broker protocol from day one. This is non-negotiable.

### 2. Pydantic Coupling Creates Friction

TaskIQ's deep coupling to Pydantic (for message formatting, result serialization, and schema validation) causes issues with: bytes fields, custom types, Pydantic v1 vs v2, and performance (msgspec request in #603).

**SwarmQ lesson:** Use msgpack as the default wire format. Support Pydantic as an optional validation layer, not as the serialization backbone.

### 3. The Process Model is Complex

TaskIQ uses multiprocessing + asyncio per worker, which creates issues with: signal handling across processes, contextvars not propagating to sync tasks, daemon process restrictions, and event loop attachment errors.

**SwarmQ lesson:** Consider a simpler process model initially. One event loop per worker process, with explicit process management. Avoid the multiprocessing+asyncio combination unless absolutely necessary.

### 4. Redis Streams Need Lua Scripts

Multiple race conditions in taskiq-redis stem from non-atomic Python-level operations on Redis Streams (lock check + lock acquire, autoclaim races).

**SwarmQ lesson:** All Redis operations that need atomicity must use Lua scripts. This includes: message claim, ack, nack, visibility timeout extension, and deduplication checks.

### 5. The Scheduler is a Separate Problem

TaskIQ's scheduler has persistent issues with timing, deduplication, and missed tasks. Users want it embedded in the worker (like Celery beat).

**SwarmQ lesson:** Build the scheduler as an embedded component with leader election, not a separate process.

---

## Priority Matrix for SwarmQ

| Priority | Area | Why |
|----------|------|-----|
| P0 | Nack/reject in broker protocol | Root cause of ~15-20 downstream issues |
| P0 | Worker lifecycle state machine | Signal handling bugs are the oldest open issues |
| P0 | Connection resilience + backoff | CPU-burn on broker failure is trivial to prevent |
| P1 | Serialization pipeline | Pluggable, end-to-end, not Pydantic-coupled |
| P1 | Atomic Redis operations (Lua) | Race conditions from non-atomic patterns |
| P1 | Multi-queue routing | Users need this; TaskIQ's approach is confusing |
| P2 | Embedded scheduler with leader election | Avoids entire class of timing/duplication bugs |
| P2 | Middleware execution order (onion model) | Reversed exit hooks break middleware patterns |
| P2 | InMemoryBroker fidelity | Test broker must match production semantics |
| P3 | Type safety (mypy/pyright) | Important for adoption but not a blocker |
| P3 | Documentation | Common confusion points are predictable |

---

## Issue Reference Index

### taskiq-python/taskiq (core)
| # | Title | Theme |
|---|-------|-------|
| 605 | CronSpec timezone serialization | Serialization |
| 603 | First-class msgspec support | Serialization |
| 593 | Worker hangs with mise (signal handling) | Worker Lifecycle |
| 587 | Unable to serialize AsyncTaskiqTask | Serialization |
| 586 | post_send fires after task execution | Middleware |
| 578 | How to implement dead_letter logic | Nack/Reject |
| 574 | Stuck in reload loop | Worker Lifecycle |
| 569 | Receiver doesn't ack/nack unfound tasks | Nack/Reject |
| 565 | Endless reload loop | Worker Lifecycle |
| 556 | Tasks remaining in queue | Worker Lifecycle |
| 544 | Worker doesn't pick up specific tasks | Worker Lifecycle |
| 530 | Context.requeue skips middlewares | Middleware |
| 528 | max-prefetch off by one | Worker Lifecycle |
| 527 | Slow get_schedules misses jobs | Scheduler |
| 524 | ZeroMQ doesn't work on Windows | Worker Lifecycle |
| 505 | Middleware order reversed for exit hooks | Middleware |
| 491 | PydanticSchemaGenerationError with TaskiqDepends | Testing/DI |
| 488 | Redis ConnectionError | Connection Resilience |
| 480 | High CPU when broker unavailable | Connection Resilience |
| 479 | Noisy log warnings on task error | Result Backend |
| 470 | Worker thrashing on startup | Worker Lifecycle |
| 468 | max-tasks-per-child vs concurrent processing | Worker Lifecycle |
| 461 | Scheduler SIGTERM handler missing | Worker Lifecycle |
| 457 | Pydantic model with bytes fails | Serialization |
| 456 | Exception serialization lossy | Serialization |
| 455 | Broker mismatch log spam | Scheduler/Routing |
| 441 | No way to send arbitrary types | Serialization |
| 439 | Send task by name | Developer Experience |
| 438 | Embedded scheduler request | Scheduler |
| 436 | Can't register methods as tasks | Developer Experience |
| 396 | Concurrent limit invalid | Worker Lifecycle |
| 377 | DI context not propagating in tests | Testing |
| 372 | Unhandled KeyboardInterrupt | Worker Lifecycle |
| 368 | DB QueuePool limit exceeded | Connection Resilience |
| 322 | Add messages rejection | Nack/Reject |
| 305 | How to cancel a running task | Developer Experience |
| 301 | with_labels mutates task labels | Developer Experience |
| 296 | Scheduler sends task twice | Scheduler |
| 291 | InMemoryBroker skips result_backend startup | Testing |
| 282 | Future attached to different loop | Async/Event Loop |
| 244 | Scheduled tasks duplication | Scheduler |
| 63 | Worker hangs sometimes | Worker Lifecycle |

### taskiq-python/taskiq-redis
| # | Title | Theme |
|---|-------|-------|
| 123 | Race condition in RedisStreamBroker | Broker Reliability |
| 122 | Full SCAN every update_interval | Scheduler/Performance |
| 104 | Endless blocking on Azure Redis | Connection Resilience |
| 101 | Disable redelivery for long tasks | Broker Reliability |
| 97 | Sentinel broker missing features | Broker Reliability |
| 87 | Autoclaim timeout on shutdown | Broker Reliability |
| 86 | Get stream id of a task | Broker Reliability |
| 78 | get_schedules slow with many keys | Scheduler/Performance |
| 75 | Incorrect return_value after v1.0 | Serialization |
| 72 | Connection closed on worker stop | Connection Resilience |
| 70 | Connection error sending from task | Connection Resilience |

### taskiq-python/taskiq-aio-pika
| # | Title | Theme |
|---|-------|-------|
| 51 | Cannot enqueue to another queue | Routing |
| 49 | Two brokers receive all tasks | Routing |
| 46 | Prefetched messages fail on disconnect | Connection Resilience |
| 35 | Crash loop from poison messages | Nack/Reject |

### taskiq-python/taskiq-dependencies
| # | Title | Theme |
|---|-------|-------|
| 35 | __getattr__ dependencies crash resolver | DI |
| 22 | ArgRef for function argument references | DI |
