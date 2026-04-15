# Celery Issue Intelligence Report for SwarmQ

**Source:** github.com/celery/celery
**Analyzed:** ~200 open issues + ~90 recently closed issues (2024-2026), plus deep-dive into ~50 high-signal older issues (2015-2023)
**Date:** 2026-04-15

This report extracts strategic lessons from Celery's issue tracker to inform SwarmQ's architecture. Issues are clustered into themes representing systemic weaknesses, not individual bugs. Each theme concludes with concrete lessons for SwarmQ.

---

## Table of Contents

1. [Canvas/Workflow Engine Fragility](#1-canvasworkflow-engine-fragility)
2. [Broker Connection Resilience & Reconnection](#2-broker-connection-resilience--reconnection)
3. [Visibility Timeout & Task Redelivery](#3-visibility-timeout--task-redelivery)
4. [Worker Lifecycle & Shutdown](#4-worker-lifecycle--shutdown)
5. [Memory Leaks](#5-memory-leaks)
6. [Missing Native Async/Await Support](#6-missing-native-asyncawait-support)
7. [Celery Beat / Scheduler Problems](#7-celery-beat--scheduler-problems)
8. [Result Backend Fragility](#8-result-backend-fragility)
9. [Task Duplication & At-Least-Once Semantics](#9-task-duplication--at-least-once-semantics)
10. [Serialization & Type System](#10-serialization--type-system)
11. [Configuration Complexity & Documentation Gaps](#11-configuration-complexity--documentation-gaps)
12. [Observability & Debugging Gaps](#12-observability--debugging-gaps)
13. [DLQ / Retry / Error Handling](#13-dlq--retry--error-handling)
14. [Broker-Specific Traps](#14-broker-specific-traps)

---

## 1. Canvas/Workflow Engine Fragility

**Impact: CRITICAL | Issues: 50+ open (oldest from 2016) | Trend: Chronic, no resolution in sight**

This is Celery's single largest category of open bugs. The canvas primitives (chain, group, chord) form a workflow DSL that is deeply broken at scale and under edge conditions.

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 9202 | Exponential growth in function calls for chains beyond 100 tasks | 2024 | **100% CPU/memory** on moderate chains |
| 9674 | Failed task in nested chains/groups causes `chord_unlock` to retry for eternity | 2025 | **Infinite retry loop, resource exhaustion** |
| 5327 | Recursion error on complex chains of chords | 2019 | Stack overflow on deep workflows |
| 5286 | Chain of chords takes all resources and fails | 2019 | OOM on complex workflows |
| 3571 | Nested chords take exponential time | 2016 | **10 years old, still open** |
| 6437 | Celery hangs when exception in group of chains (Redis) | 2020 | Silent workflow halt |
| 6631 | Rest of chain lost after task retry | 2021 | **Silent data loss** |
| 6325 | Replace in nested chains leads to stale tasks | 2020 | Tasks stuck forever |
| 8182 | Chaining a single group won't wait for completion | 2023 | Race condition |
| 8211 | Chord raises ChordError even with error handler defined | 2023 | Error handling broken |
| 5612 | Chord callback not called with multi-level nested chains | 2019 | Silent failure |
| 5230 | Memory leak with chord (reproducible docker-compose example) | 2018 | Memory leak |
| 4925 | Unstable group workflow feature | 2018 | General instability |
| 9772 | `chain(group, group, group)()` throws AttributeError | 2025 | Basic composition broken |
| 9608 | Custom task_id on chain produces incomplete graph | 2025 | Graph integrity failure |
| 5492 | Chain results save to ResultStore instead of broker message | 2019 | Architectural issue |

### Root Cause Analysis

The canvas is built on **recursive signature objects** that compose at the Python level before submission. This means:

1. **O(n^2) or worse complexity** for chain construction -- `maybe_signature()` is called recursively during `.apply_async()`, and each nested level multiplies the call count
2. **State management is distributed** across the broker (messages), result backend (intermediate results), and in-memory worker state -- any desynchronization causes silent failures
3. **Chord completion tracking** uses a counter in the result backend that is decremented as tasks complete. If any task fails to decrement (crash, timeout, connection loss), the chord hangs forever or retries infinitely
4. **No transactional semantics** -- partial workflow execution is possible and common. If a chain breaks midway, there's no cleanup, no rollback, and often no notification
5. **Error propagation is inconsistent** across different primitive combinations. Errors in groups don't always propagate to parent chains. Chord error handlers don't fire in all cases

### Lessons for SwarmQ

1. **Workflows must be DAGs stored as data, not recursive Python objects.** Store the workflow graph in a durable, queryable format (DB or broker metadata). Never build workflows via recursive function calls at submission time.

2. **Workflow execution must be transactional.** If step 3 of 5 fails, the system must know the state of steps 1-2 and be able to clean up or retry from a known state.

3. **Chord/group completion tracking must be crash-safe.** Use atomic operations (Redis MULTI/EXEC or Lua scripts) for completion counters. Design for the case where a worker crashes mid-task and never decrements the counter -- use timeouts or health checks, not just counters.

4. **Set hard limits on workflow depth and breadth.** Celery allows arbitrary nesting which leads to exponential blowup. SwarmQ should enforce configurable limits and fail fast with clear errors.

5. **Test workflows with 100+ tasks, nested 3+ levels deep, with random failures injected.** This is where Celery breaks down, and where most users hit real pain.

6. **Separate workflow orchestration from task execution.** The workflow engine should be a distinct component that manages the DAG, not something bolted onto the task execution path.

---

## 2. Broker Connection Resilience & Reconnection

**Impact: CRITICAL | Issues: 30+ | Trend: Recurring -- same patterns across Redis, RabbitMQ, SQS**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 3773 | Can't ack: BrokenPipeError (94 comments) | 2017 | **Priority: Critical, 94 comments, still open** |
| 10205 | Worker stops consuming Redis tasks after connection reset, no recovery | 2026 | Closed but recurs |
| 10096 | Issues reconnecting after Redis failover | 2026 | Sentinel/failover broken |
| 9631 | Consumers losing connection behind Redis HAProxy | 2025 | Proxy environments |
| 9729 | Lost connection to RabbitMQ, could not reconnect | 2025 | Closed, recurs |
| 9586 | Redis reconnection on every task submit | 2025 | Performance degradation |
| 9705 | Failing to re-establish connection to Redis | 2025 | Closed but recurs |
| 8628 | Broker transport connection fail | 2023 | Generic connection loss |
| 8289 | Producer channel not closed, TCP connection unexpectedly closed | 2023 | Connection pool leak |
| 6348 | No failover when losing Redis Sentinel master | 2020 | Sentinel support broken |
| 5716 | Deadlock on Redis OOM | 2019 | Deadlock, not just failure |
| 8845 | Fails to handle scheduled tasks after connection recovery | 2024 | Post-recovery corruption |
| 4867 | Connection reset by peer after start | 2018 | Connection stability |
| 10102 | Connection retry doesn't work with --purge | 2026 | Startup edge case |
| 9462 | Worker consumes unexpected queue after RecoverableConnectionError | 2024 | **State corruption after reconnect** |

### Root Cause Analysis

1. **Connection recovery is incomplete.** Celery reconnects to the broker but doesn't fully restore consumer state. After reconnection, workers may stop consuming, consume from wrong queues, or have stale QoS settings.

2. **No distinction between "connection lost" and "broker unhealthy."** A transient network blip and a dead broker get the same retry logic, leading to either aggressive reconnection storms or slow recovery.

3. **Connection pool management is leaky.** Producer connections are not properly returned to the pool, leading to resource exhaustion under load.

4. **Sentinel/failover support is bolted on.** Redis Sentinel, Redis Cluster, and RabbitMQ clustering each have unique reconnection requirements that Celery's generic connection handling doesn't accommodate.

### Lessons for SwarmQ

1. **Connection recovery must restore full consumer state.** After reconnecting, verify: correct queues are subscribed, QoS/prefetch is correct, no stale state remains. Test this explicitly.

2. **Implement circuit breaker pattern.** Distinguish transient failures (retry immediately) from broker outages (back off exponentially, alert). Don't let a dead broker cause reconnection storms.

3. **Connection pools need explicit health checks.** Periodically ping connections in the pool. Remove stale connections proactively, don't wait for them to fail on use.

4. **Test every broker topology.** Single instance, Sentinel, Cluster, behind HAProxy/load balancer, with TLS, with ACLs. Each is a different failure mode. Don't treat them as the same.

5. **Make reconnection observable.** Log connection state transitions, expose metrics for connection pool size, reconnection count, time since last successful broker contact.

---

## 3. Visibility Timeout & Task Redelivery

**Impact: CRITICAL | Issues: 15+ | Trend: Fundamental design tension, unfixable without architectural change**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 5935 | Long running jobs redelivering after visibility timeout (39 comments) | 2020 | **Most-discussed pain point** |
| 6236 | ETA tasks restored before visibility timeout reached | 2020 | Premature redelivery |
| 6229 | Tasks longer than visibility timeout not re-queued with acks_late | 2020 | Contradictory behavior |
| 9339 | Visibility timeout not functioning as expected | 2024 | Still confusing users |
| 9349 | Feature request: visibility_timeout per worker | 2024 | Workaround needed |
| 4976 | Worker restarted, visibility timeout not exceeded, task not re-executed | 2018 | Task lost |
| 9663 | ETA task prefetched after visibility timeout in same worker | 2025 | Closed, but core issue remains |
| 3430 | Long tasks executed multiple times | 2016 | **10 years of this problem** |
| 8875 | SQS messages taken off queue on warm shutdown, processed only at visibility timeout | 2024 | SQS-specific variant |
| 10172 | SQS warm shutdown leaving in-flight messages | 2026 | Still happening |

### Root Cause Analysis

This is arguably Celery's most fundamental architectural problem. The visibility timeout model creates an impossible tradeoff:

- **Too short:** Long-running tasks get redelivered while still executing, causing duplicates
- **Too long:** If a worker crashes, tasks sit invisible for the entire timeout before being redelivered
- **With acks_late:** Tasks that exceed the visibility timeout get redelivered to another worker while the first worker is still processing them. Both workers complete the task. No deduplication.
- **With acks_early (default):** If the worker crashes after ack but before completion, the task is lost forever

Redis and SQS both use visibility timeout as their primary delivery guarantee mechanism. RabbitMQ uses a different model (heartbeats + consumer cancellation), but Celery's abstraction layer masks this difference and applies visibility-timeout-like semantics even where they don't apply.

### Lessons for SwarmQ

1. **Implement heartbeat-based task liveness, not visibility timeout.** Workers should send periodic heartbeats for in-progress tasks. If heartbeats stop, the task is considered abandoned. This decouples "how long the task runs" from "how long to wait before redelivery."

2. **Support exactly-once semantics via idempotency keys.** Even with heartbeats, network partitions can cause duplicate delivery. Provide built-in deduplication using task IDs stored atomically in the result backend.

3. **Separate the concept of "task claimed" from "task completed."** Three states: unclaimed (in queue), claimed (worker heartbeating), completed (result stored). Visibility timeout is a poor proxy for this.

4. **Make the task lifecycle explicit and queryable.** Users should be able to ask "what state is task X in?" and get a definitive answer, not a probabilistic one based on whether the visibility timeout has expired.

5. **For Redis broker: use Lua scripts for atomic claim-and-extend operations.** Don't rely on separate GET/SET/EXPIRE sequences that can race.

---

## 4. Worker Lifecycle & Shutdown

**Impact: HIGH | Issues: 20+ | Trend: Stable, ongoing**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 10180 | Warm shutdown doesn't cancel prefetched tasks (thread pool) | 2026 | Closed, recently fixed |
| 10172 | SQS warm shutdown leaves in-flight messages | 2026 | Open |
| 10083 | Warm shutdown broken with celery>=5.5.0b3 + eventlet | 2026 | Closed, regression |
| 10084 | AttributeError '_sentinel_poll' during cold shutdown | 2026 | Closed, regression |
| 8880 | Workers terminated automatically: "Canceling task consumer" | 2024 | Unexplained worker death |
| 8417 | Tasks cannot be revoked after warm shutdown | 2023 | Shutdown state confusion |
| 4354 | Celery not returning tasks to pool on shutdown | 2017 | Tasks lost on shutdown |
| 3802 | Successful acks_late tasks not acked on warm shutdown | 2017 | **9 years old, still open** |
| 9781 | ForkPoolWorker exited with SIGKILL | 2025 | OOM kills |
| 8306 | Beat + worker at 100% CPU in Docker | 2023 | Resource exhaustion |
| 7808 | Worker deadlock in logging (fork lock) | 2025 | Closed |
| 6656 | Workers remain in defunct state / zombie processes | 2021 | Zombie processes |
| 8952 | WorkerLostError SIGNAL 11 (SIGSEGV) | 2024 | Worker crashes |
| 9886 | --detach might stall celery in container | 2025 | Container-specific |
| 9588 | Delays in consumer after recycled child process | 2025 | Post-recycle lag |
| 9250 | AttributeError on reconnection with worker_cancel_long_running_tasks_on_connection_loss | 2024 | Crash in recovery path |

### Root Cause Analysis

1. **Prefork model is inherently complex.** Celery's default pool forks child processes, which introduces all the classic fork hazards: deadlocks on locks held at fork time, inherited but invalid file descriptors, zombie processes when signals aren't propagated correctly.

2. **Shutdown has too many modes.** Warm shutdown (SIGTERM, finish current tasks) and cold shutdown (SIGQUIT, terminate immediately) behave differently across pool types (prefork, gevent, eventlet, threads) and broker types. The combinatorial explosion means many paths are untested.

3. **Signal handling across process boundaries is fragile.** The parent process must propagate signals to children, wait for them to complete, and handle the case where children don't respond. This often goes wrong.

4. **Container environments expose assumptions.** Celery was designed for bare-metal/VM deployments. Container environments (PID 1 issues, missing init system, resource limits) create new failure modes.

### Lessons for SwarmQ

1. **Avoid fork. Use threads or async.** Fork is a footgun for long-running daemon processes. Python's threading (with GIL) or asyncio avoids the entire class of fork-related bugs.

2. **One shutdown mode, well-defined.** On SIGTERM: stop accepting new tasks, wait up to a configurable timeout for in-progress tasks, then cancel remaining tasks and exit. Simple, predictable, testable.

3. **Prefetched but unstarted tasks must be returned to the queue on shutdown.** This must be atomic -- either the task is returned or it's being processed, never in limbo.

4. **Design for containers from day one.** Proper PID 1 behavior, graceful shutdown on SIGTERM, health endpoints, resource-aware behavior.

5. **Worker process lifecycle must be observable.** Expose worker state (starting, consuming, draining, shutting down) as a metric/endpoint. Log every state transition.

---

## 5. Memory Leaks

**Impact: HIGH | Issues: 10+ dedicated, many more implicit | Trend: Chronic**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 9475 | Memory leak with redis + chain + group | 2024 | Recent, open |
| 6089 | TID.reply.celery.pidbox objects consuming memory | 2020 | Redis pubsub leak |
| 5391 | Producer high memory with Redis broker | 2019 | Open |
| 5344 | Memory leak with .get() on RPC backend | 2019 | Open |
| 5230 | Memory leak with chord (docker-compose repro) | 2018 | Open |
| 4843 | Continuous memory leak | 2018 | Open |
| 4832 | Memory leak in events.State | 2018 | Open |
| 8343 | Creating tasks inside a task uses lots of RAM | 2023 | Open |
| 9797 | Group results not cleaned up on forget() | 2025 | Closed |
| 7960 | Repeated creation of result backend with eventlet | 2022 | Open |
| 4824 | Chord doesn't unsubscribe from Redis channels | 2018 | Open |

### Root Cause Analysis

1. **Redis pubsub subscriptions accumulate.** Each chord creates a Redis pubsub subscription to track completion. If the chord finishes abnormally (error, timeout, worker crash), the subscription is never cleaned up. Over time this exhausts Redis connections and worker memory.

2. **In-memory caches grow unboundedly.** Worker-side caches for task state, revocation sets, and event history have no size limits. In long-running workers processing millions of tasks, these caches consume gigabytes.

3. **Result metadata is not aggressively cleaned.** Intermediate results from chains/chords persist in the result backend long after the workflow completes. `result_expires` exists but doesn't cover all metadata types.

4. **The `--max-memory-per-child` workaround exists** but it's a band-aid that causes its own problems (task interruption, restart overhead).

### Lessons for SwarmQ

1. **All internal caches must be bounded.** Use LRU caches with configurable max size. Never use unbounded dicts or lists for tracking state.

2. **Resource cleanup must be guaranteed, not best-effort.** Use context managers / `try/finally` for broker subscriptions, connections, and temporary state. Design for the crash case -- have a background sweeper that cleans up orphaned resources.

3. **Result metadata must have automatic TTL.** Every piece of stored state (results, intermediate workflow state, completion counters) must have an expiration. Make this the default, not an opt-in.

4. **Memory usage must be observable.** Expose per-worker memory usage, cache sizes, active subscription counts. Alert before problems become critical.

---

## 6. Missing Native Async/Await Support

**Impact: HIGH (strategic) | Issues: 5-10 direct, but affects entire ecosystem | Trend: Stalled**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 3884 | Celery asyncio tasks (32 comments) | 2017 | **9 years old, still open** |
| 6603 | Awaitable task result | 2021 | Feature request, open |
| 6552 | Support async functions | 2020 | Feature request, open |
| 10044 | Celery + Gevent doesn't yield when delaying tasks | 2025 | Gevent workaround fails |
| 9257 | Deprecate eventlet-based concurrency | 2024 | Eventlet being abandoned upstream |
| 9195 | Error with eventlet and Celery integration | 2024 | Eventlet breaking |
| 8262 | SynchronousOnlyOperation from celery task using gevent | 2023 | Django async incompatible |

### Root Cause Analysis

Celery was built in the pre-asyncio era (2009). Its concurrency model is based on:

1. **Prefork** (default): Multiprocessing, one task per process. No async.
2. **Eventlet/Gevent**: Monkey-patching. Brittle, incompatible with many libraries, eventlet is now effectively abandoned.
3. **Threads**: Limited by GIL for CPU work, but workable for I/O.

There is no way to write `async def my_task()` and have Celery run it in an event loop. This is a fundamental limitation because:

- Modern Python libraries (httpx, asyncpg, aiohttp) are async-first
- Django 4+ and FastAPI are async-native
- Eventlet (Celery's primary async workaround) is being deprecated
- Users must choose between Celery's task infrastructure and Python's async ecosystem

The Celery team has discussed asyncio support for years but it would require a near-complete rewrite of the worker internals, which are built on synchronous I/O with Kombu.

### Lessons for SwarmQ

1. **Async-native from day one.** This is SwarmQ's single biggest competitive advantage. The task execution loop should be an asyncio event loop. Tasks should be `async def` by default, with a wrapper for sync tasks.

2. **Support both async and sync tasks transparently.** Run sync tasks in a thread pool executor within the event loop. Users shouldn't have to think about this.

3. **The broker client must be async.** Use aio-pika for RabbitMQ, aioredis/valkey-py async for Redis. Synchronous broker I/O in an async worker is a bottleneck and a deadlock risk.

4. **Don't build on eventlet or gevent.** They are dying technologies. Build on stdlib asyncio.

---

## 7. Celery Beat / Scheduler Problems

**Impact: MEDIUM-HIGH | Issues: 20+ | Trend: Chronic, some very old**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 1495 | Failover for celery beat | 2013 | **13 years old, still open** |
| 7148 | Celery beat should be stateless | 2021 | Architectural request |
| 10107 | Hourly crontab skips during DST fallback | 2026 | Recent, open |
| 9715 | Crontab schedules continuously when timezone mismatch | 2025 | Open |
| 6438 | Beat stops sending tasks on DST changes | 2020 | Open |
| 9653 | crontab() executed one hour late after DST | 2025 | Closed |
| 7649 | Beat gets stuck if scheduler requests retry later | 2022 | Open |
| 6742 | System datetime change stops beat | 2021 | Open |
| 4815 | Beat: remote management and distributed capabilities | 2018 | Feature request |
| 2301 | Adding jitter to recurring tasks | 2014 | **12 years old** |
| 8518 | Tasks missed when using celery beat | 2023 | Open |
| 4568 | Periodic task runs too frequently | 2018 | Open |
| 4307 | Crontab doesn't work like real crontab (day_of_week + day_of_month) | 2017 | Open |
| 9622 | Beat stopped scheduling events | 2025 | Closed, recurs |
| 9951 | Beat crashes with UnpicklingError | 2025 | Closed |

### Root Cause Analysis

1. **Single point of failure.** Celery Beat is a single process. If it dies, no periodic tasks run. There is no built-in failover, leader election, or distributed mode. This issue (#1495) has been open for 13 years.

2. **DST handling is fundamentally broken.** Hourly crontabs skip or double-fire during DST transitions. The scheduler compares local times without properly handling the ambiguous hour during fall-back or the missing hour during spring-forward.

3. **Stateful with fragile persistence.** Beat stores its schedule state in a shelve file or database. Corruption of this file (which happens on crashes) can cause missed or duplicated schedules. Users have been asking for a stateless design since at least 2021.

4. **No jitter support.** When Beat fires 1000 periodic tasks at exactly the same second, they all hit the broker simultaneously. There's no built-in jitter to spread the load.

### Lessons for SwarmQ

1. **The scheduler must be distributed.** Use the broker itself (e.g., Redis sorted sets with Lua scripts) for schedule storage and leader election. Any worker can be the scheduler; if the current leader dies, another takes over within seconds.

2. **Use UTC internally, always.** Store all schedule times in UTC. Convert to local time only for display. This eliminates the entire class of DST bugs.

3. **Built-in jitter.** Add configurable jitter (random delay within a window) to all periodic schedules. This prevents thundering herd on the broker.

4. **Stateless scheduler design.** The schedule definition lives in code/config. The only runtime state is "when did this last fire?" which is stored in the broker atomically. No local files, no shelve, no corruption risk.

5. **Missed-fire policy.** When the scheduler was down and comes back up, what happens to missed schedules? Provide explicit policies: skip (default), fire-once (coalesce), or fire-all.

---

## 8. Result Backend Fragility

**Impact: HIGH | Issues: 15+ | Trend: Stable**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 8588 | Tasks silently fail with result None if Redis backend goes away | 2023 | **Silent data loss** |
| 9654 | Results not stored when task_ignore_result=True but apply_async says ignore_result=False | 2025 | Closed, config confusion |
| 10194 | Soft time limit doesn't trigger on_failure or update result backend | 2026 | Closed |
| 10097 | Missing index on date_done causes lock contention + state corruption during cleanup | 2026 | Closed |
| 10109 | Stale connection failures due to missing pool health defaults | 2026 | Closed |
| 10106 | TaskSet.date_done default evaluated once at import | 2026 | Closed |
| 9613 | RedisBackend doesn't retry on store_result even with result_backend_always_retry | 2025 | Closed |
| 8336 | Database backend doesn't store children | 2023 | Open |
| 8853 | Pickle + Elasticsearch backend throws SerializationError | 2024 | Open |
| 9370 | result_compression doesn't work | 2024 | Open |
| 8877 | AbortableTask KeyError after abort with result_extended | 2024 | Open |
| 8166 | Redis backend._pending_messages not released | 2023 | Memory leak |
| 3808 | Subscribe too much celery-task-meta using Redis backend | 2017 | Open |
| 10119 | Support configuring additional connection errors for Redis backend | 2026 | Closed |

### Root Cause Analysis

1. **Result storage is fire-and-forget.** If the result backend is down when a task completes, the result is silently lost. `result_backend_always_retry` exists but doesn't cover all code paths (as #9613 shows).

2. **Result backend connection handling is separate from broker connection handling.** They have different retry logic, different connection pools, different health check behavior. Users expect them to be equally resilient.

3. **The result backend is optional but many features require it.** Chords, chains with result passing, and task state queries all need the result backend. But the default is "no result backend," so users often discover this requirement after deployment.

4. **TTL/cleanup is poorly implemented.** The `result_expires` setting exists but cleanup is a separate process (celery beat + backend_cleanup). For database backends, cleanup can cause lock contention on tables with millions of rows.

### Lessons for SwarmQ

1. **Result storage must be guaranteed or explicitly disabled.** If result storage is enabled, a failure to store must retry (with circuit breaker) or raise a visible error. Never silently lose results.

2. **Unify broker and result backend connection management.** Same retry logic, same health checks, same observability. Better yet, use the same backend for both (Redis can serve as both broker and result store).

3. **Result cleanup must be online, not batch.** Use Redis TTL or similar per-key expiration. Don't rely on a periodic batch cleanup job that can cause table locks.

4. **Make the result backend required for workflows.** If a user defines a chain or group, require a result backend. Fail at definition time, not at runtime.

---

## 9. Task Duplication & At-Least-Once Semantics

**Impact: HIGH | Issues: 15+ | Trend: Fundamental, ongoing**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 9882 | Task duplication with Valkey 8.0.1 | 2025 | Open |
| 8585 | Multiple tasks executed on cluster node restart | 2023 | Open |
| 5186 | retry_backoff creating duplicate tasks | 2018 | Open |
| 4426 | Task executed twice when worker restarts | 2017 | Open |
| 4400 | Same task runs multiple times at once | 2017 | Open |
| 3312 | Duplicate task in reserved tasks | 2016 | Open |
| 2481 | Celery multiplying unwanted tasks | 2015 | **11 years old** |
| 9262 | Tasks repeatedly executed by multiple workers | 2024 | Open |
| 6358 | Same retry task received multiple times | 2020 | Open |
| 5164 | High load system, fast tasks, some lost | 2018 | Open |
| 3430 | Long tasks executed multiple times | 2016 | Open (see also #5935) |

### Root Cause Analysis

This is the flip side of the visibility timeout problem (#3). Celery provides at-least-once delivery, which means:

1. **Duplicate execution is by design**, but users don't expect it. The documentation doesn't adequately convey that tasks may execute more than once.
2. **No built-in deduplication.** There's no task-level idempotency key or deduplication mechanism. Users must implement their own.
3. **Prefetching multiplies the problem.** Workers prefetch multiple messages. On restart, prefetched-but-unstarted tasks may be both returned to the queue AND redelivered by the broker, causing duplicates.
4. **Broker failover creates duplicates.** When a Redis Sentinel fails over or a RabbitMQ node goes down, in-flight messages may be redelivered to new consumers while still being processed by old ones.

### Lessons for SwarmQ

1. **Provide built-in idempotency.** Allow tasks to declare an idempotency key. SwarmQ stores a record of completed idempotency keys (with TTL) and skips re-execution of duplicates.

2. **At-most-once and at-least-once should be configurable per task.** Some tasks (sending emails) need at-least-once. Others (updating a counter) need at-most-once. Don't force a single delivery semantic.

3. **Document delivery semantics prominently.** Make it impossible for users to miss the fact that tasks may execute more than once without idempotency protection.

4. **Prefetch count should default to 1.** Celery's default prefetch of 4 causes confusion and increases duplicate risk on restart. A prefetch of 1 is safer and only marginally slower for most workloads.

---

## 10. Serialization & Type System

**Impact: MEDIUM | Issues: 10+ | Trend: Stable**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 8852 | ExceptionGroup always results in UnpicklableException | 2024 | Open |
| 8542 | send_task ignores serializer property on task | 2023 | Open |
| 8285 | Error when event_serializer=msgpack | 2023 | Open |
| 4710 | Unable to use msgpack with groups | 2018 | Open |
| 9442 | pydantic=true not effective | 2024 | Open |
| 9794 | Pydantic integration doesn't work with string annotations | 2025 | Closed |
| 9951 | Beat crashes with UnpicklingError | 2025 | Closed |
| 9091 | Propagate serializer change to AsynPool.send_job | 2024 | Open |
| 8812 | Wrong destination -- received unknown message | 2024 | Deserialization failure |
| 7975 | Worker killed if queue message is in wrong format | 2022 | Open |

### Root Cause Analysis

1. **Pickle is the default but a security risk.** Celery defaults to JSON now, but pickle is still widely used because JSON can't serialize complex Python objects. There's no middle ground -- no safe, expressive serializer built in.

2. **Serializer selection is inconsistent across components.** Tasks, results, events, and internal messages can each use different serializers, but the interactions aren't well-tested. Using msgpack for events breaks. Using a custom serializer for tasks doesn't propagate to canvas primitives.

3. **Type coercion is lossy.** JSON serialization loses type information (datetime becomes string, tuple becomes list, Decimal becomes float). Users discover this at runtime, not at definition time.

4. **No schema validation.** Task arguments are not validated at submission time. A type mismatch (e.g., passing a dict where a list is expected) only surfaces when the worker tries to execute the task.

### Lessons for SwarmQ

1. **JSON-only by default, with a type-aware extension.** Support a JSON superset that preserves datetime, UUID, Decimal, bytes, etc. via type tags. Never default to pickle.

2. **Validate task arguments at submission time.** Use Pydantic models or dataclasses for task parameter declarations. Reject invalid arguments before they hit the broker.

3. **One serializer for everything.** Tasks, results, events, internal messages -- all use the same serializer. Eliminate the combinatorial explosion of serializer mismatches.

4. **Reject undeserializable messages gracefully.** If a message can't be deserialized, send it to a dead-letter queue with the raw bytes and an error description. Never crash the worker.

---

## 11. Configuration Complexity & Documentation Gaps

**Impact: MEDIUM | Issues: 25+ documentation issues | Trend: Stable, chronic**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 9279 | Why is disabling prefetch only possible with late ack? | 2024 | Confusing coupling |
| 9281 | Link between config and CLI options unclear | 2024 | Open |
| 8203 | Missing docs on worker pool types | 2023 | Open |
| 8737 | What is --statedb for? | 2023 | Open |
| 8396 | Deprecation documentation | 2023 | Open |
| 9523 | Clarify priority int meaning (higher or lower?) | 2025 | Closed, but confusing |
| 9940 | task_default_exchange confusing | 2025 | Open |
| 9921 | Password character restrictions undocumented | 2025 | Open |
| 9034 | link_error in send_task not working as documented | 2024 | Open |
| 8661 | Worker initialization undocumented | 2023 | Open |
| 9787 | Task handler execution order unclear | 2025 | Closed |
| 10067 | Migration guide 4.x to 5.x incomplete for gevent | 2026 | Open |
| 8909 | How to use group periodically -- missing docs | 2024 | Open |

### Root Cause Analysis

1. **300+ configuration settings.** Celery has an enormous configuration surface with complex interactions between settings. Many settings only make sense in combination (e.g., `acks_late` + `task_reject_on_worker_lost` + visibility timeout).

2. **Config key naming changed between v3 and v4.** The old `CELERY_` prefixed uppercase settings were replaced with `task_` / `worker_` / `broker_` lowercase settings. Both still work, leading to confusion in documentation and Stack Overflow answers.

3. **Implicit dependencies between settings.** Disabling prefetch requires `acks_late`. Priority requires specific broker configuration. These dependencies are not enforced or clearly documented.

### Lessons for SwarmQ

1. **Minimal configuration surface.** Start with <30 settings. Every setting must have a clear, documented reason for existing.

2. **Validate configuration at startup.** Check for contradictory settings, missing dependencies, and deprecated values. Fail fast with a clear error message, not silently at runtime.

3. **Typed configuration with documentation inline.** Use a Pydantic model for configuration. Every field has a type, a default, a description, and validation. IDE autocomplete works automatically.

4. **No setting aliases.** One name for each setting, forever. Don't rename settings across versions.

---

## 12. Observability & Debugging Gaps

**Impact: MEDIUM | Issues: 10+ | Trend: Increasing demand**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 9814 | Flower is no longer maintained | 2025 | Closed, ecosystem risk |
| 9318 | inspect.active returns finished tasks | 2024 | Incorrect introspection |
| 9056 | Worker memsample not updating | 2024 | Broken metrics |
| 9506 | setup_logging silently discards exceptions | 2025 | Open |
| 8731 | Log duplication: INFO and WARNING | 2023 | Open |
| 9245 | "No nodes replied within time constraint" | 2024 | Open |
| 8125 | Connection pool visibility | 2023 | Open |
| 8307 | Errors in signals not propagated, logged instead | 2023 | Open |
| 9569 | Celery inspect leaks beat_dburi password | 2025 | Closed |
| 9563 | Flower leaks broker password | 2025 | Closed |
| 8556 | messages_ready for Redis broker | 2023 | Open |
| 9722 | celery status should show friendly error, not traceback | 2025 | Closed |
| 9619 | loglevel interferes with boto3/opentelemetry | 2025 | Closed |

### Root Cause Analysis

1. **Flower (the primary monitoring tool) is no longer maintained.** The ecosystem's main monitoring solution is abandoned. There is no official replacement.

2. **Worker introspection is unreliable.** `inspect.active()` can return stale data. Control commands time out silently. There's no way to get a reliable snapshot of what every worker is doing.

3. **Logging is inconsistent.** Log levels are wrong (errors logged as warnings), exceptions are swallowed, password-containing URLs are logged, and Celery's logging configuration fights with application logging.

4. **No OpenTelemetry integration.** Modern observability stacks use distributed tracing. Celery has events, but they don't integrate with OTLP/Jaeger/Datadog tracing.

### Lessons for SwarmQ

1. **Built-in observability, not bolted-on.** Prometheus metrics endpoint on every worker (task counts, latencies, queue depths, memory, connection pool stats). OpenTelemetry trace context propagation through task chains.

2. **Reliable worker introspection.** A health endpoint on each worker that returns its current state: what tasks it's running, queue depths, connection status. No distributed broadcast needed -- direct HTTP or shared state in Redis.

3. **Structured logging.** JSON logs with consistent fields: task_id, worker_id, queue, duration, status. Never log credentials.

4. **A simple built-in dashboard.** Not a full Flower replacement, but a basic web UI showing workers, queues, and recent tasks. Enough that users don't need a third-party tool for basic visibility.

---

## 13. DLQ / Retry / Error Handling

**Impact: MEDIUM-HIGH | Issues: 15+ | Trend: Growing demand**

### Key Issues

| # | Title | Age | Signal |
|---|-------|-----|--------|
| 10195 | New-style errbacks called with no exception handling -- one failure halts the rest | 2026 | Open |
| 9674 | chord_unlock retries eternally on nested chain/group failure | 2025 | Open |
| 8947 | Celery retries tasks forever | 2024 | Open |
| 9082 | Avoid infinite retries for reject_on_worker_lost | 2024 | Open |
| 8211 | Chord raises ChordError even with error handler defined | 2023 | Open |
| 6976 | Chain on_error fails | 2021 | Open |
| 4222 | Reject exception doesn't cause task to fail | 2017 | Open |
| 9596 | Task acks on hard timeout despite acks_on_failure_or_timeout=False | 2025 | Closed |
| 10128 | Ack of failure and timeout should be decoupled | 2026 | Closed |
| 10194 | Soft time limit doesn't trigger on_failure or update result | 2026 | Closed |
| 9305 | Celery doesn't react to invalid arguments | 2024 | Open |
| 8852 | ExceptionGroup always UnpicklableException | 2024 | Open |

### Root Cause Analysis

1. **No built-in DLQ.** Celery has no concept of a dead-letter queue. Failed tasks are either retried (potentially forever), silently dropped, or left in the result backend as "FAILURE." There's no quarantine for poison messages.

2. **Retry logic is primitive.** `max_retries` exists but there's no built-in exponential backoff with jitter (users must implement `self.retry(countdown=2**self.request.retries)`). The `retry_backoff` option was added but has bugs (#5186).

3. **Error callbacks (errbacks) are fragile.** Errbacks can themselves fail, and when they do, the failure is swallowed. In chords, error handlers don't fire in all cases. In chains, errors don't always propagate correctly.

4. **Timeout handling is inconsistent.** Soft time limits, hard time limits, and visibility timeouts interact in confusing ways. A soft time limit doesn't always trigger on_failure. A hard timeout may or may not ack the task depending on configuration.

### Lessons for SwarmQ

1. **Built-in DLQ, always.** Every queue has an associated dead-letter queue. Tasks that exceed max retries go there automatically. DLQ tasks are queryable, replayable, and have full context (original args, exception, retry history).

2. **Exponential backoff with jitter as the default retry strategy.** `max_retries`, `initial_delay`, `max_delay`, `jitter` -- all configurable per task. No manual `countdown` calculation.

3. **Error handlers must be crash-safe.** If an error callback fails, log the failure but continue executing remaining error callbacks. Never let a failing errback halt error processing.

4. **Timeout semantics must be simple and documented.**
   - `soft_timeout`: raises SoftTimeLimitExceeded, task can catch and clean up
   - `hard_timeout`: kills the task, marks as FAILURE, triggers error callbacks
   - No other timeout modes. No interaction with visibility timeout.

---

## 14. Broker-Specific Traps

**Impact: MEDIUM-HIGH | Issues: Spread across broker components | Trend: Increasing with new broker types**

### Redis-Specific

| # | Title | Signal |
|---|-------|--------|
| 8276 | Redis Cluster not supported as broker | Major limitation |
| 9436 | Can't connect to Redis Cluster | Open |
| 9019 | AWS MemoryDB / ElastiCache Cluster Mode unsupported | Open |
| 5716 | Deadlock on Redis OOM | Workers freeze |
| 4465 | Celery.close() leaks Redis connections | Open |
| 4039 | Operation timed out with Redis backend | Open |
| 8832 | Redis PING too many values to unpack | Open |
| 9882 | Task duplication with Valkey | Open |
| 8968 | How to use Redis Cluster as backend | Open |

### RabbitMQ-Specific

| # | Title | Signal |
|---|-------|--------|
| 9699 | Worker error: binding to non-existent broadcast exchange | Open |
| 9867 | RabbitMQ detects cycle with quorum queues + delayed retry | Closed |
| 9913 | --disable-prefetch doesn't work with RabbitMQ | Closed |
| 9912 | ETA tasks stuck in delayed queue with quorum queues | Closed |
| 9671 | Auto-create missing queues as quorum type | Closed |
| 8265 | Handle RabbitMQ Stream Queues | Open |

### SQS-Specific

| # | Title | Signal |
|---|-------|--------|
| 9749 | Celery with SQS is slow | Closed |
| 8875 | SQS warm shutdown leaves messages until visibility timeout | Open |
| 8243 | Worker halts on SQS error | Open |
| 8015 | SQS Signature expired | Open |
| 8395 | time_limit and soft_time_limit don't work with SQS | Open |
| 6852 | SQS FIFO consumption stops with many workers | Open |
| 8352 | Same Message Group tasks run in parallel (FIFO violated) | Open |

### Root Cause Analysis

1. **Leaky abstraction.** Celery's Kombu transport layer abstracts over fundamentally different messaging systems. Redis (data structure store with pub/sub), RabbitMQ (AMQP message broker), and SQS (HTTP-based queue) have different semantics, but Celery tries to make them look the same. This leads to broken assumptions everywhere.

2. **Redis Cluster is unsupported.** This is a major limitation for production Redis deployments. Standard Redis is a single point of failure; Cluster mode solves this, but Celery can't use it.

3. **RabbitMQ quorum queues are poorly supported.** Quorum queues are RabbitMQ's recommended queue type for production, but Celery's delayed delivery, auto-queue-creation, and retry mechanisms don't work correctly with them.

4. **SQS's HTTP-based model clashes with Celery's assumptions.** SQS has high latency per operation, 256KB message size limit, visibility timeout as the only delivery guarantee, and FIFO ordering that Celery doesn't respect.

### Lessons for SwarmQ

1. **Support fewer brokers, but support them perfectly.** Start with one broker (Valkey/Redis) and make it bulletproof. Add RabbitMQ later with a separate, purpose-built transport. Don't try to abstract them behind a common interface that hides their differences.

2. **Redis Cluster support from day one.** If Redis/Valkey is the primary broker, Cluster mode must work. Use hash tags to ensure related keys (task, result, dedup) land on the same shard.

3. **Expose broker capabilities, don't hide them.** If the broker supports priority, expose it. If it doesn't, say so clearly. Don't fake capabilities that the broker doesn't natively support.

4. **Test with the broker configurations users actually run in production:** Redis Sentinel, Redis Cluster (via Valkey), RabbitMQ with quorum queues, behind load balancers/proxies, with TLS, with ACLs.

---

## Summary: Top 10 Lessons for SwarmQ

These are the highest-leverage design decisions informed by Celery's decade of battle scars:

### 1. Async-Native Architecture
Celery's lack of async support is its biggest strategic weakness. SwarmQ's entire worker loop, broker client, and result backend should be asyncio-native. This is the primary differentiator.

### 2. Workflows as Data, Not Recursive Objects
Store DAGs in a durable, queryable format. Never build workflows via recursive function calls. Enforce depth/breadth limits. Test with 100+ task workflows with random failures.

### 3. Heartbeat-Based Task Liveness
Replace visibility timeout with worker heartbeats for in-progress tasks. Decouple "how long a task runs" from "how long to wait before redelivery."

### 4. Built-in Idempotency
Provide task-level idempotency keys with automatic deduplication. Make it trivial for users to write safe, non-duplicating tasks.

### 5. Built-in DLQ and Smart Retry
Every queue has a dead-letter queue. Exponential backoff with jitter is the default. Failed tasks are quarantined with full context, not silently dropped.

### 6. Distributed Scheduler
No single-process scheduler. Use the broker for schedule state with leader election. Handle DST by using UTC internally. Add jitter to all periodic schedules.

### 7. Guaranteed Result Storage
If result storage is enabled, results are stored reliably or an error is raised. Use per-key TTL, not batch cleanup. Unify broker and result backend connection management.

### 8. Observable by Default
Prometheus metrics, structured JSON logging, health endpoints, OpenTelemetry trace propagation -- all built in, not bolted on.

### 9. Minimal Configuration
<30 settings with sane defaults. Typed configuration (Pydantic). Validate at startup. No setting aliases or renames across versions.

### 10. One Broker, Done Right
Start with Valkey/Redis (including Cluster mode) and make it perfect. Don't abstract over fundamentally different messaging systems with a leaky abstraction layer.

---

## Appendix: Issue Statistics

### Open Issues by Age

- 2024-2026: ~80 issues (active)
- 2021-2023: ~60 issues (long-standing)
- 2018-2020: ~40 issues (abandoned/architectural)
- 2015-2017: ~15 issues (fundamental design issues, will never be fixed)

### Most Common Labels on Open Issues

1. `Issue Type: Bug Report` -- dominant
2. `Component: Canvas` -- 50+ open issues
3. `Component: Redis Broker` -- 30+ open issues
4. `Category: Documentation` -- 25+ open issues
5. `Component: Amazon SQS Broker` -- 9 open issues
6. `memory leak` -- 6 open issues
7. `Worker Hangs` -- 5 open issues
8. `Component: Celerybeat` -- 20+ open issues

### Recurrence Signals (open + recently closed in same theme)

| Theme | Open | Closed (30d) | Recurrence |
|-------|------|--------------|------------|
| Broker reconnection | 10+ | 3 (10205, 10096, 9705) | HIGH |
| Canvas/workflow | 50+ | 2 (10191, 9835) | HIGH |
| Shutdown/lifecycle | 8+ | 3 (10180, 10083, 10084) | HIGH |
| Result backend | 8+ | 4 (10194, 10097, 10109, 9654) | MEDIUM |
| Scheduler/DST | 10+ | 2 (9653, 9622) | MEDIUM |
