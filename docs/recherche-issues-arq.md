# Recherche: arq GitHub Issues Analysis

**Source:** [python-arq/arq](https://github.com/python-arq/arq) (2,877 stars)
**Analyzed:** 82 open issues + 42 recently closed issues (2022-2026)
**Date:** 2026-04-15

arq is an async Python task queue using Redis, created by Samuel Colvin (Pydantic creator).
It is officially in **maintenance-only mode** since October 2025 (#510).

---

## 1. Maintenance Status and Project Health

**Issues:** #510, #437, #414, #441, #519, #513

arq was declared maintenance-only in October 2025 (#510): "the pydantic team and I
don't have time right now to put significant effort into arq. We'll continue to fix
critical security issues." The earlier discussion (#437, March 2024) explored the
future of arq at length, with the creator acknowledging that the project needs more
investment than he can provide.

Key symptoms of stalled maintenance:
- v0.26 release was requested in April 2024 (#441), PyPI lagged behind GitHub (#489)
- Outdated dependencies (#519), redis version pinning blocks users on redis>=5 (#454, #458)
- Python 3.14 compatibility broken (#512) -- asyncio.get_event_loop() removal
- 82 open issues, many dating back to 2021-2022 with no response

**SwarmQ lesson:** This is the single biggest competitive opportunity. arq users are
actively looking for alternatives. SwarmQ should explicitly position against arq's
abandonment. More tactically: maintain aggressive Python version compatibility (test
against alpha/beta releases) and keep dependencies current. A "bus factor of 1"
project will eventually stall -- build community governance from day one.

---

## 2. Redis Connection Resilience and Reconnection

**Issues:** #498, #435, #386, #493, #471, #315, #231, #155, #270, #235

This is the most severe operational pain point. When Redis connections drop, arq
workers **crash and do not recover**:

- **Worker crashes on Redis restart** (#498): "After restarting the redis, worker
  crashes" -- the worker's async_run() throws a ConnectionError and exits entirely.
- **Worker stops processing after Redis interruption** (#435): "After Redis comes back,
  the worker stops doing jobs/tasks. We are only using arq for cron jobs." In
  Kubernetes environments, this means pods appear healthy but are effectively dead.
- **No built-in retry on connection loss** (#386): The conn_retries/conn_timeout
  settings only apply to initial connection. Any transient network hiccup during
  operation kills the worker. A PR was proposed to expose redis-py's Retry helpers
  but was never merged.
- **SSL connection issues** (#493, #319): Heroku-Redis, custom SSLContext objects, and
  similar configurations break silently or produce confusing errors.
- **Too many Redis calls** (#471): Random spikes in Redis calls under certain conditions.

**SwarmQ lessons:**
1. Connection resilience must be a first-class concern, not an afterthought. The broker
   connection layer needs automatic reconnection with exponential backoff, and workers
   must survive transient broker outages without crashing.
2. Never let a single connection failure crash the entire worker process. Use a
   connection manager that retries transparently.
3. Support Redis Sentinel (#235 timeout not exposed) and Redis Cluster (#385, #358, #357)
   natively. Many production deployments use HA Redis configurations.
4. SSL/TLS configuration must be a documented, tested path -- not a secondary concern.

---

## 3. Job Loss, Stuck Jobs, and Race Conditions

**Issues:** #482, #342, #329, #316, #343, #402, #432, #466

Jobs getting lost, stuck, or duplicated is the most trust-destroying category of bugs:

- **Race condition with retries and multiple workers** (#482): When Worker A finishes
  job X and schedules a retry (incrementing the score), Worker B may have already
  fetched the same job and proceeds to run it again. The retry deferral is ignored.
  This is a fundamental atomicity problem in arq's Redis operations.
- **Jobs stuck in queue** (#316): "4 replicas, queued=10, but jobs not executing." New
  jobs work, but existing queued jobs remain stuck indefinitely.
- **Tasks stuck after worker crash** (#343): "After the crash and restart of the worker,
  tasks don't restart automatically for hours (sometimes 4 or 5 hours)." The
  in_progress_key timeout is set to max_job_timeout, which can be very long.
- **Job status reports "not found"** (#342, #329): Race between job status sampling and
  worker finish_job. The finish_job function uses non-atomic Redis operations, creating
  a window where a job exists but its status reads as "not found."
- **Cannot re-enqueue with same job_id** (#432): After a job completes, users cannot
  re-enqueue with the same _job_id, even though execution is fully complete. The result
  key lingers and blocks re-enqueue.
- **Lost job retried after WORST timeout** (#402): When a job is lost (worker dies), it
  is retried after the worst-case global timeout, not the job's own timeout.

**SwarmQ lessons:**
1. All job state transitions (enqueue, pick, complete, retry) must be atomic. Use Lua
   scripts or transactions to prevent race conditions between multiple workers.
2. The in_progress/heartbeat mechanism is critical. Use short heartbeat intervals
   (seconds, not hours) so stuck jobs are detected quickly. arq's approach of setting
   in_progress_key to max_timeout means hours of delay after a crash.
3. Job deduplication by ID needs a clear lifecycle: pending -> active -> completed ->
   result_ttl_expired -> re-enqueueable. arq conflates "result exists" with "job cannot
   be re-enqueued."
4. Job status must be queryable at all times without race windows. Consider using a
   single hash key per job rather than multiple keys that update non-atomically.

---

## 4. Worker Health, Zombie Detection, and Shutdown

**Issues:** #407, #369, #70, #290, #395, #405, #394, #276

Workers becoming zombies or failing to shut down cleanly is a persistent theme:

- **No real heartbeat mechanism** (#407): arq's `in_progress_key` is a static TTL set
  at job start, not a continuously-refreshed heartbeat. If a worker dies mid-job, the
  job is stuck until the TTL expires (which equals max_timeout -- potentially hours).
  The issue author proposes lowering the default and refreshing on each heartbeat cycle.
- **Workers stuck in terminating state** (#369): When handle_sig_wait_for_completion is
  activated, workers get stuck because task cleanup is nested under allow_pick_jobs,
  creating a deadlock in the shutdown path.
- **Cannot abort running jobs** (#290): open since 2022, this is a fundamental gap.
  Job.abort() sets a flag but does not actually cancel the running coroutine.
- **Synchronous jobs not cancelled after timeout** (#395): Sync jobs ignore timeout
  entirely because they block the event loop.
- **Job abort not working** (#394, #405): Multiple reports of abort simply not working.
- **Queue statistics disordered with multiple workers** (#276): Health/stats reporting
  breaks when multiple workers share a queue.
- **Periodic worker restart** (#70): Feature request from 2017 to restart workers after
  N jobs to combat memory leaks -- never implemented.

**SwarmQ lessons:**
1. Implement a real heartbeat: workers must periodically refresh their liveness key
   (every 5-10 seconds). If the key expires, other workers know the original is dead
   and can reclaim the job.
2. Job cancellation must actually cancel the coroutine (asyncio.Task.cancel()), not
   just set a flag that the job may never check.
3. Graceful shutdown must have a clear protocol: stop accepting new jobs, wait for
   running jobs up to a timeout, then force-cancel. The wait-for-completion path must
   not deadlock.
4. Sync job execution needs a separate thread pool with proper timeout enforcement via
   thread interruption (or process isolation).
5. Worker stats must be scoped per-worker, then aggregated -- not a shared counter.

---

## 5. Retry and Timeout Mechanism Deficiencies

**Issues:** #162, #401, #402, #359, #320, #353, #481, #267, #491

arq's retry mechanism is one of its weakest areas:

- **No manual retry from result** (#162): Open since 2019. Users cannot programmatically
  retry a failed job from its result object. The only retry mechanism is raising a
  `Retry` exception inside the job itself.
- **How to retry a timed-out job?** (#401): No built-in mechanism to retry jobs that
  timed out. Once timed out, they are just failed.
- **Timeout inconsistencies with Retry** (#359): When a job uses Retry with deferral,
  the timeout clock is unclear -- does it reset per retry attempt or accumulate?
- **Jobs expire after ~24 hours regardless of job_timeout** (#320): Long-running jobs
  with many short retries (e.g., polling for a file) expire after roughly 24 hours
  even with high max_tries and job_timeout settings.
- **_job_try parameter broken** (#353): Setting _job_try in enqueue_job causes
  "max retries exceeded" errors.
- **Cannot serialize Retry error with msgpack** (#481): Using non-pickle serializers
  breaks the retry mechanism.
- **Retry timing is unreliable** (#491, closed): "I am seeing very random timing.
  Sometimes over 15 seconds, sometimes less than 9 seconds" for retry delays.
- **Cannot cancel without incrementing retries** (#267): Cancellation counts as a retry
  attempt, burning through max_tries.

**SwarmQ lessons:**
1. Retry must be a first-class, configurable policy -- not an exception you raise.
   Support exponential backoff, linear backoff, custom schedules, and per-job override.
2. Retry policy must be separable from the job function. A job should not need to know
   about retry logic (separation of concerns).
3. Timeouts must be clearly scoped: per-attempt timeout vs. total job lifetime.
4. Cancellation and retry must be orthogonal. Cancelling a job should not count as a
   failed attempt.
5. Retry state (attempt number, next retry time) must be precisely tracked and
   serialization-agnostic.

---

## 6. Cron/Scheduled Job Problems

**Issues:** #196, #465, #459, #450, #351, #280, #347, #193, #263, #430, #483 (closed)

Cron jobs have a surprising number of sharp edges:

- **Duplicate execution with multiple workers** (#196): Open since 2020. "Unique cron
  executes multiple times when running multiple workers." This is a fundamental
  distributed scheduling problem that arq does not solve.
- **Cron timeout defaults to 300s even with timeout=None** (#465): The cron wrapper
  overrides the worker's timeout setting with a hardcoded default.
- **Cannot run cron more frequently than 60 seconds** (#459): Sub-minute scheduling
  does not work correctly when job_id is set.
- **No dynamic cron jobs** (#450): Cron jobs must be defined statically in WorkerSettings
  at startup. Cannot add/remove/modify schedules at runtime.
- **No timezone support** (#351): Cron jobs use local time of the host. In cloud/container
  deployments, this is unpredictable. Rejected twice (#195, #251) with workarounds
  that are not ergonomic.
- **No cron job parameters** (#263): Cannot pass arguments to cron functions.
- **Cron results not accessible** (#347): No built-in way to retrieve cron job results.
- **Overlap prevention missing** (#430): No built-in way to prevent a new cron execution
  if the previous one is still running.
- **Crash on month-specific cron** (#483, closed): Timezone-naive vs timezone-aware
  datetime comparison crashes the worker when using month specifications.

**SwarmQ lessons:**
1. Distributed cron needs leader election or distributed locking. A simple Redis lock
   before execution prevents duplicates across workers.
2. Support timezone-aware scheduling natively. Use UTC internally, allow user-specified
   timezones for display and configuration.
3. Allow dynamic schedule management via API (add, remove, modify, pause schedules).
4. Support sub-second and sub-minute intervals as first-class citizens.
5. Cron overlap policy must be explicit: skip, queue, or allow parallel execution.
6. Cron functions should accept parameters like any other job.

---

## 7. Job Result Handling and Status Querying

**Issues:** #342, #329, #346, #344, #363, #390, #417, #325, #425, #309

Getting job results and status is unreliable:

- **Job.result() hangs indefinitely** (#390): If queue_name is anything other than the
  default "arq:queue", result polling hangs forever. The default queue name is
  hardcoded somewhere in the result lookup path.
- **Job.result() and Job.abort() hang for no-keep-result jobs** (#363): When
  keep_result=False (the default!), calling result() or abort() hangs because it polls
  for a result key that will never be written.
- **Cannot differentiate results across queues** (#344, #346): All results stored under
  `arq:result:jobid` regardless of queue. Multiple services sharing one Redis instance
  can collide.
- **Function-level keep_result_s not respected for errors** (#417): Error results ignore
  the per-function keep_result setting.
- **No task progress reporting** (#425): No built-in mechanism for jobs to report
  progress to callers.
- **Exception serialization is poor** (#309): Error details are lost or mangled during
  serialization.
- **No way to query single job by ID** (#240, #372 closed): Must know both job_id and
  queue to look up a job.

**SwarmQ lessons:**
1. Result storage must be scoped to the queue/namespace. Use `{prefix}:{queue}:result:{job_id}`.
2. Polling-based result retrieval is fragile. Use pub/sub notification when a job
   completes, with polling as fallback.
3. The default behavior must be consistent: if keep_result=False, then result() should
   raise an error immediately, not hang.
4. Built-in progress reporting (job.update_progress(0.5)) is a high-value feature.
5. Exception results should preserve the full traceback, exception type, and message
   in a serialization-agnostic format.

---

## 8. Concurrency Control and Job Deduplication

**Issues:** #432, #457, #419 (closed), #453, #443, #360, #426 (closed), #39

Users consistently need finer-grained concurrency control than arq provides:

- **Cannot re-enqueue completed job with same ID** (#432): The deduplication mechanism
  (job_id uniqueness) persists beyond job completion. Users must wait for result TTL
  to expire before re-enqueueing.
- **No per-function max_jobs** (#419, closed): max_jobs is global across all functions.
  Users need different concurrency limits for different job types (e.g., 10 concurrent
  crawlers but unlimited email jobs).
- **No rate limiting** (#453, #426): Cannot enforce "max N jobs per minute" for
  rate-limited APIs.
- **No task priority** (#360): All jobs are FIFO within a queue. No mechanism to
  prioritize urgent jobs over batch jobs.
- **Scheduling unique repeated jobs** (#457): Difficulty scheduling the same job to run
  repeatedly at different times with uniqueness constraints.

**SwarmQ lessons:**
1. Job deduplication must have configurable scope: per-enqueue (prevent double-submit),
   per-execution (prevent concurrent runs), or per-result (prevent re-run while result
   exists).
2. Per-function concurrency limits are essential for production use. Different job types
   have different resource profiles.
3. Rate limiting (X jobs per time window) should be built-in, not left to the user.
4. Priority queues (or priority levels within a queue) are a must-have for any
   non-trivial deployment.

---

## 9. Async/Event Loop Issues

**Issues:** #512, #480, #144, #324, #404, #395, #499

Being async-native creates its own class of problems:

- **Python 3.14 breaks asyncio.get_event_loop()** (#512): Python 3.14 enforces the
  deprecation of get_event_loop() when no loop is running. arq's CLI entry point and
  worker startup use this deprecated pattern.
- **"Event loop is closed" running demo** (#480): Basic usage patterns hit closed-loop
  errors on Python 3.10+.
- **Test event loop issues** (#144): Open since 2019. Tests fail with "no current event
  loop in thread 'MainThread'" -- this affects arq's own test suite.
- **Cannot run two workers in one process** (#324): worker.async_run() blocks, making
  it impossible to run multiple workers or embed a worker in another async application.
- **Sync jobs block the event loop** (#395): Synchronous job functions block the entire
  worker, preventing heartbeats, job pickup, and cancellation.
- **Debugging with debugpy** (#499): Debuggers that interact with the event loop cause
  issues.

**SwarmQ lessons:**
1. Never use asyncio.get_event_loop() -- always use asyncio.get_running_loop() or
   accept a loop parameter. Test against Python pre-release versions in CI.
2. Worker.run() must be non-blocking (return a Task/Future) so workers can be embedded
   in larger async applications.
3. Sync job functions must run in a thread executor (loop.run_in_executor) to avoid
   blocking the event loop. This is non-negotiable.
4. Provide first-class testing utilities: mock workers, in-memory brokers, or fakeredis
   integration.

---

## 10. Serialization and Type Safety

**Issues:** #497, #505, #481, #384 (closed), #339 (closed), #249

Serialization is a quiet but pervasive source of pain:

- **No native Pydantic serialization** (#497): Despite sharing a creator with Pydantic,
  arq uses pickle by default and has no Pydantic integration. Users working with
  Pydantic models (the vast majority of FastAPI users) must write custom serializers.
- **decode_responses parameter issues** (#505): Switching from aioredis to redis-py
  broke string/bytes handling. Users cannot control decode_responses on ArqRedis.
- **Msgpack breaks Retry** (#481): Alternative serializers are not tested against arq's
  internal objects like Retry exceptions.
- **Dataclass deserialization fails** (#384, closed): pickle-based serialization requires
  the exact same class definition available at deserialization time.
- **Cannot deserialize jobs externally** (#339, closed): Monitoring tools, dashboards,
  and other services cannot read arq job data because pickle requires the original
  Python environment.
- **No type hints for cron_jobs** (#249): WorkerSettings.cron_jobs has no type
  annotations, making IDE support poor.

**SwarmQ lessons:**
1. Default to a human-readable, cross-service serializer (JSON or msgpack). Pickle
   should be opt-in, never the default -- it is a security risk and prevents external
   tooling.
2. First-class Pydantic model support for job arguments and results. Leverage Pydantic
   v2's serialization for type-safe, cross-service job definitions.
3. Internal objects (retry signals, errors) must be serializable with the same
   serializer as job payloads. No special cases.
4. Full type annotations throughout the public API. Job function signatures should be
   preserved and validated at enqueue time.

---

## 11. Testing and Developer Experience

**Issues:** #397, #429, #120, #297, #317, #286, #99, #469, #130

The developer and operator experience has significant gaps:

- **No fakeredis integration** (#397): No documented way to inject a fake Redis for
  testing. Users must run a real Redis for tests.
- **--watch does not reload** (#429): The file watcher claims to reload but does not
  actually pick up code changes.
- **No dashboard/monitoring** (#120, #297, #317, #469): Repeatedly requested since 2019.
  No built-in web UI, no Prometheus metrics endpoint, no structured way to observe
  queue depth, worker health, or job status.
- **Confusing log levels** (#286): Debug-level logs are noisy, important events are
  logged at wrong levels.
- **No job ID in logs** (#99): Cannot correlate log lines to specific jobs.
- **arq --check command** (#130): Requested health check CLI command, never implemented.

**SwarmQ lessons:**
1. Provide an in-memory broker for testing. Unit tests should not require Redis.
2. Ship Prometheus/OpenTelemetry metrics from day one: queue depth, job duration,
   error rate, worker count, retry rate.
3. Structured logging with job_id, queue, worker_id in every log line.
4. A health check endpoint (HTTP or CLI) for Kubernetes liveness/readiness probes.
5. Hot-reload for development must actually work.

---

## 12. Scalability and Work Distribution

**Issues:** #503, #502, #455, #39, #245, #264

Scaling arq beyond simple use cases reveals architectural limits:

- **No work distribution / load balancing** (#503, #502): Multiple workers simply race
  to pop from the same sorted set. There is no intelligent distribution, no affinity,
  no backpressure.
- **Cannot batch-fetch jobs** (#455): Workers poll one-by-one. When finishing a batch,
  there is no mechanism to prefetch the next batch for pipeline efficiency.
- **No task orchestration** (#245): No built-in workflows, chains, groups, or chords.
  Users wanting "run A then B then C" must manually enqueue from within jobs (#264).
- **Pipeline arguments** (#39): Open since 2017. Cannot pipe one job's output as input
  to the next job.

**SwarmQ lessons:**
1. Work distribution should support both competing-consumers (simple) and routed
   dispatch (for affinity/locality).
2. Batch operations (enqueue many, fetch many) are essential for throughput.
3. Workflow primitives (chain, group, chord) are a key differentiator over arq. Even
   basic "run after" semantics would cover most use cases.
4. Backpressure signaling (queue full, worker overloaded) prevents cascading failures.

---

## Summary: Top Lessons for SwarmQ by Priority

### Critical (address in v0.1)

| Area | arq Failure | SwarmQ Design Response |
|------|------------|----------------------|
| Connection resilience | Worker crashes on any Redis hiccup | Auto-reconnect with backoff; never crash the worker process |
| Job atomicity | Race conditions between workers on retry/status | Lua scripts for all state transitions |
| Heartbeat | Static TTL = hours of stuck jobs after crash | 10-second heartbeat refresh; fast dead-worker detection |
| Retry policy | Must raise exception inside job; no backoff | Declarative retry policy with exponential backoff, separate from job code |
| Graceful shutdown | Deadlocks on SIGTERM | Clear shutdown protocol: drain, wait, force-cancel |

### High (address in v0.2)

| Area | arq Failure | SwarmQ Design Response |
|------|------------|----------------------|
| Cron dedup | Duplicate execution across workers | Distributed lock before cron execution |
| Result handling | Hangs, namespace collisions, no progress | Scoped results, pub/sub notification, progress API |
| Concurrency control | Global max_jobs only | Per-function limits, rate limiting, priority levels |
| Serialization | Pickle default, breaks external tools | JSON/msgpack default, Pydantic integration |
| Monitoring | Nothing built-in | Prometheus metrics, structured logging, health endpoint |

### Medium (address in v0.3+)

| Area | arq Failure | SwarmQ Design Response |
|------|------------|----------------------|
| Workflows | No orchestration primitives | Chain, group, chord support |
| Testing | Requires real Redis | In-memory broker for unit tests |
| Dynamic scheduling | Static cron only | Runtime schedule management API |
| Multi-broker | Redis-only | Pluggable broker abstraction (Redis/Valkey as primary) |
| Type safety | Minimal type hints | Full typing, function signature validation at enqueue |

---

## Appendix: All Analyzed Issues

### Open Issues (82 total)

| # | Title | Theme |
|---|-------|-------|
| 519 | Still uses older dependencies | Maintenance |
| 512 | Python 3.14 Compatibility: asyncio.get_event_loop() | Async/Event Loop |
| 510 | Maintenance only mode | Maintenance |
| 505 | Issue with decode_responses parameter | Serialization |
| 503 | work distribution among workers | Scalability |
| 502 | Is there built-in worker management or load balancing? | Scalability |
| 501 | Ensuring the burst worker shuts down if no jobs | Worker Health |
| 499 | Debugging with debugpy | Async/Event Loop |
| 498 | After restarting the redis, worker crashes | Connection |
| 497 | Native support for Pydantic serialization | Serialization |
| 493 | Heroku-Redis SSL Issues | Connection |
| 485 | How to safely re-enqueue an aborted job? | Retry |
| 484 | Is there a way to handle exceptions? | DX |
| 482 | Race condition with retries and multiple workers | Job Loss |
| 481 | Unable to serialize Retry error with msgpack | Serialization |
| 480 | "RuntimeError: Event loop is closed" | Async/Event Loop |
| 474 | How to delete a job? | Result Handling |
| 472 | Support for OO WorkerSettings classes | DX |
| 471 | Too many Redis calls happen randomly | Connection |
| 469 | How to monitor | Monitoring |
| 466 | Unable to run redis.queued_jobs() | Result Handling |
| 465 | Cron jobs timeout after 300s when timeout=None | Cron |
| 459 | Unable to run cron every 5 seconds | Cron |
| 457 | Scheduling repeated unique jobs | Concurrency |
| 455 | Poll for next batch of job when about to finish | Scalability |
| 454 | Library does not support latest redis version | Maintenance |
| 453 | Time based Queue. Handling X jobs per minute | Concurrency |
| 450 | Dynamic Cron Jobs | Cron |
| 447 | crash if expires_ms is negative | Job Loss |
| 443 | Concurrent Functions | Concurrency |
| 441 | v0.26 release | Maintenance |
| 436 | Allow for user specified Redis key prefix | Connection |
| 434 | How do I handle job time out errors within the task? | Retry |
| 432 | Cannot enqueue job with same _job_id again | Concurrency |
| 429 | --watch does not reload code changes | DX |
| 428 | RQ publisher to ARQ consumer | DX |
| 425 | Task Progress | Result Handling |
| 418 | redis-py complains about pool.close vs pool.aclose | Connection |
| 417 | Function level keep_result_s not respected for errors | Result Handling |
| 416 | How can one self-re-enqueue when using _job_id? | Concurrency |
| 415 | Add args/kwargs to job context for hooks | DX |
| 410 | Question: failed job reason | Result Handling |
| 407 | Feature Request: heartbeat functionality | Worker Health |
| 405 | Job aborts failing | Worker Health |
| 404 | Use pubsub for improved response times | Result Handling |
| 402 | Lost job retried after WORST timeout, not ITS timeout | Retry |
| 401 | How to retry a timed out job? | Retry |
| 397 | Testing: is there a way to inject fakeredis? | Testing |
| 395 | Synchronous Jobs not cancelled after timeout | Worker Health |
| 394 | Job abort not working? | Worker Health |
| 385 | Redis Cluster support | Connection |
| 360 | Feature Request: Task Priority | Concurrency |
| 359 | timeout inconsistencies with Retry | Retry |
| 347 | Cron Results | Cron |
| 346 | Unable to differentiate results | Result Handling |
| 342 | Enqueued jobs report "not found" until worker sees them | Job Loss |
| 317 | Prometheus metrics | Monitoring |
| 315 | Issue using redis_pool and watchgod reloading | Connection |
| 309 | Exception serialization improvement | Serialization |
| 297 | Monitoring of queues and task results | Monitoring |
| 290 | Cannot abort running jobs | Worker Health |
| 286 | Confusing debug log levels | DX |
| 280 | Specifying weekday strings in cron jobs | Cron |
| 277 | Event hooks | DX |
| 276 | Queue statistics disordered with multiple workers | Worker Health |
| 270 | support redis socket | Connection |
| 267 | Cancel Jobs without Incrementing Retries | Retry |
| 264 | How to run a job that creates more jobs? | Scalability |
| 263 | Cron jobs with parameters | Cron |
| 249 | Type hints for cron_jobs | Serialization |
| 246 | method to cancel jobs | Worker Health |
| 245 | Support for task orchestration | Scalability |
| 240 | Document querying single Job by ID | Result Handling |
| 235 | Sentinel timeout not exposed | Connection |
| 231 | Intermittent worker timeout | Worker Health |
| 196 | Unique cron executes multiple times with multiple workers | Cron |
| 193 | Cron job scheduling behaviour | Cron |
| 162 | Manually retrying job from result object | Retry |
| 155 | warning and close if pool is None | Connection |
| 144 | Tests fail with RuntimeError: no current event loop | Async/Event Loop |
| 130 | arq --check command result | Monitoring |
| 120 | Guidance for dashboard functionality | Monitoring |
| 99 | prefix logs with the job id | DX |
| 70 | restart the worker periodically | Worker Health |
| 39 | pipeline argument to a concurrent job | Scalability |
