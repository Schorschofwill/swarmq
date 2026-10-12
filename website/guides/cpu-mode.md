# CPU mode (`--pool cpu`)

SwarmQ is pure asyncio. A synchronous Pillow, numpy or ImageMagick call
blocks the event loop and with it every concurrent task of the worker, and
a `hard_timeout` never reaches a blocked loop. CPU mode moves task bodies
out of the loop process into short-lived **compute children** forked from a
pre-warmed `forkserver`.

```bash
swarmq worker --module myapp.tasks --pool cpu --queues images
```

## What changes

| Concern | IO worker (default) | CPU mode |
|---|---|---|
| Where `run()` executes | in the worker's event loop | in a compute child, one per task |
| `--concurrency` | tasks in flight | compute slots; default = CPUs assigned to the worker (affinity ∩ cgroup quota) divided by `--processes` |
| `hard_timeout` | asyncio cancel; cannot interrupt a blocked loop | kills the child's process group (SIGTERM, 1 s grace, SIGKILL); the task follows the usual retry/DLQ path |
| Cancel | asyncio cancel | kills the child; the task ends as cancelled, no retry |
| Gates, middleware, signals, ack | loop process | loop process — unchanged |
| `FromDishka[...]`, `Annotated[X, FromComponent(...)]`, aliases | resolved in the loop | resolved in the child; the container object is built at import time, never resolved in the forkserver. The child imports the task module again, so the annotations must be resolvable at class definition there too (see [dependency injection](../reference/api.md#dependency-injection-dishka)) |

Tasks **without** dependency injection get a fresh child per task — nothing
survives a task, so no leak ever survives either. Tasks **with**
`FromDishka` reuse a warm child per slot and are recycled when its RSS after
a task exceeds `SWARMQ_RECYCLE_RSS_MB` (default 250 MB in CPU mode) or after
`SWARMQ_RECYCLE_MAX_TASKS` tasks (default 1000). Every recycling is logged
and counted (`swarmq_recyclings_total{kind="compute_child",reason=...}`) and
emits the `WORKER_RECYCLING` signal.

## Sizing

- **CPU:** slots = CPUs assigned to the worker. Pillow and BLAS scale only up
  to the core count; each child gets one thread (`OMP_NUM_THREADS`,
  `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`,
  `MAGICK_THREAD_LIMIT` = `SWARMQ_COMPUTE_THREAD_LIMIT`). Values you set in the
  environment win.
- **Memory:** budget ≈ slots × recycle threshold + loop process + forkserver +
  one replacement child, plus idle DI children when the workload mixes DI and
  non-DI tasks. Derive the RSS threshold from your cgroup limit and leave room
  for the rolling replacement.
- **Latency vs throughput:** a single image gets slower with one thread per
  child; throughput at full load stays the same. Raise
  `SWARMQ_COMPUTE_THREAD_LIMIT` for latency-critical queues.
- **Connections:** N slots mean N connection pools. APP-scoped singletons live
  as long as their child.

## Behaviour changes for task authors

- After a kill (`hard_timeout`, cancel, child death) REQUEST-scope finalizers
  run only if the child reacts to SIGTERM within one second. An open database
  transaction is rolled back by the server when the connection drops;
  autocommit sequences and external side effects are not. Slice tasks so a
  kill before the last commit leaves no visible half-state.
- `hard_timeout` covers fork, DI setup and all three lifecycle methods, not
  only `run()`. `soft_timeout` cannot interrupt a blocking body.
- Return values and exceptions cross the process boundary via pickle. A
  return value that cannot be pickled is stored as an error result (no retry),
  exactly like a non-serializable value today. Exceptions keep their type
  (`Retry`, `NoRetry`, `retry_exceptions`) when importable.
- `FromDishka[X] | None = None` never injected anything; since 0.5.0 the worker
  refuses to start with such an annotation (`ConfigurationError`) in every mode.
  The same applies to an alias (`Depends[X] | None`), also under
  `from __future__ import annotations`.

### Fire-and-forget and an unavailable compute slot

A fire-and-forget message is acknowledged before the task body is dispatched.
If the compute child for that message never completes its ready handshake
(for example because the forkserver died), there is no message left to
redeliver: the worker logs `compute_slot_unavailable_fire_and_forget_dropped`
and the message is lost. At-least-once messages are redelivered instead.
Repeated handshake failures mark the pool unhealthy; `/ready` then reports
`compute pool unhealthy` so the supervisor or orchestrator can replace the
worker.

## Environment and startup checks

- `MALLOC_TRIM_THRESHOLD_` (131072) and `MALLOC_ARENA_MAX` (2) are set for the
  compute children before the forkserver starts. They stop glibc from holding
  freed memory; measured cost about 14 % runtime per image. Override with
  `SWARMQ_MALLOC_TRIM_THRESHOLD` / `SWARMQ_MALLOC_ARENA_MAX`.
- The forkserver preloads the modules of your registered tasks and must never
  compute at import time: after warm-up it has to have exactly one thread,
  otherwise the worker refuses to start (exit `78`). The message names the
  extra threads and the likely cause: SwarmQ's opt-in log queue
  (`swarmq-log-lsnr`), a logging `QueueListener` your app started at import
  time, or else a BLAS/OpenMP computation. SwarmQ itself starts no log
  thread in the forkserver. If the forkserver is restarted silently, the
  same check runs again before the next compute child gets work; with more
  than one thread the worker logs `forkserver_restarted_multithreaded`,
  rejects the forkserver and reports the pool as unhealthy on `/ready`.
  It then logs `compute_fork_source_rejected`, stops consuming and shuts
  down gracefully; the running message goes back to the queue without a
  retry count. Under `swarmq worker` the worker subprocess exits with code
  `71` and the supervisor restarts it with a fresh forkserver. With an
  embedded CPU pool, `Worker.start()` raises
  `swarmq.supervision.ForkSourceRejected` after the shutdown, and your app
  decides how to continue — SwarmQ does not end your process.
- Logging that your task modules set up at import time follows the same
  "app wins" rule as in the IO pool; `logging.basicConfig()` has no effect
  there ([Logging](../reference/logging.md#when-a-task-module-configures-logging)).
- `--pool cpu` cannot be combined with `--autoscale` (later stage) and needs
  a POSIX platform with `forkserver`. Memory and thread checks read `/proc`.
- RabbitMQ's `consumer_timeout` (default 30 min) bounds any single delivery.
  The worker warns at startup when a task's `hard_timeout` exceeds
  `SWARMQ_CONSUMER_TIMEOUT_S`; raise both together for long jobs. When a
  task runs longer anyway, the broker closes the channel (up to 4.2) or
  cancels the consumer (from 4.3, quorum queues). From 0.11.0 the
  consumer watch registers the consumer again, and the execution guard
  keeps the redelivered message from running next to the original; its
  running markers are renewed by the main process, not by the compute
  child. Details: [consumer timeout](../explanation/delivery-semantics.md#consumer-timeout).
- `/ready` reports a `consumers` entry: not ready once a consumed queue
  has had no active consumer for `SWARMQ_CONSUMER_READY_GRACE_S`
  (default 30 s). Never use `/ready` as a liveness probe.
- `/ready` reports the forkserver as an additional dependency.

## Relationship to `--processes`

`--processes N` starts N loop processes under the supervisor, each with its
own forkserver and its own slots; the slot default is divided by N. See
[Configuration](../reference/configuration.md) for every variable and the
[CLI reference](../reference/cli.md) for the flags.
