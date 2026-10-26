# CPU mode (`--pool cpu`)

SwarmQ is pure asyncio. A synchronous Pillow, numpy or ImageMagick call
blocks the event loop and with it every concurrent task of the worker,
and a `hard_timeout` never reaches a blocked loop. CPU mode moves task
bodies out of the loop process into short-lived **compute children**
forked from a pre-warmed `forkserver`.

```bash
swarmq worker --module myapp.tasks --pool cpu --queues images
```

`SWARMQ_POOL=cpu` does the same without the flag. An embedded app uses
`SwarmQConfig(pool="cpu")` or `SwarmQ(..., pool="cpu")`.

## What changes

| Concern | IO worker (default) | CPU mode |
|---|---|---|
| Where `run()` executes | in the worker's event loop | in a compute child, one per task |
| `--concurrency` | tasks in flight | compute slots; default = CPUs assigned to the worker (affinity ∩ cgroup quota), divided by the number of worker processes (see [below](#relationship-to-processes)). An embedded CPU pool without `concurrency` uses the assigned CPUs too (since 0.12.3, before: 10); an explicit `concurrency` always wins |
| `hard_timeout` | asyncio cancel; cannot interrupt a blocked loop | kills the child's process group (SIGTERM, 1 s grace, SIGKILL); the task follows the usual retry/DLQ path |
| Cancel | asyncio cancel | kills the child; the task ends as cancelled, no retry |
| Gates, middleware, signals, ack | loop process | loop process, unchanged |
| `FromDishka[...]`, `Annotated[X, FromComponent(...)]`, aliases | resolved in the loop | resolved in the child; the container object is built at import time, never resolved in the forkserver. The child imports the task module again, so the annotations must be resolvable at class definition there too (see [dependency injection](../reference/api.md#dependency-injection-dishka)) |

Tasks **without** dependency injection get a fresh child per task:
nothing survives a task, so no leak survives either. Tasks **with**
`FromDishka` reuse a warm child per slot, which is recycled when its RSS
after a task exceeds `SWARMQ_RECYCLE_RSS_MB` (default 250 MB in CPU
mode) or after `SWARMQ_RECYCLE_MAX_TASKS` tasks (default 1000). Every
recycling is logged, counted
(`swarmq_recyclings_total{kind="compute_child",reason=...}`) and emits
the `WORKER_RECYCLING` signal.

## Sizing

- **CPU:** slots = CPUs assigned to the worker. Pillow and BLAS scale
  only up to the core count; each child gets one thread
  (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`,
  `NUMEXPR_NUM_THREADS`, `MAGICK_THREAD_LIMIT` =
  `SWARMQ_COMPUTE_THREAD_LIMIT`, default 1). Values you set in the
  environment win.
- **Memory:** budget ≈ slots × recycle threshold + loop process +
  forkserver + one replacement child, plus idle DI children when the
  workload mixes DI and non-DI tasks. Derive the RSS threshold from your
  cgroup limit and leave room for the rolling replacement.
- **Latency vs throughput:** a single image gets slower with one thread
  per child; throughput at full load stays the same. Raise
  `SWARMQ_COMPUTE_THREAD_LIMIT` for latency-critical queues.
- **Connections:** N slots mean N connection pools. APP-scoped
  singletons live as long as their child.

## Behaviour changes for task authors

- After a kill (`hard_timeout`, cancel, child death) REQUEST-scope
  finalizers run only if the child reacts to SIGTERM within one second.
  The database server rolls back an open transaction when the
  connection drops; autocommit sequences and external side effects stay.
  Slice tasks so that a kill before the last commit leaves no visible
  half-state.
- `hard_timeout` covers fork, DI setup and all three lifecycle methods,
  not only `run()`. `soft_timeout` cannot interrupt a blocking body.
- Return values and exceptions cross the process boundary via pickle. A
  return value that cannot be pickled is stored as an error result (no
  retry), like a value that cannot be serialised in the IO pool.
  Exceptions keep their type (`Retry`, `NoRetry`, `retry_exceptions`)
  when importable; `TaskError.error_class` names the class the child
  reported.
- `FromDishka[X] | None = None` never injected anything; the worker
  refuses to start with such an annotation (`ConfigurationError`) in
  every mode. The same applies to an alias (`Depends[X] | None`), also
  under `from __future__ import annotations`.

### Fire-and-forget and an unavailable compute slot

A fire-and-forget message is acknowledged before the task body is
dispatched. If the compute child for that message never completes its
ready handshake (for example because the forkserver died), there is no
message left to redeliver: the worker logs
`compute_slot_unavailable_fire_and_forget_dropped` and the message is
lost. At-least-once messages are redelivered instead. Repeated
handshake failures mark the pool unhealthy; `/ready` then reports
`compute pool unhealthy` so the supervisor or orchestrator can replace
the worker.

## Environment and startup checks

- **glibc:** `MALLOC_TRIM_THRESHOLD_` (131072) and `MALLOC_ARENA_MAX`
  (2) are set for the compute children before the forkserver starts.
  They stop glibc from holding freed memory, at a measured cost of
  about 14 % runtime per image. Override with
  `SWARMQ_MALLOC_TRIM_THRESHOLD` / `SWARMQ_MALLOC_ARENA_MAX`.
- **No computing at import time:** the forkserver preloads the modules
  of your registered tasks and must have exactly one thread after
  warm-up, otherwise the worker refuses to start (exit `78`). The
  message names the extra threads and the likely cause: SwarmQ's opt-in
  log queue (`swarmq-log-lsnr`), a logging `QueueListener` your app
  started at import time, or a BLAS/OpenMP computation. SwarmQ itself
  starts no log thread in the forkserver.
- **Silent forkserver restart:** the same check runs again before the
  next compute child gets work. With more than one thread the worker
  logs `forkserver_restarted_multithreaded`, rejects the forkserver,
  reports the pool as unhealthy on `/ready`, logs
  `compute_fork_source_rejected`, stops consuming and shuts down
  gracefully; the running message goes back to the queue without a
  retry count. Under `swarmq worker` the worker process exits with code
  `71` and the supervisor restarts it with a fresh forkserver. With an
  embedded CPU pool, `Worker.start()` raises
  `swarmq.supervision.ForkSourceRejected` after the shutdown, and your
  app decides how to continue; SwarmQ does not end your process.
- **Logging:** logging that your task modules set up at import time
  follows the same "app wins" rule as in the IO pool;
  `logging.basicConfig()` has no effect there
  ([Logging](../reference/logging.md#when-a-task-module-configures-logging)).
- **Platform:** `--pool cpu` needs a POSIX platform with `forkserver`
  and cannot be combined with `--autoscale`. Memory and thread checks
  read `/proc`.
- **Long tasks:** RabbitMQ's `consumer_timeout` (default 30 min) bounds
  any single delivery. The worker warns at startup when a task's
  `hard_timeout` exceeds `SWARMQ_CONSUMER_TIMEOUT_S`; raise both
  together for long jobs. When a task runs longer anyway, the broker
  closes the channel (up to 4.2) or cancels the consumer (from 4.3,
  quorum queues). The consumer watch registers the consumer again, and
  the execution guard keeps the redelivered message from running next
  to the original; its running markers are renewed by the loop process,
  not by the compute child. Details:
  [consumer timeout](../explanation/delivery-semantics.md#consumer-timeout).
- **Readiness:** `/ready` has a `compute` entry for the forkserver and
  the pool, next to `consumers`. The `consumers` entry turns red once a
  consumed queue has had no active consumer for
  `SWARMQ_CONSUMER_READY_GRACE_S` (default 30 s), and at once when a
  consumed queue was deleted (since 0.14.0). Never use `/ready` as a
  liveness probe; use `/health`.

## Relationship to `--processes`

`--processes N` starts N loop processes under the supervisor, each with
its own forkserver and its own slots. The default slot count is the
assigned CPUs divided by N, or by `--max-processes` when the control
channel is enabled, so a setpoint at the headroom never overbooks the
CPUs. With `--pool cpu` the start refuses a `--max-processes` above the
assigned CPUs, and an explicit `--concurrency` whose product with
`--max-processes` exceeds them.

`swarmq worker --pool cpu --check` prints the resulting `slots`. See
[Configuration](../reference/configuration.md) for every variable and
the [CLI reference](../reference/cli.md) for the flags.
