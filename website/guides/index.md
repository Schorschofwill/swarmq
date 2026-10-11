# How-to guides

Goal-oriented recipes for common tasks. Each guide answers one
question — *"how do I do X?"* — with the steps, a code snippet,
and a verification hint. Read the guide you need; skip the rest.

Use the guides when you already know what you're trying to
accomplish. For a structured walkthrough from scratch, see the
[tutorials](../tutorials/index.md). For exhaustive parameter
listings, see the [reference](../reference/index.md).

## Task definition and scheduling

- [Defining tasks](defining-tasks.md) — the `Task` subclass
  shape, allowed argument types, lifecycle hooks, and the
  anti-patterns that bite in production.
- [Scheduling and retries](scheduling-and-retries.md) — immediate
  vs deferred (`delay`, `eta`), cron schedules, retry backoff,
  `Retry` / `NoRetry`, timeouts, and `max_age`.

## Cross-cutting behaviour

- [Middleware](middleware.md) — six hooks around every task
  lifecycle event.
- [Signals](signals.md) — register async handlers on specific
  lifecycle events (`TASK_FAILED`, `TASK_RATE_LIMITED`, etc.).
- [Rate limiting](rate-limiting.md) — global sliding-window limits
  declared as `"100/m"` on `TaskInfo`.
- [Locking and deduplication](locking-and-dedup.md) — serialise
  executions (lock) or collapse redundant enqueues (unique task).
- [Workflows](workflows.md) — compose tasks into `chain`, `group`
  and `chord`, start them with `app.apply()` and wait for, inspect or
  cancel them through a `WorkflowHandle`.

## Operations

- [Deploying and upgrading](deployment.md) — rollout order, grace
  periods, vhost and namespace, permissions and policies, rollback
  rules; the upgrade path from 0.11 to 0.12.
- [Dead letter queues](dead-letter-queues.md) — inspect, retry,
  and purge DLQ'd messages with `swarmq dlq`.
- [CPU mode](cpu-mode.md) — `--pool cpu` runs task bodies in
  compute children with real killability; sizing, recycling and the
  supervisor that is now the default for every worker.
- [Hot reload (dev-only)](hot-reload.md) — `--reload` for the
  local developer loop; never for production.
- [Testing tasks](testing-tasks.md) — unit-test `Task.run()`
  without a broker; integration-test with the full stack.
