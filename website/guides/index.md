# How-to guides

Goal-oriented recipes: each guide answers one *"how do I do X?"*
question with the steps, a code snippet and a way to verify the result.
Pick the guide you need and skip the rest.

New to SwarmQ? Start with the [tutorials](../tutorials/index.md). Every
parameter and default is listed in the [reference](../reference/index.md).

## Tasks and scheduling

<div class="grid cards" markdown>

-   **[Defining tasks](defining-tasks.md)**

    ---

    Write a `Task` subclass with the right `TaskInfo`, argument types and
    lifecycle hooks, and avoid the anti-patterns that bite in production.

-   **[Scheduling and retries](scheduling-and-retries.md)**

    ---

    Run a task now, later (`delay`, `eta`) or on a cron, and control
    retries, backoff, timeouts, expiry and cancellation.

-   **[Workflows](workflows.md)**

    ---

    Compose tasks into `chain`, `group` and `chord`, start them with one
    call and wait for, inspect or cancel the whole run.

-   **[Testing tasks](testing-tasks.md)**

    ---

    Unit-test `run()` without a broker, then cover the integration with
    the full stack.

</div>

## Cross-cutting behaviour

<div class="grid cards" markdown>

-   **[Middleware](middleware.md)**

    ---

    Apply logging, tracing or metrics uniformly through six hooks around
    enqueue, execution, errors and retries.

-   **[Signals](signals.md)**

    ---

    React to single lifecycle events such as `TASK_FAILED` or a leader
    change with async handlers.

-   **[Rate limiting](rate-limiting.md)**

    ---

    Enforce a global quota such as `"100/m"` across every worker that
    shares the Valkey.

-   **[Locking and deduplication](locking-and-dedup.md)**

    ---

    Keep the same work from running in parallel (lock) or from being
    enqueued twice (unique task).

</div>

## Operations

<div class="grid cards" markdown>

-   **[Deploying and upgrading](deployment.md)**

    ---

    Roll out to production and upgrade to 0.14 without losing or doubling
    tasks: checks, order, grace periods, permissions and rollback.

-   **[Dead letter queues](dead-letter-queues.md)**

    ---

    Find out why messages landed in `{queue}.XQ`, then list, retry or
    purge them with `swarmq dlq` and bound how long they stay.

-   **[CPU mode](cpu-mode.md)**

    ---

    Run blocking, CPU-heavy task bodies in killable compute children with
    `--pool cpu`, and size slots and memory for them.

-   **[Hot reload (dev-only)](hot-reload.md)**

    ---

    Restart workers automatically on every saved change during local
    development with `--reload`.

</div>
