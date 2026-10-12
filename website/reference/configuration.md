# Configuration

SwarmQ has two configuration surfaces: **environment variables** that
the worker reads at startup (and the CLI also honours), and
**`TaskInfo` fields** declared per task class.

Precedence depends on how SwarmQ runs:

- **`swarmq worker`**: CLI flag, then env var, then default, for every
  worker option. The supervisor resolves the values once and hands them
  to its worker subprocesses and compute children, which do not read
  the environment again; `swarmq worker --check` validates the same
  resolved values.
- **Embedded `SwarmQ(...)`**: `SwarmQConfig` keyword arguments first,
  then `SWARMQ_*` env vars override them; defaults fill the rest.

## Environment variables

### Required

| Variable | Purpose |
|---|---|
| `SWARMQ_BROKER_URL` | AMQP URL for RabbitMQ, e.g. `amqp://guest:guest@localhost:5672/`. |
| `SWARMQ_BACKEND_URL` | Valkey/Redis URL for results and shared state, e.g. `valkey://localhost:6379/0`. |

The CLI prints a descriptive error if either is missing; see the
[error catalog](errors.md#configurationerror).

### Worker behaviour

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_QUEUES` | `default` | Comma-separated queue names the worker consumes. |
| `SWARMQ_CONCURRENCY` | `10` (io) / assigned CPUs (cpu) | Maximum tasks in flight per worker process. With `SWARMQ_POOL=cpu` the number of compute slots; see [`--concurrency`](cli.md#options). |
| `SWARMQ_RESULT_TTL` | `86400` | Seconds that stored results live in Valkey. |
| `SWARMQ_RETRY_MAX_DELAY` | `3600.0` | Cap on retry backoff in seconds. |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` | Total shutdown budget in seconds from SIGTERM (since 0.12; before, only the drain): draining in-flight tasks, releasing locks, the scheduler handover, closing connections and, under the supervisor, its SIGTERM/SIGKILL escalation all fit in it. The drain ends 5 s (at most half) before the end; under the supervisor the escalation ends 2 s before it. Set Kubernetes `terminationGracePeriodSeconds` and Docker `stop_grace_period` to at least this value plus 15 s ([shutdown budget](../explanation/delivery-semantics.md#shutdown-budget)). |
| `SWARMQ_PUBLISH_TIMEOUT_S` | `120.0` | Upper bound on a complete publish, including internal retries. A publish that exceeds it fails with a broker connection error instead of waiting indefinitely for a confirm that a silently dead transport will never deliver. During graceful shutdown the shorter of this value and the remaining shutdown grace period applies. |
| `SWARMQ_DELIVERY_LIMIT` | `20` | Delivery budget for quorum queues (`x-delivery-limit`); ignored for classic queues. Once a message has been delivered this many times, the broker itself dead-letters it — the safety net that stops a task which reliably kills the process (SIGSEGV/OOM) from being redelivered forever without ever reaching `max_retries`. The default covers three to four task-level retries per delivery plus headroom for broker hiccups. That headroom is narrower since the publish deadline: a publish that breaks its deadline inside the worker redelivers the in-flight message instead of acking it, and each round costs one delivery. Before each such hand-back the worker waits up to 5 s (growing with the delivery count), so the default budget lasts more than a minute. The worker also counts `x-acquired-count` (RabbitMQ 4.3, which does not count a requeue in `x-delivery-count`). Raise it if you must ride out longer broker outages; lower it to force a tighter delivery window. Existing queues keep the limit they were created with; the worker then dead-letters at the smaller of both values minus one ([effective delivery limit](../explanation/delivery-semantics.md#effective-delivery-limit)). |
| `SWARMQ_QUEUE_TYPE` | `quorum` | `quorum` (recommended) or `classic` RabbitMQ queue type. Only quorum queues carry `x-delivery-limit`; without it a message that keeps hanging workers is redelivered without bound (the worker warns at startup). Changing it for existing queues fails with `QueueArgumentsMismatch` (exit 78); [migrate](../explanation/delivery-semantics.md#changing-queue-arguments) the queues instead. |
| `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY` | `auto` | Dead-lettering of quorum queues. `auto`: new queues get `x-dead-letter-strategy=at-least-once` and `x-overflow=reject-publish` (not `{queue}.XQ`), existing queues keep their arguments and one WARNING `queue_dead_letter_at_most_once` per process lists those without at-least-once. `at-least-once`: as `auto`, but an existing queue without at-least-once (strategy and `x-overflow=reject-publish`) fails with `QueueArgumentsMismatch` (exit 78); not allowed with `SWARMQ_QUEUE_TYPE=classic` (`ConfigurationError`). `legacy`: the arguments of 0.11 (at-most-once), no warning ([dead-lettering guarantee](../explanation/delivery-semantics.md#dead-lettering-guarantee)). |
| `SWARMQ_BROKER_PUBLISH_CONNECTION` | `separate` | `separate`: a worker consumes and acknowledges over a second RabbitMQ connection, opened by its first `consume()`; publishes (retry, dead-letter and deferral copies, workflow steps, cron fires) keep the first one. During a resource alarm (memory, disk) RabbitMQ blocks only connections that publish, so acknowledgements keep flowing. A producer that never consumes keeps one connection. `shared`: one connection as before 0.12. With an injected `connection=`/`connection_pool=` it is always `shared` ([broker resource alarm](../explanation/architecture.md#failure-domains)). |
| `SWARMQ_POOL` | `io` | `io` or `cpu` — see the [CPU mode guide](../guides/cpu-mode.md). |
| `SWARMQ_RECYCLE_RSS_MB` | unset (`250` in CPU mode) | Recycle a process after a task once its RSS exceeds this many MB. IO worker: opt-in self-recycling; CPU mode: threshold for reused DI children. |
| `SWARMQ_RECYCLE_MAX_TASKS` | unset (`1000` in CPU mode) | Recycle a process after this many tasks (second line behind the RSS threshold). |
| `SWARMQ_HEARTBEAT_INTERVAL_S` | `5` | How often the worker writes its liveness timestamp for the supervisor. |
| `SWARMQ_HEARTBEAT_TIMEOUT_S` | `60` | After this long without a heartbeat the supervisor replaces the worker (SIGTERM, then SIGKILL). Must exceed the interval and every legitimate loop stall; keep it below RabbitMQ's heartbeat detection. |
| `SWARMQ_CONSUMER_TIMEOUT_S` | `1800` | The broker's `consumer_timeout`; the worker warns when a task's `hard_timeout` exceeds it. |
| `SWARMQ_CONSUMER_WATCH_INTERVAL_S` | `5.0` | Seconds between two checks of the [consumer watch](../explanation/delivery-semantics.md#consumer-timeout): a consumer the broker ended (`basic.cancel` from RabbitMQ 4.3, channel close up to 4.2) is cancelled and registered again within two intervals. Must be `> 0`. |
| `SWARMQ_CONSUMER_READY_GRACE_S` | `30.0` | `/ready` reports the worker not ready once a consumed queue has had no active consumer for longer than this. Must be `> 0`. Never use `/ready` as a liveness probe. |
| `SWARMQ_EXECUTION_GUARD` | `true` | [Execution guard](../explanation/delivery-semantics.md#execution-guard): a redelivered message does not run while the same execution runs elsewhere, after it finished, or after a retry superseded it. `false` switches it off. |
| `SWARMQ_EXECUTION_GUARD_TTL_S` | `60.0` | TTL of the running marker; the worker renews it every third of the TTL while the task runs. After a crash the redelivery runs once the marker expired. Must be `> 0`; raise it if task code may block the event loop for longer. |
| `SWARMQ_EXECUTION_GUARD_DONE_TTL_S` | `max(retry_max_delay, consumer_timeout_s) + 300` | How long done and superseded markers live. Must be at least `SWARMQ_EXECUTION_GUARD_TTL_S`. |
| `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` | `3` | After this many expirations of the same execution without release, the delivery goes into `{queue}.XQ` as `WorkerHungError`. Must be `> 0`. |
| `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` | `604800` | Backlog the markers survive: once a redelivery of an execution was deferred (it comes back behind the queue's backlog) or an ack failed, the execution's state lives at least this long (or `max_age + 300`, if longer); the done marker of a replay lives this long when `SWARMQ_DEAD_LETTER_TTL_S` is `none`; and the completed marker of a schedule fire is capped here. Must be `> 0` ([marker retention](../explanation/delivery-semantics.md#execution-guard)). |
| `SWARMQ_DEAD_LETTER_TTL_S` | `604800` | Seconds a message the worker dead-letters stays in `{queue}.XQ` ([retention](../guides/dead-letter-queues.md#retention-dead-lettered-messages-expire)); reading does not extend it. `none` keeps messages without limit. Must be `> 0`. The `swarmq dlq` CLI keeps each message's own retention (`x-swarmq-dead-letter-ttl-ms`) and uses this variable only for old stock without it. |
| `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES` | `true` | `false` stores the error result of a terminal task failure (`NoRetry`, exception outside `retry_exceptions`, retries exhausted) and acknowledges the message instead of dead-lettering it ([details](../guides/dead-letter-queues.md#switching-off-dead-lettering-of-task-failures)). `TaskInfo.dead_letter` wins. |
| `SWARMQ_LOCK_WAIT_MAX_S` | `21600` | Upper limit in seconds for how long a message waits for a held task lock, measured from its first conflict (`x-swarmq-lock-wait-since`); afterwards it is dead-lettered with `LockWaitExceeded`. Before that only a stalled holder (same holder longer than its TTL + 60 s) or `max_age` ends the wait. Must be `> 0` and finite ([lock acquisition](../guides/locking-and-dedup.md#lock-acquisition-behaviour)). |
| `SWARMQ_VALKEY_EVICTION_CHECK` | `strict` | What a worker does when Valkey may evict swarmq keys ([Valkey requirements](#valkey-requirements)): `strict` refuses to start with exit `78` under an evicting `maxmemory-policy` (anything but `noeviction` with a `maxmemory` limit, or with an unknown limit), `warn` logs `valkey_eviction_policy_unsafe` and starts, `off` does not check at all (no query, no warning). A policy that cannot be read (ACL forbids `CONFIG` and `INFO`) never blocks, it only logs `valkey_eviction_policy_unknown`. Case does not matter. Also read by `swarmq check` and by every other `ValkeyBackend` (CLI, producers), where only `off` changes anything. |
| `SWARMQ_NAMESPACE` | *unset* | Namespace of this app on a shared Valkey ([namespace](#namespace-several-apps-on-one-valkey)): every Valkey key, Pub/Sub channel, the scheduler leader and the control channel move from `sq:…` to `<namespace>:sq:…`. 1–64 characters `a-z 0-9 _ . -` (lowercase only: Valkey keys are case-sensitive, `App1` and `app1` would be two apps), starting with a letter or digit, not `sq`; anything else is a configuration error (exit `78`). Empty means unset. Queue names stay unchanged. Every `ValkeyBackend` (worker, producer, CLI, control channel) reads it, unless it is built with `namespace=` ([where the namespace comes from](#where-the-namespace-comes-from)). Set it only for a new app or after [moving the app](#moving-an-app-into-a-namespace), and only once every component runs 0.12. |
| `SWARMQ_UNKNOWN_TASK_GRACE_S` | `900.0` | Grace period in seconds for a task name this worker does not register (rolling deploy: producers ahead of the workers). Within it the worker defers the message with backoff (1 s, then as long as it has waited so far, at most 60 s per round) and keeps the first contact in `x-swarmq-unknown-since`; afterwards it dead-letters the message with an error result and `TASK_REJECTED` (`unregistered_task`). `0` switches the grace period off (dead-letter at first contact). Must be `>= 0` ([unregistered tasks](../guides/dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)). |
| `SWARMQ_MALLOC_TRIM_THRESHOLD` | `131072` | `MALLOC_TRIM_THRESHOLD_` for compute children (glibc growth). |
| `SWARMQ_MALLOC_ARENA_MAX` | `2` | `MALLOC_ARENA_MAX` for compute children. |
| `SWARMQ_COMPUTE_THREAD_LIMIT` | `1` | Threads per compute child (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`, `MAGICK_THREAD_LIMIT`). |
| `SWARMQ_CANCEL_POLL_INTERVAL` | `1.0` | Seconds between in-task cancel-flag checks. |
| `SWARMQ_BACKEND_OP_TIMEOUT` | `5.0` | Upper bound in seconds on individual Valkey ops in the worker pipeline: cancel check, schedule-fire dedup and claim, rate-limit check, lock acquire, the result read of a cancelled workflow step, result writes, the result notification after a pre-start cancel, and the completion or release of a schedule-fire claim. A check before execution that runs into it counts as an unreachable backend and defers the message instead of waiting for the Valkey client's socket timeout. Must be `> 0`. |
| `SWARMQ_BACKEND_DEFER_BASE_DELAY` | `1.0` | Seconds a message waits after its first deferral when a check before execution (schedule-fire dedup, cancel check, rate limit, lock) cannot reach the backend. Each further deferral of the same message doubles the wait. Every wait varies randomly by ±20 % so messages deferred together do not return together. The message goes back through the delay queue as a new message, so a long backend outage neither consumes `max_retries` nor the delivery limit. |
| `SWARMQ_BACKEND_DEFER_MAX_DELAY` | `60.0` | Upper bound for the doubled wait in seconds, before the ±20 % variation (so a single wait can be up to 1.2 times this value); must be at least `SWARMQ_BACKEND_DEFER_BASE_DELAY`. Bounds how long a message can wait after the backend is back. |
| `SWARMQ_BURST_MODE` | `false` | If `true`, worker exits after `SWARMQ_BURST_IDLE_TIMEOUT` seconds of queue silence. |
| `SWARMQ_BURST_IDLE_TIMEOUT` | `5.0` | Seconds of idle needed before burst exits. |
| `SWARMQ_PRIORITIES` | `false` | If `true`, auto-declare and consume the `.high`/`.normal`/`.low` triplet for every queue. |
| `SWARMQ_MAX_MESSAGE_BODY_BYTES` | `16777216` | Upper bound for an incoming task message body (16 MiB). Must be `> 0`. A larger body is not decoded: the worker logs `message_body_too_large` and routes the message to the DLQ (`TASK_REJECTED` with reason `body_too_large`). |

### Logging

These variables steer the logging that `swarmq worker` sets up in its
own processes. An app that embeds `SwarmQ(...)` configures logging
itself; there the variables only affect the
[opt-in log queue](logging.md#opt-in-log-queue), the lifecycle level of
an embedded worker and the compute children of an embedded CPU pool.
Details, recipes and the event catalog:
[Logging reference](logging.md).

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root level of SwarmQ's processes; with `SWARMQ_LOG_QUEUE` the level of the `swarmq` logger. |
| `SWARMQ_LOG_FORMAT` | `human` (CLI) | `human` or `json`. Wins over `SWARMQ_LOG_JSON`. |
| `SWARMQ_LOG_JSON` | `true` | Alias for the format: `true`/`1`/`yes` means `json`, anything else `human`. The default `true` is the `SwarmQConfig.log_json` default; `swarmq worker` defaults to `human`. |
| `SWARMQ_LOG_CONFIG` | unset | Path of a `dictConfig` file (JSON; YAML with PyYAML) used instead of the built-in setup. Not combinable with an explicit format or `SWARMQ_CONFIGURE_LOGGING=false`. |
| `SWARMQ_CONFIGURE_LOGGING` | `true` | `false` leaves logging in every SwarmQ process to your app. |
| `SWARMQ_LOGGER_LEVELS` | unset | Comma-separated `NAME=LEVEL` pairs, e.g. `aio_pika=WARNING,valkey=ERROR`. |
| `SWARMQ_LOG_QUEUE_SIZE` | `10000` | Capacity of the in-process log queue. |
| `SWARMQ_LOG_LIFECYCLE_LEVEL` | `DEBUG` | Level of `task_received` and `task_started`. |
| `SWARMQ_LOG_DRAIN_TIMEOUT` | `5` | Upper bound in seconds for every flush of the log queues (at exit, before a reload, before each compute result, in `SwarmQ.__aexit__`); records not written in time count as dropped. Greater than 0 and at most `threading.TIMEOUT_MAX`. See [bounded flushing](logging.md#bounded-flushing). |
| `SWARMQ_LOG_QUEUE` | `false` | Embedded apps: route `swarmq.*` events through a SwarmQ background thread to your root handlers ([opt-in log queue](logging.md#opt-in-log-queue)). |

The supervisor of `swarmq worker` resolves these values once (CLI flag
before env var) and hands the result to its worker subprocesses and
compute children, which do not read the environment again. The same
CLI > env > default order applies to every other worker option with a
flag (see [CLI reference](cli.md)). Invalid values end `swarmq worker`
with exit `78`.

### Scheduler

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_SCHEDULER` | `on` | `off` keeps this worker out of the scheduler leader election (also `swarmq worker --no-scheduler`). It still reports its crons, version and queues, so the leader does not prune its crons. Crons, `delay`/`eta` tasks and oneshots fire only while another worker on the same Valkey and namespace runs with `on`; with `off` everywhere nothing fires. Any other value is a `ConfigurationError` (exit `78`). Workers before this release ignore the variable ([turning the scheduler off](../guides/scheduling-and-retries.md#turning-the-scheduler-off-on-single-workers)). |
| `SWARMQ_SCHEDULER_POLL_INTERVAL` | `1.0` | Seconds between scheduler tick polls. |
| `SWARMQ_SCHEDULER_HEARTBEAT_INTERVAL` | `10.0` | Seconds between leader heartbeat renewals. |
| `SWARMQ_SCHEDULER_LOCK_TTL` | `30.0` | Leader lock TTL; must be `≥ 2 × heartbeat_interval`. |
| `SWARMQ_SCHEDULER_DEDUP_TTL` | `max(60, retry_max_delay + 300)` | Dedup TTL for scheduler-originated dispatches; the lower bound of the completed marker, whose TTL follows the time the fire waited in the queue (`2 × wait + 300`, capped at `max_age + 300` and `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S`). |
| `SWARMQ_SCHEDULER_INFLIGHT_TTL` | `300.0` | TTL of the schedule-fire claim a worker holds while a scheduled task runs. A claim that could not be released blocks a redelivery of the same fire as a duplicate for at most this long. |
| `SWARMQ_SCHEDULER_TIMEZONE` | *unset* (UTC) | IANA zone for cron expressions of tasks without their own `TaskInfo.timezone`, e.g. `Europe/Berlin`. `UTC` and `Etc/UTC` mean UTC. An unknown zone is a configuration error. Change it in the same deploy as the expressions it affects; prefer the zone per task ([time zones](../guides/scheduling-and-retries.md#time-zones)). |
| `SWARMQ_SCHEDULER_SEEN_INTERVAL_S` | `60.0` | Seconds between [cron reports](../guides/scheduling-and-retries.md#cron-reports-and-the-leader): every worker reports its static crons at start and then at this interval; the leader adopts reported crons at the same interval. A report stays live for three intervals. Must be `> 0`; use the same value in every app on one Valkey. |
| `SWARMQ_SCHEDULER_PRUNE` | `on` | What the leader does with a static cron that was reported once and that no worker has reported for `SWARMQ_SCHEDULER_PRUNE_AFTER_S`: `on` removes it, `log` only logs `scheduler_cron_prune_skipped`, `off` does nothing. The scheduler leader's value applies, whichever app it belongs to: set the same value in every app on the Valkey. Requires every app on the Valkey to run 0.10.0 or later ([removing crons](../guides/scheduling-and-retries.md#removing-crons-no-app-reports-anymore)). |
| `SWARMQ_SCHEDULER_PRUNE_AFTER_S` | `86400.0` | Period in seconds after which a silent static cron is removed. Must be at least `3 × SWARMQ_SCHEDULER_SEEN_INTERVAL_S`. Cron reports are kept for twice this period, at least 7 days. The scheduler leader's value applies: set the same value in every app on the Valkey. |
| `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` | `86400.0` | How long, in seconds, the leader keeps retrying a deferred task (`delay=`/`eta=`) whose queue does not exist, counted from the first unroutable attempt; it retries every 60 s and then drops the task with an error result. Must be > 0. The scheduler leader's value applies ([missing queues](../guides/scheduling-and-retries.md#missing-queues)). |
| `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` | `SWARMQ_SCHEDULER_LOCK_TTL` | The scheduler leader releases the leader lock after at least 2 failed publishes (`BrokerConnectionError`, not counting `UnroutableMessage`, `MessageHeadersTooLarge`, `BrokerPublishBlocked`, `PublishRejected` and `PublishIndeterminate`) spread over at least this many seconds, with no successful publish in between and no gap longer than this between two failures; a new term as leader starts a new series; it then does not apply again for `2 × SWARMQ_SCHEDULER_LOCK_TTL`, so a worker whose broker works takes over (it emits `SCHEDULER_LEADER_FAILOVER`). `0` disables the step-down. Must be `≥ 0`. Set `0` while apps older than 0.12 share the Valkey without namespace ([namespace](#namespace-several-apps-on-one-valkey)). |

### Observability

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_METRICS_PORT` | *unset* | **Base port** for `/metrics`. The worker in slot `s` serves on `SWARMQ_METRICS_PORT + s`; an embedded worker without a slot serves on the base port itself. Requires `pip install swarmq[metrics]`. Must be `1..65535`. |
| `SWARMQ_METRICS_HOST` | `127.0.0.1` | Bind address for the metrics server. Set `0.0.0.0` when Prometheus scrapes from another host or container. |
| `SWARMQ_HEALTH_PORT` | *unset* | **Base port** for `/health` and `/ready`, same slot offset. Must be `1..65535`. |
| `SWARMQ_HEALTH_HOST` | `127.0.0.1` | Bind address for the health server. Set `0.0.0.0` for Kubernetes `httpGet` probes: the kubelet probes the pod IP, not loopback. |
| `SWARMQ_METRICS_SAMPLE_INTERVAL` | `10.0` | Seconds between samples of the `swarmq_queue_depth`, `swarmq_dlq_messages` and `swarmq_worker_uptime_seconds` gauges; the same tick dispatches `WORKER_HEARTBEAT`. Must be `> 0`. The sampler only runs when metrics are enabled. |
| `SWARMQ_WORKER_ID` | hostname | Base of the worker identity that appears as `worker_id` in signals, metric labels and the log context. See [worker identity](#worker-identity). |

#### Metric catalog

Every worker with metrics enabled (`--metrics-port`/`SWARMQ_METRICS_PORT`,
`pip install 'swarmq[metrics]'`) exposes these series on `/metrics`:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `swarmq_tasks_total` | counter | `task_name`, `queue`, `outcome` | Tasks by outcome: `completed`, `failed`, `cancelled`, `rate_limited`, `duplicate`, `expired`, `dlq`. `failed` counts every failed attempt, `dlq` the terminal dead-lettering. |
| `swarmq_task_duration_seconds` | histogram | `task_name`, `queue` | Duration of `run()` for completed tasks. Buckets from 5 ms to 300 s. |
| `swarmq_tasks_active` | gauge | `queue` | Tasks currently in flight. |
| `swarmq_retries_total` | counter | `task_name`, `queue` | Retries scheduled. |
| `swarmq_tasks_rejected_total` | counter | `queue`, `reason` | Messages the worker rejected (`TASK_REJECTED`), e.g. `body_too_large`, `undecodable` (a copy with retention in `.XQ`) or `dlq_publish_failed` (broker DLX without diagnostic headers); `unregistered_task` counts a task dead-lettered after `SWARMQ_UNKNOWN_TASK_GRACE_S`. |
| `swarmq_tasks_unknown_deferred_total` | counter | `queue` | Messages the worker deferred because it does not register the task, within `SWARMQ_UNKNOWN_TASK_GRACE_S` (`TASK_UNKNOWN_DEFERRED`; [unregistered tasks](../guides/dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)). |
| `swarmq_recyclings_total` | counter | `kind`, `reason` | Process recyclings; `kind` is `worker` or `compute_child`. |
| `swarmq_queue_depth` | gauge | `queue`, `kind` | Broker-reported message count, sampled every `SWARMQ_METRICS_SAMPLE_INTERVAL`: `kind="pending"` is `{queue}` plus all delay queues, `kind="delayed"` (since 0.12) the delay queues only ([delay tiers](../explanation/delivery-semantics.md#delay-tiers)). |
| `swarmq_dlq_messages` | gauge | `queue` | Messages in `{queue}.XQ`, sampled. |
| `swarmq_lock_waits` | gauge | `worker_id` | Messages this worker deferred because their task lock was held and saw again within the last 3 minutes. A message that waits longer than that in the main queue (a backlog of more than about 2 minutes on top of the at most 60 s deferral) drops out, so under backlog the gauge undercounts. |
| `swarmq_lock_wait_oldest_seconds` | gauge | `worker_id` | Seconds the oldest of these messages has waited since its first lock conflict. |
| `swarmq_supervisor_locked_slots` | gauge | `worker_id` | Worker slots the supervisor has locked after 5 crashes or too many hangs within an hour; each lock lasts a 1 h cooldown (since 0.12). Only under the supervisor (`swarmq worker`); every worker of a supervisor reports the same value. |
| `swarmq_worker_prefetch_limit` | gauge | `worker_id` | Unacknowledged messages this worker may hold: consumed queues (each priority band counts) × `concurrency`, because prefetch applies per consumer (since 0.12, [prefetch per consumer](../explanation/delivery-semantics.md#prefetch-per-consumer)). |
| `swarmq_delay_tiers_unavailable` | gauge | `queue` | Delay tiers of the queue the broker refused to declare; their delays use `{queue}.DQ` ([delay tiers](../explanation/delivery-semantics.md#delay-tiers)). |
| `swarmq_worker_uptime_seconds` | gauge | `worker_id` | Seconds since the worker started, sampled. |
| `swarmq_scheduler_leader` | gauge | `worker_id` | `1` while this worker holds the scheduler leader lock, else `0`. |
| `swarmq_scheduler_leader_outdated` | gauge | `worker_id` | `1` while this worker sees a scheduler leader running a swarmq below the minimum leader version (currently 0.12), else `0` ([leader version](../guides/scheduling-and-retries.md#leader-version)). |
| `swarmq_scheduler_cron_conflicts_total` | counter | `schedule_name` | Leader selection ticks that saw several live definitions of one static cron for more than 10 minutes ([conflicts](../guides/scheduling-and-retries.md#conflicts)). |
| `swarmq_scheduler_crons_held` | gauge | `worker_id` | Static crons the scheduler leader holds because no live worker reports them and it does not register them itself; `0` when this worker is not leader ([held crons](../guides/scheduling-and-retries.md#crons-nobody-reports-are-held)). |
| `swarmq_scheduler_queues_orphaned` | gauge | `worker_id` | Reported queues the scheduler leader finds with messages, no consumer and no live worker reporting them (signal `SCHEDULER_QUEUE_ORPHANED`); `0` when this worker is not leader. Alert on `> 0` ([moving a task to another queue](../guides/defining-tasks.md#moving-a-task-to-another-queue)). |
| `swarmq_valkey_memory_used_ratio` | gauge | `worker_id` | Valkey `used_memory / maxmemory`, measured by the scheduler leader at most once per hour (signal `SCHEDULER_VALKEY_CHECKED`); only with a `maxmemory` limit. Under `noeviction` a full Valkey refuses writes (OOM) for every app on it; alert well before `1` ([Valkey requirements](#valkey-requirements)). |
| `swarmq_scheduler_fires_unroutable_total` | counter | `queue`, `kind` | Scheduler fires the broker returned because the queue does not exist: `cron_lost` (lost cron fire) or `oneshot_dropped` (deferred task dropped after `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`) ([missing queues](../guides/scheduling-and-retries.md#missing-queues)). |
| `swarmq_consumer_reregistrations_total` | counter | `queue` | Consumers the [consumer watch](../explanation/delivery-semantics.md#consumer-timeout) registered again after the broker ended them; the reason is in the log event `consumer_reregistered`. |
| `swarmq_execution_guard_events_total` | counter | `event` | Events of the [execution guard](../explanation/delivery-semantics.md#execution-guard): `duplicate_skipped` (redelivery acknowledged and skipped), `deferred` (same execution runs elsewhere), `orphan_limit` (dead-lettered as `WorkerHungError`), `lease_lost` (running marker expired or taken over while the task ran), `defer_stalled` (the deferrals of one redelivery exceeded `SWARMQ_CONSUMER_TIMEOUT_S`; the original runs unusually long or hangs). |
| `swarmq_log_records_dropped_total` | counter | none | Log records dropped because the log queue was full (see [Logging](logging.md#non-blocking-output-and-dropped-records)). |

`task_name` and `queue` are taken verbatim from the message; keep task
names bounded, otherwise every distinct value creates its own series.

#### Base ports and slots

The supervisor runs its workers in slots `0..N-1`, where `N` is
`--processes` or the upper bound of `--autoscale`. Each slot binds its
own observability ports, derived from the base port:

| Slot | `/metrics` with `SWARMQ_METRICS_PORT=9000` | `/health` with `SWARMQ_HEALTH_PORT=9100` |
|---|---|---|
| `0` | `9000` | `9100` |
| `1` | `9001` | `9101` |
| `2` | `9002` | `9102` |

A slot keeps its ports for the life of the run: a worker that crashes,
hangs or recycles itself is replaced in the same slot, and the
replacement re-binds the same port (with retries while the predecessor
releases it). Scrape targets therefore stay stable across restarts.

Two configurations are refused at startup with a `ConfigurationError`
(exit `78`), and `swarmq worker --check` reports the same:

- a range `[base, base + N)` that runs past `65535` — lower the base
  port or run fewer processes;
- metrics and health ranges that overlap — move one base port at least
  `N` ports away.

The `--check` payload lists the exact ports as `metrics_ports` and
`health_ports`, so an operator does not have to compute them.

#### Prometheus example

One target per slot; the bind host must be reachable from Prometheus
(`SWARMQ_METRICS_HOST=0.0.0.0` when it scrapes over the network):

```yaml
scrape_configs:
  - job_name: swarmq
    static_configs:
      - targets:
          - worker-host:9000   # slot 0
          - worker-host:9001   # slot 1
          - worker-host:9002   # slot 2
```

With `--autoscale MIN,MAX` list `MAX` targets; slots that are not
running are scraped as `down`, which is expected while the pool is
scaled down.

### Worker identity

Every worker carries a stable identity in the `worker_id` field of
the `WORKER_*`, `SCHEDULER_*` and `WORKER_RECYCLING` signals, in the
labels of `swarmq_worker_uptime_seconds` and `swarmq_scheduler_leader`,
and in the structured log context:

| Runs as | `worker_id` | Example |
|---|---|---|
| Under the supervisor (`swarmq worker`) | `<base>:<slot>` | `web-3:0`, `web-3:1` |
| Embedded (`await app.start_worker()`) | `<base>:p<pid>` | `web-3:p41872` |

`<base>` is `SWARMQ_WORKER_ID` (or the `worker_id=` kwarg of
`SwarmQConfig`); the default is the hostname. Compute children in CPU
mode report the identity of the loop process that owns them, so
`WORKER_RECYCLING` with `kind="compute_child"` carries the slot as
well. Read the slot in dashboards as the stable dimension: a slot's
uptime restarts when its worker is replaced, its identity does not.

All ports are validated at config-time: a bad value raises
`ConfigurationError` before the worker tries to bind.

### CLI output

| Variable | Default | Description |
|---|---|---|
| `SWARMQ_DEFAULT_FORMAT` | *TTY-detect* | Overrides the default `--format` for output-shaped CLI subcommands (`schedule list/add/remove/status`, `dlq list/inspect/retry/purge`, `inspect`). Accepts `table` or `json`; any other value (typo, empty string) is ignored silently and the TTY-aware default applies (`table` on a terminal, `json` on a pipe). An explicit `--format` on the command line always wins. |

### Valkey requirements

swarmq keeps state in Valkey that must never vanish on its own: task
locks, the scheduler leader lock, the execution guard, unique and fired
markers, schedules and delayed one-shots. Run Valkey (8 or later) like
this:

- **`maxmemory-policy noeviction`.** Every other policy lets Valkey
  evict keys under memory pressure; `volatile-*` evicts exactly the keys
  with a TTL, which are the locks and markers. The result is tasks
  running twice. With `noeviction` a full Valkey refuses writes instead
  (OOM, swarmq treats it as an outage); watch
  `swarmq_valkey_memory_used_ratio`. Without a `maxmemory` limit
  (`maxmemory 0`) Valkey never evicts, whatever the policy.
- **AOF** (`appendonly yes`, `appendfsync everysec`). Without it a
  restart loses everything since the last RDB snapshot: delayed tasks,
  CLI crons, and switch-offs by `swarmq schedule remove` (the next
  report brings such a cron back active).
- **No cluster mode.** Use a single primary, with replicas if needed.
- **Managed services:** some fix the policy (ElastiCache Serverless runs
  `volatile-lru`); they are not suitable. Where `CONFIG` is forbidden by
  ACL, swarmq reads `INFO memory` instead.

A worker reads `maxmemory-policy` and `maxmemory` when it connects (at
most 2 s, never fails because of the check) and, with
`SWARMQ_VALKEY_EVICTION_CHECK=strict` (default), refuses to start with
exit `78` under an evicting policy. The scheduler leader checks again
at most once per hour and logs `valkey_eviction_policy_unsafe` if the
policy changed. Each worker remembers the epoch key `[<namespace>:]sq:{sched}:epoch`;
if it disappears (Valkey lost its data: restart without persistence,
`FLUSHALL`, failover with lost writes) it logs
`valkey_data_loss_detected`. Limits: a data loss while all processes
restart at the same time goes unnoticed, so does a loss since an RDB
snapshot that brings the epoch back, and the policy is checked only
hourly.

**Before upgrading to 0.12,** run `swarmq check` with the new version in
a separate virtualenv against production (read-only, see
[CLI](cli.md#swarmq-check)); in Kubernetes optionally as an init
container. Exit `78` means the new workers would refuse to start. Do not
find that out by rolling out: under Kubernetes exit `78` puts the pods
into `CrashLoopBackOff`, under Docker with a `restart:` policy into a
restart loop. Fix the policy, or set `SWARMQ_VALKEY_EVICTION_CHECK=warn`
deliberately.

### Namespace (several apps on one Valkey)

Without a namespace all apps on one Valkey share the keys under `sq:`:
rate limits and task locks with the same name, unique hashes and one
scheduler leader for all apps. That leader publishes the crons of every
app through its own broker. Set `SWARMQ_NAMESPACE` per app to separate
them: every key and Pub/Sub channel, the leader lock and the control
channel then live under `<namespace>:sq:…` (the hash tags such as
`{sched}` stay the same). Queue names are not prefixed: give every app
its own RabbitMQ vhost.

- **Separate vhosts need a namespace.** With one leader for all apps a
  cron of app B goes through the broker of app A; on another vhost the
  queue does not exist there and the fire is lost
  (`scheduler_fire_unroutable_foreign`).
- **Without namespace** the leader steps down after repeated publish
  failures (`SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S`), so a leader with a
  broken broker no longer stops the crons of all apps.
- **The namespace does not migrate data.** Keys under `sq:` (schedules,
  CLI crons, delayed tasks, results, locks) are not seen under the new
  namespace. Set it for a new app, or move the app as described in
  [moving an app into a namespace](#moving-an-app-into-a-namespace).
  `swarmq check` shows the namespace and counts keys without it
  ([CLI](cli.md#swarmq-check)).
- **Rollout:** first run every component (workers, producers, CLI,
  control service) on 0.12, then set the namespace everywhere in the
  same step. Mixed values split the app: a producer without namespace
  writes results and schedules where the worker does not look.
- **Rollback:** once the namespace is set, roll back only to 0.12 or
  later. 0.11.1 refuses to start with `SWARMQ_NAMESPACE` set (see below);
  0.11.0 would silently ignore it.
- **While apps older than 0.12 share the Valkey without namespace**, set
  `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0`. Otherwise the leadership can
  move to a 0.11 app, and the 0.12 features of the leader (stable
  one-shot IDs, cron dedup, held crons) are off while it leads.

#### Where the namespace comes from

The namespace of the backend applies everywhere: to every key and
channel, and to the hints in log events and in `swarmq check`.

1. **`ValkeyBackend(..., namespace="app1")`** (or `keys=`) sets it
   explicitly. `namespace=""` explicitly means *no namespace*.
2. **Otherwise the configuration applies:** `SWARMQ_NAMESPACE`, or
   `SwarmQ(broker, backend, namespace="app1")` in code (the environment
   variable wins over the code value, as for every setting). A backend
   built without `namespace=` takes this value when it is handed to
   `SwarmQ(...)`; on its own (CLI, control channel) it reads
   `SWARMQ_NAMESPACE`.
3. With an explicit backend parameter and no configured namespace,
   `SwarmQ(...)` fills `app.config.namespace` from the backend.

If the backend parameter and the configuration are both set and differ
-- this includes `namespace=""` next to a set `SWARMQ_NAMESPACE` --,
`SwarmQ(...)` raises `ConfigurationError` (`namespace mismatch`; a
worker exits with `78`).

#### Moving an app into a namespace

The namespace moves no data, and old and new processes of one app must
never run side by side. For an existing app without namespace:

1. **Stop the app completely**: every worker and scheduler. On
   Kubernetes use `strategy: Recreate` or scale the deployment to 0, on
   Docker stop all its containers. A rolling update would run pods with
   and without namespace at the same time: two leaders, crons fired
   twice, results written where the other half does not look.
2. **Without `SWARMQ_NAMESPACE`**, list the app's schedules with
   `swarmq schedule list` and delete each of the app's crons and
   one-shots with `swarmq schedule remove --permanent <name>`. Left
   under `sq:`, the leader of another app without namespace keeps firing
   them.
3. **With `SWARMQ_NAMESPACE`**, create the app's CLI crons and one-shots
   again (`swarmq schedule add …`). Crons defined in code register
   themselves when the workers start.
4. **Move every app on this Valkey to 0.12 first.** A leader older than
   0.12 keeps firing the old definitions it has seen for up to 24 hours
   (until it prunes them), so crons run twice.
5. **Valkey ACLs:** key patterns `~sq:*` become `~<namespace>:sq:*`,
   channel patterns `&sq:*` become `&<namespace>:sq:*`.
6. **Tools that read swarmq's keys directly** (dashboards such as
   swarmv, scripts with `valkey-cli`) must use the namespace too.

Then start the app with the namespace. `swarmq check` with
`SWARMQ_NAMESPACE` set counts the keys still left without namespace.

### Namespace requires 0.12

`SWARMQ_NAMESPACE` belongs to swarmq 0.12 and later. 0.11.1 rejects it:
if it is set and not empty, worker, scheduler and `SwarmQ(...)` stop
with `ConfigurationError` (`swarmq worker`: exit 78), and so do the CLI
commands `swarmq schedule`, `swarmq dlq` and `swarmq inspect` (exit 78,
before any connection). `swarmq worker --check` reports it as an error
(exit 1). Without that check a rollback to 0.11.x would silently run
without the namespace, on different Valkey keys. From 0.12 on the
variable is supported, and only an invalid value stops these entry
points (exit `78`; `swarmq worker --check`: exit 1).

## `SwarmQConfig` kwargs

Programmatic equivalent of the env vars — pass any of these as
keyword arguments to `SwarmQ(broker, backend, **kwargs)`:

```python
app = SwarmQ(
    broker, backend,
    queues=["emails", "reports"],
    concurrency=50,
    result_ttl=3600,
    shutdown_timeout=60,
    burst_mode=False,
    priorities=True,
)
```

Field names match the env vars without the `SWARMQ_` prefix and in
lowercase. In an embedded app env vars always win when both are set
(`swarmq worker` resolves CLI flags before env vars instead) — pass kwargs only
when you need a non-default that you do not want in the shell
environment. One exception to the naming rule: `SWARMQ_LOG_FORMAT` sets
the field `log_json`. `worker_id=` sets the base of the
[worker identity](#worker-identity); an embedded worker has no slot,
so `metrics_port` and `health_port` are bound as given.

`SwarmQ(...)` does not configure logging. The logging fields
(`log_level`, `log_json`, `log_config`, `configure_logging`,
`logger_levels`, `log_queue_size`, `log_lifecycle_level`, `log_queue`,
`log_drain_timeout`) steer only the
[opt-in log queue](logging.md#opt-in-log-queue), the lifecycle level of
an embedded worker and the compute children of an embedded CPU pool.
With `log_queue=True`, `log_drain_timeout` bounds the flush in
`__aexit__` and at process exit; an invalid value raises
`ConfigurationError`.

## `TaskInfo` fields

Declared on each `Task` subclass via `task_info = TaskInfo(...)`.
The `TaskInfo` dataclass is **frozen**: fields are set once at class
definition time and cannot be mutated afterwards.

### Identity and routing

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | *required* | Canonical task name used by the client. Max 255 chars. |
| `queues` | `list[str]` | *required, ≥1* | Queue names the task is published to. The client uses `queues[0]` unless an explicit `queue=` is passed. |
| `priority` | `"high" \| "normal" \| "low"` | `"normal"` | With `--priorities`, routes to `{queue}.high`/`.low`. |
| `delivery` | `DeliveryMode` | `AT_LEAST_ONCE` | Alternative: `FIRE_AND_FORGET`. |

### Timeouts

| Field | Type | Default | Description |
|---|---|---|---|
| `soft_timeout` | `int \| None` | `None` | Seconds before the worker raises `SoftTimeoutError` inside the task. |
| `hard_timeout` | `int \| None` | `None` | Seconds before the worker cancels the task coroutine outright. Must be `> soft_timeout` when both set. Required when `lock` is set. |

### Retries

| Field | Type | Default | Description |
|---|---|---|---|
| `max_retries` | `int` | `0` | Number of retry attempts before DLQ. `0` means no retries. |
| `retry_backoff` | `str` | `"exponential"` | One of `"exponential"`, `"linear"`, `"fixed"`. |
| `retry_base_delay` | `float` | `1.0` | Base delay (seconds) for the backoff formula. Must be `≥ 0`. |
| `retry_exceptions` | `tuple[type[BaseException], ...] \| None` | `None` | Whitelist — only these exceptions trigger a retry. `None` retries any exception. |
| `dead_letter` | `bool \| None` | `None` | Dead-letter terminal failures of this task (`NoRetry`, exception outside `retry_exceptions`, retries exhausted). `False` stores the error result and acknowledges instead; `None` follows `SWARMQ_DEAD_LETTER_TERMINAL_FAILURES`. See [Dead letter queues](../guides/dead-letter-queues.md#switching-off-dead-lettering-of-task-failures). |

### Rate limiting

| Field | Type | Default | Description |
|---|---|---|---|
| `rate_limit` | `str \| None` | `None` | Celery-style string like `"100/m"`, `"10/s"`, `"1000/h"`. Validated at class definition. |

### Locking and concurrency

| Field | Type | Default | Description |
|---|---|---|---|
| `lock` | `str \| None` | `None` | Lock name; can be a format string like `"user-{user_id}"`. Requires `hard_timeout`. |
| `max_concurrency` | `int` | `1` | With `lock`: max concurrent executions. `1` is a mutex; `> 1` is a semaphore. Requires `lock`. |

### Scheduling (cron)

| Field | Type | Default | Description |
|---|---|---|---|
| `cron` | `str \| None` | `None` | Standard 5-field cron expression. Extended syntax (`L`, `W`, `#`) is rejected. |
| `catchup` | `bool` | `False` | Reserved for a future release. `True` raises `ValueError`. |
| `timezone` | `str \| None` | `None` | IANA zone in which `cron` is evaluated, e.g. `"Europe/Berlin"`. `None` falls back to `SWARMQ_SCHEDULER_TIMEZONE`, otherwise UTC; an explicit `"UTC"` beats that default. Requires `cron`; an unknown zone raises `ValueError`. See [time zones](../guides/scheduling-and-retries.md#time-zones). |

### Deduplication

| Field | Type | Default | Description |
|---|---|---|---|
| `unique_for` | `int \| None` | `None` | Seconds; enables dedup on `(task_name, args, kwargs)` within the TTL. `0` is rejected. |
| `unique_until` | `"start" \| "completion"` | `"completion"` | When to release the dedup flag. `"start"` releases at worker pickup; `"completion"` at terminal state. |

### Expiry

| Field | Type | Default | Description |
|---|---|---|---|
| `max_age` | `int \| None` | `None` | Seconds; worker routes the message to DLQ with `TASK_EXPIRED` if age at pickup exceeds this. Must be `> 0` when set. |

### Result storage

| Field | Type | Default | Description |
|---|---|---|---|
| `result_ttl` | `int \| None` | `None` | Per-task override for `SwarmQConfig.result_ttl`. |

## Validation behaviour

`TaskInfo.__post_init__` validates every field at class definition
time. A bad value raises `ValueError` on import of your task module,
not at first `schedule()` call. This is deliberate: a misconfigured
task should fail the worker's startup, not the user's request three
hours later.

Examples of errors surfaced at import:

- `queues` empty → `ValueError("queues must have at least 1 element")`.
- `rate_limit="100"` → `ValueError` from `parse_rate_limit` ("missing unit").
- `lock="user-{}"` with `hard_timeout=None` → `ValueError` naming the lock-TTL contract.
- `catchup=True` → `ValueError("catchup=True is reserved for a future release")`.
- `max_concurrency=2` without `lock` → `ValueError("max_concurrency requires lock to be set")`.
- `unique_for=0` → `ValueError("unique_for=0 is invalid; use None to disable dedup")`.

Fix the offending `TaskInfo` line and re-run — no worker reset
required because the error prevents the worker from starting in the
first place.

## See also

- [CLI reference](cli.md) — the flag equivalents for the worker env vars
- [Logging](logging.md) — logger names, fields, events and integration
  recipes
- [API reference](api.md) — `SwarmQ`, `TaskInfo`, and `Task` signatures
- [Defining tasks](../guides/defining-tasks.md) — worked examples of
  each `TaskInfo` field
