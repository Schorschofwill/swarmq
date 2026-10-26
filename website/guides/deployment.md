# Deploying and upgrading

How to roll out swarmq to production and how to upgrade to 0.14.4
without losing or doubling tasks. The steps are in order; each links to
the reference page with the details.

## Prerequisites

- **Versions:** Python 3.14, RabbitMQ 4.2 or 4.3, Valkey 8. Older
  brokers and Valkey servers are not tested.
- **Every app that shares the RabbitMQ vhost or the Valkey database**
  is part of the rollout. The steps apply to all of them, not only to
  the app you change.
- **Rollback plan:** know which image Kubernetes or Docker returns to
  on a rollback ([step 6](#step-6-rollback-rules)).

## Step 0: check the new version before the rollout

Install the new version in a separate virtualenv and run two read-only
checks against production before you deploy anything:

```bash
python3.14 -m venv /tmp/swarmq-check
/tmp/swarmq-check/bin/pip install 'swarmq==0.14.*'

# 1. Environment: Valkey, broker, SWARMQ_* configuration
SWARMQ_BACKEND_URL=valkey://valkey:6379/0 \
SWARMQ_BROKER_URL=amqp://user:pass@rabbitmq:5672/app \
SWARMQ_SHUTDOWN_TIMEOUT=30 \
  /tmp/swarmq-check/bin/swarmq check --format table

# 2. Task modules: TaskInfo values, cron expressions, imports, DI
#    (the virtualenv also needs your app's dependencies)
/tmp/swarmq-check/bin/swarmq worker --module myapp.tasks --app-dir /path/to/app --check
```

Set the same `SWARMQ_*` variables as your workers (above all
`SWARMQ_NAMESPACE` and `SWARMQ_VALKEY_EVICTION_CHECK`). `swarmq check`
only reads: `CONFIG GET`, `INFO`, `SCAN` and reads on Valkey, passive
declares on RabbitMQ. In Kubernetes it can also run as an init
container.

| `swarmq check` exit | Meaning | Action |
|---|---|---|
| `0` | nothing blocks; warnings possible | read the warnings (AOF off, Valkey older than 8, cluster mode, keys without namespace, orphaned queues, scheduler switched off) |
| `78` | a new worker would refuse to start: evicting `maxmemory-policy` under `strict`, invalid `SWARMQ_NAMESPACE`, or any `SWARMQ_*` value the start rejects (for example a value above its upper bound) | fix it before the rollout; see [Valkey requirements](../reference/configuration.md#valkey-requirements) and [worker behaviour](../reference/configuration.md#worker-behaviour) |
| `1` | Valkey unreachable, `SWARMQ_BACKEND_URL` missing, or `SWARMQ_BROKER_URL` set and the broker unreachable | fix access and run again |

`swarmq worker --check` imports the task module like the worker start
and reports `"ok": false` for everything the start would reject. Since
0.12.3 and 0.13 that includes values that used to start: cron
expressions outside the crontab(5) syntax or without a next fire time,
`soft_timeout`/`hard_timeout` ≤ 0, and `TaskInfo` deadlines above ten
years ([upgrade notes](#changes-that-can-stop-a-deploy)).

Do not find exit `78` out by rolling out: a worker that refuses to
start exits `78` on every restart, Kubernetes shows
`CrashLoopBackOff`, Docker with a `restart:` policy loops. Details:
[`swarmq check`](../reference/cli.md#swarmq-check) and
[exit codes](../reference/cli.md#exit-codes).

## Step 1: choose the upgrade path

| Every app on the vhost/Valkey runs | Upgrade path |
|---|---|
| 0.11.0 or older | First roll out **0.11.1 to every app, as a deployment of its own**, and let it settle. Then continue with the next row. |
| 0.11.1 | Go to 0.14.4 directly. The rules for leaving 0.11 apply: workers before producers ([step 2](#step-2-workers-before-producers)), namespace last ([step 4](#step-4-vhost-and-namespace)), rollback only to 0.11.1 ([step 6](#step-6-rollback-rules)). |
| 0.12.x or 0.13.x | Go to 0.14.4 directly. 0.12.0 to 0.14.4 run side by side on the same queues and keys. |
| 0.14.x | Patch upgrade, no format changes. 0.14.4 only fixes a shutdown case in the retry and dead-letter path. |

**Why 0.11.1 first:** Kubernetes (`kubectl rollout undo`) and Docker
roll back to the image that ran before. If that is 0.11.0, the rollback
fails on queues that 0.12 or later created (`PRECONDITION_FAILED`;
0.11.0 cannot declare at-least-once queues), and 0.11.0 silently
ignores a `SWARMQ_NAMESPACE`. 0.11.1 adopts the new queue arguments and
refuses a namespace with exit `78`
([changelog 0.11.1](../reference/changelog.md#0111-2026-10-10)).

Read the notes for every version you skip; the
[changelog](../reference/changelog.md) has the full upgrade notes.

### Changes that can stop a deploy

These turn values that used to start into errors. `swarmq check` and
`swarmq worker --check` (step 0) find all of them.

| Since | Change | Effect |
|---|---|---|
| 0.12.3 | A `TaskInfo` cron without a next fire time (`0 0 30 2 *`) raises `ValueError` at import. | exit `1`, restarts up to the restart limit |
| 0.12.3 | `inf`, `nan` and negative values in deadline, TTL and interval settings are configuration errors; use `none` where unlimited is supported. | exit `78` |
| 0.12.3 | An `ImportError` or `SyntaxError` in the task module ends the worker after one attempt. | exit `78` (was `1` with restarts) |
| 0.13.0 | Cron expressions follow a crontab(5) positive list: exactly 5 fields; `L`, `W`, `#`, `R`, `H`, `?`, `@reboot` and 6- or 7-field expressions are rejected. | `TaskInfo`: exit `1`; `swarmq schedule add --cron`: exit `1` |
| 0.13.0 | Configuration values have upper bounds: every deadline and TTL at most `315360000` s (ten years), `concurrency` at most `65535`. | exit `78` |
| 0.13.0 | `TaskInfo.result_ttl`, `max_age`, `unique_for` and `hard_timeout` at most `315360000` s; `nan` and infinite values rejected. | exit `1` |
| 0.13.0 | A `SWARMQ_WORKER_ID` that contains a pipe character is rejected. | exit `78` |
| 0.13.1 | `soft_timeout` and `hard_timeout` must be greater than 0 (`False`, `0`, negative values, `nan`, non-numbers are rejected; `None` means no timeout). | exit `1` |

Changes at run time that need a decision or an adjusted alert:

| Since | Change |
|---|---|
| 0.12.3 | Enqueue middleware (`pre_enqueue`, `post_enqueue`) and `TASK_ENQUEUED` also run for `schedule(delay=…/eta=…)`; a vetoing middleware now also blocks deferred calls. `unique_for` of `schedule()` must be `> 0` and finite (`None` switches dedup off). |
| 0.12.4 | Connecting to Valkey has a deadline, `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (default 30 s). Lock names that start with `{sched}` are reserved; a template that renders such a name dead-letters the message. |
| 0.13.0 | Log levels: `shutdown_sigterm_escalation` is INFO (alert on ERROR `shutdown_sigkill_escalation` instead); a confirmed retry copy whose ack failed logs WARNING `retry_ack_failed_after_publish`. A failed result store is retried for about 30 to 40 s before the message is redelivered; the slot stays occupied meanwhile. A missing group member result is recorded as `WorkflowResultExpired`. `swarmq schedule status` has a field `process`; `schedule add --format json` reports `already_exists` differently. |
| 0.13.1 | `delay`, `eta` and `swarmq schedule add --at` at most ten years ahead (`ValueError`, exit `1`). A cancelled `schedule()` keeps its `unique_for` claim. A lasting Valkey error reply in a workflow advance fails the workflow at the delivery limit. |
| 0.14.0 | Against a host that drops packets, Valkey connect waits up to its deadline and commands fail after about 38 s; error texts changed. Alarm log lines changed. The channel count at the broker is lower and stays flat across reconnects. A consumed queue deleted while the worker runs is fenced off: ERROR `consumer_queue_missing`, `/ready` red, the other queues keep running. |
| 0.14.1 | An embedded `start_worker()` raises `BrokerConnectionError` for a connection loss while it subscribes (was a raw `OSError`/`AMQPConnectionError`/`RuntimeError`). |
| 0.14.2 | `--autoscale` ignores draining workers for `MIN`, `MAX` and the scale-up threshold; with `--max-processes N` up to `N − MAX` draining processes may run in addition to `MAX` active ones. |
| 0.14.3 | A delay tier that cannot be declared again logs WARNING `delay_tier_redeclare_failed` and uses `{queue}.DQ` until it is restored. |

Only code that implements swarmq's contracts itself is affected by
these: own `ExecutionGuardStore` implementations need `peek` (0.13.0),
own `ResultBackend` implementations must allow concurrent
`connect()`/`close()` (0.14.0), `LockStore.force_release` now reports
what it removed (0.14.0). See the [API reference](../reference/api.md).

## Step 2: workers before producers

Start or roll the **workers before the producers** (web apps, scripts,
other services that only send).

- **For every new queue, permanently:** queues are declared by the
  workers that consume them. A publish to a queue that does not exist
  raises `UnroutableMessage` instead of being dropped silently. Deploy
  the workers of a new queue before the first producer sends to it.
  The same holds for `TaskInfo(priority=...)`: the workers of that queue
  need `--priorities`
  ([missing queues](scheduling-and-retries.md#missing-queues)).
- **For new task names:** a worker defers a task name it does not know
  yet for `SWARMQ_UNKNOWN_TASK_GRACE_S` (default 15 minutes), so an
  ordinary rolling deploy loses nothing. A worker before 0.12 sends such
  a message to the `.XQ` at first contact; while one still consumes the
  queue, producers with new task names must not be ahead of the workers
  ([unregistered tasks](dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)).
- **Producers in other services:** producer-side fixes take effect only
  where the producer runs the new version, for example the enqueue
  middleware for `delay`/`eta` (0.12.3) or `unique_for=None` with
  `delay`/`eta` (producer and scheduler leader from 0.12.2). Update
  those services too.
- **Moving a task to another queue:** run a worker for the old queue
  until it is drained; the scheduler leader reports orphaned queues
  ([moving a task](defining-tasks.md#moving-a-task-to-another-queue)).

While a worker older than 0.12 holds the scheduler leadership, the
newer workers log `scheduler_leader_outdated` until the leadership
moves ([leader version](scheduling-and-retries.md#leader-version)).
That is expected during the rollout. A leader from 0.13 on keeps firing
crons that an older leader adopted, but takes over no new or changed
cron outside the crontab(5) syntax and logs ERROR
`scheduler_cron_report_invalid`; switch such a cron to 5 fields and
deploy the workers.

## Step 3: grace period of the orchestrator

`SWARMQ_SHUTDOWN_TIMEOUT` (default 30 s) is the **total** budget from
`SIGTERM`: the drain of running tasks ends 5 s before (under the
supervisor of `swarmq worker` 7 s before), then locks are released, the
scheduler hands over and the connections close. Give the orchestrator a
margin on top:

| Orchestrator | Setting | Value |
|---|---|---|
| Kubernetes | `terminationGracePeriodSeconds` (default 30) | ≥ `SWARMQ_SHUTDOWN_TIMEOUT + 15` |
| Docker / Compose | `stop_grace_period` (default 10 s) | ≥ `SWARMQ_SHUTDOWN_TIMEOUT + 15 s` |

```yaml
# Kubernetes
spec:
  terminationGracePeriodSeconds: 45   # SWARMQ_SHUTDOWN_TIMEOUT=30
---
# Docker Compose
services:
  worker:
    stop_grace_period: 45s            # SWARMQ_SHUTDOWN_TIMEOUT=30
```

`swarmq check` prints the value as `min_grace_period_s`. If long tasks
should finish during a deploy, raise `SWARMQ_SHUTDOWN_TIMEOUT` and the
grace period together
([shutdown budget](../explanation/delivery-semantics.md#shutdown-budget)).
A `SIGKILL` before the budget ends cuts off the lock release and the
last publishes.

**Roll out with surge, not by stopping first.** Start the new workers
before the old ones get `SIGTERM` (Kubernetes `maxSurge` ≥ 1 with
`maxUnavailable: 0`, Compose `update_config.order: start-first`). A
worker that crashes while a schedule fire runs leaves a claim behind;
the redelivery waits for it and runs only on workers from 0.12.1 on
([schedule fires after a crash](../explanation/delivery-semantics.md#what-aborts-the-ack)).

Further container hints:

- **Probes:** use `/health` for liveness and `/ready` for readiness,
  never `/ready` for liveness. The health server binds to `127.0.0.1`
  by default; for Kubernetes probes, which reach the pod IP, set
  `SWARMQ_HEALTH_HOST=0.0.0.0`
  ([observability](../reference/configuration.md#observability)).
- **Signals:** start the worker in exec form (`CMD ["swarmq",
  "worker", ...]`) so it receives `SIGTERM` itself; a shell as PID 1
  does not forward it. An init process (`docker run --init`, tini)
  reaps orphaned grandchildren.
- **Working directory:** `swarmq worker` imports `--module` from the
  working directory, so set `WORKDIR` to the directory that contains
  your package (`WORKDIR /app` for `/app/myapp/tasks.py`), or pass
  `--app-dir /app`
  ([finding the task module](../reference/cli.md#finding-the-task-module)).
- **Clocks:** run NTP on every host. `max_age` compares the producer's
  clock with the worker's, and the lock wait tolerates a skew of about
  60 s.

## Step 4: vhost and namespace

- **One RabbitMQ vhost per app.** Queue names are not prefixed; two
  apps on one vhost share queues of the same name, including
  `default`.
- **One namespace per app on a shared Valkey.** Without
  `SWARMQ_NAMESPACE` all apps on one Valkey share the keys under
  `sq:`, the task locks, the rate limits and one scheduler leader for
  all crons. With separate vhosts on one Valkey the namespace is
  required: otherwise a cron of app B goes through the broker of app A,
  where its queue does not exist, and the fire is lost.
- **Lowercase only:** `[a-z0-9][a-z0-9_.-]{0,63}`, not `sq`.
  Uppercase letters are a `ConfigurationError` (exit `78`), because
  Valkey keys are case-sensitive and `App1` and `app1` would be two
  apps.
- **Order:** first move every component of the app (workers,
  producers, CLI, control service) to 0.12 or later, then set
  `SWARMQ_NAMESPACE` everywhere in the same step. Mixed values split
  the app.
- **New app or migration:** the namespace moves no data. For a new app
  just set it. An existing app is moved with a full stop (Kubernetes
  `strategy: Recreate` or scale to 0, no rolling update), its CLI crons
  and one-shots are removed without namespace and created again with
  it, and Valkey ACL patterns change from `~sq:*`/`&sq:*` to
  `~<namespace>:sq:*`/`&<namespace>:sq:*`. Move every other app on the
  Valkey to 0.12 or later before: a leader older than 0.12 keeps firing
  the old definitions for up to 24 h
  ([moving an app into a namespace](../reference/configuration.md#moving-an-app-into-a-namespace)).
- **While apps older than 0.12 share the Valkey without namespace,**
  set `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0`, so the leadership does
  not move to such an app
  ([namespace](../reference/configuration.md#namespace-several-apps-on-one-valkey)).
- **`SWARMQ_SCHEDULER=off`** keeps a worker out of the scheduler leader
  election, for example a developer machine against a copy of live data.
  At least one worker per Valkey and namespace must keep `on`, otherwise
  no cron, `delay`/`eta` task or one-shot fires
  ([turning the scheduler off](scheduling-and-retries.md#turning-the-scheduler-off-on-single-workers)).

## Step 5: permissions, policies, sizing

### RabbitMQ permissions

The swarmq user needs configure, write and read permission on every
queue of a family, and write permission on the default exchange
(`amq.default`). A family `{queue}` consists of:

| Queue | Purpose |
|---|---|
| `{queue}` | the queue workers consume |
| `{queue}.DQ` | delays over one hour |
| `{queue}.DQ.1s`, `.4s`, `.15s`, `.1m`, `.4m`, `.15m`, `.1h` | the seven [delay tiers](../explanation/delivery-semantics.md#delay-tiers) |
| `{queue}.XQ` | the dead letter queue |

That is 10 queues per family. With `SWARMQ_PRIORITIES` the bands
`{queue}.high` and `{queue}.low` are families of their own: 30 queues.
Check the queue limit of the vhost (`max-queues`) against the number of
queues times 10 (or 30). A tier the broker refuses is not fatal: the
worker logs `delay_tier_unavailable`, uses `{queue}.DQ` for it and sets
`swarmq_delay_tiers_unavailable`.

### RabbitMQ policies

- **One policy per queue.** RabbitMQ applies only the policy with the
  highest priority; a new policy displaces an existing one. Put every
  key a queue needs (for example `dead-letter-strategy`, `overflow` and
  `max-length`) into one policy.
- **Pattern for delay queues:** `\.DQ(\.\w+)?$` instead of `\.DQ$`, so
  the tiers are covered.
- **At-least-once for existing queues:** new quorum queues get
  `x-dead-letter-strategy=at-least-once` and `x-overflow=reject-publish`.
  Existing queues keep their arguments; a policy with
  `dead-letter-strategy=at-least-once` and `overflow=reject-publish`
  fixes their dead-lettering
  ([dead-lettering guarantee](../explanation/delivery-semantics.md#dead-lettering-guarantee)).
- **`max-length` with `reject-publish`:** a publish to a full queue gets
  a nack instead of dropping the oldest message, and fails with
  `PublishRejected` (a `BrokerConnectionError`) after five attempts.
  The same holds for an **operator policy** with `max-length`. Size
  `max-length` so that it is never reached in normal operation, or do
  not set it on `{queue}` and `{queue}.DQ*`. To bound the `.XQ` see
  [bounding the `.XQ`](dead-letter-queues.md#bounding-the-xq-with-a-broker-policy).
- **No `delivery-limit` policy** on swarmq queues: swarmq cannot see it
  ([effective delivery limit](../explanation/delivery-semantics.md#effective-delivery-limit)).
- **Changing `SWARMQ_QUEUE_TYPE`** of an existing queue ends with
  `QueueArgumentsMismatch` (exit `78`); migrate the queue
  ([changing queue arguments](../explanation/delivery-semantics.md#changing-queue-arguments)).
- **Never delete a queue that a running worker consumes**, also not one
  that looks orphaned; stop its workers first. The worker stops
  consuming it, logs the ERROR `consumer_queue_missing` and stays not
  ready (`/ready` names the queue) until it restarts or the queue exists
  again; its other queues keep running. A deleted delay tier is
  declared again by the next deferral (WARNING
  `delay_tier_redeclare_failed` if that fails)
  ([deleting a consumed queue](../explanation/delivery-semantics.md#deleting-a-consumed-queue)).

### Connections and processes

| Process | RabbitMQ connections |
|---|---|
| worker process (each child of `swarmq worker`) | 2: publish and consume (`SWARMQ_BROKER_PUBLISH_CONNECTION=separate`, default) |
| producer, CLI command | 1 |

A host with `--max-processes 8` thus opens up to 16 connections. Check
`connection_max`, the vhost's `max-connections`, user limits and alerts
on the connection count before the rollout, or keep one connection per
worker with `SWARMQ_BROKER_PUBLISH_CONNECTION=shared` (then acks can
hang behind a blocked publish during a broker resource alarm). Plan
memory for the headroom: under `--autoscale` with `--max-processes N`,
up to `N − MAX` draining processes can run next to `MAX` active ones.

The Valkey client pool has no limit; each process opens connections as
its concurrency needs them. Keep Valkey's `maxclients` well above the
number of processes times their concurrency.

### Valkey

- **`maxmemory-policy noeviction`**, with a `maxmemory` limit that
  leaves headroom; watch `swarmq_valkey_memory_used_ratio`. Every other
  policy can evict locks and markers, and tasks run twice. A worker
  refuses to start under an evicting policy unless
  `SWARMQ_VALKEY_EVICTION_CHECK=warn` or `off` is set deliberately.
- **AOF** (`appendonly yes`, `appendfsync everysec`).
- **No cluster mode**, no Sentinel; a single primary.
- **Memory for `max_age`:** for a task with `max_age` the execution
  markers live at least `max_age + 300` s: about 100 MB per million
  tasks a day with `max_age` 24 h.
- **Connect deadline:** a worker, producer or CLI command gives up
  connecting after `SWARMQ_BACKEND_CONNECT_TIMEOUT_S` (default 30 s)
  with `BackendConnectionError`; a port that refuses connections fails
  after about 3 s.

Details: [Valkey requirements](../reference/configuration.md#valkey-requirements).

## Step 6: rollback rules

- **Within 0.12.0 to 0.14.4 a rollback is possible** in both
  directions on the same queues and keys; 0.14 was tested against
  0.13.1, 0.12.3, 0.12.0, 0.11.1 and 0.11.0. Each fix stays only on the
  processes that keep the new version.
- **Never below 0.11.1.** 0.11.0 cannot declare queues that 0.12 or
  later created and fails with `PRECONDITION_FAILED` (step 1).
- **With a namespace, roll back only to 0.12 or later.** 0.11.1 refuses
  to start with `SWARMQ_NAMESPACE` set (exit `78`); unset the namespace
  only with a full migration back.
- **Code that uses newer API** does not run on the old version, for
  example `ValkeyBackend(connect_timeout_s=...)` (0.12.4) or
  `from swarmq import BrokerPublishBlocked, PublishRejected` (0.12.3;
  import them from `swarmq.exceptions` instead).
- **Delay tiers stay.** After a rollback their messages still return to
  `{queue}` when due; delete a tier only when it is empty.
- **What is off after a rollback to 0.11.1 or in mixed operation with
  it:**
    - Stable one-shot IDs, cron dedup by definition and held crons need
      a leader from 0.12 on; while a 0.11 app leads they are off.
    - The grace period for unknown tasks and replays after a broker
      dead-lettering need every worker of the queue from 0.12 on: a
      replay that reaches a 0.11 worker is skipped, a replayed cron
      fire as well.
    - A `swarmq dlq` command of 0.11 still reads the DLQ destructively.
    - A 0.11 worker counts lock conflicts with its old counter and
      dead-letters at the 10th; 0.11 workers keep one broker connection.
    - 0.11 semaphore acquirers still shorten the lock key.

The complete list for 0.12 is in the changelog under
[known limits](../reference/changelog.md#known-limits).

## Verify

After the rollout:

- `swarmq check` ends with exit `0`.
- `swarmq schedule status` shows the new version for the leader and
  every worker, and `swarmq_scheduler_leader_outdated` is `0`.
- No `queue_dead_letter_at_most_once` for queues you expect at
  at-least-once, no `delay_tier_unavailable` and no
  `delay_tier_redeclare_failed`.
- `swarmq_scheduler_queues_orphaned` is `0`, and the key count in
  Valkey levels off within a week (TTLs of 24 h and 7 days).
- Alerts exist for `consumer_queue_missing` and
  `shutdown_sigkill_escalation` (both ERROR).

## See also

- [Configuration](../reference/configuration.md): every `SWARMQ_*`
  variable.
- [CLI](../reference/cli.md): `swarmq check`, `--check`, exit codes.
- [Delivery semantics](../explanation/delivery-semantics.md): delay
  tiers, queue arguments, shutdown budget.
- [Changelog](../reference/changelog.md): the upgrade notes of every
  release.
