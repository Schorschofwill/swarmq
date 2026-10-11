# Deploying and upgrading

How to roll out swarmq to production, and how to upgrade from 0.11
to 0.12 without losing or doubling tasks. The steps are in order; each
one links to the reference page with the details.

## Prerequisites

- **Versions:** Python 3.14, RabbitMQ 4.2 or 4.3, Valkey 8. Older
  brokers and Valkey servers are not tested.
- **Every app that shares the RabbitMQ vhost or the Valkey database**
  is part of the rollout. The steps below apply to all of them, not
  only to the app you change.
- **Rollback plan:** know which image Kubernetes or Docker returns to
  on a rollback (step 6).

## Step 0: run `swarmq check` with the new version

Install the new version in a separate virtualenv and run the read-only
check against production before you deploy anything:

```bash
python3.14 -m venv /tmp/swarmq-check
/tmp/swarmq-check/bin/pip install 'swarmq==0.12.*'
SWARMQ_BACKEND_URL=valkey://valkey:6379/0 \
SWARMQ_BROKER_URL=amqp://user:pass@rabbitmq:5672/app \
SWARMQ_SHUTDOWN_TIMEOUT=30 \
  /tmp/swarmq-check/bin/swarmq check --format table
```

While only a release candidate of 0.12 exists, `pip` needs `--pre`
(`pip install --pre 'swarmq==0.12.*'`).

Set the same `SWARMQ_*` variables as your workers (above all
`SWARMQ_NAMESPACE` and `SWARMQ_VALKEY_EVICTION_CHECK`). The check only
reads: `CONFIG GET`, `INFO`, `SCAN` and reads on Valkey, passive
declares on RabbitMQ. In Kubernetes it can also run as an init
container.

| Exit | Meaning | Action |
|---|---|---|
| `0` | nothing blocks; warnings possible | read the warnings (AOF off, Valkey older than 8, cluster mode, keys without namespace, orphaned queues) |
| `78` | a 0.12 worker would refuse to start (evicting `maxmemory-policy` under `strict`, invalid `SWARMQ_NAMESPACE`) | fix it before the rollout; see [Valkey requirements](../reference/configuration.md#valkey-requirements) |
| `1` | Valkey unreachable, `SWARMQ_BACKEND_URL` missing, or `SWARMQ_BROKER_URL` set and the broker unreachable | fix access and run again |

Do not find exit `78` out by rolling out: a worker that refuses to
start exits `78` on every restart, Kubernetes shows
`CrashLoopBackOff`, Docker with a `restart:` policy loops. Details of
the sections and findings: [`swarmq check`](../reference/cli.md#swarmq-check).

## Step 1: move every app to 0.11.1 first

Roll out **0.11.1 to every app** that uses the same RabbitMQ vhost or
Valkey, **as a deployment of its own**, and let it settle before you
start with 0.12.

The reason is the rollback: Kubernetes (`kubectl rollout undo`) and
Docker return to the image that ran before. If that is 0.11.0, the
rollback fails on queues that 0.12 created (`PRECONDITION_FAILED`,
0.11.0 cannot declare at-least-once queues), and 0.11.0 silently
ignores a `SWARMQ_NAMESPACE`. 0.11.1 adopts the queue arguments of 0.12
and refuses a namespace with exit `78`
([changelog 0.11.1](../reference/changelog.md#0111-2026-10-10)).

## Step 2: workers before producers

Start or roll the **workers before the producers** (web apps, scripts,
other services that only send).

- **During the upgrade:** only 0.12 workers defer a task name they do
  not know yet (`SWARMQ_UNKNOWN_TASK_GRACE_S`, default 15 minutes). A
  0.11 worker that consumes the same queue sends such a message to the
  `.XQ` at first contact. Until every worker of a queue runs 0.12, a
  producer with new task names must not be ahead of the workers
  ([unregistered tasks](dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)).
- **For every new queue, permanently:** queues are declared by the
  workers that consume them. Since 0.12 a publish to a queue that does
  not exist raises `UnroutableMessage` instead of being dropped
  silently. Deploy the workers of a new queue before the first producer
  sends to it. The same holds for `TaskInfo(priority=...)`: the workers
  of that queue need `--priorities`
  ([missing queues](scheduling-and-retries.md#missing-queues)).
- **Moving a task to another queue:** run a worker for the old queue
  until it is drained; the scheduler leader reports orphaned queues
  ([moving a task](defining-tasks.md#moving-a-task-to-another-queue)).

While 0.11 and 0.12 workers run side by side, the 0.12 workers log
`scheduler_leader_outdated` until the leadership moves to a 0.12
worker ([leader version](scheduling-and-retries.md#leader-version)).
That is expected during the rollout.

## Step 3: grace period of the orchestrator

`SWARMQ_SHUTDOWN_TIMEOUT` (default 30 s) is, since 0.12, the **total**
budget from `SIGTERM`: the drain of running tasks ends 5 s before
(under the supervisor of `swarmq worker` 7 s before), then locks are
released, the scheduler hands over and the connections close. Give the
orchestrator a margin on top:

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

`swarmq check` prints the value as `min_grace_period_s`. With the
default of 30 s the drain is shorter than before 0.12 (25 s embedded,
23 s under the supervisor). If long tasks should finish during a deploy,
raise `SWARMQ_SHUTDOWN_TIMEOUT` and the grace period together
([shutdown budget](../explanation/delivery-semantics.md#shutdown-budget)).

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
- **Order:** first every component of the app (workers, producers, CLI,
  control service) on 0.12, then set `SWARMQ_NAMESPACE` everywhere in
  the same step. Mixed values split the app.
- **New app or migration:** the namespace moves no data. For a new app
  just set it. An existing app is moved with a full stop (Kubernetes
  `strategy: Recreate` or scale to 0, no rolling update), its CLI crons
  and one-shots are removed without namespace and created again with
  it, and Valkey ACL patterns change from `~sq:*`/`&sq:*` to
  `~<namespace>:sq:*`/`&<namespace>:sq:*`. Move every other app on the
  Valkey to 0.12 before: a leader older than 0.12 keeps firing the old
  definitions for up to 24 h
  ([moving an app into a namespace](../reference/configuration.md#moving-an-app-into-a-namespace)).
- **While apps older than 0.12 share the Valkey without namespace,**
  set `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0`, so the leadership does
  not move to a 0.11 app
  ([namespace](../reference/configuration.md#namespace-several-apps-on-one-valkey)).

## Step 5: permissions, policies, sizing

### RabbitMQ permissions

The swarmq user needs configure, write and read permission on every
queue of a family, and write permission on the default exchange
(`amq.default`). A family `{queue}` consists of:

| Queue | Purpose |
|---|---|
| `{queue}` | the queue workers consume |
| `{queue}.DQ` | delays over one hour |
| `{queue}.DQ.1s`, `.4s`, `.15s`, `.1m`, `.4m`, `.15m`, `.1h` | the seven [delay tiers](../explanation/delivery-semantics.md#delay-tiers) (new in 0.12) |
| `{queue}.XQ` | the dead letter queue |

That is 10 queues per family. With `SWARMQ_PRIORITIES` the bands
`{queue}.high` and `{queue}.low` are families of their own: 30 queues.
Check the queue limit of the vhost (`max-queues`) against the number of
queues times 10 (or 30). A tier the broker refuses is not fatal; the
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
  The same holds for an **operator policy** with `max-length`. Size `max-length` so that it
  is never reached in normal operation, or do not set it on `{queue}`
  and `{queue}.DQ*`. To bound the `.XQ` see
  [bounding the `.XQ`](dead-letter-queues.md#bounding-the-xq-with-a-broker-policy).
- **No `delivery-limit` policy** on swarmq queues: swarmq cannot see it
  ([effective delivery limit](../explanation/delivery-semantics.md#effective-delivery-limit)).
- **Changing `SWARMQ_QUEUE_TYPE`** of an existing queue ends with
  `QueueArgumentsMismatch` (exit `78`); migrate the queue
  ([changing queue arguments](../explanation/delivery-semantics.md#changing-queue-arguments)).

### Connections

| Process | RabbitMQ connections |
|---|---|
| worker process (each child of `swarmq worker`) | 2: publish and consume (`SWARMQ_BROKER_PUBLISH_CONNECTION=separate`, default) |
| producer, CLI command | 1 |

A host with `--max-processes 8` thus opens 16 connections. Check
`connection_max`, the vhost's `max-connections`, user limits and alerts
on the connection count before the rollout, or keep one connection per
worker with `SWARMQ_BROKER_PUBLISH_CONNECTION=shared` (then acks can
hang behind a blocked publish during a broker resource alarm).

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

Details: [Valkey requirements](../reference/configuration.md#valkey-requirements).

## Step 6: rollback rules

- **Roll back only to 0.11.1 or later.** 0.11.0 cannot declare queues
  that 0.12 created and fails with `PRECONDITION_FAILED` (step 1).
- **With a namespace, roll back only to 0.12 or later.** 0.11.1 refuses
  to start with `SWARMQ_NAMESPACE` set (exit `78`); unset the namespace
  only with a full migration back.
- **Delay tiers stay.** After a rollback their messages still return to
  `{queue}` when due; delete a tier only when it is empty.
- **What is off after a rollback or in mixed operation:**
  - Stable one-shot IDs, cron dedup by definition and held crons need a
    leader on 0.12; while a 0.11 app leads they are off.
  - The grace period for unknown tasks and replays after a broker
    dead-lettering need every worker on 0.12: a replay that reaches a
    0.11 worker is skipped, a replayed cron fire as well.
  - A `swarmq dlq` command of 0.11 still reads the DLQ destructively.
  - A 0.11 worker counts lock conflicts with its old counter and
    dead-letters at the 10th; 0.11 workers keep one broker connection.
  - 0.11 semaphore acquirers still shorten the lock key.

The complete list is in the changelog under
[known limits](../reference/changelog.md#known-limits).

## Verify

After the rollout:

- `swarmq check` ends with exit `0`.
- `swarmq schedule status` shows a leader on 0.12, and
  `swarmq_scheduler_leader_outdated` is `0`.
- No `queue_dead_letter_at_most_once` for queues you expect at
  at-least-once, no `delay_tier_unavailable`.
- `swarmq_scheduler_queues_orphaned` is `0`, and the key count in
  Valkey levels off within a week (TTLs of 24 h and 7 days).

## See also

- [Configuration](../reference/configuration.md): every `SWARMQ_*`
  variable.
- [CLI](../reference/cli.md): `swarmq check`, exit codes.
- [Delivery semantics](../explanation/delivery-semantics.md): delay
  tiers, queue arguments, shutdown budget.
- [Changelog](../reference/changelog.md): upgrade notes for 0.12.
