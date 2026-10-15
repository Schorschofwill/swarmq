# Changelog

All notable changes to SwarmQ. This project follows
[Keep a Changelog](https://keepachangelog.com/) and
[Semantic Versioning](https://semver.org/).

## [0.12.1] — 2026-10-15

0.12.1 is 0.12.1rc1 plus the fixes below. Read
[0.12.1rc1](#0121rc1-2026-10-14) for the main changes and the upgrade
notes (surge deploy, grace period, mixed operation with 0.12.0); they
apply unchanged. Install with `pip install 'swarmq==0.12.*'`; `--pre`
is no longer needed.

### Fixed

- **`ValkeyBackend.close()` no longer leaves connections open.** Locks,
  the execution guard and other stores keep the client beyond
  `close()`, for example when handlers finish only after a worker was
  aborted. Their commands opened new connections that nobody closed.
  After `close()` the pool now refuses new connections, and a
  connection returned to it is disconnected.
- **An aborted worker lets the retries of the execution guard run out
  as the orderly shutdown does:** one last bounded attempt, then none.
  Before, they kept running against the closed backend until the event
  loop closed.
- **CPU mode: no more "Event loop is closed" traceback on stderr when a
  worker ends.** If the end of a child's channel arrived only after the
  event loop had closed (a grandchild still held the descriptor, or the
  reader thread ran late under load), its reader thread failed. It now
  ends quietly.
- **`SCHEDULER_CRON_UNOWNED` with `held=True` is sent only after the
  leader has committed the postponed fire time** of the held cron.
  Before, a receiver that read the schedule saw the old time.
- **A successful reload of the logging configuration (`SIGHUP`) no
  longer leaks a file descriptor in the supervisor.** The stderr pipe of
  the `dictConfig` probe stayed open on every valid reload.

## [0.12.1rc1] — 2026-10-14

Release candidate for 0.12.1 (install with `pip install --pre`). It
fixes the three P1 findings of the second bug hunt and part of a
fourth: a publish after an expired shutdown deadline that ran a task
twice (FS-02), cron and `delay`/`eta` fires lost after a worker crash
(FS-03), locks and concurrency slots blocked by a failing DI finalizer
(FS-04), and a fire claim lost to a client retry (FS-06, fire-claim
part). No configuration changes; 0.12.1 runs next to 0.12.0 on the
same queues. Follow the upgrade notes below.

### Upgrade notes

1. **Surge deploy.** Start the new workers before the old ones stop
   (Kubernetes `maxSurge` ≥ 1 with `maxUnavailable: 0`, or start the
   new containers first). The FS-03 fix works only on the worker that
   receives the redelivery: a 0.12.0 worker still acks a deferred fire
   copy as a duplicate while the claim is held, and the fire is lost as
   before.
2. **Grace period of the orchestrator at least
   `SWARMQ_SHUTDOWN_TIMEOUT`** (recommended
   `SWARMQ_SHUTDOWN_TIMEOUT + 15 s`, as in
   [step 3 of the deployment guide](../guides/deployment.md#step-3-grace-period-of-the-orchestrator)).
   A `SIGKILL` before the budget ends cuts off the lock release and the
   last publishes, and a cron fire running at that moment becomes the
   crash case of FS-03.
3. **Mixed operation with 0.12.0.** Deferred fire copies carry the new
   header `x-swarmq-schedfire-defer-count`; 0.12.0 ignores it. On a
   rollback a 0.12.0 worker acks such a copy as a duplicate while the
   claim is held and runs it once the claim has expired. Messages of
   0.12.0 run unchanged on 0.12.1.
4. **Classic queues:** the FS-03 fix recognises a redelivery by the
   broker's delivery counters, which only quorum queues (the default)
   carry. On a classic queue the first redelivery after a crash is
   still acked as a duplicate, so that fire is lost as before.
5. **Dead-letter queue:** a deferred fire copy that ends up in the DLQ
   keeps the defer counter; `swarmq dlq retry` drops it, so the replay
   starts with a fresh backoff. DLQ entries of 0.12.0 carry no counter
   and replay as before.

### Fixed

- **FS-02: a publish whose shutdown deadline has already passed sends
  nothing.** Before, swarmq still sent it and reported
  `PublishIndeterminate`; the retry or DLQ copy plus the requeue of the
  original ran the task twice. This happened at the end of a drain,
  and during recycling systematically after about 23 s of drain. Now
  the publish raises `BrokerConnectionError` ("not sent") without
  touching the channel. A task cancelled by the drain still ends with an
  error result, but no retry or DLQ copy goes out: the original is
  redelivered, without using a retry and without a DLQ entry. During
  recycling this can repeat on every recycle, bounded by the delivery
  limit. The publish deadline of a recycle now follows its actual drain
  end, and the cleanup budget starts again from there. A deadline that
  passes during the backoff or channel setup of a later attempt still
  gives `PublishIndeterminate`. A fire the leader cannot send after the
  deadline counts towards its step-down
  ([what aborts the ack](../explanation/delivery-semantics.md#what-aborts-the-ack)).
- **FS-03: a cron or `delay`/`eta` fire is no longer lost when its
  worker dies (SIGKILL, OOM) after claiming it.** Before, the broker's
  redelivery found the dead worker's in-flight claim and was acked as a
  duplicate. Now the worker defers such a redelivery to the delay queue
  with a growing backoff (`schedfire_redelivery_deferred`) and marks the
  copy with `x-swarmq-schedfire-defer-count`. The copy is acked once the
  fire has completed and runs once the claim has expired
  (`SWARMQ_SCHEDULER_INFLIGHT_TTL`, or `hard_timeout + 30` s if larger).
  If the original is still running (only its channel was lost), the
  copy waits instead of running alongside. The deferral is capped; past
  the cap the worker logs `schedfire_defer_stalled` and acks the copy. A
  second publish of the same fire by the leader is still acked as a
  duplicate. A worker shut down while fires wait for a free
  concurrency slot now releases their claims, so the redelivery runs
  them. Give cron tasks a `hard_timeout`: the claim then outlasts every
  run, which closes the one remaining case of two runs at the same time
  ([what aborts the ack](../explanation/delivery-semantics.md#what-aborts-the-ack)).
- **FS-04: locks and concurrency slots are released even when a DI
  finalizer fails.** Before, a failing Dishka finalizer (`ExitError`)
  skipped the lock release and the slot return. The lock keeper kept
  extending the lock as long as the worker lived; after `concurrency`
  such failures the worker processed nothing, and `/ready` still
  reported it ready. Now lock, slot and fire claim are released on every
  error and cancellation path. If the shutdown cancels a task during the
  DI teardown, lock and slot are released only after the finalizer (for
  example a commit) has finished, so the commit does not run without
  its mutex.
- **FS-06 (fire claim): a fire claim is no longer lost when the Valkey
  client repeats the claim after a lost reply.** The repeated call took
  the worker's own claim for a foreign one, and the fire was acked as a
  duplicate. The unique claim and the mutex keep the same flaw for now;
  a later release fixes them.

## [0.12.0] — 2026-10-13

0.12.0 is 0.12.0rc2 plus the fix below. The two release candidates
describe all changes since 0.11.1: read
[0.12.0rc1](#0120rc1-2026-10-11) (upgrade notes, roll out 0.11.1
first) and [0.12.0rc2](#0120rc2-2026-10-12). Install with
`pip install 'swarmq==0.12.*'`; `--pre` is no longer needed. Accepted
by the load-test suite at 0.12.0rc2 (conformance, smokes, 1 h and 6 h
runs, no loss).

### Fixed

- **B25: `swarmq inspect` names `--app-dir` and the directory searched
  first when `--module` cannot be found**, as `swarmq worker` and
  `swarmq worker --check` already did. A missing dependency of a module
  that was found keeps the plain message
  ([finding the task module](cli.md#finding-the-task-module)).

## [0.12.0rc2] — 2026-10-12

Second release candidate for 0.12.0 (install with `pip install --pre`).
It adds the fix for a connection loss during publish (B24) and the
improvements from the Phoenix migration analysis below. The upgrade
notes of [0.12.0rc1](#0120rc1-2026-10-11) apply unchanged.

### Added

- **Error results carry `error_class` (fully-qualified exception class)
  and `TaskError.error_class` exposes it; in CPU mode it is the class
  reported by the child.** The form is `module.qualname`, built-ins as
  `builtins.ValueError`. Paths without a task exception name the public
  swarmq class where one exists (`TaskNotFoundError`, `LockWaitExceeded`,
  `RedeliveryLimitError`, `UnroutableMessage`,
  `ResultSerializationError`). A failed workflow stores it as
  `error.class`, and `handle.get()` raises `TaskError` with it. Failures
  the worker generates itself (hard timeout, drain, lost compute child,
  expiry) carry no class; the key is then missing, as in results of
  older workers. `error_message` is unchanged: an exception without text
  still gives `""`. Older clients ignore the new field
  ([message formats](agent/message-formats.md#task-result-valkey),
  [`TaskError`](errors.md#taskerror)).

- **`SWARMQ_SCHEDULER=on|off` and `swarmq worker --no-scheduler` keep a
  worker out of the scheduler leader election.** It still reports its
  crons, so the leader does not prune them. If no worker runs the
  scheduler, crons, oneshots and `delay`/`eta` tasks do not fire.
  `scheduler_disabled` (WARNING) is logged once per worker process;
  `swarmq check` has a new section `scheduler` with the findings
  `scheduler_disabled` (warning) and `scheduler_invalid` (error, exit
  `78`). Older workers (up to 0.12.0rc1) ignore the variable
  ([turning the scheduler off](../guides/scheduling-and-retries.md#turning-the-scheduler-off-on-single-workers)).

- **`swarmq worker` and `swarmq inspect` find task modules in the
  working directory again; `--app-dir DIR` names another directory.**
  The console script used to search only the virtualenv's `bin`
  directory, so `--module myapp.tasks` needed `PYTHONPATH` unless the
  worker was started with `python -m swarmq.cli`. The working directory
  now comes first on `sys.path` as with `python -m`, but not under
  `PYTHONSAFEPATH`/`-P`; an explicit `--app-dir` always applies. It also
  covers `--configure`, `--check`, the supervisor's worker children and
  the compute children of `--pool cpu`. Known limit: a file in the app
  directory shadows an installed package or standard-library module of
  the same name (`valkey.py`, `queue.py`); `PYTHONSAFEPATH=1` turns the
  default off. A module that cannot be found now names `--app-dir` and
  the directory searched first
  ([finding the task module](cli.md#finding-the-task-module)).

### Changed

- **Under `from __future__ import annotations`, a DI method that uses an
  alias of `FromDishka` or `FromComponent` but cannot be resolved at
  class definition now fails the worker start with
  `ConfigurationError` (exit `78`); it used to run silently without
  injection.** This covers a forward reference to a later class and an
  unresolvable non-DI parameter next to it; the message names the task,
  the parameter and the unresolvable one. `Depends[X] | None` (an alias,
  optional) is now rejected like `FromDishka[X] | None`
  ([dependency injection](api.md#dependency-injection-dishka)).

- **`raise Retry(...)` now retries even when the task sets
  `retry_exceptions` without listing `Retry`; it used to go straight to
  the DLQ.** `Retry` still counts against `max_retries`; with
  `max_retries=0` it ends terminally
  ([`Retry`](errors.md#retry)).

- **Docs: argument serialization, middleware argument changes,
  `ResultExpiredError` (never raised) and the DLQ retention note now
  describe the actual behaviour.** No behaviour change. Arguments travel as
  JSON: `datetime`, `Decimal`, `UUID` and `bytes` arrive as `str`,
  `tuple`/`set` as `list`; an object JSON cannot encode raises
  `ResultSerializationError` (with `delay`/`eta` a plain `TypeError`)
  ([defining tasks](../guides/defining-tasks.md#arguments-and-return-values)).
  A change to the existing `message["kwargs"]` dict or `message["args"]`
  list in `pre_execute` reaches `run()`, also in CPU mode; this is now a
  documented contract ([middleware](../guides/middleware.md)).
  `ResultExpiredError` is never raised; an expired result raises
  `ResultNotReadyError` ([errors](errors.md#resultexpirederror)); the
  machine-readable error catalog says `not raised (reserved)`.
  Unknown tasks no longer reach the `.XQ` via the broker's dead-letter
  exchange ([dead-letter queues](../guides/dead-letter-queues.md)).

- **Messages the worker rejects (`undecodable`,
  `middleware_chain_factory`, `body_too_large`) now go to `{queue}.XQ` as
  a copy with the dead-letter retention and the diagnostic header, like
  unregistered tasks; they no longer hold up expiry at the head of the
  `.XQ`.** The reason and error travel in the existing
  `x-swarmq-traceback` as `<reason>: <error>`, so `swarmq dlq list` shows
  them in `ERROR_PREVIEW` (e.g. `undecodable: …`). `TASK_REJECTED` and
  `swarmq_tasks_rejected_total` fire once with the same `reason` values
  as before; no error result is stored and `TASK_DLQ` does not fire. The
  broker's dead-letter exchange remains the fallback when the copy fails.
  Workers up to 0.12.0rc1 on the same queue still route these messages
  via the dead-letter exchange; both end up in `.XQ`
  ([dead-letter queues](../guides/dead-letter-queues.md#three-ways-into-the-xq)).

### Fixed

- **`Annotated[X, FromComponent(...)]` and aliases of `FromDishka` are
  injected under `from __future__ import annotations`; they were
  silently skipped.** The DI check also runs in embedded workers
  (`SwarmQ.start_worker()`, before any connection) and in
  `swarmq worker --check`, which now imports `--module` for it and checks
  the tasks that import registers
  ([dependency injection](api.md#dependency-injection-dishka)).
- **On Python 3.14 a task without DI no longer fails at class definition
  because of an unresolvable annotation** (without
  `from __future__ import annotations`); a lock template with
  placeholders renders for such a task too.
- **Building the app no longer fails with `NameError` on an unresolvable
  annotation of `run()`** (for example a type imported only under
  `TYPE_CHECKING`), with or without `from __future__ import annotations`;
  `SwarmQ(...)` and the `swarmq worker` start read these annotations
  tolerantly.

- **`task_sent_to_dlq` is no longer logged when the dead-letter publish
  failed and the message was redelivered; `dlq_publish_failed` reports
  that case.** The event still appears when the copy lies in the
  dead-letter queue (also if only the ack failed afterwards) and when a
  lock or unknown-task path routed the message there via the broker DLX
  ([logging reference](logging.md)).
- **B24: a connection loss while waiting for the publisher confirm is
  `PublishIndeterminate`, no longer a raw `AMQPConnectionError`.** When
  the broker connection broke after a message was sent but before the
  broker confirmed it (broker restart, network cut), `schedule()` and
  the other publish paths ended with `aiormq.exceptions.AMQPConnectionError`.
  The message had usually arrived and ran; a producer that retried, as
  documented for `BrokerConnectionError`, ran the task twice, and
  `schedule()` released the `unique_for` claim. swarmq now tells the
  phases apart: if the message was written to the socket, the publish
  ends as `PublishIndeterminate` and swarmq does not resend it (this
  also applies to a closed channel and to aio-pika's internal
  cancellation, which swarmq used to resend); if the connection broke
  before that, swarmq retries as before and ends with
  `BrokerConnectionError` if that fails. The worker's retry and
  dead-letter copies, workflow steps, scheduler fires and `swarmq dlq
  retry` get the same distinction. An ack that fails with
  `AMQPConnectionError` is now a `BrokerConnectionError`
  ([`PublishIndeterminate`](errors.md#publishindeterminate)).
- **`schedule_many()` keeps the `unique_for` claim of an indeterminate
  element**, as `schedule()` does; it used to release the claims of all
  failed elements, so a retry could run a task whose message had
  arrived. If any element is indeterminate, the batch now raises its
  `PublishIndeterminate` instead of the first error by index
  ([`schedule_many()`](api.md#schedule_many)).
- **A broken frame from the broker no longer escapes as a raw pamqp
  error.** When aiormq's reader stops at an unreadable frame
  (`pamqp.exceptions.AMQPFrameError` and the like) before the message was
  written, swarmq retries on a new channel like after any other
  connection loss and ends with `BrokerConnectionError` if that fails;
  after the message was written it is `PublishIndeterminate`. A failed
  ack with such an error is a `BrokerConnectionError` too.
- **No more "Future exception was never retrieved" after a publish that
  ended before its message was written.** aiormq creates the
  publisher-confirm future before writing; when the write failed or the
  caller cancelled the publish (`asyncio.wait_for`), nobody awaited it,
  and asyncio logged its connection error on garbage collection. swarmq
  now retrieves the exception of exactly that future.

### Known limits

- **A rejected message whose copy fails lands in `.XQ` without
  retention, as before (dead-letter exchange fallback).** If the copy
  ended with an indeterminate publish before the fallback, the message
  can be in `.XQ` twice, once with and once without retention. A broker
  policy on `\.XQ$` remains the only complete bound.

- **`error_class` is advisory.** Results of older workers do not have
  it, nor do paths without an exception class (expiry by `max_age`,
  `WorkerHungError`, a lock template error) and failures the worker
  generates itself (hard timeout, drain, lost compute child, child
  without error detail). In CPU mode it names the class reported by the
  child, even if the parent could not load it.

- **DI annotations that resolve neither at class definition nor at
  worker start, without a recognisable marker** (for example a type
  imported only under `TYPE_CHECKING`, or under PEP 649
  `Depends[Later] | None`) are not injected and not reported; that is
  the price of never failing the class definition of a task without DI.
  `FromDishka` in the annotation text still raises `NameError` with a
  hint.
- **PEP 563 and PEP 649 report an unresolvable DI method differently:**
  `ConfigurationError` at worker start with the future import,
  `NameError` at class definition without it. Both keep the behaviour of
  0.12.
- **`swarmq worker --check` checks only the tasks that importing
  `--module` registers.**

## [0.12.0rc1] — 2026-10-11

Release candidate for 0.12.0 (install with `pip install --pre`). It fixes the findings of the review
"operation over time" (BZ-01 to BZ-29): lost and doubled tasks, waits
that grow without bound, and state that builds up over weeks of
operation. BZ-20 stays a limit and is now reported. **Roll out 0.11.1
to every app first, as a deployment of its own**, then follow
[deploying and upgrading](../guides/deployment.md).

### Upgrade notes

#### Order

1. **Run `swarmq check` first** with 0.12 in a separate virtualenv
   against production (`pip install --pre 'swarmq==0.12.*'`; it only reads:
   `CONFIG GET`, `INFO`, `SCAN`, passive declares). In Kubernetes it
   can run as an init container. Exit `78` means the new workers would
   refuse to start; do not find that out by rolling out: a worker that
   refuses to start exits `78` on every restart, Kubernetes shows
   `CrashLoopBackOff`, Docker with a `restart:` policy loops. Set
   `maxmemory-policy noeviction`, or set
   `SWARMQ_VALKEY_EVICTION_CHECK=warn` deliberately (for example on a
   managed service with a fixed policy, accepting the risk)
   ([CLI](cli.md#swarmq-check)).
2. **Every app on 0.11.1 first**, as a deployment of its own. A
   rollback via Kubernetes or Docker returns to the image deployed
   before; 0.11.0 fails on queues that 0.12 created and ignores a
   namespace silently ([0.11.1](#0111-2026-10-10)).
3. **Workers before producers.** For every new queue this now holds
   permanently: a publish to a queue no worker has declared raises
   `UnroutableMessage` (BZ-03). During the rollout from 0.11 it also
   holds for new task names, because only 0.12 workers defer an unknown
   task (BZ-06).
4. **Grace period:** Kubernetes `terminationGracePeriodSeconds` and
   Docker `stop_grace_period` at least `SWARMQ_SHUTDOWN_TIMEOUT + 15 s`
   (`swarmq check` shows `min_grace_period_s`). With the default of
   30 s an embedded worker now drains 25 s, a worker under the
   supervisor 23 s (before 30 s); to give long tasks the old drain,
   raise `SWARMQ_SHUTDOWN_TIMEOUT` by 7 s together with the grace
   period (BZ-18).
5. **Namespace last.** First every component (workers, producers,
   CLI, control service) on 0.12, then set `SWARMQ_NAMESPACE`
   everywhere in the same step; mixed values split the app. Only for a
   new app or after moving the app: the namespace migrates no data
   under `sq:` (schedules, CLI crons, delayed tasks, results, locks).
   To move an app, stop it completely (Kubernetes `strategy: Recreate`
   or scale to 0, no rolling update), remove its crons and one-shots
   without namespace (`swarmq schedule list`, `swarmq schedule remove
   --permanent`), create its CLI crons and one-shots again with it,
   change Valkey ACL patterns from `~sq:*` to `~<namespace>:sq:*`
   (channels `&sq:*` to `&<namespace>:sq:*`) and point tools that read
   the keys directly (dashboards such as swarmv) at the namespace. Move
   every app on the Valkey to 0.12 before: a leader older than 0.12
   keeps firing the old definitions for up to 24 hours. Separate
   RabbitMQ vhosts on one Valkey need a namespace per app. While apps
   older than 0.12 share the Valkey without namespace, set
   `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S=0`, otherwise the leadership
   can move to a 0.11 app (BZ-10;
   [procedure](configuration.md#moving-an-app-into-a-namespace)).
6. **Connections:** workers open two connections to RabbitMQ
   (producers one). Check `connection_max`, per-vhost
   `max-connections`, user limits and connection-count alerts before
   the rollout, or set `SWARMQ_BROKER_PUBLISH_CONNECTION=shared`
   (BZ-16).
7. **Queues, permissions, policies:** a queue family now has up to
   seven more queues, the delay tiers `{queue}.DQ.1s` to
   `{queue}.DQ.1h` (21 with `SWARMQ_PRIORITIES`); permissions and
   delay-queue policies need the pattern `\.DQ(\.\w+)?$` (BZ-01). Only
   one policy applies to a queue: put every key (at-least-once
   dead-lettering, `max-length`) into one. Do not set a
   `delivery-limit` policy on swarmq queues (BZ-15, BZ-19).

#### Behaviour changes

- **Valkey eviction (BZ-04):** a worker refuses to start
  (`ConfigurationError`, exit `78`) when Valkey may evict swarmq keys
  (any policy but `noeviction` with a `maxmemory` limit or an unknown
  limit). A policy that cannot be read never blocks.
- **Unroutable publishes (BZ-03):** `send()`/`schedule()` to a queue
  that does not exist raise `UnroutableMessage` at once instead of
  succeeding while the broker drops the message. A task with
  `TaskInfo(priority="high"|"low")` whose workers run without
  `--priorities` now raises it too.
- **Shutdown (BZ-18):** `SWARMQ_SHUTDOWN_TIMEOUT` is the total budget
  from `SIGTERM`, no longer the drain alone;
  `ProcessManager(shutdown_grace_s=...)` means the total budget too.
- **Lock conflicts (BZ-05):** a message whose task lock is held waits
  by time instead of 10 attempts; `LockConflictLimitError` no longer
  appears (`LockWaitExceeded` instead). `x-swarmq-lock-conflict-count`
  is neither raised nor read by 0.12; the new
  `x-swarmq-lock-conflicts` feeds the signal kwarg
  `lock_conflict_count`, whose meaning is unchanged.
  `task_lock_conflict` is logged on the first conflict and then about
  once a minute; `lock_conflict_dlq_failed` names the field
  `lock_conflicts` instead of `lock_conflict_count`; the DLQ copy no
  longer carries a conflict counter header (the count is in the
  `x-swarmq-traceback` text, "(N conflicts)").
- **Unknown tasks (BZ-06):** a 0.12 worker defers a task it does not
  register for 15 minutes before it dead-letters it.
  `unregistered_task` now follows `TASK_DLQ`, the DLQ copy carries
  SwarmQ headers, and `task_not_found` comes only after the grace
  period.
- **Delivery limit (BZ-15):** the worker dead-letters at the smaller of
  `SWARMQ_DELIVERY_LIMIT` and the queue's `x-delivery-limit`: a queue
  with limit 20 under `SWARMQ_DELIVERY_LIMIT=50` is dead-lettered by the
  worker at 19 deliveries, with an error result, instead of silently by
  the broker. A changed queue type ends with `QueueArgumentsMismatch`,
  exit `78`, instead of a restart loop.
  `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=at-least-once` together with
  `SWARMQ_QUEUE_TYPE=classic` is a `ConfigurationError`.
- **At-least-once dead-lettering (BZ-19):** on new quorum queues with a
  `max-length` policy (also an operator policy), a publish to a full
  queue gets a nack instead of drop-head and fails with
  `PublishRejected` after five attempts. Duplicates from at-least-once
  dead-lettering run twice for tasks without execution guard
  (`FIRE_AND_FORGET`, `execution_guard=False`). Each worker process
  logs `queue_dead_letter_at_most_once` for existing queues, and each
  start costs up to two `PRECONDITION_FAILED` channel errors in the
  broker log per queue that 0.12 created.
- **Negative publisher confirms** raise `PublishRejected` (a subclass of
  `BrokerConnectionError`) instead of a plain `BrokerConnectionError`;
  a publish on a blocked connection raises `BrokerPublishBlocked` after
  `SWARMQ_PUBLISH_TIMEOUT_S` instead of `PublishIndeterminate`
  (BZ-16). Existing `except BrokerConnectionError` handlers still catch
  both.
- **Cancellation (BZ-23):** after the cancelled delivery the flag is
  shortened to at most 1 h instead of deleted: the same `task_id`
  enqueued again within that hour is cancelled, and `cancel()` returns
  `False`. `cancel()` also reads the one-shot entry of the task ID and
  raises `BackendConnectionError` when that read fails.
- **Crons with `unique_for` (BZ-09)** get a new fingerprint. While 0.11
  and 0.12 workers of one app both report such a cron for more than
  10 minutes, the leader logs `scheduler_cron_conflict` (the 0.12
  definition wins); a 0.12 leader rewrites the definition once
  (`scheduler_static_cron_updated`).
- **Leader version (KTD11):** workers warn about every leader before
  0.12 (was 0.10); during the rollout `scheduler_leader_outdated` is
  expected until a 0.12 worker leads.
- **Autoscaling (BZ-01):** `swarmq worker --autoscale` counts only the
  messages ready in `{queue}` itself (`ready_depth`); a backlog of
  deferred messages no longer scales up.
- **`swarmq dlq retry` (BZ-08)** keeps the `x-swarmq-replay-id` a 0.12
  worker stamped and restarts `x-swarmq-enqueued-at`; a second retry of
  the same `.XQ` message is skipped as a duplicate.
- **Queue declarations** run on a separate channel after a passive
  check; a queue without `x-delivery-limit` is adopted (WARNING;
  RabbitMQ applies 20). A refusal swarmq does not recognise (queue
  limit, permission) is reported with the broker message and no longer
  advises deleting the queue.
- **Retry backoff (BZ-26):** above `retry_max_delay` an `exponential`
  or `linear` retry waits 0.6 to 0.8 × `retry_max_delay` instead of
  exactly the cap.

#### New defaults

| Setting | Default | Effect |
|---|---|---|
| `SWARMQ_VALKEY_EVICTION_CHECK` | `strict` | refuse to start under an evicting policy |
| `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY` | `auto` | new quorum queues at-least-once with `reject-publish` |
| `SWARMQ_UNKNOWN_TASK_GRACE_S` | `900` | defer unknown tasks 15 min |
| `SWARMQ_LOCK_WAIT_MAX_S` | `21600` | longest wait for a task lock |
| `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` | `604800` | execution state behind a backlog lives 7 days |
| `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` | `86400` | retry a deferred task to a missing queue for a day |
| `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` | `SWARMQ_SCHEDULER_LOCK_TTL` | leader steps down after repeated publish failures |
| `SWARMQ_BROKER_PUBLISH_CONNECTION` | `separate` | workers use two broker connections |
| `SWARMQ_SHUTDOWN_TIMEOUT` | `30` (unchanged) | now the total shutdown budget |
| cancel flag | `max(24 h, result_ttl)` | was `result_ttl` |
| locked worker slot | 1 h cooldown | was locked until the supervisor restarted |

### Fixed

- **BZ-01: a short retry or deferral no longer waits behind a long
  one.** All delays of a queue shared `{queue}.DQ`, and RabbitMQ only
  expires the message at its head: a retry waiting an hour held back
  every lock, rate-limit, backend or guard deferral behind it. The
  worker now uses seven delay tiers `{queue}.DQ.1s` to `{queue}.DQ.1h`
  and puts a delay into the smallest tier that reaches it; delays over
  one hour stay in `{queue}.DQ`. The tiers are created at worker start
  with the arguments of `{queue}.DQ`; a refusal is not fatal (the
  delays of that tier use `{queue}.DQ`, `delay_tier_unavailable`)
  ([delay tiers](../explanation/delivery-semantics.md#delay-tiers)).
- **BZ-02: a deferred task runs under the ID that `schedule()`
  returned.** `schedule(delay=|eta=)` returned one ID, and the leader
  fired the task under a new one: `get_result()` timed out, `cancel()`
  had no effect, and the dedup flag of `unique_for` was never released.
  The one-shot entry now stores the ID in the new field
  `ScheduleDefinition.task_id`; entries without it fire under the ID in
  their name `oneshot:<task_id>`. Crons still get a new ID per fire
  ([deferred execution](../guides/scheduling-and-retries.md#immediate-vs-deferred-execution)).
- **BZ-03: a publish to a queue that does not exist no longer
  disappears silently.** The broker returned such a message
  (`basic.return`), but the publisher confirm still arrived, so
  `send()`/`schedule()` reported success and the message was gone.
  Publish channels now treat a return as an error: the publish raises
  `UnroutableMessage` at once, without retries.
  - The scheduler leader counts a cron fire to a missing queue as lost:
    it commits the fire, emits `SCHEDULER_FIRE_UNROUTABLE`
    (`kind="cron_lost"`) and logs `scheduler_fire_unroutable` at most
    once per cron and 10 minutes, plus
    `scheduler_fire_unroutable_foreign` when another app's worker
    reports the cron.
  - A deferred task whose queue is missing is retried every 60 s. After
    `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S`, counted from the first
    unroutable attempt (`ScheduleDefinition.unroutable_since`), it is
    dropped: `scheduler_oneshot_unroutable_dropped`,
    `kind="oneshot_dropped"`, an error result under its task ID, and its
    `unique_for` flag is released.
  - A worker whose own `{queue}.DQ`/`.XQ` (or delay tier) was deleted
    while it ran declares the queue family once again, through the same
    chain as at startup, and publishes the retry or DLQ copy again
    (`own_queue_unroutable_redeclaring`).
  - A workflow step whose next step goes to a missing queue marks the
    workflow `failed` at once (`workflow_advance_unroutable`) instead
    of being redelivered up to the delivery limit
  ([missing queues](../guides/scheduling-and-retries.md#missing-queues)).
- **BZ-04: evicted locks and markers, and Valkey data loss, no longer
  go unnoticed.** Under `volatile-*` or `allkeys-*` Valkey silently
  evicted locks, the leader lock, the execution guard and the unique
  and fired markers, and tasks ran twice. On connect the backend now
  reads `maxmemory-policy` and `maxmemory` (`CONFIG GET`, else `INFO
  memory`, at most 2 s, never fails); under an evicting policy every
  connect logs `valkey_eviction_policy_unsafe`, a worker with
  `strict` stops with exit `78` (producers and CLI commands only warn),
  and the scheduler leader checks again at most once per hour. A cron disabled with `swarmq schedule remove` came back
  active after Valkey lost its data; each worker now remembers the
  epoch key `[<namespace>:]sq:{sched}:epoch` and logs
  `valkey_data_loss_detected` when it is gone. The internal
  recommendation of `volatile-lru` is corrected to `noeviction` plus
  AOF ([Valkey requirements](configuration.md#valkey-requirements)).
  At runtime, a lock that is gone or held by another owner at
  extension or release is reported as `task_lock_lost` (see BZ-25).
- **BZ-05: a lock conflict waits by time instead of 10 attempts.** The
  10th conflict (about 51 s) dead-lettered the message without
  `TASK_DLQ`, result or workflow mark. A message whose task lock is
  held is now deferred (100 ms, doubling, at most 60 s) as long as the
  lock changes hands or its holder is alive. It is dead-lettered as
  `LockWaitExceeded`, with an error result, `TASK_DLQ` and the workflow
  marked failed, only when the same holder keeps the lock longer than
  the key's TTL + 60 s or `SWARMQ_LOCK_WAIT_MAX_S` passed since the
  first conflict; `max_age` stays the hard limit. The wait uses the
  workers' wall clocks (a skew of up to about 60 s is tolerated). A new
  wait begins when the broker redelivers the message (a worker died
  after acquiring the lock, or returned it at shutdown) and on the copy
  of a later execution (`x-swarmq-lock-wait-gen`: retry count and
  replay ID); classic queues carry no delivery counter, there a
  redelivery continues the old wait. A lock store whose `observe`
  returns no `LockObservation` counts as unable to report holders.
  `swarmq dlq retry` also removes the 0.11 counter
  `x-swarmq-lock-conflict-count`
  ([lock acquisition](../guides/locking-and-dedup.md#lock-acquisition-behaviour)).
- **BZ-06: an unregistered task no longer ends in the `.XQ` at first
  contact.** During a rolling deploy with producers ahead of the
  workers, every new task name was rejected to the broker DLX without
  retention or error result, and every workflow with such a step
  failed. The worker now defers the message with backoff (1 s, then as
  long as it has waited so far, at most 60 s per round) for
  `SWARMQ_UNKNOWN_TASK_GRACE_S`, measured from the first contact in
  `x-swarmq-unknown-since`; a scheduler fire keeps its in-flight claim
  and its unique flag meanwhile. After the grace period the worker
  dead-letters the message itself: error result, workflow step marked
  failed, unique flag released, `TASK_DLQ`, retention and
  `x-swarmq-traceback` on the DLQ copy, then `TASK_REJECTED` with
  `unregistered_task` and `task_not_found` (ERROR)
  ([unregistered tasks](../guides/dead-letter-queues.md#unregistered-tasks-and-rolling-deploys)).
- **BZ-07: a new scheduler leader no longer scans the whole Valkey
  database.** On takeover it looked for the handover marker of its
  predecessor with `SCAN`, before its first lock renewal; with millions
  of keys that could outlast the lock TTL, and the leader role
  flapped. The marker now has a fixed key,
  `[<namespace>:]sq:{sched}:handover`, read with `GETDEL`, and the
  leader renews its lock right after the takeover. Leaders before 0.12
  still find the marker, and their marker is still recognised
  ([leader takeover](../guides/scheduling-and-retries.md#leader-takeover)).
- **BZ-08: a DLQ replay without a new `x-swarmq-replay-id` is no
  longer skipped.** "Move messages" in the RabbitMQ UI, a shovel,
  swarmv and a CLI before 0.11 replay a dead-lettered message with its
  headers; the execution guard saw the done marker of the first run and
  acknowledged the replay without running it. The worker now stamps
  the replay ID of the next generation on every DLQ copy it publishes
  (`x-swarmq-dlq-gen`), and a message the broker dead-lettered itself
  (delivery limit, `route_via_dlx`) gets a new identity from `x-death`;
  every copy SwarmQ publishes carries that count on as
  `x-swarmq-xdeath-base`, because the broker rebuilds `x-death` on the
  next dead-lettering. A replayed scheduler fire runs as well.
  `swarmq dlq retry` derives the replay ID of a message without a
  matching `x-swarmq-dlq-gen` (0.11 worker, broker dead-lettering, old
  stock) from its identity, so a repeated retry runs once; only a
  message without a task ID gets a random ID. It treats a stamped
  message the broker dead-lettered again, or one with a replay ID the
  worker does not read, as a new generation
  ([execution guard](../explanation/delivery-semantics.md#execution-guard),
  [retrying messages](../guides/dead-letter-queues.md#retrying-messages)).
- **BZ-09: `unique_for` of a cron no longer depends on which worker is
  the leader.** The leader read `unique_for` from its own task
  registry, so a leader from another app, another `--module` or the
  other half of separate CPU and IO workers fired the cron without
  dedup, and long runs overlapped. Workers now report `unique_for` and
  `unique_until` with their crons, and the leader takes both from the
  definition (new field `ScheduleDefinition.unique_for`); a definition
  without them (written before 0.12, or a CLI schedule) still uses the
  leader's `TaskInfo`. The cron fingerprint includes both values only
  when `unique_for` is set. If the publish of a fire fails after the
  leader claimed the flag, the leader now releases it (not after
  `PublishIndeterminate`, and not when `schedule(delay=|eta=)` claimed
  it); before, the fire was counted as a duplicate on the next tick and
  lost ([unique crons](../guides/locking-and-dedup.md#unique-crons)).
- **BZ-10: several apps on one Valkey can be separated.** Without a
  namespace all apps shared rate-limit and lock keys, unique hashes and
  one scheduler leader, and a leader with a broken broker stopped the
  crons of every app. `SWARMQ_NAMESPACE` moves every Valkey key,
  Pub/Sub channel, the scheduler leader and the control channel from
  `sq:…` to `<namespace>:sq:…`; queue names stay unprefixed (one
  RabbitMQ vhost per app). Without a namespace everything stays
  byte-identical. A leader whose broker keeps failing now steps down:
  after at least 2 failed publishes over at least
  `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` without a success in between,
  it releases the leader lock without a handover marker (the successor
  emits `SCHEDULER_LEADER_FAILOVER`), emits `SCHEDULER_STANDBY` and
  does not apply again for 2 × lock TTL
  (`scheduler_publish_failures_stepdown`). Only real connection errors
  count: `UnroutableMessage`, `MessageHeadersTooLarge`,
  `BrokerPublishBlocked`, `PublishRejected` and `PublishIndeterminate`
  hit every candidate or one queue and do not. A gap longer than the
  period between two failures, and every new term, start a new series.
  The backend and the configuration agree on one namespace: an explicit
  `ValkeyBackend(namespace=…)` (or `keys=`) wins and fills
  `app.config.namespace`; without it the backend takes
  `SwarmQConfig.namespace` (code or `SWARMQ_NAMESPACE`) when it is
  handed to `SwarmQ(...)`. Both set and different (also `namespace=""`
  next to a set `SWARMQ_NAMESPACE`) is a `ConfigurationError`
  (`namespace mismatch`, exit `78`). The namespace is lowercase only
  (`[a-z0-9][a-z0-9_.-]{0,63}`, not `sq`); an invalid value stops
  workers, `SwarmQ(...)` and the CLI commands before any connection
  (exit `78`; `swarmq worker --check`: exit 1).
  `scheduler_fire_unroutable_foreign` names `SWARMQ_NAMESPACE` in its
  hint, and lock recovery hints (`valkey-cli DEL …`) name the key with
  the backend's namespace
  ([namespace](configuration.md#namespace-several-apps-on-one-valkey),
  [where the namespace comes from](configuration.md#where-the-namespace-comes-from)).
- **BZ-11: a cron whose task was removed from the code no longer fires
  until the prune period runs out.** Every fire of such a cron ended as
  an unregistered task in the `.XQ`, for up to 24 h. The scheduler
  leader now holds a static cron that a worker from 0.12 has reported
  at some point, that no live worker reports any more and that the
  leader does not register itself: the fire moves to `now +
  SWARMQ_SCHEDULER_SEEN_INTERVAL_S` without publishing until a worker
  reports it again or the leader prunes it (`SCHEDULER_CRON_UNOWNED`,
  `scheduler_cron_held_unowned`). A CLI schedule of the same name is
  never held. Right after a takeover the leader waits up to 5 s for the
  cron reports; without them it fires as before (fail-open,
  `scheduler_unowned_check_skipped`)
  ([held crons](../guides/scheduling-and-retries.md#crons-nobody-reports-are-held)).
- **BZ-12: done and completed markers no longer expire behind a long
  backlog.** The execution guard's done marker and the completed
  marker of a schedule fire lived a fixed 3900 s, measured from the
  run, not from the time a deferred copy or a duplicate fire waits in
  the queue; after more than 65 minutes of backlog the copy ran a
  second time. The claim that defers a redelivery now extends the
  execution's state to `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` or
  `max_age + 300`, also when the running delivery only releases its
  marker (for example to wait for a lock) or runs longer than the done
  TTL; the markers written afterwards never shorten it, and a failed
  ack or a copy whose ack failed marks the execution for the same
  backlog. The completed marker of a schedule fire lives `2 × wait +
  300` seconds, where `wait` is how long the fire waited in the queue
  (capped at `max_age + 300` and the backlog TTL, never below
  `SWARMQ_SCHEDULER_DEDUP_TTL`). For a task with `max_age`, the done
  and superseded markers and the completed marker live at least
  `max_age + 300` seconds, even when the run started at once (the
  completed marker stays capped at the backlog TTL). **Cost:** one
  small hash per execution for `max_age + 300` seconds, about 100 MB at
  one million tasks a day with `max_age` of 24 h; the operator controls
  it through `max_age`. Set `max_age` on cron tasks and on tasks that
  must not run twice
  ([execution guard](../explanation/delivery-semantics.md#execution-guard)).
- **BZ-13: workflows no longer hang until their TTL when a step ends
  outside the retry path.** A step that expires (`max_age`), fails its
  lock template, ends its lock wait (`LockWaitExceeded`) or is not
  registered in the worker now marks its workflow `failed`, so `get()`
  raises `TaskError` instead of timing out. Lock template errors and
  the end of a lock wait also store an error result and fire
  `TASK_DLQ`. A late duplicate of a step that already succeeded does
  not mark the workflow failed, and a workflow whose state expired logs
  `workflow_state_missing` instead of ending silently. At the delivery
  limit, a workflow mark that fails because Valkey is unreachable no
  longer hands the delivery back; it is dead-lettered anyway
  ([failure modes](../guides/workflows.md#failure-modes)).
- **BZ-14: orphaned queues are reported.** When a task moved to
  another queue or `--queues` changed, the backlog of the old queue
  stayed without a consumer, with no log line and no metric. Every
  worker now reports the queues it consumes to Valkey on every report
  tick (hash `[<namespace>:]sq:{sched}:report:queues`, timestamps from
  Valkey's `TIME`). On every adoption tick (not right after a takeover)
  the leader checks the queues no live worker has reported for three
  ticks with a passive declare, without the management API: messages
  and no consumer make a queue orphaned. It logs
  `scheduler_queue_orphaned` (WARNING) and
  `scheduler_queue_orphan_resolved` (INFO) once per transition, emits
  `SCHEDULER_QUEUE_ORPHANED` and sets
  `swarmq_scheduler_queues_orphaned`. Entries of queues that are gone
  or empty and unreported for 7 days are removed; each check has a
  30 s time limit (`scheduler_queue_check_failed`)
  ([moving a task to another queue](../guides/defining-tasks.md#moving-a-task-to-another-queue)).
- **BZ-15: changed queue arguments end with exit 78 instead of a
  restart loop.** A changed `SWARMQ_DELIVERY_LIMIT` keeps the existing
  queues declarable (their limit applies, with a WARNING
  `queue_argument_adopted`; a queue without `x-delivery-limit` is
  adopted as well). Any other recognised conflict, such as a changed
  `SWARMQ_QUEUE_TYPE`, raises `QueueArgumentsMismatch` with the
  declared and the existing value; it used to end as
  `BrokerConnectionError`, exit 1 and endless restarts. Declarations
  run on a separate short-lived channel after a passive check (a queue
  that exists is declared with the arguments of 0.11, so queues created
  by 0.11 never see a refusal), never on the publish channel, and the
  result is cached per process (dropped on `UnroutableMessage` or a
  missing queue). Only the first adoption of
  `x-dead-letter-strategy`/`x-overflow` per process is logged as INFO,
  later ones as DEBUG; an `x-overflow` adopted first is kept when the
  strategy follows. A connection loss during the declaration is a
  `BrokerConnectionError`. The worker uses the effective delivery limit
  (the smaller of `SWARMQ_DELIVERY_LIMIT` and the queue's
  `x-delivery-limit`; missing: 20, `-1`: unlimited, `0`: no
  redelivery)
  ([existing queue arguments](../explanation/delivery-semantics.md#existing-queue-arguments),
  [changing queue arguments](../explanation/delivery-semantics.md#changing-queue-arguments)).
- **BZ-16: acks no longer hang behind a blocked publish.** Publishes
  and consumption shared one connection, and during a broker resource
  alarm RabbitMQ blocks connections that publish. A worker now
  consumes over a second connection, opened by its first `consume()`:
  the consume channel, acks and nacks move there, publishes stay on the
  first connection. Producers that never consume, and brokers built on
  an injected connection or pool, keep one connection. Queue-depth and DLQ-depth reads use the consume
  connection when it is open. A publish on a blocked connection sends
  nothing and waits for the unblock within `SWARMQ_PUBLISH_TIMEOUT_S`;
  then it raises `BrokerPublishBlocked`, keeps the publish channel and
  releases a `unique_for` claim (`publish_connection_blocked`). The
  check reads the state synchronously, without a round trip. A worker
  start or `connect()` cancelled while a connection is being opened
  closes the half-open connection instead of leaving aio-pika
  reconnecting in the background every 5 s
  ([error catalog](errors.md#brokerpublishblocked)).
- **BZ-17: a task that fails with a huge error text no longer sends its
  neighbours back to the queue.** `x-swarmq-traceback` and
  `x-swarmq-lock-render-error` are bounded to 8 KiB (first 6 KiB and
  last 2 KiB); before, an error text beyond `frame_max` (128 KiB) made
  the broker close the worker connection, and every message the worker
  held was redelivered. Every publish now also checks its headers
  against half of `frame_max` and raises `MessageHeadersTooLarge`
  without sending
  ([inspecting a message](../guides/dead-letter-queues.md#inspecting-one-message)).
- **BZ-18: shutdown fits into the orchestrator's grace period.** The
  drain alone used the whole `SWARMQ_SHUTDOWN_TIMEOUT`, and lock
  release, the scheduler handover, execution-guard markers and
  connection close came after it; under the supervisor its own
  `SIGTERM` (after the full value) and `SIGKILL` (5 s later) came after
  that, so the default shutdown took 35 s and more, past Kubernetes'
  default `SIGKILL` at 30 s. Now the drain ends 5 s before the end (at
  most half the budget), and the supervisor sends `SIGTERM` at
  `T - 7 s`, `SIGKILL` at `T - 2 s` and has collected its workers by
  `T`; its worker drains until `T - 7 s`. The publish deadline in the
  shutdown is the end of the drain. Every cleanup step has a time limit
  (the rest of the budget, at least 1 s): compute pool, detached lock
  releases, execution-guard markers, each lock release, scheduler stop,
  DI container, broker close (both connections) and backend close; a
  step that exceeds it logs `shutdown_step_timeout` and the shutdown
  goes on. Tasks that ignore cancellation are awaited at most 1 s. The
  recycling drain is unchanged (largest `hard_timeout`)
  ([shutdown budget](../explanation/delivery-semantics.md#shutdown-budget)).
- **BZ-19: new quorum queues dead-letter at-least-once.** Retries and
  the delivery limit move messages by broker dead-lettering, which
  RabbitMQ does at-most-once by default for quorum queues: a leader
  change could lose them, although the documentation promised
  at-least-once. New queues now get
  `x-dead-letter-strategy=at-least-once` and `x-overflow=reject-publish`
  on `{queue}`, `{queue}.DQ` and the delay tiers (not `{queue}.XQ`).
  Existing queues keep their arguments; one WARNING
  `queue_dead_letter_at_most_once` per process lists them (also those
  with `at-least-once` but another `x-overflow`). A policy with
  `dead-letter-strategy=at-least-once` and `overflow=reject-publish`
  fixes their dead-lettering but does not end the warning, since
  swarmq cannot see policies; only
  `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=legacy` does
  ([dead-lettering guarantee](../explanation/delivery-semantics.md#dead-lettering-guarantee)).
- **BZ-20 (limit, now reported): prefetch per consumer.**
  `--concurrency` is the prefetch of each consumer: a worker consuming
  K queues (each priority band counts) holds up to K × concurrency
  unacknowledged messages, and the last waits up to K × `hard_timeout`
  for its ack. Global QoS does not work with quorum queues (RabbitMQ
  4.2 closes the connection, 4.3 ignores it), so this stays a limit.
  New: the start warning `prefetch_exceeds_consumer_timeout` (once per
  start, when K ≥ 2 and `hard_timeout` × K exceeds
  `SWARMQ_CONSUMER_TIMEOUT_S`) and the gauge
  `swarmq_worker_prefetch_limit{worker_id}` (K × concurrency)
  ([prefetch per consumer](../explanation/delivery-semantics.md#prefetch-per-consumer)).
- **BZ-21: `dlq list`, `dlq inspect` and `dlq retry` no longer lose
  messages when the reader dies.** They took each message with
  `no_ack=True` and put it back only at the end; a kill, OOM or network
  loss in between dropped what had been read. They now read with
  acknowledgement on an own channel (on the consume connection when it
  is open), copy each message to the end of `{queue}.XQ` (with its
  remaining retention) or, for `retry`, to `{queue}`, and acknowledge
  the original only after the broker confirmed the copy. Copies carry
  no broker delivery counters (`x-delivery-count`,
  `x-acquired-count`), so repeated listings do not use up the XQ's
  delivery limit. A failed acknowledgement logs `dlq_read_ack_failed`
  (WARNING) and leaves the message twice
  ([listing DLQ contents](../guides/dead-letter-queues.md#listing-dlq-contents)).
- **BZ-22: a unique task that expires through `max_age` releases its
  dedup flag.** Before, the flag stayed until `unique_for` ran out, and
  every new `schedule()` with the same arguments silently returned the
  ID of the expired task. The flag is released once the expired message
  is in the DLQ, checked against its task ID
  ([when to release the flag](../guides/locking-and-dedup.md#when-to-release-the-flag)).
- **BZ-23: a cancelled task no longer runs after waiting longer than
  `result_ttl`.** `app.cancel()` gave the flag a TTL of `result_ttl`,
  and the worker deleted it after the cancelled delivery; a task that
  waited longer (backlog, stopped workers, long `delay`), or a
  duplicate of a cancelled message, ran anyway. The flag now lives
  `max(24 h, result_ttl)`, plus the wait until a deferred task fires
  and its `max_age` + 300 s when the client's registry knows the task.
  Once the worker has handled the cancelled delivery, also when the
  task is cancelled while it runs, it shortens the flag to at most 1 h
  (`PEXPIRE … LT`) instead of deleting it; a shorter flag is never
  extended ([`cancel()`](api.md#cancel)).
- **BZ-24: a producer without a registry no longer loses dedup and
  priority silently.** A client without a registry cannot apply
  `unique_for`, `priority` or the delivery mode from the task's
  `TaskInfo`; it now logs a WARNING `client_task_defaults_unresolved`
  once per task name and client instance (at most 1000 names)
  ([client-only mode](api.md#schedule)).
- **BZ-25: task locks survive a handler that runs longer than its lock
  TTL.** The worker now extends a held mutex or semaphore slot while
  the handler runs, starting one window before the lock would expire (a
  window is three ticks of `min(max(TTL / 3, 50 ms), 20 s)`); before,
  the lock expired silently and a second worker acquired it. From a
  `hard_timeout` of 30 s on, a task that finishes within
  `hard_timeout - 30 s` costs no extra round trip; with a shorter
  `hard_timeout` the window covers the whole TTL. A holder killed hard
  still leaves the lock behind until `hard_timeout + 30 s`, at most one
  window longer. A lock that is gone or held by another owner at
  extension or release (evicted, Valkey data loss) is reported as
  `task_lock_lost` (WARNING, `stage` `running` or `release`); when no
  extension succeeded before the lock's deadline (Valkey down), it is
  reported with `stage="expired"`. A failing keeper tick logs
  `lock_keeper_tick_failed` and the keeper keeps running; at most 32
  extensions run at once. A lock store whose `extend` returns neither
  `True` nor `False` counts as not supporting it
  (`lock_extend_unsupported`). `lock_acquire.lua` now only extends the
  key's TTL and never shortens it, so a semaphore acquirer with a short
  TTL no longer expires the key under a holder with a longer one
  ([locks while the task runs](../guides/locking-and-dedup.md#locks-while-the-task-runs)).
- **BZ-26: retries at the delay cap no longer come back all at once.**
  Above `retry_max_delay` an `exponential` or `linear` retry now waits
  between 0.6 and 0.8 × `retry_max_delay` instead of exactly the cap,
  and a very high attempt number (from about 1024 on) no longer raises
  `OverflowError`. `fixed` stays without jitter
  ([backoff formulas](../guides/scheduling-and-retries.md#backoff-formulas)).
- **BZ-27: a locked worker slot runs again after a cooldown.** The
  supervisor locks a slot whose worker crashed 5 times within an hour
  (or after too many hung workers); the lock lasted until the
  supervisor restarted. Now it lasts 1 h
  (`ProcessManager.SLOT_COOLDOWN_S`); then the supervisor logs
  `worker_slot_unlocked` and fills the slot again with an empty restart
  history. `worker_restart_limit_exceeded` and
  `hang_replacement_budget_exhausted` carry `cooldown_s`. If every slot
  is locked and no worker is left, the supervisor still exits `1` so
  the orchestrator applies its own backoff. `SlotTable.lock(slot)`
  without `until` still locks for good.
- **BZ-28: dead entries in the fire index no longer stop every cron.**
  The leader reads at most 100 due entries per tick. Index entries
  without a definition and disabled definitions that still had a fire
  time (written before 0.10) filled that limit, so 100 of them blocked
  all crons. The poll now takes them out of the fire index before the
  limit applies, at most 1000 per tick; the definitions stay. A
  definition the leader cannot decode no longer fails the whole tick
  either: it is logged as `schedule_def_malformed_parked` and taken out
  of the fire index. `sched_poll.lua` keeps its keys and arguments but
  now writes (`ZREM`)
  ([fire index](../guides/scheduling-and-retries.md#fire-index)).
- **BZ-29: a delivery the worker keeps handing back no longer loops
  forever on RabbitMQ 4.3, and no longer burns its delivery budget in a
  moment.** RabbitMQ 4.3 does not count `nack(requeue=True)` in
  `x-delivery-count`, only in `x-acquired-count`. The delivery-count
  check now counts both and dead-letters such a delivery as
  `RedeliveryLimitError` one round before `SWARMQ_DELIVERY_LIMIT`;
  before, a delivery that a check handed back each time (Valkey error
  reply, failed dead-letter publish, no compute slot) cycled without
  end on 4.3 and used up its roughly 20 deliveries on 4.2 within a
  moment. Each hand-back now first waits `min(0.1 s × 2^k, 5 s)`
  (k = earlier deliveries) while holding the slot; a shutdown ends the
  wait ([delivery semantics](../explanation/delivery-semantics.md)).
- **KTD11: the leader version check warns about every leader before
  0.12** (minimum leader version, was 0.10), because the scheduler
  fixes above only take effect while the leader has them. The `hint`,
  the gauge description and the signal documentation no longer mention
  time zones
  ([leader version](../guides/scheduling-and-retries.md#leader-version)).

### Added

#### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `SWARMQ_VALKEY_EVICTION_CHECK` | `strict` | `strict`, `warn` or `off`: what a worker does under an evicting Valkey policy (BZ-04) |
| `SWARMQ_NAMESPACE` | unset | app namespace for every Valkey key and channel; `SwarmQConfig.namespace` (BZ-10) |
| `SWARMQ_SCHEDULER_PUBLISH_STEPDOWN_S` | `SWARMQ_SCHEDULER_LOCK_TTL` | leader steps down after failed publishes over this period; `0` disables (BZ-10) |
| `SWARMQ_QUEUE_DEAD_LETTER_STRATEGY` | `auto` | `auto`, `at-least-once` or `legacy`: dead-lettering of new quorum queues (BZ-19) |
| `SWARMQ_UNKNOWN_TASK_GRACE_S` | `900` | how long a worker defers an unknown task; `0` dead-letters at first contact (BZ-06) |
| `SWARMQ_LOCK_WAIT_MAX_S` | `21600` | upper limit for waiting on a held task lock, from the first conflict; `> 0` and finite (BZ-05) |
| `SWARMQ_EXECUTION_GUARD_BACKLOG_TTL_S` | `604800` | how long the execution state lives behind a backlog (BZ-12) |
| `SWARMQ_SCHEDULER_UNROUTABLE_ONESHOT_MAX_S` | `86400` | how long the leader retries a deferred task whose queue is missing (BZ-03) |
| `SWARMQ_BROKER_PUBLISH_CONNECTION` | `separate` | `separate` or `shared`: second broker connection for consuming; `SwarmQConfig.broker_publish_connection` (BZ-16) |

See [configuration](configuration.md).

#### CLI

- **`swarmq check [--format (table|json)]`**: read-only check of the
  environment before an upgrade. Section `valkey`: eviction policy,
  `maxmemory`, memory, AOF, version, cluster mode, `namespace` and
  `unprefixed_keys` (keys without namespace next to a namespace,
  counted by `SCAN` for at most 5 seconds; `unprefixed_keys_complete`
  tells a full count from a lower bound, and in cluster mode only the
  connected node is counted). Section `shutdown`:
  `min_grace_period_s`. Section `queues` (with `SWARMQ_BROKER_URL`):
  orphaned queues and reported queues without `.XQ`. Findings
  `eviction_policy`, `eviction_policy_unknown`, `aof_disabled`,
  `cluster`, `version`, `unprefixed_keys`, `unprefixed_keys_unknown`,
  `orphaned_queue`, `dead_letter_queue_missing`, `queues_unchecked`.
  Exit `78` when a worker would refuse to start, `0` otherwise
  (warnings allowed), `1` when Valkey, or a configured broker, is
  unreachable ([CLI](cli.md#swarmq-check)).
- Help texts (`--max-processes`, `schedule list`) write keys as
  `[<namespace>:]sq:…`.

#### Exceptions

All new broker exceptions are subclasses of `BrokerConnectionError`,
so existing handlers still catch them
([error catalog](errors.md)):

- `swarmq.UnroutableMessage`: the broker returned a publish because no
  queue with that name exists (BZ-03).
- `swarmq.MessageHeadersTooLarge`: the headers do not fit into half of
  the negotiated `frame_max`, even with long texts truncated; nothing
  was sent (BZ-17).
- `swarmq.exceptions.BrokerPublishBlocked`: the connection stayed
  blocked by a resource alarm until the publish deadline; nothing was
  sent (BZ-16).
- `swarmq.exceptions.PublishRejected`: the broker answered every
  attempt with a negative confirm (`reject-publish` on a full queue, a
  quorum queue without quorum).
- `swarmq.QueueArgumentsMismatch` (subclass of `ConfigurationError`):
  an existing queue has an argument that differs from the
  configuration and that swarmq does not adopt; `swarmq worker` exits
  with `78` (BZ-15).
- `swarmq.RedeliveryLimitError` and `swarmq.LockWaitExceeded`: never
  raised; the `error_type` of a DLQ copy whose deliveries were handed
  back until the delivery limit (BZ-29), or whose wait for a task lock
  ended (BZ-05).

#### Signals

([signals](../guides/signals.md))

- `TASK_UNKNOWN_DEFERRED` (`task_id`, `task_name`, `queue`, `delay`,
  `unknown_since`) (BZ-06).
- `SCHEDULER_FIRE_UNROUTABLE` (`worker_id`, `schedule_name`, `queue`,
  `kind`: `cron_lost` or `oneshot_dropped`) (BZ-03).
- `SCHEDULER_CRON_UNOWNED` (`worker_id`, `schedule_name`, `task_name`,
  `held`, `held_count`) (BZ-11).
- `SCHEDULER_VALKEY_CHECKED` (BZ-04).
- `SCHEDULER_QUEUE_ORPHANED` (BZ-14).

#### Metrics

([metric catalog](configuration.md#metric-catalog))

- `swarmq_valkey_memory_used_ratio{worker_id}` (BZ-04).
- `swarmq_lock_waits{worker_id}` and
  `swarmq_lock_wait_oldest_seconds{worker_id}` (BZ-05).
- `swarmq_tasks_unknown_deferred_total{queue}` (BZ-06).
- `swarmq_scheduler_fires_unroutable_total{queue, kind}` (BZ-03).
- `swarmq_scheduler_crons_held{worker_id}` (BZ-11).
- `swarmq_scheduler_queues_orphaned{worker_id}` (BZ-14).
- `swarmq_delay_tiers_unavailable{queue}` (BZ-01).
- `swarmq_supervisor_locked_slots` (BZ-27).
- `swarmq_worker_prefetch_limit{worker_id}` (BZ-20).
- `swarmq_queue_depth{kind="delayed"}`: messages in the delay queues
  only; `kind="pending"` keeps its meaning (`{queue}` plus all delay
  queues, now including the tiers) (BZ-01).
- `swarmq_queue_depth` and `swarmq_dlq_messages` per consumed queue:
  with `SWARMQ_PRIORITIES` the worker now also samples the
  `{queue}.high` and `{queue}.low` bands.

#### Log events

([logging](logging.md))

- Valkey environment (BZ-04): `valkey_eviction_policy_unsafe`,
  `valkey_eviction_policy_unknown`, `valkey_data_loss_detected`.
- Locks (BZ-05, BZ-25): `lock_wait_exceeded`,
  `lock_observe_unsupported`, `lock_observe_failed`, `task_lock_lost`,
  `task_lock_held_past_hard_timeout`, `lock_extend_failed`,
  `lock_extend_unsupported`, `lock_keeper_tick_failed`.
- Unknown tasks (BZ-06): `task_unknown_deferred`,
  `unknown_task_defer_failed`, `unknown_task_dlq_failed`.
- Missing queues (BZ-03): `scheduler_fire_unroutable`,
  `scheduler_fire_unroutable_foreign`, `scheduler_oneshot_unroutable`,
  `scheduler_oneshot_unroutable_dropped`,
  `own_queue_unroutable_redeclaring`, `workflow_advance_unroutable`.
- Scheduler (BZ-09, BZ-10, BZ-11, BZ-14, BZ-28):
  `scheduler_unique_release_failed`,
  `scheduler_publish_failures_stepdown`, `scheduler_cron_held_unowned`,
  `scheduler_cron_unowned_released`, `scheduler_unowned_check_skipped`,
  `scheduler_queue_orphaned`, `scheduler_queue_orphan_resolved`,
  `scheduler_queue_check_failed`, `schedule_def_malformed_parked`.
- Queues (BZ-01, BZ-19): `delay_tier_unavailable`,
  `queue_dead_letter_at_most_once`.
- Broker connections and DLQ (BZ-16, BZ-21):
  `publish_connection_blocked`, `publish_blocked_state_unreadable`,
  `dlq_read_ack_failed`.
- Others: `workflow_state_missing` (BZ-13),
  `client_task_defaults_unresolved` (BZ-24), `shutdown_step_timeout`
  (BZ-18), `worker_slot_unlocked` (BZ-27),
  `prefetch_exceeds_consumer_timeout` (BZ-20).

#### Headers, Valkey keys and golden files

All additive; existing golden entries are byte-identical unless named
([headers](agent/message-formats.md#amqp-headers)).

- **Headers:** `x-swarmq-unknown-since` (BZ-06);
  `x-swarmq-xdeath-base`, `x-swarmq-dlq-gen` (BZ-08);
  `x-swarmq-lock-conflicts`, `x-swarmq-lock-wait-since`,
  `x-swarmq-lock-holder`, `x-swarmq-lock-holder-since`,
  `x-swarmq-lock-holder-ttl-ms`, `x-swarmq-lock-wait-gen` (BZ-05).
- **Valkey keys** (each under `[<namespace>:]`): `sq:{sched}:epoch`
  (no TTL, one key; BZ-04), `sq:{sched}:handover` (a few seconds TTL;
  BZ-07), `sq:{sched}:report:known` (names a 0.12 worker has reported;
  removed with the definition; BZ-11), `sq:{sched}:report:queues`
  (expires 7 days after the last report; BZ-14).
- **Schedule definition:** new fields `task_id` (BZ-02),
  `unroutable_since` (BZ-03) and `unique_for` (BZ-09), each `null` by
  default.
- **Golden files:** `keys.json` (epoch, handover, report keys);
  `keys.namespace.json` and `channels.namespace.txt` (new, keys and
  channels with namespace; `channels.txt` unchanged); `headers.txt`
  (the headers above); `lua_contract.txt` (new `lock_extend`;
  `guard_claim` takes a sixth argument; `lock_acquire` only extends the
  TTL; `sched_poll.lua` now writes); `queue_args.json` (new keys
  `quorum_at_least_once` and `delay_tiers`, new description under
  `delay`); `values/schedule_definition.json` (new `null` fields).

#### API and contracts

- `RabbitMQBroker.ready_depth(queue)` (messages ready in `{queue}`
  itself), `RabbitMQBroker.publish_connection`, optional broker method
  `queue_statuses(names)` with `swarmq.contracts.broker.QueueStatus`.
- `ValkeyBackend(..., namespace=...)`, `ValkeyBackend.namespace`,
  `ValkeyBackend.reconcile_namespace`,
  `ValkeyBackend.eviction_check`/`recheck_eviction`,
  `swarmq.keyspace.validate_namespace`.
- `LockStore.observe` and `LockStore.extend`;
  `ScheduleStore.ensure_epoch`, `report_queues`, `queue_reports`,
  `forget_queue_reports` with `swarmq.contracts.QueueReports`. Stores
  and brokers without the optional methods skip the feature.
- `ProcessManager.SLOT_COOLDOWN_S`, `SlotTable.lock(slot, until=...)`.

### Takes effect only when …

- **… the scheduler leader runs 0.12:** stable one-shot IDs (BZ-02),
  cron dedup from the definition (BZ-09), held crons (BZ-11), the
  orphaned-queue check (BZ-14) and the fixed handover key (BZ-07).
  While a leader before 0.12 holds the role, deferred tasks fire under
  a new ID, a long cron run can overlap the next fire, and removed
  crons fire into the `.XQ` again. The gauge
  `swarmq_scheduler_leader_outdated` shows it.
- **… every worker of a queue runs 0.12:**
  - the grace period for unknown tasks (BZ-06): a 0.11 worker rejects
    an unknown task, also a deferral copy, at first contact;
  - replays after a broker dead-lettering (BZ-08): a replay that
    reaches a 0.11 worker is skipped, a replayed cron fire as well;
  - marker retention behind a backlog (BZ-12): a 0.11 worker shortens
    the state unconditionally when it writes its done marker;
  - the lock wait (BZ-05): a 0.11 worker that defers a message itself
    still raises the old counter and dead-letters at the 10th conflict;
  - lock extension (BZ-25): 0.11 semaphore acquirers still shorten the
    key;
  - two connections (BZ-16): during an alarm the acks of 0.11 workers
    hang as before.
- **… the `swarmq dlq` command runs 0.12** (BZ-21): a `dlq` command of
  0.11 still reads destructively.
- **… the queue is new** (BZ-19): only new quorum queues get
  at-least-once dead-lettering; existing queues need a policy or a
  migration. The delay tiers (BZ-01) are created by the worker that
  consumes the queue; producers and the scheduler never publish to a
  delay queue, and a process that has not declared the family uses
  `{queue}.DQ`.
- **… a worker from 0.12 has reported the cron or the queue:** crons
  reported only by workers before 0.12 are not held (BZ-11), queues
  only they consume are not checked (BZ-14).
- **… the client's registry knows the task:** only then does the cancel
  flag cover the wait until a deferred task fires and its `max_age`
  (BZ-23).
- **… every app on the Valkey runs 0.12** before a namespace is set
  (BZ-10).

### Known limits

- **BZ-01:** over one hour the head-of-line blocking of `{queue}.DQ`
  remains; inside a tier a delay can take up to four times as long, at
  least up to one second.
- **BZ-04:** a data loss while all processes restart at once goes
  unnoticed, so does a loss since an RDB snapshot that restores the
  epoch; the policy is checked only hourly.
- **BZ-05/BZ-25:** mixed `hard_timeout` under one semaphore name is not
  supported: an acquirer sweeps slots older than its own TTL. A blocked
  event loop blocks the lock extension as well; the lock can expire and
  is reported as `task_lock_lost`.
- **BZ-08:**
  - A broker-DLX replay or a cron replay that reaches a 0.11 worker is
    skipped (lost); a replay in flight during the version switch can
    run twice.
  - After a UI move the first execution counts the raw `x-death`;
    repeated UI moves after another broker dead-lettering are skipped
    (use `swarmq dlq retry`).
  - `route_via_dlx` after a UI move keeps the old
    `x-swarmq-dead-lettered-at` (a `nack` cannot set headers), so
    `dlq list` can drop such a message as expired.
  - A UI replay with `max_age` can expire at once.
  - With `dead_letter_ttl_s=None` replay identities are protected for
    7 days, not forever.
- **BZ-10:**
  - With separate vhosts on one Valkey the namespace is required.
  - Once the namespace is set, no rollback below 0.12.
  - Moving an existing app into a namespace needs a full stop; results,
    delayed tasks and locks under `sq:` are not taken over.
  - A leader older than 0.12 on the same Valkey fires the old
    definitions for up to 24 h; crons run twice.
  - Valkey ACL patterns and tools that read the keys directly (swarmv)
    must follow the namespace.
  - `swarmq check` counts keys without prefix for at most 5 s (then
    "at least n") and in cluster mode only on the connected node.
  - Stepping down: `ACCESS_REFUSED` on publish (write permission of the
    user on `amq.default`) still counts. A dead TCP connection without
    RST first ends as `PublishIndeterminate` and counts only once the
    heartbeat detects the loss.
- **BZ-11:** a held cron does not catch up its missed occurrences.
- **BZ-12:** a backlog that builds up only after the run is not covered
  for tasks without `max_age`. The completed marker stays capped at
  7 days; the done and superseded markers have no cap and live longer
  with a `max_age` over 7 days.
- **BZ-14:** a queue with any consumer never counts as orphaned; a
  queue whose worker restarts slowly can count as orphaned until it
  consumes again.
- **BZ-16:** RabbitMQ announces a block only to a connection that
  publishes during the alarm, so the first publish after the alarm
  starts still ends as `PublishIndeterminate`. The blocked state is
  read from a private aiormq attribute; if a later aiormq drops it,
  swarmq warns once (`publish_blocked_state_unreadable`) and publishes
  unchecked.
- **BZ-19:** duplicates run twice for tasks without execution guard;
  rollback only to 0.11.1 or later; `reject-publish` together with a
  `max-length` policy makes publishes to a full queue fail.
- **BZ-20:** prefetch applies per consumer.
- **BZ-21:** a reader that dies between a copy and its acknowledgement
  leaves the message twice in the DLQ (a repeated `dlq retry` runs it
  once); parallel `dlq list` runs each see only part of the DLQ.
- **BZ-23:** a task published right away is cancellable for 24 h.
- **KTD11:** the gauge `swarmq_scheduler_leader_outdated` stays at `1`
  as long as a 0.11 app on the same Valkey holds the leadership.
- **Delivery limit by policy:** a `delivery-limit` policy is not
  visible to swarmq and not supported on swarmq queues.

## [0.11.1] — 2026-10-10

Rollback target before 0.12. **Roll out 0.11.1 to every app first,
as a deployment of its own, and only then move to 0.12.** A rollback
via Kubernetes or Docker returns to the previously deployed image; if
that is 0.11.0, it fails on queues that 0.12 created, and it silently
ignores a namespace.

### Changed

- **Existing queue arguments are adopted.** When a queue already exists
  with a different `x-dead-letter-strategy` or `x-overflow` (as 0.12
  creates them), the worker declares it with the existing values
  instead of failing with `PRECONDITION_FAILED`. A different
  `x-delivery-limit` is adopted as well and logged as WARNING
  `queue_argument_adopted`: below `SWARMQ_DELIVERY_LIMIT` the broker
  dead-letters after the existing limit itself, without an error
  result; above it the worker still dead-letters at
  `SWARMQ_DELIVERY_LIMIT - 1` deliveries. Every other argument conflict
  fails as in 0.11.0, including a queue without `x-delivery-limit`.
  Adopting `x-overflow=reject-publish` means: with a `max-length`
  policy a publish to a full queue gets a nack instead of drop-head and
  fails after the publish retries. Each declaration is first
  checked on a separate short-lived channel, so a conflict never closes
  the publish channel. Queues the worker creates get exactly the same
  arguments as in 0.11.0
  ([delivery semantics](../explanation/delivery-semantics.md#existing-queue-arguments)).
- **`SWARMQ_NAMESPACE` is rejected.** If it is set and not empty,
  worker, scheduler and `SwarmQ(...)` stop with `ConfigurationError`
  (`swarmq worker`: exit 78), as do `swarmq schedule`, `swarmq dlq`
  and `swarmq inspect` (exit 78); `swarmq worker --check` reports it as
  an error. A namespace requires swarmq >= 0.12;
  0.11.x would silently use the keys without namespace
  ([configuration](configuration.md#namespace-requires-012)).

## [0.11.0] — 2026-10-09

A worker keeps consuming after a broker consumer timeout, and a
redelivered message no longer runs next to its original. Dead-lettered
messages expire after 7 days, dead-lettering of task failures can be
switched off, and static crons that no app reports anymore are
removed. **Before workers on 0.11.0 become scheduler leader, every app
on the same Valkey must run at least 0.10.0**, otherwise crons of an
older app can be removed; see the
[upgrade notes](#upgrading-from-010x).

### Changed (breaking)

- **`HealthChecker(compute_check=...)` is now
  `HealthChecker(checks={...})`.** The readiness hook takes any number
  of named checks, each an async callable returning `(ok, error)` and
  shown as its own entry in `/ready`. Replace
  `compute_check=fn` with `checks={"compute": fn}`. Only code that
  builds a `HealthChecker` itself is affected; `swarmq worker` and
  `start_worker()` wire it for you
  ([API](api.md#health-and-metrics)).
- **Static crons that no worker reports anymore are removed.** The
  leader removes a static cron that was reported at least once and that
  no worker has reported for `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (default
  24 h); before, a cron removed from code kept firing. This requires
  every app on the same Valkey to run 0.10.0 or later, because only
  those report their crons: a cron that was reported once and is now
  served only by an older app (for example after a rollback) would be
  removed although the app still runs. Workaround:
  `SWARMQ_SCHEDULER_PRUNE=log` in the 0.11.0 apps until the last app
  is upgraded. Crons that were never reported (old entries, apps before
  0.10.0) are never removed automatically. See
  [removing crons](../guides/scheduling-and-retries.md#removing-crons-no-app-reports-anymore).
- **Dead-lettered messages expire after 7 days by default.** Every
  message the worker dead-letters carries a per-message `expiration`
  of `SWARMQ_DEAD_LETTER_TTL_S` (default `604800`) and the headers
  `x-swarmq-dead-lettered-at` and `x-swarmq-dead-letter-ttl-ms`. Set `SWARMQ_DEAD_LETTER_TTL_S=none` to
  keep them without limit, as before. See
  [retention](../guides/dead-letter-queues.md#retention-dead-lettered-messages-expire).
- **A `ScheduleStore` of your own needs new methods** for cron pruning:
  `prune_cron`, `pruned_disabled` and `forget_pruned_disabled`. A
  backend of your own runs without the execution guard unless it
  exposes an `ExecutionGuardStore` as `execution_guard`. New types in
  `swarmq.contracts`: `ExecutionGuardStore`, `GuardClaim`
  ([API reference](api.md#coordination-store-protocols)).

### Changed

- **Duplicate deliveries are skipped.** The new
  [execution guard](../explanation/delivery-semantics.md#execution-guard)
  (on by default, `SWARMQ_EXECUTION_GUARD`) keeps a redelivered message
  from running while the same execution runs elsewhere, after it
  finished, or after a retry superseded it. A redelivery of a finished
  task is acknowledged and skipped (`duplicate_delivery_skipped`); one
  whose original still runs goes back through the delay queue with
  backoff (`execution_guard_deferred`), not counted as a retry.
  **Slower recovery after a crash:** the redeliveries of a SIGKILLed
  or OOM-killed worker run only once its running markers expired --
  with the defaults roughly one to two minutes after the crash instead
  of right away
  ([details](../explanation/delivery-semantics.md#execution-guard));
  `SWARMQ_EXECUTION_GUARD_TTL_S` shortens it. A marker that a running
  worker could not set during a short Valkey outage (done, superseded,
  release) is retried in the background for about that TTL, and once
  more right away on shutdown -- the redelivery does not wait for the
  TTL and no orphan is counted. Cost:
  two Lua calls per task plus a batched renewal; in the load suite no
  measurable throughput loss under realistic load, −7.4 % in the worst
  case (one worker, only ~0.3 ms tasks, CPU-saturated), +11–12 % CPU
  per task and +0.15–0.2 ms latency for fast tasks. Workers before
  0.11.0 in the same deployment do not protect the deliveries they
  handle.
- **`/ready` checks the consumers.** It reports a worker not ready
  before its first `consume` and once a consumed queue (including
  priority queues) has had no active consumer for
  `SWARMQ_CONSUMER_READY_GRACE_S` (default 30 s). Never use `/ready` as
  a liveness probe; use `/health`.
- **`swarmq dlq retry` sets a new `x-swarmq-replay-id`** on every
  message it moves, so the execution guard lets the replay run, and
  removes `x-swarmq-dead-lettered-at` and the retention. `dlq list` and
  `dlq inspect` put messages back with their remaining retention and
  drop expired ones; messages without the header (old stock, messages
  the broker's dead-letter exchange routed) get the full retention on
  their first read. The retention comes from the message's
  `x-swarmq-dead-letter-ttl-ms`; the CLI uses its own
  `SWARMQ_DEAD_LETTER_TTL_S` only for messages without that header.
- **The dead-letter path stores the error result with the task's
  `result_ttl`**, like every other result of the task (before:
  always `SWARMQ_RESULT_TTL`).
- **`task_deferred_backend_unavailable`** can carry
  `gate="execution_guard"` when Valkey is unreachable for the guard's
  claim.
- **New Valkey keys:** `sq:guard:{<identity>}:run` and
  `sq:guard:{<identity>}:state` (identity = task id, plus `|replay id`
  after a DLQ replay) and the set `sq:{sched}:disabled`. Cron reports
  are kept for twice `SWARMQ_SCHEDULER_PRUNE_AFTER_S` if that is longer
  than 7 days. New Lua scripts `guard_claim`, `guard_finish`,
  `sched_prune`. The golden fixtures (`keys.json`, `lua_contract.txt`,
  `headers.txt`) are regenerated; the queue arguments are unchanged, so
  existing deployments start without `PRECONDITION_FAILED`.
- The dashboard swarmv shows the new headers of DLQ messages
  (`x-swarmq-dead-lettered-at`, `x-swarmq-replay-id`).
- `swarmq schedule --help` no longer says that `prune` is not in v1.
- **Tests:** integration and e2e suites default to the swarmq test
  containers on 5699/15699 (RabbitMQ) and 6399 (Valkey) and refuse to
  flush a local Valkey or RabbitMQ on the default ports 6379/5672
  unless `SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1` is set.

### Added

- **Consumer watch** (`SWARMQ_CONSUMER_WATCH_INTERVAL_S`, default 5 s):
  cancels a consumer the broker ended and registers it again within two
  intervals, without a restart and without leaving a zombie consumer at
  the broker. Log `consumer_reregistered` (reason `broker_cancel` or
  `channel_replaced`), metric
  `swarmq_consumer_reregistrations_total{queue}`; further events
  `consumer_cancel_failed`, `consumer_reregister_failed`,
  `consumer_watch_channel_timeout`, `consumer_watch_tick_failed`. See
  [consumer timeout](../explanation/delivery-semantics.md#consumer-timeout).
- **`SWARMQ_CONSUMER_READY_GRACE_S`** (default `30`): grace period of
  the `/ready` consumer check.
- **Execution guard settings:** `SWARMQ_EXECUTION_GUARD` (default
  `true`), `SWARMQ_EXECUTION_GUARD_TTL_S` (`60`),
  `SWARMQ_EXECUTION_GUARD_DONE_TTL_S` (default
  `max(retry_max_delay, consumer_timeout_s) + 300`, at least the TTL),
  `SWARMQ_EXECUTION_GUARD_MAX_ORPHANS` (`3`; the same execution expiring
  that often without release goes to the DLQ as `WorkerHungError`).
  Metric `swarmq_execution_guard_events_total{event}` with `event` in
  `duplicate_skipped`, `deferred`, `orphan_limit`, `lease_lost`. Log
  events `duplicate_delivery_skipped`, `execution_guard_deferred`,
  `execution_guard_defer_failed_redelivered`,
  `execution_guard_orphan_limit`, `execution_guard_lease_lost`,
  `execution_guard_renew_failed`, `execution_guard_finish_failed`,
  `execution_guard_finish_recovered`, `execution_guard_finish_gave_up`.
- **`SWARMQ_DEAD_LETTER_TTL_S`** (default `604800`, `none` =
  unlimited) and **`SWARMQ_DEAD_LETTER_TERMINAL_FAILURES`** (default
  `true`) with **`TaskInfo(dead_letter=...)`**, which wins over the
  global setting. Switched off, `NoRetry`, an exception outside
  `retry_exceptions` and exhausted retries store the error result and
  acknowledge the message, as tasks with `max_retries=0` did before 0.1.2. An alert
  on `swarmq_tasks_total{outcome="dlq"}` then no longer covers these
  tasks; `TASK_FAILED` and `outcome="failed"` keep firing per attempt.
  The delivery-count check, the orphan limit, undecodable messages,
  `max_age` and lock errors still go into the `.XQ`. See
  [switching it off](../guides/dead-letter-queues.md#switching-off-dead-lettering-of-task-failures).
- **Cron pruning settings:** `SWARMQ_SCHEDULER_PRUNE` (`on`, `log`,
  `off`; default `on`) and `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (default
  `86400`, at least 3 × `SWARMQ_SCHEDULER_SEEN_INTERVAL_S`). Log events
  `scheduler_cron_pruned`, `scheduler_cron_prune_candidate`,
  `scheduler_cron_prune_skipped`, `scheduler_cron_prune_failed`. A cron
  that was disabled when it was removed comes back disabled.
- **`swarmq schedule prune --orphans [--format]`** removes static crons
  that no worker has ever reported; JSON output `{"removed": [...]}`.
  **`swarmq schedule list`** shows the newest report per cron in the
  column `LAST_SEEN` (`never` for a static cron without any report, `-`
  for CLI schedules), JSON `last_seen_ms`
  ([CLI](cli.md#swarmq-schedule-prune)).
- **Headers** `x-swarmq-replay-id`, `x-swarmq-guard-defer-count` and
  `x-swarmq-dead-lettered-at`
  ([message formats](agent/message-formats.md)).
- **Settle rules for RabbitMQ 4.3:** an ack or nack for a delivery
  whose consumer the broker cancelled waits for the watch's own cancel
  (`delivery_settle_deferred`) and is dropped if that cancel does not
  come within 10 s (`delivery_settle_withheld`), so a late settle cannot wake the
  zombie consumer.
- **`swarmq.exceptions.SettleFailedAfterPublish`** (subclass of
  `BrokerConnectionError`, worker-internal): a retry or dead-letter copy
  was published, only the ack of the original failed
  ([error catalog](errors.md#settlefailedafterpublish)).
- Documentation:
  [execution guard and consumer timeout](../explanation/delivery-semantics.md#execution-guard),
  DLQ retention and the trigger matrix with dead-lettering switched off
  ([Dead letter queues](../guides/dead-letter-queues.md#when-messages-land-in-the-dlq)).

### Fixed

- **A worker no longer stops consuming after a consumer timeout on
  RabbitMQ 4.3, and a task no longer runs twice after one on 4.2 and
  earlier.** On 4.3 the quorum queue cancels the consumer and leaves the
  channel open; the consumer stayed at the broker as a zombie, a late
  ack woke it, and its deliveries were silently dropped. Up to 4.2 the
  broker closes the channel, and aio-pika's restore received the
  message again while the original still ran. The consumer watch now
  cancels and re-registers the consumer, settles wait for that cancel,
  and the execution guard defers or skips the redelivery.
- **The dead-letter queue no longer grows without bound:** dead-lettered
  messages expire, reading the DLQ does not extend their retention, and
  dead-lettering of terminal task failures can be switched off.
- **A cron removed from code stops firing** once no worker has reported
  it for `SWARMQ_SCHEDULER_PRUNE_AFTER_S`.
- `log_records_dropped` no longer counts the stop sentinel of a log
  queue whose pipe multiprocessing had already closed at process exit
  (it reported 2 instead of 1 under CPU load).
- When the copy of a failed task is in the DLQ and only the ack of the
  original fails (channel lost), the failure now counts as
  dead-lettered: the unique flag is released, `TASK_DLQ` fires, and
  the worker logs `dlq_ack_failed_after_publish` (WARNING) instead of
  `dlq_publish_failed`. The execution guard skips the redelivery. The
  lock gate (conflict cap, render error) no longer tries the DLX route
  on top in that case.
- **`dlq list`/`inspect`/`retry` keep the retention a message was
  dead-lettered with.** The worker writes it to
  `x-swarmq-dead-letter-ttl-ms` (milliseconds or `"none"`); a CLI with a
  shorter or default `SWARMQ_DEAD_LETTER_TTL_S` no longer drops messages
  the worker kept longer or without limit.
- On RabbitMQ 4.3, a dead-letter `nack` that waits in vain for the
  consumer watch's own cancel now fails instead of returning quietly; the
  execution guard frees the message so the broker's redelivery runs,
  instead of skipping it as done.
- A claim of the execution guard that timed out on the client but ran
  in Valkey is released in the background (only the own marker), so it
  no longer counts as an orphan later.
- The execution guard frees its running marker before the schedfire and
  unique finalisation, so a shutdown cancel there no longer keeps it
  alive until the process ends.
- A cron that was disabled when it was pruned and is reported again is
  created disabled with the WARNING `scheduler_cron_revived_disabled`;
  `schedule remove --permanent` now also clears that mark.
- When the deferrals of a redelivery add up to more than
  `consumer_timeout_s` because the original still runs, the worker logs
  `execution_guard_defer_stalled` (WARNING) and counts
  `event="defer_stalled"` once per message; it keeps deferring.

### Upgrading from 0.10.x

1. **Every app on the Valkey must run 0.10.0 or later** before workers
   on 0.11.0 can become scheduler leader. Check with
   `swarmq schedule status` that `LEADER_VERSION` and all `WORKERS` show
   0.10.0 or later. If an app cannot be upgraded yet, set
   `SWARMQ_SCHEDULER_PRUNE=log` in every 0.11.0 app and watch
   `scheduler_cron_prune_skipped`; switch it back to `on` once all apps
   report.
2. **Look at the crons without any report:** `swarmq schedule list`
   shows them with `LAST_SEEN` `never`, and the leader logs
   `scheduler_cron_prune_candidate`. Remove the ones you no longer need
   with `swarmq schedule prune --orphans`, but only once every app runs
   0.10.0 or later.
3. **Liveness probes:** if a probe points at `/ready`, move it to
   `/health`. `/ready` can now turn red for 30 s without a consumer,
   and a restart does not help.
4. **Own `HealthChecker` or `ScheduleStore`:** switch to
   `checks={"compute": ...}` and add the three `ScheduleStore` methods.
5. **Dead-letter queues:** decide on `SWARMQ_DEAD_LETTER_TTL_S` (7 days
   by default; `none` keeps the old behaviour). The `swarmq dlq` CLI
   keeps the retention each message was dead-lettered with. Old messages get the retention when they
   are first read. Messages the broker's dead-letter exchange routes
   carry no retention, so the
   [broker policy on `\.XQ$`](../guides/dead-letter-queues.md#bounding-the-xq-with-a-broker-policy)
   from 0.10.0 remains the workaround that bounds every path and the
   length; mind that only one policy applies per queue and that
   overflow is deleted.
6. **Switching off dead-lettering** (`dead_letter=False`): move alerts
   from `outcome="dlq"` to `outcome="failed"` for these tasks.
7. **Execution guard during the rolling upgrade:** it protects only
   deliveries that a 0.11.0 worker handles. Leave it on; switch it off
   with `SWARMQ_EXECUTION_GUARD=false` only if you have measured a
   problem.

## [0.10.0] — 2026-10-09

Crons get a time zone with a fixed rule for clock changes, and every
worker reports its crons, so crons of apps whose workers never become
leader arrive too. **Upgrade every app on a shared Valkey to 0.10.0
before you give any cron a time zone**, and move hand-converted UTC
crons before 25 October 2026, 01:00 UTC:
[migration guide](../guides/scheduling-and-retries.md#migrating-hand-converted-utc-crons).

### Changed (breaking)

- **Upgrade in two steps when crons get time zones.** A scheduler
  leader before 0.10.0 does not know the new `timezone` field: it drops
  the zone of every cron it fires and fires it in UTC again. Upgrade
  every app on the same Valkey first, without changing crons, and check
  with `swarmq schedule status` that `LEADER_VERSION` and all `WORKERS`
  show 0.10.0; only then set zones and change the expressions back to
  local time, both in the same commit. Details and the equivalence
  recipe:
  [migration guide](../guides/scheduling-and-retries.md#migrating-hand-converted-utc-crons).
- **A `ScheduleStore` of your own needs new methods.** `fire_ts(name)`;
  `put(..., fire_ts_ms=None)` must leave the fire index untouched;
  `commit_fire` takes the new keyword `fired_payload`; `replan_if_unchanged` re-plans an unscored entry atomically; for cron reports
  `report_cron`, `cron_reports` and `prune_cron_reports`; for the
  leader version `report_worker_version`, `worker_versions`,
  `set_leader_version` and `leader_info`. New types in
  `swarmq.contracts`: `CronReport`, `CronReports`, `LeaderInfo`.
  Signatures and semantics:
  [API reference](api.md#coordination-store-protocols).

### Changed

- **Static crons arrive through cron reports instead of the leader's
  registry.** Before, the leader wrote the crons of its own task
  registry when it took over leadership, and only inserted or updated
  them. Crons of an app whose workers never became leader never
  arrived, and a change waited for the next leader change. Now every
  worker reports its static crons at start and every
  `SWARMQ_SCHEDULER_SEEN_INTERVAL_S` (default 60 s); the leader adopts,
  per name, the live definition that appeared most recently. A change
  in the code of any app takes effect within two intervals after the
  first worker with the new code starts; during a rolling deploy the
  newer code state wins, and an old worker that restarts later does
  not reset it. See
  [cron reports](../guides/scheduling-and-retries.md#cron-reports-and-the-leader).
- **`swarmq schedule remove --permanent` on a static cron that a live
  worker reports:** the cron comes back within one interval (before:
  at the next leader change). It stays gone once no code reports it
  anymore. A cron disabled without `--permanent` stays disabled.
- **Log events of the cron sync.** `scheduler_sync_taskinfo_crons_failed`
  is gone. New: `scheduler_cron_report_failed`,
  `scheduler_cron_reports_read_failed`,
  `scheduler_cron_reports_prune_failed`, `scheduler_cron_adopt_failed`
  (WARNING) and `scheduler_cron_report_invalid` (ERROR). A cron that a
  live worker reports no longer logs
  `scheduler_task_not_in_local_registry` when it fires on a leader of
  another app. See the
  [event catalog](logging.md#event-catalog).
- **New Valkey keys**, all under the `{sched}` hash tag:
  `sq:{sched}:report:first_seen`, `sq:{sched}:report:last_seen`,
  `sq:{sched}:report:def` (field `name|fingerprint`),
  `sq:{sched}:leader_version` (with the lock's TTL) and
  `sq:{sched}:worker_version:<worker_id>` (TTL three report
  intervals). The leader deletes reports not refreshed for 7 days. The
  golden fixtures of the backend contract (`keys.json`,
  `lua_contract.txt`, schedule definition) are regenerated for the new
  keys, scripts and the `timezone` field; the queue arguments are
  unchanged.
- **Dependencies:** `croniter>=6.2` (was `>=2`; its behaviour in the
  doubled hour is only established for these versions) and new
  `tzdata`, so zones resolve in images without `/usr/share/zoneinfo`.
- **Tests and CI pin RabbitMQ exactly:** `4.3.6` as the main broker, a
  second `4.2.9` for the consumer-timeout characterisation, `3.13.7`
  in the chaos stack. Behaviour after a consumer timeout differs
  between 4.2 and 4.3; the tests record it per version.

### Added

- **Time zones for crons.** `TaskInfo(timezone="Europe/Berlin")` (IANA
  name, checked at class definition) or the default
  `SWARMQ_SCHEDULER_TIMEZONE` / `SwarmQConfig(scheduler_timezone=...)`.
  A zone on the task beats the default, also an explicit `"UTC"`.
  Without a zone nothing changes: `"UTC"`, `"Etc/UTC"` and no zone are
  stored the same way, so existing schedules keep their fingerprint.
  [Time zones](../guides/scheduling-and-retries.md#time-zones).
- **Rule for clock changes.** In the doubled autumn hour a cron
  without `*` in its hour field fires only in the first pass; with `*`
  (also `*/n`, `@hourly`) it follows real time. In the spring gap a
  fixed hour fires once at the next existing time. Table and examples:
  [clock changes](../guides/scheduling-and-retries.md#clock-changes).
- **`swarmq.scheduler.next_fire_times(cron, start, *, timezone=None,
  count=1)`** calculates fires with the scheduler's rule, for example
  to check before the switch that a converted expression keeps its
  fires ([API](api.md#next_fire_times)).
- **`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`** (default `60`, `> 0`): interval
  of the cron reports and of the leader's adoption. A report lives three
  intervals; use the same value in every app on one Valkey.
- **Conflicts:** when several definitions of one cron stay live for
  more than 10 minutes (typically same-named tasks in two apps), the
  leader logs `scheduler_cron_conflict`, sends
  `Signal.SCHEDULER_CRON_CONFLICT` (`worker_id`, `schedule_name`,
  `fingerprints`) and counts
  `swarmq_scheduler_cron_conflicts_total{schedule_name}`.
- **Leader version.** The leader writes its swarmq version next to the
  lock, every worker reports its own. A worker that sees a leader
  without a version or with one before 0.10.0 logs
  `scheduler_leader_outdated` (logger `swarmq.scheduler.leader_version`,
  once per leader), sends `Signal.SCHEDULER_LEADER_OUTDATED`
  (`worker_id`, `leader`, `leader_version`, `outdated`) and sets the
  gauge `swarmq_scheduler_leader_outdated{worker_id}`.
  `swarmq schedule status` shows `LEADER_VERSION` and a `WORKERS`
  block; `--format json` adds `leader_version` and `workers`.
  Write or read errors log `scheduler_leader_version_write_failed` and
  `scheduler_leader_version_check_failed`.
- `SchedulerLoop(static_crons=..., version=...)` for code that runs the
  scheduler loop itself.
- **Guide: bounding `{queue}.XQ` with a broker policy.** The DLQ has
  neither TTL nor length limit, and every terminal failure lands
  there, also with `max_retries=0`. As an immediate workaround, a
  RabbitMQ policy on `\.XQ$` with `message-ttl` and `max-length`
  bounds it. Only one policy applies per queue, and overflow is
  deleted, not dead-lettered further:
  [Dead letter queues](../guides/dead-letter-queues.md#bounding-the-xq-with-a-broker-policy).

### Fixed

- **A due fire is no longer lost when a static cron changes.** If the
  old fire is already due, it stays and fires once with the new
  content; otherwise the next fire is recalculated.
- **A changed queue, time zone or task name of a static cron is
  adopted.** Before, only a changed expression replaced the stored
  definition.
- **A cron whose next fire cannot be calculated no longer stalls the
  poll loop.** Before, the next fire was calculated after the publish,
  and an error ended the whole poll tick: the following entries of the
  batch did not fire, and the entry stayed due for the next tick. Now
  the next fire is calculated before the publish; on an error only this
  entry is logged (`scheduler_next_fire_failed`, ERROR) and leaves the
  fire index, and the leader plans it again once a live worker reports
  it and the fire can be calculated. This re-planning is atomic: a
  `swarmq schedule remove` that lands meanwhile stays in effect. If the
  entry cannot be taken out of the index, the leader logs
  `scheduler_park_failed` (WARNING, logger `swarmq.scheduler.fire_plan`)
  and tries again on the next tick; `scheduler_park_owner_mismatch`
  (WARNING) means it lost the leader lock meanwhile.
- **Disabled definitions get no fire-index entry.** Entries that older
  versions left in the index are still skipped.
- **A fire no longer resets a definition adopted meanwhile.** The
  commit after a fire writes the definition back only if it has not
  changed since the poll.
- **A lost broker channel during the final ack no longer ends as
  "Task exception was never retrieved".** When RabbitMQ went away while
  the worker settled a delivery, aiormq's `ChannelInvalidStateError`
  escaped from the message task, and asyncio logged it at ERROR without
  task context. The outcome was already right (RabbitMQ redelivers
  unacknowledged messages), only the log was wrong. `ack`, `redeliver`
  and `route_via_dlx` now raise `BrokerConnectionError` on a lost
  channel or connection, as the broker contract documents; cancelling
  the calling task still raises `CancelledError`. The worker logs one
  WARNING `delivery_settle_failed` with task context, `error_type` and
  `error`. Any other unexpected error that ends a delivery is logged as
  ERROR `delivery_handler_failed` with task context and traceback
  instead of escaping from the task.
- **Error logs in the worker carry `error_type` and `error`.**
  `backend_connection_error` (result could not be stored) had neither
  and was always ERROR; it is now WARNING for `AT_LEAST_ONCE`, where
  the redelivery heals it, and stays ERROR for `FIRE_AND_FORGET`, where
  the result is lost. `workflow_advance_backend_error` and
  `workflow_advance_broker_error` follow the same rule. Also with error
  fields now: `task_expired_result_store_failed`,
  `task_expired_dlq_publish_failed`, `task_expired_nack_also_failed`,
  `cancel_consume_partial_failure` and
  `compute_recycle_replacement_failed`.

## [0.9.0] — 2026-10-07

### Changed (breaking)

- **CPU pool: `logging.basicConfig()` in a task module no longer takes
  effect, and `--log-format`, `--log-level` and `--log-config` apply in
  the compute children.** Both pools now follow the same "app wins"
  rule. Before, the forkserver imported your task modules with an
  unconfigured root logger, so a `basicConfig()` at import time won in
  every compute child: against `--log-format json` (plain-text lines in
  the JSON stream), against `--log-config` (task lines in the app's
  format) and against `--log-level` (INFO lines lost when `basicConfig`
  set no level). Now the forkserver attaches a placeholder handler to the
  root logger before it imports your modules (it starts no thread and
  passes WARNING and above on to `lastResort` when no other handler gets
  the record), and each compute child applies the rule of the IO worker: `basicConfig` has no effect, a
  module that replaces the root handlers (for example with
  `logging.config.dictConfig`) wins, and added handlers write next to
  SwarmQ's. `logging_reconfigured_by_app` and
  `logging_handlers_added_by_app` appear once per pool, from the logger
  `swarmq.worker.compute.pool`. Every process forked from the forkserver
  starts without the placeholder, so `basicConfig` works in processes
  your app starts from the shared forkserver itself (for example a
  `ProcessPoolExecutor` with the default start method `forkserver` on
  Python 3.14). Under `--no-configure-logging`
  (`configure_logging=False`) `basicConfig` takes effect in both pools.
  The same applies to the compute children of an embedded CPU pool. A
  main module started by path (`python app.py`) is imported by the
  forkserver before SwarmQ's preload, so its import-time logging still
  wins there; the rule is guaranteed with the `swarmq` command and
  `python -m`. **Migration:** put the format you set with `basicConfig`
  into a `--log-config` file, or pass `--no-configure-logging` if your
  module should own logging in every process. If your app logs through
  its own `QueueHandler`, start its `QueueListener` in the `--configure`
  hook rather than at import time; SwarmQ then starts it in every
  compute child. Details:
  [Logging](logging.md#when-a-task-module-configures-logging).
- **One log line per deferral when the backend is unreachable before
  execution.** The gate warnings `cancel_check_backend_error`,
  `workflow_cancel_check_backend_error`, `cancel_store_result_failed` and
  `schedfire_dedup_backend_error` are gone; `rate_limit_check_failed`
  and `lock_acquire_failed` (ERROR) now appear only for errors other
  than an unreachable backend. Their details moved into
  `task_deferred_backend_unavailable`: the new field `stage`
  (`completed_check`, `inflight_claim`, `cancel_check`,
  `workflow_cancel_check`, `cancel_store_result`, `check`, `acquire`)
  plus `schedfire_key`, `workflow_id` or `lock_name` where they apply.
  `task_defer_failed_redelivered` carries the same fields and the
  backend error as `backend_error_type` and `backend_error`. Alerts on
  the removed events should match `task_deferred_backend_unavailable`
  instead. When Valkey answers such a check with an error instead
  (`WRONGTYPE`, script error), the schedule-fire and cancel checks log
  the new ERROR event `task_gate_backend_error` and requeue in place;
  `error_type` in these and other backend log lines then reads
  `BackendResponseError` (see Added).

### Changed

- **The listener thread is now called `swarmq-log-lsnr`** (was
  `swarmq-log-listener`): Linux keeps 15 characters of a thread name,
  and the forkserver check recognises the thread by that name. Only code
  that looks the thread up by name is affected.
- **`log_records_dropped` is an ordinary record from the listener
  thread** (logger `swarmq.logging`, level WARNING, field `count`),
  written through the listener's own handlers at most once per second
  and once more when it stops. Before, the code that logged printed the
  notice to stderr itself. With the opt-in log queue your handlers
  format it; `log_json` there only selects the format of the new
  `log_drain_timeout_exceeded` line.
- **`log_config_reload_failed` now comes from the logger
  `swarmq.process_manager.reload`** (was `swarmq.process_manager.run`).
  A check of the `--log-config` file that runs into its 30-second limit
  is reported as `log_config_reload_probe_timeout` instead. The other
  reload events `process_manager_reload_execvp`,
  `reload_aborted_alive_children` and `reload_execvp_failed` moved to
  the same logger (was `swarmq.process_manager.supervisor`).
- **The `swarmq` command starts through `swarmq.cli:console_main`**
  (was `swarmq.cli:main`), like `python -m swarmq.cli`. It ends the
  process hard when a log thread is still stuck after the final flush
  (see Added). `swarmq.cli.main()` is unchanged and never exits hard.

### Added

- **`BackendResponseError`** (`from swarmq import BackendResponseError`),
  a subclass of `BackendConnectionError`: Valkey was reachable but
  answered an operation with an error (`WRONGTYPE`, script error). Every
  `except BackendConnectionError` still catches it. The checks before
  execution do not defer a message for it (waiting does not help) but
  requeue it in place, so a persistent error reaches `{queue}.XQ`
  through the delivery limit. Error replies that heal by waiting count
  as an outage and stay a plain `BackendConnectionError`: `OOM`
  (maxmemory reached), `READONLY` (failover), `LOADING`, `TRYAGAIN`,
  `CLUSTERDOWN`, `MASTERDOWN` and `MISCONF`. The schedule-fire and
  cancel checks log
  the new ERROR event `task_gate_backend_error` (fields `gate`, `stage`,
  `error_type`, `error`, plus `schedfire_key` or `workflow_id`).
- **`log_drain_timeout`** (`--log-drain-timeout SECONDS`,
  `SWARMQ_LOG_DRAIN_TIMEOUT`, `SwarmQConfig(log_drain_timeout=...)`,
  `LoggingSettings.drain_timeout`; default 5; precedence CLI > env >
  config > default): upper bound for every flush of the log queues — at
  process exit, when a worker subprocess ends, before the supervisor
  re-executes itself for a reload, in a compute child before each
  result, for the listeners of a `--log-config` file and in
  `SwarmQ.__aexit__`. All listeners of a process share the deadline and
  are stopped in parallel; records not written by then count as dropped.
  A value that is not a finite number greater than 0 and at most
  `threading.TIMEOUT_MAX` ends `swarmq worker` with exit `78` (a
  `ConfigurationError` for `SwarmQConfig`). `shutdown_logging()` takes
  an optional `timeout`.
- **Hard exit of SwarmQ's own processes.** If a given-up log thread is
  still stuck after the flush, the `swarmq` command, worker subprocesses
  and compute children end via `os._exit` with the exit code they would
  have had, instead of aborting with `Fatal Python error` (exit `134`)
  or hanging. Registered `atexit` functions that have not run yet do
  not run then; stdout is not flushed when it shares the stuck file or
  pipe (`2>&1`, journald), and stdout and stderr are flushed for at
  most 0.2 seconds. If the `swarmq` command, a worker subprocess or a
  compute child ends with an exception, its traceback (or the message
  of a `SystemExit`) is written to stderr first, shortened to 4096 bytes
  and only if stderr does not block; Ctrl-C then ends the `swarmq`
  command with `130`. Apps that embed SwarmQ are never ended hard.
- **New events:** `log_handler_errors` (a handler, filter or formatter
  raised; field `count`), `log_drain_timeout_exceeded` (a line written
  directly to stderr when a flush exceeded the deadline, only if stderr
  does not block; fields `count`, `timeout_s`),
  `log_config_reload_probe_timeout` (fields `path`, `timeout_s`,
  `hint`), `forkserver_restarted_multithreaded` (ERROR; fields `pid`,
  `threads`, `thread_names`, `hint`), `compute_fork_source_rejected`
  (ERROR; fields `reason`, `hint`) and `worker_fork_source_rejected`
  (supervisor, WARNING; fields `worker_id`, `pid`, `exitcode`).
- **The forkserver thread check names the threads** it found and the
  likely cause: the opt-in log queue, a logging `QueueListener` of your
  app started at import time, or else a BLAS/OpenMP computation. It
  also runs after a silent restart of the forkserver, before the next
  compute child gets work. A restarted forkserver with more than one
  thread is rejected: `/ready` reports the pool as unhealthy, the worker
  logs `compute_fork_source_rejected`, stops consuming and shuts down
  gracefully (the running message goes back to the queue without a
  retry count). A worker subprocess then exits with code `71`, and the
  supervisor restarts it with a fresh forkserver (within the restart
  budget). With an embedded CPU pool, `Worker.start()` raises
  `swarmq.supervision.ForkSourceRejected` after the shutdown; your app
  decides whether to restart.

### Fixed

- **Agent spec: exit codes.** `errors.json` gave `ConfigurationError` the
  CLI exit code `1`; `swarmq worker` ends with `78`. `cli.json` now also
  lists `78` and `130`.

- **A scheduler fire is no longer lost when Valkey fails right after the
  worker claimed it.** The worker claims a fire in Valkey before it runs
  the task. When a later check (cancel check, delivery count) ended the
  delivery, the claim was never released; when the release itself
  failed (Valkey down), the deferred, retried or requeued copy saw the
  claim, took itself for a duplicate and acked itself, so the fire never
  ran (the claim lives up to `SWARMQ_SCHEDULER_INFLIGHT_TTL`, default
  300 s). Now the claim stores an owner token, every
  copy carries it in the new header `x-swarmq-schedfire-claim`, and a
  copy that finds its own token takes the claim over (the token
  rotates, so a broker redelivery of that copy is still a duplicate).
  Real duplicates (a second publish of the fire, a redelivery of the
  original) are still dropped. Checks that end the delivery before the
  task, or end it with an exception, now also release the claim,
  owner-checked. The token is chosen before the claim call: when its
  reply is lost (timeout, reset) Valkey may or may not have written the
  claim, so the copy carries the new token and the ones it was offered
  (comma-separated, newest first, at most 16) and takes the claim over
  either way. A requeue in place (`nack(requeue=True)`: shutdown, a
  publish fallback, an error other than an outage) cannot carry a token,
  so the claim is released before the requeue. If that release fails
  (Valkey down, or a third-party store raises any exception), the worker
  publishes a copy carrying the token to the delay queue instead, with
  the backoff of a deferral, and acks the original once the broker
  confirmed the copy (WARNING `task_requeued_as_copy`); only if that
  publish fails too does it requeue in place
  (`task_copy_failed_redelivered`), and the fire can then still be
  dropped as a duplicate while the claim lives. A claim held by a
  delivery whose checks are cancelled (shutdown) is released even if a
  second cancel arrives during the release. The worker offers at most 16
  tokens from the header to the claim script. New Lua script
  `sched_fire_claim.lua`; `ScheduleStore.claim_fire` takes the new
  optional keywords `owner` and `takeover` (one token or a sequence of
  tokens), `ScheduleStore.release_fire_claim` the new optional keyword
  `owner`, and `ScheduleStore.commit_fire` the new keyword `fired_ts_ms`
  (see below). Worker and scheduler always pass them: a third-party
  store that does not accept them raises `TypeError` on every fire.
  **Rolling upgrade:** the protection needs every worker on 0.9.0; a
  worker of an older version neither writes nor recognises the token,
  so a copy it handles can still be dropped as a duplicate until all
  workers run 0.9.0.
- **Retry copies no longer inherit the broker's delivery counters.** The
  copy published for a retry, a rate-limit hit or a lock conflict took
  `x-delivery-count` and `x-acquired-count` from the original.
  RabbitMQ 4 keeps a publisher-set `x-delivery-count` on delivery, and
  RabbitMQ 3.13 also counts requeues; a copy whose inherited count
  reached `SWARMQ_DELIVERY_LIMIT - 1` went to `{queue}.XQ` as
  `WorkerHungError` although the task had retries left. These copies are
  new messages with their own delivery budget now, like the deferral
  copies.
- **A backend outage before execution no longer drains the delivery
  budget.** When a check that runs before the task (schedule-fire dedup,
  cancel check, rate limit, lock) could not reach Valkey, the worker
  requeued the message in place (`nack(requeue=True)`), roughly every
  1.5 s (refused connection) to 5 s (blackhole). On a broker that counts
  requeues against `x-delivery-limit` (RabbitMQ 3.13), the broker moved
  the messages to `{queue}.XQ` after about 20 rounds, without
  `TASK_DLQ`, error result or log line; on RabbitMQ 4 the message cycled
  without backoff. Now the worker publishes a copy to the delay queue
  and acks the original only after the broker confirmed the copy. The
  wait grows from `SWARMQ_BACKEND_DEFER_BASE_DELAY` (new, default 1 s)
  and doubles per deferral up to `SWARMQ_BACKEND_DEFER_MAX_DELAY` (new,
  default 60 s), varied by ±20 % so messages deferred together do not
  return together; the copy keeps all SwarmQ headers but not the broker's
  delivery counters (new header `x-swarmq-backend-defer-count`). The
  count starts over once the backend has answered: copies for a retry,
  a rate-limit hit or a lock conflict do not carry it, and a message the
  broker hands back after a delivery that did not defer (requeue after
  the checks, crash; recognised by `x-delivery-count` or
  `x-acquired-count` on quorum queues) waits the base delay again. It is
  not a retry: `max_retries` stays untouched and no `TASK_RETRYING`
  fires. Each deferral logs the new WARNING event
  `task_deferred_backend_unavailable` (fields `gate`, `stage`,
  `defer_count`, `delay_ms`, `error_type`, `error`; see Changed). If
  the broker cannot take the copy either, the worker requeues in place
  as before and logs `task_defer_failed_redelivered`.
  Only an outage defers: an error reply from Valkey
  (`BackendResponseError`, see Added) is requeued in place as before, so
  a persistent error still ends in `{queue}.XQ` through the delivery
  limit instead of being deferred forever.
  `ValkeyRateLimitStore.check` now raises `BackendConnectionError` for
  connection and timeout errors and for the error replies that heal by
  waiting (see `BackendResponseError` under Added), and
  `BackendResponseError` for other error replies (before, it raised the
  raw `valkey.exceptions.ResponseError`). A third-party `RateLimitStore` gets the
  deferral only if its `check` raises `BackendConnectionError` for
  transport errors; any other exception keeps the requeue in place.
- **Checks before execution no longer hang on an unresponsive
  backend.** The rate-limit check, the lock acquire, the schedule-fire
  dedup check and claim, and the result read of a cancelled workflow
  step had no time limit of their own. When Valkey stopped answering
  without closing the connection (a blackhole, for example a dropped
  network path), the delivery waited for the Valkey client's socket
  timeout (10 s per attempt, plus its retries). These calls are now
  bounded by `SWARMQ_BACKEND_OP_TIMEOUT` (default 5 s); a timeout counts
  as an unreachable backend, and the message is deferred as described
  above. The result notification after a pre-start cancel and the
  completion or release of a schedule-fire claim are bounded the same
  way; a timeout there logs `publish_notification_failed` or
  `schedfire_finalize_failed` and the worker carries on. A lock acquire
  that times out after Valkey executed it leaves the lock held until its
  TTL, as a socket timeout did before.
- **A scheduler fire no longer overwrites a schedule change made while
  it fires.** A `swarmq schedule remove` (disable or `--permanent`) or a
  redeploy with a new cron that landed between poll and commit was
  undone: the schedule came back enabled, was recreated, or got its old
  cron back. The commit now applies only while the schedule still has
  the score that was fired; `sched_commit.lua` checks this atomically
  (new optional `ARGV[5]`, backwards compatible), and
  `ScheduleStore.commit_fire` takes a new optional keyword
  `fired_ts_ms`. A schedule removed and recreated with the same next
  fire time between poll and commit cannot be told apart from the fired
  one; the commit applies there.
- **An aborted worker start no longer leaves the embedded scheduler
  running.** Cancelling `start_worker()`, or a failure after the
  scheduler had started (executor start, queue declaration,
  `WORKER_ONLINE`), left its election and poll tasks running; the
  process kept renewing the scheduler leader lock and no other worker
  could become leader.
- **A full log queue no longer stalls the code that logs.** With a
  blocked stderr the drop notice was written by the caller — on the
  event loop — which stood still for seconds (2.9 s measured; without a
  reader of stderr indefinitely). With a closed stderr a log call raised
  `ValueError` into application code. The caller now only counts.
- **A raising handler, filter or formatter no longer kills the listener
  thread.** Afterwards every record was silently dropped, and the flush
  at exit hung forever. The same applied to the queue handlers of a
  `--log-config` file, where a full bounded queue also printed a
  traceback in the caller. Errors are now caught and counted
  (`log_handler_errors`), and a dead listener restarts with the next
  record.
- **Flushing the log queues can no longer hang a process.** A blocked
  stderr or a slow handler made every flush last as long as the target:
  the exit of every SwarmQ process, the supervisor before a reload (its
  workers were already stopped), and a compute child, whose result then
  never arrived. Every flush is now bounded by `log_drain_timeout`.
  Records left in a queue whose listener is not running are written
  within the deadline or, after it, counted as dropped; until the flush
  returns, no record starts a listener that the flush would give up at
  once.
- **`SwarmQ.__aexit__` with `log_queue=True` no longer blocks the event
  loop** while the opt-in queue drains (up to 12 s measured with a slow
  handler). If it is cancelled while waiting, the cancellation
  propagates at once and the flush continues in the background up to
  the deadline.
- **`SwarmQ.__aexit__` with `log_queue=True` no longer raises when the
  event loop's default executor is already shut down**
  (`loop.shutdown_default_executor()`, as `asyncio.run` does at the
  end). The flush raised `RuntimeError: Executor shutdown has been
  called` into your code; it now waits on a thread of its own, bounded
  by `log_drain_timeout` as before.
- **CPU pool with the opt-in log queue:** a `swarmq.*` record logged at
  import time while `SwarmQ(log_queue=True)` or `SWARMQ_LOG_QUEUE=1` was
  active (also from calling `setup_dishka()` twice) started a listener
  thread in the forkserver. The worker then refused to start with exit
  `78` and a hint that blamed a BLAS computation. The forkserver now
  writes such records synchronously.
- **CPU pool with your own logging `QueueHandler`:** a `QueueListener`
  started at import time made the worker refuse to start with the same
  misleading hint; one started in the `--configure` hook was never
  started in the compute children, so all their lines were lost although
  `logging_reconfigured_by_app` was reported. SwarmQ now starts it in
  every child, and the startup message names the app listener.
- **CPU pool logging matched neither the IO pool nor the
  documentation** (`basicConfig` won against `--log-format json`,
  `--log-config` and `--log-level`); see Changed (breaking).
- **A silently restarted forkserver was never checked for threads.**
- **Hot reload with `--log-config`:** the file was checked inside the
  `SIGHUP` signal handler, which froze supervision for up to 30 seconds
  (dead workers were not replaced, autoscaling and the control channel
  paused). A `SIGTERM` during the check turned into a restart instead of
  a shutdown, a second `SIGHUP` started a second check in parallel, and
  the file watcher triggered again during the check. A check that ran
  into its time limit, or was interrupted by Ctrl-C, was reported as an
  invalid configuration. The check now runs outside the signal handler,
  in its own process group; several `SIGHUP`s lead to one check.
- **`SIGTERM` always ends the supervisor.** A `SIGHUP` that arrived
  shortly after a `SIGTERM` restarted the supervisor instead of shutting
  it down — in every deployment, with or without `--log-config`. A
  `SIGTERM` during the worker stop for a reload is honoured as well, and
  one that another thread of the supervisor receives right before it
  re-executes itself stays pending and ends the new supervisor.
- **Forking from task code:** after `os.fork()` the queue handlers of a
  `--log-config` file lost every record in the child (with a bounded
  queue, each record printed a `Logging error`). A fork while a listener
  was writing could leave the child with `RuntimeError: reentrant call`
  on every record. The child now gets fresh queues, and the fork waits
  up to 50 ms for running writes. A `multiprocessing` queue (or manager
  proxy) stays shared with the parent: the child starts no listener of
  its own, and the parent's listener writes the child's records. Under a
  raw `os.fork()` SwarmQ restarts the queue's feeder thread in the
  child, and a manager proxy opens its own connection to the manager
  instead of sharing the parent's. A target that stays busy is waited
  for only at the first fork, not again at every following one.
- **Records logged while the interpreter shuts down** (for example
  "Future exception was never retrieved" or from a `__del__`) caused a
  `--- Logging error ---` and were lost; they are now written
  synchronously.
- **A `multiprocessing` queue in a `--log-config` file at process
  exit:** the exit hook of `multiprocessing` usually runs before
  SwarmQ's and closed the queue's pipe while the listener still read
  from it. Records not yet read were lost, and the listener died with an
  `EOFError`/`OSError` traceback — restarted over and over, often
  hundreds of tracebacks. SwarmQ now flushes these listeners before
  `multiprocessing` closes the queue. A record that still arrives after
  that (for example from an `atexit` function registered before
  `multiprocessing` was imported) counts as dropped, without a
  traceback, and a queue that is already closed when the flush discards
  its records after the deadline no longer raises `EOFError`/`OSError`
  out of `shutdown_logging()`.

## [0.8.0] — 2026-10-06

### Changed (breaking)

- **`SwarmQ()` no longer configures logging.** Creating a `SwarmQ` app
  leaves root handlers, root level and the structlog configuration
  untouched (before, it installed its own root handler and called
  `structlog.configure`). SwarmQ logs through named loggers
  `swarmq.<module>` into stdlib `logging`: the event name is the record
  message, the fields are `LogRecord` attributes. Without any logging
  configuration, WARNING and above still reach Python's `lastResort`
  handler — SwarmQ deliberately installs no `NullHandler`. An app whose
  root level is WARNING and that relied on `SwarmQ()` for its log output
  no longer sees SwarmQ's INFO events. **Migration:**
    - Plain stdlib app: configure logging yourself, e.g.
      `logging.basicConfig(level=logging.INFO)`, with a formatter that
      shows the fields — a JSON formatter that reads the record
      attributes, or `swarmq.logging.build_formatter(json=True, colors=False)`.
    - structlog app: add `structlog.stdlib.ExtraAdder()` to the
      `foreign_pre_chain` of your `ProcessorFormatter`. Without it,
      SwarmQ's fields are silently dropped.
    - `log_level` and `log_json` of `SwarmQConfig` keep working as fields
      but now only steer the new opt-in log queue (`log_level` sets the
      level of the `swarmq` logger, `log_json` the format of the drop
      notice).

    Recipes: [Logging reference](logging.md#integrating-with-your-app).

- **Log output contract.** Every line from processes that `swarmq worker`
  starts carries the new key `logger` next to `event`, `level` and
  `timestamp`. Renamed events: `swarmq.dedup.detected` → `dedup_detected`,
  `swarmq.dedup.rollback_failed` → `dedup_rollback_failed`,
  `publish_notification failed` → `publish_notification_failed`,
  `publish_workflow_notification failed` →
  `publish_workflow_notification_failed` (field `wf_id` → `workflow_id`),
  `health_server_started`/`_stopped`/`_request_failed` and
  `metrics_server_started`/`_stopped`/`_request_failed` →
  `http_server_started`/`http_server_stopped`/`http_server_request_failed`
  with the field `server` (`health_server`/`metrics_server`). In
  `watch_dir_inside_venv_or_stdlib` the field `module` is now
  `module_name`; in general a field whose name collides with a
  `LogRecord` attribute is prefixed with `swarmq_` (`name` →
  `swarmq_name`). Events that report an exception carry `error_type` and
  `error`; a traceback is attached only for unexpected errors. This now
  includes `task_sent_to_dlq` (`error_type` is the class name of the
  exception that sent the task to the DLQ, `WorkerHungError` when the
  delivery limit was reached). Grep-based alerts and dashboards need
  updating.
- **Log levels follow one rule** (ERROR: data loss or a failed task or
  process; WARNING: degradation that heals itself; INFO: process and task
  lifecycle; DEBUG: diagnosis). `check_backend_failed` and
  `check_broker_failed` INFO → WARNING; `task_sent_to_dlq` WARNING →
  ERROR; cases healed by a TTL ERROR → WARNING:
  `shutdown_lock_release_failed`, `scheduler_stop_failed`,
  `lock_release_failed`, `cancel_check_backend_error`,
  `workflow_cancel_check_backend_error`, `cancel_store_result_failed`,
  `schedfire_dedup_backend_error`. `worker_died` is INFO for a clean exit
  or an expected shutdown, WARNING otherwise.
- **Logging settings resolve once, in the supervisor.** Precedence is
  CLI flag > env var > `SwarmQConfig` > default. The supervisor passes
  the resolved settings to its worker subprocesses and compute children,
  which no longer read the environment; a flag now wins in every process.

- **CLI options now win over `SWARMQ_*` environment variables in all processes,
  for all `swarmq worker` options.** Before, the environment won inside the
  worker processes for `--queues`, `--concurrency`, `--pool`, `--burst`,
  `--burst-idle-timeout`, `--priorities`, `--metrics-port` and `--health-port`
  (e.g. `SWARMQ_CONCURRENCY=5` beat `--concurrency 20`), while the supervisor
  used the flag. Now every option resolves once as CLI > environment >
  default, and supervisor, worker processes, compute children and `--check`
  see the same values. Without the flag the environment variable still applies.

### Added

- **Logging options for `swarmq worker`:** `--log-config PATH`
  (`SWARMQ_LOG_CONFIG`; a `dictConfig` file in JSON, YAML with PyYAML;
  `disable_existing_loggers` defaults to `false`; validated once by the
  supervisor and re-validated before a reload, which is aborted with
  `log_config_reload_failed` if the file became invalid),
  `--no-configure-logging` (`SWARMQ_CONFIGURE_LOGGING=false`),
  `--logger-level NAME=LEVEL` (repeatable; `SWARMQ_LOGGER_LEVELS`),
  `--log-queue-size N` (`SWARMQ_LOG_QUEUE_SIZE`, default 10000),
  `--log-lifecycle-level LEVEL` (`SWARMQ_LOG_LIFECYCLE_LEVEL`, default
  DEBUG) and `SWARMQ_LOG_FORMAT` (wins over the alias `SWARMQ_LOG_JSON`).
  `CRITICAL` is accepted for every level. Invalid values or
  contradicting combinations (`--log-config` with an explicit format or
  with `--no-configure-logging`) end the run with exit 78 and one
  `worker_config_error` line; a broken config file raises the new
  `LoggingConfigError`.
- **Task lifecycle events** on the logger `swarmq.task`: `task_received`
  and `task_started` (lifecycle level, default DEBUG), `task_succeeded`,
  `task_failed` and `task_cancelled` (INFO, with `duration_ms`;
  `task_failed` with `error_type`/`error`; `compute_child` for results
  from the CPU pool). Silence them with
  `--logger-level swarmq.task=WARNING`.
- **Task context on every event inside a task:** `task_id`, `task_name`,
  `queue`, `worker_id`, plus the new `workflow_id` (when valid) and
  `attempt` (`retry_count + 1`), and `compute_child` in compute children.
  The context is bound per message and restored afterwards; context
  variables of your app are no longer cleared.
- **Opt-in log queue for embedded apps:** `SwarmQConfig(log_queue=True)`
  / `SWARMQ_LOG_QUEUE=true` routes `swarmq.*` events through the
  background thread `swarmq-log-listener` to the app's root handlers, so
  slow handlers no longer block the event loop. Off by default.
- **"App wins" rule:** a task module or `--configure` hook that replaces
  the root handlers keeps them; SwarmQ routes its own events through its
  queue to them and logs `logging_reconfigured_by_app` (re-enabling
  `swarmq.*` loggers the app's `dictConfig` disabled). Added handlers are
  reported as WARNING `logging_handlers_added_by_app`.
- **Dropped log records are visible:** a full log queue drops instead of
  blocking (WARNING and above keep 10 % headroom), reports
  `log_records_dropped` at most once per second (a JSON line in JSON
  mode) and counts the drops in the metric
  `swarmq_log_records_dropped_total`.
- **New events** `worker_connection_error`, `worker_recycle_ack_timeout`
  (exit 75), `log_config_reload_failed`, `logging_reconfigured_by_app`,
  `logging_handlers_added_by_app`, `log_records_dropped`, each as a
  structured line in the configured format.
- **Public logging API** in `swarmq.logging`: `get_logger`,
  `build_formatter`, `configure_logging`, `shutdown_logging`,
  `load_log_config` and `LoggingConfigError`. New reference page:
  [Logging](logging.md).
- **More exceptions importable from `swarmq`:** `LoggingConfigError`,
  `SoftTimeoutError` and `PublishIndeterminate` (the same classes as in
  `swarmq.exceptions`).

### Deprecated

- `swarmq.logging.setup_logging` (entry points use `configure_logging`)
  and `swarmq.logging.flush_logging` (now an alias of
  `shutdown_logging`).

### Fixed

- **Hot reload works after `python -m swarmq.cli`.** The supervisor
  re-executed itself from `sys.argv`; after `-m` that is the path of
  `swarmq/cli/__main__.py`, so the new image ran the file as a script —
  without the working directory on `sys.path` (`--module ... cannot be
  imported`) and with spawned workers failing in the bootstrapping phase.
  The re-exec now uses `sys.orig_argv`, which keeps `-m` and interpreter
  flags; `swarmq/cli/__main__.py` only calls `main()` as the main module.
  Starting through the `swarmq` command is unchanged.

### Dependencies

- `structlog` is now constrained to `>=24,<27`: `swarmq.logging.get_logger`
  depends on the call depth of `structlog.stdlib.BoundLogger`, which the
  test suite pins for 26.x.

## [0.7.0] — 2026-09-22

### Added

- **Control channel for worker groups.** `swarmq worker --max-processes N`
  enables it and declares the headroom; `--group LABEL` names the group
  (default: the sorted queue set). A setpoint `{"min": M, "max": K}` under
  `sq:control:setpoint:<group>` is read before the first worker spawns and
  every 5 seconds afterwards: a higher lower bound starts workers at once,
  a lower upper bound drains the surplus gently, a deleted key restores the
  start-up arguments, and the fixed form `N..N` switches autoscaling off.
  Out-of-range, inverted, unreadable and range-on-fixed-group setpoints are
  rejected with a reason; a setpoint never makes the supervisor fail to
  start. Each supervisor reports its effective bounds, their source, the
  last setpoint, a rejection reason and its slot counts under
  `sq:control:status:<group>:<supervisor>` with an expiry, so a controlling
  service can tell rejected from not-yet-applied from backend-unreachable.
  Slot table, observability port ranges and (with `--pool cpu`) the default
  compute slots are sized from the headroom at start-up; the pre-flight
  rejects a headroom below `--processes` or the `--autoscale` upper bound,
  or one that would overbook the assigned CPUs. Without `--max-processes`
  nothing changes. See the [CLI reference](cli.md#control-channel).

## [0.6.2] — 2026-09-15

### Changed

- **README refreshed.** The package description on PyPI listed features under
  "What's planned next" that have long shipped (priorities, expiry, burst,
  supervisor, autoscaling, metrics, health, hot reload, agent spec). It now
  describes the current feature set, the CPU worker mode, the supervisor
  defaults and the two remaining roadmap items.
- **Dependencies refreshed (2026-09-15).** Verified against aio-pika 10.0.1
  and aiormq 7.0.0 (pamqp 4), structlog 26.1, valkey with libvalkey 4.1,
  msgpack 1.2, prometheus-client 0.26, watchfiles 1.2; the declared lower
  bounds are unchanged, so existing installations keep working. Dev
  toolchain: mypy 2.3, ruff 0.16 (new rule `RUF036`, `None` last in type
  unions), pytest 9.1, hypothesis 6.168. Unit, integration, e2e, property
  and contract suites pass on the new set.

### Fixed

- **Worker subprocesses die with their supervisor, also on SIGKILL.** A
  supervisor that was killed hard (test teardown, OOM killer, `docker kill`)
  left its workers running as orphans; they kept renewing the scheduler
  leader lock by heartbeat and blocked every later leader in the same
  database. The child process target now arms `PR_SET_PDEATHSIG` with
  SIGTERM (Linux), so the death of the supervisor triggers the same graceful
  shutdown as an orderly stop; a child whose supervisor died between spawn
  and arming exits instead of starting as an orphan. On platforms without
  `prctl` the behaviour is unchanged.

## [0.6.1] — 2026-09-15

### Fixed

- **`TASK_REJECTED` carries the task identity whenever the body decoded.**
  For `reason="unregistered_task"` and `reason="middleware_chain_factory"`
  the signal now includes `task_id` and `task_name`; before, `task_id` was
  `None` on both paths although the worker had already decoded the message
  and logged the id. Without it a rejected message could not be found in
  `.XQ` with `swarmq dlq inspect`. `body_too_large` and `undecodable` still
  report `None`, as documented. Found by the load-test suite at pin v0.6.0
  (its backlog entry B14).
- **The supervisor unblocks the inherited signal mask before it runs any
  worker entry.** `ProcessManager` blocks SIGINT/SIGTERM/SIGHUP while it
  spawns children, and `spawn` hands that mask down. swarmq's own child
  entry lifted it; a custom `worker_entry` passed to `ProcessManager` did
  not and never saw the SIGTERM its shutdown watcher sent, so a graceful
  stop ended only with the orchestrator's SIGKILL after the full stop
  timeout. The child process target now lifts the mask itself, before the
  entry runs. Found by the load-test suite (backlog entry B15).

## [0.6.0] — 2026-09-14

### Changed (breaking)

- **`worker_id` has a fixed shape.** In every signal, metric label and log
  context the worker identity is `<base>:<slot>` under the supervisor and
  `<base>:p<pid>` for an embedded `start_worker()`. `<base>` is
  `SWARMQ_WORKER_ID`, otherwise the hostname. Previously it was an opaque
  object id that changed with every restart. Applies to `WORKER_*`,
  `SCHEDULER_*`, `WORKER_RECYCLING` (also `kind="compute_child"`) and the
  labels of `swarmq_worker_uptime_seconds` and `swarmq_scheduler_leader`.
  Dashboards can read the slot; uptime restarts when a slot is recycled.
- **`--metrics-port` and `--health-port` are base ports.** The worker in
  slot `s` binds `base + s`; multi-process mode with a fixed port is no
  longer refused. Refused instead: a range `[base, base + N)` that runs past
  65535, or metrics and health ranges that overlap. `N` is `--processes` or
  the upper bound of `--autoscale`. Prometheus needs one target per slot;
  `swarmq worker --check` prints both ranges (`metrics_ports`,
  `health_ports`).
- **Exit code 78 for configuration errors, on every level.** A
  `ConfigurationError` or `ModuleNotFoundError` in the parent process of
  `swarmq worker` emits the structured log line `worker_config_error` and
  exits 78 (previously 1). A worker child that exits 78 — including an
  `ImportError` while importing the task module — ends the supervisor with
  78 as well (previously 1). Connection errors to broker or backend stay at
  1. Orchestrators can now tell "do not restart" from "try again".
- **Workflow steps run through the enqueue middleware.** Every step the
  `WorkflowAdvancer` publishes, and every step of the initial front in
  `Client.apply`, passes `pre_enqueue` before the publish and `post_enqueue`
  after it, in the same chain as `Client.schedule`, with
  `delivery_mode="at_least_once"` and the extra context keys `workflow_id`
  and `step_path`. `pre_enqueue` runs for all steps of a front before the
  first is published; if it raises, no step of that front is published, the
  workflow is marked failed (`error.type` = exception name) and the message
  is acked. A `pre_enqueue` that assumes client context can now reject
  workflow steps.
- **The `<workflow>` pseudo-event is gone.** `Client.apply` no longer emits
  `TASK_ENQUEUED` with `task_name="<workflow>"`. Each step emits
  `TASK_ENQUEUED` with its real identity, followed by
  `WORKFLOW_STEP_ENQUEUED`.

### Added

- **`swarmq worker --configure MODULE:CALLABLE`**: the callable receives
  the `SwarmQ` app after construction and before the worker starts; an
  awaitable return value is awaited. Register middleware and signal
  handlers without writing your own worker entry point. Runs exactly once
  per worker process, never in the forkserver and never in `schedule`,
  `dlq` or `inspect`. A failing hook is a configuration error (exit 78).
  `--check` validates the `MODULE:CALLABLE` syntax and that the module can
  be found, without importing it.
- **Signal `TASK_REJECTED`** with `queue`, `reason`, `task_id`, `task_name`,
  `error` for every message the worker hands to the broker's dead-letter
  exchange without diagnostic headers. `reason` is one of `body_too_large`,
  `undecodable`, `unregistered_task`, `middleware_chain_factory`,
  `dlq_publish_failed`; `task_id` and `task_name` are `None` when the body
  never decoded. Counter `swarmq_tasks_rejected_total{queue,reason}`.
  `swarmq_tasks_total{outcome="dlq"}` still counts only `TASK_DLQ`. The
  [dead letter guide](../guides/dead-letter-queues.md#three-ways-into-the-xq)
  names the three ways into `.XQ`.
- **Signal `WORKFLOW_STEP_ENQUEUED`** with `task_id`, `task_name`, `queue`,
  `workflow_id`, `step_path` after every published workflow step, in
  addition to `TASK_ENQUEUED`.
- **`SWARMQ_WORKER_ID`** (also `SwarmQ(worker_id=...)`) sets the base of the
  worker identity; default is the hostname.
- **Slot table in the supervisor.** Slots are `0..N-1`. A replacement for a
  crashed, hung or recycled worker gets the same slot and therefore the same
  ports; scale-up takes the lowest free slot, scale-down removes the
  youngest worker by start time. A slot whose restart budget is exhausted
  is locked and never backfilled. During a rolling replacement the old and
  the new process share the slot; the autoscaler counts it once. The
  replacement re-binds its port with retries; the budget covers the SIGKILL
  of a hanging predecessor and the port release of a recycling one.

### Fixed

- **Lost workflow state updates under parallel branches.** Every write of
  the workflow state by the advancer is a compare-and-set against the
  version it read (field `version`; documents from 0.5.x count as 0). On a
  conflict the advancer re-reads, merges its local changes (`step_tasks`
  additively; `status`, `final_result_task_id` and `error` when it is
  terminal locally) and writes again, up to 25 rounds; a fresh terminal
  status ends the advance without a write. Above the limit the message is
  redelivered. The previously `xfail`ed test for the lost step marker is now
  a regression test.
- **Failure marking distinguishes error types.** When marking a workflow as
  failed hits a `BackendConnectionError`, the message is redelivered
  (at-least-once) instead of acked; any other error acks and logs the
  failure with the workflow id.
- **Configuration errors in the child are structured log lines.** The
  error text appears in the configured format (`human` or `json`), not as a
  raw stderr line; the supervisor logs `worker_config_error`. A child that
  exits 75 (recycle without acknowledgement) is logged as
  `worker_recycle_noack` and restarted within the budget.
- **The log formatter no longer prints tracebacks.** If formatting a record
  fails (for example a raising `__repr__` in `record.args`), the formatter
  emits one line in the configured format with
  `event="log_record_unformattable"`, logger name, level and error text.
- **No `RuntimeWarning` per compute child.** A worker started with
  `python -m package.module` in CPU mode re-imported its main module in
  every compute child, with a `runpy` warning each time. The forkserver
  preload now registers it once as `__main__`; child start stays at about
  5 ms.

## [0.5.1] — 2026-09-06

### Fixed

- A failure while *calling* a task's `run()` — wrong arguments (`TypeError`),
  or a `run()` that is not a coroutine — is now an ordinary task error and
  follows the retry/DLQ path. Previously the exception escaped the message
  handler and the delivery stayed unacknowledged until the channel closed.
- CPU mode: the forkserver preload now includes the loop process's main
  module and the compute-child runtime. Without them every fresh compute
  child re-imported both (about 60 ms per task); child start is back to
  about 5 ms as measured in the feasibility study. `SlotPool.recent_spawn_ms`
  records spawn and handshake timings for diagnostics.

## [0.5.0] — 2026-09-03

### Changed (breaking)

- **The supervisor is the default.** `swarmq worker` always starts a
  `ProcessManager` with the worker as a subprocess, also with `--processes 1`.
  Two processes instead of one; `SIGHUP` now means reload (as it did with
  `--processes`); a crashing worker is restarted internally within the budget
  (5 per hour) before the process exits, so alert on the `worker_died` log
  event rather than on container restarts. `--burst` still exits 0 once every
  worker drained.
- **Startup check for ineffective `FromDishka` annotations.** A parameter such
  as `db: FromDishka[Db] | None = None` never injected anything and silently
  stayed `None`; the worker now refuses to start with a `ConfigurationError`
  naming task and parameter — in every pool mode. Declare
  `db: FromDishka[Db]` without `| None` and without a default.
- **Configuration errors end the run after one attempt.** A worker subprocess
  that fails its startup checks exits with code 78; the supervisor does not
  restart it and exits 1 instead of burning the restart budget.

### Added

- **CPU mode** (`--pool cpu`, `SWARMQ_POOL`): task bodies run in compute
  children forked from a pre-warmed `forkserver`; the loop process never
  computes. `hard_timeout` and cancel kill the child's process group;
  `--concurrency` means compute slots and defaults to the CPUs assigned to the
  worker (affinity and cgroup quota) divided by `--processes`. Fresh child per
  task without DI, reused child with RSS/task-count recycling for `FromDishka`
  tasks. See the [CPU mode guide](../guides/cpu-mode.md).
- **Heartbeat to the supervisor**: a worker whose event loop stops writing its
  liveness timestamp for `SWARMQ_HEARTBEAT_TIMEOUT_S` (default 60 s) is
  replaced (SIGTERM, then SIGKILL), with its own budget of 2 × delivery limit
  per hour. A message that keeps hanging workers is dead-lettered by the worker
  from the penultimate delivery on (`x-delivery-count`), with an error result,
  workflow failure, unique release and `TASK_DLQ` — not silently by the broker.
- **Self-recycling of the IO worker** (`SWARMQ_RECYCLE_RSS_MB`,
  `SWARMQ_RECYCLE_MAX_TASKS`, opt-in): after a task the worker asks the
  supervisor for a replacement, waits for the acknowledgement and drains
  without forced cancellation.
- Signal `WORKER_RECYCLING` and metric `swarmq_recyclings_total{kind,reason}`.
- New settings: `SWARMQ_HEARTBEAT_INTERVAL_S`, `SWARMQ_HEARTBEAT_TIMEOUT_S`,
  `SWARMQ_CONSUMER_TIMEOUT_S` (startup warning when a `hard_timeout` exceeds
  it), `SWARMQ_MALLOC_TRIM_THRESHOLD`, `SWARMQ_MALLOC_ARENA_MAX`,
  `SWARMQ_COMPUTE_THREAD_LIMIT`. `/ready` reports the forkserver in CPU mode;
  startup warns when `queue_type` is not `quorum` (no delivery limit).

## [0.4.1] — 2026-08-30

### Added

- **`SWARMQ_DELIVERY_LIMIT` / `SwarmQConfig.delivery_limit`** (default `20`,
  unchanged behaviour) — the delivery budget quorum queues are declared with
  (`x-delivery-limit`). Once a message has been delivered that many times the
  broker itself dead-letters it; the budget is the safety net that stops a task
  which reliably kills the process (SIGSEGV/OOM) from being redelivered forever
  without ever reaching `max_retries`.

  The default covers three to four task-level retries per delivery plus headroom
  for broker hiccups. That headroom got narrower with the publish deadline
  introduced in 0.3.0: a publish that breaks its deadline inside the worker
  redelivers the in-flight message instead of acking it, so a prolonged broker
  outage now spends deliveries it previously did not. Raise the value to ride
  out longer outages, lower it to force a tighter delivery window. Classic
  queues ignore the setting — they do not know the argument.

## [0.4.0] — 2026-08-30

### Changed (breaking)

- **A group whose member is itself a `chain`, `group` or `chord` no longer
  returns a truncated result list.** Since 0.2.0 such a member was dropped
  from the list without any error — `group(a, chain(b, c))` returned a
  one-element list, and `handle.children()` reported every step complete at
  the same time. The same code builds the input a chord body receives, so a
  chord over such a header computed on a short list rather than returning
  one.

  **Affected versions: 0.2.0 and 0.3.0.** A truncated list cannot be told
  apart from a complete one after the fact — stored results give you no
  signal. The only answer is the workflow *definition*: **a run was affected
  if its topology contains a `group`, or a chord header, with a member that
  is itself a `chain`, `group` or `chord`.** If you have such workflows,
  their past results may be wrong and anything computed from them with them.

- **An incomplete member list is now an error, for every group.** Where the
  aggregation previously skipped a missing member in silence, it now raises
  `WorkflowValidationError` naming the group path and both counts. This
  guard sits at the shared fan-in for *all* groups and chord headers, flat
  ones included — so a member step marker lost for any other reason (a step
  lost in the publish-then-mark window, for instance) now fails loudly where
  it previously produced a result. That is the point: a wrong answer you
  cannot detect is worse than a visible failure.

- **Drain nested workflows before upgrading.** A workflow already running
  across the upgrade will hit the new guard and fail instead of returning
  its (wrong) truncated result. This applies per fleet, not per process:
  until every worker runs the new version, an old worker can still truncate
  silently.


## [0.3.0] — 2026-08-30

**Never published to PyPI.** Tagged, but it still
carried the silently truncated `group` result list fixed in 0.4.0 — shipping
it would have meant knowingly releasing a wrong-answer bug. The entry stays
for the record; upgrade from 0.2.0 straight to 0.4.0 and read both sections.

### Changed (breaking)

- **`swarmq worker` refuses multi-process mode with a fixed observability
  port.** Requesting more than one worker process (`--processes N` with
  N > 1, or `--autoscale MIN,MAX` with MAX > 1) while a metrics or health
  port is in effect now fails at startup instead of silently running
  under-provisioned. Every child bound the same port, so only the first
  survived while the rest burned their restart budget. The check uses the
  resolved value, so `SWARMQ_METRICS_PORT` / `SWARMQ_HEALTH_PORT` are
  refused just like the flags, and `swarmq worker --check` reports the same
  conflict. Drop the fixed port for multi-process mode, or run a single
  process with it — a port per child does not exist yet.
- **New `PublishIndeterminate` exception** (a subclass of
  `BrokerConnectionError`, so existing handlers are unaffected) for a publish
  that exceeded its deadline while waiting for the broker's confirm. In this
  one case a `unique_for` dedup claim is **kept** rather than released, so a
  retry is refused until the claim expires instead of running the task twice.
- **`Publisher` gained `set_publish_deadline`.** Custom brokers implementing
  `swarmq.contracts.Publisher` should add it; the worker calls it through a
  guard, so an implementation without it still shuts down cleanly but does
  not get the shutdown-coupled publish deadline.

### Added

- **`SWARMQ_PUBLISH_TIMEOUT_S` / `SwarmQConfig.publish_timeout_s`**
  (default `120.0`) — an upper bound on a complete publish including its
  internal retries. Active by default. A publish that exceeds it raises
  `BrokerConnectionError` with an indeterminate-delivery caveat instead of
  waiting forever for a confirm the transport will never deliver. During
  graceful shutdown the shorter of this value and the remaining grace
  period applies.

### Fixed

- **A publish could hang indefinitely on a silently dead transport.** No
  exception is raised and the channel still reads as open, so the existing
  retry path never engaged. In one load-test run a five-second broker stall
  became a 62-minute standstill that SIGTERM could not end.
- **Every JSON log line carried its payload twice.** The formatter's own
  working keys (`_record`, `_from_structlog`) were rendered instead of
  stripped, doubling log volume and making the output hard to read by hand.
  The console renderer carried them too and loses them as well.

## [0.2.0] — 2026-08-27

### Changed (breaking — 0.2.0)

- **`ValkeyBackend` legacy method aliases removed.** The contract names
  replace the Valkey-flavoured ones — e.g. `setnx_unique` →
  `claim_unique`, `get_results_batch` → `get_results`,
  `publish_notification` → `notify_result`, `subscribe_and_wait` →
  `wait_for_result`/`wait_for_workflow`; schedule/schedfire primitives
  moved to the `backend.schedules` store (`zrangebyscore_schedules` →
  `schedules.poll_due`, …). Semantics, key layout, and Lua contracts are
  unchanged (pinned by golden fixtures). See the migration table in the
  0.2.0 breaking-changes inventory.
- **`RabbitMQBroker` admin/verb aliases removed.** `get_queue_depth` →
  `queue_depth`, `get_dlq_depth` → `dead_letter_depth`,
  `list_dead_letter_queue` → `list_dead_letters`, `inspect_dead_letter_message` →
  `inspect_dead_letter`, `retry_dead_letter_message` → `retry_dead_letter`,
  `retry_all_dead_letter_messages` → `retry_all_dead_letters`,
  `purge_dead_letter_queue` → `purge_dead_letters`; the delivery verbs are
  now `redeliver`, `route_via_dlx`, `retry_later`, and `dead_letter`
  (the latter two confirm the original delivery themselves — drop any
  separate `ack`).
- **Constructors are typed against the contracts** in `swarmq.contracts`
  (`Worker`, `Client`, `SwarmQ`). `RabbitMQBroker`/`ValkeyBackend`
  implement them; existing code runs unchanged.
- **Internal module layout:** `worker`, `client`, `scheduler`, and
  `process_manager` are packages now; class re-exports such as
  `swarmq.locking.LockManager`, `swarmq.limits.RateLimiter`, and
  `swarmq.workflow_orchestration.ChordManager` moved to
  `swarmq.backend.valkey.*` (not part of the public surface; listed here
  because the CLI surface whitelist pins them).

### Removed (breaking — 0.2.0)

- **`ValkeyBackend.create_pubsub()`.** Use `wait_for_result` /
  `watch_progress` / `wait_for_workflow`; the notification channel names
  (`sq:notify:…`) remain stable for callers that bring their own client.

### Fixed (0.2.0)

- **Priority-band preservation on retry.** Retries, rate-limit and lock
  requeues now target the delay/dead-letter destination of the queue the
  message was **delivered** on — a retry from `tasks.high` stays in
  `tasks.high` even when the message body names the base queue
  (previously the band was lost for foreign-producer messages). Covered
  by `tests/e2e/test_retry_band.py`. This is the single behavioural
  change of the 0.2.0 restructuring.

### Internal (0.2.0)

- **Contracts backbone.** Explicit protocols in `swarmq.contracts`
  (Publisher/Consumer/Broker, BrokerAdmin, ResultBackend, LockStore,
  RateLimitStore, ChordStore, ScheduleStore) with batch-publish hooks
  for the planned bulk fast-path; an in-memory stub in the test suite
  implements the same contracts, and one contract-test suite runs
  identically against the stub, RabbitMQ, and Valkey.
- **Package split.** The four former monolith modules are packages
  now (`worker/`, `client/`, `scheduler/`, `process_manager/`), with
  CI-enforced structure guards (module size, import cycles, import
  purity of the core packages).
- **Project-wide mypy strict.** Both legacy override blocks are gone;
  every remaining `# type: ignore` carries an error code.

### Fixed (0.2.0, post-inventory)

- **Expired-task error results are actually stored now.** The expiry
  gate passed a dict instead of encoded bytes to the result store; the
  SET failed silently against real Valkey and `get_result()` of an
  expired task hung until its timeout instead of raising
  `TaskError("expired")`.

### Rollout (0.2.0)

- Deploy the same version across the whole worker fleet.
- No Valkey migration: key layout, Lua contracts, channels, headers,
  and queue arguments are unchanged (pinned by golden fixtures).
- In-flight 0.1.2 messages remain valid; a 0.2.0 worker consumes
  queues declared by 0.1.2 without `PRECONDITION_FAILED`.
- Rollback is a redeploy of 0.1.2.

## [0.1.2] — 2026-06-07

- Ship the `py.typed` marker so type checkers see the package as typed.

## [0.1.1] — 2026-06-07

- Trim the sdist to `src/` via whitelist; move `libvalkey` to the `fast` extra; declare the readme.

## [0.1.0] — 2026-06-07

First PyPI release. Everything below shipped with it.

### Performance

- **`import swarmq.cli` no longer eagerly loads `valkey` and
  `aio_pika`.** The package init in `swarmq/__init__.py` and the
  CLI facade now resolve public surfaces (`SwarmQ`, `ValkeyBackend`,
  `RabbitMQBroker`, `Metrics`, `HealthChecker`, `Middleware`,
  `Signal`, `Task`, etc.) lazily via PEP-562 `__getattr__`. Identity
  is preserved on first access — `from swarmq import ValkeyBackend`
  still returns the canonical class object, no API change for
  consumers. Median `importtime` cumulative latency for
  `import swarmq.cli` drops from ~188 ms to ~100 ms (best case
  ~78 ms). Affects `swarmq --help`, `swarmq worker --check`, and any
  agent-side introspection that imports the CLI namespace without
  needing the heavy backends.

### Behaviour Changes

- **`--format` default is now TTY-aware (CLI-04).** For
  subcommands that accept `--format` (`dlq list`, `dlq inspect`,
  `dlq retry`, `dlq purge`, `schedule list`, `schedule add`,
  `schedule remove`, `schedule status`, `inspect`), the output
  format now defaults to `table` when stdout is a TTY and `json`
  when stdout is piped, redirected, or otherwise non-interactive.
  Previously the default was always `table`.

  **Scripts relying on the old piped-table default** can restore
  the previous behaviour without code changes by exporting
  `SWARMQ_DEFAULT_FORMAT=table`. Explicit `--format=table` flags
  continue to work unchanged. An explicit `--format=…` on the
  command line always wins over the env var.

  Invalid `SWARMQ_DEFAULT_FORMAT` values (typo, unknown format,
  empty string) are silently ignored — the CLI falls back to the
  TTY-aware default rather than failing. See
  [CLI output](configuration.md#cli-output).

### Added — Phase 5 Wave 2 (Dev Experience)

- **R5.3 Hot reload** (`pip install swarmq[reload]`). `--reload` adds a
  `watchfiles`-backed daemon thread that fires `SIGHUP` at the
  supervisor on `.py` changes under the task module's directory,
  triggering the existing R4.6 reload pipeline (drain → execvpe).
  New flags: `--reload`, `--watch-dir PATH` (repeatable). Filter
  ignores `__pycache__`, `*.pyc`, editor noise (`4913`, `*.swp`,
  `*.bak`, `*.tmp`). Logs `hot_reload_enabled` WARNING on every
  start — developer-only feature. See
  [guides/hot-reload](../guides/hot-reload.md).
- **`swarmq worker --check`.** Dry-run validation: verifies
  `--module` is importable, and with `--reload` that `watchfiles`
  is installed + watch-dirs resolve. Prints a JSON summary and
  exits `0`/`1` without starting the supervisor or touching
  broker/backend. Use for systemd-unit and dev-container smoke
  tests before a full worker start.

### Added — Phase 5 Wave 1 (Operations)

- **R5.1 Prometheus metrics** (`pip install swarmq[metrics]`). Seven
  metrics with `task_name`/`queue`/`outcome` labels:
  `swarmq_tasks_total`, `swarmq_task_duration_seconds`,
  `swarmq_tasks_active`, `swarmq_queue_depth`,
  `swarmq_worker_uptime_seconds`, `swarmq_retries_total`,
  `swarmq_dlq_messages`. Stdlib-only HTTP `/metrics` endpoint.
- **R5.2 Health + Ready endpoints.** `GET /health` (liveness,
  always 200 while event loop alive), `GET /ready` (200 when
  broker + backend reachable, 503 with sanitised JSON body
  listing the unhealthy dependency otherwise). `HealthChecker`,
  `HealthServer`, `serve_health` as public API.
- **Auto-wire.** `SwarmQConfig.metrics_port` / `health_port` (or
  `--metrics-port` / `--health-port` on `swarmq worker`) opts in
  to both servers + signal-based instrumentation with no Python
  entrypoint boilerplate.

### Added — Phase 4 (Operator Features)

- **R4.3 Task Priorities.** `TaskInfo(priority="high"|"normal"|"low")`
  routes to `{queue}.{priority}` suffixes. Worker auto-declares the
  triplet when `priorities=True` (or `--priorities`). Warns at start
  when tasks declare non-normal priority but the worker config has
  priorities disabled.
- **R4.4 Task Expiry.** `TaskInfo(max_age=seconds)` + the
  `x-swarmq-enqueued-at` header stamped at publish (Client +
  Scheduler). Worker routes expired messages to DLQ with
  `TASK_EXPIRED` signal + `TaskError("task expired: …")` result
  before rate-limit / lock / pre_execute stages.
- **R4.5 Burst-Mode.** `swarmq worker --burst` exits 0 once all
  queues have been idle for `--burst-idle-timeout` seconds.
  `SwarmQConfig.burst_mode` / `burst_idle_timeout` +
  `SWARMQ_BURST_MODE` / `SWARMQ_BURST_IDLE_TIMEOUT` env vars.
- **R4.6 ProcessManager.** `swarmq worker --processes N` (alias
  `-p`) runs N worker subprocesses under a supervisor that auto-
  restarts crashed workers (rolling-window limit: 5 restarts per
  hour per worker). `spawn`-context keeps aio-pika + Valkey
  connections scoped per subprocess. SIGHUP triggers
  `os.execvp()`-based reload after graceful drain; SIGTERM /
  SIGINT trigger the same 3-phase shutdown (cooperative event
  → SIGTERM → SIGKILL) as a single-process worker.
- **R4.7 Autoscaling.** `swarmq worker --autoscale=MIN,MAX`
  scales worker count based on queue depth. Scale-up when
  pending tasks exceed `50 * active`; scale-down after 60s
  sustained idle; 30s cooldown between decisions. Queue-depth
  sampled via new `RabbitMQBroker.get_queue_depth()` (passive
  `declare(..., passive=True)` on main + `.DQ`).

### Added — Phase 3 (Comfort Features)

- **R3.5 Unique Tasks.** `TaskInfo(unique_for=seconds,
  unique_until="start"|"completion")` dedups by canonical-encoded
  args. Owner-check via Lua for TTL-expiry-race protection. Both
  `Client.schedule` and `Client.schedule_many` paths.
- **R3.6 Task Cancellation.** `Client.cancel(task_id)` sets a
  Valkey flag the worker checks pre-pickup + during execution.
  Dedicated `TASK_CANCELLED` signal. Cancels propagate as
  `TaskCancelledError` via `get_result`.
- **R3.7 Progress Tracking.** `Task.update_progress(current, total,
  message)` throttled at 500ms per instance with force-flush on
  `current == total`. `Client.get_progress` + `Client.watch_progress`
  AsyncIterator stream updates via Pub/Sub with HASH fallback on
  disconnect. ETA via linear extrapolation.
- **R3.10 DLQ-CLI.** `swarmq dlq list|inspect|retry|purge` for
  operator recovery. Drain-and-republish scan pattern, exit codes
  0 (success), 2 (config), 3 (task_id not found).
- **Embedded scheduler.** Cron + delayed (`delay=` / `eta=`) +
  recurring with leader-election failover. Graceful handover
  without TTL wait.
- **Full CLI.** `swarmq worker`, `swarmq schedule`, `swarmq inspect`
  — no Python entrypoint required for typical deployments.

### Added — E2E Hardening (separate initiative)

- **Chaos suite** (`tests/chaos/`): toxiproxy + container-restart
  coverage for Worker-SIGKILL, Broker/Backend restart, network
  chaos (latency/bandwidth/packet-loss/full-down), Clock-Skew,
  Message Corruption, Byzantine workers.
- **Property-based tests** (`tests/property/`): hypothesis against
  dedup-hash, progress, serialization, backoff, rate-limiter.
- **Fuzz** (`tests/fuzz/`): byte-fuzz on message parser +
  AMQP-header / Pub/Sub external-publisher fuzz.
- **Stress** (`tests/stress/`): 10-worker × 1000-task exact-once,
  50-parallel-cancel winner, 200-parallel progress-throttle.
- **Long-soak** (`tests/soak/`): 1h opt-in leak-detection harness
  with psutil RSS / FD / Valkey-conn / Rabbit-channel counters.
- **Three-tier CI** (GitHub Actions): pre-merge ≤ 5 min, nightly
  chaos+stress+fuzz ≤ 30 min, weekly mutation+soak ≤ 4h.

### Reliability hardening (ce-review follow-ups)

- **RabbitMQBroker** channel-recreate after connection reset;
  `_publish_with_retry` bounded-retry wrapper preserving
  asyncio task-level cancellation.
- **Quorum-queue `x-delivery-limit=20`** on main queue — pathological
  SIGSEGV-loops now terminate in DLQ instead of endless redelivery.
- **ValkeyBackend** retry policy + `health_check_interval` on the
  underlying valkey-py client.
- **Client.watch_progress** re-subscribes on Pub/Sub disconnect.
  valkey-py does not auto-resubscribe.
- **Task.update_progress** asyncio.Lock around the 500ms throttle
  read-compare-write — previously raced under `asyncio.gather`.
- **DLQ-drain republish** routes through `_publish_with_retry` —
  transient channel errors no longer silently drop drained messages.
- **HTTP surfaces** (`/metrics`, `/health`, `/ready`) bounded by
  `asyncio.timeout(5.0)` covering request-line + header drain +
  200-line cap. Binary request lines return 400, not log-spam.
  Default bind `127.0.0.1`. `/ready` error body sanitised to
  prevent host:port leak.

## [0.x] — earlier phases

Phase 1 (minimal round-trip) + Phase 2 (retries, DLQ, logging,
graceful shutdown) + Phase 3 core comfort features (middleware,
signals, rate-limit, locking) landed in the same branch as the
items above. Individual commit history in git.
