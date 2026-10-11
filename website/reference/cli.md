# CLI reference

The `swarmq` binary is installed as a console script when you
`pip install swarmq`. Every subcommand reads from `SWARMQ_*`
environment variables by default; explicit flags win. `swarmq worker`
resolves its settings once in the supervisor (flag, then env var, then
default) and passes the resolved values to its worker subprocesses and
compute children; they do not re-read the environment, so a flag wins
in every process. This holds for every worker option that has an env
var twin (queues, concurrency, pool, burst, priorities, ports, logging).
`swarmq worker --check` resolves the same way and validates exactly the
values the workers would get. An embedded `SwarmQ(...)` app is not
affected: there the `SwarmQ`/`SwarmQConfig` keyword arguments apply
first and `SWARMQ_*` variables override them.

## Synopsis

```text
swarmq [--version]
swarmq worker   --module DOTTED.PATH [options]
swarmq schedule (list | add | remove | status) [options]
swarmq dlq      (list | inspect | retry | purge) [options]
swarmq inspect  --module DOTTED.PATH [--format FORMAT]
swarmq check    [--format FORMAT]
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success or graceful shutdown |
| `1` | Runtime error: broker or backend unreachable, `swarmq dlq`/`schedule` failed at runtime. Retrying later is the right response. Also every configuration error of `swarmq schedule`, `swarmq dlq` and `swarmq inspect` (missing `SWARMQ_BROKER_URL`/`SWARMQ_BACKEND_URL`, invalid `--args`/`--kwargs` JSON, invalid `--cron`/`--at`, `inspect --module` not found or registering no tasks) and every failure of `swarmq worker --check`, including argparse errors. |
| `2` | Bad arguments (argparse-detected usage error), also `swarmq dlq retry` with both a `task_id` and `--all` or with neither (message on stderr). Not under `swarmq worker --check`, which reports argparse errors as JSON with exit `1`. |
| `3` | `swarmq dlq inspect` / `swarmq dlq retry <task_id>` found no matching DLQ entry |
| `78` | Configuration error of `swarmq worker` (`EX_CONFIG`): `ConfigurationError`, task module not importable, `--configure` hook failed, port range invalid, invalid logging settings or logging config file (`LoggingConfigError`). Emitted by the parent process and by the supervisor when a worker child exits `78`. Retrying without a change is pointless. `swarmq check`: a worker would refuse to start in this environment (evicting Valkey `maxmemory-policy` under `SWARMQ_VALKEY_EVICTION_CHECK=strict`). |
| `130` | Interrupted by Ctrl-C (`KeyboardInterrupt`, 128 + `SIGINT`) outside the command's own handling, for example while modules are imported. A running command or worker treats Ctrl-C as a graceful shutdown and exits `0`. |

`78` follows `sysexits.h`. Every configuration error, whether it
surfaces in the parent process or inside a worker subprocess, ends the
whole run with `78` after a single attempt and logs one structured
`worker_config_error` line in the configured `--log-format`. Connection
errors stay at `1`, because there "try again" is right. In systemd use
`RestartPreventExitStatus=78` so a broken deployment does not restart in
a loop; in Kubernetes alert on the exit code rather than on restarts.
Exit `75` (`EX_TEMPFAIL`) is internal: a worker child that asked for
recycling without getting an acknowledgement logs
`worker_recycle_ack_timeout` and exits with it, the supervisor logs
`worker_recycle_noack` and restarts it within the
budget; it never reaches the caller. Exit `71` (`EX_OSERR`) is internal
too: a CPU worker whose silently restarted forkserver was rejected stops
consuming, logs `compute_fork_source_rejected` and exits with it; the
supervisor logs `worker_fork_source_rejected` and restarts it with a
fresh forkserver within the budget.

If a given-up log thread is still stuck after the final flush, the
`swarmq` command ends via `os._exit` with the exit code it would have had
(a traceback or `SystemExit` message goes to stderr first, if stderr
does not block) instead of aborting with `Fatal Python error` (exit
`134`) or hanging. See [Logging](logging.md#bounded-flushing).

For `swarmq worker`, level `DEBUG` (`--log-level DEBUG` or
`SWARMQ_LOG_LEVEL=DEBUG`) adds the full Python traceback to connection
errors (exit `1`) and to configuration errors raised after logging is
set up (exit `78`). `schedule`, `dlq` and `inspect` have no log level
and print a single error line.

## `swarmq worker`

Import a Python module so its `Task` subclasses register, connect to
broker + backend, and consume. `SWARMQ_BROKER_URL` and
`SWARMQ_BACKEND_URL` must be set.

```text
swarmq worker --module DOTTED.PATH
              [--configure MODULE:CALLABLE]
              [--queues A,B,C]
              [--concurrency N] [--pool {io,cpu}]
              [--log-level LEVEL] [--log-format FORMAT]
              [--log-config PATH | --no-configure-logging]
              [--logger-level NAME=LEVEL]...
              [--log-queue-size N] [--log-lifecycle-level LEVEL]
              [--log-drain-timeout SECONDS]
              [--burst] [--burst-idle-timeout SECONDS]
              [--priorities]
              [--metrics-port PORT] [--health-port PORT]
              [--processes N | -p N] [--autoscale MIN,MAX]
              [--max-processes N] [--group LABEL]
              [--reload] [--watch-dir PATH]...
              [--check]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--module DOTTED.PATH` | *required* | Python module containing `Task` subclasses to register (e.g. `myapp.tasks`). Must be importable from the current `PYTHONPATH`. |
| `--configure MODULE:CALLABLE` | unset | Callable invoked with the `SwarmQ` app after construction and before the worker starts, e.g. `myapp.hooks:setup`. Use it to register middleware and signal handlers without writing your own worker entry point. Runs exactly once per worker process, never in the forkserver and never in `schedule`, `dlq` or `inspect`; an awaitable return value is awaited. Any exception from the hook is a configuration error (exit `78`). See [the example](#registering-middleware-with-configure). |
| `--queues A,B,C` | `default` | Comma-separated queue names the worker consumes. Env var: `SWARMQ_QUEUES`. |
| `--concurrency N` | `10` (io) / assigned CPUs (cpu) | Maximum tasks executed in parallel. With `--pool cpu` the number of compute slots; the default is the CPUs assigned to the worker (affinity and cgroup quota) divided by `--max-processes` if set, otherwise by `--processes`. Env var: `SWARMQ_CONCURRENCY`. |
| `--pool {io,cpu}` | `io` | Execution pool. `cpu` runs every task body in a compute child forked from a pre-warmed forkserver: the loop process never computes, `hard_timeout` and cancel kill the child. POSIX only; not combinable with `--autoscale`. See the [CPU mode guide](../guides/cpu-mode.md). |
| `--log-level LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root level of every SwarmQ process. Env var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` or `json` on stderr. JSON is fit for Loki, Datadog, CloudWatch. Env var: `SWARMQ_LOG_FORMAT` (alias `SWARMQ_LOG_JSON=true/false`; `SWARMQ_LOG_FORMAT` wins). |
| `--log-config PATH` | unset | Logging `dictConfig` file (JSON; YAML with PyYAML installed) applied in every SwarmQ process (compute children included) instead of the built-in setup. Validated once by the supervisor and again before every `SIGHUP` reload, in a separate process with a 30-second limit while supervision continues; an invalid file (`log_config_reload_failed`) or a check that runs out of time (`log_config_reload_probe_timeout`) keeps the running workers. Not combinable with an explicit format or `--no-configure-logging`. Env var: `SWARMQ_LOG_CONFIG`. See [Logging](logging.md#own-dictconfig-file-log-config). |
| `--no-configure-logging` | configure | Set up no logging in any SwarmQ process; your task module or `--configure` hook owns it. Env var: `SWARMQ_CONFIGURE_LOGGING=false`. |
| `--logger-level NAME=LEVEL` | none | Level for one logger, e.g. `aio_pika=WARNING` or `swarmq.task=WARNING`. Repeatable; applied last. Env var: `SWARMQ_LOGGER_LEVELS` (comma-separated). |
| `--log-queue-size N` | `10000` | Capacity of the in-process log queue; when it is full, records are dropped instead of blocking. Env var: `SWARMQ_LOG_QUEUE_SIZE`. |
| `--log-lifecycle-level LEVEL` | `DEBUG` | Level of the `task_received` and `task_started` events. Env var: `SWARMQ_LOG_LIFECYCLE_LEVEL`. |
| `--log-drain-timeout SECONDS` | `5` | Upper bound for every flush of the log queues: at exit, before a reload, before each compute result. Records not written in time count as dropped. Must be greater than 0 and at most `threading.TIMEOUT_MAX`, otherwise exit `78`. Env var: `SWARMQ_LOG_DRAIN_TIMEOUT`. See [Logging](logging.md#bounded-flushing). |
| `--burst` | off | Exit cleanly once all queues have been idle for `--burst-idle-timeout` seconds. Env var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Seconds of queue silence required before `--burst` exits. Env var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--priorities` | off | Auto-declare + consume the `{queue}.high`/`.normal`/`.low` triplet for every queue. Env var: `SWARMQ_PRIORITIES`. Without it, publishing a task with `TaskInfo(priority="high"\|"low")` raises `UnroutableMessage` (from 0.12; before, the message was dropped silently). |
| `--metrics-port PORT` | unset | **Base port** for Prometheus `/metrics`: the worker in slot `s` serves on `PORT + s`, so `--processes 3 --metrics-port 9000` occupies `9000-9002`. Requires `pip install 'swarmq[metrics]'`. Bind host: `SWARMQ_METRICS_HOST` (default `127.0.0.1`). See [base ports](configuration.md#observability). |
| `--health-port PORT` | unset | **Base port** for `/health` + `/ready`, same slot offset as `--metrics-port`. Bind host: `SWARMQ_HEALTH_HOST` (default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Run N worker subprocesses in slots `0..N-1`. Since 0.5.0 every `swarmq worker` runs under the supervisor, also with `N = 1`: it restarts crashed workers (max 5 per hour) into the same slot, replaces workers whose heartbeat stops, reloads on `SIGHUP` by re-executing itself with `os.execvpe()` and the argv (`sys.orig_argv`) and environment captured at start-up, so `python -m swarmq.cli worker ...` reloads too, and exits 0 once all workers ended cleanly (`--burst`). `SIGTERM` and `SIGINT` always end the supervisor, also while a reload is being checked or prepared. A configuration error in any worker ends the run after a single attempt with exit `78`. |
| `--autoscale MIN,MAX` | unset | Enable queue-depth-driven autoscaling between `MIN` and `MAX` worker subprocesses. Implies `--processes`. Since 0.12 it counts only the messages ready in each queue itself, not the delay queues ([delay tiers](../explanation/delivery-semantics.md#delay-tiers)). |
| `--max-processes N` | unset | **Enables the control channel** and declares the headroom: a setpoint stored in the backend may raise this supervisor to at most `N` worker processes. `N` must be at least `--processes` and the `--autoscale` upper bound; with `--pool cpu` it must not overbook the assigned CPUs (`N` without `--concurrency`, `N × --concurrency` with it). Without this flag the supervisor never reads or writes setpoints and behaves exactly as before. See [control channel](#control-channel). |
| `--group LABEL` | sorted set of `--queues` | Group key of the control channel. Every supervisor with the same key (in the same backend) follows the same setpoint, each on its own. Deployments that share a backend and serve the same queues need distinct labels, otherwise they steer each other. |
| `--reload` | off | Watch task modules and trigger SIGHUP reload on change. Developer-only. Requires `pip install 'swarmq[reload]'`. |
| `--watch-dir PATH` | parent of `--module` source | Directory to watch (repeatable). Ignored without `--reload`. |
| `--check` | off | Dry-run validation: verify `--module` is importable, that a `--configure` spec has the form `MODULE:CALLABLE` and its module can be found (without importing it), with `--reload` that `watchfiles` is installed + watch-dirs resolve, that the observability port ranges fit below 65535 and do not overlap, that the logging settings are valid and a `--log-config` file loads and is accepted by `dictConfig` (probed in a separate interpreter), that `--pool cpu` is not combined with `--autoscale`, and that `--max-processes` is not below `--processes` or the `--autoscale` upper bound and does not overbook the assigned CPUs with `--pool cpu`. Prints a JSON summary including `metrics_ports` and `health_ports` (the exact ports the slots will bind) and exits `0`/`1` without starting the supervisor or touching broker/backend. Payload: [message formats](agent/message-formats.md#swarmq-worker-check-payload). |

### Examples

Minimal local worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"
swarmq worker --module myapp.tasks
```

Production shape with JSON logs, three worker subprocesses,
Prometheus, and health. The three slots serve metrics on
`9100-9102` and health on `9200-9202`:

```bash
swarmq worker \
  --module myapp.tasks \
  --queues priority,default \
  --concurrency 25 \
  --processes 3 \
  --log-format json \
  --metrics-port 9100 \
  --health-port 9200
```

### Registering middleware with `--configure`

```python title="myapp/hooks.py"
from swarmq import Signal, SwarmQ
from swarmq.logging import get_logger

from myapp.middleware import TimingMiddleware

log = get_logger("myapp.hooks")


async def setup(app: SwarmQ) -> None:
    app.add_middleware(TimingMiddleware())

    @app.on_signal(Signal.TASK_REJECTED)
    async def on_rejected(queue, reason, **kwargs):
        log.warning("task_rejected", queue=queue, reason=reason)
```

```bash
swarmq worker --module myapp.tasks --configure myapp.hooks:setup
```

The hook may be a plain function or a coroutine function. It runs in
every worker subprocess after the app exists and before the worker
connects; it does not run in the supervisor, so keep the module free
of side effects at import time.

Autoscaled deployment:

```bash
swarmq worker --module myapp.tasks --autoscale 2,10
```

Burst mode for a cron-style batch drain:

```bash
swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
```

### Control channel

`--max-processes N` lets an operator (or a controlling service such as
the swarmq dashboard) change the process count and the autoscale bounds
of a worker group at runtime, without a deploy and without a console:

```bash
swarmq worker --module myapp.tasks --autoscale 2,4 --max-processes 16 --group orders-eu
```

**Setpoint.** The controlling service writes one JSON value per group to
`[<namespace>:]sq:control:setpoint:<group>` (`<namespace>:` only with `SWARMQ_NAMESPACE`), for example `{"min": 2, "max": 8}`. A
fixed process count `N` is the setpoint `{"min": N, "max": N}` and
switches autoscaling off for the group. The supervisor reads the key
once before it spawns the first worker and then every 5 seconds. A
higher lower bound starts the missing workers immediately, a lower
upper bound drains the surplus workers gently (the running task
finishes, then the process exits). Deleting the key restores the
start-up arguments. A new deploy therefore comes up with the last
setpoint, not with the deployment file.

**Rejection.** A setpoint is rejected -- and the previous bounds stay in
effect -- when its upper bound exceeds the headroom, its lower bound is
below 1, its bounds are inverted, the value is not readable as
`{"min": N, "max": M}`, or a range is written for a group without
autoscaling (fixed `--processes` or `--pool cpu` accept only the fixed
form `N..N`). A setpoint never makes the supervisor fail to start: if
the backend is unreachable at start-up, the group comes up with its
start-up arguments and applies the setpoint once the backend answers.

**Status.** Every supervisor reports under
`[<namespace>:]sq:control:status:<group>:<supervisor>` (the supervisor id is
`SWARMQ_WORKER_ID` or the hostname plus `:p<pid>`): the group key, the
last setpoint read (or `null`), whether the store was `present`,
`absent` or `unread` (unreachable), the effective bounds and their
`source` (`startup` or `setpoint`), the `rejection` reason if any, the
slot counts (`size`, `occupied`, `locked`) and `reported_at`. The key
expires after the shutdown grace period plus the termination window
plus one sample interval, so a dead supervisor disappears on its own.
A reader can therefore tell "rejected" from "not yet applied" and from
"backend unreachable" without looking at the process.

**What the headroom sizes.** The slot table, the observability port
ranges (`--metrics-port`/`--health-port` cover `PORT..PORT+N-1`) and,
with `--pool cpu`, the default compute slots per worker (assigned CPUs
divided by `N`) are derived from `N` once at start-up and checked once.
None of them grows at runtime: raising the headroom needs a deploy.
Enabling the control channel on an existing deployment may therefore
require moving a base port. Slots locked by the restart budget stay
locked; the status reports their number.

**Security.** Whoever may write to the backend may set the size of a
worker group. The setpoint and status keys live under their own
prefixes (`[<namespace>:]sq:control:setpoint:` and
`[<namespace>:]sq:control:status:`), separate from task and result keys,
so an operator can grant write access to setpoints and to statuses
separately (for example with Valkey ACLs such as
`~<namespace>:sq:control:setpoint:*`; without namespace
`~sq:control:setpoint:*`).
Until the controlling service has its own credentials with such
restricted write access, every task producer that shares the backend
URL can also set setpoints.

## `swarmq schedule`

Manage the embedded scheduler's schedule definitions in Valkey.
Requires `SWARMQ_BACKEND_URL`; does not connect to the broker.

### `swarmq schedule list`

```text
swarmq schedule list [--format (table|json)]
```

Lists every schedule in `sq:sched:defs` with its cron, next fire
time, queue, and dedup state. The column `LAST_SEEN` shows the newest
[cron report](../guides/scheduling-and-retries.md#cron-reports-and-the-leader)
of a static cron (UTC), `never` for a static cron no worker has ever
reported (a candidate for [`schedule prune --orphans`](#swarmq-schedule-prune))
and `-` for CLI schedules, which are never reported. `--format json`
adds `last_seen_ms` (Unix milliseconds or `null`) to every entry.

### `swarmq schedule add`

```text
swarmq schedule add NAME
                    (--cron EXPR | --at ISO_DATETIME)
                    --task TASK_NAME
                    [--args JSON_ARRAY] [--kwargs JSON_OBJECT]
                    [--queue QUEUE]
                    [--unique-for SECONDS]
                    [--unique-until (start|completion)]
                    [--format (table|json)]
```

Creates a new schedule (insert-if-absent). Pass exactly one of
`--cron` (5-field expression) or `--at` (ISO 8601 one-shot time;
without a timezone it is read as UTC). `--args` (default `[]`) must be
a JSON array, `--kwargs` (default `{}`) a JSON object; anything else
exits `1`. `--queue` defaults to `default`. `--task` is not checked
against the task registry: a typo only surfaces when the schedule fires
on a worker.

`--unique-for SECONDS` claims a dedup flag over task name + args +
kwargs before inserting the schedule; its TTL is `SECONDS` plus the time
until the first fire. A second `schedule add` with the same task, args
and kwargs while the flag is held is rejected as a duplicate (`json`
output: `inserted: false`, `duplicate_of: <owner>`). `--unique-until`
(`completion` by default, `start`) decides whether the flag is released
when the task reaches a terminal state or when a worker picks it up; it
only applies together with `--unique-for`.

Example:

```bash
swarmq schedule add nightly-billing \
  --cron '0 3 * * *' \
  --task run_billing \
  --queue priority \
  --kwargs '{"tenant": "acme"}'
```

### `swarmq schedule remove`

```text
swarmq schedule remove NAME [--permanent] [--format (table|json)]
```

Without `--permanent`, the schedule is disabled but kept; it stays
disabled when its code changes. With `--permanent`, it is hard-deleted,
and a mark from pruning a disabled cron (`sq:{sched}:disabled`) is
cleared, also when the cron itself is already gone; the next report then
creates it enabled. A static cron that a live worker still reports is adopted again by the
leader within one `SWARMQ_SCHEDULER_SEEN_INTERVAL_S`; it only stays
gone once no code reports it anymore (see
[cron reports](../guides/scheduling-and-retries.md#cron-reports-and-the-leader)).

### `swarmq schedule status`

```text
swarmq schedule status [--limit N] [--format (table|json)]
```

Prints the current leader worker (if any), its swarmq version, the
next N schedules due within 24 hours (`--limit`, default `10`) and the
workers that report crons, with their swarmq versions.

```text
LEADER: host-a:4711:3f9c2a1b
LEADER_VERSION: 0.10.0
NOW:    2026-10-20T08:00:00+00:00
UPCOMING:
  2026-10-21T01:00:00+00:00  daily_report  task=daily_report  queue=reports
WORKERS:
  worker-a  0.10.0
  worker-b  0.10.0
```

`LEADER_VERSION` is `(not reported: swarmq before 0.10.0)` when the
leader runs a swarmq that writes no version, and `-` without a leader.
`WORKERS` lists every worker whose report is live (three
`SWARMQ_SCHEDULER_SEEN_INTERVAL_S`), `(none)` without any. Every
version shown here should be at least the minimum leader version
(currently 0.12; see
[leader version](../guides/scheduling-and-retries.md#leader-version)).
Fire times are shown in UTC.

`--format json` adds the fields `leader_version` (string or `null`)
and `workers` (a list of `{"worker_id": ..., "version": ...}`) to
`leader`, `now_ms` and `upcoming`.

### `swarmq schedule prune`

```text
swarmq schedule prune --orphans [--format (table|json)]
```

Removes every static cron that no worker has ever reported: old
entries from before 0.10.0 and crons of apps that run a swarmq before
0.10.0. The leader never removes these on its own (it logs
`scheduler_cron_prune_candidate`); crons that were reported and then
fell silent it removes after `SWARMQ_SCHEDULER_PRUNE_AFTER_S` (see
[removing crons](../guides/scheduling-and-retries.md#removing-crons-no-app-reports-anymore)).
CLI schedules are never touched. Each cron is removed atomically
together with its fire, only while it still has no report. A cron that
was disabled comes back disabled when a worker reports it again.
`--orphans` is required. Prints `removed orphan schedule 'NAME'` per
cron, or `no orphan schedules (every static schedule has been
reported)`; `--format json` prints `{"removed": [...]}`.

Run it only when every app on the Valkey runs 0.10.0 or later;
otherwise the crons of an older app are removed and only come back
when that app reports them after its upgrade.

## `swarmq dlq`

Operate on `{queue}.XQ`, the broker-level dead-letter queue. Needs
`SWARMQ_BROKER_URL`; does not connect to the backend.

### `swarmq dlq list`

```text
swarmq dlq list [--queue QUEUE] [--limit N] [--format (table|json)]
```

Drain-and-republish scan — shows `task_id`, `task_name`, retry-count
header, error preview, body size. `--queue` defaults to `default` (DLQ
`default.XQ`), `--limit` to `100`. Repeated calls are safe: messages
stay in the DLQ with their remaining retention; reading does not
extend it, and messages whose retention ran out are dropped (see
[retention](../guides/dead-letter-queues.md#retention-dead-lettered-messages-expire)).
The table output shows `(truncated)` when `--limit` is hit;
`--format json` includes a `truncated: bool` envelope.

All `dlq` subcommands that put messages back compute the remaining
retention from the message's own headers `x-swarmq-dead-lettered-at`
and `x-swarmq-dead-letter-ttl-ms`. Only a message without them (old
stock, broker dead-letter exchange) gets both headers and the full
`SWARMQ_DEAD_LETTER_TTL_S` of the CLI (default `604800`, `none` =
unlimited) on its first read.

### `swarmq dlq inspect`

```text
swarmq dlq inspect TASK_ID [--queue QUEUE]
                            [--scan-limit N]
                            [--format (table|json)]
```

Finds one DLQ message by `task_id` and prints its full headers and
decoded body. Non-destructive. Exits `3` when the task_id is not
found (use `--scan-limit`, default `1000`, to widen the search).

### `swarmq dlq retry`

```text
swarmq dlq retry (TASK_ID | --all)
                 [--queue QUEUE] [--limit N] [--scan-limit N]
                 [--format (table|json)]
```

Republishes into the main queue. `--all` moves every message (up to
`--limit`, default `10000`); a single `TASK_ID` is looked up among the
first `--scan-limit` messages (default `1000`). Pass exactly one of
`TASK_ID` and `--all`, otherwise the command exits `2`. The `x-swarmq-retry-count` header is
reset so the task gets its full retry budget; `x-swarmq-dead-lettered-at`
and the retention are removed, and a new `x-swarmq-replay-id` lets the
message pass the
[execution guard](../explanation/delivery-semantics.md#execution-guard);
all other headers survive. Exits `3` when a single-task retry finds
nothing.

### `swarmq dlq purge`

```text
swarmq dlq purge [--queue QUEUE] [--format (table|json)]
```

Native AMQP `queue.purge()` on `{queue}.XQ`. Irreversible — use when
the DLQ contents are known-bad and not worth inspecting.

## `swarmq inspect`

```text
swarmq inspect --module DOTTED.PATH [--format (table|json)]
```

Import a module, register its `Task` subclasses, and print each
task's `TaskInfo` metadata — queues, retry policy, rate limit, lock,
dedup settings, cron. Read-only. Does not connect to broker or
backend. Useful as a container healthcheck or in CI.

### Example

```bash
$ swarmq inspect --module myapp.tasks
TASK           QUEUES        DELIVERY       RETRIES  RATE_LIMIT  LOCK               UNIQUE_FOR  UNIQUE_UNTIL  CRON
render_invoice pdf           at_least_once  5        -           -                  -           -             -
send_email     default,mail  at_least_once  3        100/m       -                  -           -             -
sync_user      users         at_least_once  2        -           user-{user_id}     60          completion    -
```

## `swarmq check`

```text
swarmq check [--format (table|json)]
```

Read-only check of the environment swarmq runs in (new in 0.12). Reads
`SWARMQ_BACKEND_URL`, `SWARMQ_VALKEY_EVICTION_CHECK` and
`SWARMQ_SHUTDOWN_TIMEOUT` and sends only `CONFIG GET` and `INFO` to
Valkey. Run it **before an upgrade**, with the new swarmq version in a
separate virtualenv, against production; in Kubernetes optionally as an
init container ([Valkey requirements](configuration.md#valkey-requirements)).
Not the same as `swarmq worker --check`, which dry-runs a worker start.

Sections:

- `valkey`: `maxmemory_policy`, `maxmemory`, `used_memory`, `eviction`
  (`safe`, `unsafe`, `unknown`), the effective `eviction_check`,
  `aof_enabled`, `version`, `cluster_enabled`, `namespace`
  (`SWARMQ_NAMESPACE`, `null` without) and `unprefixed_keys`: with a
  namespace the number of keys without it (`sq:*`, counted by `SCAN` up
  to 10000 within 5 seconds), else `null`, and `unprefixed_keys_complete`
  (`false` when `SCAN` hit the time limit: the count is then a lower
  bound). In cluster mode `SCAN` sees only the node it is connected to.
  Values Valkey does not report (ACL) are `null`.
- `shutdown`: `shutdown_timeout` and `min_grace_period_s`
  (`shutdown_timeout + 15`), the least `terminationGracePeriodSeconds`
  (Kubernetes) or `stop_grace_period` (Docker) to configure.

`findings` lists each problem with `section`, `level` (`error` or
`warning`), `code` and `message`: `eviction_policy` (error under
`strict`, else warning), `eviction_policy_unknown`, `aof_disabled`,
`cluster`, `version` (older than 8), `unprefixed_keys` (keys without
namespace next to a namespace: apps without `SWARMQ_NAMESPACE` on the
same Valkey, or data from before the namespace was set, which the
namespace does not migrate; [namespace](configuration.md#namespace-several-apps-on-one-valkey)),
`unprefixed_keys_unknown` (`SCAN` failed, is forbidden, or hit its time
limit before finding a key). An incomplete count reads `at least <n>
keys … (count incomplete …)`; in cluster mode the message adds that only
one node was counted.

Exit codes: `0` when nothing blocks (warnings allowed), `78` when a
worker would refuse to start in this environment (evicting policy under
`SWARMQ_VALKEY_EVICTION_CHECK=strict`, invalid `SWARMQ_NAMESPACE`), `1` when Valkey is unreachable
or `SWARMQ_BACKEND_URL` is missing.

```bash
$ SWARMQ_BACKEND_URL=valkey://valkey:6379/0 swarmq check --format table
valkey  url  valkey://valkey:6379/0
valkey  eviction_check  strict
valkey  eviction  unsafe
valkey  maxmemory_policy  volatile-lru
...
ERROR  valkey: maxmemory-policy volatile-lru may evict swarmq keys (...)
WARNING  valkey: AOF is off: (...)
FAILED
```

## Global flags

| Flag | Description |
|---|---|
| `--version` | Print `swarmq <version>` and exit `0`. |
| `-h`, `--help` | Print the help text for the current command or subcommand and exit `0`. |

## See also

- [Configuration](configuration.md) — the env vars these flags shadow
- [Logging](logging.md) — output format, events and integration recipes
- [Dead letter queues](../guides/dead-letter-queues.md) — recovery
  workflow using `swarmq dlq`
- [Hot reload (dev-only)](../guides/hot-reload.md) — when to use
  `--reload` and when not to
