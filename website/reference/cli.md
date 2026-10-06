# CLI reference

The `swarmq` binary is installed as a console script when you
`pip install swarmq`. Every subcommand reads from `SWARMQ_*`
environment variables by default; explicit flags win. `swarmq worker`
resolves its settings once in the supervisor (flag, then env var, then
default) and passes the resolved values to its worker subprocesses and
compute children; they do not re-read the environment, so a flag wins
in every process.

## Synopsis

```text
swarmq [--version]
swarmq worker   --module DOTTED.PATH [options]
swarmq schedule (list | add | remove | status) [options]
swarmq dlq      (list | inspect | retry | purge) [options]
swarmq inspect  --module DOTTED.PATH [--format FORMAT]
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success or graceful shutdown |
| `1` | Runtime error: broker or backend unreachable, `swarmq dlq`/`schedule` failed at runtime. Retrying later is the right response. |
| `2` | Bad arguments (argparse-detected usage error) |
| `3` | `swarmq dlq inspect` / `swarmq dlq retry <task_id>` found no matching DLQ entry |
| `78` | Configuration error of `swarmq worker` (`EX_CONFIG`): `ConfigurationError`, task module not importable, `--configure` hook failed, port range invalid, invalid logging settings or logging config file (`LoggingConfigError`). Emitted by the parent process and by the supervisor when a worker child exits `78`. Retrying without a change is pointless. |

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
budget; it never reaches the caller.

`--log-level DEBUG` adds a full Python traceback to stderr when exit
code `1` or `78` fires.

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
| `--concurrency N` | `10` (io) / assigned CPUs (cpu) | Maximum tasks executed in parallel. With `--pool cpu` the number of compute slots; the default is the CPUs assigned to the worker (affinity and cgroup quota) divided by `--processes`. Env var: `SWARMQ_CONCURRENCY`. |
| `--pool {io,cpu}` | `io` | Execution pool. `cpu` runs every task body in a compute child forked from a pre-warmed forkserver: the loop process never computes, `hard_timeout` and cancel kill the child. POSIX only; not combinable with `--autoscale`. See the [CPU mode guide](../guides/cpu-mode.md). |
| `--log-level LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Root level of every SwarmQ process. Env var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` or `json` on stderr. JSON is fit for Loki, Datadog, CloudWatch. Env var: `SWARMQ_LOG_FORMAT` (alias `SWARMQ_LOG_JSON=true/false`; `SWARMQ_LOG_FORMAT` wins). |
| `--log-config PATH` | unset | Logging `dictConfig` file (JSON; YAML with PyYAML installed) applied in every SwarmQ process instead of the built-in setup. Validated once by the supervisor. Not combinable with an explicit format or `--no-configure-logging`. Env var: `SWARMQ_LOG_CONFIG`. See [Logging](logging.md#own-dictconfig-file-log-config). |
| `--no-configure-logging` | configure | Set up no logging in any SwarmQ process; your task module or `--configure` hook owns it. Env var: `SWARMQ_CONFIGURE_LOGGING=false`. |
| `--logger-level NAME=LEVEL` | none | Level for one logger, e.g. `aio_pika=WARNING` or `swarmq.task=WARNING`. Repeatable; applied last. Env var: `SWARMQ_LOGGER_LEVELS` (comma-separated). |
| `--log-queue-size N` | `10000` | Capacity of the in-process log queue; when it is full, records are dropped instead of blocking. Env var: `SWARMQ_LOG_QUEUE_SIZE`. |
| `--log-lifecycle-level LEVEL` | `DEBUG` | Level of the `task_received` and `task_started` events. Env var: `SWARMQ_LOG_LIFECYCLE_LEVEL`. |
| `--burst` | off | Exit cleanly once all queues have been idle for `--burst-idle-timeout` seconds. Env var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Seconds of queue silence required before `--burst` exits. Env var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--priorities` | off | Auto-declare + consume the `{queue}.high`/`.normal`/`.low` triplet for every queue. Env var: `SWARMQ_PRIORITIES`. |
| `--metrics-port PORT` | unset | **Base port** for Prometheus `/metrics`: the worker in slot `s` serves on `PORT + s`, so `--processes 3 --metrics-port 9000` occupies `9000-9002`. Requires `pip install 'swarmq[metrics]'`. Bind host: `SWARMQ_METRICS_HOST` (default `127.0.0.1`). See [base ports](configuration.md#observability). |
| `--health-port PORT` | unset | **Base port** for `/health` + `/ready`, same slot offset as `--metrics-port`. Bind host: `SWARMQ_HEALTH_HOST` (default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Run N worker subprocesses in slots `0..N-1`. Since 0.5.0 every `swarmq worker` runs under the supervisor, also with `N = 1`: it restarts crashed workers (max 5 per hour) into the same slot, replaces workers whose heartbeat stops, reloads on `SIGHUP` via `os.execvp()`, and exits 0 once all workers ended cleanly (`--burst`). A configuration error in any worker ends the run after a single attempt with exit `78`. |
| `--autoscale MIN,MAX` | unset | Enable queue-depth-driven autoscaling between `MIN` and `MAX` worker subprocesses. Implies `--processes`. |
| `--max-processes N` | unset | **Enables the control channel** and declares the headroom: a setpoint stored in the backend may raise this supervisor to at most `N` worker processes. `N` must be at least `--processes` and the `--autoscale` upper bound; with `--pool cpu` it must not overbook the assigned CPUs (`N` without `--concurrency`, `N × --concurrency` with it). Without this flag the supervisor never reads or writes setpoints and behaves exactly as before. See [control channel](#control-channel). |
| `--group LABEL` | sorted set of `--queues` | Group key of the control channel. Every supervisor with the same key (in the same backend) follows the same setpoint, each on its own. Deployments that share a backend and serve the same queues need distinct labels, otherwise they steer each other. |
| `--reload` | off | Watch task modules and trigger SIGHUP reload on change. Developer-only. Requires `pip install 'swarmq[reload]'`. |
| `--watch-dir PATH` | parent of `--module` source | Directory to watch (repeatable). Ignored without `--reload`. |
| `--check` | off | Dry-run validation: verify `--module` is importable, that a `--configure` spec has the form `MODULE:CALLABLE` and its module can be found (without importing it), with `--reload` that `watchfiles` is installed + watch-dirs resolve, and that the observability port ranges fit below 65535 and do not overlap. Prints a JSON summary including `metrics_ports` and `health_ports` (the exact ports the slots will bind) and exits `0`/`1` without starting the supervisor or touching broker/backend. Payload: [message formats](agent/message-formats.md#swarmq-worker-check-payload). |

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

from myapp.middleware import TimingMiddleware


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
`sq:control:setpoint:<group>`, for example `{"min": 2, "max": 8}`. A
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
`sq:control:status:<group>:<supervisor>` (the supervisor id is
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
prefixes (`sq:control:setpoint:` and `sq:control:status:`), separate
from task and result keys, so an operator can grant write access to
setpoints and to statuses separately (for example with Valkey ACLs).
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
time, queue, and dedup state.

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
`--cron` (5-field expression) or `--at` (ISO 8601 one-shot time).
`--args` and `--kwargs` must be JSON.

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

Without `--permanent`, the schedule is disabled but kept. With
`--permanent`, it is hard-deleted so insert-if-absent can re-sync
from code.

### `swarmq schedule status`

```text
swarmq schedule status [--limit N] [--format (table|json)]
```

Prints the current leader worker (if any) and the next N schedules
due within 24 hours.

## `swarmq dlq`

Operate on `{queue}.XQ`, the broker-level dead-letter queue. Needs
`SWARMQ_BROKER_URL`; does not connect to the backend.

### `swarmq dlq list`

```text
swarmq dlq list [--queue QUEUE] [--limit N] [--format (table|json)]
```

Drain-and-republish scan — shows `task_id`, `task_name`, retry-count
header, error preview, body size. Repeated calls are safe: messages
stay in the DLQ. The table output shows `(truncated)` when
`--limit` is hit; `--format json` includes a `truncated: bool`
envelope.

### `swarmq dlq inspect`

```text
swarmq dlq inspect TASK_ID [--queue QUEUE]
                            [--scan-limit N]
                            [--format (table|json)]
```

Finds one DLQ message by `task_id` and prints its full headers and
decoded body. Non-destructive. Exits `3` when the task_id is not
found (use `--scan-limit` to widen the search).

### `swarmq dlq retry`

```text
swarmq dlq retry (TASK_ID | --all)
                 [--queue QUEUE] [--limit N] [--scan-limit N]
                 [--format (table|json)]
```

Republishes into the main queue. `--all` moves every message (up to
`--limit`, default `10000`). The `x-swarmq-retry-count` header is
reset so the task gets its full retry budget; all other headers
survive. Exits `3` when a single-task retry finds nothing.

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
