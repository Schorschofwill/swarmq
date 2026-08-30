# CLI reference

The `swarmq` binary is installed as a console script when you
`pip install swarmq`. Every subcommand reads from `SWARMQ_*`
environment variables by default; explicit flags win.

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
| `1` | Runtime or configuration error (broker/backend unreachable, module import failed, `ConfigurationError`) |
| `2` | Bad arguments (argparse-detected usage error) |
| `3` | `swarmq dlq inspect` / `swarmq dlq retry <task_id>` found no matching DLQ entry |

`--log-level DEBUG` adds a full Python traceback to stderr when
exit code `1` fires.

## `swarmq worker`

Import a Python module so its `Task` subclasses register, connect to
broker + backend, and consume. `SWARMQ_BROKER_URL` and
`SWARMQ_BACKEND_URL` must be set.

```text
swarmq worker --module DOTTED.PATH
              [--queues A,B,C]
              [--concurrency N]
              [--log-level LEVEL] [--log-format FORMAT]
              [--burst] [--burst-idle-timeout SECONDS]
              [--priorities]
              [--metrics-port PORT] [--health-port PORT]
              [--processes N | -p N] [--autoscale MIN,MAX]
              [--reload] [--watch-dir PATH]...
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--module DOTTED.PATH` | *required* | Python module containing `Task` subclasses to register (e.g. `myapp.tasks`). Must be importable from the current `PYTHONPATH`. |
| `--queues A,B,C` | `default` | Comma-separated queue names the worker consumes. Env var: `SWARMQ_QUEUES`. |
| `--concurrency N` | `10` | Maximum tasks executed in parallel. Env var: `SWARMQ_CONCURRENCY`. |
| `--log-level LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`. Env var: `SWARMQ_LOG_LEVEL`. |
| `--log-format FORMAT` | `human` | `human` or `json`. JSON is fit for Loki, Datadog, CloudWatch. Env var: `SWARMQ_LOG_JSON`. |
| `--burst` | off | Exit cleanly once all queues have been idle for `--burst-idle-timeout` seconds. Env var: `SWARMQ_BURST_MODE`. |
| `--burst-idle-timeout SECONDS` | `5.0` | Seconds of queue silence required before `--burst` exits. Env var: `SWARMQ_BURST_IDLE_TIMEOUT`. |
| `--priorities` | off | Auto-declare + consume the `{queue}.high`/`.normal`/`.low` triplet for every queue. Env var: `SWARMQ_PRIORITIES`. |
| `--metrics-port PORT` | unset | Serve Prometheus `/metrics` on this port. Requires `pip install 'swarmq[metrics]'`. Bind host: `SWARMQ_METRICS_HOST` (default `127.0.0.1`). |
| `--health-port PORT` | unset | Serve `/health` + `/ready` on this port. Bind host: `SWARMQ_HEALTH_HOST` (default `127.0.0.1`). |
| `--processes N`, `-p N` | `1` | Run N subprocesses under a supervisor. The supervisor restarts crashed workers (max 5 per hour) and reloads on `SIGHUP` via `os.execvp()`. |
| `--autoscale MIN,MAX` | unset | Enable queue-depth-driven autoscaling between `MIN` and `MAX` worker subprocesses. Implies `--processes`. |
| `--reload` | off | Watch task modules and trigger SIGHUP reload on change. Developer-only. Requires `pip install 'swarmq[reload]'`. |
| `--watch-dir PATH` | parent of `--module` source | Directory to watch (repeatable). Ignored without `--reload`. |
| `--check` | off | Dry-run validation: verify `--module` is importable, with `--reload` that `watchfiles` is installed + watch-dirs resolve, and that multi-process mode is not combined with a fixed `--metrics-port`/`--health-port`. Prints a JSON summary and exits `0`/`1` without starting the supervisor or touching broker/backend. |

### Examples

Minimal local worker:

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"
swarmq worker --module myapp.tasks
```

Production shape with JSON logs, three worker subprocesses,
Prometheus, and health:

```bash
swarmq worker \
  --module myapp.tasks \
  --queues priority,default \
  --concurrency 25 \
  --processes 3 \
  --log-format json \
  --metrics-port 9108 \
  --health-port 9109
```

Autoscaled deployment:

```bash
swarmq worker --module myapp.tasks --autoscale 2,10
```

Burst mode for a cron-style batch drain:

```bash
swarmq worker --module myapp.tasks --burst --burst-idle-timeout 10
```

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
- [Dead letter queues](../guides/dead-letter-queues.md) — recovery
  workflow using `swarmq dlq`
- [Hot reload (dev-only)](../guides/hot-reload.md) — when to use
  `--reload` and when not to
