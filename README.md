# SwarmQ

Async-first Python task queue on RabbitMQ + Valkey.

> **Status:** 0.6.x on PyPI. Core, workflow primitives, operator
> features (priorities, expiry, burst, supervisor, autoscaling), CPU
> worker mode, metrics, health, hot reload and the machine-readable agent
> spec are all shipped. About 2 700 tests across unit / integration /
> e2e / chaos / property / fuzz / stress suites, plus a separate load-test
> suite that gates every release with conformance, smoke and one-hour
> reference runs. Not yet in production use by anyone but the author.

## Quick Start

```bash
# 1. Start RabbitMQ + Valkey (compose file lives in this repo)
docker compose up -d

# 2. Define a task
cat > myapp/tasks.py <<'EOF'
from swarmq import Task, TaskInfo

class SendEmail(Task):
    task_info = TaskInfo(name="send_email", queues=["default"])

    async def run(self, to: str, subject: str) -> str:
        # ... your code here ...
        return f"sent to {to}"
EOF

# 3. Tell SwarmQ where the broker and backend live
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"

# 4. Run a worker
swarmq worker --module myapp.tasks --queues default --concurrency 10
```

The worker handles SIGTERM / SIGINT for graceful shutdown. Multiple
queues: `--queues default,emails,priority`. Structured logs:
`--log-format json`.

`swarmq worker` sets up logging for its own processes (stderr, `human`
or `json`). An app that embeds `SwarmQ(...)` keeps full control: SwarmQ
never configures logging there and only emits events through named
`swarmq.*` loggers into stdlib `logging`, with the fields as record
attributes. Configure a handler yourself (for example
`logging.basicConfig(level=logging.INFO)`); structlog apps add
`structlog.stdlib.ExtraAdder()` to their `foreign_pre_chain`. Details in
the logging reference (`website/reference/logging.md`).

## Hello World — Client side

```python
import asyncio
from swarmq import SwarmQ, RabbitMQBroker, ValkeyBackend
import myapp.tasks  # noqa — auto-registers SendEmail

async def main():
    app = SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
        queues=["default"],
    )
    async with app:
        task_id = await app.schedule(
            "send_email", to="op@example.com", subject="hi",
        )
        result = await app.get_result(task_id, timeout=10)
        print(result)  # sent to op@example.com

asyncio.run(main())
```

## Reusing an existing connection pool (producer)

If your app already holds its own RabbitMQ connection and/or Valkey
client, hand them to SwarmQ instead of a URL — SwarmQ reuses them rather
than opening a second, parallel connection to the same server. You can
pass either a high-level client/connection **or** a low-level pool, and
mix sources per backend:

```python
import aio_pika
import valkey.asyncio
from swarmq import SwarmQ, RabbitMQBroker, ValkeyBackend

# objects your app already owns
connection = await aio_pika.connect_robust("amqp://guest:guest@localhost:5672/")
valkey_client = valkey.asyncio.Valkey.from_url("valkey://localhost:6379/0")

app = SwarmQ(
    RabbitMQBroker(connection=connection),   # or connection_pool=<aio_pika.pool.Pool>
    ValkeyBackend(client=valkey_client),     # or pool=<valkey ConnectionPool>
    # no broker_url / backend_url needed when a connection object is injected
)
async with app:
    await app.schedule("send_email", to="op@example.com", subject="hi")
# closing `app` does NOT close `connection` or `valkey_client` — your app keeps them
```

Things to know:

- **Ownership:** SwarmQ never closes a connection/client/pool you inject —
  it belongs to your app. SwarmQ does close the publish channel it opens
  on a borrowed RabbitMQ connection (that channel is SwarmQ's).
- **Producer-only:** injection is for the task-*sending* path. Calling
  `start_worker()` on an injected broker/backend raises
  `ConfigurationError` — workers are spawned as subprocesses and
  reconstruct their broker/backend from `SWARMQ_BROKER_URL` /
  `SWARMQ_BACKEND_URL`, which a borrowed connection can't cross.
- **Robustness is yours:** an injected Valkey client/pool does **not**
  inherit SwarmQ's retry / health-check policy, and an injected aio-pika
  connection should be a robust one (`connect_robust`) for SwarmQ's
  channel-reconnect handling to behave as designed.
- **Same namespace assumed:** the injected object must point at the same
  server and logical namespace (RabbitMQ vhost, Valkey DB) the rest of
  your config expects — SwarmQ does not validate this, and a mismatch
  silently sends/reads tasks in the wrong namespace.

## What's in the box

### Core

- **At-least-once delivery** with Quorum-Queue durability and
  `x-delivery-limit=20` against pathological redelivery loops.
- **Retries** with exponential / linear / fixed backoff +
  configurable `max_retries`, `NoRetry`, `Retry(delay=...)`.
- **DLQ** routing for retry-exhaustion + `swarmq dlq list/inspect/
  retry/purge` CLI for operator recovery.
- **Middleware** stack with 6 hooks (pre/post enqueue, pre/post
  execute, on_error, on_retry).
- **Signals** — 23 lifecycle events with `@app.on_signal(...)`
  decorator, from `TASK_ENQUEUED` over `TASK_DLQ` / `TASK_REJECTED` to
  worker, scheduler and workflow-step events.
- **Rate limiting** — Cloudflare 2-period sliding window via Lua
  (`TaskInfo(rate_limit="100/m")`).
- **Locking** — mutex + semaphore via Valkey atomic primitives, with
  format-string keys (`lock="user:{user_id}"`).
- **Unique tasks** — dedup by canonical-encoded args, with
  `unique_until="start"` or `"completion"`.
- **Cancellation** — pre-pickup + during-execution via
  `Client.cancel(task_id)`, dedicated `TASK_CANCELLED` signal.
- **Scheduler** — cron, delayed (`eta=...`), recurring with
  leader-election failover.
- **Progress tracking** — `Task.update_progress(current, total, msg)`
  + `Client.get_progress` / `Client.watch_progress` AsyncIterator.
- **CLI** — `swarmq worker`, `swarmq schedule`, `swarmq dlq *`,
  `swarmq inspect`.

### Workflow primitives

- **`chain` / `group` / `chord`** with `Signature` building blocks
  (`Task.s()`, `sig("name")`, immutable `Task.si()`) and operator
  syntax (`A.s() | B.s()`, `group(...) | merge.s()`). Flat-DAG
  composition with construction-time limits (`max_workflow_depth=10`,
  `max_group_size=1000`).
- **Fire-and-forget `apply()`** returns a reattachable handle:
  `await app.apply(chain(...))` → `handle.get(timeout=...)`, `cancel()`,
  `children`, stable `workflow_id` reattach via `app.workflow(id)`.
- **Worker-driven orchestration** — workers advance the workflow on each
  task completion (chain result-injection, atomic+idempotent chord
  fan-in via Lua). Fail-fast with liveness: a failed step marks the
  workflow failed and wakes `get()` instead of hanging.

```python
result = await (await app.apply(
    chord(group(Download.s(u) for u in urls), Merge.s())
)).get(timeout=30)
```

### Operations

- **Supervisor by default** — every `swarmq worker` runs under a
  process manager, also with `--processes 1`: it restarts crashed
  workers into the same slot (5/hr rolling-window budget), replaces
  workers whose heartbeat stops, reloads on SIGHUP via `os.execvp`
  (replaying the `sys.argv` snapshot captured at start — the container
  CMD or systemd ExecStart stays the trust boundary for argv), and
  takes its children down with it even on SIGKILL. Configuration errors
  end the run with exit code `78` on every level; connection errors stay
  at `1`.
- **Multi-process workers** (`--processes N`) with a stable slot
  identity: `worker_id` is `<host>:<slot>` in signals, metrics and logs,
  and `--metrics-port` / `--health-port` are base ports — slot `s`
  binds `base + s`, so scrape targets survive restarts.
- **Autoscaling** (`--autoscale=MIN,MAX`) — queue-depth-driven
  scale up/down with 30s cooldown, 60s idle window, and a 30s
  minimum worker age that suppresses spawn→immediate-kill churn
  when a crashed worker is restarted during an idle period.
- **Control channel** (`--max-processes N`, `--group LABEL`) — a
  setpoint `{"min": M, "max": K}` under `sq:control:setpoint:<group>`
  changes a group's process count and autoscale bounds at runtime: read
  before the first child spawns and every 5s after, applied within one
  sampling interval, and reverted to the startup arguments when the key
  is removed. The setpoint is per supervisor, bounded by the headroom
  the operator declares at start, and out-of-range, inverted or
  unreadable values are rejected with a reason instead of taking
  effect. Each supervisor reports its effective bounds, their origin
  and its slot counts under `sq:control:status:<group>:<supervisor>`
  with an expiry, so a controlling service can tell "rejected" from
  "not yet applied" from "backend unreachable". Off unless
  `--max-processes` is given.
- **CPU worker mode** (`--pool cpu`) — task bodies run in compute
  children forked from a pre-warmed forkserver; the loop process never
  computes, `hard_timeout` and cancel kill the child's process group,
  DI children are recycled by RSS or task count. POSIX only.
- **Priorities** (`--priorities`, `TaskInfo.priority`), **expiry**
  (`TaskInfo.max_age` → `TASK_EXPIRED` + DLQ) and **burst mode**
  (`--burst --burst-idle-timeout N` drains and exits 0).
- **`--configure MODULE:CALLABLE`** — register middleware and signal
  handlers on the CLI worker without writing your own entry point.
- **Prometheus metrics** (`pip install swarmq[metrics]`) — 10 metrics
  exposed on `/metrics`, drop-in no-op when the dep is absent.
- **Health endpoints** — `/health` (liveness) + `/ready` (broker +
  backend reachable, forkserver in CPU mode, structured 503 body).
- **Hot reload** (`--reload`, `pip install swarmq[reload]`) — file-
  watch trigger that fires the existing SIGHUP drain-and-execvpe
  pipeline. Developer-only (WARNING log on every start); see
  `website/guides/hot-reload.md`.

### Reliability hardening (E2E + Chaos suites)

- **Channel-recreate** survives RabbitMQ broker restarts mid-publish.
- **Pub/Sub resubscribe** survives Valkey restarts mid-stream.
- **Toxiproxy chaos suite** verifies behavior under network latency,
  bandwidth limits, slicer (packet loss), full disconnect, and
  partial partitions.
- **Property-based tests** on dedup-hash, serialization, backoff,
  rate-limiter; **fuzz-tests** on message parsers and external
  publishers.

## Configuration

Worker behaviour is configured via env vars and CLI flags (CLI flag
wins where both are set).

| Variable / flag | Default | Purpose |
|---|---|---|
| `SWARMQ_BROKER_URL` | required | AMQP URL for RabbitMQ |
| `SWARMQ_BACKEND_URL` | required | Valkey/Redis URL for results |
| `--queues a,b,c` | `default` | Queues to consume |
| `--concurrency N` | `10` | Max parallel tasks per worker |
| `--log-level` | `INFO` | DEBUG / INFO / WARNING / ERROR / CRITICAL |
| `--log-format` | `human` | `human` or `json` (`SWARMQ_LOG_FORMAT`) |
| `--log-config PATH` | unset | Your own logging `dictConfig` file instead of the built-in setup |
| `--no-configure-logging` | off | Leave logging entirely to your task module |
| `--logger-level NAME=LEVEL` | none | Per-logger level, repeatable (e.g. `aio_pika=WARNING`) |

Other flags: `--processes`, `--autoscale`, `--pool`, `--metrics-port`,
`--health-port`, `--burst`, `--priorities`, `--configure`, `--check`,
`--log-queue-size`, `--log-lifecycle-level`.
Every flag has an env-var twin (`SWARMQ_QUEUES`, `SWARMQ_CONCURRENCY`,
`SWARMQ_POOL`, …); the full list with defaults is in the
configuration reference (`website/reference/configuration.md`) and the
CLI reference (`website/reference/cli.md`).

## Status and roadmap

Everything listed above is shipped and covered by tests; the
changelog (`website/reference/changelog.md`) records each release. A
machine-readable agent spec — generated JSON for errors, config, CLI
and message schemas plus an `llms.txt` index under
`website/reference/agent/spec/` — is kept in sync with the code by tests
(`python -m swarmq.agent_spec`).

Still open:

- **Bulk-throughput optimisation** — a `schedule_many` fast path
  (design note in `docs/brainstorms/implementation-order-requirements.md`).
- **24-hour acceptance run** of the load-test suite before the first
  production deployment, and the resulting SLO thresholds for p95
  latency and throughput.

## Project layout

```
src/swarmq/
  app.py             SwarmQ application
  worker.py          Consumer loop
  client.py          Schedule API + result retrieval
  task.py            Task base + TaskInfo
  middleware.py      Middleware base + chain
  signals.py         Signal enum + dispatcher
  limits.py          RateLimiter + parser
  locking.py         Mutex / semaphore
  cancellation.py    Cancel-flag protocol
  progress.py        ProgressInfo (R3.7)
  scheduler.py       Embedded cron + delayed scheduler
  metrics.py         Prometheus exporter (R5.1)
  health.py          /health + /ready (R5.2)
  cli.py             argparse entrypoint
  lua/               Lua scripts loaded into Valkey
  broker/rabbitmq.py
  backend/valkey.py
tests/
  unit/              ~895 fast, no infra
  integration/       broker + backend, real Docker
  e2e/               full client + worker round-trip
  chaos/             toxiproxy + container restarts (opt-in, nightly)
  property/          hypothesis property-based (opt-in)
  fuzz/              byte + JSON fuzz (opt-in)
  stress/            high-parallelism (opt-in)
  soak/              1h leak detection (opt-in, weekly)
docs/
  brainstorms/       requirements (per feature)
  plans/             implementation plans (per feature)
  solutions/         postmortem-style learnings
  testing/           test strategy + feature-parity matrix
```

## Development

```bash
# Fast dev loop — unit only
uv run --extra test pytest tests/unit -q

# E2E suite (needs docker compose up -d)
SWARMQ_TEST_RABBITMQ_PORT=5673 SWARMQ_TEST_VALKEY_PORT=6380 \
  uv run --extra test pytest tests/e2e -q

# Chaos suite (starts its own toxiproxy stack per test class)
uv run --extra test pytest tests/chaos -q -m chaos

# E2E feature-combinations suite (31 combos × 3 failure tiers)
# Tier-1 only — no Toxiproxy needed, runs as part of the regular e2e
# suite above. Tier-2/3 (chaos-marked) needs the Toxiproxy stack:
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d
uv run --extra test pytest tests/e2e/combinations -v -m chaos

# Property + fuzz
uv run --extra test pytest tests/property tests/fuzz -q -m "property or fuzz"
```

CI is `.gitlab-ci.yml`: every push runs lint, types, the lock check, unit
(including property and parser-fuzz), integration and e2e — all blocking.
Stress and soak are manual jobs. The chaos suite and the chaos-backed fuzz
run are executed locally before a release; see
`docs/testing/test-strategy.md` for why and how.

For coding conventions and TDD discipline see `AGENTS.md`.

## License

[MIT](LICENSE).
