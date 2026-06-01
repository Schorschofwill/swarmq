# SwarmQ

Async-first Python task queue on RabbitMQ + Valkey.

> **Status:** Phase 3 + E2E hardening complete; Phase 5 in progress
> (metrics + health endpoints landed, hot-reload + agent-docs to come).
> 1280+ tests across unit / integration / e2e / chaos / property / fuzz
> / stress suites. Not yet production-tested by anyone but the author.

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

## What's in the box

### Core (Phase 1+2+3)

- **At-least-once delivery** with Quorum-Queue durability and
  `x-delivery-limit=20` against pathological redelivery loops.
- **Retries** with exponential / linear / fixed backoff +
  configurable `max_retries`, `NoRetry`, `Retry(delay=...)`.
- **DLQ** routing for retry-exhaustion + `swarmq dlq list/inspect/
  retry/purge` CLI for operator recovery.
- **Middleware** stack with 6 hooks (pre/post enqueue, pre/post
  execute, on_error, on_retry).
- **Signals** — 14 lifecycle events with `@app.on_signal(...)`
  decorator.
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

### Operations (Phase 4 + Phase 5)

- **Multi-process workers** (`--processes N`) — supervisor starts N
  subprocesses, restarts crashed workers (5/hr rolling-window limit),
  reloads on SIGHUP via `os.execvp`. SIGHUP replays the `sys.argv`
  snapshot captured at supervisor start (by design — in-process
  argv-mutation cannot influence the re-exec vector). Sichere
  Launch-Entrypoints (Container CMD, systemd ExecStart) bleiben die
  Trust-Grenze für das initiale argv.
- **Autoscaling** (`--autoscale=MIN,MAX`) — queue-depth-driven
  scale up/down with 30s cooldown, 60s idle window, and a 30s
  minimum worker age that suppresses spawn→immediate-kill churn
  when a crashed worker is restarted during an idle period.
- **Prometheus metrics** (`pip install swarmq[metrics]`) — 7 metrics
  exposed on `/metrics`, drop-in no-op when the dep is absent.
- **Health endpoints** — `/health` (liveness) + `/ready` (broker +
  backend reachable, structured 503 body).
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
| `--log-level` | `INFO` | DEBUG / INFO / WARNING / ERROR |
| `--log-format` | `human` | `human` or `json` |

Other env vars: `SWARMQ_RESULT_TTL`, `SWARMQ_QUEUES`,
`SWARMQ_CONCURRENCY`, `SWARMQ_LOG_LEVEL`, `SWARMQ_LOG_JSON`. Full
list in `src/swarmq/config.py`.

## What's planned next

Phase 4 operator features complete (Priorities, Expiry, Burst,
ProcessManager, Autoscaling). Phase 5 Wave 1 complete (Metrics,
Health, Hot reload). Workflow primitives (`chain` / `group` / `chord`
+ Signatures) implemented — see "Workflow primitives" above.
Machine-readable agent spec (R5.5) implemented — generated JSON for
errors, config, CLI, and message schemas plus an `llms.txt` index
under `reference/agent/spec/`, kept in sync with the code by tests
(`python -m swarmq.agent_spec`).
Remaining — bulk-throughput optimisation (`schedule_many` fast path)
— see `docs/brainstorms/implementation-order-requirements.md`.

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

Three-tier CI in `.github/workflows/`: pre-merge (≤5 min, every PR),
nightly (chaos + stress + fuzz), weekly (mutation + 1h soak).

For coding conventions and TDD discipline see `AGENTS.md`.

## License

TBD.
