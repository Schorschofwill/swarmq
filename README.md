# SwarmQ

Python task queue on RabbitMQ + Valkey, built async-first.

> **Status:** early development — Phase 3 of 5. The core round-trip,
> retry/DLQ, middleware, signals, and global rate limiting are in.
> Locking, deduplication, cancellation, scheduler, and full CLI are
> still on the roadmap. Not production-tested by anyone but the author.

## Quick Start

Bring up the broker and backend, then run a worker against your tasks
in one command — no Python entrypoint required.

```bash
# 1. Start RabbitMQ + Valkey (compose file lives in this repo)
docker compose up -d

# 2. Define a task
cat > myapp/tasks.py <<'EOF'
from swarmq.task import Task, TaskInfo

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

The worker handles SIGTERM/SIGINT for graceful shutdown. To consume
multiple queues: `--queues default,emails,priority`. For
agent/log-aggregator output: `--log-format json`.

## What works today

- RabbitMQ broker + Valkey result backend
- At-least-once delivery with retries, exponential/linear/fixed backoff,
  DLQ routing
- Middleware (6 hooks: pre/post enqueue, pre/post execute, on_error,
  on_retry) with stack semantics
- Signal/event system (14 lifecycle signals + `@on_signal` decorator)
- Global rate limiting via Lua + sliding-window counter
  (`TaskInfo(rate_limit="100/m")`)
- Soft and hard timeouts, semaphore-bounded concurrency
- Pub/Sub-based result notification (no result-polling latency)
- Graceful shutdown with task drain
- 476 tests (unit + integration + e2e) on CI-equivalent local runs

## What's planned next

- Phase 3b.2: Task locking (mutex + semaphore) and unique tasks (deduplication)
- Phase 3b.3: Task cancellation
- Phase 3c rest: Scheduler, progress tracking, additional CLI subcommands
  (`dlq`, `schedule`)
- Phase 4: Workflow primitives (chain / group / chord), bulk enqueue,
  task expiry
- Phase 5: Prometheus metrics, health endpoints, autoscaling

See `docs/brainstorms/implementation-order-requirements.md` for the
full roadmap and `docs/plans/` for per-phase plans.

## Configuration

Worker behaviour is configured via environment variables and CLI flags
(CLI flag wins where both are set).

| Variable / flag | Default | Purpose |
|---|---|---|
| `SWARMQ_BROKER_URL` | required | AMQP URL for RabbitMQ |
| `SWARMQ_BACKEND_URL` | required | Valkey/Redis URL for results |
| `--queues a,b,c` | `default` | Queues to consume |
| `--concurrency N` | `10` | Max parallel tasks per worker |
| `--log-level` | `INFO` | DEBUG / INFO / WARNING / ERROR |
| `--log-format` | `human` | `human` or `json` |

`SWARMQ_*` env vars also exist for `RESULT_TTL`, `QUEUES`,
`CONCURRENCY`, `LOG_LEVEL`, `LOG_JSON` — see `src/swarmq/config.py`.

## Project layout

```
src/swarmq/        library code
  app.py           SwarmQ application — broker/backend wiring + start_worker
  worker.py        Consumer loop with middleware, signals, rate-limit, retry
  client.py        Schedule API and result retrieval
  task.py          Task base class + TaskInfo
  middleware.py    Middleware base + chain
  signals.py       Signal enum + dispatcher
  limits.py        RateLimiter + parser
  lua/             Lua scripts loaded into Valkey
  broker/          RabbitMQ broker
  backend/         Valkey backend
tests/             unit + integration + e2e
benchmarks/        bench.py with --repeat for stable median measurements
docs/
  brainstorms/     requirements documents (per feature)
  plans/           implementation plans (per feature)
  solutions/       postmortem-style learnings
  recherche-*.md   external library / pattern research
```

## Development

```bash
# Run all tests (476 currently)
uv run pytest tests/

# Run benchmarks against the docker-compose stack
docker compose up -d
cd benchmarks && uv run --with-requirements requirements.txt python -u bench.py --repeat 5
```

For hacking on the library, see `AGENTS.md` for the coding conventions
and TDD discipline expected.

## Honest comparison

How does SwarmQ stack up against Celery, Dramatiq, TaskIQ, arq, RQ,
Huey? A side-by-side table is on the roadmap but deferred until the
CLI exists (you are reading the README that closes that milestone) so
that "stranger can run a worker without writing bootstrap code"
becomes true. The comparison will live at `docs/comparison.html` once
written; in the meantime, see `docs/evaluation-task-queues.md` for the
research that informed SwarmQ's design.

## License

TBD.
