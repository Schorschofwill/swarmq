<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo/swarmq-horizontal-dark.svg">
    <img src="docs/assets/logo/swarmq-horizontal-light.svg" alt="SwarmQ" width="420">
  </picture>
</p>
<!-- pypi-start -->

<p align="center">
  <strong>The async-first Python task queue that does not lose your work.</strong><br>
  RabbitMQ quorum queues for delivery, Valkey for state – and everything a production worker needs in the core.
</p>

<p align="center">
  <a href="https://pypi.org/project/swarmq/"><img alt="PyPI" src="https://img.shields.io/pypi/v/swarmq?color=1F2B47&label=PyPI"></a>
  <a href="https://pypi.org/project/swarmq/"><img alt="Python 3.14+" src="https://img.shields.io/badge/python-3.14%2B-1F2B47"></a>
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-B4842F"></a>
  <img alt="Typed" src="https://img.shields.io/badge/typing-mypy%20%7C%20pyright-B4842F">
  <img alt="Tests" src="https://img.shields.io/badge/tests-7%20000%2B-1F2B47">
  <img alt="Coverage ≥97 %" src="https://img.shields.io/badge/coverage-%E2%89%A597%25-1F2B47">
  <a href="https://github.com/Schorschofwill/swarmq/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/Schorschofwill/swarmq/ci.yml?branch=main&label=CI"></a>
  <a href="https://swarmq.readthedocs.io/"><img alt="Docs" src="https://img.shields.io/readthedocs/swarmq?label=docs"></a>
</p>

<!-- pypi-skip -->
<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#why-swarmq">Why SwarmQ</a> ·
  <a href="#feature-tour">Features</a> ·
  <a href="#how-it-compares">Comparison</a> ·
  <a href="https://swarmq.readthedocs.io/">Documentation</a> ·
  <a href="website/reference/changelog.md">Changelog</a>
</p>
<!-- pypi-resume -->

---

```python
from swarmq import Task, TaskInfo

class SendEmail(Task):
    task_info = TaskInfo(name="send_email", queues=["default"],
                         rate_limit="100/m", max_retries=5, soft_timeout=30)

    async def run(self, to: str, subject: str) -> str:
        await mailer.send(to, subject)
        return f"sent to {to}"
```

```bash
swarmq worker --module myapp.tasks --processes 4 --autoscale 2,8
```

One `async def`, one command. Retries with backoff, a distributed rate limit, timeouts,
a supervisor that restarts crashed workers and autoscaling on queue depth come along –
no plugins, no glue code.

## Why SwarmQ

<table>
<tr>
<td width="50%" valign="top">

**⚡ Async to the core**<br>
Workers are `asyncio` all the way down (aio-pika, valkey-py). Thousands of I/O-bound
tasks share one event loop with bounded concurrency – no thread pools, no new loop per job.

</td>
<td width="50%" valign="top">

**🛡️ At-least-once by default**<br>
Quorum queues, ack *after* execution, a delivery limit against poison loops and a
dead-letter queue with a recovery CLI. A crash, a broker restart or a Valkey outage
delays work – it does not drop it.

</td>
</tr>
<tr>
<td valign="top">

**🧮 CPU work without blocking the loop**<br>
`--pool cpu` runs task bodies in compute children forked from a pre-warmed forkserver
(a few milliseconds per start). Hard timeouts and cancel kill the child's process group, never
the worker.

</td>
<td valign="top">

**🎛️ Control built in, not bolted on**<br>
Distributed sliding-window rate limits, per-key locks and semaphores, unique tasks,
priorities, expiry, cancellation and progress – all atomic in Valkey via Lua.

</td>
</tr>
<tr>
<td valign="top">

**🔗 Workflows that finish – or fail loudly**<br>
`chain`, `group` and `chord` with operator syntax. Workers advance the workflow, the
chord fan-in is atomic and idempotent, and a failed step wakes every waiter instead of
hanging.

</td>
<td valign="top">

**📈 Operable from day one**<br>
Supervisor by default, autoscaling, a runtime control channel, Prometheus metrics,
`/health` and `/ready`, structured non-blocking logging, cron with IANA time zones and
leader failover.

</td>
</tr>
</table>

**Tested like infrastructure.** More than 7 000 tests across unit, integration, e2e,
chaos (Toxiproxy), property, fuzz, stress and soak suites, a 100 % diff-coverage gate,
mutation testing, and a separate load-test suite that accepts every release with
conformance checks, smoke runs and one-hour reference runs (24 hours for larger changes).
Deliberate behaviour is recorded in a decision register and pinned by tests.

## Quick start

```bash
pip install swarmq            # Python 3.14+
docker compose up -d          # RabbitMQ + Valkey from this repository
```

**1. Define a task** – `myapp/tasks.py`:

```python
from swarmq import Task, TaskInfo

class SendEmail(Task):
    task_info = TaskInfo(name="send_email", queues=["default"])

    async def run(self, to: str, subject: str) -> str:
        return f"sent to {to}"
```

**2. Start a worker:**

```bash
export SWARMQ_BROKER_URL="amqp://guest:guest@localhost:5672/"
export SWARMQ_BACKEND_URL="valkey://localhost:6379/0"
swarmq worker --module myapp.tasks
```

**3. Schedule from your app and wait for the result:**

```python
import asyncio
from swarmq import SwarmQ, RabbitMQBroker, ValkeyBackend
import myapp.tasks  # registers SendEmail

async def main() -> None:
    app = SwarmQ(
        RabbitMQBroker("amqp://guest:guest@localhost:5672/"),
        ValkeyBackend("valkey://localhost:6379/0"),
    )
    async with app:
        task_id = await app.schedule("send_email", to="op@example.com", subject="hi")
        print(await app.get_result(task_id, timeout=10))  # sent to op@example.com

asyncio.run(main())
```

The worker drains gracefully on `SIGTERM`. Next steps: the
[getting-started guide](website/getting-started.md) and the
[first-worker tutorial](website/tutorials/first-email-worker.md).

## Feature tour

<details open>
<summary><strong>Reliable delivery</strong></summary>

- **At-least-once** on RabbitMQ quorum queues, ack after execution, `x-delivery-limit`
  against redelivery loops; `DeliveryMode.FIRE_AND_FORGET` per task where speed beats
  durability.
- **Retries** with exponential, linear or fixed backoff and jitter; `Retry(delay=...)`
  and `NoRetry` from inside the task.
- **Dead-letter queue** on exhausted retries, expiry or rejection, with
  `swarmq dlq list / inspect / retry / purge`.
- **Backend outages** before execution defer the message with backoff instead of
  burning its delivery budget; publish, channel and pub/sub paths reconnect on their own.
- **Soft and hard timeouts** – `soft_timeout` fails the attempt (retry policy applies),
  `hard_timeout` cancels it (in CPU mode: kills the compute child).

</details>

<details>
<summary><strong>Task control</strong></summary>

- **Rate limits** – `TaskInfo(rate_limit="100/m")`, a two-period sliding window in Lua,
  shared by every worker.
- **Locks** – mutex or semaphore per key: `lock="user:{user_id}"`.
- **Unique tasks** – deduplicated by canonical arguments until start or completion.
- **Priorities**, **expiry** (`max_age`), **cancellation** before and during execution,
  **progress** reporting with `app.watch_progress(...)`.
- **Scheduler** – cron with an IANA time zone per task and a fixed rule for clock
  changes, delayed tasks (`eta=`), leader election with failover; one Valkey can serve
  several apps (`SWARMQ_NAMESPACE`).
- **Batch API** – `schedule_many`, `get_results`, `run`.

</details>

<details>
<summary><strong>Workflows</strong></summary>

```python
from swarmq import chain, chord, group

handle = await app.apply(chord(group(Download.s(u) for u in urls), Merge.s()))
result = await handle.get(timeout=30)
```

- `chain`, `group`, `chord` with `Task.s()`, immutable `Task.si()`, `sig("name")` and
  `|` syntax; construction-time limits on depth and group size.
- `app.apply(...)` returns a handle: `get()`, `status()`, `children()`, `cancel()`, and
  reattach anywhere with `app.workflow(workflow_id)`.

</details>

<details>
<summary><strong>Running workers</strong></summary>

- **Supervisor by default** – restarts crashed workers into the same slot, replaces
  workers with a stalled heartbeat, reloads on `SIGHUP`, takes its children down even on
  `SIGKILL`. Exit code `78` for configuration errors.
- **Multi-process** (`--processes N`) with stable slot identities (`<host>:<slot>`) in
  logs, metrics and signals.
- **Autoscaling** (`--autoscale MIN,MAX`) on queue depth, with cooldown and idle windows.
- **Control channel** (`--max-processes`, `--group`) – change process counts and
  autoscale bounds at runtime through Valkey, with a status report per supervisor.
- **CPU mode** (`--pool cpu`) – forkserver-based compute children, recycled by RSS or
  task count.
- **Burst mode** (`--burst`) for batch jobs; **hot reload** (`--reload`) for development.

</details>

<details>
<summary><strong>Extensibility and observability</strong></summary>

- **Middleware** with six hooks and stack semantics; **30 lifecycle signals** with
  `@app.on_signal(...)`; `--configure MODULE:CALLABLE` wires both into the CLI worker.
- **Dependency injection** via Dishka (`pip install 'swarmq[dishka]'`): annotate
  parameters with `FromDishka[...]`, one request scope per task.
- **Structured logging** – human or JSON through a bounded, non-blocking queue; every
  event inside a task carries `task_id`, `queue`, `worker_id`, `attempt` and more. Your
  own `dictConfig` and structlog work too.
- **Prometheus metrics** (`pip install 'swarmq[metrics]'`) and **HTTP health**
  (`/health`, `/ready` with a structured 503 body).
- **Bring your own connections** – hand an existing aio-pika connection or Valkey client
  to `RabbitMQBroker` / `ValkeyBackend`; SwarmQ never closes what it did not open.
- **Machine-readable spec** – JSON schemas for messages, errors, config and CLI plus an
  `llms.txt`, generated from the code and checked by tests.

</details>

## How it compares

A condensed view of a feature comparison of eleven Python task queues (core features of
the current stable release, October 2026). ✅ in the core · ◐ partial or manual ·
🔌 plugin only · ❌ not available

| | **SwarmQ** | Celery | Dramatiq | TaskIQ | arq |
|---|:-:|:-:|:-:|:-:|:-:|
| Async tasks in the worker | ✅ | ❌ | ◐ | ✅ | ✅ |
| At-least-once by default | ✅ | ❌ | ✅ | ◐ | ✅ |
| Soft **and** hard timeout | ✅ | ✅ | ◐ | ◐ | ◐ |
| Distributed rate limit | ✅ | ❌ | ◐ | ❌ | ❌ |
| Locks / concurrency per key | ✅ | ◐ | ◐ | ❌ | ◐ |
| DLQ with requeue tooling | ✅ | ❌ | ◐ | 🔌 | ❌ |
| Cron without duplicates across instances | ✅ | 🔌 | ❌ | ❌ | ✅ |
| Workflows (chain / group / chord) | ✅ | ✅ | ✅ | 🔌 | ❌ |
| Pool for CPU-bound work | ✅ | ✅ | ◐ | ✅ | ❌ |
| Autoscaling | ✅ | ◐ | ❌ | ❌ | ❌ |
| HTTP health endpoint | ✅ | ❌ | ❌ | ❌ | ◐ |

**Where SwarmQ is the wrong pick – today:** it is young (0.x, one maintainer, no
production users besides the author), needs Python 3.14+ and *both* RabbitMQ and Valkey,
runs `async def` tasks only, has no dashboard, no in-memory test broker and no Windows
support. If you need the largest ecosystem, choose Celery; for simple synchronous jobs on
Redis, RQ or Dramatiq.

## Configuration at a glance

Everything is set by CLI flag, `SWARMQ_*` environment variable or constructor argument;
`swarmq worker` resolves each option once as **flag > env > default** for the
supervisor, all workers and compute children alike.

| Flag / variable | Default | Purpose |
|---|---|---|
| `SWARMQ_BROKER_URL` | required | AMQP URL of RabbitMQ |
| `SWARMQ_BACKEND_URL` | required | Valkey URL for results and coordination |
| `--queues a,b` | `default` | Queues to consume |
| `--concurrency N` | `10` (io) / CPUs ÷ processes (cpu) | Parallel tasks per worker |
| `--processes N` / `--autoscale MIN,MAX` | `1` / off | Process count, fixed or by queue depth |
| `--pool io\|cpu` | `io` | Event loop only, or forkserver compute children |
| `--log-format human\|json` | `human` | Log output on stderr |
| `--metrics-port` / `--health-port` | off | Base ports; slot `s` binds `base + s` |

The complete list lives in the [configuration](website/reference/configuration.md) and
[CLI](website/reference/cli.md) references.

## Documentation

| | |
|---|---|
| 🚀 [Getting started](website/getting-started.md) | Install, run a worker, get a result |
| 🎓 [Tutorial](website/tutorials/first-email-worker.md) | Build a complete worker step by step |
| 🧭 [How-to guides](website/guides/index.md) | Retries, rate limits, locking, workflows, CPU mode, deployment, testing |
| 📚 [Reference](website/reference/index.md) | API, CLI, configuration, logging, errors, changelog |
| 🧠 [Explanation](website/explanation/index.md) | Architecture, delivery semantics, design decisions |
| 🤖 [Agent reference](website/reference/agent/index.md) | Machine-readable spec and `llms.txt` |

The documentation is available in English and German and builds with
`uv run --extra docs mkdocs serve`.

<!-- pypi-end -->
## Development

SwarmQ is developed strictly test-first; the rules are in [`AGENTS.md`](AGENTS.md), the
test strategy in [`docs/testing/test-strategy.md`](docs/testing/test-strategy.md).

```bash
uv run just            # list recipes
uv run just unit       # fast unit tests, no services
uv run just all        # unit, property, fuzz, integration, e2e (test ports 5699/6399)
uv run just lint       # format + ruff
uv run just types      # mypy, basedpyright, stubtest
```

Integration and e2e recipes start against dedicated test ports and take a shared lock,
so they never touch a RabbitMQ or Valkey on the default ports.

## License

[MIT](LICENSE) · Logo and brand assets: [`docs/assets/logo`](docs/assets/logo/README.md)
