---
title: "feat: Minimum CLI — `swarmq worker`"
type: feat
status: completed
date: 2026-04-16
completed: 2026-04-16
origin: docs/brainstorms/2026-04-16-cli-minimum-worker-requirements.md
---

# feat: Minimum CLI — `swarmq worker`

## Overview

Implement the smallest CLI that lets a stranger run SwarmQ without writing a Python entrypoint:

```bash
swarmq worker --module myapp.tasks --queues default --concurrency 10
```

The `[project.scripts]` entry already points at `swarmq.cli:main` — that file just needs to exist. Everything the CLI needs (`SwarmQConfig`, `SwarmQ`, `Task` auto-registration, graceful shutdown) is already shipped in Phases 1-3a. This plan is glue plus tests, not new infrastructure.

## Problem Frame

Five-reviewer document-review of plan 006 surfaced the actual blocker for SwarmQ adoption: there's no CLI. Anyone who wants to evaluate the library has to read tests, copy a bootstrap, and run `python -m`. That's three steps too many. After this plan ships, the README quickstart is a single command.

## Requirements Trace

- **R1.** `swarmq worker --module DOTTED.PATH` imports the module, then delegates to `SwarmQ.start_worker()`. Tasks register themselves via `Task.__init_subclass__`.
- **R2.** `--queues a,b,c` (comma-separated) → `SwarmQConfig.queues`. Default `default`. Empty / whitespace-only after parsing → ArgumentTypeError.
- **R3.** `--concurrency N` (positive int) → `SwarmQConfig.concurrency`. Default 10. Reuse the `_positive_int` shape from `benchmarks/bench.py`.
- **R4.** `--log-level {DEBUG,INFO,WARNING,ERROR}` → `SwarmQConfig.log_level`. Default INFO.
- **R5.** `--log-format {human,json}` → `SwarmQConfig.log_json` (`json` → True). Default human.
- **R6.** `SWARMQ_BROKER_URL` and `SWARMQ_BACKEND_URL` env vars supply broker/backend URLs (already in `SwarmQConfig.__post_init__`). CLI does not duplicate as flags in this minimum.
- **R7.** Module-import errors and "no tasks registered" exit non-zero with a single-line human-readable error. Stack trace only at `--log-level DEBUG`.
- **R8.** SIGTERM/SIGINT → existing graceful shutdown (Phase 2). CLI adds nothing on top.
- **R9.** Graceful exit → 0. Startup failure → non-zero with one error line.
- **R10.** `swarmq --version` and `swarmq --help` and `swarmq worker --help` work and exit 0.

## Scope Boundaries

- One subcommand (`worker`) only. No `dlq`, `schedule`, `tasks`, `inspect`.
- No auto-discovery of packages (`--module myapp` recursive). Explicit module path only.
- No `--burst`, `--reload`, `--check`, `--health-port`, `--metrics-port`, `--autoscale`, `--metrics-port` — Phase 3c+ or 5.
- No config-file support. Env vars + CLI flags are the only configuration channels.
- No interactive prompts.

### Deferred to Separate Tasks

- Additional subcommands (DLQ, scheduler, tasks-list) — separate plans when there's evidence of demand.
- Auto-discovery of task modules across a package — Phase 3c if at all.
- README update demonstrating the CLI — separate doc task once the CLI lands.

## Context & Research

### Existing primitives the CLI just wraps

- `pyproject.toml:31` — `swarmq = "swarmq.cli:main"` already declared. We just need to create the file.
- `src/swarmq/config.py` — `SwarmQConfig` with `broker_url`, `backend_url`, `queues`, `concurrency`, `log_level`, `log_json` plus `SWARMQ_*` env-var support.
- `src/swarmq/app.py` — `SwarmQ.__init__(broker, backend, **config_kwargs)` and `SwarmQ.start_worker()` are the two calls the CLI makes.
- `src/swarmq/broker/rabbitmq.py` and `src/swarmq/backend/valkey.py` — concrete broker/backend; CLI constructs them with URLs from config.
- `src/swarmq/task.py:__init_subclass__` — auto-registers `Task` subclasses on import. CLI just needs to `importlib.import_module` the path and the registry fills itself.
- `src/swarmq/worker.py:start()` — already installs SIGTERM/SIGINT handlers. CLI does not duplicate.
- `benchmarks/bench.py:_positive_int` — the argparse type-function shape we mirror for `--concurrency`.

### Institutional learnings to apply

- `docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-2026-04-16.md` — not directly related, but reinforces "let the worker handle its lifecycle; don't add a parallel one in the CLI."
- `docs/solutions/patterns/measure-before-planning-variance-claims-2026-04-16.md` — keeps the plan honest: no premature optimization of CLI startup time without a measurement.

## Key Technical Decisions

- **argparse, not click/typer.** Same precedent as `bench.py`; no new dependency. Subcommand support is one `add_subparsers()` call away — fine for one subcommand, scales to a few without rewriting.
- **`swarmq.cli` as a single file, not a package.** One `main()`, one `_build_parser()`, one `_run_worker(args)`. Splitting into subpackages is premature for one subcommand.
- **Module import via `importlib.import_module(args.module)` directly.** No try/except around it for the happy path — let `ModuleNotFoundError` surface and be caught at the top-level CLI handler. The handler renders a single-line error with the module name and exits non-zero.
- **No tasks registered → error.** After import, `Task._registered_tasks` should be non-empty. If empty, the CLI exits with `"no tasks registered after importing {module}; check that your tasks subclass swarmq.Task"`.
- **`SwarmQConfig` already does the env-var merge.** CLI passes flags as `**config_kwargs` to `SwarmQ()` constructor, which constructs `SwarmQConfig`. Env vars override defaults but not explicit CLI flags. Verify this precedence is what `SwarmQConfig` already does — if not, document the divergence.
- **Use `asyncio.run(swarmq.start_worker())` at the CLI entrypoint.** `start_worker()` is the existing async coroutine that owns connect → consume → block-on-shutdown.
- **Top-level error handling: one try/except in `main()`** that catches `ConfigurationError`, `BrokerConnectionError`, `BackendConnectionError`, `ModuleNotFoundError`. Prints `f"swarmq worker: {exc}"` to stderr, exits 1. At `--log-level DEBUG`, prints traceback before exiting.
- **`--version` reads `swarmq.__version__`** if present; otherwise reads `importlib.metadata.version("swarmq")`. Falls back to `"unknown"` if neither works.

## Open Questions

### Resolved during planning

- **CLI framework:** argparse (no new dep)
- **Subcommand structure:** add_subparsers, one subcommand for now, room to grow
- **Module discovery:** explicit `importlib.import_module`, no recursion
- **Error rendering:** single-line stderr at default log level; full traceback at DEBUG

### Deferred to implementation

- Exact phrasing of error messages — wordsmith during implementation
- Whether `--module` is positional or named — implementation decides; named (`--module`) is more typical for argparse-discoverability
- Where `__version__` lives in `src/swarmq/__init__.py` — likely already exists; check during implementation

## Implementation Units

- [x] **Unit 1: `swarmq.cli` module — argparse + main + run_worker**

**Goal:** Create `src/swarmq/cli.py` with `main()` as the script entrypoint and a single `worker` subcommand that launches `SwarmQ.start_worker()`.

**Requirements:** R1, R2, R3, R4, R5, R6, R8, R9, R10

**Dependencies:** None

**Files:**
- Create: `src/swarmq/cli.py`
- Test: `tests/unit/test_cli.py`

**Approach:**
- `main()` is sync — builds the parser, parses argv, dispatches to the subcommand handler. Subcommand handler for `worker` is sync at the boundary, then calls `asyncio.run(_run_worker_async(args))`.
- `_build_parser()` creates the top-level parser with `--version` and adds `worker` subparser with `--module`, `--queues`, `--concurrency`, `--log-level`, `--log-format`. The `--queues` arg uses `type=` to split-and-validate. The `--concurrency` arg uses the same `_positive_int` pattern as `bench.py` — duplicate or import.
- `_run_worker_async(args)`:
  1. `importlib.import_module(args.module)` — let exceptions propagate to top-level handler
  2. Construct `RabbitMQBroker(broker_url)` and `ValkeyBackend(backend_url)` from `SwarmQConfig` (which reads env vars)
  3. Construct `SwarmQ(broker, backend, queues=args.queues, concurrency=args.concurrency, log_level=args.log_level, log_json=(args.log_format == "json"))`
  4. Verify `Task._registered_tasks` is non-empty; otherwise raise `ConfigurationError("no tasks registered after importing {args.module}")`
  5. `async with app: await app.start_worker()`
- Top-level exception handler in `main()` catches the documented exception classes, formats single-line error, exits 1. At DEBUG-level, prints traceback before exit.

**Patterns to follow:**
- argparse layout: `bench.py` lines around `def main` and `_positive_int`
- Async-CLI entrypoint: `tests/e2e/test_*.py` shows `asyncio.run(main())`-style invocation
- Error wrapping: `swarmq.exceptions` already has the exception types; just import and catch at the boundary

**Test scenarios:**
- Happy path: `_build_parser()` produces a parser that accepts `worker --module foo --queues a,b --concurrency 5 --log-level DEBUG --log-format json` and yields the right Namespace
- Edge case: `--concurrency 0` → ArgumentTypeError
- Edge case: `--concurrency abc` → ArgumentTypeError
- Edge case: `--queues ""` → ArgumentTypeError (no valid queue names after split)
- Edge case: `--queues "a, b , c "` → `["a", "b", "c"]` (whitespace stripped, mirrors `SwarmQConfig` env-var behaviour)
- Edge case: `--log-format jsonl` → ArgumentError (choices)
- Edge case: `--log-level VERBOSE` → ArgumentError (choices)
- Happy path: `--version` exits 0, prints something non-empty
- Happy path: `worker --help` exits 0, mentions `--module`, `--queues`, `--concurrency`
- Error path: `_run_worker_async` with non-existent module → `ModuleNotFoundError` propagates
- Error path: `_run_worker_async` after importing a module with no tasks → `ConfigurationError`
- Error rendering: top-level `main()` with mocked `_run_worker_async` raising `BrokerConnectionError` → exits 1, stderr contains a single line with the error

**Verification:**
- `uv run pytest tests/unit/test_cli.py` green
- `uv run swarmq --version` exits 0
- `uv run swarmq --help` shows the `worker` subcommand
- `uv run swarmq worker --help` shows all the flags

---

- [x] **Unit 2: Integration / smoke test — real subprocess against Docker**

**Goal:** Spawn `swarmq worker` as a subprocess against running RabbitMQ + Valkey, schedule a task via the existing client, verify execution, then SIGTERM the worker and check it exits 0.

**Requirements:** R1, R8, R9 — end-to-end proof that the CLI delegates correctly to `start_worker()` and that signal handling round-trips.

**Dependencies:** Unit 1

**Files:**
- Create: `tests/integration/test_cli_worker.py`
- Create: `tests/integration/_cli_fixture_tasks.py` (a small module with one `EchoTask`-style class for the worker to load)

**Approach:**
- Pytest fixture starts `subprocess.Popen(["uv", "run", "swarmq", "worker", "--module", "tests.integration._cli_fixture_tasks", "--queues", "test_q", "--concurrency", "2", "--log-format", "json"], env={...with SWARMQ_BROKER_URL and SWARMQ_BACKEND_URL set to localhost})`
- Wait for worker to be ready: poll for the JSON-log line `{"event": "worker_started"}` on stdout (or use a queue-declare confirmation in RabbitMQ — pick the simpler signal during implementation).
- Schedule a task via `SwarmQ.client.schedule(...)` from the test process; verify the result.
- Send SIGTERM to the subprocess; assert it exits 0 within `_BENCH_HANG_TIMEOUT`-equivalent (use `60s` here).
- Assert no `Connection closed`, no `TimeoutError`, no `Traceback` in the worker's stderr.

**Patterns to follow:**
- `tests/integration/test_benchmark_smoke.py` — subprocess spawning, port-check skip, asserting exit code and stderr signals.
- Same `_port_open` helper as benchmark smoke (extract to `tests/integration/conftest.py` if not already there as part of the maintainability cleanup deferred from plan 005).

**Test scenarios:**
- Happy path: worker starts, executes scheduled task, exits 0 on SIGTERM
- Failure: invalid `--module` → exits non-zero quickly with the documented error message

**Verification:**
- `uv run pytest tests/integration/test_cli_worker.py` green when Docker is running
- Test is auto-skipped via the existing `check_docker` fixture when Docker is not running

---

- [x] **Unit 3: README quickstart entry**

**Goal:** Add the canonical CLI invocation to README so the first-time user finds it without reading the source.

**Requirements:** R1 + R10 — the canonical one-liner is now valid and useful.

**Dependencies:** Unit 1, Unit 2

**Files:**
- Modify: `README.md`

**Approach:**
- Add a `## Quick Start` section near the top with:
  ```bash
  pip install swarmq
  export SWARMQ_BROKER_URL=amqp://guest:guest@localhost:5672/
  export SWARMQ_BACKEND_URL=valkey://localhost:6379/0
  swarmq worker --module myapp.tasks --queues default --concurrency 10
  ```
- 3-line example task definition for `myapp/tasks.py` so the snippet is self-contained.
- Link to the existing `docker-compose.yml` for the broker/backend setup.

**Test scenarios:** none — pure documentation, verified manually

**Test expectation:** none — pure documentation

**Verification:**
- README renders correctly on GitHub (manual visual)
- Snippet works against the project's `docker compose up`

## System-Wide Impact

- **Interaction graph:** New `swarmq.cli` module imports `swarmq.app`, `swarmq.config`, `swarmq.broker.rabbitmq`, `swarmq.backend.valkey`, `swarmq.exceptions`. No reverse imports — `cli` is a leaf.
- **Error propagation:** Errors flow to a single top-level handler in `main()` which renders one-line stderr and `sys.exit(1)`. No unhandled exceptions reach the user as a Python traceback unless `--log-level DEBUG`.
- **State lifecycle risks:** None new — `SwarmQ.start_worker()` already owns the lifecycle.
- **API surface parity:** Adds the `swarmq` console script. No changes to the Python API.
- **Unchanged invariants:** `SwarmQ`, `Worker`, `SwarmQConfig`, `Task`, every test from previous phases — untouched.

## Risks & Dependencies

| Risk | Mitigation |
|---|---|
| `importlib.import_module` runs side effects in user code | Documented in `--help` text; standard Python convention; user's responsibility |
| Worker doesn't shut down within timeout in integration test → flaky CI | Use the existing `shutdown_timeout` config; integration test sets a generous wait; failure means the actual worker has a shutdown bug, not the CLI |
| Env-var precedence (env vs CLI flag) is ambiguous | Document explicitly: CLI flag wins; env vars supply defaults that CLI flags override. Verify in unit tests with monkeypatched env |
| README snippet drifts from real behaviour | Integration test exercises the same one-liner shape; README example checked against test fixture |
| Existing `swarmq = "swarmq.cli:main"` entry might conflict if `cli.py` already exists as a stub | Verify `src/swarmq/cli.py` is absent before Unit 1; if a stub exists, replace its contents fully |

## Documentation / Operational Notes

- README quickstart is Unit 3 above.
- No new ops concerns — worker lifecycle is unchanged.
- `swarmq --version` should land in CI output and bug reports; tag in release notes when this ships.

## Sources & References

- Origin: `docs/brainstorms/2026-04-16-cli-minimum-worker-requirements.md`
- Trigger: product-lens review of plan 006 — `.context/compound-engineering/document-review/plan006-product.json`
- Implementation-order brainstorm (R3.10 is the long-term CLI scope this is a subset of): `docs/brainstorms/implementation-order-requirements.md`
- Existing entrypoint declaration: `pyproject.toml:31`
- Existing config / env-var handling: `src/swarmq/config.py`
- Worker lifecycle (signal handling, graceful shutdown): `src/swarmq/worker.py:start`, `src/swarmq/worker.py:_execute_shutdown`
