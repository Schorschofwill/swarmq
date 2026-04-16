---
title: "CLI Minimum: `swarmq worker` so a stranger can run SwarmQ"
type: feature-brief
status: ready-for-planning
date: 2026-04-16
audience: external-user (first-time SwarmQ runner)
---

# CLI Minimum: `swarmq worker` so a stranger can run SwarmQ

## Problem Frame

SwarmQ is at Phase 3b.1. Anyone who wants to try the library has to write a Python entrypoint that imports `SwarmQ`, constructs broker + backend, registers tasks, and calls `start_worker()`. That bootstrap is documented in tests and brainstorms but not as a one-liner. The product-lens reviewer of the deferred comparison-table plan made the case explicitly: "a stranger cannot run a worker without writing custom bootstrap code. Build the CLI before you build a comparison page."

This brief defines the **smallest CLI** that removes that friction. It scopes deliberately to one subcommand. Bigger CLI (DLQ inspection, schedule management, task discovery) waits until there is evidence anyone wants it.

## Goal

After `pip install swarmq`, a user with `myapp/tasks.py` defining one or more `Task` subclasses can run:

```bash
swarmq worker --module myapp.tasks --queues default --concurrency 10
```

…and a worker comes up, declares the queue, starts consuming, and shuts down cleanly on Ctrl-C or SIGTERM. No Python bootstrap file required.

## Out of Scope

- Subcommands other than `worker` (no `dlq`, `schedule`, `tasks list`, `inspect`, `purge`)
- Auto-discovery: `--module myapp` (rekursive Package-Traversierung). Explicit module path only.
- `--burst`, `--autoscale`, `--reload`, `--check`, `--health-port`, `--metrics-port` — Phase-3c-späte oder Phase-5-Themen
- Worker-Pool / Multi-Process via CLI (Phase 4 R4.6 ProcessManager)
- TUI / interactive prompts
- Config-File-Support (`swarmq.toml`) — env-vars + CLI-flags reichen für Phase 3c

## Stakeholders & Audience

- **Primary:** First-time SwarmQ users who want to evaluate the library quickly. They have a Python project, want to see "does this actually work for my use case", and will give up if there's no `swarmq worker` command.
- **Secondary:** Existing users (Georg) who currently re-write the same `start_worker()` boilerplate in every test setup. The CLI replaces this with a single command for non-test usage.
- **Tertiary:** Agents (LLM tools) that want to spawn a SwarmQ worker programmatically — `--log-format json` makes the output machine-readable.

## Decisions Resolved in Brainstorming

| Decision | Choice |
|---|---|
| Scope | One subcommand: `worker` only |
| Task discovery | Explicit `--module dotted.path` via `importlib.import_module` |
| Output | Human-readable default; opt-in `--log-format json` (reuses existing `SwarmQConfig.log_json`) |

## Functional Requirements

- **R1.** `swarmq worker --module DOTTED.PATH` is the canonical invocation. The module is imported once at startup; any `Task` subclass defined in it registers itself via `Task.__init_subclass__` (already shipped in Phase 1).
- **R2.** `--queues a,b,c` (comma-separated) selects which queues this worker consumes. Default: `default`. Validates that at least one queue is named.
- **R3.** `--concurrency N` (positive int) sets the worker's semaphore. Default: 10. Reuse the `_positive_int` argparse helper from `benchmarks/bench.py` (or duplicate the same shape).
- **R4.** `--log-level {DEBUG,INFO,WARNING,ERROR}` (default: INFO). Maps to existing `SwarmQConfig.log_level`.
- **R5.** `--log-format {human,json}` (default: human). Maps to existing `SwarmQConfig.log_json` (`json` → `True`).
- **R6.** Broker and backend URLs come from environment: `SWARMQ_BROKER_URL`, `SWARMQ_BACKEND_URL` — already established in `SwarmQConfig`. CLI does not duplicate these as flags in this minimum.
- **R7.** Module-import errors and missing-task errors print a clear message with the module path that failed and exit non-zero. No stack trace dump unless `--log-level DEBUG`.
- **R8.** SIGTERM and SIGINT trigger the worker's existing graceful shutdown (Phase 2). The CLI does not add its own signal handling on top.
- **R9.** Successful run exits 0 on graceful shutdown. Any startup failure (broker unreachable, module import error, no tasks registered) exits non-zero with a single human-readable error line.
- **R10.** `swarmq --version` and `swarmq --help` work. `swarmq worker --help` shows the flag list.

## Non-Functional Requirements

- **NFR1.** No new runtime dependency. argparse from stdlib (consistent with bench.py).
- **NFR2.** CLI startup overhead under 200ms before `start_worker()` is called (importlib + arg parse only).
- **NFR3.** The CLI module is testable without spawning a real broker — module-import + arg-parsing logic is unit-testable; the start_worker() call gets mocked.
- **NFR4.** The `swarmq` entrypoint is registered via `[project.scripts]` in `pyproject.toml` (already declared as `swarmq = "swarmq.cli:main"` — that file just doesn't exist yet).

## Acceptance Criteria

- A documentation example shows the canonical one-liner and works against the existing `docker compose up` setup.
- E2E test: define a `Task` in a fixture module, run the `swarmq worker` CLI as a subprocess against real RabbitMQ + Valkey, schedule a task via the existing client, verify it executes and the worker shuts down cleanly on SIGTERM.
- Unit tests cover: arg parsing happy paths, invalid `--concurrency 0`, invalid `--module nonexistent`, `--log-format json` correctly maps to `log_json=True`, `--queues` parsing.
- The integration smoke (`tests/integration/test_benchmark_smoke.py`-style) gains a sibling that verifies `swarmq worker --help` exits 0 with expected flag list.

## Honest Risks

| Risk | Mitigation |
|---|---|
| `importlib.import_module` runs side effects (module-level code) — not just registers tasks | Document this in `--help` text; standard Python convention; user's responsibility |
| `--queues default` doesn't exist on the broker — first message dies on enqueue | Worker.start() already declares queues; CLI inherits this |
| User expects `--module myapp` (package) to recurse — gets misleading "no tasks found" | Error message specifically says "did you mean `myapp.tasks` (a module, not a package)?" |
| Help text drift from actual behaviour | argparse builds help text from the same definitions that drive parsing — no drift |

## Related Existing Code

- `pyproject.toml:31` — `swarmq = "swarmq.cli:main"` is already declared. The `cli` module just doesn't exist yet.
- `src/swarmq/config.py` — `SwarmQConfig` already supports `log_level`, `log_json`, `concurrency`, `queues`, `broker_url`, `backend_url` via env vars.
- `src/swarmq/app.py` — `SwarmQ.start_worker()` already does everything the CLI needs to delegate to.
- `src/swarmq/task.py` — `Task.__init_subclass__` registers subclasses on import; no work needed here.
- `benchmarks/bench.py:_positive_int` — argparse helper for `--repeat`, reusable shape for `--concurrency`.

## Sources

- Brainstorm origin: product-lens review of plan 006 (`/home/georg/Dokumente/Development/swarmq_v2/.context/compound-engineering/document-review/plan006-product.json`) — quote: "Build the CLI before you build a comparison page"
- Implementation-order requirements (R3.10): `docs/brainstorms/implementation-order-requirements.md`
- This is intentionally a **subset** of R3.10. Future iterations can add the deferred subcommands and flags one at a time as evidence emerges.
