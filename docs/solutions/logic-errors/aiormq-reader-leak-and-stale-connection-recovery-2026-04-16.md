---
title: "Benchmark Debugging: aiormq _reader-Task-Leak + stale RobustConnection between scenarios"
date: 2026-04-16
category: logic-errors
module: benchmarks
problem_type: logic_error
component: tooling
symptoms:
  - "bench.py hangs in Szenario 6 (full_pipeline) after ~14 min when running n=50000 -s all, no output, Python process alive but wchar frozen"
  - "Running n=50000 -s all: SIGUSR1 stack dump shows 25 live asyncio tasks, ~10 of them aiormq _reader tasks inherited from previous scenarios"
  - "After full_pipeline, next scenario's await connection.channel() raises RuntimeError('<Connection ...> closed') — even though RobustConnection.is_closed still reports False"
  - "At n=2000 -s all the hang does not occur — bug is load-dependent and only surfaces once scenarios accumulate state"
root_cause: logic_error
resolution_type: code_fix
severity: high
tags:
  - aio-pika
  - aiormq
  - asyncio
  - task-leak
  - resource-leak
  - queue-iterator
  - robust-connection
  - benchmark
  - sigusr1
  - faulthandler
---

# Benchmark Debugging: aiormq _reader-Task-Leak + stale RobustConnection between scenarios

## Problem

`benchmarks/bench.py` consolidates nine broker scenarios into one `-s all` run. Under load (n >= 50000) the run hung indefinitely in Szenario 6 `full_pipeline`. A smaller run (n=2000) completed cleanly, so the defect was not in any single scenario — it accumulated across scenarios. After fixing the primary hang, a second regression appeared: Szenario 7 crashed with `RuntimeError: <Connection ...> closed` because the shared `aio_pika.RobustConnection` carried stale aiormq state forward while its `is_closed` flag still reported `False`.

Two independent bugs, both benchmark-only (swarmq production code uses different patterns). Same root class: aio-pika lifecycle edges that behave correctly under normal consumer patterns but leak state when repeatedly canceled or re-used under load.

## Symptoms

- **Hang, no output:** Python process alive (open TCP sockets to 5672 / 6379) but `/proc/PID/io` `wchar` frozen. `CPU` slowly declining from 11 % to ~10 %. No exception, no log.
- **Load-dependent:** `n=2000 -s all` completes cleanly; `n=50000 -s all` hangs in Szenario 6 after ~14 min.
- **Cross-scenario leak visible in stack dump:** 25 asyncio tasks alive at hang time, ~10 of them aiormq `_reader` tasks with IDs from previous scenarios (e.g. Task-464489 alongside Task-614604 of the current scenario).
- **Broker side confirms leak:** `rabbitmqctl list_queues` at hang time: `bench_queue 10 0` — ten unacked messages, zero active consumers on the RabbitMQ side. The Python side still holds them in the iterator tree.
- **Follow-on regression after first fix:** `-s all -n 2000` crashes in Szenario 7 with `RuntimeError: <Connection ...> closed` from `await conn.channel()`, immediately after `full_pipeline` completes.

## What Didn't Work

**Attempt 1 — wrap `ch.close()` in try/except to prevent "exception masking".**
Two reviewers independently flagged (cross-reviewer confidence 0.98) that a bare `await ch.close()` in a `finally` block could replace the original exception if close itself raises. This turned out to be **wrong for aio-pika specifically**: swallowing the close exception puts the RobustConnection into an internal state where the NEXT `connection.channel()` raises `RuntimeError('closed')`. Reverted.

**Attempt 2 — trust `RobustConnection.is_closed` to decide whether to reconnect.**
`is_closed` still reports `False` after the aiormq socket is dead. The recovery branch never fires, and we hand out a dead connection. The flag is not a reliable liveness signal after a bursty close-storm.

**Attempt 3 — purge the queue between scenarios to clear cross-scenario state.**
`purge()` already ran at the start of every scenario in `run_scenario()`. Purging more aggressively does not help because the leaked state is on the Python side (orphan `_reader` tasks and async iterators), not in the queue.

**Attempt 4 — assume the problem is cold-start variance.**
An earlier attempt planned per-scenario warm-up + N-repeats, which assumed the variance we observed was a cold-start symptom. A baseline of N=10 runs of `pubsub_notify` with the existing global warm-up showed CV of 4.4–7.1 %, meaning cold-start wasn't the problem at all. The "±18 % between two runs" observation was an N=2 outlier, not representative.

## Solution

### Part A — Channel leak via `async for message in queue:`

`aio-pika` queues are async-iterable, but iterating directly (not via `queue.iterator()` context manager) leaves the broker-side consumer and the aiormq `_reader` task alive when the surrounding task is cancelled. Closing the channel in a loose `await ch.close()` after the loop only runs on the happy path — cancellation skips it entirely.

**Broken pattern (old `full_pipeline.consumer`):**

```python
async def consumer() -> None:
    ch = await self.connection.channel()
    await ch.set_qos(prefetch_count=10)
    queue = await ch.declare_queue(QUEUE_NAME, durable=True)

    async for message in queue:          # consumer tag registered broker-side
        await message.ack()
        if done.is_set():
            break
    await ch.close()                     # skipped on task.cancel()
```

**Corrected pattern with `_bench_channel` helper:**

```python
@asynccontextmanager
async def _bench_channel(connection):
    ch = await connection.channel()
    try:
        yield ch
    finally:
        await ch.close()                 # runs on cancel too


async def consumer() -> None:
    async with _bench_channel(self.connection) as ch:
        await ch.set_qos(prefetch_count=10)
        queue = await ch.declare_queue(QUEUE_NAME, durable=True)
        async with queue.iterator() as it:           # CRITICAL: iterator ctx mgr
            async for message in it:
                await message.ack()
                if done.is_set():
                    break
```

Both pieces are required:
- `_bench_channel` guarantees `ch.close()` on every exit including `asyncio.CancelledError`.
- `async with queue.iterator()` issues `basic.cancel` broker-side and tears down the `_reader` task on exit, including cancellation.

### Part B — Stale RobustConnection recovery

`aio_pika.RobustConnection.is_closed` reports whether the aio-pika wrapper thinks the connection is open, not whether the underlying aiormq socket is still usable. After a bursty close-storm (many channels closing concurrently in `full_pipeline`), the aiormq side can be dead while `is_closed` is still `False`. The next `connection.channel()` call then raises `RuntimeError('closed')` synchronously with no async retry path.

**Fix: probe+reconnect before every scenario.**

```python
async def _ensure_connection(self):
    if self.connection is None or self.connection.is_closed:
        self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        return self.connection
    # Probe: open+close a throwaway channel. If aiormq is dead, this
    # raises — we close the wrapper explicitly and rebuild.
    try:
        probe_ch = await self.connection.channel()
        await probe_ch.close()
    except (RuntimeError, aio_pika.exceptions.AMQPError, OSError):
        try:
            await self.connection.close()
        except Exception:
            pass
        self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
    return self.connection
```

Called at the top of `purge()`, which itself runs at the start of every scenario. The probe is cheap (open + close one channel, ~1 ms) and catches the stale-state regression before it crashes the next scenario.

### Part C — Do NOT wrap `ch.close()` in try/except

The instinct is correct for generic Python code: a raising `finally` handler that re-raises masks the original exception. But for aio-pika specifically, the close exception carries state-clearing side effects. Swallowing it breaks subsequent operations on the same connection.

```python
# WRONG — breaks aio-pika lifecycle:
finally:
    try:
        await ch.close()
    except Exception:
        pass

# RIGHT — let it propagate. The caller's asyncio.gather(return_exceptions=True)
# already absorbs it:
finally:
    await ch.close()
```

### Part D — Diagnostic tooling

`bench.py` now ships with a `SIGUSR1` handler that dumps every thread stack + every live asyncio task (with full coroutine stacks via `Task.print_stack`) and a `SIGUSR2` handler via `faulthandler.register` for C-level dumps. Both are installed at module import. Without these, tracking down "the process is alive but doing nothing" takes hours.

```
kill -USR1 <pid>   # Python threads + asyncio tasks -> stderr
kill -USR2 <pid>   # C-level faulthandler dump
```

A simple watchdog script polls `/proc/PID/io` `wchar` every few seconds; if it is frozen for 60 s it sends `SIGUSR1` and the dump lands in the log. This is how the 10 orphan `_reader` tasks were identified.

## Why This Works

**Iterator context manager (`async with queue.iterator() as it`):**
The iterator's `__aexit__` issues `basic.cancel(consumer_tag)` broker-side and tears down the `_reader` task, regardless of whether exit was normal or via cancellation. Without the context manager, `CancelledError` propagates out of the `async for` loop before any cleanup can run.

**Channel context manager (`async with _bench_channel(conn) as ch`):**
The `finally: await ch.close()` runs on every exit, including cancellation. aio-pika channels support `async with` directly but the explicit helper also makes every channel site searchable and AST-lintable (see regression guards below).

**Probe+reconnect:**
`is_closed` is a one-way flag set by explicit close calls. It does not observe the underlying TCP socket. A cheap probe channel is the only way to detect the mismatch without waiting for the next real operation to fail.

## Prevention

1. **AST guard: no bare `async for` over queue variables.**
   A unit test in `tests/unit/test_benchmark_guard.py` parses `bench.py` and fails if any `async for` over a variable named `queue` or `*_queue` is not wrapped in `async with ...iterator()`. The guard matches the original bug shape exactly.

2. **AST guard: every `connection.channel()` inside `_bench_channel` or `_ensure_connection`.**
   Same file, second test. Any new `await conn.channel()` outside those two sites fails the build, preventing the helper from being quietly bypassed.

3. **Integration smoke test for the aggregate `-s all` path.**
   `tests/integration/test_benchmark_smoke.py` runs `bench.py -n 500 -s all` as a subprocess and asserts exit 0, no `"Connection closed"`, no `TimeoutError`, no `Traceback`, and that every scenario header appears. The 500-msg size runs in ~3 seconds so it can stay in CI.

4. **Hard timeout on completion waits.**
   `await done.wait()` is now always wrapped in `asyncio.wait_for(done.wait(), timeout=_BENCH_HANG_TIMEOUT)` (300 s). Any scenario that does not complete in 5 minutes raises with a pointer to send `SIGUSR1` for a stack dump, instead of hanging the whole suite.

5. **SIGUSR1 stack dumper ships with `bench.py`.**
   Not strictly prevention, but radically shortens the next debugging cycle if a related bug reoccurs.

## Scope note

These fixes are **only in `benchmarks/bench.py`**. Production swarmq uses:
- `queue.consume(callback)` with explicit `consumer_tag` tracking (`src/swarmq/broker/rabbitmq.py:102-112`)
- `cancel_consume()` via `queue.cancel(tag)` during graceful shutdown
- Process-scoped connections (no cross-scenario state)

These patterns already avoid both bugs. No production code changed.

## References

- **Commits:** 6f90079 (iterator + try/finally), da250b5 (handler polish), ecc5fc2 (probe+reconnect + timeouts), b7345f0 (full migration to `_bench_channel` + AST guards)
- **Files:** `benchmarks/bench.py`, `tests/unit/test_benchmark_guard.py`, `tests/integration/test_benchmark_smoke.py`
- **Related:** `docs/solutions/logic-errors/swarmq-async-semaphore-deadlock-and-error-wrapping-2026-04-15.md` — similar class (aio-pika consumer callbacks done wrong) in production code earlier
- **aio-pika docs on iterator cleanup:** `aio_pika/queue.py:__aenter__/__aexit__` — the context manager issues `basic.cancel` and nacks prefetched messages on exit
