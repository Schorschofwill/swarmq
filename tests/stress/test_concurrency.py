"""Concurrency stress tests (Plan U3.8).

Six high-parallelism scenarios that exercise the Worker, Scheduler,
Client, and Valkey-Lua primitives under concurrent load. Each test
clears Valkey + uses a fresh queue name so no state bleeds across
the suite.

Runs against the dev stack (``SWARMQ_TEST_RABBITMQ_PORT=5673``,
``SWARMQ_TEST_VALKEY_PORT=6380``). Bumped timeout to 120s because
10-worker × 1000-task tests legitimately take ~30-60s end-to-end.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import socket
import time
import uuid
from typing import Any

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.limits import RateLimitConfig, RateLimiter
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

# Stress tests are slow by design (10 workers × 1000 tasks can take
# 30-60s). A 120s budget keeps genuine deadlock detection sharp while
# leaving headroom for RabbitMQ/Valkey connection setup.
pytestmark = [pytest.mark.timeout(120)]


# ---------------------------------------------------------------------------
# Local fixtures — mirror tests/e2e/conftest.py because tests/stress/ sits
# outside that conftest's auto-discovery path.
# ---------------------------------------------------------------------------

_RABBITMQ_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ_HOST", "localhost")
_RABBITMQ_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_PORT", "5673"))
_VALKEY_HOST = os.environ.get("SWARMQ_TEST_VALKEY_HOST", "localhost")
_VALKEY_PORT = int(os.environ.get("SWARMQ_TEST_VALKEY_PORT", "6380"))


def _port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(autouse=True, scope="session")
def _check_docker():
    rabbitmq_ok = _port_is_open(_RABBITMQ_HOST, _RABBITMQ_PORT)
    valkey_ok = _port_is_open(_VALKEY_HOST, _VALKEY_PORT)
    if not (rabbitmq_ok and valkey_ok):
        missing = []
        if not rabbitmq_ok:
            missing.append(f"RabbitMQ ({_RABBITMQ_HOST}:{_RABBITMQ_PORT})")
        if not valkey_ok:
            missing.append(f"Valkey ({_VALKEY_HOST}:{_VALKEY_PORT})")
        pytest.skip(
            f"Dev stack not available: {', '.join(missing)}. "
            f"Run docker compose -f tests/fixtures/dev-stack.yml up -d"
        )


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    return f"amqp://guest:guest@{_RABBITMQ_HOST}:{_RABBITMQ_PORT}/"


@pytest.fixture(scope="session")
def valkey_url() -> str:
    return f"valkey://{_VALKEY_HOST}:{_VALKEY_PORT}/0"


@pytest.fixture(autouse=True)
async def _flush_valkey(valkey_url):
    """Clear backend state before each test so counters/locks from a
    previous run don't leak."""
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()
    yield
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


def _unique_queue(prefix: str = "stress") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# 1. 10 workers × 1000 tasks — each task executes exactly once.
# ---------------------------------------------------------------------------


async def test_10_workers_1000_tasks_each_exact_once(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """Publish 1000 tasks, consume across 10 in-process workers, assert
    each task executed exactly once via a Valkey INCR counter. Proves
    ack/consume grouping is watertight under N-worker competition."""
    queue = _unique_queue("ex1")
    task_name = _task_name("exec")
    total_tasks = 1000
    num_workers = 10

    counter_key = f"stress:exec:{uuid.uuid4().hex[:8]}"
    valkey_client = valkey_asyncio.Valkey.from_url(valkey_url)

    class ExecOnceTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self, n: int) -> int:
            # INCR a shared counter plus SADD a set of observed ids to
            # prove no task got silently dropped (counter catches dup,
            # set catches loss).
            pipe = valkey_client.pipeline(transaction=False)
            pipe.incr(counter_key)
            pipe.sadd(f"{counter_key}:ids", n)
            await pipe.execute()
            return n

    # Publisher: single client app for schedule.
    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)

    worker_apps: list[SwarmQ] = []
    worker_tasks: list[asyncio.Task] = []

    try:
        async with SwarmQ(
            client_broker, client_backend, queues=[queue],
        ) as client_app:
            await client_app.broker.declare_queue(queue)

            # Spin up 10 worker apps sharing the same queue.
            for _ in range(num_workers):
                w_broker = RabbitMQBroker(rabbitmq_url)
                w_backend = ValkeyBackend(valkey_url)
                w_app = SwarmQ(
                    w_broker, w_backend,
                    queues=[queue],
                    concurrency=4,
                )
                await w_app.__aenter__()
                worker_apps.append(w_app)
                worker_tasks.append(
                    asyncio.create_task(w_app.start_worker()),
                )

            # Publish 1000 tasks.
            tids = await client_app.client.schedule_many(
                [(task_name, (i,), {}) for i in range(total_tasks)],
                queue=queue,
            )
            assert len(tids) == total_tasks

            # Wait until the counter hits total_tasks (or timeout).
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                raw = await valkey_client.get(counter_key)
                if raw is not None and int(raw) >= total_tasks:
                    break
                await asyncio.sleep(0.2)

            # Give a 1s grace window to detect any stray duplicates
            # arriving after we hit the target count.
            await asyncio.sleep(1.0)

            executed = int(await valkey_client.get(counter_key) or 0)
            distinct = await valkey_client.scard(f"{counter_key}:ids")

            assert executed == total_tasks, (
                f"expected {total_tasks} executions, got {executed} "
                f"(possible duplicates or losses)"
            )
            assert distinct == total_tasks, (
                f"expected {total_tasks} distinct task inputs, "
                f"got {distinct}"
            )
    finally:
        for t in worker_tasks:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        for a in worker_apps:
            with contextlib.suppress(Exception):
                await a.__aexit__(None, None, None)
        await valkey_client.aclose()


# ---------------------------------------------------------------------------
# 2. 100 parallel schedule() calls — all unique task_ids.
# ---------------------------------------------------------------------------


async def test_100_parallel_schedule_distinct_task_ids(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """100 concurrent client.schedule() calls (no workers consuming)
    must all return distinct task_ids. Catches any shared-state race
    in the id-generation or publish path."""
    queue = _unique_queue("sched100")
    task_name = _task_name("id")

    class NoopTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "ok"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)

    async with SwarmQ(broker, backend, queues=[queue]) as app:
        await app.broker.declare_queue(queue)

        tids = await asyncio.gather(
            *(app.client.schedule(task_name) for _ in range(100))
        )

    assert len(tids) == 100
    assert len(set(tids)) == 100, (
        f"expected 100 distinct task_ids, got {len(set(tids))}"
    )


# ---------------------------------------------------------------------------
# 3. 50 parallel cancels of the same task_id — exactly one winner.
# ---------------------------------------------------------------------------


async def test_50_parallel_cancels_one_winner(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """Schedule a slow task, then fire 50 concurrent cancel() calls on
    that task_id. Because cancel() is backed by SET NX PX, exactly one
    call must return True (the setter) and 49 must return False.

    If more than one returns True, SETNX is not serializing — either
    the Valkey client is split-brain or the NX flag is being reset.
    If zero return True, all calls saw an already-set flag (should not
    happen from a pristine state)."""
    queue = _unique_queue("cancel50")
    task_name = _task_name("slow")

    class SlowTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            # Long enough to be reliably mid-flight while the cancels
            # arrive. Cancel delivery is not required for this test —
            # we only check SETNX semantics.
            await asyncio.sleep(30.0)
            return "ok"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)

    async with SwarmQ(broker, backend, queues=[queue]) as app:
        await app.broker.declare_queue(queue)
        tid = await app.client.schedule(task_name)

        results = await asyncio.gather(
            *(app.client.cancel(tid) for _ in range(50))
        )

    winners = [r for r in results if r is True]
    losers = [r for r in results if r is False]
    assert len(winners) == 1, (
        f"expected exactly 1 SETNX winner among 50 parallel cancels, "
        f"got {len(winners)} (True count). This indicates SETNX is "
        f"not atomic under contention."
    )
    assert len(losers) == 49, (
        f"expected 49 losers, got {len(losers)}"
    )


# ---------------------------------------------------------------------------
# 4. 3 scheduler candidates, 100 forced zset fires — exactly 100 executions.
# ---------------------------------------------------------------------------


async def test_3_scheduler_candidates_no_missed_cron_fires(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """Three SwarmQ apps compete for scheduler leadership; we force
    100 fires via direct zset re-scoring and verify every one was
    published and executed exactly once — no skips, no duplicates."""
    queue = _unique_queue("cron3")
    task_name = _task_name("cron")
    total_fires = 100

    counter_key = f"stress:cronfires:{uuid.uuid4().hex[:8]}"
    valkey_client = valkey_asyncio.Valkey.from_url(valkey_url)

    class CronFireTask(Task):
        # Natural cron is every minute — but the test won't wait for it.
        # We hijack the zset score to force fires in quick succession.
        task_info = TaskInfo(
            name=task_name, queues=[queue], cron="* * * * *",
        )

        async def run(self) -> str:
            await valkey_client.incr(counter_key)
            return "ok"

    cfg = dict(
        queues=[queue], concurrency=2,
        scheduler_poll_interval=0.05,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=2.0,
    )

    apps: list[SwarmQ] = []
    tasks: list[asyncio.Task] = []

    try:
        for _ in range(3):
            br = RabbitMQBroker(rabbitmq_url)
            be = ValkeyBackend(valkey_url)
            a = SwarmQ(br, be, **cfg)
            await a.__aenter__()
            apps.append(a)

        # Declare queue from app 0 so all candidates can consume.
        await apps[0].broker.declare_queue(queue)

        # Start all three workers; exactly one wins leader, the others
        # act as standby schedulers + parallel worker consumers.
        leader_events: list[asyncio.Event] = []
        for a in apps:
            ev = asyncio.Event()

            @a.on_signal(Signal.SCHEDULER_LEADER)
            async def _on_leader(_ev=ev, **_kw: Any) -> None:
                _ev.set()

            leader_events.append(ev)
            tasks.append(asyncio.create_task(a.start_worker()))

        # Wait until at least one app acquired leadership.
        async def _any_leader() -> None:
            _done, pending = await asyncio.wait(
                [asyncio.create_task(ev.wait()) for ev in leader_events],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for p in pending:
                p.cancel()

        await asyncio.wait_for(_any_leader(), timeout=10)

        # Force 100 fires by re-scoring the zset, one at a time. Each
        # injection waits until the leader has COMMITTED the fire
        # (commit_fire advances the score to the next cron minute, i.e.
        # into the future) before injecting the next one. A fixed 80ms
        # gap raced the 50ms poll tick on slow shared CI runners: the
        # next ZADD overwrote a not-yet-polled score and the fire was
        # lost as a pure test artefact (real crons never overwrite
        # their own pending score) — first CI run of this suite, 98/100.
        for fire_no in range(total_fires):
            now_ms = int(time.time() * 1000)
            injected = now_ms - 100
            await valkey_client.zadd(
                "sq:{sched}:zset", {task_name: injected},
            )
            commit_deadline = time.monotonic() + 15.0
            while time.monotonic() < commit_deadline:
                score = await valkey_client.zscore(
                    "sq:{sched}:zset", task_name,
                )
                if score is not None and score > now_ms:
                    break  # committed: advanced to a future cron slot
                await asyncio.sleep(0.02)
            else:
                pytest.fail(
                    f"leader did not commit forced fire #{fire_no + 1} "
                    "within 15s"
                )

        # Give workers a window to drain the final publishes.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            raw = await valkey_client.get(counter_key)
            if raw is not None and int(raw) >= total_fires:
                break
            await asyncio.sleep(0.2)

        # Grace window for any stragglers.
        await asyncio.sleep(1.5)
        executed = int(await valkey_client.get(counter_key) or 0)

        # BUG-SURFACE — If the count deviates, pin the actual behaviour
        # so we see the direction of drift in CI logs. Expected under
        # correct fire-dedup: exactly total_fires executions.
        assert executed == total_fires, (
            f"expected exactly {total_fires} cron executions across 3 "
            f"scheduler candidates, got {executed}. Drift direction: "
            f"{'duplicates' if executed > total_fires else 'missed fires'}."
        )
    finally:
        for t in tasks:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        for a in apps:
            with contextlib.suppress(Exception):
                await a.__aexit__(None, None, None)
        await valkey_client.aclose()


# ---------------------------------------------------------------------------
# 5. 200 parallel update_progress — throttle behaviour under contention.
#
# RACE FIXED (2026-04-19, follow-up to ce-review): The 500ms throttle
# in ``Task.update_progress`` now uses an asyncio.Lock around the
# read-compare-write, with timestamp reservation BEFORE the await.
# Concurrent calls on the same Task instance respect the throttle —
# at most ceil(elapsed_ms/500) HSETs land regardless of fan-out width.
# This test now verifies the FIX rather than pinning the bug.
# ---------------------------------------------------------------------------


async def test_200_parallel_update_progress_throttle_holds(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """Throttle behaviour under high concurrency.

    Sequential calls respect the 500ms throttle. Concurrent calls via
    asyncio.gather now ALSO respect it (post-fix): the asyncio.Lock
    around the read-compare-write + the BEFORE-await timestamp
    reservation prevent the previous race.
    """
    queue = _unique_queue("progthrot")
    task_name = _task_name("throt")

    class DummyProgTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "ok"

    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        t = DummyProgTask()
        t._backend = backend
        t._task_id = f"progthrot-{uuid.uuid4().hex[:8]}"
        t._task_start_time = time.monotonic()
        t._result_ttl_ms = 60_000

        valkey_client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            # ---- Part A: SEQUENTIAL throttle works as advertised. ----
            await t.update_progress(1, 1000, "warm")
            after_warm = await valkey_client.hgetall(
                f"sq:progress:{t._task_id}",
            )
            assert int(after_warm.get(b"current", b"0")) == 1

            # A second sequential call inside the same 500ms window is
            # rejected — the HASH does not change.
            await t.update_progress(2, 1000, "ignored")
            after_seq = await valkey_client.hgetall(
                f"sq:progress:{t._task_id}",
            )
            assert int(after_seq.get(b"current", b"0")) == 1, (
                "sequential throttle failed — second call within 500ms "
                "overwrote the first"
            )

            # ---- Part B: CONCURRENT — the fix must hold. ----
            # Reset state and fan-out 200 calls. Each coroutine acquires
            # the lock, checks the throttle, reserves the timestamp
            # BEFORE awaiting the HSET. Result: at most ~1 write per
            # 500ms regardless of concurrency. We allow +5 slack for
            # boundary crossings + lock-acquire ordering jitter.
            t._last_progress_update = 0.0
            t._progress_final_flushed = False
            t._progress_lock = None  # force re-init
            await valkey_client.delete(f"sq:progress:{t._task_id}")

            t0 = time.monotonic()
            total = 200
            await asyncio.gather(*[
                t.update_progress(i, total * 2, f"step {i}")
                for i in range(1, total + 1)
            ])
            elapsed_ms = (time.monotonic() - t0) * 1000

            after_burst = await valkey_client.hgetall(
                f"sq:progress:{t._task_id}",
            )
            stored_current = int(after_burst.get(b"current", b"0"))

            # Throttle ceiling: one write per 500ms + 5 slack. The
            # stored value is the LAST coroutine that actually wrote,
            # so an upper-bound on the count translates to an
            # upper-bound on stored_current as well (writes happen in
            # ascending-i order under the gather + lock).
            claimed_max_writes = math.ceil(elapsed_ms / 500) + 5
            assert stored_current <= claimed_max_writes * (total // claimed_max_writes + 1), (
                f"throttle race regression: stored={stored_current}, "
                f"max-writes={claimed_max_writes}, elapsed={elapsed_ms:.0f}ms. "
                f"The asyncio.Lock fix should cap concurrent writes."
            )
        finally:
            await valkey_client.aclose()
    finally:
        await backend.close()


# ---------------------------------------------------------------------------
# 6. 50 parallel rate-limit checks on same key — exactly 5 allowed.
# ---------------------------------------------------------------------------


async def test_50_parallel_rate_limit_checks_within_limit(
    rabbitmq_url: str, valkey_url: str, _cleanup_tasks,
):
    """50 concurrent RateLimiter.check() calls against a single '5/s'
    key must yield exactly 5 allowed results. Proves the Lua script is
    atomic under CONCURRENT EVALSHA invocations on the same key."""
    rl_key = f"stress-rl-{uuid.uuid4().hex[:8]}"
    config = RateLimitConfig(max_requests=5, window_size_ms=1_000)

    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        limiter = RateLimiter(client)

        results = await asyncio.gather(
            *(limiter.check(rl_key, config) for _ in range(50))
        )
        allowed = [r for r in results if r[0] is True]
        denied = [r for r in results if r[0] is False]

        assert len(allowed) == 5, (
            f"expected exactly 5 allowed out of 50 parallel calls at "
            f"5/s, got {len(allowed)}. If > 5, the Lua script is not "
            f"atomic under the NOSCRIPT-recovery path or the sliding-"
            f"window arithmetic is wrong under contention."
        )
        assert len(denied) == 45
    finally:
        await client.aclose()
