"""E2E tests for R3.5 Unique Tasks / Deduplication.

Full-stack: client, worker, RabbitMQ, Valkey, signals. Covers the three
primary user-outcome scenarios from origin brainstorm §Success Criteria:

1. Submit-button double-click (completion-mode dedup + signal).
2. Webhook idempotency through a retry-backoff window.
3. Locking + unique combined (R3.4 × R3.5 orthogonality).
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.support.wait import wait_until

# Worker drain + Valkey setup takes ~1s; 20s ceiling flags deadlocks
# without burning CI time.
pytestmark = [pytest.mark.timeout(20)]


def _unique_queue() -> str:
    return f"e2e-unique-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@contextlib.asynccontextmanager
async def _running_worker(rabbitmq_url: str, valkey_url: str, queue: str):
    """Spawn a worker and wait for WORKER_ONLINE before yielding."""
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=5.0)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


# ---------------------------------------------------------------------------
# Scenario 1: Submit-button double-click
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_submit_button_double_click_dedup(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Rapid double-schedule with identical args within unique_for window:
    both calls return the same task_id, TASK_DUPLICATE_DETECTED fires
    exactly once, task executes exactly once.
    """
    queue = _unique_queue()
    task_name = _task_name("submit")
    executions: list[tuple[int, int]] = []

    class OrderTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            unique_for=30,  # completion mode is default
        )

        @override
        async def run(self, user_id: int, order_id: int) -> str:
            executions.append((user_id, order_id))
            return f"placed:{order_id}"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    captured_dups: list[dict[str, Any]] = []

    @client_app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
    async def _on_dup(**kwargs: Any) -> None:
        captured_dups.append(kwargs)

    async with client_app:
        await client_broker.declare_queue(queue)

        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # Double-click — second schedule arrives before first finishes.
            tid_1 = await client_app.schedule(
                task_name,
                user_id=7,
                order_id=42,
            )
            tid_2 = await client_app.schedule(
                task_name,
                user_id=7,
                order_id=42,
            )

            assert tid_1 == tid_2, "dup should return the same task_id"
            result = await client_app.get_result(tid_1, timeout=8)
            assert result == "placed:42"

        # Exactly one execution despite two schedule calls.
        assert len(executions) == 1
        # Exactly one duplicate signal.
        assert len(captured_dups) == 1
        dup_payload = captured_dups[0]
        assert dup_payload["existing_task_id"] == tid_1
        assert dup_payload["task_name"] == task_name
        assert dup_payload["source"] == "client.schedule"
        assert len(dup_payload["dedup_hash"]) == 64  # sha256 hex


# ---------------------------------------------------------------------------
# Scenario 2: Webhook idempotency — dup during retry window, then re-runnable
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_webhook_idempotency_across_retry_window(
    rabbitmq_url: str,
    valkey_url: str,
):
    """unique_until=completion holds the flag across retry delays.

    We simulate a duplicate webhook delivery by calling schedule() twice
    with identical args while the first delivery is still pending. The
    second gets the existing task_id. After completion, the flag is
    released and a fresh schedule() with the same args is accepted.
    """
    queue = _unique_queue()
    task_name = _task_name("webhook")
    executions: list[str] = []

    class WebhookTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            unique_for=60,
            unique_until="completion",
        )

        @override
        async def run(self, event_id: str) -> dict[str, str]:
            executions.append(event_id)
            # Short sleep so the parallel schedule hits while we are
            # still owner of the flag.
            await asyncio.sleep(0.3)
            return {"processed": event_id}

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_broker.declare_queue(queue)

        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # First delivery.
            tid_a = await client_app.schedule(task_name, event_id="e-1")
            # Parallel duplicate webhook delivery — must dedup.
            tid_b = await client_app.schedule(task_name, event_id="e-1")
            assert tid_a == tid_b

            result = await client_app.get_result(tid_a, timeout=8)
            assert result == {"processed": "e-1"}

            # The result is visible before the release: notify, ack and
            # the unique release in the handler's finally come after the
            # store. Wait for the release itself while the worker runs
            # (same race as TestDLQUniqueForCombo::test_happy_path).
            valkey = client_backend._client
            assert valkey is not None

            async def unique_flag_released() -> bool:
                async for key in valkey.scan_iter(match="*unique:*"):
                    if await valkey.get(key) == tid_a.encode():
                        return False
                return True

            assert await wait_until(unique_flag_released, timeout=10), "unique flag never released"

            # After completion the flag is released: fresh schedule with
            # same args gets a NEW task_id and runs the task again.
            tid_c = await client_app.schedule(task_name, event_id="e-1")
            assert tid_c != tid_a
            result_2 = await client_app.get_result(tid_c, timeout=8)
            assert result_2 == {"processed": "e-1"}

    assert len(executions) == 2, "Expected exactly one execution per completed-release cycle"


# ---------------------------------------------------------------------------
# Scenario 3: Locking + unique combined
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_locking_and_unique_combined(
    rabbitmq_url: str,
    valkey_url: str,
):
    """A task guarded by BOTH a lock (R3.4) and unique_for (R3.5):

    * Two schedule() calls with identical args within the unique window
      → one deduplicated at enqueue time.
    * The lock serialises execution for any future calls with the same
      lock key, independent of the unique flag.
    """
    queue = _unique_queue()
    task_name = _task_name("report")
    run_markers: list[str] = []

    class ReportTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            unique_for=15,
            unique_until="completion",
            lock="report:{user_id}",
            max_concurrency=1,
            hard_timeout=10,
        )

        @override
        async def run(self, user_id: int, tag: str) -> str:
            run_markers.append(f"{user_id}:{tag}")
            await asyncio.sleep(0.15)
            return f"report:{user_id}:{tag}"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    captured_dups: list[dict[str, Any]] = []

    @client_app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
    async def _on_dup(**kwargs: Any) -> None:
        captured_dups.append(kwargs)

    async with client_app:
        await client_broker.declare_queue(queue)

        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # Two identical schedules — second dedups.
            tid_1 = await client_app.schedule(task_name, user_id=5, tag="a")
            tid_2 = await client_app.schedule(task_name, user_id=5, tag="a")
            assert tid_1 == tid_2
            r1 = await client_app.get_result(tid_1, timeout=8)
            assert r1 == "report:5:a"

            # After completion the flag is released. A NEW schedule with
            # different tag but same lock key (same user_id) runs serialised
            # via the lock — unique_for doesn't block it (different args).
            tid_3 = await client_app.schedule(task_name, user_id=5, tag="b")
            assert tid_3 != tid_1
            r3 = await client_app.get_result(tid_3, timeout=8)
            assert r3 == "report:5:b"

    # Exactly one dedup signal (only the (user_id=5, tag=a) duplicate).
    assert len(captured_dups) == 1
    # Executed exactly twice — once per unique arg-set.
    assert run_markers == ["5:a", "5:b"]


# ---------------------------------------------------------------------------
# Scenario 4: TTL-expiry race — first task's flag expires while it runs,
# second task claims the flag, first's completion release must not DEL
# the second's flag (ce-review P3 #26 / plan Unit 11 edge case)
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_ttl_expiry_race_owner_check_protects_second_task(
    rabbitmq_url: str,
    valkey_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """unique_for=1 + a first task held past its TTL: the flag expires
    mid-run, and a second schedule with the SAME args claims a fresh flag
    (here with a per-call unique_for=30). When the first task completes,
    its completion release runs the Lua owner-check and does not delete
    the second task's flag: the flag still names the second task and a
    third schedule with the same args is deduplicated onto it. Once the
    second task completes, its own release clears the flag (30 s TTL, so
    it is the release, not the expiry) and a new schedule runs fresh.

    Each task waits on its own gate instead of sleeping, so the order is
    fixed; a spy on the worker backend's ``release_unique`` tells when the
    first task's release has run and what it returned.
    """
    queue = _unique_queue()
    task_name = _task_name("race")
    gates = [asyncio.Event(), asyncio.Event()]
    started = [asyncio.Event(), asyncio.Event()]
    runs: list[str] = []

    class RaceTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            unique_for=1,  # 1-second TTL
            unique_until="completion",
        )

        @override
        async def run(self, tag: str) -> str:
            index = len(runs)
            runs.append(tag)
            if index < len(gates):
                started[index].set()
                await asyncio.wait_for(gates[index].wait(), timeout=15)
            return f"{tag}-{index}"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_broker.declare_queue(queue)
        valkey = client_backend._client
        assert valkey is not None

        async def flag_owners() -> list[str]:
            owners: list[str] = []
            async for key in valkey.scan_iter(match="*unique:*"):
                value = await valkey.get(key)
                if value is not None:
                    owners.append(value.decode())
            return owners

        async with _running_worker(rabbitmq_url, valkey_url, queue) as worker_app:
            releases: dict[str, bool] = {}
            real_release = worker_app.backend.release_unique

            async def spy_release(dedup_hash: str, owner: str, *, op_timeout: float | None = None) -> bool:
                released: bool = await real_release(dedup_hash, owner, op_timeout=op_timeout)
                releases[owner] = released
                return released

            monkeypatch.setattr(worker_app.backend, "release_unique", spy_release)

            # First task runs and is held; its 1-second flag expires mid-run.
            tid_1 = await client_app.schedule(task_name, tag="same")
            await asyncio.wait_for(started[0].wait(), timeout=8)

            async def first_flag_expired() -> bool:
                return tid_1 not in await flag_owners()

            assert await wait_until(first_flag_expired, timeout=8), "the first flag never expired"

            # Same args: the expired flag lets the second task claim a fresh one.
            tid_2 = await client_app.schedule(task_name, tag="same", unique_for=30)
            assert tid_2 != tid_1, "after the expiry the same args must claim a fresh flag"
            await asyncio.wait_for(started[1].wait(), timeout=8)

            # First task completes; its release must leave the second's flag alone.
            gates[0].set()
            assert await client_app.get_result(tid_1, timeout=8) == "same-0"
            assert await wait_until(lambda: tid_1 in releases, timeout=10), "the first release never ran"
            assert releases[tid_1] is False, "the owner-check must refuse the first task's release"
            owners = await flag_owners()
            assert tid_2 in owners, "the second task's flag must survive the first release"
            assert tid_1 not in owners
            assert await client_app.schedule(task_name, tag="same") == tid_2, (
                "while the second task runs, the same args are deduplicated onto it"
            )

            # Second task completes and releases its own flag.
            gates[1].set()
            assert await client_app.get_result(tid_2, timeout=8) == "same-1"
            assert await wait_until(lambda: tid_2 in releases, timeout=10), "the second release never ran"
            assert releases[tid_2] is True, "the second task releases its own flag"

            tid_3 = await client_app.schedule(task_name, tag="same")
            assert tid_3 not in (tid_1, tid_2), "after the release the same args run fresh"
            assert await client_app.get_result(tid_3, timeout=8) == "same-2"

    assert runs == ["same", "same", "same"]
