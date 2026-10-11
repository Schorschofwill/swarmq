"""E2E tests for worker shutdown behaviour under in-flight load (U1.9).

Spawns the CLI worker as a real subprocess against RabbitMQ + Valkey and
verifies three shutdown scenarios:

1. SIGTERM with in-flight tasks — all N complete, no redelivery
   (DLQ stays empty, each task_id executed exactly once).
2. SIGKILL mid-execution (worker child and supervisor, ``crash_worker``)
   — in-flight tasks redelivered to a successor worker; every task_id
   eventually completes, no losses. Each task ran twice (interrupted on
   A, re-run on B) — at-least-once semantics is what the broker
   guarantees here.
3. Worker-shutdown while a retry is sitting in the delay queue — the
   retry lands on the successor worker B, which re-fails it and drains
   it into the DLQ.

The fixture module lives at ``tests/e2e/_shutdown_fixture_tasks.py``.
"""

from __future__ import annotations

import asyncio
import signal
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task
from tests.support.services import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    VALKEY_HOST,
    VALKEY_PORT,
    rabbitmq_amqp_url,
    valkey_db_url,
)
from tests.support.worker_subprocess import (
    crash_worker as _crash_worker,
)
from tests.support.worker_subprocess import (
    ensure_fixture_registered as _ensure_fixture_registered_base,
)
from tests.support.worker_subprocess import (
    kill_subprocess as _kill_subprocess,
)
from tests.support.worker_subprocess import (
    port_open as _port_open,
)
from tests.support.worker_subprocess import (
    spawn_worker as _spawn_worker_base,
)
from tests.support.worker_subprocess import (
    wait_for_worker_started as _wait_for_worker_started,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

# Gemeinsame Testkonfiguration (Standard: Testcontainer 5699/6399).
_RABBITMQ_HOST = RABBITMQ_HOST
_RABBITMQ_PORT = RABBITMQ_PORT
_VALKEY_HOST = VALKEY_HOST
_VALKEY_PORT = VALKEY_PORT

_BROKER_URL = rabbitmq_amqp_url()
_BACKEND_URL = valkey_db_url()


pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.skipif(
        not (_port_open(_RABBITMQ_HOST, _RABBITMQ_PORT) and _port_open(_VALKEY_HOST, _VALKEY_PORT)),
        reason="Docker services (RabbitMQ + Valkey) not reachable",
    ),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _cleanup_tasks():
    """Reset the global Task registry after the test so repeat runs of
    the fixture module (re-imported for each test) don't accumulate."""
    yield
    Task._clear_registered_tasks()


_FIXTURE_MODULE = "tests.e2e._shutdown_fixture_tasks"


def _ensure_fixture_registered(queue: str) -> None:
    """Thin wrapper preserving the original positional signature; delegates to
    ``tests.support.worker_subprocess.ensure_fixture_registered`` with the
    shutdown-suite fixture module bound."""
    _ensure_fixture_registered_base(_FIXTURE_MODULE, queue=queue)


# ---------------------------------------------------------------------------
# Subprocess worker helpers — thin wrappers around shared tests.support.worker_subprocess
# ---------------------------------------------------------------------------


def _spawn_worker(
    queue_name: str,
    *,
    concurrency: int = 4,
    env_extra: dict[str, str] | None = None,
) -> subprocess.Popen[str]:
    """Launch the shutdown-suite worker. Backward-compat shim: matches the
    original signature, hard-codes broker/backend URLs and fixture module."""
    return _spawn_worker_base(
        queue_name,
        fixture_module=_FIXTURE_MODULE,
        broker_url=_BROKER_URL,
        backend_url=_BACKEND_URL,
        concurrency=concurrency,
        env_extra=env_extra,
    )


# ---------------------------------------------------------------------------
# Valkey/RabbitMQ helpers
# ---------------------------------------------------------------------------


async def _dlq_count(queue: str) -> int:
    """Return the broker-side message count in ``{queue}.XQ``."""
    import aio_pika

    conn = await aio_pika.connect_robust(_BROKER_URL)
    try:
        ch = await conn.channel()
        decl = await ch.declare_queue(f"{queue}.XQ", passive=True)
        count = decl.declaration_result.message_count
        assert count is not None
        return count
    finally:
        await conn.close()


async def _read_exec_counts(task_ids: list[str]) -> dict[str, int]:
    """Read ``exec_count:{task_id}`` for every task_id. Returns 0 for
    any key that hasn't been incremented (i.e. never executed)."""
    backend = ValkeyBackend(_BACKEND_URL)
    await backend.connect()
    try:
        assert backend._client is not None
        out: dict[str, int] = {}
        for tid in task_ids:
            raw = await backend._client.get(f"exec_count:{tid}")
            out[tid] = int(raw) if raw is not None else 0
        return out
    finally:
        await backend.close()


async def _wait_all_started(task_ids: list[str], *, timeout: float = 10.0) -> None:
    """Block until every task has recorded its first execution."""
    deadline = time.monotonic() + timeout
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        counts = await _read_exec_counts(task_ids)
        if all(c >= 1 for c in counts.values()):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"not every task started within {timeout}s: {counts}")


# ---------------------------------------------------------------------------
# Test 1: SIGTERM with in-flight tasks → all complete gracefully
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_sigterm_with_inflight_completes_gracefully() -> None:
    """Start a worker, pump 10 × 2s-sleep tasks, SIGTERM once they're
    in flight, expect:
      * worker exits cleanly (rc == 0)
      * every result is stored (10 results retrievable)
      * every task_id ran exactly once (exec_count == 1)
      * DLQ is empty
    """
    queue = f"e2e-shutdown-{uuid.uuid4().hex[:10]}"
    N = 10

    # Ensure the client-side task registry knows about the fixture
    # tasks — schedule() validates against the registry. Importing the
    # module has the side-effect of registering.
    _ensure_fixture_registered(queue)

    proc = _spawn_worker(queue, concurrency=N)
    try:
        _wait_for_worker_started(proc)

        broker = RabbitMQBroker(_BROKER_URL)
        backend = ValkeyBackend(_BACKEND_URL)
        app = SwarmQ(broker, backend, queues=[queue])

        task_ids: list[str] = []
        async with app:
            for _ in range(N):
                tid = await app.schedule("shutdown_sleep", 2.0, queue=queue)
                task_ids.append(tid)

            # Let the worker pick them all up (prefetch=concurrency=N,
            # so within a second they should all be in-flight).
            await asyncio.sleep(0.5)

            # SIGTERM — graceful shutdown should drain the 10 in-flight
            # tasks (each ~2s remaining), well within the default
            # shutdown_timeout of 30s.
            proc.send_signal(signal.SIGTERM)
            rc = proc.wait(timeout=30)
            assert rc == 0, f"expected clean exit, got rc={rc}"

            # All results must be retrievable. We use a tight timeout
            # because the worker has already exited — results are
            # either there or they aren't.
            results = await app.get_results(task_ids, timeout=5)
            assert len(results) == N
            assert all(r == "slept:2.0" for r in results), f"unexpected results: {results}"

        # Each task_id should have exactly one execution — no
        # redelivery happened because SIGTERM gives each task a chance
        # to finish + ack.
        counts = await _read_exec_counts(task_ids)
        assert all(c == 1 for c in counts.values()), f"expected exec_count=1 for every task, got {counts}"

        # DLQ must be empty.
        assert await _dlq_count(queue) == 0
    finally:
        _kill_subprocess(proc)


# ---------------------------------------------------------------------------
# Test 2: SIGKILL mid-execution → successor redelivers, no loss / at-least-once
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_sigkill_with_inflight_redelivers_no_loss_no_dup() -> None:
    """Start worker A, schedule 10 × 3s sleeps, SIGKILL once all are running,
    spin up worker B, expect:
      * all 10 results eventually stored (no loss)
      * every task_id executed twice: started on A, which crashed mid-run,
        and re-run on B. A count of 1 would mean A finished the task after
        all -- the "crash" was then a graceful drain, not a crash.

    NOTE: SIGKILL gives the worker no chance to ack or release locks;
    the broker will redeliver every un-acked message to worker B. Our
    ``exec_count`` counter therefore shows ``2`` for each task (once on
    A, once on B). Exactly-once at the application layer would require
    idempotency + lock-based dedup, which is out of scope for this test.
    """
    queue = f"e2e-shutdown-{uuid.uuid4().hex[:10]}"
    N = 10

    _ensure_fixture_registered(queue)

    # Ausführungsschutz (Plan 2026-10-08-1541, KTD5, R5): B führt die
    # Neuzustellungen erst aus, wenn die Running-Marker des toten A abgelaufen
    # sind -- bis dahin stellt er sie zurück. Kurze TTL statt 60 s.
    guard_env = {"SWARMQ_EXECUTION_GUARD_TTL_S": "2"}
    proc_a = _spawn_worker(queue, concurrency=N, env_extra=guard_env)
    proc_b: subprocess.Popen[str] | None = None
    try:
        _wait_for_worker_started(proc_a)

        broker = RabbitMQBroker(_BROKER_URL)
        backend = ValkeyBackend(_BACKEND_URL)
        app = SwarmQ(broker, backend, queues=[queue])

        task_ids: list[str] = []
        async with app:
            for _ in range(N):
                tid = await app.schedule("shutdown_sleep", 3.0, queue=queue)
                task_ids.append(tid)

            # Let worker A pick them all up — prefetch=N = 10.
            await _wait_all_started(task_ids)

            # Hard crash — SIGKILL the worker child AND the supervisor: no
            # graceful handling, no ack, no result for in-flight tasks. A
            # supervisor-only SIGKILL would let the child drain gracefully
            # via its parent-death signal and finish the tasks. The broker
            # redelivers to whichever consumer subscribes next.
            _crash_worker(proc_a)
            # SIGKILL produces rc = -SIGKILL (-9) on POSIX.
            assert proc_a.returncode == -signal.SIGKILL, f"expected -SIGKILL, got rc={proc_a.returncode}"

            # Start worker B — it subscribes to the same queue and
            # will receive the redelivered messages.
            proc_b = _spawn_worker(queue, concurrency=N, env_extra=guard_env)
            _wait_for_worker_started(proc_b)

            # Each task sleeps 3s. After SIGKILL and restart, worker B
            # runs each to completion. Allow generous polling.
            results = await app.get_results(task_ids, timeout=30)
            assert len(results) == N, f"expected {N} results, got {len(results)}"
            assert all(r == "slept:3.0" for r in results), f"unexpected results: {results}"

            # Clean shutdown of worker B.
            proc_b.send_signal(signal.SIGTERM)
            rc_b = proc_b.wait(timeout=30)
            assert rc_b == 0

        # At-least-once with a real crash: each task started on A, A died
        # mid-run, B re-ran the redelivered message.
        counts = await _read_exec_counts(task_ids)
        assert all(c >= 2 for c in counts.values()), (
            f"every task must have been interrupted on A and re-run on B, got {counts}"
        )
    finally:
        _kill_subprocess(proc_a)
        if proc_b is not None:
            _kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Test 3: shutdown during retry-delay → retry lands on successor
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_shutdown_during_retry_delay_lands_on_successor() -> None:
    """Start worker A, schedule an always-fail task with
    ``max_retries=2`` + ``retry_base_delay=1.5``. Wait until the first
    failure has pushed the task into ``{queue}.DQ``. SIGTERM worker A
    (the delay-queue message is broker-state, independent of worker
    lifecycle). Start worker B. Expect the retry to land on B, fail
    again, exhaust, and end up in DLQ.
    """
    queue = f"e2e-shutdown-{uuid.uuid4().hex[:10]}"

    _ensure_fixture_registered(queue)

    proc_a = _spawn_worker(queue, concurrency=2)
    proc_b: subprocess.Popen[str] | None = None
    try:
        _wait_for_worker_started(proc_a)

        broker = RabbitMQBroker(_BROKER_URL)
        backend = ValkeyBackend(_BACKEND_URL)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            task_id = await app.schedule("shutdown_always_fail", queue=queue)

            # Wait for the first execution + failure — the retry gets
            # published into `{queue}.DQ` with a 1.5s TTL. 1s is
            # enough for worker A to consume + fail + publish-to-DQ.
            await asyncio.sleep(1.0)

            # SIGTERM worker A — graceful shutdown. The message in
            # `{queue}.DQ` is broker-side state and survives.
            proc_a.send_signal(signal.SIGTERM)
            rc_a = proc_a.wait(timeout=30)
            assert rc_a == 0, f"expected clean exit, got rc={rc_a}"

            # Bring up worker B. The DQ TTL will expire and the
            # message will DLX-route back to the main queue where B
            # picks it up.
            proc_b = _spawn_worker(queue, concurrency=2)
            _wait_for_worker_started(proc_b)

            # Poll the DLQ: retry_base_delay=1.5 + max_retries=2 means
            # up to 3s of additional delays + execution time. 20s is
            # generous.
            deadline = time.monotonic() + 20.0
            final_count = 0
            while time.monotonic() < deadline:
                final_count = await _dlq_count(queue)
                if final_count >= 1:
                    break
                await asyncio.sleep(0.3)
            assert final_count == 1, f"expected 1 DLQ message after retry exhaustion, got {final_count}"

            # Verify the result is the expected error — confirms the
            # retry landed on worker B and eventually exhausted.
            from swarmq.exceptions import TaskError

            with pytest.raises(TaskError, match="always fails"):
                await app.get_result(task_id, timeout=0)

            # Clean shutdown of worker B.
            proc_b.send_signal(signal.SIGTERM)
            rc_b = proc_b.wait(timeout=30)
            assert rc_b == 0
    finally:
        _kill_subprocess(proc_a)
        if proc_b is not None:
            _kill_subprocess(proc_b)
