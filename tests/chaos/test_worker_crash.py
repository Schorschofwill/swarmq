"""F1 — Worker-Prozess-Crash (U2.1).

Four chaos scenarios for a crashing worker:

1. SIGKILL with a single in-flight task — the broker redelivers,
   a successor worker picks it up, and the task eventually
   completes (at-least-once contract — see ``exec_count`` note below).
2. SIGKILL with 50 in-flight tasks — successor drains, DLQ stays empty.
3. OOM simulation — task allocates ~50 MB, external SIGKILL
   stands in for the OOM-killer (portable across environments; a
   true ``docker run --memory=20m`` doesn't compose through the
   toxiproxy-wrapped chaos stack).
4. SIGSEGV via ``ctypes.string_at(0)`` — expensive (every delivery
   crashes a worker subprocess), rebuilt in Härtung R2 with supervisor
   + orchestrator-restart semantics. Quorum queues DO set
   ``x-delivery-limit=20`` (see ``RabbitMQBroker._declare_queue_triplet``),
   so a deterministically-crashing task is terminally dead-lettered
   after the limit rather than redelivered forever. (Corrected
   2026-06-02: the previous "no x-delivery-limit" rationale was stale.)

Notes on semantics:
  * SIGKILL redelivery means ``exec_count`` tends to be >= 1, not
    == 1 (the same task may run on the killed worker and on the
    successor).
  * Both workers talk to RabbitMQ + Valkey **through toxiproxy**
    (``chaos_stack.rabbitmq_url`` / ``chaos_stack.valkey_url``) —
    never to ``localhost:5673/6380``. The chaos_stack fixture
    guarantees a fresh broker + backend per test class.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task
from tests.chaos.conftest import ChaosStack

REPO_ROOT = Path(__file__).resolve().parents[2]

# psutil is optional in the repo's test extra; guard the OOM test.
try:  # pragma: no cover — import-time probe only
    import psutil  # type: ignore[import-not-found]
    _HAS_PSUTIL = True
except ImportError:  # pragma: no cover
    _HAS_PSUTIL = False


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------


def _ensure_fixture_registered(queue: str) -> None:
    """Register the fixture tasks in this process so ``schedule()``
    accepts them. Task._registered_tasks is cleared between tests, so
    re-import via importlib.reload is necessary."""
    os.environ["CLI_TEST_QUEUE"] = queue
    mod_name = "tests.chaos._chaos_fixture_tasks"
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])
    else:
        importlib.import_module(mod_name)


def _spawn_worker(
    stack: ChaosStack,
    queue_name: str,
    *,
    concurrency: int = 4,
    env_extra: dict[str, str] | None = None,
    extra_args: list[str] | None = None,
) -> subprocess.Popen[str]:
    """Launch ``swarmq.cli worker`` as a subprocess, pointed at the
    toxiproxy-wrapped URLs from the chaos stack."""
    env = dict(os.environ)
    env["SWARMQ_BROKER_URL"] = stack.rabbitmq_url
    env["SWARMQ_BACKEND_URL"] = stack.valkey_url
    env["CLI_TEST_QUEUE"] = queue_name
    env["PYTHONUNBUFFERED"] = "1"
    if env_extra:
        env.update(env_extra)
    return subprocess.Popen(
        [
            sys.executable,
            "-m", "swarmq.cli",
            "worker",
            "--module", "tests.chaos._chaos_fixture_tasks",
            "--queues", queue_name,
            "--concurrency", str(concurrency),
            "--log-format", "json",
            *(extra_args or []),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_worker_started(proc: subprocess.Popen[str], timeout: float = 30.0) -> None:
    """Block until the worker emits ``worker_started`` on stdout.

    stdout/stderr are merged into one PIPE by ``_spawn_worker``; we accumulate
    everything read so a startup crash surfaces the worker's own output in the
    failure message instead of a bare return code (essential for diagnosing
    environment-specific failures like CI).
    """
    deadline = time.monotonic() + timeout
    seen: list[str] = []
    while time.monotonic() < deadline:
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            if proc.poll() is not None:
                tail = proc.stdout.read() if proc.stdout else ""
                output = "".join(seen) + (tail or "")
                raise RuntimeError(
                    f"worker exited before starting; rc={proc.returncode}\n"
                    f"--- worker output ---\n{output or '(no output)'}"
                )
            continue
        seen.append(line)
        if "worker_started" in line:
            return
    raise TimeoutError(
        "worker did not emit worker_started in time\n"
        f"--- worker output ---\n{''.join(seen) or '(no output)'}"
    )


def _kill_subprocess(proc: subprocess.Popen[str]) -> None:
    """Defensive teardown so assertion failures don't orphan workers."""
    if proc.poll() is None:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


# ---------------------------------------------------------------------------
# Broker / backend probes
# ---------------------------------------------------------------------------


async def _dlq_count(stack: ChaosStack, queue: str) -> int:
    """Return the message count in ``{queue}.XQ`` via the proxied broker."""
    conn = await aio_pika.connect_robust(stack.rabbitmq_url)
    try:
        ch = await conn.channel()
        decl = await ch.declare_queue(f"{queue}.XQ", passive=True)
        return decl.declaration_result.message_count
    finally:
        await conn.close()


async def _read_exec_counts(stack: ChaosStack, task_ids: list[str]) -> dict[str, int]:
    backend = ValkeyBackend(stack.valkey_url)
    await backend.connect()
    try:
        out: dict[str, int] = {}
        for tid in task_ids:
            raw = await backend._client.get(f"exec_count:{tid}")
            out[tid] = int(raw) if raw is not None else 0
        return out
    finally:
        await backend.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def _cleanup_tasks():
    """Clear the global Task registry between tests — the fixture module
    gets re-imported (via importlib.reload) per test and each reload
    re-registers subclasses."""
    yield
    Task._clear_registered_tasks()


pytestmark = [pytest.mark.timeout(120)]


# ---------------------------------------------------------------------------
# Test class — one chaos_stack per class scope (amortised startup)
# ---------------------------------------------------------------------------


class TestWorkerCrash:
    """All four tests share a single chaos_stack (class-scoped)."""

    async def test_sigkill_inflight_redelivers_no_loss(
        self, chaos_stack: ChaosStack, _cleanup_tasks
    ) -> None:
        """Single 5s-sleep task, SIGKILL the worker mid-execution, a
        second worker takes over and the task completes.

        At-least-once: ``exec_count`` may be >= 1 (typical: 2 — one on
        the killed worker, one on the successor). Exactly-once would
        require application-layer idempotency + lock-based dedup,
        which is out of scope for this contract test."""
        queue = f"chaos-crash-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)

        proc_a = _spawn_worker(chaos_stack, queue, concurrency=2)
        proc_b: subprocess.Popen[str] | None = None
        try:
            _wait_for_worker_started(proc_a)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                tid = await app.schedule("chaos_sleep", 5.0, queue=queue)

                # Let worker A pick the task up (prefetch + concurrency
                # covers it immediately).
                await asyncio.sleep(0.8)

                os.kill(proc_a.pid, signal.SIGKILL)
                rc_a = proc_a.wait(timeout=10)
                assert rc_a == -signal.SIGKILL

                proc_b = _spawn_worker(chaos_stack, queue, concurrency=2)
                _wait_for_worker_started(proc_b)

                result = await app.get_result(tid, timeout=30)
                assert result == "slept:5.0"

                proc_b.send_signal(signal.SIGTERM)
                rc_b = proc_b.wait(timeout=30)
                assert rc_b == 0

            counts = await _read_exec_counts(chaos_stack, [tid])
            # At-least-once: document — not exactly-once.
            assert counts[tid] >= 1, f"expected exec_count>=1, got {counts}"
            assert await _dlq_count(chaos_stack, queue) == 0
        finally:
            _kill_subprocess(proc_a)
            if proc_b is not None:
                _kill_subprocess(proc_b)

    async def test_50_inflight_sigkill_no_dlq_entries(
        self, chaos_stack: ChaosStack, _cleanup_tasks
    ) -> None:
        """50 in-flight 3s sleeps, SIGKILL worker A, worker B drains
        all of them, DLQ must stay empty."""
        queue = f"chaos-crash-{uuid.uuid4().hex[:10]}"
        N = 50
        _ensure_fixture_registered(queue)

        proc_a = _spawn_worker(chaos_stack, queue, concurrency=N)
        proc_b: subprocess.Popen[str] | None = None
        try:
            _wait_for_worker_started(proc_a)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            task_ids: list[str] = []
            async with app:
                for _ in range(N):
                    tid = await app.schedule("chaos_sleep", 3.0, queue=queue)
                    task_ids.append(tid)

                # Give worker A a moment to prefetch all N.
                await asyncio.sleep(1.0)

                os.kill(proc_a.pid, signal.SIGKILL)
                rc_a = proc_a.wait(timeout=10)
                assert rc_a == -signal.SIGKILL

                proc_b = _spawn_worker(chaos_stack, queue, concurrency=N)
                _wait_for_worker_started(proc_b)

                results = await app.get_results(task_ids, timeout=60)
                assert len(results) == N, (
                    f"expected {N} results, got {len(results)}"
                )
                assert all(r == "slept:3.0" for r in results)

                proc_b.send_signal(signal.SIGTERM)
                rc_b = proc_b.wait(timeout=60)
                assert rc_b == 0

            assert await _dlq_count(chaos_stack, queue) == 0
        finally:
            _kill_subprocess(proc_a)
            if proc_b is not None:
                _kill_subprocess(proc_b)

    @pytest.mark.skipif(not _HAS_PSUTIL, reason="psutil not installed")
    async def test_oom_simulation_via_external_kill(
        self, chaos_stack: ChaosStack, _cleanup_tasks
    ) -> None:
        """OOM-kill simulation.

        Real docker ``--memory=20m`` isn't portable through the
        toxiproxy-wrapped chaos stack (compose ports don't reach into
        a standalone container), so we stand in for the OOM-killer
        with an external ``SIGKILL`` once the worker's RSS has grown
        past a threshold. Covers the same invariant: the kernel can
        kill the worker at any moment, the broker redelivers, the
        successor completes the task.
        """
        queue = f"chaos-crash-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)

        proc_a = _spawn_worker(chaos_stack, queue, concurrency=1)
        proc_b: subprocess.Popen[str] | None = None
        try:
            _wait_for_worker_started(proc_a)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                # Allocate ~50MB and hold for 10s — gives us a generous
                # window to observe the growth and kill externally.
                tid = await app.schedule(
                    "chaos_memory_hog", 50, 10.0, queue=queue
                )

                # Wait for the worker to grow above a baseline.
                # Polling psutil.memory_info().rss — we accept the task
                # as "started" once we see a ≥ 20 MB jump from start.
                ps = psutil.Process(proc_a.pid)
                baseline = ps.memory_info().rss
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline:
                    try:
                        if ps.memory_info().rss - baseline > 20 * 1024 * 1024:
                            break
                    except psutil.NoSuchProcess:
                        break
                    await asyncio.sleep(0.2)

                # External SIGKILL stands in for the kernel OOM-killer.
                try:
                    ps.kill()
                except psutil.NoSuchProcess:
                    pass
                rc_a = proc_a.wait(timeout=10)
                assert rc_a == -signal.SIGKILL

                proc_b = _spawn_worker(chaos_stack, queue, concurrency=1)
                _wait_for_worker_started(proc_b)

                # Successor completes the redelivered task.
                result = await app.get_result(tid, timeout=30)
                assert result == "alloc:50"

                proc_b.send_signal(signal.SIGTERM)
                rc_b = proc_b.wait(timeout=30)
                assert rc_b == 0

            assert await _dlq_count(chaos_stack, queue) == 0
        finally:
            _kill_subprocess(proc_a)
            if proc_b is not None:
                _kill_subprocess(proc_b)

    @pytest.mark.timeout(300)
    async def test_segv_panic_redelivers_then_dlqs_after_max_retries(
        self, chaos_stack: ChaosStack, _cleanup_tasks
    ) -> None:
        """SIGSEGV via ``ctypes.string_at(0)`` kills the worker on every
        delivery; the quorum-queue ``x-delivery-limit=20`` terminally
        dead-letters the message to ``{queue}.XQ``.

        Härtung R2: rebuilt with REALISTIC operator semantics. A
        single-worker variant cannot drive this scenario — after the
        first SIGSEGV no consumer remains, so the delivery count never
        reaches 20 (the previous version was structurally broken and
        never executed in CI; B1 gap). Now:

        * a ``--processes 2`` supervisor respawns crashed workers up to
          the crash-loop limit per slot, then exits NON-ZERO
          (P0-1 contract — asserted here end-to-end);
        * the test plays the orchestrator (systemd/k8s) and restarts
          the exhausted supervisor until the broker dead-letters.
        """
        import threading

        queue = f"chaos-crash-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)

        def _spawn_supervised() -> subprocess.Popen[str]:
            return _spawn_worker(
                chaos_stack, queue, concurrency=1,
                extra_args=["--processes", "2"],
            )

        def _drain_stdout(p: subprocess.Popen[str]) -> None:
            # Keep the merged stdout PIPE drained — ~24 crash-looping
            # spawns produce enough JSON logs to fill the pipe buffer
            # and deadlock the supervisor mid-write otherwise.
            threading.Thread(
                target=lambda: p.stdout.read() if p.stdout else None,
                daemon=True,
            ).start()

        proc = _spawn_supervised()
        supervisor_runs = 1
        try:
            _wait_for_worker_started(proc)
            _drain_stdout(proc)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                await app.schedule("chaos_segv", queue=queue)

                # Each supervisor run yields ~12 deliveries (2 initial
                # workers + 2 slots x 5 crash-restarts), then exits 1.
                # Two runs exceed the broker's delivery limit of 20.
                deadline = time.monotonic() + 240.0
                while time.monotonic() < deadline:
                    if await _dlq_count(chaos_stack, queue) >= 1:
                        break
                    if proc.poll() is not None:
                        # P0-1: crash-loop exhaustion must surface as a
                        # NON-ZERO exit so the orchestrator's restart
                        # policy takes over — exactly what we simulate.
                        assert proc.returncode != 0, (
                            "supervisor exited 0 while its workers were "
                            "crash-looping — exhaustion must be non-zero"
                        )
                        assert supervisor_runs < 6, (
                            "DLQ not reached after 5 supervisor runs "
                            "(~60 deliveries) — delivery-limit routing "
                            "is broken"
                        )
                        supervisor_runs += 1
                        proc = _spawn_supervised()
                        _drain_stdout(proc)
                    await asyncio.sleep(1.0)

                assert await _dlq_count(chaos_stack, queue) >= 1, (
                    "x-delivery-limit=20 did not route the SIGSEGV "
                    f"loop to the DLQ within 240s "
                    f"(supervisor_runs={supervisor_runs})"
                )
        finally:
            _kill_subprocess(proc)
