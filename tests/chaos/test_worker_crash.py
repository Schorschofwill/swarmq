"""F1 — Worker-Prozess-Crash (U2.1).

Four chaos scenarios for a crashing worker:

Crashes hit the worker child AND the supervisor (``crash_worker``): a
supervisor-only SIGKILL lets the child drain gracefully via its
parent-death signal, so no crash would be tested at all.

1. SIGKILL with a single in-flight task — the broker redelivers,
   a successor worker picks it up, and the task eventually
   completes (at-least-once contract — see ``exec_count`` note below).
2. SIGKILL with 50 in-flight tasks — successor drains, DLQ stays empty.
3. OOM simulation — task allocates ~50 MB, external SIGKILL on the
   worker child stands in for the OOM-killer; the supervisor restarts
   the slot (portable across environments; a
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
  * Execution guard (0.11.0): the killed worker's running marker blocks
    the redelivery until it expires, so the crash tests run their
    workers with ``SHORT_EXECUTION_GUARD_ENV`` (TTL 2 s instead of 60 s).
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

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task
from tests.chaos.conftest import ChaosStack
from tests.support.worker_subprocess import (
    SHORT_EXECUTION_GUARD_ENV,
    crash_worker,
    kill_subprocess,
    spawn_worker,
    wait_for_worker_children,
)

_FIXTURE_MODULE = "tests.chaos._chaos_fixture_tasks"


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------


def _ensure_fixture_registered(queue: str) -> None:
    """Register the fixture tasks in this process so ``schedule()``
    accepts them. Task._registered_tasks is cleared between tests, so
    re-import via importlib.reload is necessary."""
    os.environ["CLI_TEST_QUEUE"] = queue
    mod_name = _FIXTURE_MODULE
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
    """Launch ``swarmq.cli worker`` as a subprocess (own process group),
    pointed at the toxiproxy-wrapped URLs from the chaos stack."""
    return spawn_worker(
        queue_name,
        fixture_module=_FIXTURE_MODULE,
        broker_url=stack.rabbitmq_url,
        backend_url=stack.valkey_url,
        concurrency=concurrency,
        env_extra=env_extra,
        extra_args=extra_args,
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
    """Defensive teardown so assertion failures don't orphan workers: SIGTERM
    with grace, then SIGKILL for the whole process group (worker children
    included)."""
    kill_subprocess(proc)


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


async def _wait_all_started(stack: ChaosStack, task_ids: list[str], *, timeout: float = 15.0) -> None:
    """Block until every task has recorded its first execution (the worker
    is mid-run on all of them)."""
    deadline = time.monotonic() + timeout
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        counts = await _read_exec_counts(stack, task_ids)
        if all(c >= 1 for c in counts.values()):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"not every task started within {timeout}s: {counts}")


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

        proc_a = _spawn_worker(chaos_stack, queue, concurrency=2, env_extra=SHORT_EXECUTION_GUARD_ENV)
        proc_b: subprocess.Popen[str] | None = None
        try:
            _wait_for_worker_started(proc_a)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                tid = await app.schedule("chaos_sleep", 5.0, queue=queue)

                # Let worker A pick the task up and start it.
                await _wait_all_started(chaos_stack, [tid])

                # Hard crash: worker child AND supervisor. A supervisor-only
                # SIGKILL lets the child drain gracefully (parent-death
                # signal) and finish the task -- no crash at all.
                crash_worker(proc_a)
                assert proc_a.returncode == -signal.SIGKILL

                proc_b = _spawn_worker(chaos_stack, queue, concurrency=2, env_extra=SHORT_EXECUTION_GUARD_ENV)
                _wait_for_worker_started(proc_b)

                result = await app.get_result(tid, timeout=30)
                assert result == "slept:5.0"

                proc_b.send_signal(signal.SIGTERM)
                rc_b = proc_b.wait(timeout=30)
                assert rc_b == 0

            counts = await _read_exec_counts(chaos_stack, [tid])
            # Real crash: A died mid-run, B re-ran the redelivered task. A
            # count of 1 would mean A finished it after all (graceful drain).
            assert counts[tid] >= 2, f"task must have been interrupted and re-run, got {counts}"
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

        proc_a = _spawn_worker(chaos_stack, queue, concurrency=N, env_extra=SHORT_EXECUTION_GUARD_ENV)
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

                # Let worker A start all N.
                await _wait_all_started(chaos_stack, task_ids)

                # Hard crash: worker child AND supervisor. A supervisor-only
                # SIGKILL lets the child drain gracefully (parent-death
                # signal) and finish the task -- no crash at all.
                crash_worker(proc_a)
                assert proc_a.returncode == -signal.SIGKILL

                proc_b = _spawn_worker(chaos_stack, queue, concurrency=N, env_extra=SHORT_EXECUTION_GUARD_ENV)
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
            counts = await _read_exec_counts(chaos_stack, task_ids)
            assert all(c >= 2 for c in counts.values()), (
                f"every task must have been interrupted on A and re-run on B, got {counts}"
            )
        finally:
            _kill_subprocess(proc_a)
            if proc_b is not None:
                _kill_subprocess(proc_b)

    async def test_oom_simulation_via_external_kill(
        self, chaos_stack: ChaosStack, _cleanup_tasks
    ) -> None:
        """OOM-kill simulation.

        Real docker ``--memory=20m`` isn't portable through the
        toxiproxy-wrapped chaos stack (compose ports don't reach into
        a standalone container), so we stand in for the OOM-killer
        with an external ``SIGKILL`` once the worker child's RSS has
        grown past a threshold. The OOM-killer picks the process that
        holds the memory -- the worker child, not the supervisor. The
        supervisor restarts the slot, the broker redelivers, the new
        child completes the task.
        """
        queue = f"chaos-crash-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)

        proc = _spawn_worker(chaos_stack, queue, concurrency=1, env_extra=SHORT_EXECUTION_GUARD_ENV)
        try:
            _wait_for_worker_started(proc)
            (child,) = wait_for_worker_children(proc)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                baseline = child.memory_info().rss
                # Allocate ~50MB and hold for 10s — gives us a generous
                # window to observe the growth and kill externally.
                tid = await app.schedule(
                    "chaos_memory_hog", 50, 10.0, queue=queue
                )

                # Wait for the worker child to grow above its baseline
                # (≥ 20 MB): the task is running and holds the memory.
                deadline = time.monotonic() + 15.0
                while child.memory_info().rss - baseline <= 20 * 1024 * 1024:
                    assert time.monotonic() < deadline, "worker child never grew -- task not running"
                    await asyncio.sleep(0.2)

                # External SIGKILL on the child stands in for the kernel
                # OOM-killer.
                child.kill()
                child.wait(timeout=10)
                assert proc.poll() is None, "supervisor must survive the child's OOM kill"

                # The supervisor restarts the slot; the new child completes
                # the redelivered task.
                result = await app.get_result(tid, timeout=30)
                assert result == "alloc:50"
                counts = await _read_exec_counts(chaos_stack, [tid])
                assert counts[tid] >= 2, f"OOM-killed run must be re-run, got {counts}"
                (successor,) = wait_for_worker_children(proc)
                assert successor.pid != child.pid

                proc.send_signal(signal.SIGTERM)
                assert proc.wait(timeout=30) == 0

            assert await _dlq_count(chaos_stack, queue) == 0
        finally:
            _kill_subprocess(proc)

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

        Execution guard (0.11.0): each crash leaves a running marker that
        defers the redeliveries until it expires; after
        ``execution_guard_max_orphans`` (3) expiries the guard itself
        dead-letters the message (``WorkerHungError``), usually before the
        delivery limit. With the default TTL of 60 s that takes about
        3-4 minutes, at the edge of the 240 s window -- hence
        ``SHORT_EXECUTION_GUARD_ENV``. Either path lands in ``{queue}.XQ``.
        """
        import threading

        queue = f"chaos-crash-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)

        def _spawn_supervised() -> subprocess.Popen[str]:
            return _spawn_worker(
                chaos_stack, queue, concurrency=1,
                env_extra=SHORT_EXECUTION_GUARD_ENV,
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
