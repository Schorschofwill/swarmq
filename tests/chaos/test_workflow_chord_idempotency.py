"""U8 — chord-completion exactly-once under chaos (R12).

The chord body must be enqueued exactly once even when header-member
messages are redelivered (worker crash mid-fan-in) or when the broker/backend
path is degraded with latency. The guarantee rests on three pieces built in
U3/U5 and hardened in the 2026-05-31 review follow-up:

  * deterministic step ids (``workflow_step_task_id``) — a redelivered member
    re-enters the chord SADD set under the SAME id, so the distinct-member
    count cannot be inflated past N;
  * the idempotent ``chord_complete.lua`` SADD/SCARD check;
  * the publish-then-mark ``step_tasks["/b"]`` marker as the single one-shot
    for the body enqueue (the Lua no longer holds a one-shot flag, so the body
    can never be LOST in the completion->publish crash window).

A per-run Valkey counter incremented by the body task is the probe: absent a
crash of the body-running worker itself, "body enqueued once" == "body ran
once" == counter is 1.

Both workers talk to RabbitMQ + Valkey THROUGH toxiproxy
(``chaos_stack.rabbitmq_url`` / ``chaos_stack.valkey_url``).
"""

from __future__ import annotations

import asyncio
import os
import signal
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task
from swarmq.workflow import chord, group, sig
from tests.chaos.conftest import ChaosStack, ToxiproxyClient
from tests.chaos.test_worker_crash import (
    _dlq_count,
    _ensure_fixture_registered,
    _kill_subprocess,
    _spawn_worker,
    _wait_for_worker_started,
)

pytestmark = [pytest.mark.timeout(180)]


async def _body_count(stack: ChaosStack, run_key: str) -> int:
    backend = ValkeyBackend(stack.valkey_url)
    await backend.connect()
    try:
        raw = await backend._client.get(f"chord_body:{run_key}")
        return int(raw) if raw is not None else 0
    finally:
        await backend.close()


def _chord(run_key: str, *, sleep: float):
    """A 3-member chord: header members echo 1/2/3 (sleeping ``sleep`` s),
    body sums them and bumps the per-run counter."""
    return chord(
        group(
            sig("chord_header_sleep", 1, sleep),
            sig("chord_header_sleep", 2, sleep),
            sig("chord_header_sleep", 3, sleep),
        ),
        sig("chord_body_count", run_key=run_key),
    )


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


class TestChordIdempotency:
    """One chaos_stack per class (amortised ~10s startup)."""

    async def test_header_redelivery_fires_body_exactly_once(
        self, chaos_stack: ChaosStack, _cleanup_tasks
    ) -> None:
        """SIGKILL the worker mid-fan-in so unacked header members redeliver
        to a successor. The chord body must still fire exactly once and the
        workflow must complete with the correct aggregate."""
        queue = f"chaos-chord-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)
        run_key = uuid.uuid4().hex

        proc_a = _spawn_worker(chaos_stack, queue, concurrency=4)
        proc_b: object | None = None
        try:
            _wait_for_worker_started(proc_a)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])
            async with app:
                await broker.declare_queue(queue)
                # Headers sleep 3s — wide window to kill A mid-execution.
                handle = await app.apply(_chord(run_key, sleep=3.0))

                # Let worker A pick up and start the header members.
                await asyncio.sleep(1.0)
                os.kill(proc_a.pid, signal.SIGKILL)
                assert proc_a.wait(timeout=10) == -signal.SIGKILL

                # Successor takes over the redelivered (unacked) members.
                proc_b = _spawn_worker(chaos_stack, queue, concurrency=4)
                _wait_for_worker_started(proc_b)

                # Body fires exactly once → correct sum, bounded liveness.
                result = await handle.get(timeout=60)
                assert result == 6  # 1 + 2 + 3

            assert await _body_count(chaos_stack, run_key) == 1, (
                "chord body must run exactly once despite header redelivery"
            )
            assert await _dlq_count(chaos_stack, queue) == 0
        finally:
            _kill_subprocess(proc_a)
            if proc_b is not None:
                _kill_subprocess(proc_b)

    async def test_chords_under_latency_each_fire_body_once(
        self, chaos_stack: ChaosStack, toxiproxy: ToxiproxyClient, _cleanup_tasks
    ) -> None:
        """Run several chords back-to-back while the broker AND backend paths
        carry injected latency + jitter. Each chord must complete correctly
        with its body fired exactly once and no DLQ entries — the advance/
        fan-in ordering must stay race-free under timing perturbation."""
        queue = f"chaos-chord-{uuid.uuid4().hex[:10]}"
        _ensure_fixture_registered(queue)

        await toxiproxy.add_latency("rmq", latency_ms=40, jitter_ms=30)
        await toxiproxy.add_latency("vkey", latency_ms=15, jitter_ms=15)

        proc = _spawn_worker(chaos_stack, queue, concurrency=4)
        try:
            _wait_for_worker_started(proc)

            broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
            backend = ValkeyBackend(chaos_stack.valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])
            async with app:
                await broker.declare_queue(queue)
                run_keys: list[str] = []
                for _ in range(6):
                    run_key = uuid.uuid4().hex
                    run_keys.append(run_key)
                    handle = await app.apply(_chord(run_key, sleep=0.0))
                    assert await handle.get(timeout=60) == 6

            for run_key in run_keys:
                assert await _body_count(chaos_stack, run_key) == 1, (
                    f"chord body for {run_key} did not fire exactly once"
                )
            assert await _dlq_count(chaos_stack, queue) == 0
        finally:
            _kill_subprocess(proc)
