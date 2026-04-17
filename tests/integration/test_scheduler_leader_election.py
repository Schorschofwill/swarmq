"""Integration tests for SchedulerLoop leader-election against
real Valkey + RabbitMQ.

Spins up two SchedulerLoop instances pointing at the same Valkey lock
and verifies (a) exactly-one-leader, (b) TTL-based failover,
(c) graceful-shutdown handover.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.locking import LockManager
from swarmq.scheduler import (
    SCHEDULER_LOCK_NAME,
    ScheduleStore,
    SchedulerLoop,
)
from swarmq.signals import Signal, SignalDispatcher

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


async def _make_loop(rabbitmq_url, valkey_url, *, owner_token,
                     dispatcher, lock_ttl=1.0):
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    lock_manager = LockManager(backend._client)
    store = ScheduleStore(backend)
    config = SwarmQConfig(
        broker_url=rabbitmq_url,
        backend_url=valkey_url,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=lock_ttl,
    )
    loop = SchedulerLoop(
        backend=backend,
        broker=broker,
        config=config,
        lock_manager=lock_manager,
        store=store,
        signal_dispatcher=dispatcher,
        owner_token=owner_token,
    )
    return loop, broker, backend


class TestLeaderElectionIntegration:
    async def test_two_loops_elect_exactly_one_leader(
        self, rabbitmq_url, valkey_url,
    ):
        a_events: list[Signal] = []
        b_events: list[Signal] = []
        d_a = SignalDispatcher()
        d_b = SignalDispatcher()

        @d_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _la(**kw): a_events.append(Signal.SCHEDULER_LEADER)
        @d_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _lb(**kw): b_events.append(Signal.SCHEDULER_LEADER)

        loop_a, br_a, be_a = await _make_loop(
            rabbitmq_url, valkey_url,
            owner_token=f"A-{uuid.uuid4().hex[:6]}", dispatcher=d_a,
        )
        loop_b, br_b, be_b = await _make_loop(
            rabbitmq_url, valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}", dispatcher=d_b,
        )

        try:
            await loop_a.start()
            await asyncio.sleep(0.1)
            await loop_b.start()
            # Allow both loops to have run their election_tick.
            await asyncio.sleep(0.6)

            leader_count = int(loop_a.state == "leader") + int(
                loop_b.state == "leader"
            )
            assert leader_count == 1
            assert len(a_events) + len(b_events) == 1
        finally:
            await loop_a.stop()
            await loop_b.stop()
            await br_a.close()
            await br_b.close()
            await be_a.close()
            await be_b.close()

    async def test_leader_kill_causes_failover_signal(
        self, rabbitmq_url, valkey_url,
    ):
        """Simulate A's crash by not releasing the lock. B should
        acquire after the TTL expires and emit FAILOVER."""
        d_a = SignalDispatcher()
        d_b = SignalDispatcher()

        fo_events: list[Signal] = []
        @d_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _fo(**kw):
            fo_events.append(Signal.SCHEDULER_LEADER_FAILOVER)

        lock_ttl = 0.8  # short TTL so the test runs fast
        loop_a, br_a, be_a = await _make_loop(
            rabbitmq_url, valkey_url,
            owner_token=f"A-{uuid.uuid4().hex[:6]}", dispatcher=d_a,
            lock_ttl=lock_ttl,
        )
        loop_b, br_b, be_b = await _make_loop(
            rabbitmq_url, valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}", dispatcher=d_b,
            lock_ttl=lock_ttl,
        )

        try:
            await loop_a.start()
            # Wait until A has the lock.
            for _ in range(30):
                if loop_a.state == "leader":
                    break
                await asyncio.sleep(0.05)
            assert loop_a.state == "leader"

            # Start B as standby — it must see the lock held and set
            # prev_leader_seen so the failover heuristic kicks in.
            await loop_b.start()
            # Give B at least one election_tick where acquire returns False.
            await asyncio.sleep(0.4)
            assert loop_b._prev_leader_seen is True

            # "Crash" A: cancel its sub-coroutines without graceful stop
            # (no HANDOVER marker, no release). The lock key stays in
            # Valkey until TTL expires.
            if loop_a._election_task:
                loop_a._election_task.cancel()
            if loop_a._poll_task:
                loop_a._poll_task.cancel()

            # Wait past TTL + B's heartbeat_interval — B should acquire
            # and emit FAILOVER.
            await asyncio.sleep(lock_ttl + 0.8)
            assert loop_b.state == "leader"
            assert fo_events == [Signal.SCHEDULER_LEADER_FAILOVER]
        finally:
            # Clean shutdown for B, and best-effort cleanup of A's
            # resources (its tasks are already cancelled).
            await loop_b.stop()
            await br_a.close()
            await br_b.close()
            await be_a.close()
            await be_b.close()

    async def test_graceful_handover_emits_leader_not_failover(
        self, rabbitmq_url, valkey_url,
    ):
        """A.stop() sets the handover marker; B's next acquire sees it
        and emits LEADER, not FAILOVER."""
        d_a = SignalDispatcher()
        d_b = SignalDispatcher()

        b_leader: list[Signal] = []
        b_failover: list[Signal] = []
        @d_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _l(**kw): b_leader.append(Signal.SCHEDULER_LEADER)
        @d_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _fo(**kw): b_failover.append(Signal.SCHEDULER_LEADER_FAILOVER)

        loop_a, br_a, be_a = await _make_loop(
            rabbitmq_url, valkey_url,
            owner_token=f"A-{uuid.uuid4().hex[:6]}", dispatcher=d_a,
        )
        loop_b, br_b, be_b = await _make_loop(
            rabbitmq_url, valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}", dispatcher=d_b,
        )

        try:
            await loop_a.start()
            for _ in range(30):
                if loop_a.state == "leader":
                    break
                await asyncio.sleep(0.05)
            assert loop_a.state == "leader"

            await loop_b.start()
            # B sees the lock held at least once.
            await asyncio.sleep(0.4)
            assert loop_b._prev_leader_seen is True

            # Graceful shutdown of A → writes handover marker + releases.
            await loop_a.stop()

            # B's next tick acquires and emits LEADER (not FAILOVER).
            await asyncio.sleep(0.6)
            assert loop_b.state == "leader"
            assert b_leader == [Signal.SCHEDULER_LEADER]
            assert b_failover == []
        finally:
            await loop_b.stop()
            await br_a.close()
            await br_b.close()
            await be_a.close()
            await be_b.close()
