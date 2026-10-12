"""Integration tests for SchedulerLoop leader-election against
real Valkey + RabbitMQ.

Spins up two SchedulerLoop instances pointing at the same Valkey lock
and verifies (a) exactly-one-leader, (b) TTL-based failover,
(c) graceful-shutdown handover.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts import LeaderInfo
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import (
    SchedulerLoop,
    ScheduleStore,
)
from swarmq.signals import Signal, SignalDispatcher

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


async def _make_loop(rabbitmq_url, valkey_url, *, owner_token, dispatcher, lock_ttl=1.0, version=None):
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    lock_manager = backend.locks  # LockStore-Vertrag (U11)
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
        version=version,
    )
    return loop, broker, backend


class TestLeaderElectionIntegration:
    async def test_two_loops_elect_exactly_one_leader(
        self,
        rabbitmq_url,
        valkey_url,
    ):
        a_events: list[Signal] = []
        b_events: list[Signal] = []
        d_a = SignalDispatcher()
        d_b = SignalDispatcher()

        @d_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _la(**kw):
            a_events.append(Signal.SCHEDULER_LEADER)

        @d_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _lb(**kw):
            b_events.append(Signal.SCHEDULER_LEADER)

        loop_a, br_a, be_a = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=f"A-{uuid.uuid4().hex[:6]}",
            dispatcher=d_a,
        )
        loop_b, br_b, be_b = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}",
            dispatcher=d_b,
        )

        # Track leader/standby transitions via events.
        a_leader_evt = asyncio.Event()
        b_leader_evt = asyncio.Event()

        @d_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _ae(**kw) -> None:
            a_leader_evt.set()

        @d_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _be(**kw) -> None:
            b_leader_evt.set()

        try:
            await loop_a.start()
            await loop_b.start()
            # Wait until at least one side signals LEADER — whichever
            # wins the race is the leader; the other stays standby.
            done, _ = await asyncio.wait(
                [
                    asyncio.create_task(a_leader_evt.wait()),
                    asyncio.create_task(b_leader_evt.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=3,
            )
            assert done, "neither loop became leader within 3s"
            # Bounded grace to catch a pathological second-election.
            # One heartbeat_interval (0.2s) is enough for the loser's
            # acquire to return False; we wait a bit longer to be safe.
            # This is legitimate — no event is emitted for "standby
            # saw lock held"; we observe the absence of a second
            # SCHEDULER_LEADER emission.
            await asyncio.sleep(0.3)

            leader_count = int(loop_a.state == "leader") + int(loop_b.state == "leader")
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
        self,
        rabbitmq_url,
        valkey_url,
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
            rabbitmq_url,
            valkey_url,
            owner_token=f"A-{uuid.uuid4().hex[:6]}",
            dispatcher=d_a,
            lock_ttl=lock_ttl,
        )
        loop_b, br_b, be_b = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}",
            dispatcher=d_b,
            lock_ttl=lock_ttl,
        )

        # Event barriers for A-becomes-leader and B-failover.
        a_leader_evt = asyncio.Event()

        @d_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _ae(**kw) -> None:
            a_leader_evt.set()

        failover_evt = asyncio.Event()

        @d_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _fe(**kw) -> None:
            failover_evt.set()

        try:
            await loop_a.start()
            await asyncio.wait_for(a_leader_evt.wait(), timeout=3)
            assert loop_a.state == "leader"

            # Start B as standby — it must see the lock held and set
            # prev_leader_seen so the failover heuristic kicks in.
            await loop_b.start()
            # Give B at least one election_tick where acquire returns False.
            # No signal is emitted for "standby saw lock held"; the
            # minimum wait is one heartbeat_interval (0.2s) plus a
            # small margin. This is a legitimate wait on B's internal
            # state, not a startup-barrier.
            await asyncio.sleep(0.4)
            assert loop_b._prev_leader_seen is True

            # "Crash" A: cancel its sub-coroutines without graceful stop
            # (no HANDOVER marker, no release). The lock key stays in
            # Valkey until TTL expires.
            if loop_a._election_task:
                loop_a._election_task.cancel()
            if loop_a._poll_task:
                loop_a._poll_task.cancel()

            # Wait for B's FAILOVER signal. TTL-wait for lock expiry
            # (lock_ttl=0.8s) is unavoidable — we are waiting on
            # Valkey's real-time expiry clock, not a synchronization
            # primitive. The event wait adds a bounded upper limit.
            await asyncio.wait_for(failover_evt.wait(), timeout=lock_ttl + 2.0)
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
        self,
        rabbitmq_url,
        valkey_url,
    ):
        """A.stop() sets the handover marker; B's next acquire sees it
        and emits LEADER, not FAILOVER."""
        d_a = SignalDispatcher()
        d_b = SignalDispatcher()

        b_leader: list[Signal] = []
        b_failover: list[Signal] = []

        @d_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _l(**kw):
            b_leader.append(Signal.SCHEDULER_LEADER)

        @d_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _fo(**kw):
            b_failover.append(Signal.SCHEDULER_LEADER_FAILOVER)

        loop_a, br_a, be_a = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=f"A-{uuid.uuid4().hex[:6]}",
            dispatcher=d_a,
        )
        loop_b, br_b, be_b = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}",
            dispatcher=d_b,
        )

        # Event for A-becomes-leader and for B-acquires.
        a_leader_evt = asyncio.Event()

        @d_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _ae(**kw) -> None:
            a_leader_evt.set()

        b_leader_evt = asyncio.Event()

        @d_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _be(**kw) -> None:
            b_leader_evt.set()

        try:
            await loop_a.start()
            await asyncio.wait_for(a_leader_evt.wait(), timeout=3)
            assert loop_a.state == "leader"

            await loop_b.start()
            # B sees the lock held at least once — no event for
            # "standby saw lock held", one heartbeat wait is unavoidable.
            await asyncio.sleep(0.4)
            assert loop_b._prev_leader_seen is True

            # Graceful shutdown of A → writes handover marker + releases.
            await loop_a.stop()

            # B's next tick acquires and emits LEADER (not FAILOVER).
            await asyncio.wait_for(b_leader_evt.wait(), timeout=2)
            assert loop_b.state == "leader"
            assert b_leader == [Signal.SCHEDULER_LEADER]
            assert b_failover == []
        finally:
            await loop_b.stop()
            await br_a.close()
            await br_b.close()
            await be_a.close()
            await be_b.close()


class TestLeaderVersionIntegration:
    """Plan 2026-10-08-1541, U13 (KTD11, AE9) gegen echtes Valkey."""

    async def test_leader_writes_its_version_next_to_the_lock(
        self,
        rabbitmq_url,
        valkey_url,
    ):
        dispatcher = SignalDispatcher()
        leader_evt = asyncio.Event()

        @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        @dispatcher.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _leader(**kw) -> None:
            leader_evt.set()

        owner = f"A-{uuid.uuid4().hex[:6]}"
        loop, broker, backend = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=owner,
            dispatcher=dispatcher,
            version="0.10.0",
        )
        try:
            await backend.locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1)
            await loop.start()
            await asyncio.wait_for(leader_evt.wait(), timeout=3)
            assert await backend.schedules.leader_info() == LeaderInfo(owner=owner, version="0.10.0")
        finally:
            await loop.stop()
            await broker.close()
            await backend.close()

    async def test_standby_reports_a_leader_without_version(
        self,
        rabbitmq_url,
        valkey_url,
    ):
        """Covers AE9: ein 0.9.0-Leader hält das Lock und schreibt keine
        Version; der 0.10.0-Standby meldet ihn."""
        dispatcher = SignalDispatcher()
        outdated: list[dict[str, Any]] = []
        outdated_evt = asyncio.Event()

        @dispatcher.on_signal(Signal.SCHEDULER_LEADER_OUTDATED)
        async def _outdated(**kw) -> None:
            outdated.append(kw)
            outdated_evt.set()

        loop, broker, backend = await _make_loop(
            rabbitmq_url,
            valkey_url,
            owner_token=f"B-{uuid.uuid4().hex[:6]}",
            dispatcher=dispatcher,
            version="0.10.0",
        )
        legacy = f"legacy-{uuid.uuid4().hex[:6]}"
        try:
            await backend.locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1)
            token = await backend.locks.acquire(
                SCHEDULER_LOCK_NAME,
                owner=legacy,
                ttl_ms=10_000,
                max_concurrency=1,
            )
            assert token is not None
            await loop.start()
            await asyncio.wait_for(outdated_evt.wait(), timeout=3)
            assert loop.state == "standby"
            assert outdated == [
                {
                    "worker_id": loop.owner_token,
                    "leader": legacy,
                    "leader_version": None,
                    "outdated": True,
                }
            ]
        finally:
            await loop.stop()
            await backend.locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1)
            await broker.close()
            await backend.close()
