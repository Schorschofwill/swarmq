"""Gemeinsame Scheduler-Fixtures (Plan U11) — verbatim aus
``test_scheduler_loop.py``; ``lock_manager`` mockt den LockStore-Vertrag."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.signals import SignalDispatcher
from swarmq.task import Task
from tests.unit.scheduler._helpers import _LEADER_TOKEN

# AGENTS.md §Async-Code testen rule 3: 5s per-test timeout. Narrower
# than the pyproject default (30s); fails fast on deadlock without
# burning CI time (Unit 3 / H-6).
pytestmark = [pytest.mark.timeout(5)]



@pytest.fixture
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        # Short intervals so tests finish fast
        scheduler_poll_interval=0.05,
        scheduler_heartbeat_interval=0.05,
        scheduler_lock_ttl=1.0,
    )


@pytest.fixture
def backend():
    be = MagicMock()
    be.hget_schedule_def = AsyncMock(return_value=None)
    be.hset_schedule_def = AsyncMock()
    be.hdel_schedule_def = AsyncMock()
    be.hgetall_schedule_defs = AsyncMock(return_value={})
    be.zadd_schedule = AsyncMock()
    be.zrem_schedule = AsyncMock()
    be.set_schedfire_handover_marker = AsyncMock()
    be.get_and_del_schedfire_handover_marker = AsyncMock(return_value=False)
    return be


@pytest.fixture
def broker():
    b = MagicMock()
    b.publish = AsyncMock()
    return b


@pytest.fixture
def lock_manager():
    # LockStore-Vertrag (U11): acquire liefert LockToken | None,
    # renew/release nehmen den Token.
    lm = MagicMock()
    lm.acquire = AsyncMock(return_value=_LEADER_TOKEN)
    lm.renew = AsyncMock(return_value=True)
    lm.release = AsyncMock(return_value=True)
    return lm


@pytest.fixture
def store():
    s = MagicMock()
    s.poll_due = AsyncMock(return_value=[])
    s.commit_fire = AsyncMock(return_value=True)
    s.add_schedule = AsyncMock(return_value=True)
    s.update_static_schedule = AsyncMock()
    return s


@pytest.fixture
def signal_dispatcher():
    return SignalDispatcher()


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()
