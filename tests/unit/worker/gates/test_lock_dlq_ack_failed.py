"""Lock-Gate: Dead-Letter, dessen Kopie liegt und nur das Ack scheiterte
(Review Phase 2, 2026-10-09).

``SettleFailedAfterPublish`` heisst: die Kopie ist in der XQ. Der
DLX-Fallback (``nack(requeue=False)``) waere dann eine zweite Kopie bzw.
scheitert auf dem toten Channel, und das ERROR-Log ``lock_*_dlq_failed``
behauptete einen Fehlschlag, den es nicht gab.

Test-Liste:
1. Render-Fehler: Kopie liegt, Ack scheitert -> kein ``route_via_dlx``,
   kein ``lock_render_dlq_failed``, Unique-Freigabe, WARNING
   ``dlq_ack_failed_after_publish``.
2. Ende der Wartezeit (MR 13: ``lock_wait_max_s`` statt Deckel 10):
   dasselbe (kein ``route_via_dlx``, kein ``lock_conflict_dlq_failed``).
"""

from __future__ import annotations

import uuid
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import SettleFailedAfterPublish
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Redelivered
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload, lock_wait_exceeded_headers

pytestmark = pytest.mark.timeout(5)


class _RenderFails(Task):
    task_info = TaskInfo(name="render_fails", queues=["default"], lock="user-{user_id}", hard_timeout=60)

    @override
    async def run(self, user_id: int) -> None:
        return None


def _ctx(task_class: type[Task], **overrides: object) -> GateContext:
    base: dict[str, object] = {
        "task_id": "t1",
        "task_name": task_class.task_info.name,
        "queue": "default",
        "task_info": task_class.task_info,
        "task_class": task_class,
        "args": [],
        "kwargs": {},
        "handler_id": uuid.uuid4(),
    }
    base.update(overrides)
    return GateContext(**base)  # type: ignore[arg-type]


def _events(logs: list[dict], event: str) -> list[dict]:
    return [e for e in logs if e["event"] == event]


async def test_render_error_with_failed_ack_after_publish_is_terminal(worker, broker, swarmq_logs) -> None:
    worker._lock_manager = MagicMock()
    broker.dead_letter = AsyncMock(side_effect=SettleFailedAfterPublish("channel closed"))
    worker._gate_deps.maybe_release_unique = AsyncMock()
    msg = make_message(_default_payload())

    outcome = await worker._lock_gate.apply(msg, _ctx(_RenderFails))

    assert outcome is Redelivered
    broker.route_via_dlx.assert_not_awaited()
    assert _events(swarmq_logs, "lock_render_dlq_failed") == []
    worker._gate_deps.maybe_release_unique.assert_awaited_once()
    [warning] = _events(swarmq_logs, "dlq_ack_failed_after_publish")
    assert warning["log_level"] == "warning"


class _Locked(Task):
    task_info = TaskInfo(name="locked_cap", queues=["default"], lock="resource", hard_timeout=60)

    @override
    async def run(self) -> None:
        return None


async def test_wait_limit_with_failed_ack_after_publish_is_terminal(worker, broker, swarmq_logs) -> None:
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(return_value=None)  # Konflikt
    broker.dead_letter = AsyncMock(side_effect=SettleFailedAfterPublish("channel closed"))
    worker._gate_deps.maybe_release_unique = AsyncMock()
    msg = make_message(_default_payload())
    msg.headers = lock_wait_exceeded_headers(worker._config)

    outcome = await worker._lock_gate.apply(msg, _ctx(_Locked))

    assert outcome is Redelivered
    broker.route_via_dlx.assert_not_awaited()
    assert _events(swarmq_logs, "lock_conflict_dlq_failed") == []
    worker._gate_deps.maybe_release_unique.assert_awaited_once()
