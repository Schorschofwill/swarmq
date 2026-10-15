"""Unique-Freigabe im start-Modus an den terminalen Pfaden (FS-13, KTD12).

``finalize.maybe_release_unique`` gibt im start-Modus nur dann nicht frei,
wenn die Zustellung den Startpunkt erreicht hat (``ctx.start_release_reached``):
dort lief die Freigabe schon (E-034, genau eine). Endet sie vorher terminal,
wird owner-geprueft freigegeben wie im completion-Modus. Der Nachweis der fuenf
Pfade vor dem Start liegt in ``test_befund_fs13_unique_until_start.py``.

Test-Liste:
# [x] start-Modus, Task laeuft und scheitert terminal (Retries erschoepft):
#     genau eine Freigabe (die beim Start)
# [x] Lock-DLX-Rueckfall im start-Modus (DLQ-Publish scheitert): Freigabe
# [x] Unknown-Task-DLX-Rueckfall im start-Modus (DLQ-Publish scheitert): Freigabe
# Rate-Limit-Requeue gibt nicht frei: bestehender E-034-Test
#   ``test_worker.py::TestWorkerUniqueRelease::test_start_mode_rate_limited_keeps_flag_for_redelivery``
"""

from __future__ import annotations

import dataclasses
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BrokerConnectionError
from swarmq.headers import HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload, lock_wait_exceeded_headers

pytestmark = pytest.mark.timeout(5)

_HASH = "c" * 64
_TASK_ID = "start-unique-task"


class _StartUniqueFails(Task):
    task_info = TaskInfo(
        name="start_unique_fails",
        queues=["default"],
        unique_for=3600,
        unique_until="start",
        max_retries=0,
    )

    @override
    async def run(self) -> None:
        raise RuntimeError("boom")


class _StartUniqueLocked(Task):
    task_info = TaskInfo(
        name="start_unique_locked",
        queues=["default"],
        unique_for=3600,
        unique_until="start",
        lock="start-unique-mutex",
        hard_timeout=60,
    )

    @override
    async def run(self) -> None:
        raise AssertionError("Lock belegt, darf nicht laufen")


def _worker(broker: Any, backend: Any, config: SwarmQConfig) -> Worker:
    registry = TaskRegistry()
    registry.register(_StartUniqueFails)
    registry.register(_StartUniqueLocked)
    backend.release_unique = AsyncMock(return_value=True)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


def _message(task_name: str, extra_headers: dict[str, Any] | None = None) -> Any:
    return make_message(
        _default_payload(task_id=_TASK_ID, name=task_name, args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_UNIQUE_UNTIL: "start", **(extra_headers or {})},
    )


def _released(backend: Any) -> list[tuple[Any, ...]]:
    return [c.args[:2] for c in backend.release_unique.await_args_list]


async def test_start_mode_task_failing_terminally_after_start_releases_exactly_once(
    broker: Any, backend: Any, config: SwarmQConfig
) -> None:
    worker = _worker(broker, backend, config)

    await worker._handle_message(_message("start_unique_fails"))

    broker.dead_letter.assert_awaited_once()  # Retries erschoepft -> DLQ
    assert _released(backend) == [(_HASH, _TASK_ID)], "nach dem Start keine zweite Freigabe (E-034)"


def _lock_busy(worker: Worker) -> None:
    lock_manager = MagicMock()
    lock_manager.acquire = AsyncMock(return_value=None)  # alle Slots belegt
    lock_manager.release = AsyncMock(return_value=True)
    worker._lock_manager = lock_manager


async def test_start_mode_lock_dlx_fallback_before_start_releases_the_flag(
    broker: Any, backend: Any, config: SwarmQConfig
) -> None:
    worker = _worker(broker, backend, config)
    _lock_busy(worker)
    broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("dlq publish failed"))

    await worker._handle_message(_message("start_unique_locked", lock_wait_exceeded_headers(config)))

    broker.route_via_dlx.assert_awaited_once()  # terminaler DLX-Rueckfall
    assert _released(backend) == [(_HASH, _TASK_ID)]


@pytest.mark.entscheidung("E-008")
async def test_start_mode_unknown_task_dlx_fallback_releases_the_flag(
    broker: Any, backend: Any, config: SwarmQConfig
) -> None:
    """E-008 sagt die Unique-Freigabe nach der Gnadenfrist zu, auch im
    start-Modus und auch, wenn nur der DLX-Rueckfall bleibt."""
    worker = _worker(broker, backend, dataclasses.replace(config, unknown_task_grace_s=0))
    broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("dlq publish failed"))

    await worker._handle_message(_message("start_unique_renamed_away"))

    broker.route_via_dlx.assert_awaited_once()  # terminaler DLX-Rueckfall
    assert _released(backend) == [(_HASH, _TASK_ID)]
