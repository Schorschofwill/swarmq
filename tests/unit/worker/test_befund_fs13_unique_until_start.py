"""Nachweis FS-13: ``unique_until="start"`` -- endet eine Nachricht terminal,
BEVOR sie startet, bleibt das Unique-Flag bis ``unique_for`` stehen.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-13; Quelle MA-4).

``finalize.maybe_release_unique`` ist im start-Modus ein No-op ("Unit 10
already released at pickup"). Die Freigabe beim Start
(``_release_unique_on_start`` in ``worker/pipeline.py``) laeuft aber erst NACH
Cancel-, Expiry-, Lock- und Delivery-Count-Gate. Endet die Nachricht in einem
dieser Gates terminal, gibt niemand das Flag frei: Jedes ``schedule()`` mit
denselben Argumenten liefert bis ``unique_for`` still die ID der toten
Nachricht zurueck, ohne einzureihen (BZ-22 fuer den start-Modus).

Erwartet: An jedem terminalen Pfad vor dem Start wird das Flag owner-geprueft
mit der eigenen ``task_id`` freigegeben, wie im completion-Modus.

Testliste (parametrisiert):
# [x] max_age-Ablauf (Expiry-Gate, DLQ)
# [x] Pre-Start-Cancel (Cancel-Gate, Ack)
# [x] Lock-Wartezeit ueberschritten (LockWaitExceeded, DLQ)
# [x] unbekannter Task nach der Gnadenfrist (Header unique-until=start, DLQ)
# [x] Delivery-Limit erreicht (Delivery-Count-Gate, DLQ)
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload, lock_wait_exceeded_headers

pytestmark = pytest.mark.timeout(5)

_HASH = "b" * 64
_TASK_ID = "fs13-task"


class Fs13StartUnique(Task):
    task_info = TaskInfo(
        name="fs13_start_unique",
        queues=["default"],
        unique_for=3600,
        unique_until="start",
        max_age=60,
    )

    @override
    async def run(self) -> None:
        raise AssertionError("endet vor dem Start, darf nicht laufen")


class Fs13StartUniqueLocked(Task):
    task_info = TaskInfo(
        name="fs13_start_unique_locked",
        queues=["default"],
        unique_for=3600,
        unique_until="start",
        lock="fs13-mutex",
        hard_timeout=60,
    )

    @override
    async def run(self) -> None:
        raise AssertionError("Lock belegt, darf nicht laufen")


@dataclasses.dataclass(frozen=True)
class _Case:
    task_name: str
    headers: Callable[[SwarmQConfig], dict[str, Any]]
    arrange: Callable[[Worker, Any], None] = lambda _worker, _backend: None
    config: dict[str, Any] = dataclasses.field(default_factory=dict)


def _cancelled(_worker: Worker, backend: Any) -> None:
    backend.is_cancelled = AsyncMock(return_value=True)


def _lock_busy(worker: Worker, _backend: Any) -> None:
    lock_manager = MagicMock()
    lock_manager.acquire = AsyncMock(return_value=None)  # alle Slots belegt
    lock_manager.release = AsyncMock(return_value=True)
    worker._lock_manager = lock_manager


_CASES = {
    "max_age": _Case("fs13_start_unique", lambda _c: {HEADER_ENQUEUED_AT: int(time.time()) - 120}),
    "pre_start_cancel": _Case("fs13_start_unique", lambda _c: {}, arrange=_cancelled),
    "lock_wait_exceeded": _Case("fs13_start_unique_locked", lock_wait_exceeded_headers, arrange=_lock_busy),
    "unknown_task": _Case("fs13_renamed_away", lambda _c: {}, config={"unknown_task_grace_s": 0}),
    "delivery_limit": _Case("fs13_start_unique", lambda c: {"x-delivery-count": c.delivery_limit}),
}


@pytest.mark.befund("FS-13")
@pytest.mark.xfail(
    strict=True, reason="FS-13: offen - start-Modus gibt das Flag an terminalen Pfaden vor dem Start nicht frei"
)
@pytest.mark.parametrize("case", _CASES.values(), ids=_CASES.keys())
async def test_start_mode_releases_unique_flag_when_terminal_before_start(
    broker: Any, backend: Any, config: SwarmQConfig, case: _Case
) -> None:
    config = dataclasses.replace(config, **case.config)
    registry = TaskRegistry()
    registry.register(Fs13StartUnique)
    registry.register(Fs13StartUniqueLocked)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    case.arrange(worker, backend)
    backend.release_unique = AsyncMock(return_value=True)
    message = make_message(
        _default_payload(task_id=_TASK_ID, name=case.task_name, args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_UNIQUE_UNTIL: "start", **case.headers(config)},
    )

    await worker._handle_message(message)

    # Voraussetzung: die Nachricht endet terminal, ohne zu starten.
    assert broker.dead_letter.await_count + broker.ack.await_count == 1
    broker.retry_later.assert_not_awaited()
    broker.redeliver.assert_not_awaited()
    released = [c.args[:2] for c in backend.release_unique.await_args_list]
    assert released == [(_HASH, _TASK_ID)], "Unique-Flag bleibt bis unique_for stehen"
