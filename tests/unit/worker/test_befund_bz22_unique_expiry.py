"""Nachweis BZ-22: Ein Unique-Task, der ueber ``max_age`` verfaellt, gibt sein
Unique-Flag nicht frei.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-22).

``ExpiryGate`` liefert ``Terminal.never_release()`` ("Flag bleibt bewusst
stehen", ``worker/outcome.py``). Mit ``unique_for=3600`` und ``max_age=60``
blockiert der abgelaufene Task jedes erneute ``schedule()`` mit denselben
Argumenten bis zu einer Stunde: ``schedule()`` liefert still die ID des
abgelaufenen Tasks, ausgefuehrt wird nichts.

Testliste:
# [x] abgelaufene Zustellung mit Unique-Hash (completion-Modus) gibt das Flag
#     frei (owner-geprueft mit ihrer task_id)
"""

from __future__ import annotations

import time
from typing import override

import pytest

from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_UNIQUE_HASH
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_HASH = "a" * 64


class UniqueReport(Task):
    task_info = TaskInfo(name="bz22_report", queues=["default"], unique_for=3600, max_age=60)

    @override
    async def run(self) -> None:
        raise AssertionError("abgelaufen, darf nicht laufen")


@pytest.mark.befund("BZ-22")
async def test_expired_unique_task_releases_its_unique_flag(broker, backend, config) -> None:
    registry = TaskRegistry()
    registry.register(UniqueReport)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    message = make_message(
        _default_payload(task_id="bz22-task", name="bz22_report", args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_ENQUEUED_AT: int(time.time()) - 120},
    )

    await worker._handle_message(message)

    assert broker.dead_letter.await_count == 1, "Voraussetzung: abgelaufener Task geht in die DLQ"
    released = [c.args[:2] for c in backend.release_unique.await_args_list]
    assert released == [(_HASH, "bz22-task")], "Unique-Flag des abgelaufenen Tasks bleibt bis unique_for stehen"
