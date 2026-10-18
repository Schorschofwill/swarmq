"""Unique-Freigabe nach einem nachgeholten Advance (F4-01, U14, KTD8, KD6).

Endet ein Workflow-Schritt mit ``unique_for`` terminal, obwohl sein
Erfolgsergebnis schon gespeichert ist, holt ``fail_workflow_step`` den
Advance nach. Gelingt er, wird ohne DLQ-Kopie bestätigt; das Unique-Flag
wird freigegeben und der Schedfire-Claim finalisiert wie nach einer
DLQ-Kopie, sonst bliebe das Flag bis ``unique_for`` stehen und verschluckte
jedes neue ``schedule()``.

Test-Liste:
- [x] Dead-Letter nach Task-Fehler (``NoRetry``): Ack statt DLQ-Kopie, kein
      ``TASK_DLQ``, Unique-Freigabe und Schedfire-Finalisierung wie nach der
      DLQ-Kopie
- [x] Gate-Pfade mit gespeichertem Erfolg (Ablauf, Lock-Render-Fehler,
      unbekannter Task, Zustelllimit): Ack ohne DLQ-Kopie, kein ``TASK_DLQ``,
      Unique-Flag freigegeben, Erfolgsergebnis nicht überschrieben
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.cancellation import build_result_dict
from swarmq.exceptions import NoRetry
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_UNIQUE_HASH, HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.serialization import encode_result
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload, as_mock

pytestmark = pytest.mark.timeout(5)

_HASH = "c" * 64
_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"
_FIRE = "cron_u14:1700000000000"
_TASK_ID = "u14-step"


class UniqueStep(Task):
    task_info = TaskInfo(name="u14_step", queues=["default"], unique_for=3600)

    @override
    async def run(self) -> None:
        raise NoRetry("second run fails terminally")


def _success() -> bytes:
    return encode_result(
        build_result_dict(
            task_id=_TASK_ID, queue="default", task_name="u14_step", is_error=False, is_cancelled=False, result=5
        )
    )


def _worker(broker: Any, backend: Any, config: Any, *, stored: bytes | None) -> tuple[Worker, list[str]]:
    registry = TaskRegistry()
    registry.register(UniqueStep)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock(advance=AsyncMock(), fail=AsyncMock())
    backend.get_result = AsyncMock(return_value=stored)
    backend.release_unique = AsyncMock(return_value=True)
    dlq: list[str] = []

    @worker._signal_dispatcher.on_signal(Signal.TASK_DLQ)
    async def _cap(**kw: Any) -> None:
        dlq.append(kw["task_id"])

    return worker, dlq


def _message(**headers: Any) -> Any:
    return make_message(
        _default_payload(task_id=_TASK_ID, name="u14_step", args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/0", **headers},
    )


def _schedfire_calls(backend: Any) -> list[str]:
    return [name for name, _args, _kwargs in backend.schedules.mock_calls]


async def test_dead_letter_of_a_succeeded_unique_step_acks_and_releases_like_a_dlq_copy(broker, backend, config):
    # Referenz: ohne gespeichertes Ergebnis die DLQ-Kopie.
    worker, dlq = _worker(broker, backend, config, stored=None)
    await worker._handle_message(_message(**{"x-swarmq-schedfire": _FIRE}))
    assert broker.dead_letter.await_count == 1
    assert dlq == [_TASK_ID]
    reference_release = [c.args[:2] for c in backend.release_unique.await_args_list]
    reference_schedfire = _schedfire_calls(backend)
    assert reference_release == [(_HASH, _TASK_ID)]
    assert "release_fire_claim" in reference_schedfire, reference_schedfire

    broker.dead_letter.reset_mock()
    broker.ack.reset_mock()
    backend.schedules.reset_mock()
    backend.store_result.reset_mock()
    worker, dlq = _worker(broker, backend, config, stored=_success())
    msg = _message(**{"x-swarmq-schedfire": _FIRE})

    await worker._handle_message(msg)

    as_mock(worker._workflow_advancer).advance.assert_awaited_once_with(_WF_ID, "/0", _TASK_ID, 5)
    as_mock(worker._workflow_advancer).fail.assert_not_awaited()
    broker.dead_letter.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)
    assert dlq == []
    assert [c.args[:2] for c in backend.release_unique.await_args_list] == reference_release
    assert _schedfire_calls(backend) == reference_schedfire
    backend.store_result.assert_not_awaited()


class ExpiringStep(Task):
    task_info = TaskInfo(name="u14_expiring", queues=["default"], unique_for=3600, max_age=60)

    @override
    async def run(self) -> None:
        raise AssertionError("abgelaufen, darf nicht laufen")


class BadLockStep(Task):
    task_info = TaskInfo(name="u14_bad_lock", queues=["default"], unique_for=3600, lock="{missing}", hard_timeout=30)

    @override
    async def run(self) -> None:
        raise AssertionError("Lock-Render-Fehler, darf nicht laufen")


_GATE_EVENTS = {
    "expiry": "task_expired",
    "lock": "lock_name_render_failed",
    "unknown": "task_not_found",
    "delivery_limit": "delivery_limit_reached_dead_letter",
}


def _gate_case(case: str, config: Any) -> tuple[str, Any, dict[str, Any]]:
    """(Taskname, Konfiguration, Zusatz-Header) des Gate-Pfads *case*."""
    if case == "expiry":
        return "u14_expiring", config, {HEADER_ENQUEUED_AT: int(time.time()) - 120}
    if case == "lock":
        return "u14_bad_lock", config, {}
    if case == "unknown":
        return "renamed_away", dataclasses.replace(config, unknown_task_grace_s=0), {}
    limit = dataclasses.replace(config, delivery_limit=5)
    return "u14_step", limit, {"x-delivery-count": 5}


@pytest.mark.parametrize("case", sorted(_GATE_EVENTS))
async def test_gate_path_of_a_succeeded_unique_step_acks_and_releases_the_flag(
    case, broker, backend, config, swarmq_logs
):
    name, cfg, extra = _gate_case(case, config)
    registry = TaskRegistry()
    for task in (UniqueStep, ExpiringStep, BadLockStep):
        registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=cfg)
    worker._workflow_advancer = MagicMock(advance=AsyncMock(), fail=AsyncMock())
    worker._lock_manager = MagicMock()  # sonst ueberspringt das Lock-Gate (vor ``start()`` None)
    backend.get_result = AsyncMock(return_value=_success())
    backend.release_unique = AsyncMock(return_value=True)
    dlq: list[str] = []

    @worker._signal_dispatcher.on_signal(Signal.TASK_DLQ)
    async def _cap(**kw: Any) -> None:
        dlq.append(kw["task_id"])

    msg = make_message(
        _default_payload(task_id=_TASK_ID, name=name, args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: _HASH, HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/0", **extra},
    )

    await worker._handle_message(msg)

    assert _GATE_EVENTS[case] in [e["event"] for e in swarmq_logs], "Voraussetzung: der Gate-Pfad greift"
    as_mock(worker._workflow_advancer).advance.assert_awaited_once_with(_WF_ID, "/0", _TASK_ID, 5)
    as_mock(worker._workflow_advancer).fail.assert_not_awaited()
    broker.dead_letter.assert_not_awaited()
    broker.route_via_dlx.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)
    assert dlq == []
    assert [c.args[:2] for c in backend.release_unique.await_args_list] == [(_HASH, _TASK_ID)]
    backend.store_result.assert_not_awaited()
