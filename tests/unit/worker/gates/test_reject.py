"""Abgelehnte Nachrichten als Worker-Kopie mit Ablaufzeit (Plan 2026-10-12-1105, MR 8, P5b, T16).

Die Ablehnungen des Workers (``undecodable``, ``middleware_chain_factory``,
``body_too_large``) gingen per ``route_via_dlx`` ohne Ablaufzeit ins
``.XQ``. ``reject_to_dead_letter`` legt stattdessen eine Kopie ueber
``broker.dead_letter`` (Ablaufzeit, Stempel) mit Grund und Fehler im
bestehenden ``x-swarmq-traceback``; ``route_via_dlx`` bleibt der Rueckfall.

Test-Liste:
- [x] Kopie mit ``x-swarmq-traceback == "<reason>: <error>"`` und den Original-Headern, danach
      ``TASK_REJECTED`` genau einmal mit unveraendertem ``reason``
- [x] ``dead_letter`` wirft ``BrokerConnectionError`` -> ``route_via_dlx`` einmal, ``TASK_REJECTED`` genau einmal
- [x] ``SettleFailedAfterPublish`` -> kein Rueckfall, ``TASK_REJECTED`` einmal
- [x] Kopie und Rueckfall scheitern -> die Ausnahme des Rueckfalls geht durch, kein Signal
- [x] Workflow-Header -> ``fail_workflow`` best effort, ``may_redeliver=False``
- [x] langer Fehlertext -> ``x-swarmq-traceback`` begrenzt (``bounded_header_text``)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BrokerConnectionError, SettleFailedAfterPublish
from swarmq.header_text import HEADER_TEXT_LIMIT_BYTES, bounded_header_text
from swarmq.signals import Signal
from swarmq.worker.gates import GateDeps
from swarmq.worker.gates.reject import reject_to_dead_letter
from tests.unit.worker._helpers import as_mock

pytestmark = [pytest.mark.timeout(5)]


def _deps(*, dead_letter: Any = None, route: Any = None) -> GateDeps:
    broker = MagicMock()
    broker.dead_letter = AsyncMock(side_effect=dead_letter)
    broker.route_via_dlx = AsyncMock(side_effect=route)
    return GateDeps(
        broker=broker,
        backend=MagicMock(),
        config=SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x"),
        signals=MagicMock(dispatch=AsyncMock()),
        registry=MagicMock(),
        rate_limiter=lambda: None,
        lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        register_lock=lambda _h, _e: None,
    )


def _delivery(headers: dict[str, Any] | None = None) -> MagicMock:
    delivery = MagicMock()
    delivery.queue = "tasks"
    delivery.headers = headers if headers is not None else {"x-custom": "keep"}
    return delivery


def _rejected_calls(deps: GateDeps) -> list[Any]:
    return [c for c in as_mock(deps.signals).dispatch.await_args_list if c.args[0] is Signal.TASK_REJECTED]


async def test_copy_carries_reason_and_error_then_signal_fires_once() -> None:
    deps = _deps()
    delivery = _delivery()

    await reject_to_dead_letter(deps, delivery, reason="undecodable", error="Expecting value")

    as_mock(deps.broker).dead_letter.assert_awaited_once_with(
        delivery, headers={"x-custom": "keep", "x-swarmq-traceback": "undecodable: Expecting value"}
    )
    as_mock(deps.broker).route_via_dlx.assert_not_awaited()
    rejected = _rejected_calls(deps)
    assert len(rejected) == 1
    assert rejected[0].kwargs == {
        "queue": "tasks",
        "reason": "undecodable",
        "task_id": None,
        "task_name": None,
        "error": "Expecting value",
    }


async def test_failed_copy_falls_back_to_dlx_with_one_signal() -> None:
    deps = _deps(dead_letter=BrokerConnectionError("channel lost"))
    delivery = _delivery()

    await reject_to_dead_letter(deps, delivery, reason="undecodable", error="Expecting value")

    as_mock(deps.broker).route_via_dlx.assert_awaited_once_with(delivery)
    rejected = _rejected_calls(deps)
    assert len(rejected) == 1
    assert rejected[0].kwargs["reason"] == "undecodable"
    assert rejected[0].kwargs["error"] == "Expecting value"


async def test_copy_placed_but_ack_failed_counts_as_placed() -> None:
    deps = _deps(dead_letter=SettleFailedAfterPublish("ack lost"))

    await reject_to_dead_letter(deps, _delivery(), reason="undecodable", error="Expecting value")

    as_mock(deps.broker).route_via_dlx.assert_not_awaited()
    assert len(_rejected_calls(deps)) == 1


async def test_copy_and_fallback_fail_raises_like_route_via_dlx() -> None:
    deps = _deps(dead_letter=BrokerConnectionError("copy lost"), route=BrokerConnectionError("nack lost"))

    with pytest.raises(BrokerConnectionError, match="nack lost"):
        await reject_to_dead_letter(deps, _delivery(), reason="undecodable", error="Expecting value")

    assert _rejected_calls(deps) == []


async def test_copy_marks_workflow_failed_best_effort_without_redelivery() -> None:
    deps = _deps()
    deps.fail_workflow = AsyncMock(side_effect=RuntimeError("valkey down"))
    delivery = _delivery()

    await reject_to_dead_letter(
        deps, delivery, reason="middleware_chain_factory", error="boom", task_id="t1", task_name="add"
    )

    as_mock(deps.fail_workflow).assert_awaited_once()
    args = as_mock(deps.fail_workflow).await_args.args
    assert args[0] is delivery
    assert (args[1].task_id, args[1].task_name, args[1].queue) == ("t1", "add", "tasks")
    assert args[2:] == ("boom", False)
    assert len(_rejected_calls(deps)) == 1


async def test_long_error_is_bounded_in_the_header() -> None:
    deps = _deps()

    await reject_to_dead_letter(deps, _delivery(), reason="undecodable", error="x" * 20_000)

    header = as_mock(deps.broker).dead_letter.await_args.kwargs["headers"]["x-swarmq-traceback"]
    assert header == bounded_header_text("undecodable: " + "x" * 20_000)
    assert len(header.encode()) <= HEADER_TEXT_LIMIT_BYTES
