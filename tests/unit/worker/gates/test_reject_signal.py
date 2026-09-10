"""``TASK_REJECTED`` am DLX-Fallback (U6, Plan 2026-09-08-001, R12/R13).

Der Weg über ``route_via_dlx`` war stumm: eine Nachricht landete im ``.XQ``,
ohne dass ein Signal feuerte oder ein Zähler stieg. Ein Betreiber sah die
Nachricht nur, wenn er den Gauge ``swarmq_dlq_messages`` beobachtete -- und
selbst dann nicht, warum sie dort liegt. ``TASK_DLQ`` konnte den Fall nicht
mit abdecken: es verspricht Fehlerergebnis und Task-Kennung, und auf dem
Reject-Weg fehlen beide oft (KTD6).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.signals import Signal
from swarmq.worker.gates import GateDeps, route_via_dlx

pytestmark = [pytest.mark.timeout(5)]


def _deps(*, route_fails: bool = False) -> GateDeps:
    broker = MagicMock()
    broker.route_via_dlx = AsyncMock(
        side_effect=RuntimeError("broker gone") if route_fails else None
    )
    broker.dead_letter = AsyncMock()
    return GateDeps(
        broker=broker, backend=MagicMock(),
        config=SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x"),
        signals=MagicMock(dispatch=AsyncMock()), registry=MagicMock(),
        rate_limiter=lambda: None, lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(), maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        claim_schedfire=lambda _m, _t: None, register_lock=lambda _h, _e: None,
    )


def _delivery(queue: str = "tasks.high") -> MagicMock:
    delivery = MagicMock()
    delivery.queue = queue
    delivery.headers = {}
    return delivery


async def test_signal_carries_queue_reason_and_optional_identity():
    """R12: ``task_id``/``task_name`` sind oft unbekannt (der Body war zu
    groß oder nicht dekodierbar) -- dann stehen sie als ``None`` da, statt
    das Signal ganz ausfallen zu lassen."""
    deps = _deps()
    await route_via_dlx(deps, _delivery(), reason="body_too_large", error="17 MiB > 16 MiB")

    deps.signals.dispatch.assert_awaited_once()
    args, kwargs = deps.signals.dispatch.await_args
    assert args[0] is Signal.TASK_REJECTED
    assert kwargs == {
        "queue": "tasks.high",
        "reason": "body_too_large",
        "task_id": None,
        "task_name": None,
        "error": "17 MiB > 16 MiB",
    }


async def test_identity_is_passed_through_when_known():
    deps = _deps()
    await route_via_dlx(
        deps, _delivery(), reason="unregistered_task",
        task_id="t1", task_name="nope", error="task not found",
    )
    _args, kwargs = deps.signals.dispatch.await_args
    assert kwargs["task_id"] == "t1"
    assert kwargs["task_name"] == "nope"


async def test_no_signal_when_the_broker_verb_fails():
    """Das Signal folgt dem erfolgreichen Verb: sonst meldete es eine
    Ablehnung, die nie stattfand, und der Zähler zählte eine Nachricht, die
    noch in der Queue liegt."""
    deps = _deps(route_fails=True)
    with pytest.raises(RuntimeError, match="broker gone"):
        await route_via_dlx(deps, _delivery(), reason="undecodable")
    deps.signals.dispatch.assert_not_awaited()


async def test_exactly_one_broker_verb_per_rejection():
    """Ack-Invariante der Pipeline: das Signal ist kein weiteres Verb."""
    deps = _deps()
    await route_via_dlx(deps, _delivery(), reason="undecodable")
    assert deps.broker.route_via_dlx.await_count == 1
    deps.broker.dead_letter.assert_not_awaited()


async def test_fallback_broker_without_route_via_dlx_still_signals():
    """Ein Broker ohne ``route_via_dlx`` veröffentlicht an sein
    Dead-Letter-Ziel -- die Ablehnung ist dieselbe und bleibt sichtbar."""
    deps = _deps()
    deps.broker.route_via_dlx = None
    await route_via_dlx(deps, _delivery(), reason="dlq_publish_failed")
    deps.broker.dead_letter.assert_awaited_once()
    deps.signals.dispatch.assert_awaited_once()
