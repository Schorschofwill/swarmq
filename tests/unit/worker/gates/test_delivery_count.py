"""DeliveryCountGate (Plan 2026-09-03-001, KTD10): Giftnachricht in den Anwendungs-DLQ."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog

from swarmq.config import SwarmQConfig
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.delivery_count import DeliveryCountGate
from swarmq.worker.outcome import Continue, Terminal


def _gate(limit: int = 20):
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", delivery_limit=limit)
    dead_letter = AsyncMock()
    return DeliveryCountGate(deps, dead_letter=dead_letter), dead_letter


def _ctx() -> GateContext:
    return GateContext(task_id="t1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


@pytest.mark.timeout(5)
@pytest.mark.parametrize("headers", [None, {}, {"x-delivery-count": "abc"}, {"x-delivery-count": 3}])
async def test_below_limit_or_unparseable_continues(headers):
    gate, dead_letter = _gate(limit=20)
    assert await gate.apply(SimpleNamespace(headers=headers), _ctx()) is Continue
    dead_letter.assert_not_awaited()


@pytest.mark.timeout(5)
@pytest.mark.parametrize("count", [19, 20, 25])
async def test_at_penultimate_delivery_goes_to_application_dead_letter(count):
    gate, dead_letter = _gate(limit=20)
    delivery = SimpleNamespace(headers={"x-delivery-count": count})
    outcome = await gate.apply(delivery, _ctx())
    assert isinstance(outcome, Terminal) and outcome.release_unique is False
    dead_letter.assert_awaited_once()
    args = dead_letter.await_args.args
    assert args[0] is delivery and args[1].task_id == "t1"
    assert "WorkerHungError" in args[2] and f"delivered {count} times" in args[2]


@pytest.mark.timeout(5)
async def test_limit_two_dead_letters_on_first_redelivery():
    gate, _dead_letter = _gate(limit=2)
    assert await gate.apply(SimpleNamespace(headers={"x-delivery-count": 0}), _ctx()) is Continue
    assert isinstance(await gate.apply(SimpleNamespace(headers={"x-delivery-count": 1}), _ctx()), Terminal)


@pytest.mark.timeout(5)
async def test_dead_letter_path_keeps_the_app_context(app_log_context):
    """Plan 2026-10-06-0831, U10 (KTD13): der Gate-Pfad laesst Variablen
    der App stehen; den Task-Kontext stellt der Scope der Pipeline wieder her."""
    gate, _dead_letter = _gate(limit=20)

    await gate.apply(SimpleNamespace(headers={"x-delivery-count": 19}), _ctx())

    assert structlog.contextvars.get_contextvars() == app_log_context


@pytest.mark.timeout(5)
async def test_dead_letter_reports_worker_hung_error_type():
    """Plan 2026-10-06-0831, R17: ohne ausloesende Ausnahme meldet der
    Gate-Pfad den festen Typ ``WorkerHungError`` — er landet als
    ``error_type`` im Event ``task_sent_to_dlq``."""
    gate, dead_letter = _gate(limit=20)

    await gate.apply(SimpleNamespace(headers={"x-delivery-count": 19}), _ctx())

    assert dead_letter.await_args.args[3] == "WorkerHungError"


# BZ-29 (Plan 0.12.0, MR 3): ``x-acquired-count`` (RabbitMQ 4.3) zaehlt mit.


@pytest.mark.timeout(5)
@pytest.mark.parametrize(
    "headers",
    [
        {"x-acquired-count": 18},
        {"x-delivery-count": 2, "x-acquired-count": 18},
        {"x-delivery-count": 2, "x-acquired-count": "abc"},
    ],
)
async def test_acquired_count_below_limit_or_unparseable_continues(headers):
    gate, dead_letter = _gate(limit=20)
    assert await gate.apply(SimpleNamespace(headers=headers), _ctx()) is Continue
    dead_letter.assert_not_awaited()


@pytest.mark.timeout(5)
async def test_acquired_count_at_limit_reports_redelivery_limit_error():
    gate, dead_letter = _gate(limit=20)

    outcome = await gate.apply(SimpleNamespace(headers={"x-delivery-count": 1, "x-acquired-count": 25}), _ctx())

    assert isinstance(outcome, Terminal) and outcome.release_unique is False
    _delivery, _ctx_arg, message, error_type = dead_letter.await_args.args
    assert error_type == "RedeliveryLimitError"
    assert message.startswith("RedeliveryLimitError: handed back 25 times")


@pytest.mark.timeout(5)
async def test_delivery_count_at_limit_stays_worker_hung_even_with_acquired_count():
    """Der Hang-Pfad (Channel-Abbruch) zaehlt auf 4.3 beide Zaehler hoch:
    erreicht ``x-delivery-count`` die Grenze, bleibt es ``WorkerHungError``."""
    gate, dead_letter = _gate(limit=20)

    await gate.apply(SimpleNamespace(headers={"x-delivery-count": 19, "x-acquired-count": 19}), _ctx())

    assert dead_letter.await_args.args[3] == "WorkerHungError"


@pytest.mark.timeout(5)
@pytest.mark.parametrize("counters", [{"x-delivery-count": 19}, {"x-delivery-count": 2, "x-acquired-count": 19}])
async def test_fail_mark_outage_at_the_limit_dead_letters_instead_of_redelivering(worker, broker, counters):
    """Plan 0.12.0, MR 3 (Folgefehler BZ-13/BZ-29): Scheitert die
    Workflow-Fehlmarkierung voruebergehend, stellt der Dead-Letter-Pfad neu
    zu -- das Zustelllimit soll das begrenzen. Am Limit selbst darf er das
    nicht mehr: auf 4.3 zaehlt der ``nack`` nicht im ``x-delivery-count``,
    die Schleife liefe ohne Ende. Die Markierung bleibt dort best effort."""
    from unittest.mock import AsyncMock, MagicMock

    from swarmq.exceptions import BackendConnectionError
    from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
    from tests.support.messages import make_message
    from tests.unit.worker._helpers import _default_payload

    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    msg = make_message(
        _default_payload(name="add"),
        headers={HEADER_WORKFLOW_ID: "01890a5d-ac96-774b-bcce-b302099a8057", HEADER_WORKFLOW_STEP: "/1", **counters},
    )

    await worker._handle_message(msg)

    worker._workflow_advancer.fail.assert_awaited_once()
    broker.redeliver.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()


# Plan 0.12.0, MR 8 (BZ-15): das effektive Delivery-Limit der Zustellungs-Queue
# (``broker.effective_delivery_limit(delivery.queue)``) deckelt die Konfiguration.


def _gate_with_queue_limit(config_limit: int, queue_limits: dict[str, int | None]):
    gate, dead_letter = _gate(limit=config_limit)
    gate._deps.broker = SimpleNamespace(effective_delivery_limit=queue_limits.get)
    return gate, dead_letter


@pytest.mark.timeout(5)
async def test_queue_limit_below_config_dead_letters_at_queue_limit_minus_one():
    gate, dead_letter = _gate_with_queue_limit(50, {"tasks": 20})

    assert await gate.apply(SimpleNamespace(headers={"x-delivery-count": 18}, queue="tasks"), _ctx()) is Continue
    outcome = await gate.apply(SimpleNamespace(headers={"x-delivery-count": 19}, queue="tasks"), _ctx())

    assert isinstance(outcome, Terminal)
    assert "delivery limit 20" in dead_letter.await_args.args[2]


@pytest.mark.timeout(5)
async def test_queue_limit_zero_dead_letters_on_the_first_redelivery():
    """``x-delivery-limit=0`` heißt "keine Wiederzustellung" (Spike S1)."""
    gate, _dead_letter = _gate_with_queue_limit(20, {"tasks": 0})

    assert await gate.apply(SimpleNamespace(headers={"x-delivery-count": 0}, queue="tasks"), _ctx()) is Continue
    outcome = await gate.apply(SimpleNamespace(headers={"x-delivery-count": 1}, queue="tasks"), _ctx())

    assert isinstance(outcome, Terminal)


@pytest.mark.timeout(5)
async def test_queue_limit_is_looked_up_by_the_delivery_queue_not_the_body_queue():
    """Prioritätsbänder: ``ctx.queue`` ist die Queue aus dem Body (``q``), die
    Zustellung kam aber aus ``tasks.high`` mit eigenem Limit."""
    gate, _dead_letter = _gate_with_queue_limit(50, {"tasks.high": 20})

    outcome = await gate.apply(SimpleNamespace(headers={"x-delivery-count": 19}, queue="tasks.high"), _ctx())

    assert isinstance(outcome, Terminal)


@pytest.mark.timeout(5)
async def test_acquired_count_uses_the_queue_limit_too():
    gate, dead_letter = _gate_with_queue_limit(50, {"tasks": 20})

    outcome = await gate.apply(SimpleNamespace(headers={"x-acquired-count": 19}, queue="tasks"), _ctx())

    assert isinstance(outcome, Terminal)
    assert dead_letter.await_args.args[3] == "RedeliveryLimitError"


def _raising(_queue: str) -> int:
    raise RuntimeError("broker gone")


async def _async_limit(_queue: str) -> int:
    return 1


@pytest.mark.timeout(5)
@pytest.mark.parametrize(
    "broker",
    [
        pytest.param(MagicMock(), id="magicmock"),
        pytest.param(SimpleNamespace(), id="missing"),
        pytest.param(SimpleNamespace(effective_delivery_limit=_raising), id="raising"),
        pytest.param(SimpleNamespace(effective_delivery_limit=_async_limit), id="async"),
        pytest.param(SimpleNamespace(effective_delivery_limit=lambda _q: True), id="bool"),
    ],
)
async def test_unusable_queue_limit_falls_back_to_the_configuration(broker):
    """Q15: optionale Fähigkeit, bei Mock, Fehler oder Nicht-Zahl gilt die Konfiguration."""
    gate, _dead_letter = _gate(limit=20)
    gate._deps.broker = broker

    delivery = SimpleNamespace(headers={"x-delivery-count": 18}, queue="tasks")
    assert await gate.apply(delivery, _ctx()) is Continue
    delivery = SimpleNamespace(headers={"x-delivery-count": 19}, queue="tasks")
    assert isinstance(await gate.apply(delivery, _ctx()), Terminal)
