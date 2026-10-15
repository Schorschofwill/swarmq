"""Nachweis F3-16: ``fixed``-Backoff wird nicht durch ``retry_max_delay``
gedeckelt.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-16, R3D-15).

``calculate_backoff(0, "fixed", 7200, 3600)`` lieferte bis 0.12.2 7200, obwohl
der Docstring ``max_delay`` als "Maximum delay cap" beschreibt und der Guide
sagt, ``retry_max_delay`` "bounds any computed delay". Damit war auch die
Invariante ``scheduler_dedup_ttl >= retry_max_delay`` (Schutz fuer
Cron-Retries) ausgehebelt. Behoben in 0.12.3.

Festlegung (Entscheidung 11, Praezisierung E-023): ``fixed`` liefert genau
``base_delay``, hoechstens ``retry_max_delay``, ohne Jitter, also
``min(base_delay, max_delay)``. Der E-023-Test
``test_backoff.py::test_fixed_strategy_returns_base_delay`` (``base_delay`` <
``max_delay``) bleibt mit dem Fix gruen.

Testliste:
# [x] fixed mit base_delay > max_delay liefert genau max_delay (ohne Jitter),
#     fuer jeden Versuch
# [x] fixed mit base_delay == max_delay liefert genau diesen Wert
# [x] RetryHandler mit fixed, retry_base_delay=7200, retry_max_delay=3600
#     stellt die Retry-Kopie mit delay_ms == 3_600_000 ein
"""

from __future__ import annotations

from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backoff import calculate_backoff
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.middleware import MessageContext, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload


@pytest.mark.befund("F3-16")
@pytest.mark.entscheidung("E-023")
@pytest.mark.parametrize("attempt", [0, 3, 100])
def test_fixed_backoff_is_capped_by_max_delay(attempt: int) -> None:
    delay = calculate_backoff(attempt=attempt, strategy="fixed", base_delay=7200.0, max_delay=3600.0)

    assert delay == 3600.0, f"fixed-Backoff ueberschreitet retry_max_delay: {delay} > 3600.0"


@pytest.mark.entscheidung("E-023")
def test_fixed_backoff_at_the_cap_returns_exactly_the_cap() -> None:
    assert calculate_backoff(attempt=5, strategy="fixed", base_delay=3600.0, max_delay=3600.0) == 3600.0


class _SlowFixedTask(Task):
    task_info = TaskInfo(
        name="slow-fixed-task",
        queues=["default"],
        max_retries=3,
        retry_backoff="fixed",
        retry_base_delay=7200.0,
    )

    @override
    async def run(self) -> None:
        return None


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-023")
async def test_retry_handler_caps_fixed_backoff_at_retry_max_delay() -> None:
    """Der Aufrufer in ``worker/retry.py`` reicht den Deckel durch: die
    Retry-Kopie wartet ``retry_max_delay``, nicht ``retry_base_delay``."""
    broker = MagicMock(spec=RabbitMQBroker)
    broker.retry_later = AsyncMock()
    broker.dead_letter = AsyncMock()
    backend = MagicMock(spec=ValkeyBackend)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["default"],
        retry_max_delay=3600.0,
    )
    registry = TaskRegistry()
    registry.register(_SlowFixedTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._signal_dispatcher.dispatch = AsyncMock()  # type: ignore[method-assign]  # Signal-Spy statt echter Empfaenger
    ctx = GateContext(
        task_id="abc-123",
        task_name=_SlowFixedTask.task_info.name,
        queue="default",
        task_info=_SlowFixedTask.task_info,
        task_class=_SlowFixedTask,
    )
    msg = make_message(_default_payload(name="slow-fixed-task"))
    message_dict: MessageContext = {
        "task_id": "abc-123",
        "task_name": "slow-fixed-task",
        "queue": "default",
        "args": [],
        "kwargs": {},
        "delivery_mode": "at_least_once",
        "retry_count": 0,
    }

    await worker._retry_handler.evaluate(msg, ctx, RuntimeError("boom"), "boom", message_dict, MiddlewareChain([]))

    broker.dead_letter.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    assert broker.retry_later.await_args is not None
    assert broker.retry_later.await_args.kwargs["delay_ms"] == 3_600_000
