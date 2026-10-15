"""Nachweis F3-03: ``Retry(delay=<0)`` wird ungeprueft uebernommen.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-03, R3C-02).

``RetryHandler.evaluate`` rechnet ``int(exc.delay * 1000)`` und reicht eine
negative Wartezeit an ``broker.retry_later`` weiter. RabbitMQ lehnt
``expiration<0`` mit ``PRECONDITION_FAILED`` ab und schliesst den
Publish-Kanal; der Rueckfall ``redeliver_in_place`` zaehlt nicht gegen
``max_retries`` und dreht sich bis zum Zustelllimit. ``inf``/``nan`` werfen
erst im Handler (``OverflowError``/``ValueError``), die Nachricht bleibt
unbestaetigt. Ausloeser: berechnete Wartezeit wie ``reset_at - time.time()``
nach einem 429.

Festlegung (Entscheidung 6): ``Retry(delay<0)`` wird auf 0 geklemmt und
zaehlt normal gegen ``max_retries``; ``inf``/``nan`` -> ``ValueError`` beim
Erzeugen von ``Retry``. Nachweis ueber ``RetryHandler``/``retry_later``.

Die Broker-Grenze ist wie RabbitMQ modelliert: ``retry_later`` mit
negativem ``delay_ms`` scheitert.

Testliste:
# [x] Retry(delay=-5) -> retry_later mit delay_ms=0, Retry-Zaehler +1, kein
#     redeliver (zaehlt gegen max_retries)
# [x] Retry(delay=inf) / Retry(delay=nan) -> ValueError beim Erzeugen

Behoben in 0.12.3 (``Retry.__init__`` klemmt und prueft); ``befund`` bleibt
als Rueckverweis.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, cast, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import Retry
from swarmq.headers import HEADER_RETRY_COUNT
from swarmq.middleware import MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

if TYPE_CHECKING:
    from swarmq.middleware import MessageContext


class F303RateLimitedTask(Task):
    task_info = TaskInfo(name="f303_rate_limited", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise AssertionError("nicht ausgefuehrt; der Test ruft den RetryHandler direkt")


def _ctx() -> GateContext:
    info = F303RateLimitedTask.task_info
    return GateContext(
        task_id="f303-task",
        task_name=info.name,
        queue="default",
        task_info=info,
        task_class=F303RateLimitedTask,
    )


async def _reject_negative_expiration(delivery: Any, *, delay_ms: int, headers: dict[str, Any]) -> None:
    """Wie RabbitMQ: ``expiration < 0`` -> PRECONDITION_FAILED."""
    del delivery, headers
    if delay_ms < 0:
        msg = f"PRECONDITION_FAILED - invalid expiration '{delay_ms}'"
        raise RuntimeError(msg)


@pytest.mark.befund("F3-03")
@pytest.mark.entscheidung("E-060")
@pytest.mark.timeout(5)
async def test_negative_retry_delay_is_clamped_to_zero_and_counts(
    broker: MagicMock, backend: MagicMock, config: SwarmQConfig
) -> None:
    retry_later = AsyncMock(side_effect=_reject_negative_expiration)
    broker.retry_later = retry_later
    redeliver = AsyncMock()
    broker.redeliver = redeliver
    registry = TaskRegistry()
    registry.register(F303RateLimitedTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    message = make_message(_default_payload(task_id="f303-task", name="f303_rate_limited", args=[], kwargs={}))

    await worker._retry_handler.evaluate(
        message,
        _ctx(),
        Retry(delay=-5.0),
        "rate limited",
        cast("MessageContext", {"retry_count": 0}),
        MiddlewareChain([]),
    )

    retry_later.assert_awaited_once()
    assert retry_later.await_args is not None
    kwargs = retry_later.await_args.kwargs
    assert kwargs["delay_ms"] == 0, f"negative Wartezeit nicht geklemmt: delay_ms={kwargs['delay_ms']}"
    assert kwargs["headers"][HEADER_RETRY_COUNT] == 1, "der geklemmte Retry zaehlt gegen max_retries"
    redeliver.assert_not_awaited()


@pytest.mark.befund("F3-03")
@pytest.mark.entscheidung("E-060")
@pytest.mark.parametrize("delay", [math.inf, math.nan], ids=["inf", "nan"])
def test_non_finite_retry_delay_is_rejected(delay: float) -> None:
    with pytest.raises(ValueError, match="delay"):
        Retry(delay=delay)
