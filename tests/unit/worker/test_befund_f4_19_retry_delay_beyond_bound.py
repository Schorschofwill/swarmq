"""Nachweis F4-19: ``Retry(delay=...)`` ohne Obergrenze.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-19).

``RetryHandler.evaluate`` uebernimmt ``exc.delay`` ungekappt
(``retry_max_delay`` deckelt nur den berechneten Backoff) und rechnet
``int(delay * 1000)``. Ueber ``MAX_DEADLINE_MS`` lehnt RabbitMQ 4.3.6 die
Per-Message-``expiration`` der Delay-Kopie mit ``PRECONDITION_FAILED`` ab und
schliesst den Publish-Kanal; der Rueckfall ``redeliver_in_place`` zaehlt
nicht gegen ``max_retries`` und dreht sich bis zum Zustelllimit. Ab etwa
1,8e305 wirft schon ``int(delay * 1000)`` einen ``OverflowError`` ausserhalb
jedes ``try``.

Zusage: ``website/guides/scheduling-and-retries.md`` („``Retry`` ... counts
against ``max_retries``“, dieselbe Fundstelle wie F3-03). Festlegung
(Georg 2026-10-19, Option a): Der Konstruktor von ``Retry`` lehnt
``delay > MAX_DEADLINE_S`` mit ``ValueError`` ab wie ``inf``/``nan``
(E-060). Der ``ValueError`` ist dann ein gewoehnlicher Task-Fehler: ohne
Filter ein Backoff-Retry, der gegen ``max_retries`` zaehlt.

Die Broker-Grenze ist wie RabbitMQ modelliert: ``retry_later`` mit
``delay_ms > MAX_DEADLINE_MS`` scheitert unbestimmt.

Testliste:
# [x] Task wirft Retry(delay=elf Jahre) -> Retry-Kopie innerhalb der Grenze,
#     Zaehler +1, kein redeliver
# [x] dasselbe mit Retry(delay=1e306) (OverflowError-Pfad)
# [x] Retry(delay=MAX_DEADLINE_S) bleibt erlaubt (test_retry.py)

Behoben in 0.13.1 (``Retry.__init__`` prueft die Obergrenze); ``befund``
bleibt als Rueckverweis.
"""

from __future__ import annotations

from typing import Any, ClassVar, override

import pytest

from swarmq.config_validation import MAX_DEADLINE_MS, MAX_DEADLINE_S
from swarmq.exceptions import PublishIndeterminate, Retry
from swarmq.headers import HEADER_RETRY_COUNT
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

_ELEVEN_YEARS_S = 11 * 365 * 86_400


class _FarRetryTask(Task):
    task_info = TaskInfo(name="f419_far_retry", queues=["default"], max_retries=3)
    delay: ClassVar[float] = float(_ELEVEN_YEARS_S)

    @override
    async def run(self) -> None:
        raise Retry(delay=type(self).delay)


async def _reject_beyond_bound(delivery: Any, *, delay_ms: int, headers: dict[str, Any]) -> None:
    """Wie RabbitMQ 4.3.6: ``expiration > MAX_DEADLINE_MS`` -> PRECONDITION_FAILED,
    der Publish-Kanal schliesst, das Confirm bleibt aus."""
    del delivery, headers
    if delay_ms > MAX_DEADLINE_MS:
        msg = f"PRECONDITION_FAILED - invalid expiration '{delay_ms}': value_too_large"
        raise PublishIndeterminate(msg)


@pytest.mark.befund("F4-19")
@pytest.mark.entscheidung("E-060")
@pytest.mark.timeout(5)
@pytest.mark.parametrize("delay", [float(_ELEVEN_YEARS_S), 1e306], ids=["eleven-years", "overflow"])
async def test_retry_delay_beyond_the_bound_still_counts_against_max_retries(
    broker, backend, config, monkeypatch: pytest.MonkeyPatch, delay: float
) -> None:
    assert delay > MAX_DEADLINE_S
    monkeypatch.setattr(_FarRetryTask, "delay", delay)
    broker.retry_later.side_effect = _reject_beyond_bound
    registry = TaskRegistry()
    registry.register(_FarRetryTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)

    await worker._handle_message(make_message(_default_payload(name="f419_far_retry", args=[], kwargs={})))

    assert broker.retry_later.await_count == 1, "der Retry muss als Kopie in die Delay-Queue gehen"
    kwargs = broker.retry_later.await_args.kwargs
    assert kwargs["delay_ms"] <= MAX_DEADLINE_MS, f"Wartezeit ueber der Broker-Grenze: {kwargs['delay_ms']}"
    assert kwargs["headers"][HEADER_RETRY_COUNT] == 1, "der Retry zaehlt gegen max_retries"
    assert broker.redeliver.await_count == 0, "Rueckfall redeliver_in_place zaehlt nicht gegen max_retries"
