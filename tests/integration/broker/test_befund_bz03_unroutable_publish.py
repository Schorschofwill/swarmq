"""Nachweis BZ-03: Publish an eine nicht deklarierte Queue.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-03). Die
Voruntersuchungen widersprachen sich: stiller Verlust mit Quittung (F-3, E-2)
oder 120-s-Hänger bis zur Publish-Frist (D-1). Dieser Test klärt es am echten
Broker: ``publish`` an eine Queue, die niemand deklariert hat.

Erwartet (Nutzersicht): der Producer erfährt schnell und eindeutig, dass die
Nachricht nirgends gelandet ist — eine Ausnahme binnen kurzer Zeit, und zwar
keine ``PublishIndeterminate`` (die behauptet, der Broker halte die Nachricht
womöglich schon). Die Publish-Frist ist auf 3 s gesenkt, damit ein Hänger
nach D-1 als solcher sichtbar wird statt 120 s zu kosten.

Test-Liste:
- [x] unroutbarer Publish scheitert schnell mit klarem Fehler (BZ-03)
"""

from __future__ import annotations

import time
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import PublishIndeterminate

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]

_PUBLISH_TIMEOUT_S = 3.0
# Ein klarer Fehler braucht einen Round-Trip, keine Frist.
_FAST_S = 1.5


@pytest.fixture
async def broker(rabbitmq_url: str):
    b = RabbitMQBroker(rabbitmq_url)
    b.publish_timeout_s = _PUBLISH_TIMEOUT_S
    await b.connect()
    yield b
    await b.close()


async def _publish_outcome(broker: RabbitMQBroker, queue: str) -> tuple[object, float]:
    """Ergebnis (Quittung oder Ausnahme) und Dauer eines Publish."""
    started = time.monotonic()
    try:
        outcome: object = await broker.publish(queue, b'{"probe": true}')
    except Exception as exc:
        outcome = exc
    return outcome, time.monotonic() - started


@pytest.mark.befund("BZ-03")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-03: publish an eine nicht deklarierte Queue liefert eine Quittung, "
        "die Nachricht ist still verworfen (basic.return wird ignoriert)"
    ),
)
async def test_publish_to_undeclared_queue_fails_fast_with_clear_error(broker: RabbitMQBroker) -> None:
    queue = f"bz03-never-declared-{uuid.uuid4().hex[:10]}"

    outcome, elapsed = await _publish_outcome(broker, queue)

    assert isinstance(outcome, Exception), (
        f"publish to undeclared queue {queue!r} returned {outcome!r} after {elapsed:.2f}s "
        "— the message is gone, but the producer got a receipt"
    )
    assert not isinstance(outcome, PublishIndeterminate), (
        f"publish hung until the publish deadline ({elapsed:.2f}s) and reported "
        f"'indeterminate' instead of 'unroutable': {outcome!r}"
    )
    assert elapsed < _FAST_S, f"error took {elapsed:.2f}s, expected < {_FAST_S}s"
