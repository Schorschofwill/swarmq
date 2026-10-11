"""Header-Schutz an der Broker-Grenze (BZ-17, Plan 0.12.0 MR 2).

Der AMQP-Content-Header muss in einen Frame passen; sonst schliesst der
Broker die ganze Verbindung, und alle unbestaetigten Nachbar-Zustellungen
kommen zurueck. Ab der Haelfte von ``frame_max`` werden lange Texte
gekuerzt; passt es dann immer noch nicht, wirft der Publish
``MessageHeadersTooLarge``, ohne gesendet zu haben.

Testliste:
# [x] kleine Header bleiben unveraendert
# [x] Header ueber frame_max/2: lange Texte gekuerzt, Ergebnis passt
# [x] passt auch gekuerzt nicht -> MessageHeadersTooLarge
# [x] Broker-Publish kuerzt nach dem ausgehandelten frame_max der Verbindung
# [x] Broker-Publish: zu grosse Header -> MessageHeadersTooLarge ohne
#     Publish-Versuch und ohne Wiederholung (Q14: nicht als allgemeiner
#     BrokerConnectionError nach erschoepften Versuchen)
# [x] frame_max nicht lesbar (Mock, getrennte Verbindung) -> 131072
"""

from unittest.mock import AsyncMock, MagicMock

import pamqp.encode
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.header_guard import guard_headers, negotiated_frame_max
from swarmq.exceptions import MessageHeadersTooLarge

_FRAME_MAX = 131_072


def test_small_headers_are_unchanged() -> None:
    headers = {"x-swarmq-traceback": "boom", "x-swarmq-retry-count": 1}

    assert guard_headers(headers, _FRAME_MAX) == {"x-swarmq-traceback": "boom", "x-swarmq-retry-count": 1}


def _encoded_size(headers: dict[str, object]) -> int:
    return len(pamqp.encode.field_table(headers))


def test_headers_above_half_frame_max_get_long_texts_bounded() -> None:
    headers = {"x-swarmq-traceback": "E" * 200_000, "x-swarmq-lock-render-error": "R" * 100_000, "n": 1}

    guarded = guard_headers(headers, _FRAME_MAX)

    assert guarded is not None
    assert _encoded_size(guarded) <= _FRAME_MAX // 2
    assert guarded["x-swarmq-traceback"].startswith("E" * 1000)
    assert guarded["x-swarmq-lock-render-error"].startswith("R" * 1000)
    assert guarded["n"] == 1


def test_headers_that_do_not_fit_even_when_bounded_raise() -> None:
    # Viele kurze Eintraege: Kuerzen langer Texte hilft hier nicht.
    headers = {f"x-app-{i:06d}": i for i in range(10_000)}

    with pytest.raises(MessageHeadersTooLarge, match="65536"):
        guard_headers(headers, _FRAME_MAX)


def _broker(frame_max: object = None) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = MagicMock()
    broker._connection.transport.connection.connection_tune.frame_max = frame_max
    return broker


def _published_headers(broker: RabbitMQBroker) -> dict[str, object]:
    message = broker._publish_channel.default_exchange.publish.call_args.args[0]
    return message.headers


async def test_broker_publish_bounds_headers_by_the_negotiated_frame_max() -> None:
    error_text = "E" * 20_000  # unter 131072/2, ueber 32768/2

    roomy = _broker(frame_max=131_072)
    await roomy._publish_to_dead_letter_queue("tasks", b"body", headers={"x-swarmq-traceback": error_text})
    tight = _broker(frame_max=32_768)
    await tight._publish_to_dead_letter_queue("tasks", b"body", headers={"x-swarmq-traceback": error_text})

    assert _published_headers(roomy)["x-swarmq-traceback"] == error_text
    assert len(str(_published_headers(tight)["x-swarmq-traceback"]).encode()) <= 8 * 1024


@pytest.mark.parametrize("connection", [None, MagicMock(), AsyncMock(), MagicMock(transport=None)])
def test_unreadable_frame_max_falls_back_to_the_rabbitmq_default(connection: object) -> None:
    assert negotiated_frame_max(connection) == 131_072


def test_zero_frame_max_falls_back_to_the_rabbitmq_default() -> None:
    connection = MagicMock()
    connection.transport.connection.connection_tune.frame_max = 0

    assert negotiated_frame_max(connection) == 131_072


async def test_broker_publish_with_too_large_headers_raises_without_any_publish_attempt() -> None:
    broker = _broker(frame_max=131_072)
    headers = {f"x-app-{i:06d}": i for i in range(10_000)}

    with pytest.raises(MessageHeadersTooLarge):
        await broker.publish("tasks", b"body", headers=headers)

    broker._publish_channel.default_exchange.publish.assert_not_awaited()
