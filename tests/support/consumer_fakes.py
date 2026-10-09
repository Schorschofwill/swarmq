"""Fakes der aio-pika-Teile, die Consumer-Wächter (U6) und Settle-Regeln (U7)
benutzen: ``RobustQueue.cancel``/``consume``, ``channel.get_underlay_channel``
und ``consumers`` des aiormq-Channels. Das Verhalten der echten Broker steht in
``tests/integration/broker/test_consumer_timeout.py``."""

from __future__ import annotations

from typing import Any


class FakeUnderlay:
    """aiormq-Channel: ``consumers`` nach Tag, ``is_closed``."""

    def __init__(self, *tags: str) -> None:
        self.consumers: dict[str, Any] = {tag: object() for tag in tags}
        self.is_closed = False


class FakeChannel:
    """aio-pika-Channel; ``underlay`` lässt sich austauschen (Restore)."""

    def __init__(self, underlay: FakeUnderlay) -> None:
        self.underlay = underlay

    async def get_underlay_channel(self) -> FakeUnderlay:
        return self.underlay


class FakeQueue:
    """``RobustQueue`` mit Cancel/Consume auf dem aktuellen Underlay."""

    def __init__(self, name: str, channel: FakeChannel) -> None:
        self.name = name
        self.channel = channel
        self.cancelled: list[str] = []
        self.consumed: list[Any] = []
        self.cancel_error: Exception | None = None
        self.consume_error: Exception | None = None

    async def cancel(self, tag: str, timeout: float | None = None) -> None:
        if self.cancel_error is not None:
            raise self.cancel_error
        self.cancelled.append(tag)
        self.channel.underlay.consumers.pop(tag, None)

    async def consume(self, callback: Any, timeout: float | None = None) -> str:
        if self.consume_error is not None:
            raise self.consume_error
        self.consumed.append(callback)
        tag = f"{self.name}-new-{len(self.consumed)}"
        self.channel.underlay.consumers[tag] = callback
        return tag
