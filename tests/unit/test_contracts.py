"""Laufzeit-Smoke der Verträge (KTD1): ``@runtime_checkable`` prüft nur
Member-Präsenz — als Smoke, nicht als Vertragsbeweis. Plus die
Werteigenschaften der kleinen Nachrichtentypen."""

from __future__ import annotations

from swarmq.contracts import (
    Consumer,
    Delivery,
    LockToken,
    Publisher,
    PublishReceipt,
    RateLimitDecision,
)


class _HasPublisherMembers:
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def ping(self) -> None: ...
    async def declare_queue(self, queue: str) -> None: ...
    async def publish(self, queue: str, body: bytes, headers: dict | None = None) -> PublishReceipt:
        return PublishReceipt(queue=queue)

    def set_publish_deadline(self, at: float | None) -> None: ...


def test_runtime_checkable_is_true_with_members_and_false_without() -> None:
    assert isinstance(_HasPublisherMembers(), Publisher)
    assert not isinstance(_HasPublisherMembers(), Consumer)
    assert not isinstance(object(), Publisher)


def test_delivery_equality_ignores_the_broker_handle() -> None:
    a = Delivery(body=b"x", headers={"h": 1}, queue="q.high", handle=object())
    b = Delivery(body=b"x", headers={"h": 1}, queue="q.high", handle=object())
    assert a == b
    assert a != Delivery(body=b"x", headers={"h": 1}, queue="q")


def test_receipt_and_tokens_are_frozen_values() -> None:
    assert PublishReceipt(queue="q") == PublishReceipt(queue="q", message_id=None)
    assert LockToken("l", "o", 1) == LockToken(name="l", owner="o", max_concurrency=1)
    assert RateLimitDecision(True, 2, 0).retry_after_ms == 0
