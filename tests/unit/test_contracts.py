"""Laufzeit-Smoke der Verträge (KTD1): ``@runtime_checkable`` prüft nur
Member-Präsenz — als Smoke, nicht als Vertragsbeweis. Plus die
Werteigenschaften der kleinen Nachrichtentypen."""

from __future__ import annotations

import copy
import pickle
from typing import TYPE_CHECKING, Any

import pytest

from swarmq.contracts import (
    Consumer,
    Delivery,
    LockToken,
    Publisher,
    PublishReceipt,
    RateLimitDecision,
)
from swarmq.contracts.broker import DeadLetterScan

if TYPE_CHECKING:
    from collections.abc import Callable


class _HasPublisherMembers:
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def ping(self) -> None: ...
    async def declare_queue(self, queue: str) -> None: ...
    async def publish(self, queue: str, body: bytes, headers: dict[str, Any] | None = None) -> PublishReceipt:
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


# DeadLetterScan (F4-05, E-028 Nachtrag 2026-10-17, KTD11):
# [x] entpackt wie ``(erstes, scanned)``, gleich dem Tupel, trägt ``hit_limit``
# [x] die drei Formen passen ohne Unterdrückung auf die ``BrokerAdmin``-Signaturen (mypy/basedpyright)
# [x] copy/deepcopy/pickle wie beim Tupel, ``hit_limit`` bleibt erhalten (Review U17)


def test_dead_letter_scan_unpacks_like_the_pair_and_carries_hit_limit() -> None:
    scan = DeadLetterScan([{"task_id": "t-1"}], 1, hit_limit=True)

    entries, scanned = scan

    assert entries == [{"task_id": "t-1"}]
    assert scanned == 1
    assert scan.hit_limit is True
    assert scan == ([{"task_id": "t-1"}], 1)


def test_dead_letter_scan_fits_the_three_broker_admin_result_shapes() -> None:
    # Statischer Teil: mypy strict und basedpyright prüfen die Zuweisungen
    # an die ``tuple[..., int]``-Formen des Protocols (KTD11).
    listed: tuple[list[dict[str, Any]], int] = DeadLetterScan([], 0, hit_limit=False)
    found: tuple[bool, int] = DeadLetterScan(True, 3, hit_limit=False)
    moved: tuple[int, int] = DeadLetterScan(2, 5, hit_limit=True)

    assert (listed, found, moved) == (([], 0), (True, 3), (2, 5))


def _pickled(scan: object) -> object:
    return pickle.loads(pickle.dumps(scan))


@pytest.mark.parametrize("duplicate", [copy.copy, copy.deepcopy, _pickled])
def test_dead_letter_scan_survives_copy_and_pickle(duplicate: Callable[[object], object]) -> None:
    scan = DeadLetterScan([{"task_id": "t-1"}], 1, hit_limit=True)

    clone = duplicate(scan)

    assert clone == scan
    assert getattr(clone, "hit_limit", None) is True
