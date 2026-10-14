"""Scheduler-Fire nach verstrichener Publish-Endfrist (FS-02 Teil a, Plan
2026-10-14-0748, U3, KTD3, KTD18).

Ein Leader im Herunterfahren feuert zwischen ``drain_until`` und
``scheduler.stop()``. Der Publish geht nicht raus und meldet ein
eindeutiges "nicht gesendet": Der eigene Unique-Claim wird freigegeben (kein
Commit), und der Fehler zaehlt nach E-013 wie jeder
``BrokerConnectionError`` fuer den Ruecktritt.

Testliste:
- [x] Fire nach der Endfrist: nichts gesendet, Claim frei, kein Commit -- der
      naechste Poll (Frist geloescht) feuert denselben Termin
- [x] der "nicht gesendet"-Fehler zaehlt fuer den Ruecktritt (E-013-Nachtrag, KTD18)
"""

from __future__ import annotations

import asyncio
from typing import cast
from unittest.mock import AsyncMock, Mock

import msgspec
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.scheduler.stepdown import PublishStepdown
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock
from tests.unit.scheduler._helpers import _make_loop

pytestmark = [pytest.mark.timeout(5)]


def _rabbit() -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.publish_timeout_s = 60.0
    broker._connection = Mock(transport=None, is_closed=False)
    broker._connection.ready = AsyncMock()
    broker._publish_channel = AsyncMock(is_closed=False)
    return broker


def _sent(broker: RabbitMQBroker) -> list[str]:
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
    return [call.kwargs["routing_key"] for call in publish.await_args_list]


async def _unique_cron_loop(broker: RabbitMQBroker, config, lock_manager, signal_dispatcher):
    memory = InMemoryBackend(FakeClock())
    await memory.connect()
    await memory.locks.acquire(SCHEDULER_LOCK_NAME, owner="test-owner", ttl_ms=60_000, max_concurrency=1)
    definition = ScheduleDefinition(
        schedule_name="nightly",
        task_name="ping",
        args=(),
        kwargs={},
        queue="default",
        cron="*/5 * * * *",
        fire_ts_ms=1_000,
        source="cli",
        unique_for=3600,
    )
    await memory.schedules.add("nightly", msgspec.json.encode(definition), 1_000)
    loop = _make_loop(memory, broker, config, lock_manager, ScheduleStore(memory), signal_dispatcher)
    loop._state = "leader"
    return loop


async def test_fire_after_the_deadline_sends_nothing_and_releases_the_own_claim(
    config, lock_manager, signal_dispatcher
) -> None:
    broker = _rabbit()
    loop = await _unique_cron_loop(broker, config, lock_manager, signal_dispatcher)
    broker.set_publish_deadline(asyncio.get_running_loop().time() - 1.0)

    await loop._poll_tick()  # Endfrist verstrichen: nichts gesendet, kein Commit

    assert _sent(broker) == []
    broker.set_publish_deadline(None)
    await loop._poll_tick()  # derselbe Termin; der Claim war frei

    assert _sent(broker) == ["default"]


async def test_not_sent_after_the_deadline_counts_for_the_stepdown() -> None:
    broker = _rabbit()
    broker.set_publish_deadline(asyncio.get_running_loop().time() - 1.0)
    with pytest.raises(BrokerConnectionError) as info:
        await broker.publish("default", b"{}")
    error = info.value
    assert not isinstance(error, PublishIndeterminate)

    now = [1000.0]
    tracker = PublishStepdown(30.0, clock=lambda: now[0])
    assert tracker.failure(error) is False
    now[0] += 30.0

    assert tracker.failure(error) is True
