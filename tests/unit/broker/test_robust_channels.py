"""Kanäle ohne Wiederherstellung nach dem Schließen (F4-20, ``broker/rabbitmq/robust_channels.py``).

Wegwerf-Kanäle sind nicht robust; Publish- und Consume-Kanal bleiben robust und
werden beim Verwurf oder Schließen aus dem ``WeakSet`` der ``RobustConnection``
abgemeldet. Das ``WeakSet`` ist privates aio-pika-API: fehlt es, gilt fail-open
mit einer WARNING je Verbindung (wie beim Blocked-Zustand, E-025).

Test-Liste:
# [x] keine aio-pika-Verbindung (Test-Double): der Wegwerf-Kanal kommt aus ``connection.channel()``
#     (einfache ``aio_pika.Connection``: ``tests/unit/broker/test_underlay.py``, KTD6)
# [x] ``release_channel`` ohne ``RobustConnection``: nichts zu tun, keine WARNING
# [x] ``WeakSet`` fehlt (aio-pika geändert): fail-open, eine WARNING je Verbindung, kein Fehler
# [x] ``close()`` auf geliehener Verbindung: der Reconnect des Aufrufers öffnet den Publish-Kanal nicht wieder
# [x] aio-pika 10.2 (Versionsschutz, U6): ein per ``close()`` geschlossener ``RobustChannel`` kommt auch
#     ohne ``release_channel`` nach einem Reconnect nicht wieder; ein noch offener schon (verworfen,
#     das Close wartet im Alarm), darum bleibt ``release_channel``
# [x] aio-pika 10.2 (Versionsschutz, F4-22): ``_on_connected`` setzt ``connected``, bevor die
#     Kanäle wiederhergestellt sind; das Restore-Fenster aus F4-22 gibt es weiter
# [x] ``release_queue``: nur diese Queue verlässt die Restore-Liste des ``RobustChannel`` (PF-20, E-083)
# [x] ``readmit_queue``: die Queue kommt zurück in die Restore-Liste
# [x] ``_queues`` fehlt (aio-pika geändert): fail-open, eine WARNING je Kanal; ohne ``RobustChannel`` nichts zu tun
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import Iterator
from typing import Any, override
from unittest.mock import Mock

import aio_pika
import pytest
from yarl import URL

from swarmq.broker.rabbitmq.robust_channels import (
    open_throwaway_channel,
    readmit_queue,
    release_channel,
    release_queue,
)
from tests.unit.broker.test_befund_f4_20_restore_closed_channels import (
    _connected_broker,
    _FakeAiormq,
    _FakeAiormqChannel,
    _FakeTransport,
    _reconnect,
    _settle,
)

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def no_collection() -> Iterator[None]:
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


def test_a_connection_not_from_aio_pika_opens_its_own_channel() -> None:
    connection = Mock()
    channel = Mock()
    connection.channel.return_value = channel

    assert open_throwaway_channel(connection) is channel


def test_release_without_a_robust_connection_does_nothing(swarmq_logs) -> None:
    release_channel(Mock(), Mock())

    assert [e for e in swarmq_logs if e["log_level"] == "warning"] == []


async def test_a_missing_channel_set_fails_open_with_one_warning_per_connection(swarmq_logs) -> None:
    connection = aio_pika.RobustConnection(URL("amqp://guest:guest@localhost/"))
    other = aio_pika.RobustConnection(URL("amqp://guest:guest@localhost/"))
    # aio-pika ohne das private WeakSet (umbenannt oder entfernt)
    del connection._RobustConnection__channels  # type: ignore[attr-defined]
    del other._RobustConnection__channels  # type: ignore[attr-defined]

    release_channel(connection, Mock())
    release_channel(connection, Mock())
    release_channel(other, Mock())

    warnings = [e["event"] for e in swarmq_logs if e["log_level"] == "warning"]
    assert warnings == ["robust_channel_release_unavailable"] * 2


@pytest.mark.usefixtures("no_collection")
async def test_closing_a_broker_on_a_borrowed_connection_releases_its_publish_channel() -> None:
    broker, connection, first = await _connected_broker()

    await broker.close()
    fresh = await _reconnect(connection, first)

    assert sorted(fresh.channels) == [], "the caller's reconnect does not reopen swarmq's publish channel"


@pytest.mark.usefixtures("no_collection")
async def test_aio_pika_does_not_reopen_a_robust_channel_closed_with_close() -> None:
    _, connection, first = await _connected_broker()
    closed = await connection.channel()
    kept = await connection.channel()
    await closed.close()

    fresh = await _reconnect(connection, first)

    assert closed.is_closed
    assert not kept.is_closed, "precondition: the reconnect restores the channels still open"
    assert len(fresh.channels) == 2, "the publish channel and the kept one, not the closed one"


class _HeldAiormq(_FakeAiormq):
    """Öffnet Kanäle erst nach ``release``: Der Restore hängt im ``Channel.Open``."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()

    @override
    async def channel(self, **kwargs: Any) -> _FakeAiormqChannel:
        await self.release.wait()
        return await super().channel(**kwargs)


@pytest.mark.befund("F4-22")
@pytest.mark.usefixtures("no_collection")
async def test_aio_pika_sets_connected_before_it_restores_the_channels() -> None:
    broker, connection, first = await _connected_broker()
    channel = broker._publish_channel
    assert channel is not None
    connection.transport = None
    connection.connected.clear()
    await first.drop()
    await _settle()
    fresh = _HeldAiormq()
    connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection

    reconnect = asyncio.create_task(connection._on_connected())
    await asyncio.wait_for(connection.connected.wait(), timeout=1)

    assert channel.is_closed, "connected is set while the channel still waits for its restore"
    fresh.release.set()
    await reconnect
    assert not channel.is_closed


def _robust_channel() -> aio_pika.RobustChannel:

    return aio_pika.RobustChannel(aio_pika.RobustConnection(URL("amqp://guest:guest@localhost/")))


def _registered(channel: aio_pika.RobustChannel, name: str) -> aio_pika.RobustQueue:
    """Eine Queue in der Restore-Liste, wie ``declare_queue(passive=True)`` sie anlegt."""
    queue = aio_pika.RobustQueue(channel, name, passive=True)
    channel._queues[name].add(queue)
    return queue


@pytest.mark.entscheidung("E-083")
async def test_a_released_queue_leaves_the_restore_list_and_the_others_stay() -> None:
    channel = _robust_channel()
    deleted = _registered(channel, "a")
    twin = _registered(channel, "b")
    other = _registered(channel, "b")

    release_queue(deleted, "a")
    release_queue(twin, "b")

    assert dict(channel._queues) == {"b": {other}}


@pytest.mark.entscheidung("E-083")
async def test_a_missing_queue_registry_fails_open_with_one_warning_per_channel(swarmq_logs) -> None:
    channel = _robust_channel()
    other = _robust_channel()
    queue = aio_pika.RobustQueue(channel, "a", passive=True)
    # aio-pika ohne die private Restore-Liste (umbenannt oder entfernt)
    del channel._queues
    del other._queues

    release_queue(queue, "a")
    release_queue(queue, "a")
    release_queue(aio_pika.RobustQueue(other, "a", passive=True), "a")

    warnings = [e["event"] for e in swarmq_logs if e["log_level"] == "warning"]
    assert warnings == ["robust_queue_release_unavailable"] * 2


def test_release_and_readmit_without_a_robust_channel_do_nothing(swarmq_logs) -> None:
    queue = Mock(channel=Mock(spec=aio_pika.Channel))

    release_queue(queue, "a")
    readmit_queue(queue, "a")

    assert [e for e in swarmq_logs if e["log_level"] == "warning"] == []


@pytest.mark.entscheidung("E-083")
async def test_a_readmitted_queue_is_back_in_the_restore_list() -> None:
    channel = _robust_channel()
    queue = _registered(channel, "a")
    release_queue(queue, "a")

    readmit_queue(queue, "a")

    assert dict(channel._queues) == {"a": {queue}}
