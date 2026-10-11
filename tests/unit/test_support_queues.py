"""``tests.support.queues.delete_queue_family``: Aufräumen einer Queue-Familie (Plan 0.12.0, MR 1).

Ersetzt die eigenen Löschlisten der Integrationstests. Die Familie kommt aus
``swarmq.queue_names.queue_family``, damit spätere Delay-Stufen (MR 10)
automatisch mit aufgeräumt werden. Der Broker ist die Grenze und wird gemockt.

Test-Liste:
- [x] löscht Haupt-, Delay- und Dead-Letter-Queue einer Familie
- [x] mehrere Familien in einem Aufruf
- [x] ein fehlschlagendes Löschen bricht nicht ab; die übrigen Queues werden gelöscht
- [x] die Verbindung wird immer geschlossen
- [x] die Delay-Stufen ``{q}.DQ.<stufe>`` werden mit gelöscht (MR 10; Nachtrag, von Georg am 2026-10-10 freigegeben)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.support import queues

pytestmark = [pytest.mark.timeout(5)]


@pytest.fixture
def connection(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    conn = MagicMock()
    conn.close = AsyncMock()
    channel = MagicMock()
    channel.queue_delete = AsyncMock()
    channel.close = AsyncMock()
    channel.is_closed = False
    conn.channel = AsyncMock(return_value=channel)
    conn.test_channel = channel
    monkeypatch.setattr(queues.aio_pika, "connect", AsyncMock(return_value=conn))
    return conn


def _deleted(connection: MagicMock) -> list[str]:
    return [c.args[0] for c in connection.test_channel.queue_delete.await_args_list]


async def test_deletes_main_delay_and_dead_letter_queue(connection: MagicMock) -> None:
    await queues.delete_queue_family("amqp://x/", "jobs")

    assert _deleted(connection)[:3] == ["jobs", "jobs.DQ", "jobs.XQ"]


async def test_deletes_the_delay_tiers_of_the_family(connection: MagicMock) -> None:
    await queues.delete_queue_family("amqp://x/", "jobs")

    assert _deleted(connection) == [
        "jobs",
        "jobs.DQ",
        "jobs.XQ",
        *(f"jobs.DQ.{label}" for label in ("1s", "4s", "15s", "1m", "4m", "15m", "1h")),
    ]


async def test_deletes_several_families(connection: MagicMock) -> None:
    await queues.delete_queue_family("amqp://x/", "a", "b")

    deleted = _deleted(connection)
    assert deleted[:3] == ["a", "a.DQ", "a.XQ"]
    assert "b" in deleted and deleted.index("b") > deleted.index("a.XQ")


async def test_failing_delete_does_not_stop_the_others(connection: MagicMock) -> None:
    connection.test_channel.queue_delete.side_effect = [RuntimeError("406")] + [None] * 9

    await queues.delete_queue_family("amqp://x/", "jobs")

    assert _deleted(connection)[:3] == ["jobs", "jobs.DQ", "jobs.XQ"]


async def test_connection_is_closed(connection: MagicMock) -> None:
    await queues.delete_queue_family("amqp://x/", "jobs")

    connection.close.assert_awaited_once()
