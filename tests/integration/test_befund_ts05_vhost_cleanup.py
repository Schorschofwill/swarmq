"""Nachweis TS-05: Läuft das Löschen einer Worker-vhost in die Frist, bleibt
die vhost halb gelöscht liegen.

Register: docs/befunde/teststrenge-2026-10.md (TS-05; Gegenprüfung
2026-11-01).

``worker_vhosts`` (``tests/support/services.py``) löscht am Sitzungsende mit
gemeinsamer Frist von 3 s. Bricht der Client das ``DELETE /vhosts/<v>`` ab,
bricht RabbitMQ (4.3.6 und 4.2.9) auch die Löschung ab: vhost und ein Teil der
Queues bleiben. Reicht die Frist für den ersten Broker nicht, bekommt der
zweite gar kein ``DELETE``. Lokal am 2026-10-31 116 vhosts mit 13 643 Queues,
Speicheralarm, 12 Integrationstests im Timeout.

Erwartet: ``docs/testing/test-strategy.md`` (die vhost wird „am Ende samt
allen Queues gelöscht“), Zweck von ``delete_session_queues`` (Speicheralarm
und OOM vom 2026-10-13).

Testliste:
# [x] Frist läuft im DELETE ab -> die vhost ist trotzdem bald weg
"""

from __future__ import annotations

import dataclasses
import uuid

import aio_pika
import httpx
import pytest

from tests.support import services
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]

_QUEUES = 50
_BUDGET_S = 0.02


async def _declare_quorum_queues(url: str, count: int) -> None:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        for index in range(count):
            await channel.declare_queue(f"ts05-{index}", durable=True, arguments={"x-queue-type": "quorum"})
    finally:
        await connection.close()


@pytest.mark.befund("TS-05")
@pytest.mark.xfail(strict=True, reason="TS-05: ein abgebrochenes DELETE lässt die vhost halb gelöscht zurück")
async def test_a_worker_vhost_is_gone_even_when_its_delete_runs_out_of_time() -> None:
    node = dataclasses.replace(services.RABBITMQ_MAIN, vhost=f"swarmq-test-ts05-{uuid.uuid4().hex[:8]}")
    with services.worker_vhosts([node], budget=_BUDGET_S):
        await _declare_quorum_queues(node.amqp_url, _QUEUES)

    async with httpx.AsyncClient(base_url=node.management_url, auth=("guest", "guest"), timeout=30.0) as client:

        async def vhost_gone() -> bool:
            return (await client.get(f"/vhosts/{node.vhost_api}")).status_code == 404

        try:
            assert await wait_until(vhost_gone, timeout=30, interval=0.5), (
                "the worker vhost still exists 30 s after the session ended"
            )
        finally:
            await client.delete(f"/vhosts/{node.vhost_api}")  # Nachweis räumt selbst ab
