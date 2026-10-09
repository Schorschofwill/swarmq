"""Aufräumen von Queue-Familien in Integrationstests (Plan 0.12.0, MR 1).

Ersetzt die eigenen Löschlisten (``(q, f"{q}.DQ", f"{q}.XQ")``) der Tests.
Die Familie kommt aus ``swarmq.queue_names.queue_family``; kommen mit MR 10
Delay-Stufen hinzu, räumen alle Tests sie ohne Änderung mit auf.
"""

from __future__ import annotations

import aio_pika

from swarmq.queue_names import queue_family


async def delete_queue_family(url: str, *queues: str) -> None:
    """Löscht jede Queue der Familien von ``queues`` über eine eigene Verbindung.

    Fehler einzelner Löschungen (Queue fehlt, Channel geschlossen) werden
    geschluckt: Aufräumen darf keinen Test rot machen. Jede Löschung bekommt
    einen eigenen Channel, weil der Broker einen Channel nach einem Fehler schließt.
    """
    connection = await aio_pika.connect(url)
    try:
        for queue in queues:
            for name in queue_family(queue):
                channel = await connection.channel()
                try:
                    await channel.queue_delete(name)
                except Exception:  # Aufräumen ist best effort
                    pass
                finally:
                    if not channel.is_closed:
                        await channel.close()
    finally:
        await connection.close()
