"""Kanal-Close ohne Abbruch (F4-30, Plan 2026-10-29-1945, KTD1).

Bricht eine Frist oder ein Abbruch des Aufrufers ein laufendes
``channel.close()`` ab, schickt aiormq im Abbruchzweig von ``Channel.rpc`` ein
zweites ``Channel.Close(504)`` auf dieselbe Kanalnummer (aiormq 7.0.0 wie
7.2.4). Hat der Broker das erste Close schon beantwortet, ist die Nummer bei
ihm frei; der zweite Frame gilt als neuer Kanal ohne ``channel.open``, und
RabbitMQ schließt die ganze Verbindung (``CHANNEL_ERROR - expected
'channel.open'``).

``close_without_abort`` begrenzt darum nur das Warten. Das Close läuft als
eigener Task weiter, bis der Broker antwortet oder aiormqs eigene Frist
(Heartbeat) greift; dieses Modul hält ihn bis dahin fest und holt seinen
Ausgang ab, damit nichts als „never retrieved“ endet. Wie E-053 für die
Consumer-RPCs.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol


class _Closable(Protocol):
    def close(self) -> Any: ...


#: Close-Tasks, deren Aufrufer nicht mehr wartet; bis zu ihrem Ende hier gehalten.
_detached: set[asyncio.Task[Any]] = set()


def _settle(task: asyncio.Task[Any]) -> None:
    _detached.discard(task)
    if not task.cancelled():
        # Best effort: Der Aufrufer hat seine Frist schon gemeldet; ein spätes
        # Scheitern des Close ist kein eigener Fehler mehr.
        task.exception()


async def close_without_abort(channel: _Closable, *, timeout_s: float | None) -> None:
    """Wie ``asyncio.wait_for(channel.close(), timeout_s)``, nur ohne das Close abzubrechen.

    Fertig in der Frist: kehrt zurück oder wirft den Fehler des Close. Frist
    abgelaufen: ``TimeoutError``. Abbruch des Aufrufers: ``CancelledError``.
    In beiden letzten Fällen läuft das Close weiter. ``timeout_s=None`` wartet
    ohne Frist, schützt das Close aber ebenso vor einem Abbruch des Aufrufers.
    """
    task: asyncio.Task[Any] = asyncio.ensure_future(channel.close())
    _detached.add(task)
    task.add_done_callback(_settle)
    done, _ = await asyncio.wait({task}, timeout=timeout_s)
    if not done:
        raise TimeoutError(f"channel close did not finish within {timeout_s}s")
    task.result()
