"""Eng begrenzte Ausnahmen von ``filterwarnings = error`` (Plan 2026-10-13-0902, KTD2).

Jede Ausnahme gehört zu einem Befund in ``docs/befunde/teststrenge-2026-10.md``
und steht nur an den dort genannten Tests. Wird aiormq angehoben, die
Ausnahmen probeweise entfernen.

Die Filter greifen an der ResourceWarning selbst, nicht an pytests
``PytestUnraisableExceptionWarning``: Wird die Warnung in ``__del__`` zur
Exception, hält deren Traceback das Objekt fest. Transport und Socket
darunter warnen dann erst, wenn pytest die Meldung verworfen hat, also in
einem späteren, unbeteiligten Test.
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import Iterator

import pytest

# StreamWriter.__del__: offener Writer, oder Writer einer geschlossenen Loop.
_STREAMWRITER = (
    "ignore:unclosed <StreamWriter:ResourceWarning",
    "ignore:loop is closed:ResourceWarning",
)

# TS-01, TS-02, TS-03: aiormq lässt den ``StreamWriter`` einer Verbindung
# offen, wenn ihr Aufbau abgebrochen wird (TS-01), Server bzw. Proxy sie
# schließen (TS-02) oder aio-pika sie beim Wiederverbinden nach dem Schließen
# noch anlegt (TS-03). Nur diese Warnungen; der Valkey-Client nutzt ebenfalls
# StreamWriter, deshalb keine globale Ausnahme.
aiormq_streamwriter_leak = pytest.mark.filterwarnings(*_STREAMWRITER)

# TS-02 beim Broker-Neustart: Scheitert ein Wiederverbindungsversuch, bleibt
# auch der Transport offen; neben dem StreamWriter warnen dann Transport und
# Socket. Breiter als ``aiormq_streamwriter_leak`` (deckt auch offene
# Valkey-Sockets), deshalb nur an Tests, die den Broker neu starten.
aiormq_restart_leak = pytest.mark.filterwarnings(
    *_STREAMWRITER,
    "ignore:unclosed transport <_SelectorSocketTransport:ResourceWarning",
    "ignore:unclosed <socket.socket:ResourceWarning",
)


def close_orphaned_streams() -> None:
    """Schließt die Sockets von StreamWritern, deren Event-Loop schon zu ist (TS-02).

    Nach gescheiterten Wiederverbindungen beim Broker-Neustart bleiben
    aiormq-Writer zurück, an denen keine von Python aus sichtbare Referenz
    mehr hängt; sie werden erst irgendwann später frei, und ihre Finalizer
    warnten dann in einem unbeteiligten Test. Hier werden sie ohne Warnung
    geschlossen. Nur am Ende von Tests, die den Broker neu starten.
    """
    gc.collect()
    for obj in gc.get_objects():
        if not isinstance(obj, asyncio.StreamWriter):
            continue
        transport = obj.transport
        loop = obj._loop  # type: ignore[attr-defined]  # asyncio bietet keinen öffentlichen Zugriff auf die Loop des Writers
        if transport.is_closing() or not loop.is_closed():
            continue
        # transport.close() bräuchte die geschlossene Loop; deshalb direkt
        # am Socket, und den Transport als geschlossen markieren, damit
        # weder Writer noch Transport noch Socket beim Abräumen warnen.
        sock = transport._sock  # type: ignore[attr-defined]  # Socket des Selector-Transports, nicht in der Transport-API
        if sock is not None:
            sock.close()
        transport._sock = None  # type: ignore[attr-defined]  # verhindert die Warnung in __del__
        transport._closing = True  # type: ignore[attr-defined]  # is_closing() ohne die geschlossene Loop


@pytest.fixture(autouse=True, scope="module")
def close_orphaned_streams_after_module() -> Iterator[None]:
    """Autouse fuer Chaos-Bereiche (TS-02): verwaiste aiormq-Writer am
    Modulende schliessen. Netzwerk-Abrisse und Broker-Neustarts hinterlassen
    Writer, an denen keine sichtbare Referenz mehr haengt; ohne das warnten
    sie spaeter in einem unbeteiligten Test. Ein Leck, das noch im eigenen
    Test abgeraeumt wird, laesst diesen weiterhin scheitern."""
    yield
    close_orphaned_streams()
