"""``ValkeyBackend.connect()``/``close()`` nebenläufig (Plan 0.14, U2, KTD2, E-082).

Beide laufen unter einer gemeinsamen Lebenszyklus-Sperre: Ein zweites
``connect()`` während eines laufenden Aufbaus wartet und nutzt den gelungenen
Client; ``close()`` wartet ein laufendes ``connect()`` ab und schließt danach.
Kein geteilter Aufbau-Task: Scheitert der laufende Aufbau oder wird er
abgebrochen, versucht ein Wartender es selbst (offene Frage 1, Entscheid (a)),
und der Abbruch schließt den eigenen Client (F4-13).

Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert je Aufruf den
nächsten Fake-Client; dessen ``PING`` wartet auf ein Event. Ein Aufrufer, der
während des Aufbaus dazukommt, startet mit ``eager_start=True``: Er läuft bis
zu seinem ersten Warten, ohne dass der Test schläft.

Testliste:
# [x] zwei gleichzeitige connect(): ein Client, beide kehren zurück, ping() gelingt
# [x] close() während connect(): kehrt erst nach connect() zurück, danach geschlossen, Client genau einmal aclose
# [x] erster connect() scheitert: der wartende zweite versucht selbst
# [x] erster connect() abgebrochen: sein Client geschlossen, der wartende zweite verbindet
# [x] connect, close, connect: neuer Client
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError

pytestmark = pytest.mark.timeout(5)

_URL = "valkey://valkey.invalid:6379/0"
_FROM_URL = "swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url"


class _GatedValkey:
    """Fake-Client, dessen ``PING`` bis ``gate`` wartet und optional scheitert."""

    def __init__(self, gate: asyncio.Event | None = None, *, fail: BaseException | None = None) -> None:
        self._gate = gate
        self._fail = fail
        self.ping_started = asyncio.Event()
        self.pings = 0
        self.acloses = 0
        self.connection_pool = MagicMock()  # close() sperrt den Pool

    async def ping(self) -> bool:
        self.pings += 1
        self.ping_started.set()
        if self._gate is not None:
            await self._gate.wait()
        if self._fail is not None:
            raise self._fail
        return True

    async def aclose(self) -> None:
        self.acloses += 1


def _backend() -> ValkeyBackend:
    return ValkeyBackend(_URL, eviction_check="off")


@pytest.mark.entscheidung("E-082")
async def test_concurrent_connects_build_a_single_client() -> None:
    gate = asyncio.Event()
    building, spare = _GatedValkey(gate), _GatedValkey()
    backend = _backend()
    try:
        with patch(_FROM_URL, side_effect=[building, spare]) as from_url:
            first = asyncio.create_task(backend.connect())
            await building.ping_started.wait()
            second = asyncio.create_task(backend.connect(), eager_start=True)
            gate.set()
            await asyncio.gather(first, second)

            assert from_url.call_count == 1, "a second client was built while the first connect() was running"
            await backend.ping()
            assert building.pings == 2
            assert spare.acloses == 0
    finally:
        await backend.close()
    assert building.acloses == 1


@pytest.mark.entscheidung("E-082")
async def test_close_waits_for_a_running_connect_and_closes_its_client() -> None:
    gate = asyncio.Event()
    building = _GatedValkey(gate)
    backend = _backend()
    with patch(_FROM_URL, side_effect=[building]):
        connecting = asyncio.create_task(backend.connect())
        await building.ping_started.wait()
        closing = asyncio.create_task(backend.close(), eager_start=True)

        assert not closing.done(), "close() returned while connect() was still building the client"
        gate.set()
        await connecting
        await closing

    assert building.acloses == 1
    with pytest.raises(BackendConnectionError):
        await backend.ping()


@pytest.mark.entscheidung("E-082")
async def test_a_waiting_connect_tries_itself_after_the_running_one_failed() -> None:
    """Offene Frage 1, Entscheid (a): kein geteilter Fehler, der Wartende baut selbst."""
    gate = asyncio.Event()
    failing = _GatedValkey(gate, fail=ValkeyConnectionError("Error 111 connecting. Connection refused."))
    healthy = _GatedValkey()
    backend = _backend()
    try:
        with patch(_FROM_URL, side_effect=[failing, healthy]) as from_url:
            first = asyncio.create_task(backend.connect())
            await failing.ping_started.wait()
            second = asyncio.create_task(backend.connect(), eager_start=True)
            gate.set()

            with pytest.raises(BackendConnectionError):
                await first
            await second

            assert from_url.call_count == 2
        assert failing.acloses == 1, "the failed connect() closes its own client (F4-13)"
        assert healthy.pings == 1
        await backend.ping()
    finally:
        await backend.close()


@pytest.mark.entscheidung("E-082")
async def test_cancelling_the_running_connect_closes_its_client_and_the_waiter_connects() -> None:
    """Der Abbruch wirkt nur im Task des Aufrufers (F4-13); der Wartende verbindet selbst."""
    building = _GatedValkey(asyncio.Event())
    healthy = _GatedValkey()
    backend = _backend()
    try:
        with patch(_FROM_URL, side_effect=[building, healthy]):
            first = asyncio.create_task(backend.connect())
            await building.ping_started.wait()
            second = asyncio.create_task(backend.connect(), eager_start=True)
            first.cancel()

            with pytest.raises(asyncio.CancelledError):
                await first
            await second

        assert building.acloses == 1
        assert healthy.pings == 1
        await backend.ping()
    finally:
        await backend.close()


async def test_connect_after_close_builds_a_new_client() -> None:
    old, new = _GatedValkey(), _GatedValkey()
    backend = _backend()
    try:
        with patch(_FROM_URL, side_effect=[old, new]):
            await backend.connect()
            await backend.close()
            await backend.connect()

        assert old.acloses == 1
        assert new.pings == 1
        await backend.ping()
        assert new.pings == 2
    finally:
        await backend.close()
