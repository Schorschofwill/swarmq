"""Valkey-Kosten je Task (Plan 2026-10-24-1118, KTD6).

Gezählt werden Befehle und Roundtrips gegen das Test-Valkey (Ports und
Sperre setzt ``just kosten-valkey``), nicht Zeit (R10). Die Datenbank teilt
``tests/support/services.py`` zu; sie wird vor und nach jedem Test geleert.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from collections.abc import AsyncIterator, Iterable
from typing import Any, override

import pytest
from valkey.asyncio.connection import AbstractConnection

from swarmq.backend.valkey import ValkeyBackend
from swarmq.task import Task, TaskInfo
from tests.kosten.conftest import QUEUE, KostenOk, KostenRetry, KostenSchritt, Pipeline, baue_pipeline
from tests.support.inmemory import FakeClock, InMemoryBroker
from tests.support.services import (
    VALKEY_HOST,
    VALKEY_PORT,
    flush_valkey_db,
    port_is_open,
    require_safe_target,
    valkey_db_url,
)


class KostenLock(Task):
    """Rate-Limit und Lock; das Limit ist hoch genug für alle Läufe eines Tests."""

    task_info = TaskInfo(name="kosten_lock", queues=[QUEUE], rate_limit="1000/s", lock="kosten", hard_timeout=30)

    @override
    async def run(self, *args: Any) -> list[Any]:
        return list(args)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/kosten/valkey/" in str(item.fspath):
            item.add_marker(pytest.mark.kosten_valkey)


@pytest.fixture(autouse=True, scope="session")
def valkey_erreichbar() -> None:
    """Wie ``check_docker``, nur für Valkey: erst die Prüfung gegen fremde
    Stacks auf 6379, dann überspringen, wenn kein Valkey läuft (die CI
    wartet vorher auf den Dienst)."""
    require_safe_target(VALKEY_HOST, VALKEY_PORT)
    if not port_is_open(VALKEY_HOST, VALKEY_PORT):
        pytest.skip(f"Valkey ({VALKEY_HOST}:{VALKEY_PORT}) nicht erreichbar")


@pytest.fixture(autouse=True)
async def leere_valkey() -> AsyncIterator[None]:
    await flush_valkey_db(valkey_db_url())
    yield
    await flush_valkey_db(valkey_db_url())


def _befehle(paket: bytes) -> list[tuple[bytes, ...]]:
    """Die RESP-Arrays (``*n`` Bulk-Strings) eines Schreibauftrags, je Befehl
    seine Argumente."""
    befehle: list[tuple[bytes, ...]] = []
    position = 0
    while position < len(paket):
        ende = paket.index(b"\r\n", position)
        assert paket[position : position + 1] == b"*", "kein RESP-Array"
        anzahl, position = int(paket[position + 1 : ende]), ende + 2
        argumente: list[bytes] = []
        for _ in range(anzahl):
            ende = paket.index(b"\r\n", position)
            laenge = int(paket[position + 1 : ende])
            argumente.append(paket[ende + 2 : ende + 2 + laenge])
            position = ende + 2 + laenge + 2
        befehle.append(tuple(argumente))
    return befehle


class Leitung:
    """Zählt, was über ``send_packed_command`` an Valkey geht: ein Aufruf
    ist ein Roundtrip, ein Pipeline- oder MULTI-Block also einer mit allen
    seinen Befehlen. Gezählt wird nur innerhalb von ``messung``."""

    def __init__(self) -> None:
        self.roundtrips: list[list[tuple[bytes, ...]]] = []
        self._aktiv = False

    def mitschreiben(self, command: bytes | str | Iterable[bytes]) -> None:
        if not self._aktiv:
            return
        if isinstance(command, str):
            command = command.encode()
        paket = command if isinstance(command, bytes) else b"".join(command)
        self.roundtrips.append(_befehle(paket))

    @contextlib.asynccontextmanager
    async def messung(self) -> AsyncIterator[None]:
        """Zählt im Block und bis alle Tasks fertig sind, die er gestartet
        hat, etwa der Abbau eines Pub/Sub im Hintergrund (``results.py``):
        Sonst fiele dessen UNSUBSCRIBE mal in die Messung und mal nicht."""
        vorher = asyncio.all_tasks()
        self.roundtrips.clear()
        self._aktiv = True
        try:
            yield
            nachzuegler = asyncio.all_tasks() - vorher - {asyncio.current_task()}
            if nachzuegler:
                _, offen = await asyncio.wait(nachzuegler, timeout=5)
                assert not offen, f"Tasks aus der Messung laufen noch: {offen}"
        finally:
            self._aktiv = False

    def werte(self) -> dict[str, int]:
        """Roundtrips, Befehle und Befehle je Name für die Basislinie."""
        namen = Counter(f"valkey.{befehl[0].decode().upper()}" for rt in self.roundtrips for befehl in rt)
        return {
            "valkey.roundtrips": len(self.roundtrips),
            "valkey.befehle": sum(namen.values()),
            **namen,
        }


@pytest.fixture
def leitung(monkeypatch: pytest.MonkeyPatch) -> Leitung:
    """Hülle um ``send_packed_command`` jeder Verbindung (private Schnittstelle
    von valkey-py, Upgrade-Checkliste): auch Handshake, Health-Check-PING,
    Pub/Sub und das Auto-Pipelining der Skripte laufen darüber."""
    leitung = Leitung()
    original = AbstractConnection.send_packed_command

    async def zaehlend(self: AbstractConnection, command: Any, check_health: bool = True) -> None:
        leitung.mitschreiben(command)
        await original(self, command, check_health)

    monkeypatch.setattr(AbstractConnection, "send_packed_command", zaehlend)
    return leitung


@pytest.fixture
async def valkey_pipeline() -> AsyncIterator[Pipeline]:
    """Client und Worker gegen das echte ``ValkeyBackend`` samt Stores; der
    Broker bleibt ``InMemoryBroker`` (``SwarmQ.start_worker`` lehnt
    injizierte Clients ab)."""
    clock = FakeClock()
    broker, backend = InMemoryBroker(clock), ValkeyBackend(valkey_db_url(), namespace="kosten")
    await broker.connect()
    await backend.connect()
    try:
        yield await baue_pipeline(clock, broker, backend, Counter(), (KostenOk, KostenRetry, KostenSchritt, KostenLock))
    finally:
        await broker.close()
        await backend.close()
