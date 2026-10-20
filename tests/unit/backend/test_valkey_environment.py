"""Pruefung der Valkey-Eviction-Policy beim Verbinden (Plan 0.12.0, MR 14, BZ-04).

Die Nachweise stehen in ``test_befund_bz04_eviction_policy.py``; hier die
Faelle drumherum. Grenze gemockt: ``valkey.asyncio.Valkey.from_url`` liefert
einen Fake-Client.

Test-Liste:
- [x] weder ``CONFIG`` noch ``INFO`` beantwortet -> WARNING „nicht pruefbar“,
      ``connect`` wirft nicht, Ergebnis ``unknown`` am Backend
- [x] Ergebnis (Status, Policy, ``maxmemory``) liegt am Backend
- [x] ``maxmemory`` 0 -> sicher, auch mit verdraengender Policy
- [x] haengende Abfrage -> nach dem Zeitlimit ``unknown``, ``connect`` kehrt zurueck
- [x] ``eviction_check="off"`` (Parameter oder Umgebung) -> keine Abfrage, keine Warnung
- [x] injizierter Client -> keine Pruefung (kein Ergebnis)
- [x] erneute Pruefung (Leader, stuendlich): Ergebnis mit ``used_memory``, Warnung bei Befund
- [x] erneute Pruefung mit ``off`` -> ``None``, keine Abfrage
- [x] Testlücke D-2 (Runde 4): ``maxmemory`` aus ``INFO memory``, wenn
      ``CONFIG GET`` verboten ist (E-005: Grenze 0 ist sicher)
- [x] Testlücke D-2: ``CONFIG GET`` fragt nach dem Muster ``maxmemory*``
- [x] Testlücke D-2: ein hängender ``INFO``-Abschnitt in ``describe_valkey``
      endet nach der Frist, die übrigen Angaben bleiben
- [x] Testlücke D-2: ``count_keys`` genau am Limit ist vollständig
"""

from __future__ import annotations

import asyncio
import fnmatch
from typing import Any, override

import pytest
import valkey.asyncio
from valkey.exceptions import NoPermissionError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey.environment import count_keys, describe_valkey, probe_eviction
from tests.support.virtual_time import on_virtual_clock

pytestmark = pytest.mark.timeout(5)


class _FakeValkey:
    def __init__(
        self,
        config: dict[bytes, bytes] | None = None,
        info: dict[str, Any] | None = None,
    ) -> None:
        self._config = config
        self._info = info
        self.calls: list[str] = []

    async def ping(self) -> bool:
        return True

    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        self.calls.append("config_get")
        if self._config is None:
            raise NoPermissionError("this user has no permissions to run the 'config|get' command")
        return self._config

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("info")
        if self._info is None:
            raise NoPermissionError("this user has no permissions to run the 'info' command")
        return self._info

    async def aclose(self) -> None:
        return None


def _install(monkeypatch: pytest.MonkeyPatch, fake: Any) -> None:
    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(lambda cls, *a, **kw: fake))


def _events(entries: list[dict[str, Any]], level: str = "warning") -> list[str]:
    return [e["event"] for e in entries if e["log_level"] == level]


async def test_unanswered_policy_query_warns_and_connect_succeeds(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    _install(monkeypatch, _FakeValkey())
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert backend.eviction_check is not None
    assert backend.eviction_check.status == "unknown"
    assert "valkey_eviction_policy_unknown" in _events(swarmq_logs)


async def test_result_of_the_check_lies_on_the_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1048576"}))
    backend = ValkeyBackend("valkey://fake:6399/0")
    before_connect = backend.eviction_check
    assert before_connect is None

    await backend.connect()

    assert backend.eviction_check is not None
    assert (backend.eviction_check.status, backend.eviction_check.policy, backend.eviction_check.maxmemory) == (
        "unsafe",
        "allkeys-lru",
        1048576,
    )


async def test_without_memory_limit_no_policy_evicts(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    """``maxmemory`` 0: Valkey verdraengt nie, die Policy greift nicht."""
    _install(monkeypatch, _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"0"}))
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert backend.eviction_check is not None
    assert backend.eviction_check.status == "safe"
    assert _events(swarmq_logs) == []


class _HangingValkey(_FakeValkey):
    def __init__(self) -> None:
        super().__init__()
        self.never = asyncio.Event()

    @override
    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        await self.never.wait()
        return {}

    @override
    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        await self.never.wait()
        return {}


async def test_hanging_policy_query_ends_after_the_time_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swarmq.backend.valkey.environment._PROBE_TIMEOUT_S", 0.05)
    _install(monkeypatch, _HangingValkey())
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert backend.eviction_check is not None
    assert backend.eviction_check.status == "unknown"


@pytest.mark.entscheidung("E-006")
async def test_check_off_queries_nothing_and_stays_silent(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0", eviction_check="off")

    await backend.connect()

    assert fake.calls == []
    assert backend.eviction_check is None
    assert _events(swarmq_logs) == []


async def test_check_off_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ohne Parameter gilt ``SWARMQ_VALKEY_EVICTION_CHECK`` (Q11: gelesen ueber ``config.py``);
    auch CLI-Befehle bauen ihr Backend ohne ``SwarmQConfig``."""
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "OFF")
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0")

    await backend.connect()

    assert fake.calls == []


async def test_injected_client_is_not_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    backend = ValkeyBackend(client=fake)  # type: ignore[arg-type]

    await backend.connect()

    assert fake.calls == []
    assert backend.eviction_check is None


async def test_recheck_reports_memory_and_warns(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"noeviction", b"maxmemory": b"1000"}, info={"used_memory": 250})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0")
    await backend.connect()
    fake._config = {b"maxmemory-policy": b"volatile-lru", b"maxmemory": b"1000"}

    check = await backend.recheck_eviction()

    assert check is not None
    assert (check.status, check.policy, check.maxmemory, check.used_memory) == ("unsafe", "volatile-lru", 1000, 250)
    assert backend.eviction_check == check
    assert "valkey_eviction_policy_unsafe" in _events(swarmq_logs)


async def test_recheck_with_check_off_queries_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeValkey(config={b"maxmemory-policy": b"allkeys-lru", b"maxmemory": b"1"})
    _install(monkeypatch, fake)
    backend = ValkeyBackend("valkey://fake:6399/0", eviction_check="off")
    await backend.connect()

    assert await backend.recheck_eviction() is None
    assert fake.calls == []


@pytest.mark.entscheidung("E-005")
@pytest.mark.parametrize(
    ("maxmemory", "status"),
    [pytest.param(0, "safe", id="ohne-grenze"), pytest.param(1_048_576, "unsafe", id="mit-grenze")],
)
async def test_maxmemory_comes_from_info_when_config_is_forbidden(maxmemory: int, status: str) -> None:
    """Testlücke D-2 (Runde 4): Verbietet eine ACL ``CONFIG GET``, liest die
    Prüfung Policy **und** ``maxmemory`` aus ``INFO memory``. Ohne Grenze
    (``maxmemory`` 0) verdrängt Valkey nie, die Instanz ist sicher und
    blockiert den Start nicht (E-005).

    Gegenprobe-Mutant: ``environment.py`` in ``_from_info``
    ``values.get("maxmemory")`` → ``values.get("MAXMEMORY")``."""
    fake = _FakeValkey(info={"maxmemory_policy": "allkeys-lru", "maxmemory": maxmemory})

    check = await probe_eviction(fake)

    assert (check.status, check.policy, check.maxmemory) == (status, "allkeys-lru", maxmemory)


class _PatternValkey(_FakeValkey):
    """Beantwortet ``CONFIG GET`` wie Valkey: nur Parameter, die zum Muster passen."""

    @override
    async def config_get(self, pattern: str = "*", *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        answer = await super().config_get(pattern, *args, **kwargs)
        return {k: v for k, v in answer.items() if fnmatch.fnmatchcase(k.decode(), pattern)}


async def test_config_get_asks_for_the_maxmemory_parameters() -> None:
    """Testlücke D-2 (Runde 4): ``CONFIG GET`` nennt das Muster
    ``maxmemory*``, das ``maxmemory-policy`` und ``maxmemory`` trifft. Das
    Ergebnis stammt aus ``CONFIG`` (``INFO`` sagt hier bewusst etwas anderes).

    Gegenprobe-Mutant: ``environment.py`` in ``_from_config``
    ``"maxmemory*"`` → ``"XXmaxmemory*XX"``."""
    fake = _PatternValkey(
        config={b"maxmemory-policy": b"noeviction", b"maxmemory": b"4096", b"save": b""},
        info={"maxmemory_policy": "allkeys-lru", "maxmemory": 1},
    )

    check = await probe_eviction(fake)

    assert (check.status, check.policy, check.maxmemory) == ("safe", "noeviction", 4096)


class _HangingServerSection(_FakeValkey):
    """``INFO server`` hängt; die anderen Abschnitte antworten."""

    def __init__(self) -> None:
        super().__init__(
            config={b"maxmemory-policy": b"noeviction", b"maxmemory": b"0"},
            info={"used_memory": 10, "aof_enabled": 1, "cluster_enabled": 0},
        )
        self.never = asyncio.Event()

    @override
    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if section == "server":
            await self.never.wait()
        return await super().info(section, *args, **kwargs)


@on_virtual_clock
async def test_a_hanging_info_section_ends_after_the_time_limit() -> None:
    """Testlücke D-2 (Runde 4): ``describe_valkey`` (``swarmq check``) begrenzt
    jeden ``INFO``-Abschnitt; ein hängender Abschnitt liefert nach der Frist
    unbekannte Werte, die übrigen Angaben bleiben. Virtuelle Uhr: Die Frist
    von 2 s kostet keine Echtzeit.

    Gegenprobe-Mutant: ``environment.py`` in ``_info_section``
    ``asyncio.timeout(_PROBE_TIMEOUT_S)`` → ``asyncio.timeout(None)`` (unter der
    virtuellen Uhr rot als ``RuntimeError`` „nichts geplant“: das Hängen)."""
    loop = asyncio.get_running_loop()
    started = loop.time()

    description = await describe_valkey(_HangingServerSection())

    assert description["version"] is None
    assert (description["eviction"], description["aof_enabled"], description["cluster_enabled"]) == (
        "safe",
        True,
        False,
    )
    assert loop.time() - started == pytest.approx(2.0)


class _ScanningValkey:
    """``SCAN`` liefert die Seiten nacheinander; der Cursor 0 beendet die Zählung."""

    def __init__(self, pages: list[tuple[int, list[bytes]]]) -> None:
        self._pages = pages
        self.scans = 0

    async def scan(self, cursor: int, *, match: str, count: int) -> tuple[int, list[bytes]]:
        page = self._pages[self.scans]
        self.scans += 1
        return page


async def test_count_keys_exactly_at_the_limit_is_complete() -> None:
    """Testlücke D-2 (Runde 4): Erreicht die Zählung genau das Limit, endet sie
    sofort und meldet das Limit als vollständige Zahl; ``False`` gilt nur für
    den Zeitablauf.

    Gegenprobe-Mutant: ``environment.py`` in ``count_keys``
    ``return limit, True`` → ``return limit, False``."""
    fake = _ScanningValkey([(7, [b"a", b"b"]), (9, [b"c"]), (0, [b"d"])])

    assert await count_keys(fake, "sq:*", limit=3) == (3, True)
    assert fake.scans == 2
