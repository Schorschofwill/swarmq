"""Nachweis F3-18: ``swarmq check`` liefert bei ungültiger Konfiguration 0 bzw. 1
statt 78.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-18, Verdacht R3D-06).

E-006: Exit 78 bei Befunden, bei denen ein Worker den Start verweigern würde,
ausdrücklich auch bei „ungültiger Konfiguration“; ``check`` soll vor dem
Upgrade genau das vorhersagen, was der Worker tut (``cli.md:558-563``).
Heute macht ``env_valkey_eviction_check`` aus ``bogus`` still ``strict``
(Bericht „OK“, Exit 0), und ``SWARMQ_SHUTDOWN_TIMEOUT=abc`` wird als
Parse-Ausnahme zu Exit 1. Der Worker endet in beiden Fällen mit 78.

Grenze gemockt wie in ``test_cli_env_check.py``: ``valkey.asyncio.Valkey.from_url``
liefert einen Fake-Client mit sicherer Umgebung (``noeviction``).

Testliste:
# [x] SWARMQ_VALKEY_EVICTION_CHECK=bogus → Exit 78
# [x] SWARMQ_SHUTDOWN_TIMEOUT=abc → Exit 78

Behoben in 0.12.3: ``run_env_check`` baut die Konfiguration vor jeder
Verbindung wie der Pre-Flight des Starts (``start_config``); Zusatzfälle in
``test_cli_env_check.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import valkey.asyncio

from swarmq.cli import main

pytestmark = pytest.mark.timeout(5)

_SAFE_INFO: dict[str, dict[str, Any]] = {
    "memory": {"used_memory": 1024, "maxmemory": 0, "maxmemory_policy": "noeviction"},
    "server": {"valkey_version": "8.1.10", "redis_version": "7.2.4"},
    "persistence": {"aof_enabled": 1},
    "cluster": {"cluster_enabled": 0},
}


class _F318FakeValkey:
    """Nur lesende Valkey-Grenze: sichere Policy, alle Abschnitte unauffällig."""

    def __init__(self) -> None:
        self.connection_pool = SimpleNamespace(get_connection=None, release=None)

    async def ping(self) -> bool:
        return True

    async def config_get(self, *args: Any, **kwargs: Any) -> dict[bytes, bytes]:
        return {b"maxmemory-policy": b"noeviction", b"maxmemory": b"0"}

    async def info(self, section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return dict(_SAFE_INFO.get(str(section), {}))

    async def aclose(self) -> None:
        return None


@pytest.fixture
def safe_valkey(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _F318FakeValkey()

    def _from_url(*_args: object, **_kwargs: object) -> _F318FakeValkey:
        return client

    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", _from_url)
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://fake:6399/0")
    for name in (
        "SWARMQ_BROKER_URL",
        "SWARMQ_SCHEDULER",
        "SWARMQ_NAMESPACE",
        "SWARMQ_VALKEY_EVICTION_CHECK",
        "SWARMQ_SHUTDOWN_TIMEOUT",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.befund("F3-18")
@pytest.mark.entscheidung("E-006")
@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("SWARMQ_VALKEY_EVICTION_CHECK", "bogus"),
        ("SWARMQ_SHUTDOWN_TIMEOUT", "abc"),
    ],
)
@pytest.mark.usefixtures("safe_valkey")
def test_check_reports_invalid_configuration_with_exit_78(
    variable: str, value: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(variable, value)

    code = main(["check", "--format", "json"])

    capsys.readouterr()
    assert code == 78, f"{variable}={value}: der Worker verweigert den Start (78), check meldet {code}"
