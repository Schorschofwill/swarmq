"""``swarmq check``: Zaehlung der Keys ohne Praefix (Review MR 15, P2-5).

Ein Zeitablauf ist kein Fehler: die bis dahin gezaehlten Keys sind eine
Untergrenze ("mindestens n, Durchlauf unvollstaendig"). Ein scheiterndes
``SCAN`` (ACL) bleibt ``unprefixed_keys`` null. Im Cluster-Modus sieht
``SCAN`` nur einen Knoten.

Test-Liste:
- [x] Zeitablauf nach einer Seite: ``unprefixed_keys`` = Teilzaehlung,
      ``unprefixed_keys_complete`` false, Befund "at least n ... incomplete", Exit 0
- [x] vollstaendiger Durchlauf: ``unprefixed_keys_complete`` true
- [x] Zeitablauf ohne gezaehlte Keys: Befund ``unprefixed_keys_unknown`` (Zeitablauf)
- [x] Cluster-Modus mit Namensraum: Hinweis, dass nur ein Knoten gezaehlt wurde
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from swarmq.cli import main
from tests.unit.test_cli_env_check_namespace import _FakeValkey, _install

pytestmark = pytest.mark.timeout(5)


class _SlowScan(_FakeValkey):
    """Die erste Seite kommt sofort, danach haengt SCAN."""

    def __init__(self, keys: list[str]) -> None:
        super().__init__(keys)
        self.calls = 0

    async def scan(
        self, cursor: int = 0, match: str | None = None, count: int | None = None
    ) -> tuple[int, list[bytes]]:
        self.calls += 1
        if self.calls > 1:
            await asyncio.Event().wait()
        return await super().scan(cursor, match, count)


def _run_json(capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, Any]]:
    code = main(["check", "--format", "json"])
    return code, json.loads(capsys.readouterr().out)


@pytest.fixture
def slow(monkeypatch: pytest.MonkeyPatch) -> _SlowScan:
    import valkey.asyncio

    from swarmq.backend.valkey import environment

    _install(monkeypatch, [])
    client = _SlowScan(["sq:result:a", "sq:lock:b", "sq:result:c"])
    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", classmethod(lambda cls, *a, **kw: client))
    monkeypatch.setattr(environment, "_COUNT_TIMEOUT_S", 0.05)
    return client


def test_a_timeout_reports_the_partial_count(slow: _SlowScan, capsys: pytest.CaptureFixture[str]) -> None:
    code, report = _run_json(capsys)

    assert code == 0
    assert (report["valkey"]["unprefixed_keys"], report["valkey"]["unprefixed_keys_complete"]) == (2, False)
    [finding] = [f for f in report["findings"] if f["code"] == "unprefixed_keys"]
    assert finding["message"].startswith("at least 2 keys")
    assert "incomplete" in finding["message"]


def test_a_timeout_before_any_key_is_reported_as_unknown(
    slow: _SlowScan, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    slow.keys = ["app1:sq:result:x", "app1:sq:result:y", "sq:result:z"]

    code, report = _run_json(capsys)

    assert code == 0
    assert (report["valkey"]["unprefixed_keys"], report["valkey"]["unprefixed_keys_complete"]) == (0, False)
    [finding] = report["findings"]
    assert finding["code"] == "unprefixed_keys_unknown"
    assert "time limit" in finding["message"]


def test_a_full_scan_is_complete(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _install(monkeypatch, ["sq:result:a", "app1:sq:result:c", "sq:lock:b"])

    _code, report = _run_json(capsys)

    assert (report["valkey"]["unprefixed_keys"], report["valkey"]["unprefixed_keys_complete"]) == (2, True)
    [finding] = report["findings"]
    assert finding["message"].startswith("2 keys")
    assert "incomplete" not in finding["message"]


def test_cluster_mode_says_only_one_node_was_counted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    client = _install(monkeypatch, ["sq:result:a", "app1:sq:result:c"])
    original = client.info

    async def _cluster_info(section: str | None = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"cluster_enabled": 1} if section == "cluster" else await original(section)

    client.info = _cluster_info  # type: ignore[method-assign]

    _code, report = _run_json(capsys)

    [finding] = [f for f in report["findings"] if f["code"] == "unprefixed_keys"]
    assert "one node" in finding["message"]
