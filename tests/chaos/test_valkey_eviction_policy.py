"""Verdraengende Valkey-Policy gegen einen echten Server (Plan 0.12.0, MR 14, BZ-04).

Eigener Wegwerf-Container wie in ``test_disk_pressure.py`` (eindeutiger Name,
Zufallsport, ``docker run --rm``, Aufraeumen im ``finally``): der gemeinsame
Test-Valkey bleibt unberuehrt (kein ``CONFIG SET``). Der Server startet mit
``--maxmemory 64mb --maxmemory-policy volatile-lru``.

Test-Liste:
- [x] ``ValkeyBackend.connect`` liest die Policy (``unsafe``) und warnt
- [x] Worker unter ``strict`` verweigert den Start (``ConfigurationError``, Exit 78)
- [x] ``swarmq check`` endet mit 78 und nennt den Befund
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.cli import main
from swarmq.config import SwarmQConfig
from swarmq.exceptions import ConfigurationError
from swarmq.registry import TaskRegistry
from swarmq.worker import Worker
from tests.chaos.conftest import _free_port, _wait_port
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.timeout(60)]


@pytest.fixture(scope="module")
def evicting_valkey() -> Iterator[str]:
    if subprocess.run(["docker", "info"], capture_output=True, check=False).returncode != 0:
        pytest.skip("docker not available")
    container = f"swarmq-evict-vkey-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    run = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container,
            "-p",
            f"{port}:6379",
            "valkey/valkey:8-alpine",
            "valkey-server",
            "--maxmemory",
            "64mb",
            "--maxmemory-policy",
            "volatile-lru",
            "--save",
            "",
            "--appendonly",
            "no",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if run.returncode != 0:
        pytest.skip(f"could not start valkey container: {run.stderr[-300:] if run.stderr else 'no stderr'}")
    try:
        _wait_port("127.0.0.1", port, time.monotonic() + 20)
        time.sleep(0.3)
        yield f"valkey://127.0.0.1:{port}/0"
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=15, check=False)


async def test_connect_reads_the_evicting_policy_and_warns(evicting_valkey: str) -> None:
    backend = ValkeyBackend(evicting_valkey, eviction_check="warn")
    with capture_swarmq_logs() as entries:
        await backend.connect()
    try:
        assert backend.eviction_check is not None
        assert backend.eviction_check.status == "unsafe"
        assert backend.eviction_check.policy == "volatile-lru"
        assert backend.eviction_check.maxmemory == 64 * 1024 * 1024
        assert "valkey_eviction_policy_unsafe" in [e["event"] for e in entries]
    finally:
        await backend.close()


async def test_worker_refuses_to_start_under_strict(evicting_valkey: str) -> None:
    broker: Any = MagicMock(spec=RabbitMQBroker)
    broker.connect = AsyncMock()
    broker.close = AsyncMock()
    config = SwarmQConfig(broker_url="amqp://unused", backend_url=evicting_valkey, valkey_eviction_check="strict")
    backend = ValkeyBackend(evicting_valkey)
    worker = Worker(broker=broker, backend=backend, registry=TaskRegistry(), config=config)

    try:
        with pytest.raises(ConfigurationError, match="volatile-lru"):
            await worker.start()
    finally:
        # Das Backend gehoert dem Aufrufer; ein abgebrochener Start schliesst es nicht.
        await backend.close()


def test_check_exits_78(
    evicting_valkey: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SWARMQ_BACKEND_URL", evicting_valkey)
    monkeypatch.delenv("SWARMQ_VALKEY_EVICTION_CHECK", raising=False)

    code = main(["check", "--format", "json"])

    report = json.loads(capsys.readouterr().out)
    assert code == 78
    assert [f["code"] for f in report["findings"] if f["level"] == "error"] == ["eviction_policy"]
